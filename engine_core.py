"""
engine_core.py — WealthPilot v3 Shared Core
Fixes applied:
  - DB write retry logic (prevents scanner/app SQLite lock collisions)
"""
from __future__ import annotations
import json, os, sqlite3, smtplib, ssl, time
from datetime import datetime, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
DB_PATH  = BASE_DIR / "ledger" / "portfolio.db"
LOG_PATH = BASE_DIR / "ledger" / "activity.jsonl"
CFG_PATH = BASE_DIR / "ledger" / "config.json"


# ── DB with retry (Fix #4: SQLite lock collisions between scanner and app) ────
def get_db(retries: int = 5, delay: float = 0.3) -> sqlite3.Connection:
    """
    Opens SQLite with WAL mode + retry on lock.
    WAL (Write-Ahead Logging) allows concurrent reads while writing,
    virtually eliminating lock collisions between app.py and scanner.py.
    """
    DB_PATH.parent.mkdir(exist_ok=True)
    last_err = None
    for attempt in range(retries):
        try:
            c = sqlite3.connect(DB_PATH, timeout=10)
            c.row_factory = sqlite3.Row
            # WAL mode: concurrent readers don't block writers
            c.execute("PRAGMA journal_mode=WAL")
            c.execute("PRAGMA busy_timeout=5000")  # wait up to 5s on lock
            c.executescript("""
                CREATE TABLE IF NOT EXISTS trades (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    date TEXT, side TEXT DEFAULT 'conservative',
                    symbol TEXT, action TEXT,
                    shares REAL, price REAL, amount REAL,
                    gain REAL DEFAULT 0,
                    note TEXT, mode TEXT DEFAULT 'auto'
                );
                CREATE TABLE IF NOT EXISTS savings (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    date TEXT, amount REAL, source TEXT, total REAL
                );
                CREATE TABLE IF NOT EXISTS withdrawals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    date TEXT, amount REAL, reason TEXT, total_after REAL
                );
                CREATE TABLE IF NOT EXISTS alerts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    date TEXT, type TEXT, symbol TEXT, detail TEXT,
                    side TEXT DEFAULT 'conservative', seen INTEGER DEFAULT 0
                );
                CREATE TABLE IF NOT EXISTS scan_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    date TEXT, type TEXT, result TEXT, action_taken TEXT
                );
                CREATE TABLE IF NOT EXISTS cross_transfers (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    date TEXT, from_side TEXT, to_side TEXT,
                    amount REAL, source_gain REAL, note TEXT
                );
                CREATE TABLE IF NOT EXISTS email_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    date TEXT, subject TEXT, status TEXT, error TEXT
                );
                CREATE TABLE IF NOT EXISTS portfolio_snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    date TEXT, conservative_value REAL, crypto_value REAL,
                    savings REAL, total REAL
                );
                CREATE TABLE IF NOT EXISTS rotation_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    date TEXT, sold_symbol TEXT, sold_amount REAL,
                    sold_price REAL, gain REAL, principal REAL,
                    bought_symbol TEXT, bought_amount REAL, saved REAL, reason TEXT
                );
                CREATE TABLE IF NOT EXISTS principal_protection_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    date TEXT, symbol TEXT, principal_extracted REAL,
                    trigger TEXT, note TEXT
                );
            """)
            c.commit()
            _migrate(c)   # patch any columns missing from older DB versions
            return c
        except sqlite3.OperationalError as e:
            last_err = e
            if attempt < retries - 1:
                time.sleep(delay * (attempt + 1))
    raise RuntimeError(f"DB unavailable after {retries} retries: {last_err}")


def db_write(fn):
    """
    Decorator for safe DB writes with retry.
    Usage: @db_write on any function that commits to the DB.
    """
    import functools
    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        for attempt in range(5):
            try:
                return fn(*args, **kwargs)
            except sqlite3.OperationalError as e:
                if "locked" in str(e).lower() and attempt < 4:
                    time.sleep(0.2 * (attempt + 1))
                else:
                    raise
    return wrapper


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00","Z")

def local_date() -> str:
    return datetime.now().strftime("%B %d, %Y")

def log(event: dict) -> None:
    LOG_PATH.parent.mkdir(exist_ok=True)
    try:
        with LOG_PATH.open("a") as f:
            f.write(json.dumps({**event, "ts": utc_now()}) + "\n")
    except Exception:
        pass  # logging must never crash the app


def _migrate(c: sqlite3.Connection) -> None:
    """
    Safe schema migrations for existing databases.
    ALTER TABLE ADD COLUMN is idempotent-guarded by checking PRAGMA table_info.
    Runs every startup — only adds columns that don't already exist.
    This is what fixes the 'no such column: side' error on databases created
    by earlier versions of WealthPilot.
    """
    def has_column(table: str, col: str) -> bool:
        rows = c.execute(f"PRAGMA table_info({table})").fetchall()
        return any(r[1] == col for r in rows)

    def has_table(table: str) -> bool:
        return bool(c.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,)
        ).fetchone())

    migrations = [
        # trades table
        ("trades", "side",   "ALTER TABLE trades ADD COLUMN side TEXT DEFAULT 'conservative'"),
        ("trades", "gain",   "ALTER TABLE trades ADD COLUMN gain REAL DEFAULT 0"),
        ("trades", "shares", "ALTER TABLE trades ADD COLUMN shares REAL"),
        # alerts table
        ("alerts", "side",   "ALTER TABLE alerts ADD COLUMN side TEXT DEFAULT 'conservative'"),
        # rotation_log — principal column added in v3 fixed
        ("rotation_log", "principal", "ALTER TABLE rotation_log ADD COLUMN principal REAL DEFAULT 0"),
    ]

    for table, col, sql in migrations:
        if has_table(table) and not has_column(table, col):
            try:
                c.execute(sql)
                c.commit()
                log({"event": "db_migration", "table": table, "col": col, "status": "applied"})
            except Exception as e:
                log({"event": "db_migration_error", "table": table, "col": col, "error": str(e)})

    # Back-fill side='conservative' for any rows that got NULL from the migration
    if has_table("trades") and has_column("trades", "side"):
        c.execute("UPDATE trades SET side='conservative' WHERE side IS NULL")
        c.commit()
    if has_table("alerts") and has_column("alerts", "side"):
        c.execute("UPDATE alerts SET side='conservative' WHERE side IS NULL")
        c.commit()


# ── Config ────────────────────────────────────────────────────────────────────
DEFAULTS = {
    "risk_profile":          "moderate",
    "monthly_budget":        100.0,
    "mode":                  "paper",      # paper | live
    "auto_mode":             True,
    "rotation_threshold":    1.0,          # % yield improvement to trigger rotation
    "rotation_safety":       True,         # must also improve safety score
    "crash_sell":            True,
    "crash_threshold":       -0.05,        # -5% SPY
    "ma200_filter":          True,         # NEW: block buys when below 200-day MA
    "scan_interval_hours":   4,
    "scan_throttle_ms":      500,          # NEW: ms pause between each ETF scan (rate limit)
    "wash_sale_cooldown_days": 31,         # NEW: Canada superficial loss rule
    "crypto_symbols":        ["BTC/CAD","ETH/CAD"],
    "crypto_profit_take":    0.20,
    "crypto_stop_loss":      -0.15,
    "crypto_use_atr_stop":   True,         # NEW: volatility-adjusted stops
    "crypto_atr_multiplier": 2.0,          # NEW: how many ATRs below entry = stop
    "crypto_principal_protect": True,      # NEW: pull original cost when position doubles
    "crypto_reinvest_pct":   0.70,
    "cross_transfer_pct":    0.30,
    "savings_goal":          500.0,
}

def load_config() -> dict:
    cfg = dict(DEFAULTS)
    if CFG_PATH.exists():
        try:
            cfg.update(json.loads(CFG_PATH.read_text()))
        except Exception:
            pass
    return cfg

def save_config(updates: dict) -> dict:
    cfg = load_config()
    cfg.update(updates)
    CFG_PATH.parent.mkdir(exist_ok=True)
    CFG_PATH.write_text(json.dumps(cfg, indent=2))
    return cfg


# ── Email — MC Gardener SMTP pattern ─────────────────────────────────────────
def smtp_send(subject: str, html: str) -> tuple[bool, str]:
    """
    Gmail SMTP port 587 STARTTLS.
    Env vars match MC Gardener Pro exactly:
      ALERT_EMAIL_FROM, ALERT_EMAIL_PASSWORD (App Password), ALERT_EMAIL_TO
    """
    frm = os.getenv("ALERT_EMAIL_FROM","")
    pwd = os.getenv("ALERT_EMAIL_PASSWORD","")
    to  = os.getenv("ALERT_EMAIL_TO", frm)
    if not frm or not pwd:
        return False, "Email not configured (ALERT_EMAIL_FROM / ALERT_EMAIL_PASSWORD missing)"
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject; msg["From"] = frm; msg["To"] = to
    msg.attach(MIMEText(html, "html"))
    try:
        ctx = ssl.create_default_context()
        with smtplib.SMTP("smtp.gmail.com", 587) as srv:
            srv.ehlo(); srv.starttls(context=ctx)
            srv.login(frm, pwd); srv.sendmail(frm, to, msg.as_string())
        conn = get_db()
        conn.execute("INSERT INTO email_log (date,subject,status,error) VALUES (?,?,?,?)",
                     (utc_now(), subject, "sent", ""))
        conn.commit(); conn.close()
        return True, "sent"
    except Exception as e:
        try:
            conn = get_db()
            conn.execute("INSERT INTO email_log (date,subject,status,error) VALUES (?,?,?,?)",
                         (utc_now(), subject, "failed", str(e)))
            conn.commit(); conn.close()
        except Exception:
            pass
        return False, str(e)


# ── Savings ───────────────────────────────────────────────────────────────────
def add_savings(amount: float, source: str) -> float:
    conn = get_db()
    prev = conn.execute("SELECT total FROM savings ORDER BY id DESC LIMIT 1").fetchone()
    new_total = round((prev["total"] if prev else 0.0) + amount, 2)
    conn.execute("INSERT INTO savings (date,amount,source,total) VALUES (?,?,?,?)",
                 (utc_now(), amount, source, new_total))
    conn.commit(); conn.close()
    return new_total

def get_savings_balance() -> float:
    conn = get_db()
    row = conn.execute("SELECT total FROM savings ORDER BY id DESC LIMIT 1").fetchone()
    conn.close()
    return row["total"] if row else 0.0

def withdraw_savings(amount: float, reason: str = "") -> dict:
    conn = get_db()
    current = get_savings_balance()
    if amount <= 0: conn.close(); return {"status":"error","reason":"Amount must be > $0"}
    if amount > current:
        conn.close()
        return {"status":"error","reason":f"Insufficient savings. Available: ${current:.2f}"}
    new_total   = round(current - amount, 2)
    reason_text = reason.strip() or "No reason provided"
    conn.execute("INSERT INTO savings (date,amount,source,total) VALUES (?,?,?,?)",
                 (utc_now(), -amount, f"withdrawal: {reason_text}", new_total))
    conn.execute("INSERT INTO withdrawals (date,amount,reason,total_after) VALUES (?,?,?,?)",
                 (utc_now(), amount, reason_text, new_total))
    conn.commit(); conn.close()
    log({"event":"withdrawal","amount":amount,"reason":reason_text,"balance_after":new_total})
    return {"status":"ok","withdrawn":amount,"balance_before":current,"balance_after":new_total,
            "note":"Ledger updated. Transfer manually via Alpaca or your bank."}
