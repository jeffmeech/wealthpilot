"""
engine.py — WealthPilot Core Engine v2
New: variable contributions, savings withdrawal, daily HTML email report,
     per-position rationale, MC Gardener-style Gmail SMTP.
"""
from __future__ import annotations
import json, os, sqlite3, smtplib, ssl
from datetime import datetime, timezone, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from typing import Any

BASE_DIR  = Path(__file__).resolve().parent
DB_PATH   = BASE_DIR / "ledger" / "portfolio.db"
LOG_PATH  = BASE_DIR / "ledger" / "activity.jsonl"
CFG_PATH  = BASE_DIR / "ledger" / "config.json"

ALPACA_PAPER_BASE = "https://paper-api.alpaca.markets/v2"
ALPACA_LIVE_BASE  = "https://api.alpaca.markets/v2"
ALPACA_DATA_BASE  = "https://data.alpaca.markets/v2"

RISK_PROFILES = {
    "conservative": {
        "label": "Conservative",
        "description": "Capital preservation first. Bonds, gold, staples. Weathers crashes.",
        "color": "#60b8f0",
        "allocations": [
            ("SCHD",  0.25, "Dividend growth ETF — steady income, 10yr track record"),
            ("VYM",   0.20, "High yield dividend — broad diversification"),
            ("TLT",   0.20, "Long-term treasury bonds — rises when stocks fall"),
            ("XLP",   0.15, "Consumer staples — recession-proof spending"),
            ("IAUM",  0.10, "Gold micro ETF — inflation and geopolitical hedge"),
            ("XLU",   0.10, "Utilities — defensive income, low correlation"),
        ]
    },
    "moderate": {
        "label": "Moderate",
        "description": "Balanced growth and income. Bonds buffer downturns.",
        "color": "#c8f060",
        "allocations": [
            ("SCHD",  0.30, "Dividend growth ETF — core income holding"),
            ("VYM",   0.20, "High yield dividend — portfolio income base"),
            ("QQQ",   0.20, "Nasdaq-100 growth — tech and innovation upside"),
            ("TLT",   0.15, "Treasury bond hedge — crash buffer"),
            ("IAUM",  0.10, "Gold hedge — store of value"),
            ("XLP",   0.05, "Consumer staples — stability anchor"),
        ]
    },
    "aggressive": {
        "label": "Aggressive",
        "description": "Growth-tilted with dividend income. Higher volatility.",
        "color": "#f0a060",
        "allocations": [
            ("QQQ",   0.35, "Nasdaq-100 growth — primary growth engine"),
            ("SCHD",  0.25, "Dividend growth — income to offset volatility"),
            ("VGT",   0.20, "Tech sector ETF — concentrated tech exposure"),
            ("VYM",   0.15, "High yield dividend — income cushion"),
            ("IAUM",  0.05, "Gold micro hedge — minimal crash protection"),
        ]
    },
    "max": {
        "label": "Max Growth",
        "description": "Pure growth. High risk/reward. Not crash-resistant.",
        "color": "#f06060",
        "allocations": [
            ("QQQ",   0.40, "Nasdaq-100 — broad tech and growth"),
            ("VGT",   0.30, "Tech sector — concentrated upside"),
            ("SCHD",  0.20, "Dividend growth — partial income floor"),
            ("ARKK",  0.10, "Disruptive innovation — high-beta speculation"),
        ]
    }
}

REINVEST_PCT = 0.70
SAVINGS_PCT  = 0.30
GOAL_SAVINGS = 500.0


# ── DB ────────────────────────────────────────────────────────────────────────
def get_db() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(exist_ok=True)
    c = sqlite3.connect(DB_PATH)
    c.row_factory = sqlite3.Row
    c.executescript("""
        CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT, symbol TEXT, action TEXT,
            shares REAL, price REAL, amount REAL, note TEXT,
            mode TEXT DEFAULT 'auto'
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
            date TEXT, type TEXT, symbol TEXT, detail TEXT, seen INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS portfolio_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT, market_value REAL, invested REAL, savings REAL, risk_profile TEXT
        );
        CREATE TABLE IF NOT EXISTS email_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT, subject TEXT, status TEXT, error TEXT
        );
    """)
    c.commit()
    return c


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

def local_date() -> str:
    return datetime.now().strftime("%B %d, %Y")

def log(event: dict) -> None:
    LOG_PATH.parent.mkdir(exist_ok=True)
    with LOG_PATH.open("a") as f:
        f.write(json.dumps({**event, "ts": utc_now()}) + "\n")


# ── Config ────────────────────────────────────────────────────────────────────
def load_config() -> dict:
    defaults = {"risk_profile": "moderate", "monthly_budget": 100.0, "mode": "paper", "auto_mode": True}
    if CFG_PATH.exists():
        try:
            saved = json.loads(CFG_PATH.read_text())
            defaults.update(saved)
        except Exception:
            pass
    return defaults

def save_config(cfg: dict) -> None:
    CFG_PATH.parent.mkdir(exist_ok=True)
    CFG_PATH.write_text(json.dumps(cfg, indent=2))


# ── Alpaca HTTP ───────────────────────────────────────────────────────────────
def _base() -> str:
    return ALPACA_LIVE_BASE if load_config().get("mode") == "live" else ALPACA_PAPER_BASE

def _headers() -> dict:
    return {"APCA-API-KEY-ID": os.getenv("ALPACA_PAPER_KEY",""), "APCA-API-SECRET-KEY": os.getenv("ALPACA_PAPER_SECRET","")}

def _get(path: str, base: str | None = None) -> Any:
    import urllib.request
    url = (base or _base()).rstrip("/") + "/" + path.lstrip("/")
    try:
        with urllib.request.urlopen(urllib.request.Request(url, headers=_headers()), timeout=10) as r:
            return json.loads(r.read())
    except Exception as e:
        log({"event": "get_error", "url": url, "error": str(e)}); return None

def _post(path: str, body: dict, base: str | None = None) -> Any:
    import urllib.request, urllib.error
    url  = (base or _base()).rstrip("/") + "/" + path.lstrip("/")
    data = json.dumps(body).encode()
    req  = urllib.request.Request(url, data=data, headers={**_headers(),"Content-Type":"application/json"}, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        txt = e.read().decode()
        log({"event":"post_error","url":url,"status":e.code,"detail":txt}); return {"error":txt,"status":e.code}
    except Exception as e:
        log({"event":"post_error","url":url,"error":str(e)}); return None

def get_quote(symbol: str) -> float | None:
    data = _get(f"/stocks/{symbol}/trades/latest", base=ALPACA_DATA_BASE)
    return float(data["trade"]["p"]) if data and "trade" in data else None

def get_account() -> dict | None: return _get("/account")
def get_positions() -> list[dict]:
    r = _get("/positions"); return r if isinstance(r, list) else []
def get_orders(status="all", limit=20) -> list[dict]:
    r = _get(f"/orders?status={status}&limit={limit}&direction=desc"); return r if isinstance(r, list) else []

def place_market_buy(symbol: str, notional: float) -> dict:
    return _post("/orders", {"symbol":symbol,"notional":round(notional,2),"side":"buy","type":"market","time_in_force":"day"}) or {"error":"No response"}

def place_market_sell(symbol: str, qty: float) -> dict:
    return _post("/orders", {"symbol":symbol,"qty":round(qty,6),"side":"sell","type":"market","time_in_force":"day"}) or {"error":"No response"}


# ── Market intelligence ───────────────────────────────────────────────────────
def get_spy_change() -> float | None:
    data = _get("/stocks/SPY/bars/latest?timeframe=1Day", base=ALPACA_DATA_BASE)
    if data and "bar" in data:
        b = data["bar"]
        if b.get("o") and b["o"] != 0: return (b["c"] - b["o"]) / b["o"]
    return None

def compute_rsi(closes: list[float], period: int = 14) -> float | None:
    if len(closes) < period + 1: return None
    gains, losses = [], []
    for i in range(1, len(closes)):
        d = closes[i] - closes[i-1]; gains.append(max(d,0)); losses.append(max(-d,0))
    ag = sum(gains[-period:]) / period; al = sum(losses[-period:]) / period
    return 100.0 if al == 0 else 100 - (100 / (1 + ag/al))

def get_bars(symbol: str, days: int = 30) -> list[float]:
    end = datetime.now(timezone.utc); start = end - timedelta(days=days+10)
    url = (f"/stocks/{symbol}/bars?timeframe=1Day&start={start.strftime('%Y-%m-%d')}"
           f"&end={end.strftime('%Y-%m-%d')}&limit={days+10}")
    data = _get(url, base=ALPACA_DATA_BASE)
    return [b["c"] for b in data["bars"]] if data and "bars" in data else []

def market_sentiment() -> dict:
    spy_change = get_spy_change(); closes = get_bars("SPY", 30); rsi = compute_rsi(closes) if closes else None
    signal = "hold"; detail = "Markets normal. Auto-pilot running."
    if spy_change is not None:
        if   spy_change <= -0.05: signal="crash";   detail=f"SPY down {abs(spy_change)*100:.1f}% — crash protection active, deploy paused."
        elif spy_change <= -0.02: signal="caution"; detail=f"Market softening ({spy_change*100:.1f}%). Monitoring closely."
        elif rsi and rsi < 32:    signal="buy_dip"; detail=f"RSI {rsi:.0f} — market oversold. Favourable entry point."
        elif rsi and rsi > 70:    signal="caution"; detail=f"RSI {rsi:.0f} — overbought territory. Holding steady."
    return {"signal": signal, "spy_change": spy_change, "rsi": rsi, "detail": detail}


# ── Deploy — variable amount ───────────────────────────────────────────────────
def run_deploy(amount: float | None = None, mode: str = "auto") -> dict:
    cfg    = load_config()
    budget = float(amount or cfg.get("monthly_budget", 100.0))
    if budget < 1: return {"status": "error", "reason": "Amount must be at least $1.00"}

    invest_amount = round(budget * REINVEST_PCT, 2)
    save_amount   = round(budget * SAVINGS_PCT, 2)
    allocations   = RISK_PROFILES[cfg.get("risk_profile","moderate")]["allocations"]
    conn = get_db(); today = utc_now(); orders = []

    sentiment = market_sentiment()
    if sentiment["signal"] == "crash":
        conn.execute("INSERT INTO alerts (date,type,symbol,detail) VALUES (?,?,?,?)",
                     (today,"market_crash","SPY",sentiment["detail"]))
        conn.commit(); conn.close()
        return {"status":"halted","reason":sentiment["detail"],"invested":0,"saved":0}

    for symbol, weight, desc in allocations:
        notional = round(invest_amount * weight, 2)
        if notional < 1.0: continue
        price = get_quote(symbol); order = place_market_buy(symbol, notional)
        shares_approx = round(notional/price, 6) if price else None
        conn.execute("INSERT INTO trades (date,symbol,action,shares,price,amount,note,mode) VALUES (?,?,?,?,?,?,?,?)",
                     (today,symbol,"buy",shares_approx,price,notional,f"deploy ${budget:.0f} – {desc}",mode))
        orders.append({"symbol":symbol,"amount":notional,"weight":f"{weight*100:.0f}%",
                       "price":price,"desc":desc,
                       "order_id":order.get("id") if isinstance(order,dict) else None,
                       "status":"filled" if isinstance(order,dict) and order.get("id") else "error"})
        log({"event":"buy","symbol":symbol,"amount":notional})

    prev = conn.execute("SELECT total FROM savings ORDER BY id DESC LIMIT 1").fetchone()
    new_total = (prev["total"] if prev else 0.0) + save_amount
    conn.execute("INSERT INTO savings (date,amount,source,total) VALUES (?,?,?,?)",
                 (today, save_amount, f"deploy-${budget:.0f}", new_total))
    conn.execute("INSERT INTO portfolio_snapshots (date,market_value,invested,savings,risk_profile) VALUES (?,?,?,?,?)",
                 (today, 0, invest_amount, new_total, cfg.get("risk_profile")))
    conn.commit(); conn.close()
    return {"status":"ok","budget":budget,"invested":invest_amount,"saved":save_amount,"orders":orders,"sentiment":sentiment}


# ── Savings withdrawal ─────────────────────────────────────────────────────────
def withdraw_savings(amount: float, reason: str = "") -> dict:
    """
    Record a withdrawal from the savings vault.
    This updates the ledger — actual cash transfer is done manually in Alpaca or your bank.
    """
    conn = get_db()
    prev = conn.execute("SELECT total FROM savings ORDER BY id DESC LIMIT 1").fetchone()
    current = prev["total"] if prev else 0.0
    if amount <= 0: conn.close(); return {"status":"error","reason":"Amount must be greater than $0"}
    if amount > current: conn.close(); return {"status":"error","reason":f"Insufficient savings. Available: ${current:.2f}"}

    new_total   = round(current - amount, 2)
    today       = utc_now()
    reason_text = reason.strip() or "No reason provided"
    conn.execute("INSERT INTO savings (date,amount,source,total) VALUES (?,?,?,?)",
                 (today, -amount, f"withdrawal: {reason_text}", new_total))
    conn.execute("INSERT INTO withdrawals (date,amount,reason,total_after) VALUES (?,?,?,?)",
                 (today, amount, reason_text, new_total))
    conn.commit(); conn.close()
    log({"event":"withdrawal","amount":amount,"reason":reason_text,"balance_after":new_total})
    return {"status":"ok","withdrawn":amount,"reason":reason_text,
            "balance_before":current,"balance_after":new_total,
            "note":"Ledger updated. Transfer funds manually via Alpaca or your bank."}

def get_withdrawal_history() -> list[dict]:
    conn = get_db()
    rows = conn.execute("SELECT * FROM withdrawals ORDER BY id DESC LIMIT 20").fetchall()
    conn.close(); return [dict(r) for r in rows]


# ── Manual trades ──────────────────────────────────────────────────────────────
def manual_buy(symbol: str, amount: float) -> dict:
    conn=get_db(); today=utc_now(); price=get_quote(symbol); order=place_market_buy(symbol,amount)
    conn.execute("INSERT INTO trades (date,symbol,action,shares,price,amount,note,mode) VALUES (?,?,?,?,?,?,?,?)",
                 (today,symbol,"buy",round(amount/price,6) if price else None,price,amount,"manual buy","manual"))
    conn.commit(); conn.close()
    return {"symbol":symbol,"amount":amount,"price":price,
            "order_id":order.get("id") if isinstance(order,dict) else None,
            "status":"filled" if isinstance(order,dict) and order.get("id") else "error"}

def manual_sell(symbol: str, qty: float) -> dict:
    conn=get_db(); today=utc_now(); price=get_quote(symbol); order=place_market_sell(symbol,qty)
    amount=round(qty*price,2) if price else 0
    conn.execute("INSERT INTO trades (date,symbol,action,shares,price,amount,note,mode) VALUES (?,?,?,?,?,?,?,?)",
                 (today,symbol,"sell",qty,price,amount,"manual sell","manual"))
    conn.commit(); conn.close()
    return {"symbol":symbol,"qty":qty,"price":price,"amount":amount,
            "order_id":order.get("id") if isinstance(order,dict) else None,
            "status":"filled" if isinstance(order,dict) and order.get("id") else "error"}

def handle_dividend(symbol: str, amount: float) -> dict:
    reinvest=round(amount*REINVEST_PCT,2); save=round(amount*SAVINGS_PCT,2)
    conn=get_db(); today=utc_now(); price=get_quote(symbol)
    place_market_buy(symbol,reinvest)
    conn.execute("INSERT INTO trades (date,symbol,action,shares,price,amount,note,mode) VALUES (?,?,?,?,?,?,?,?)",
                 (today,symbol,"dividend",round(reinvest/price,6) if price else None,price,reinvest,f"dividend reinvest (total ${amount:.2f})","auto"))
    prev=conn.execute("SELECT total FROM savings ORDER BY id DESC LIMIT 1").fetchone()
    new_total=(prev["total"] if prev else 0.0)+save
    conn.execute("INSERT INTO savings (date,amount,source,total) VALUES (?,?,?,?)",(today,save,"dividend",new_total))
    conn.commit(); conn.close()
    return {"symbol":symbol,"total":amount,"reinvested":reinvest,"saved":save}

def check_alerts() -> list[dict]:
    new_alerts=[]; conn=get_db(); today=utc_now()
    for pos in get_positions():
        sym=pos.get("symbol",""); u=float(pos.get("unrealized_plpc",0))
        if u<=-0.10:
            detail=f"{sym} down {abs(u)*100:.1f}% from cost basis"
            conn.execute("INSERT INTO alerts (date,type,symbol,detail) VALUES (?,?,?,?)",(today,"position_drop",sym,detail))
            new_alerts.append({"type":"position_drop","symbol":sym,"detail":detail})
    s=market_sentiment()
    if s["signal"] in ("crash","caution"):
        conn.execute("INSERT INTO alerts (date,type,symbol,detail) VALUES (?,?,?,?)",(today,s["signal"],"SPY",s["detail"]))
        new_alerts.append({"type":s["signal"],"symbol":"SPY","detail":s["detail"]})
    conn.commit(); conn.close(); return new_alerts

def dismiss_alerts() -> None:
    conn=get_db(); conn.execute("UPDATE alerts SET seen=1"); conn.commit(); conn.close()


# ── Email — MC Gardener SMTP pattern ──────────────────────────────────────────
def _smtp_send(subject: str, html_body: str) -> tuple[bool, str]:
    """
    Gmail SMTP on port 587 with STARTTLS.
    Env vars (same names as MC Gardener Pro):
      ALERT_EMAIL_FROM      — your Gmail address
      ALERT_EMAIL_PASSWORD  — Gmail App Password (16 chars, not your login)
      ALERT_EMAIL_TO        — recipient (defaults to FROM)
    """
    from_addr = os.getenv("ALERT_EMAIL_FROM","")
    password  = os.getenv("ALERT_EMAIL_PASSWORD","")
    to_addr   = os.getenv("ALERT_EMAIL_TO", from_addr)
    if not from_addr or not password:
        return False, "Email not configured. Add ALERT_EMAIL_FROM and ALERT_EMAIL_PASSWORD to .env"
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject; msg["From"] = from_addr; msg["To"] = to_addr
    msg.attach(MIMEText(html_body, "html"))
    try:
        ctx = ssl.create_default_context()
        with smtplib.SMTP("smtp.gmail.com", 587) as srv:
            srv.ehlo(); srv.starttls(context=ctx); srv.login(from_addr, password)
            srv.sendmail(from_addr, to_addr, msg.as_string())
        return True, "sent"
    except Exception as e:
        return False, str(e)


def _rationale(symbol: str, pl_pct: float, sentiment: dict) -> str:
    descs = {
        "SCHD": "Schwab U.S. Dividend Equity ETF — dividend growth strategy with consistent annual increases.",
        "VYM":  "Vanguard High Dividend Yield ETF — broad high-yield dividend exposure across 400+ companies.",
        "QQQ":  "Invesco Nasdaq-100 ETF — tracks the 100 largest non-financial Nasdaq companies.",
        "TLT":  "iShares 20+ Year Treasury Bond ETF — long-duration government bonds, historically inverse to equities.",
        "VGT":  "Vanguard Information Technology ETF — concentrated exposure to the technology sector.",
        "XLP":  "Consumer Staples Select Sector SPDR — defensive holdings in food, beverage, and household goods.",
        "IAUM": "iShares Gold Trust Micro ETF — low-cost gold exposure as inflation and geopolitical hedge.",
        "XLU":  "Utilities Select Sector SPDR — regulated utility companies with steady income characteristics.",
        "ARKK": "ARK Innovation ETF — high-growth disruptive technology, high beta and volatility.",
    }
    base = descs.get(symbol, f"{symbol} — held per risk profile allocation.")
    perf = (f"Up {pl_pct:.1f}% from cost basis — performing well." if pl_pct >= 5 else
            f"Up {pl_pct:.1f}% — tracking positively." if pl_pct >= 0 else
            f"Down {abs(pl_pct):.1f}% — within normal variance, no action required." if pl_pct >= -5 else
            f"Down {abs(pl_pct):.1f}% — monitoring; long-term thesis intact.")
    rsi = sentiment.get("rsi"); sig = sentiment.get("signal","hold")
    rec = ("RSI below 35 — oversold market. Favourable accumulation opportunity." if sig=="buy_dip" and rsi and rsi<35 else
           "Market in correction. Auto-deploy paused. Holding per long-term strategy." if sig=="crash" else
           "Market showing weakness. Maintaining allocation, no rebalancing triggered." if sig=="caution" else
           "Market conditions normal. Auto-pilot maintaining target allocation.")
    return f"{base} {perf} {rec}"


def build_daily_report_html(positions: list, dashboard: dict) -> str:
    today_str   = local_date()
    s           = dashboard.get("sentiment",{})
    cfg         = load_config()
    pk          = cfg.get("risk_profile","moderate")
    pi          = RISK_PROFILES.get(pk, RISK_PROFILES["moderate"])
    mode_label  = "Paper Trading" if cfg.get("mode")=="paper" else "Live Trading"
    total_inv   = dashboard.get("total_invested",0)
    savings     = dashboard.get("savings_total",0)
    grand       = dashboard.get("grand_total",0)
    gl          = dashboard.get("gain_loss",0)
    gl_pct      = dashboard.get("gain_loss_pct",0)
    sig         = s.get("signal","hold")
    rsi_str     = f"{s['rsi']:.1f}" if s.get("rsi") else "N/A"
    spy_str     = f"{s['spy_change']*100:+.2f}%" if s.get("spy_change") is not None else "N/A"
    gl_clr      = "#00c853" if gl>=0 else "#ff5252"
    gl_arrow    = "▲" if gl>=0 else "▼"

    sig_map = {"hold":("#1a3a5c","#40c4ff","📊 Market Normal"),
               "buy_dip":("#0a2a15","#00e676","📈 Buy the Dip"),
               "caution":("#2a2000","#ffab40","⚠ Use Caution"),
               "crash":("#2a0a0a","#ff5252","🚨 Crash Alert")}
    sig_bg, sig_clr, sig_lbl = sig_map.get(sig, sig_map["hold"])

    pos_rows = ""
    for pos in positions:
        sym=pos.get("symbol",""); pl=pos.get("unrealized_pl",0)
        plpct=pos.get("unrealized_plpc",0)*100; mv=pos.get("market_value",0)
        price=pos.get("current_price",0); shares=pos.get("shares",0)
        clr="#00c853" if pl>=0 else "#ff5252"; arrow="▲" if pl>=0 else "▼"
        rat=_rationale(sym,plpct,s)
        pos_rows += f"""
        <tr>
          <td style="padding:14px 16px;border-bottom:1px solid #1e2d3d"><strong style="font-size:15px;color:#e2eaf5">{sym}</strong></td>
          <td style="padding:14px 16px;border-bottom:1px solid #1e2d3d;color:#a0b4c8">{shares:.6f}</td>
          <td style="padding:14px 16px;border-bottom:1px solid #1e2d3d;color:#a0b4c8">${price:,.2f}</td>
          <td style="padding:14px 16px;border-bottom:1px solid #1e2d3d;color:#a0b4c8">${mv:,.2f}</td>
          <td style="padding:14px 16px;border-bottom:1px solid #1e2d3d;color:{clr};font-weight:600">{arrow} ${abs(pl):,.2f} ({plpct:+.2f}%)</td>
        </tr>
        <tr><td colspan="5" style="padding:4px 16px 14px;border-bottom:1px solid #1e2d3d;font-size:11px;color:#6a8aaa;font-style:italic;line-height:1.5">{rat}</td></tr>"""

    if not pos_rows:
        pos_rows = '<tr><td colspan="5" style="padding:24px;text-align:center;color:#6a8aaa">No open positions yet.</td></tr>'

    withdrawals = get_withdrawal_history()
    w_rows = "".join(f"""<tr>
      <td style="padding:8px 12px;border-bottom:1px solid #1e2d3d;color:#a0b4c8;font-size:12px">{w['date'][:10]}</td>
      <td style="padding:8px 12px;border-bottom:1px solid #1e2d3d;color:#ff8a65;font-size:12px">${w['amount']:,.2f}</td>
      <td style="padding:8px 12px;border-bottom:1px solid #1e2d3d;color:#6a8aaa;font-size:12px">{w['reason']}</td>
    </tr>""" for w in withdrawals[:5]) or '<tr><td colspan="3" style="padding:12px;text-align:center;color:#6a8aaa;font-size:12px">No withdrawals on record.</td></tr>'

    dip_tip = f"<li><strong style='color:#00e676'>Dip opportunity detected:</strong> RSI {s.get('rsi',0):.0f} — market oversold. Consider an additional contribution this week.</li>" if sig=="buy_dip" else ""
    crash_tip = "<li><strong style='color:#ff5252'>Crash protocol active:</strong> Auto-deploy is paused. Review positions before next scheduled contribution.</li>" if sig=="crash" else ""
    caution_tip = "<li><strong style='color:#ffab40'>Market caution signal:</strong> Monitoring conditions closely. No rebalancing required at this time.</li>" if sig=="caution" else ""

    return f"""<!DOCTYPE html><html><head><meta charset="UTF-8"/></head>
<body style="margin:0;padding:0;background:#080b0f;font-family:'Segoe UI',Arial,sans-serif">
<div style="max-width:680px;margin:0 auto;padding:32px 16px">

  <div style="background:linear-gradient(135deg,#0d1a2e,#0a1a0f);border:1px solid #1c2635;border-radius:12px;padding:28px 32px;margin-bottom:20px">
    <div style="font-size:22px;font-weight:800;color:#e2eaf5;letter-spacing:-.5px">💹 WealthPilot</div>
    <div style="font-size:11px;color:#5a7a9a;text-transform:uppercase;letter-spacing:.1em;margin-top:2px">Daily Portfolio Report</div>
    <div style="font-size:13px;color:#5a7a9a;margin-top:10px">{today_str} &nbsp;·&nbsp; {mode_label} &nbsp;·&nbsp; Risk: <strong style="color:#a0b4c8">{pi['label']}</strong></div>
  </div>

  <div style="background:{sig_bg};border:1px solid {sig_clr}44;border-radius:10px;padding:16px 20px;margin-bottom:20px">
    <div style="font-size:15px;font-weight:700;color:{sig_clr}">{sig_lbl}</div>
    <div style="font-size:12px;color:#a0b4c8;margin-top:3px">{s.get('detail','')}</div>
    <div style="margin-top:8px;font-size:12px;color:#6a8aaa">SPY: <strong style="color:{sig_clr}">{spy_str}</strong> &nbsp;·&nbsp; RSI: <strong style="color:#a0b4c8">{rsi_str}</strong></div>
  </div>

  <div style="display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-bottom:20px">
    {"".join(f'''<div style="background:#0d1117;border:1px solid #1c2635;border-radius:10px;padding:18px 20px">
      <div style="font-size:10px;color:#5a7a9a;text-transform:uppercase;letter-spacing:.1em">{lbl}</div>
      <div style="font-size:26px;font-weight:800;color:{clr};margin-top:4px">{val}</div>
      <div style="font-size:11px;color:#5a7a9a;margin-top:3px">{sub}</div>
    </div>''' for lbl,clr,val,sub in [
        ("Grand Total","#00e676",f"${grand:,.2f}","Portfolio + savings"),
        ("Unrealized Gain/Loss",gl_clr,f"{gl_arrow} ${abs(gl):,.2f}",f"{gl_pct:+.2f}% on invested capital"),
        ("Total Invested","#e2eaf5",f"${total_inv:,.2f}","Capital deployed to date"),
        ("Savings Vault","#40c4ff",f"${savings:,.2f}","Protected 30% — never reinvested"),
    ])}
  </div>

  <div style="background:#0d1117;border:1px solid #1c2635;border-radius:10px;overflow:hidden;margin-bottom:20px">
    <div style="padding:16px 20px;border-bottom:1px solid #1c2635">
      <div style="font-size:14px;font-weight:700;color:#e2eaf5">Portfolio Positions</div>
      <div style="font-size:11px;color:#5a7a9a;margin-top:2px">With rationale and market context for each holding</div>
    </div>
    <table style="width:100%;border-collapse:collapse">
      <thead><tr style="background:rgba(0,0,0,.3)">
        {"".join(f'<th style="text-align:left;padding:10px 16px;font-size:10px;color:#3a5a7a;text-transform:uppercase;letter-spacing:.1em">{h}</th>' for h in ["Symbol","Shares","Price","Value","Gain / Loss"])}
      </tr></thead>
      <tbody>{pos_rows}</tbody>
    </table>
  </div>

  <div style="background:#0a1a10;border:1px solid #1a3a20;border-radius:10px;padding:20px;margin-bottom:20px">
    <div style="font-size:14px;font-weight:700;color:#00e676;margin-bottom:12px">🧠 Auto-Pilot Recommendations</div>
    <ul style="margin:0;padding-left:20px;color:#a0b4c8;font-size:12px;line-height:2.2">
      <li>Continue monthly contributions on schedule — dollar-cost averaging smooths entry price over time.</li>
      {dip_tip}{crash_tip}{caution_tip}
      <li>Savings vault at <strong style="color:#40c4ff">${savings:,.2f}</strong> — {min(savings/500*100,100):.0f}% toward $500 goal.</li>
      <li>All positions held per <strong style="color:#e2eaf5">{pi['label']}</strong> profile — no rebalancing triggered.</li>
    </ul>
  </div>

  <div style="background:#0d1117;border:1px solid #1c2635;border-radius:10px;overflow:hidden;margin-bottom:20px">
    <div style="padding:14px 20px;border-bottom:1px solid #1c2635">
      <div style="font-size:13px;font-weight:700;color:#e2eaf5">Recent Withdrawals</div>
    </div>
    <table style="width:100%;border-collapse:collapse">
      <thead><tr style="background:rgba(0,0,0,.3)">
        <th style="text-align:left;padding:8px 12px;font-size:10px;color:#3a5a7a;text-transform:uppercase;letter-spacing:.1em">Date</th>
        <th style="text-align:left;padding:8px 12px;font-size:10px;color:#3a5a7a;text-transform:uppercase;letter-spacing:.1em">Amount</th>
        <th style="text-align:left;padding:8px 12px;font-size:10px;color:#3a5a7a;text-transform:uppercase;letter-spacing:.1em">Reason</th>
      </tr></thead>
      <tbody>{w_rows}</tbody>
    </table>
  </div>

  <div style="text-align:center;padding:20px 0;border-top:1px solid #1c2635;font-size:11px;color:#3a5a7a;line-height:1.8">
    WealthPilot &nbsp;·&nbsp; Automated Investment Engine &nbsp;·&nbsp; Report generated {today_str}<br/>
    <span style="color:#2a3f55">Informational only. Not financial advice. All investing involves risk.</span>
  </div>
</div></body></html>"""


def send_daily_report() -> dict:
    dashboard = get_dashboard()
    html      = build_daily_report_html(dashboard.get("holdings",[]), dashboard)
    subject   = f"WealthPilot Daily Report — {local_date()}"
    ok, msg   = _smtp_send(subject, html)
    conn      = get_db()
    conn.execute("INSERT INTO email_log (date,subject,status,error) VALUES (?,?,?,?)",
                 (utc_now(), subject, "sent" if ok else "failed", "" if ok else msg))
    conn.commit(); conn.close()
    log({"event":"email_report","status":"sent" if ok else "failed","error":msg})
    return {"sent": ok, "message": msg}

def get_email_log() -> list[dict]:
    conn = get_db()
    rows = conn.execute("SELECT * FROM email_log ORDER BY id DESC LIMIT 10").fetchall()
    conn.close(); return [dict(r) for r in rows]


# ── Dashboard ──────────────────────────────────────────────────────────────────
def get_dashboard() -> dict:
    conn=get_db(); cfg=load_config()
    positions=get_positions(); account=get_account()
    total_market_value=0.0; holdings=[]
    for pos in positions:
        mv=float(pos.get("market_value",0)); total_market_value+=mv
        holdings.append({"symbol":pos["symbol"],"shares":float(pos.get("qty",0)),
            "avg_cost":float(pos.get("avg_entry_price",0)),"current_price":float(pos.get("current_price",0)),
            "market_value":mv,"cost_basis":float(pos.get("cost_basis",0)),
            "unrealized_pl":float(pos.get("unrealized_pl",0)),"unrealized_plpc":float(pos.get("unrealized_plpc",0))})

    total_invested = conn.execute("SELECT COALESCE(SUM(amount),0) FROM trades WHERE action='buy'").fetchone()[0]
    sav_row        = conn.execute("SELECT total FROM savings ORDER BY id DESC LIMIT 1").fetchone()
    savings_total  = sav_row["total"] if sav_row else 0.0
    gain_loss      = total_market_value - total_invested

    activity     = conn.execute("SELECT * FROM trades ORDER BY id DESC LIMIT 20").fetchall()
    alerts       = conn.execute("SELECT * FROM alerts WHERE seen=0 ORDER BY date DESC").fetchall()
    sav_hist     = conn.execute("SELECT date,amount,source,total FROM savings ORDER BY id DESC LIMIT 12").fetchall()
    recent_orders= get_orders(status="all", limit=10)
    email_log    = get_email_log()
    withdrawals  = get_withdrawal_history()
    sentiment    = market_sentiment()

    pk=cfg.get("risk_profile","moderate"); pi=RISK_PROFILES.get(pk,RISK_PROFILES["moderate"])
    buying_power=float(account.get("buying_power",0)) if account else 0.0
    conn.close()

    return {
        "holdings": holdings, "total_invested": round(total_invested,2),
        "total_market_value": round(total_market_value,2), "savings_total": round(savings_total,2),
        "grand_total": round(total_market_value+savings_total,2), "gain_loss": round(gain_loss,2),
        "gain_loss_pct": round((gain_loss/total_invested*100) if total_invested else 0, 2),
        "buying_power": round(buying_power,2),
        "savings_history": [dict(r) for r in sav_hist],
        "recent_activity": [dict(r) for r in activity],
        "recent_orders": recent_orders if isinstance(recent_orders,list) else [],
        "unseen_alerts": [dict(a) for a in alerts],
        "withdrawals": withdrawals, "sentiment": sentiment,
        "risk_profile": pk,
        "profile_info": {"label":pi["label"],"description":pi["description"],"color":pi["color"],
            "allocations":[{"symbol":s,"weight":w,"desc":d} for s,w,d in pi["allocations"]]},
        "mode": cfg.get("mode","paper"), "auto_mode": cfg.get("auto_mode",True),
        "monthly_budget": cfg.get("monthly_budget",100.0),
        "alpaca_connected": bool(os.getenv("ALPACA_PAPER_KEY")),
        "email_configured": bool(os.getenv("ALERT_EMAIL_FROM") and os.getenv("ALERT_EMAIL_PASSWORD")),
        "email_log": email_log, "server_time": utc_now(), "goal_savings": GOAL_SAVINGS,
    }
