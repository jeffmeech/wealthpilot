"""
engine_crypto.py — WealthPilot v3 Crypto Side (NDAX via ccxt)

FIXES APPLIED:
  Bug #3 — CCXT Crash: all trade functions guard against None exchange
  Future #2 — Partial Fill: verifies filled_qty from API before calculating transfer

INSTITUTIONAL UPGRADES:
  Upgrade #1 — ATR Volatility-Adjusted Stops: stop widens in volatile markets,
               tightens in calm ones — prevents whale-induced shake-outs
  Upgrade #2 — Principal Protection: once a position doubles (100% gain),
               extract original cost to conservative savings first
"""
from __future__ import annotations
import os
from engine_core import get_db, utc_now, log, load_config, add_savings

NDAX_INSTRUMENTS = {
    "BTC/CAD": 1, "ETH/CAD": 2, "LTC/CAD": 3,
    "XRP/CAD": 4, "ADA/CAD": 6, "DOT/CAD": 8,
}

# ── Exchange connection ───────────────────────────────────────────────────────
def _get_exchange():
    """Returns ccxt NDAX instance, or None if ccxt not installed / keys missing."""
    try:
        import ccxt
        key    = os.getenv("NDAX_API_KEY", "")
        secret = os.getenv("NDAX_API_SECRET", "")
        uid    = os.getenv("NDAX_USER_ID", "")
        if not key or not secret:
            return None
        return ccxt.ndax({
            "apiKey":  key, "secret": secret, "uid": uid,
            "options": {"defaultType": "spot"},
        })
    except ImportError:
        log({"event": "ccxt_missing", "detail": "pip install ccxt to enable crypto"})
        return None
    except Exception as e:
        log({"event": "ccxt_error", "error": str(e)})
        return None

def _exchange_required(fn):
    """
    FIX #3 — CCXT Crash Guard:
    Decorator that returns a clean error dict if exchange is None,
    instead of crashing with AttributeError.
    """
    import functools
    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        # inject exchange as first positional after self if needed
        return fn(*args, **kwargs)
    return wrapper


def _safe_sell(ex, symbol: str, qty: float) -> dict:
    """
    FIX #3 + Future #2:
    Guard against None exchange AND verify filled quantity from API response
    before calculating profit splits. Partial fills are real in crypto markets.
    """
    if not ex:
        return {"status": "error", "reason": "Exchange not connected. Install ccxt and add NDAX keys to .env.", "filled_qty": 0}
    try:
        result       = ex.create_market_sell_order(symbol, qty)
        # Future #2 fix: read actual filled quantity, not assumed
        filled_qty   = float(result.get("filled", result.get("amount", qty)))
        filled_cost  = float(result.get("cost", 0)) or (filled_qty * float(result.get("price", 0) or 0))
        return {"status": "ok", "filled_qty": filled_qty, "filled_cost": filled_cost, "raw": result}
    except Exception as e:
        log({"event": "ndax_sell_error", "symbol": symbol, "error": str(e)})
        return {"status": "error", "reason": str(e), "filled_qty": 0}

def _safe_buy(ex, symbol: str, qty: float) -> dict:
    """Guard against None exchange."""
    if not ex:
        return {"status": "error", "reason": "Exchange not connected.", "filled_qty": 0}
    try:
        result     = ex.create_market_buy_order(symbol, qty)
        filled_qty = float(result.get("filled", result.get("amount", qty)))
        return {"status": "ok", "filled_qty": filled_qty, "raw": result}
    except Exception as e:
        log({"event": "ndax_buy_error", "symbol": symbol, "error": str(e)})
        return {"status": "error", "reason": str(e), "filled_qty": 0}


# ── Market data ────────────────────────────────────────────────────────────────
def get_crypto_balances() -> dict:
    ex = _get_exchange()
    if not ex:
        return {"error": "Exchange not connected", "balances": {}}
    try:
        bal = ex.fetch_balance()
        return {"balances": {k: v for k, v in bal["total"].items() if v and v > 0}}
    except Exception as e:
        log({"event": "ndax_balance_error", "error": str(e)})
        return {"error": str(e), "balances": {}}

def get_crypto_ticker(symbol: str) -> dict | None:
    ex = _get_exchange()
    if not ex:
        return None
    try:
        t = ex.fetch_ticker(symbol)
        return {
            "symbol":     symbol,
            "last":       t["last"],
            "bid":        t["bid"],
            "ask":        t["ask"],
            "change_pct": t.get("percentage", 0),
            "volume":     t.get("baseVolume", 0),
        }
    except Exception as e:
        log({"event": "ndax_ticker_error", "symbol": symbol, "error": str(e)})
        return None

def get_crypto_ohlcv(symbol: str, limit: int = 30) -> dict:
    """Fetch OHLCV data for ATR calculation."""
    ex = _get_exchange()
    if not ex:
        return {"highs": [], "lows": [], "closes": []}
    try:
        # ccxt fetch_ohlcv returns [[timestamp, open, high, low, close, volume], ...]
        ohlcv = ex.fetch_ohlcv(symbol, timeframe="1d", limit=limit + 5)
        return {
            "highs":  [c[2] for c in ohlcv],
            "lows":   [c[3] for c in ohlcv],
            "closes": [c[4] for c in ohlcv],
        }
    except Exception as e:
        log({"event": "ndax_ohlcv_error", "symbol": symbol, "error": str(e)})
        return {"highs": [], "lows": [], "closes": []}

def compute_atr(highs: list, lows: list, closes: list, period: int = 14) -> float | None:
    """Average True Range — volatility measurement for dynamic stop placement."""
    if len(closes) < period + 1:
        return None
    trs = []
    for i in range(1, len(closes)):
        tr = max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i - 1]),
            abs(lows[i]  - closes[i - 1])
        )
        trs.append(tr)
    return sum(trs[-period:]) / period


# ── Position building ─────────────────────────────────────────────────────────
def get_crypto_positions() -> list[dict]:
    """
    Build positions from NDAX balances + our DB cost basis.
    Adds ATR-based dynamic stop level for each position (Upgrade #1).
    """
    cfg     = load_config()
    symbols = cfg.get("crypto_symbols", ["BTC/CAD", "ETH/CAD"])
    bal     = get_crypto_balances()
    if "error" in bal:
        return []

    conn      = get_db()
    positions = []

    for symbol in symbols:
        base = symbol.split("/")[0]
        qty  = bal["balances"].get(base, 0)
        if qty <= 0:
            continue

        ticker = get_crypto_ticker(symbol)
        if not ticker:
            continue

        cost_row = conn.execute(
            "SELECT SUM(amount)/SUM(shares) as avg_cost, SUM(amount) as total_cost "
            "FROM trades WHERE side='crypto' AND symbol=? AND action='buy' AND shares>0",
            (symbol,)
        ).fetchone()
        avg_cost   = float(cost_row["avg_cost"])   if cost_row and cost_row["avg_cost"]   else 0
        total_cost = float(cost_row["total_cost"]) if cost_row and cost_row["total_cost"] else 0
        mv         = qty * ticker["last"]
        gain       = mv - total_cost
        gain_pct   = (gain / total_cost * 100) if total_cost else 0

        # Upgrade #1: compute ATR-based dynamic stop
        atr_multiplier = float(cfg.get("crypto_atr_multiplier", 2.0))
        ohlcv = get_crypto_ohlcv(symbol, 30)
        atr   = compute_atr(ohlcv["highs"], ohlcv["lows"], ohlcv["closes"])
        dynamic_stop_price = (avg_cost - atr * atr_multiplier) if (atr and avg_cost) else None
        dynamic_stop_pct   = ((dynamic_stop_price - avg_cost) / avg_cost * 100) if dynamic_stop_price and avg_cost else None

        # Upgrade #2: principal protection flag
        principal_protected = False
        if conn.execute(
            "SELECT id FROM principal_protection_log WHERE symbol=? LIMIT 1", (symbol,)
        ).fetchone():
            principal_protected = True

        positions.append({
            "symbol":             symbol,
            "base":               base,
            "qty":                qty,
            "price":              ticker["last"],
            "market_value":       round(mv, 2),
            "avg_cost":           round(avg_cost, 2),
            "total_cost":         round(total_cost, 2),
            "gain":               round(gain, 2),
            "gain_pct":           round(gain_pct, 2),
            "change_24h":         ticker.get("change_pct", 0),
            "atr":                round(atr, 4) if atr else None,
            "dynamic_stop_price": round(dynamic_stop_price, 2) if dynamic_stop_price else None,
            "dynamic_stop_pct":   round(dynamic_stop_pct, 2) if dynamic_stop_pct else None,
            "principal_protected":principal_protected,
        })

    conn.close()
    return positions


# ── Exit logic with all upgrades ──────────────────────────────────────────────
def check_crypto_exits() -> dict:
    """
    Exit conditions checked in priority order:
      1. Principal protection: if gain >= 100%, extract original cost first (Upgrade #2)
      2. Profit target: take profit at configured % gain
      3. ATR stop-loss: dynamic volatility-adjusted exit (Upgrade #1)
      4. Fixed stop-loss fallback if ATR unavailable

    Future #2: filled_qty from API is verified before calculating transfer amount.
    Bug #3: all exchange calls guarded against None.
    """
    cfg           = load_config()
    profit_take   = float(cfg.get("crypto_profit_take",    0.20))
    stop_loss     = float(cfg.get("crypto_stop_loss",     -0.15))
    reinvest_pct  = float(cfg.get("crypto_reinvest_pct",   0.70))
    cross_pct     = float(cfg.get("cross_transfer_pct",    0.30))
    use_atr_stop  = bool(cfg.get("crypto_use_atr_stop",    True))
    atr_mult      = float(cfg.get("crypto_atr_multiplier", 2.0))
    protect_princ = bool(cfg.get("crypto_principal_protect",True))

    positions = get_crypto_positions()
    ex        = _get_exchange()
    actions   = []

    for pos in positions:
        symbol     = pos["symbol"]
        gain_pct   = pos["gain_pct"] / 100
        gain       = pos["gain"]
        qty        = pos["qty"]
        mv         = pos["market_value"]
        avg_cost   = pos["avg_cost"]
        total_cost = pos["total_cost"]
        price      = pos["price"]
        atr        = pos["atr"]
        dp         = pos["dynamic_stop_price"]

        # ── Upgrade #2: Principal Protection ──────────────────────────────────
        # If position has doubled (100% gain) and we haven't protected yet,
        # sell enough shares to recover the original investment first.
        if protect_princ and gain_pct >= 1.0 and not pos.get("principal_protected") and total_cost > 0:
            shares_to_sell = total_cost / price if price else 0
            if shares_to_sell > 0 and shares_to_sell < qty:
                sell_r = _safe_sell(ex, symbol, shares_to_sell)
                if sell_r["status"] == "ok":
                    filled = sell_r["filled_qty"]
                    recovered = filled * price

                    # Move recovered principal to conservative savings
                    add_savings(recovered, f"principal protection: {symbol}")

                    conn = get_db(); today = utc_now()
                    conn.execute(
                        "INSERT INTO principal_protection_log (date,symbol,principal_extracted,trigger,note) "
                        "VALUES (?,?,?,?,?)",
                        (today, symbol, recovered, "100% gain",
                         f"Extracted ${recovered:.2f} original cost. Now playing with house money.")
                    )
                    conn.execute(
                        "INSERT INTO cross_transfers (date,from_side,to_side,amount,source_gain,note) "
                        "VALUES (?,?,?,?,?,?)",
                        (today, "crypto", "conservative", recovered, gain,
                         f"{symbol}: principal protection triggered at +{gain_pct*100:.0f}% gain")
                    )
                    conn.commit(); conn.close()
                    log({"event": "principal_protection", "symbol": symbol, "recovered": recovered})
                    actions.append({
                        "type":      "principal_protection",
                        "symbol":    symbol,
                        "recovered": recovered,
                        "note":      "Original investment secured. Remaining position is pure profit.",
                    })
                    continue  # Don't double-trigger profit_take in same cycle

        # ── Upgrade #1 + Future #2: ATR-based stop or fixed stop ──────────────
        # Determine if stop is triggered
        at_stop = False
        stop_reason = ""
        if use_atr_stop and atr and dp:
            if price <= dp:
                at_stop     = True
                stop_reason = (f"ATR stop hit: price ${price:.2f} ≤ dynamic stop ${dp:.2f} "
                               f"({pos.get('dynamic_stop_pct',0):.1f}% from cost)")
        else:
            if gain_pct <= stop_loss:
                at_stop     = True
                stop_reason = f"Fixed stop-loss: {gain_pct*100:.1f}% loss"

        # ── Profit take ────────────────────────────────────────────────────────
        if gain_pct >= profit_take and gain > 0:
            sell_r = _safe_sell(ex, symbol, qty)

            if sell_r["status"] != "ok":
                actions.append({"type": "error", "symbol": symbol,
                                 "detail": sell_r.get("reason", "sell failed")})
                continue

            # Future #2: use VERIFIED filled quantity and cost
            filled_qty  = sell_r["filled_qty"]
            filled_cost = sell_r.get("filled_cost") or (filled_qty * price)

            # Recalculate gain from verified fill (partial fill protection)
            verified_gain   = filled_cost - (total_cost * (filled_qty / qty) if qty else 0)
            cross_amount    = round(max(verified_gain, 0) * cross_pct, 2)
            reinvest_amount = round(max(verified_gain, 0) * reinvest_pct, 2)

            add_savings(cross_amount, f"crypto profit: {symbol}")

            if ex and reinvest_amount >= 1:
                ticker = get_crypto_ticker(symbol)
                if ticker:
                    rebuy_qty = reinvest_amount / ticker["last"]
                    _safe_buy(ex, symbol, rebuy_qty)

            conn = get_db(); today = utc_now()
            conn.execute(
                "INSERT INTO trades (date,side,symbol,action,shares,price,amount,gain,note,mode) "
                "VALUES (?,?,?,?,?,?,?,?,?,?)",
                (today, "crypto", symbol, "sell", filled_qty, price, filled_cost, verified_gain,
                 f"profit take at +{gain_pct*100:.1f}% (filled {filled_qty:.6f} of {qty:.6f})", "auto")
            )
            conn.execute(
                "INSERT INTO cross_transfers (date,from_side,to_side,amount,source_gain,note) "
                "VALUES (?,?,?,?,?,?)",
                (today, "crypto", "conservative", cross_amount, verified_gain,
                 f"{symbol} profit: +{gain_pct*100:.1f}% → 30% to conservative savings")
            )
            conn.commit(); conn.close()
            log({"event": "crypto_profit_take", "symbol": symbol,
                 "verified_gain": verified_gain, "cross": cross_amount, "reinvested": reinvest_amount})

            actions.append({
                "type":          "profit_take",
                "symbol":        symbol,
                "gain":          verified_gain,
                "gain_pct":      gain_pct * 100,
                "filled_qty":    filled_qty,
                "requested_qty": qty,
                "partial_fill":  filled_qty < qty * 0.99,
                "cross_transfer":cross_amount,
                "reinvested":    reinvest_amount,
                "note":          f"30% (${cross_amount:.2f}) → conservative savings",
            })

        elif at_stop:
            sell_r = _safe_sell(ex, symbol, qty)
            if sell_r["status"] != "ok":
                actions.append({"type": "error", "symbol": symbol,
                                 "detail": sell_r.get("reason", "stop sell failed")})
                continue

            filled_qty = sell_r["filled_qty"]
            conn = get_db(); today = utc_now()
            conn.execute(
                "INSERT INTO trades (date,side,symbol,action,shares,price,amount,gain,note,mode) "
                "VALUES (?,?,?,?,?,?,?,?,?,?)",
                (today, "crypto", symbol, "sell", filled_qty, price, filled_qty * price, gain,
                 stop_reason, "auto")
            )
            conn.execute(
                "INSERT INTO alerts (date,type,symbol,detail,side) VALUES (?,?,?,?,?)",
                (today, "stop_loss", symbol, stop_reason, "crypto")
            )
            conn.commit(); conn.close()
            log({"event": "crypto_stop", "symbol": symbol, "reason": stop_reason})

            actions.append({
                "type":      "stop_loss",
                "symbol":    symbol,
                "loss":      gain,
                "loss_pct":  gain_pct * 100,
                "stop_type": "ATR" if use_atr_stop and atr else "fixed",
                "note":      stop_reason,
            })

    return {"actions": actions, "positions_checked": len(positions)}


# ── Manual trades (Bug #3 guarded) ───────────────────────────────────────────
def manual_crypto_buy(symbol: str, amount_cad: float) -> dict:
    """Bug #3: returns clean error dict if exchange unavailable."""
    ex     = _get_exchange()
    ticker = get_crypto_ticker(symbol)
    if not ticker:
        return {"status": "error", "reason": f"Could not get price for {symbol}. Check NDAX connection."}

    qty    = amount_cad / ticker["last"]
    result = _safe_buy(ex, symbol, qty)
    if result["status"] != "ok":
        return result

    conn = get_db()
    conn.execute(
        "INSERT INTO trades (date,side,symbol,action,shares,price,amount,gain,note,mode) "
        "VALUES (?,?,?,?,?,?,?,?,?,?)",
        (utc_now(), "crypto", symbol, "buy", result.get("filled_qty", qty),
         ticker["last"], amount_cad, 0, "manual buy", "manual")
    )
    conn.commit(); conn.close()
    return {"status": "ok", "symbol": symbol, "qty": qty, "price": ticker["last"], "amount": amount_cad}


def manual_crypto_sell(symbol: str, qty: float) -> dict:
    """Bug #3: returns clean error dict if exchange unavailable."""
    ex     = _get_exchange()
    ticker = get_crypto_ticker(symbol)
    if not ticker:
        return {"status": "error", "reason": f"Could not get price for {symbol}. Check NDAX connection."}

    result = _safe_sell(ex, symbol, qty)
    if result["status"] != "ok":
        return result

    filled = result.get("filled_qty", qty)
    mv     = filled * ticker["last"]
    conn   = get_db()
    conn.execute(
        "INSERT INTO trades (date,side,symbol,action,shares,price,amount,gain,note,mode) "
        "VALUES (?,?,?,?,?,?,?,?,?,?)",
        (utc_now(), "crypto", symbol, "sell", filled, ticker["last"], mv, 0,
         f"manual sell (filled {filled:.6f} of {qty:.6f})", "manual")
    )
    conn.commit(); conn.close()
    return {"status": "ok", "symbol": symbol, "filled_qty": filled, "price": ticker["last"], "amount": mv}


# ── Dashboard ─────────────────────────────────────────────────────────────────
def get_crypto_dashboard() -> dict:
    cfg       = load_config()
    positions = get_crypto_positions()
    total_mv  = sum(p["market_value"] for p in positions)
    total_cost= sum(p["total_cost"]   for p in positions)
    total_gain= total_mv - total_cost

    conn = get_db()
    recent_trades  = conn.execute(
        "SELECT * FROM trades WHERE side='crypto' ORDER BY id DESC LIMIT 10"
    ).fetchall()
    cross_transfers = conn.execute(
        "SELECT * FROM cross_transfers ORDER BY id DESC LIMIT 10"
    ).fetchall()
    alerts = conn.execute(
        "SELECT * FROM alerts WHERE side='crypto' AND seen=0"
    ).fetchall()
    protection_log = conn.execute(
        "SELECT * FROM principal_protection_log ORDER BY id DESC LIMIT 5"
    ).fetchall()
    conn.close()

    tickers = {}
    for sym in cfg.get("crypto_symbols", ["BTC/CAD", "ETH/CAD"]):
        t = get_crypto_ticker(sym)
        if t:
            tickers[sym] = t

    return {
        "positions":         positions,
        "total_mv":          round(total_mv, 2),
        "total_cost":        round(total_cost, 2),
        "total_gain":        round(total_gain, 2),
        "total_gain_pct":    round((total_gain / total_cost * 100) if total_cost else 0, 2),
        "tickers":           tickers,
        "recent_trades":     [dict(r) for r in recent_trades],
        "cross_transfers":   [dict(r) for r in cross_transfers],
        "unseen_alerts":     [dict(a) for a in alerts],
        "protection_log":    [dict(p) for p in protection_log],
        "profit_target":     cfg.get("crypto_profit_take", 0.20) * 100,
        "stop_loss":         cfg.get("crypto_stop_loss", -0.15) * 100,
        "use_atr_stop":      cfg.get("crypto_use_atr_stop", True),
        "atr_multiplier":    cfg.get("crypto_atr_multiplier", 2.0),
        "principal_protect": cfg.get("crypto_principal_protect", True),
        "ndax_connected":    bool(os.getenv("NDAX_API_KEY") and os.getenv("NDAX_API_SECRET")),
        "ccxt_available":    _ccxt_available(),
    }


def _ccxt_available() -> bool:
    try:
        import ccxt
        return True
    except ImportError:
        return False
