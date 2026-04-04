"""
engine_conservative.py — WealthPilot v3 Conservative Side

FIXES APPLIED:
  Bug #1 — Ghost Key: _h() now reads ALPACA_LIVE_KEY/SECRET in live mode
  Bug #2 — Leftover Cash: rotation now reinvests principal + 70% of gain, saves 30% of gain only
  Future #1 — Wash Sale: 31-day cooldown on symbols sold at a loss (Canada superficial loss rule)
  Future #3 — Rate Limiting: 500ms throttle between ETF scans
  Future #4 — DB Locking: handled by engine_core WAL + retry

INSTITUTIONAL UPGRADES:
  Upgrade #3 — Quality Factor Penalties: payout ratio penalizes high-yield traps
  Upgrade #4 — 200-Day MA Filter: block new buys when SPY is in a bear trend
"""
from __future__ import annotations
import json, os, time
from datetime import datetime, timezone, timedelta
from typing import Any
from engine_core import get_db, utc_now, log, load_config, add_savings

ALPACA_PAPER = "https://paper-api.alpaca.markets/v2"
ALPACA_LIVE  = "https://api.alpaca.markets/v2"
ALPACA_DATA  = "https://data.alpaca.markets/v2"

# ── Dividend universe ─────────────────────────────────────────────────────────
# (symbol, base_yield_pct, base_safety_score, payout_ratio_pct, description)
# payout_ratio > 80% triggers a quality penalty (Upgrade #3: avoid yield traps)
DIVIDEND_UNIVERSE = [
    ("SCHD",  3.7, 9.2, 40, "Schwab Dividend Equity — 12yr growth streak, 0.06% ER"),
    ("VYM",   3.1, 8.8, 44, "Vanguard High Yield — 400+ holdings, low ER"),
    ("DGRO",  2.5, 8.5, 42, "iShares Dividend Growth — 5yr+ consecutive increases"),
    ("HDV",   4.1, 7.9, 58, "iShares High Dividend — energy/staples heavy, higher yield"),
    ("JEPI",  7.8, 7.2, 95, "JPMorgan Equity Premium Income — covered call, high income"),
    ("DIVO",  4.9, 7.8, 55, "Amplify CWP Enhanced Dividend — quality dividend payers"),
    ("NOBL",  2.1, 9.5, 38, "S&P 500 Dividend Aristocrats — 25+ yr increase streak"),
    ("SDY",   2.8, 8.9, 45, "SPDR Dividend ETF — 20+ yr growth streak required"),
    ("VIG",   1.9, 9.0, 36, "Vanguard Dividend Appreciation — 10yr growth minimum"),
    ("PEY",   5.2, 7.1, 82, "Invesco High Yield Equity Dividend — top 50 yielders"),
    ("SPHD",  4.6, 7.5, 72, "Invesco Low Volatility High Dividend — defensive tilt"),
    ("SPYD",  4.8, 7.3, 76, "SPDR Portfolio High Dividend — 80 highest S&P yielders"),
    ("DVY",   3.9, 8.1, 61, "iShares Select Dividend — screens for dividend consistency"),
    ("FVD",   2.6, 8.6, 43, "First Trust Value Line Dividend — value + dividend combo"),
    ("XLP",   2.9, 8.4, 52, "Consumer Staples SPDR — recession-proof dividend payers"),
    ("XLU",   3.4, 8.0, 65, "Utilities SPDR — regulated income, interest rate sensitive"),
]


# ── Alpaca HTTP ───────────────────────────────────────────────────────────────
def _base() -> str:
    return ALPACA_LIVE if load_config().get("mode") == "live" else ALPACA_PAPER

def _h() -> dict:
    """
    FIX #1 — Ghost Key Bug:
    Reads ALPACA_LIVE_KEY/SECRET when in live mode, ALPACA_PAPER_KEY/SECRET for paper.
    Both sets of keys can live in .env simultaneously so switching modes is instant.
    """
    cfg = load_config()
    if cfg.get("mode") == "live":
        return {
            "APCA-API-KEY-ID":     os.getenv("ALPACA_LIVE_KEY", ""),
            "APCA-API-KEY-SECRET": os.getenv("ALPACA_LIVE_SECRET", ""),
        }
    return {
        "APCA-API-KEY-ID":     os.getenv("ALPACA_PAPER_KEY", ""),
        "APCA-API-KEY-SECRET": os.getenv("ALPACA_PAPER_SECRET", ""),
    }

def _get(path: str, base: str | None = None) -> Any:
    import urllib.request
    url = (base or _base()).rstrip("/") + "/" + path.lstrip("/")
    try:
        with urllib.request.urlopen(
            urllib.request.Request(url, headers=_h()), timeout=10
        ) as r:
            return json.loads(r.read())
    except Exception as e:
        log({"event": "alpaca_get_error", "url": url, "error": str(e)})
        return None

def _post(path: str, body: dict) -> Any:
    import urllib.request, urllib.error
    url  = _base().rstrip("/") + "/" + path.lstrip("/")
    data = json.dumps(body).encode()
    req  = urllib.request.Request(
        url, data=data,
        headers={**_h(), "Content-Type": "application/json"},
        method="POST"
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        txt = e.read().decode()
        log({"event": "alpaca_post_error", "url": url, "status": e.code, "detail": txt})
        return {"error": txt, "status": e.code}
    except Exception as e:
        log({"event": "alpaca_post_error", "url": url, "error": str(e)})
        return None

def _delete(path: str) -> Any:
    import urllib.request
    url = _base().rstrip("/") + "/" + path.lstrip("/")
    req = urllib.request.Request(url, headers=_h(), method="DELETE")
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            raw = r.read()
            return json.loads(raw) if raw else {}
    except Exception as e:
        log({"event": "alpaca_delete_error", "url": url, "error": str(e)})
        return None

def get_account() -> dict:
    r = _get("/account")
    return r if isinstance(r, dict) else {}

def get_positions() -> list[dict]:
    r = _get("/positions")
    return r if isinstance(r, list) else []

def get_orders(status: str = "all", limit: int = 20) -> list[dict]:
    r = _get(f"/orders?status={status}&limit={limit}&direction=desc")
    return r if isinstance(r, list) else []

def get_quote(symbol: str) -> float | None:
    """
    Latest trade price. Uses feed=iex so paper-only Alpaca accounts work.
    Paper accounts cannot access the SIP feed — IEX is the correct feed for them.
    """
    data = _get(f"/stocks/{symbol}/trades/latest?feed=iex", base=ALPACA_DATA)
    if data and "trade" in data:
        return float(data["trade"]["p"])
    # Fallback: try latest bar if trade endpoint returns nothing
    bar_data = _get(f"/stocks/{symbol}/bars/latest?timeframe=1Min&feed=iex", base=ALPACA_DATA)
    if bar_data and "bar" in bar_data:
        return float(bar_data["bar"]["c"])
    return None

def get_bars(symbol: str, days: int = 30) -> list[float]:
    end   = datetime.now(timezone.utc)
    start = end - timedelta(days=days + 10)
    url   = (
        f"/stocks/{symbol}/bars?timeframe=1Day"
        f"&start={start.strftime('%Y-%m-%d')}"
        f"&end={end.strftime('%Y-%m-%d')}"
        f"&limit={days + 10}"
        f"&feed=iex"     # required for paper-only Alpaca accounts
    )
    data = _get(url, base=ALPACA_DATA)
    return [b["c"] for b in data["bars"]] if data and "bars" in data else []

def buy_notional(symbol: str, notional: float) -> dict:
    return _post("/orders", {
        "symbol": symbol, "notional": round(notional, 2),
        "side": "buy", "type": "market", "time_in_force": "day",
    }) or {}

def sell_qty(symbol: str, qty: float) -> dict:
    return _post("/orders", {
        "symbol": symbol, "qty": round(qty, 6),
        "side": "sell", "type": "market", "time_in_force": "day",
    }) or {}

def close_all_positions() -> list[dict]:
    positions = get_positions()
    results   = []
    for pos in positions:
        sym = pos.get("symbol", "")
        qty = float(pos.get("qty", 0))
        if qty > 0:
            r = sell_qty(sym, qty)
            results.append({"symbol": sym, "qty": qty, "result": r})
            log({"event": "crash_sell", "symbol": sym, "qty": qty})
    return results


# ── Market intelligence ───────────────────────────────────────────────────────
def compute_rsi(closes: list[float], period: int = 14) -> float | None:
    if len(closes) < period + 1:
        return None
    gains, losses = [], []
    for i in range(1, len(closes)):
        d = closes[i] - closes[i - 1]
        gains.append(max(d, 0))
        losses.append(max(-d, 0))
    ag = sum(gains[-period:]) / period
    al = sum(losses[-period:]) / period
    return 100.0 if al == 0 else 100 - (100 / (1 + ag / al))

def compute_ma200(closes: list[float]) -> float | None:
    """200-day moving average — the global bull/bear filter."""
    if len(closes) < 200:
        return None
    return sum(closes[-200:]) / 200

def compute_atr(highs: list[float], lows: list[float], closes: list[float], period: int = 14) -> float | None:
    """Average True Range — measures volatility for dynamic stop placement."""
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

def get_ohlc_bars(symbol: str, days: int = 220) -> dict:
    """Fetch OHLC bars for MA200 and ATR calculations."""
    end   = datetime.now(timezone.utc)
    start = end - timedelta(days=days + 20)
    url   = (
        f"/stocks/{symbol}/bars?timeframe=1Day"
        f"&start={start.strftime('%Y-%m-%d')}"
        f"&end={end.strftime('%Y-%m-%d')}"
        f"&limit={days + 20}"
        f"&feed=iex"     # required for paper-only accounts
    )
    data = _get(url, base=ALPACA_DATA)
    if not data or "bars" not in data:
        return {"highs": [], "lows": [], "closes": []}
    return {
        "highs":  [b["h"] for b in data["bars"]],
        "lows":   [b["l"] for b in data["bars"]],
        "closes": [b["c"] for b in data["bars"]],
    }

def market_sentiment() -> dict:
    """
    Full market regime detection:
      - SPY daily % change (crash trigger)
      - RSI (buy_dip / overbought)
      - 200-day MA (Upgrade #4: bear market filter — blocks buys in downtrend)
    """
    cfg    = load_config()
    ohlc   = get_ohlc_bars("SPY", 220)
    closes = ohlc["closes"]

    spy_change = None
    if len(closes) >= 2:
        spy_change = (closes[-1] - closes[-2]) / closes[-2] if closes[-2] != 0 else None

    rsi   = compute_rsi(closes[-31:]) if len(closes) >= 15 else None
    ma200 = compute_ma200(closes)
    above_ma200 = (closes[-1] > ma200) if (ma200 and closes) else None

    signal = "hold"
    detail = "Markets normal. Auto-pilot active."
    regime = "bull" if above_ma200 else ("bear" if above_ma200 is False else "unknown")

    if spy_change is not None:
        if spy_change <= cfg.get("crash_threshold", -0.05):
            signal = "crash"
            detail = f"SPY down {abs(spy_change)*100:.1f}% today — crash protocol active."
        elif spy_change <= -0.02:
            signal = "caution"
            detail = f"Market softening ({spy_change*100:.1f}%). Monitoring closely."
        elif regime == "bear" and cfg.get("ma200_filter", True):
            signal = "caution"
            detail = f"SPY below 200-day MA (${ma200:.2f}) — defensive mode. Holding existing positions, no new buys."
        elif rsi and rsi < 32:
            signal = "buy_dip"
            detail = f"RSI {rsi:.0f} — oversold. Favourable entry."
        elif rsi and rsi > 72:
            signal = "caution"
            detail = f"RSI {rsi:.0f} — overbought. Holding steady."

    return {
        "signal":       signal,
        "spy_change":   spy_change,
        "rsi":          rsi,
        "ma200":        round(ma200, 2) if ma200 else None,
        "above_ma200":  above_ma200,
        "regime":       regime,
        "detail":       detail,
    }


# ── Wash sale guard (Future Issue #1 — Canada Superficial Loss Rule) ──────────
def _recently_sold_at_loss(symbol: str, cooldown_days: int = 31) -> bool:
    """
    Returns True if this symbol was sold at a loss within the cooldown window.
    Prevents triggering Canada's superficial loss rule which disqualifies
    the tax loss if you rebuy within 30 days before or after the sale.
    """
    conn     = get_db()
    cutoff   = (datetime.now(timezone.utc) - timedelta(days=cooldown_days)).isoformat()
    row      = conn.execute(
        "SELECT id FROM trades WHERE side='conservative' AND symbol=? "
        "AND action='sell' AND gain < 0 AND date > ? LIMIT 1",
        (symbol, cutoff)
    ).fetchone()
    conn.close()
    return row is not None


# ── Quality factor scoring (Upgrade #3 — Dynamic Factor Scoring) ─────────────
def _quality_adjusted_score(symbol: str, yield_pct: float, safety: float,
                              payout_ratio: float) -> tuple[float, str]:
    """
    Score = (yield × 0.5) + (safety × 0.5)
    Penalty applied when payout_ratio > 80%:
      - 80–90%: -0.5 penalty (caution zone)
      - 90–100%: -1.0 penalty (yield trap warning)
      - >100%: -2.0 penalty (unsustainable — company paying more than it earns)
    This prevents the bot from chasing high-yield traps like PEY or JEPI
    when their fundamentals deteriorate.
    """
    base_score = (yield_pct * 0.5) + (safety * 0.5)
    penalty    = 0.0
    flag       = "clean"

    if payout_ratio > 100:
        penalty = 2.0
        flag    = "⚠ YIELD TRAP: payout > 100% (paying out more than earned)"
    elif payout_ratio > 90:
        penalty = 1.0
        flag    = "⚠ High payout ratio (90%+) — sustainability risk"
    elif payout_ratio > 80:
        penalty = 0.5
        flag    = "⚡ Elevated payout ratio (80%+) — monitor closely"

    return round(base_score - penalty, 3), flag


# ── Dividend scanner ──────────────────────────────────────────────────────────
def scan_dividend_opportunities() -> list[dict]:
    """
    Scores every ETF in DIVIDEND_UNIVERSE.
    - Quality penalty for high payout ratios (Upgrade #3)
    - RSI dip bonus for oversold ETFs
    - 500ms throttle between API calls (Future Issue #3: rate limiting)
    """
    cfg      = load_config()
    throttle = cfg.get("scan_throttle_ms", 500) / 1000.0
    scored   = []

    for sym, yld, safety, payout, desc in DIVIDEND_UNIVERSE:
        price  = get_quote(sym)
        closes = get_bars(sym, 30) if price else []
        rsi    = compute_rsi(closes) if len(closes) > 15 else None

        score, quality_flag = _quality_adjusted_score(sym, yld, safety, payout)

        # RSI dip bonus: oversold ETF is a buy signal
        if rsi and rsi < 35:
            score += 0.3

        scored.append({
            "symbol":       sym,
            "yield_pct":    yld,
            "safety":       safety,
            "payout_ratio": payout,
            "score":        score,
            "price":        price,
            "rsi":          round(rsi, 1) if rsi else None,
            "desc":         desc,
            "quality_flag": quality_flag,
            "signal":       "buy_dip" if rsi and rsi < 35 else
                            "caution" if rsi and rsi > 72 else "neutral",
        })

        # Rate limit throttle — prevents Alpaca banning IP as universe grows
        time.sleep(throttle)

    scored.sort(key=lambda x: x["score"], reverse=True)
    return scored


# ── Rotation (Bugs #1 + #2 fixed, Future #1 wash sale guard) ─────────────────
def check_rotation() -> dict:
    """
    For each current position, check if a meaningfully better ETF exists.

    FIX #2 — Leftover Cash:
      OLD (wrong): sell full position, reinvest only 70% of GAIN → $1,030 stuck in cash
      NEW (correct): sell full position, reinvest PRINCIPAL + 70% of GAIN, save 30% of GAIN
      Example: position worth $1,100, cost $1,000, gain $100
        → Reinvest: $1,000 (principal) + $70 (70% of gain) = $1,070
        → Save: $30 (30% of gain)
        → Zero cash leak. Full stack stays invested.

    Future #1 — Wash Sale:
      Will not rotate a symbol that was recently sold at a loss (31-day cooldown).
    """
    cfg        = load_config()
    threshold  = float(cfg.get("rotation_threshold", 1.0))
    cooldown   = int(cfg.get("wash_sale_cooldown_days", 31))
    positions  = get_positions()
    sentiment  = market_sentiment()
    actions    = []

    # Crash protocol: sell everything
    if sentiment["signal"] == "crash" and cfg.get("crash_sell"):
        results = close_all_positions()
        conn    = get_db(); today = utc_now()
        conn.execute("INSERT INTO scan_log (date,type,result,action_taken) VALUES (?,?,?,?)",
                     (today, "crash_sell", "crash detected", f"Closed {len(results)} positions"))
        conn.execute("INSERT INTO alerts (date,type,symbol,detail,side) VALUES (?,?,?,?,?)",
                     (today, "crash", "SPY", sentiment["detail"], "conservative"))
        conn.commit(); conn.close()
        return {"action": "crash_sell", "positions_closed": len(results),
                "detail": sentiment["detail"]}

    # 200-day MA bear filter: hold existing, don't rotate into new positions
    if sentiment.get("regime") == "bear" and cfg.get("ma200_filter", True):
        return {"action": "defensive_hold",
                "detail": sentiment["detail"],
                "rotations": [],
                "sentiment": sentiment}

    opportunities = scan_dividend_opportunities()
    best          = opportunities[0] if opportunities else None

    for pos in positions:
        sym  = pos.get("symbol", "")
        qty  = float(pos.get("qty", 0))
        cost = float(pos.get("cost_basis", 0))
        mv   = float(pos.get("market_value", 0))
        gain = mv - cost

        # Only rotate if there's actual gain to share (never rotate at a loss)
        if gain <= 0:
            continue

        current = next((x for x in opportunities if x["symbol"] == sym), None)
        if not current or not best or best["symbol"] == sym:
            continue

        yield_improvement = best["yield_pct"] - current["yield_pct"]
        safety_ok = best["safety"] >= current["safety"] if cfg.get("rotation_safety") else True

        # Wash sale guard: don't buy back within 31 days of a loss sale
        if _recently_sold_at_loss(best["symbol"], cooldown):
            log({"event": "wash_sale_skip", "symbol": best["symbol"],
                 "reason": f"sold at loss within {cooldown} days"})
            continue

        if yield_improvement >= threshold and safety_ok:
            sell_result = sell_qty(sym, qty)

            # FIX #2: correct split — principal goes back in, only GAIN is split 70/30
            save_amount   = round(gain * 0.30, 2)
            reinvest_gain = round(gain * 0.70, 2)
            reinvest_total = round(cost + reinvest_gain, 2)   # principal + 70% of gain

            buy_result  = buy_notional(best["symbol"], reinvest_total) if reinvest_total >= 1 else {}
            new_savings = add_savings(save_amount, f"rotation gain: {sym}→{best['symbol']}")

            conn  = get_db(); today = utc_now()
            conn.execute(
                "INSERT INTO rotation_log (date,sold_symbol,sold_amount,sold_price,gain,principal,"
                "bought_symbol,bought_amount,saved,reason) VALUES (?,?,?,?,?,?,?,?,?,?)",
                (today, sym, mv, get_quote(sym), gain, cost,
                 best["symbol"], reinvest_total, save_amount,
                 f"{best['symbol']} yields {best['yield_pct']:.1f}% vs {current['yield_pct']:.1f}%"
                 f" (+{yield_improvement:.1f}%) · safety {best['safety']:.1f} vs {current['safety']:.1f}"
                 f" · payout {best['payout_ratio']}%")
            )
            conn.execute(
                "INSERT INTO trades (date,side,symbol,action,shares,price,amount,gain,note,mode) "
                "VALUES (?,?,?,?,?,?,?,?,?,?)",
                (today, "conservative", sym, "sell", qty, get_quote(sym), mv, gain,
                 f"rotation → {best['symbol']}", "auto")
            )
            conn.execute(
                "INSERT INTO trades (date,side,symbol,action,shares,price,amount,gain,note,mode) "
                "VALUES (?,?,?,?,?,?,?,?,?,?)",
                (today, "conservative", best["symbol"], "buy", None, best["price"],
                 reinvest_total, 0,
                 f"rotation from {sym}: principal ${cost:.2f} + 70% gain ${reinvest_gain:.2f}", "auto")
            )
            conn.execute(
                "INSERT INTO scan_log (date,type,result,action_taken) VALUES (?,?,?,?)",
                (today, "rotation", f"{sym}→{best['symbol']}",
                 f"Gain ${gain:.2f}: reinvested ${reinvest_total:.2f} (principal+70%), saved ${save_amount:.2f}")
            )
            conn.commit(); conn.close()
            log({"event": "rotation", "from": sym, "to": best["symbol"],
                 "gain": gain, "principal": cost,
                 "reinvested": reinvest_total, "saved": save_amount})

            actions.append({
                "type":            "rotation",
                "from":            sym,
                "to":              best["symbol"],
                "principal":       cost,
                "gain":            gain,
                "reinvested":      reinvest_total,
                "saved":           save_amount,
                "reason":          f"+{yield_improvement:.1f}% yield · payout {best['payout_ratio']}%",
                "quality_flag":    best.get("quality_flag", ""),
            })

    conn = get_db()
    conn.execute("INSERT INTO scan_log (date,type,result,action_taken) VALUES (?,?,?,?)",
                 (utc_now(), "dividend_scan",
                  f"Scanned {len(opportunities)} ETFs · regime: {sentiment.get('regime','?')}",
                  f"{len(actions)} rotations"))
    conn.commit(); conn.close()

    return {
        "action":           "scan_complete",
        "rotations":        actions,
        "top_opportunity":  best,
        "sentiment":        sentiment,
    }


def initial_buy(amount: float) -> dict:
    """
    First deploy. Buys top 3 ranked ETFs for immediate diversification.
    Blocked in bear regime (200-MA filter) and crash.
    """
    sentiment = market_sentiment()
    if sentiment["signal"] == "crash":
        return {"status": "halted", "reason": sentiment["detail"]}
    if sentiment.get("regime") == "bear" and load_config().get("ma200_filter", True):
        return {"status": "halted",
                "reason": f"Bear market detected (SPY below 200-day MA ${sentiment.get('ma200'):.2f}). "
                          "Deploy paused to protect capital. Will resume when trend recovers."}

    invest = round(amount * 0.70, 2)
    save   = round(amount * 0.30, 2)
    opps   = scan_dividend_opportunities()

    if not opps:
        return {"status": "error", "reason": "No opportunities found"}

    top3    = opps[:3]
    per     = round(invest / len(top3), 2)
    results = []
    conn    = get_db(); today = utc_now()

    for etf in top3:
        if per < 1:
            continue
        order = buy_notional(etf["symbol"], per)
        price = etf["price"]
        conn.execute(
            "INSERT INTO trades (date,side,symbol,action,shares,price,amount,gain,note,mode) "
            "VALUES (?,?,?,?,?,?,?,?,?,?)",
            (today, "conservative", etf["symbol"], "buy",
             round(per / price, 6) if price else None, price, per, 0,
             f"initial deploy — rank #{top3.index(etf)+1} · yield {etf['yield_pct']:.1f}%"
             f" · payout {etf['payout_ratio']}%", "auto")
        )
        results.append({
            "symbol":       etf["symbol"],
            "amount":       per,
            "yield_pct":    etf["yield_pct"],
            "payout_ratio": etf["payout_ratio"],
            "quality_flag": etf.get("quality_flag", ""),
            "order_id":     order.get("id") if isinstance(order, dict) else None,
        })

    add_savings(save, "initial deploy")
    conn.commit(); conn.close()
    return {
        "status":    "ok",
        "invested":  invest,
        "saved":     save,
        "buys":      results,
        "sentiment": sentiment,
    }


def get_conservative_dashboard() -> dict:
    conn      = get_db()
    account   = get_account()
    positions = get_positions()
    sentiment = market_sentiment()
    opps      = scan_dividend_opportunities()

    total_mv = sum(float(p.get("market_value", 0)) for p in positions)
    holdings = [{
        "symbol":          p["symbol"],
        "shares":          float(p.get("qty", 0)),
        "avg_cost":        float(p.get("avg_entry_price", 0)),
        "current_price":   float(p.get("current_price", 0)),
        "market_value":    float(p.get("market_value", 0)),
        "cost_basis":      float(p.get("cost_basis", 0)),
        "unrealized_pl":   float(p.get("unrealized_pl", 0)),
        "unrealized_plpc": float(p.get("unrealized_plpc", 0)),
    } for p in positions]

    # Fix #3: Net cost = total bought - total sold (prevents phantom negative G/L
    # from old migration rows that had no side column and were double-counted)
    buy_cost = conn.execute(
        "SELECT COALESCE(SUM(amount), 0) FROM trades "
        "WHERE side='conservative' AND action='buy'"
    ).fetchone()[0] or 0.0

    sell_proceeds = conn.execute(
        "SELECT COALESCE(SUM(amount), 0) FROM trades "
        "WHERE side='conservative' AND action='sell'"
    ).fetchone()[0] or 0.0

    total_cost = max(buy_cost - sell_proceeds, 0.0)  # net capital deployed
    rotations = conn.execute(
        "SELECT * FROM rotation_log ORDER BY id DESC LIMIT 10"
    ).fetchall()
    scan_log  = conn.execute(
        "SELECT * FROM scan_log ORDER BY id DESC LIMIT 5"
    ).fetchall()

    buying_power = float(account.get("buying_power", 0)) if account else 0.0
    conn.close()

    return {
        "holdings":         holdings,
        "total_mv":         round(total_mv, 2),
        "total_cost":       round(total_cost, 2),
        "gain_loss":        round(total_mv - total_cost, 2),
        "gain_loss_pct":    round((total_mv - total_cost) / total_cost * 100 if total_cost else 0, 2),
        "buying_power":     round(buying_power, 2),
        "sentiment":        sentiment,
        "opportunities":    opps[:8],
        "rotations":        [dict(r) for r in rotations],
        "scan_log":         [dict(s) for s in scan_log],
        "orders":           get_orders("all", 10),
        "alpaca_connected": bool(os.getenv("ALPACA_PAPER_KEY") or os.getenv("ALPACA_LIVE_KEY")),
    }
