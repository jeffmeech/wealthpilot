"""
app.py — WealthPilot v3 Flask Server
"""
from dotenv import load_dotenv
load_dotenv()

from flask import Flask, jsonify, render_template, request
import engine_core as core
import engine_conservative as cons
import engine_crypto as crypto

app = Flask(__name__, template_folder="templates")

# ── Dashboard ──────────────────────────────────────────────────────────────────
@app.route("/")
def index(): return render_template("index.html")

@app.route("/api/dashboard")
def api_dashboard():
    cons_data   = cons.get_conservative_dashboard()
    crypto_data = crypto.get_crypto_dashboard()
    cfg         = core.load_config()
    conn        = core.get_db()
    savings     = core.get_savings_balance()
    sav_hist    = conn.execute("SELECT date,amount,source,total FROM savings ORDER BY id DESC LIMIT 12").fetchall()
    alerts      = conn.execute("SELECT * FROM alerts WHERE seen=0 ORDER BY date DESC").fetchall()
    withdrawals = conn.execute("SELECT * FROM withdrawals ORDER BY id DESC LIMIT 10").fetchall()
    cross       = conn.execute("SELECT * FROM cross_transfers ORDER BY id DESC LIMIT 10").fetchall()
    email_log   = conn.execute("SELECT * FROM email_log ORDER BY id DESC LIMIT 10").fetchall()
    conn.close()

    return jsonify({
        "conservative":    cons_data,
        "crypto":          crypto_data,
        "savings_total":   round(savings, 2),
        "savings_history": [dict(r) for r in sav_hist],
        "unseen_alerts":   [dict(a) for a in alerts],
        "withdrawals":     [dict(w) for w in withdrawals],
        "cross_transfers": [dict(c) for c in cross],
        "email_log":       [dict(e) for e in email_log],
        "config":          cfg,
        "server_time":     core.utc_now(),
        "goal_savings":    cfg.get("savings_goal", 500.0),
        "email_configured":bool(__import__("os").getenv("ALERT_EMAIL_FROM") and
                                __import__("os").getenv("ALERT_EMAIL_PASSWORD")),
    })

# ── Conservative ───────────────────────────────────────────────────────────────
@app.route("/api/conservative/deploy", methods=["POST"])
def api_deploy():
    data   = request.get_json(silent=True) or {}
    amount = float(data.get("amount", core.load_config().get("monthly_budget",100)))
    result = cons.initial_buy(amount)
    return jsonify({"ok": result.get("status")=="ok", "result": result})

@app.route("/api/conservative/scan", methods=["POST"])
def api_scan():
    result = cons.check_rotation()
    return jsonify({"ok": True, "result": result})

@app.route("/api/conservative/buy", methods=["POST"])
def api_cons_buy():
    data = request.get_json(silent=True) or {}
    sym  = data.get("symbol","").upper(); amt = float(data.get("amount",0))
    if not sym or amt < 1: return jsonify({"ok":False,"error":"symbol + amount >= $1"}),400
    order = cons.buy_notional(sym, amt)
    conn  = core.get_db()
    price = cons.get_quote(sym)
    conn.execute("INSERT INTO trades (date,side,symbol,action,shares,price,amount,gain,note,mode) VALUES (?,?,?,?,?,?,?,?,?,?)",
                 (core.utc_now(),"conservative",sym,"buy",round(amt/price,6) if price else None,price,amt,0,"manual buy","manual"))
    conn.commit(); conn.close()
    return jsonify({"ok": True, "result": {"symbol":sym,"amount":amt,"order_id":order.get("id")}})

@app.route("/api/conservative/sell", methods=["POST"])
def api_cons_sell():
    data = request.get_json(silent=True) or {}
    sym  = data.get("symbol","").upper(); qty = float(data.get("qty",0))
    if not sym or qty <= 0: return jsonify({"ok":False,"error":"symbol + qty > 0"}),400
    order = cons.sell_qty(sym, qty)
    price = cons.get_quote(sym)
    conn  = core.get_db()
    conn.execute("INSERT INTO trades (date,side,symbol,action,shares,price,amount,gain,note,mode) VALUES (?,?,?,?,?,?,?,?,?,?)",
                 (core.utc_now(),"conservative",sym,"sell",qty,price,round(qty*(price or 0),2),0,"manual sell","manual"))
    conn.commit(); conn.close()
    return jsonify({"ok": True, "result": {"symbol":sym,"qty":qty,"order_id":order.get("id")}})

@app.route("/api/conservative/opportunities")
def api_opportunities():
    return jsonify({"items": cons.scan_dividend_opportunities()})

@app.route("/api/conservative/sentiment")
def api_sentiment():
    return jsonify(cons.market_sentiment())

# ── Crypto ─────────────────────────────────────────────────────────────────────
@app.route("/api/crypto/check", methods=["POST"])
def api_crypto_check():
    result = crypto.check_crypto_exits()
    return jsonify({"ok": True, "result": result})

@app.route("/api/crypto/buy", methods=["POST"])
def api_crypto_buy():
    data   = request.get_json(silent=True) or {}
    symbol = data.get("symbol","").upper(); amount = float(data.get("amount",0))
    if not symbol or amount < 1: return jsonify({"ok":False,"error":"symbol + amount >= 1"}),400
    result = crypto.manual_crypto_buy(symbol, amount)
    return jsonify({"ok": result.get("status")=="ok", "result": result})

@app.route("/api/crypto/sell", methods=["POST"])
def api_crypto_sell():
    data   = request.get_json(silent=True) or {}
    symbol = data.get("symbol",""); qty = float(data.get("qty",0))
    if not symbol or qty <= 0: return jsonify({"ok":False,"error":"symbol + qty > 0"}),400
    result = crypto.manual_crypto_sell(symbol, qty)
    return jsonify({"ok": result.get("status")=="ok", "result": result})

# ── Savings ────────────────────────────────────────────────────────────────────
@app.route("/api/savings/withdraw", methods=["POST"])
def api_withdraw():
    data   = request.get_json(silent=True) or {}
    amount = float(data.get("amount",0)); reason = data.get("reason","")
    result = core.withdraw_savings(amount, reason)
    return jsonify({"ok": result.get("status")=="ok", "result": result})

# ── Alerts ─────────────────────────────────────────────────────────────────────
@app.route("/api/alerts/dismiss", methods=["POST"])
def api_dismiss():
    conn = core.get_db(); conn.execute("UPDATE alerts SET seen=1"); conn.commit(); conn.close()
    return jsonify({"ok": True})

# ── Email ──────────────────────────────────────────────────────────────────────
@app.route("/api/email/send", methods=["POST"])
def api_send_report():
    from engine_conservative import get_conservative_dashboard
    from engine_crypto import get_crypto_dashboard
    cons_d  = get_conservative_dashboard()
    cry_d   = get_crypto_dashboard()
    savings = core.get_savings_balance()
    html    = _build_full_report(cons_d, cry_d, savings)
    ok, msg = core.smtp_send(f"WealthPilot Daily Report — {core.local_date()}", html)
    return jsonify({"ok": ok, "message": msg})

def _build_full_report(cons_d, cry_d, savings) -> str:
    from engine_conservative import market_sentiment
    s = cons_d.get("sentiment",{})
    sig = s.get("signal","hold")
    sig_map = {"hold":("#1a3a5c","#40c4ff","📊 Normal"),"buy_dip":("#0a2a15","#00e676","📈 Buy Dip"),
               "caution":("#2a2000","#ffab40","⚠ Caution"),"crash":("#2a0a0a","#ff5252","🚨 Crash")}
    bg,clr,lbl = sig_map.get(sig,sig_map["hold"])
    gl = cons_d.get("gain_loss",0); gl_c = "#00c853" if gl>=0 else "#ff5252"

    pos_rows = "".join(f"""<tr>
      <td style="padding:12px 14px;border-bottom:1px solid #1e2d3d;color:#e2eaf5"><strong>{h['symbol']}</strong></td>
      <td style="padding:12px 14px;border-bottom:1px solid #1e2d3d;color:#a0b4c8">${h['market_value']:,.2f}</td>
      <td style="padding:12px 14px;border-bottom:1px solid #1e2d3d;color:{'#00c853' if h['unrealized_pl']>=0 else '#ff5252'}">
        {'▲' if h['unrealized_pl']>=0 else '▼'} ${abs(h['unrealized_pl']):,.2f} ({h['unrealized_plpc']*100:+.2f}%)</td>
    </tr>""" for h in cons_d.get("holdings",[]))

    crypto_rows = "".join(f"""<tr>
      <td style="padding:12px 14px;border-bottom:1px solid #1e2d3d;color:#e2eaf5"><strong>{p['symbol']}</strong></td>
      <td style="padding:12px 14px;border-bottom:1px solid #1e2d3d;color:#a0b4c8">${p['market_value']:,.2f}</td>
      <td style="padding:12px 14px;border-bottom:1px solid #1e2d3d;color:{'#00c853' if p['gain']>=0 else '#ff5252'}">
        {'▲' if p['gain']>=0 else '▼'} ${abs(p['gain']):,.2f} ({p['gain_pct']:+.2f}%)</td>
    </tr>""" for p in cry_d.get("positions",[]))

    opp = cons_d.get("opportunities",[{}])[0]
    total = cons_d.get("total_mv",0) + cry_d.get("total_mv",0) + savings

    return f"""<!DOCTYPE html><html><head><meta charset="UTF-8"/></head>
<body style="margin:0;padding:0;background:#080b0f;font-family:'Segoe UI',Arial,sans-serif">
<div style="max-width:680px;margin:0 auto;padding:28px 16px">
  <div style="background:linear-gradient(135deg,#0d1a2e,#0a1a0f);border:1px solid #1c2635;border-radius:12px;padding:24px;margin-bottom:16px">
    <div style="font-size:22px;font-weight:800;color:#e2eaf5">💹 WealthPilot Daily Report</div>
    <div style="font-size:12px;color:#5a7a9a;margin-top:4px">{core.local_date()}</div>
  </div>
  <div style="background:{bg};border:1px solid {clr}44;border-radius:10px;padding:14px 18px;margin-bottom:16px">
    <strong style="color:{clr}">{lbl}</strong>
    <span style="color:#a0b4c8;font-size:12px;margin-left:8px">{s.get('detail','')}</span>
    <span style="margin-left:16px;font-size:12px;color:#6a8aaa">RSI: {f"{s['rsi']:.0f}" if s.get('rsi') else 'N/A'}</span>
  </div>
  <div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:12px;margin-bottom:16px">
    <div style="background:#0d1117;border:1px solid #1c2635;border-radius:10px;padding:16px">
      <div style="font-size:10px;color:#5a7a9a;text-transform:uppercase;letter-spacing:.1em">Total Wealth</div>
      <div style="font-size:22px;font-weight:800;color:#00e676;margin-top:4px">${total:,.2f}</div>
    </div>
    <div style="background:#0d1117;border:1px solid #1c2635;border-radius:10px;padding:16px">
      <div style="font-size:10px;color:#5a7a9a;text-transform:uppercase;letter-spacing:.1em">Conservative G/L</div>
      <div style="font-size:22px;font-weight:800;color:{gl_c};margin-top:4px">{'▲' if gl>=0 else '▼'} ${abs(gl):,.2f}</div>
    </div>
    <div style="background:#0d1117;border:1px solid #1c2635;border-radius:10px;padding:16px">
      <div style="font-size:10px;color:#5a7a9a;text-transform:uppercase;letter-spacing:.1em">Savings Vault</div>
      <div style="font-size:22px;font-weight:800;color:#40c4ff;margin-top:4px">${savings:,.2f}</div>
    </div>
  </div>
  <div style="background:#0d1117;border:1px solid #1c2635;border-radius:10px;overflow:hidden;margin-bottom:16px">
    <div style="padding:14px 18px;border-bottom:1px solid #1c2635;font-size:13px;font-weight:700;color:#e2eaf5">Conservative Positions</div>
    <table style="width:100%;border-collapse:collapse">
      <thead><tr style="background:rgba(0,0,0,.3)">
        {"".join(f'<th style="text-align:left;padding:8px 14px;font-size:10px;color:#3a5a7a;text-transform:uppercase">{h}</th>' for h in ["Symbol","Value","Gain/Loss"])}
      </tr></thead>
      <tbody>{pos_rows or "<tr><td colspan=3 style='padding:16px;text-align:center;color:#6a8aaa'>No positions</td></tr>"}</tbody>
    </table>
  </div>
  <div style="background:#0d1117;border:1px solid #1c2635;border-radius:10px;overflow:hidden;margin-bottom:16px">
    <div style="padding:14px 18px;border-bottom:1px solid #1c2635;font-size:13px;font-weight:700;color:#e2eaf5">Crypto Positions (NDAX)</div>
    <table style="width:100%;border-collapse:collapse">
      <thead><tr style="background:rgba(0,0,0,.3)">
        {"".join(f'<th style="text-align:left;padding:8px 14px;font-size:10px;color:#3a5a7a;text-transform:uppercase">{h}</th>' for h in ["Symbol","Value","Gain/Loss"])}
      </tr></thead>
      <tbody>{crypto_rows or "<tr><td colspan=3 style='padding:16px;text-align:center;color:#6a8aaa'>No positions or NDAX not connected</td></tr>"}</tbody>
    </table>
  </div>
  {f'<div style="background:#0a2a15;border:1px solid #1a4a25;border-radius:10px;padding:14px 18px;margin-bottom:16px"><strong style="color:#00e676">🔄 Best Dividend Opportunity:</strong> <span style="color:#a0b4c8;font-size:12px">{opp.get("symbol")} — {opp.get("yield_pct",0):.1f}% yield, safety {opp.get("safety",0):.1f}/10. {opp.get("desc","")}</span></div>' if opp else ""}
  <div style="text-align:center;padding:16px 0;border-top:1px solid #1c2635;font-size:11px;color:#3a5a7a">
    WealthPilot · {core.local_date()} · Not financial advice.
  </div>
</div></body></html>"""

# ── Settings ───────────────────────────────────────────────────────────────────
@app.route("/api/settings", methods=["GET"])
def api_get_settings(): return jsonify(core.load_config())

@app.route("/api/settings", methods=["POST"])
def api_save_settings():
    data = request.get_json(silent=True) or {}
    cfg  = core.save_config(data)
    return jsonify({"ok": True, "config": cfg})

@app.route("/api/quote/<symbol>")
def api_quote(symbol): return jsonify({"symbol":symbol,"price":cons.get_quote(symbol.upper())})

@app.route("/api/debug")
def api_debug():
    """
    Live diagnostics — visit http://localhost:5000/api/debug to see exactly
    what Alpaca and NDAX are returning. Useful when SPY/RSI shows dashes.
    """
    import os
    cfg = core.load_config()

    # Test Alpaca data feed
    spy_quote  = cons.get_quote("SPY")
    spy_bars   = cons.get_bars("SPY", 5)
    account    = cons.get_account()
    positions  = cons.get_positions()

    # Test NDAX
    ndax_bal   = crypto.get_crypto_balances()
    btc_ticker = crypto.get_crypto_ticker("BTC/CAD")

    return jsonify({
        "alpaca": {
            "mode":            cfg.get("mode"),
            "paper_key_set":   bool(os.getenv("ALPACA_PAPER_KEY")),
            "live_key_set":    bool(os.getenv("ALPACA_LIVE_KEY")),
            "account_ok":      bool(account and account.get("id")),
            "account_status":  account.get("status") if account else "no response",
            "buying_power":    account.get("buying_power") if account else None,
            "spy_quote":       spy_quote,
            "spy_bars_count":  len(spy_bars),
            "spy_bars_sample": spy_bars[-3:] if spy_bars else [],
            "positions_count": len(positions),
        },
        "ndax": {
            "key_set":         bool(os.getenv("NDAX_API_KEY")),
            "ccxt_available":  crypto._ccxt_available(),
            "balances":        ndax_bal,
            "btc_ticker":      btc_ticker,
        },
        "config": {
            "mode":            cfg.get("mode"),
            "ma200_filter":    cfg.get("ma200_filter"),
            "crash_threshold": cfg.get("crash_threshold"),
        },
    })

@app.route("/healthz")
def healthz(): return jsonify({"ok":True,"time":core.utc_now()})

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
