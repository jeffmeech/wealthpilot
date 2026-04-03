"""
scanner.py — WealthPilot v3 Background Scanner
Runs every 4 hours during market hours.
  1. Dividend scan: check for better opportunities, rotate if threshold met
  2. Crypto check: profit targets and stop-losses
  3. Email alert if any action taken

Run: python scanner.py
Or cron: 30 9,13,17 * * 1-5 python /path/to/scanner.py
"""
from __future__ import annotations
import time, signal, sys
from datetime import datetime, timezone
from engine_core import log, load_config, smtp_send, utc_now, local_date
from engine_conservative import check_rotation, market_sentiment
from engine_crypto import check_crypto_exits, get_crypto_positions

running = True

def is_market_hours() -> bool:
    """Rough check — US market hours Mon-Fri 9:30-16:00 ET."""
    now = datetime.now()
    if now.weekday() >= 5: return False  # weekend
    hour = now.hour
    return 9 <= hour <= 16

def build_alert_email(rotation_result: dict, crypto_result: dict) -> str | None:
    """Build email only if actions were taken."""
    rotations = rotation_result.get("rotations",[])
    crypto_actions = crypto_result.get("actions",[])
    if not rotations and not crypto_actions:
        return None

    rows = ""
    for r in rotations:
        rows += f"""<tr>
          <td style="padding:10px 14px;border-bottom:1px solid #1e2d3d;color:#e2eaf5">
            Conservative: {r['from']} → {r['to']}</td>
          <td style="padding:10px 14px;border-bottom:1px solid #1e2d3d;color:#00e676">
            Gain ${r['gain']:.2f} split: ${r['reinvested']:.2f} reinvested / ${r['saved']:.2f} saved</td>
          <td style="padding:10px 14px;border-bottom:1px solid #1e2d3d;color:#5a7a9a">
            {r['reason']}</td>
        </tr>"""

    for a in crypto_actions:
        color = "#00e676" if a["type"] == "profit_take" else "#ff5252"
        label = f"Profit +{a.get('gain_pct',0):.1f}%" if a["type"] == "profit_take" else f"Stop-loss {a.get('loss_pct',0):.1f}%"
        detail = a.get("note","") if a["type"] == "profit_take" else f"Loss: ${abs(a.get('loss',0)):.2f}"
        rows += f"""<tr>
          <td style="padding:10px 14px;border-bottom:1px solid #1e2d3d;color:#e2eaf5">
            Crypto: {a['symbol']}</td>
          <td style="padding:10px 14px;border-bottom:1px solid #1e2d3d;color:{color}">{label}</td>
          <td style="padding:10px 14px;border-bottom:1px solid #1e2d3d;color:#5a7a9a">{detail}</td>
        </tr>"""

    return f"""<!DOCTYPE html><html><head><meta charset="UTF-8"/></head>
<body style="margin:0;padding:0;background:#080b0f;font-family:'Segoe UI',Arial,sans-serif">
<div style="max-width:640px;margin:0 auto;padding:28px 16px">
  <div style="background:linear-gradient(135deg,#0d1a2e,#0a1a0f);border:1px solid #1c2635;
      border-radius:12px;padding:24px;margin-bottom:18px">
    <div style="font-size:20px;font-weight:800;color:#e2eaf5">💹 WealthPilot — Action Taken</div>
    <div style="font-size:12px;color:#5a7a9a;margin-top:4px">{local_date()} · Auto-pilot executed {len(rotations)+len(crypto_actions)} action(s)</div>
  </div>
  <div style="background:#0d1117;border:1px solid #1c2635;border-radius:10px;overflow:hidden">
    <table style="width:100%;border-collapse:collapse">
      <thead><tr style="background:rgba(0,0,0,.3)">
        <th style="text-align:left;padding:10px 14px;font-size:10px;color:#3a5a7a;text-transform:uppercase">Event</th>
        <th style="text-align:left;padding:10px 14px;font-size:10px;color:#3a5a7a;text-transform:uppercase">Result</th>
        <th style="text-align:left;padding:10px 14px;font-size:10px;color:#3a5a7a;text-transform:uppercase">Reason</th>
      </tr></thead>
      <tbody>{rows}</tbody>
    </table>
  </div>
  <div style="text-align:center;margin-top:20px;font-size:11px;color:#3a5a7a">
    WealthPilot Auto-Pilot · {local_date()} · Not financial advice.
  </div>
</div></body></html>"""


def run_scan() -> dict:
    log({"event":"scanner_run","ts":utc_now()})
    rotation = check_rotation()
    crypto   = check_crypto_exits()

    html = build_alert_email(rotation, crypto)
    email_sent = False
    if html:
        ok, _ = smtp_send(f"WealthPilot Auto-Pilot — {len(rotation.get('rotations',[]))+len(crypto.get('actions',[]))} action(s) taken", html)
        email_sent = ok

    return {
        "conservative": rotation,
        "crypto":       crypto,
        "email_sent":   email_sent,
        "ts":           utc_now(),
    }


def shutdown(sig, frame):
    global running; running = False
    log({"event":"scanner_shutdown"}); sys.exit(0)


if __name__ == "__main__":
    signal.signal(signal.SIGINT,  shutdown)
    signal.signal(signal.SIGTERM, shutdown)
    cfg = load_config()
    interval_hours = cfg.get("scan_interval_hours", 4)
    interval_secs  = interval_hours * 3600

    print(f"[WealthPilot Scanner] Starting. Scan every {interval_hours}h during market hours.")
    log({"event":"scanner_start","interval_hours":interval_hours})

    # Run immediately on start
    try:
        result = run_scan()
        print(f"[Scanner] Initial scan complete: {result['conservative'].get('action')} | "
              f"crypto actions: {len(result['crypto'].get('actions',[]))}")
    except Exception as e:
        print(f"[Scanner] Error: {e}"); log({"event":"scanner_error","error":str(e)})

    while running:
        time.sleep(interval_secs)
        if not running: break
        try:
            result = run_scan()
            print(f"[Scanner] Scan: {result['conservative'].get('action')} | "
                  f"crypto: {len(result['crypto'].get('actions',[]))} actions")
        except Exception as e:
            print(f"[Scanner] Error: {e}"); log({"event":"scanner_error","error":str(e)})
