"""
parkmind_level1.py — PARKMIND Level 1 Entry Point
Flask app: routes + startup only. All logic lives in core/.
"""
import threading
from flask import Flask, request, jsonify, render_template_string, redirect, url_for, session

from core.config import ENTRY_GATE, EXIT_GATE, WEB_PORT
from core import state
from core.database import (
    init_db, load_state_from_db, save_event,
    log_decision, get_recent_cars, get_recent_decisions, get_total_penalties
)
from core.simulator import (
    sim_login, sync_state, open_gate, close_gate
)
from core.logic import (
    natural_spot_key, send_to_exit
)
from core.events import handle_event

app = Flask(__name__)
app.secret_key = "pretty-little-hackers-level1"

# ------------------------------------------------------------------
# Dashboard users
# ------------------------------------------------------------------
USERS = {
    "admin":    {"password": "admin",    "role": "Admin"},
    "operator": {"password": "operator", "role": "Operator"},
}

# ------------------------------------------------------------------
# HTML templates
# ------------------------------------------------------------------
LOGIN_HTML = """
<!doctype html>
<title>PARKMIND Login</title>
<style>
body{font-family:Arial;background:#111827;color:white;display:grid;place-items:center;height:100vh;margin:0}
.card{background:#1f2937;padding:32px;border-radius:18px;width:320px}
input,button{width:100%;box-sizing:border-box;padding:12px;margin:8px 0;border-radius:8px;border:0}
button{font-weight:bold;cursor:pointer}.err{color:#fca5a5}
</style>
<div class="card">
<h1>PARKMIND</h1><p>Level 1 Control Center</p>
{% if error %}<p class="err">{{error}}</p>{% endif %}
<form method="post">
<input name="username" placeholder="Username" required>
<input name="password" type="password" placeholder="Password" required>
<button>Login</button>
</form>
<p style="opacity:.7">admin/admin or operator/operator</p>
</div>
"""

DASH_HTML = """
<!doctype html>
<html><head><meta http-equiv="refresh" content="2"><title>PARKMIND</title>
<style>
body{font-family:Arial;margin:0;background:#0f172a;color:#e5e7eb}
header{padding:18px 24px;background:#111827;display:flex;justify-content:space-between;align-items:center}
.wrap{padding:18px;display:grid;grid-template-columns:1.2fr .8fr;gap:16px}
.card{background:#1e293b;border-radius:14px;padding:16px}
.stat{display:inline-block;background:#111827;border-radius:10px;padding:10px 14px;margin:4px}
table{width:100%;border-collapse:collapse;font-size:14px}
th,td{padding:8px;border-bottom:1px solid #334155;text-align:left}
.free{color:#86efac}.busy{color:#fca5a5}.warn{color:#fde68a}
button{padding:7px 10px;border:0;border-radius:8px;cursor:pointer}form{display:inline}
.small{font-size:12px;opacity:.8}.ok{color:#86efac}.bad{color:#fca5a5}
.co-safe{color:#86efac}.co-low{color:#fde68a}.co-mod{color:#fb923c}.co-high{color:#fca5a5;font-weight:bold}
</style></head><body>
<header><div><b>PARKMIND</b> — Pretty Little Hackers</div>
<div>{{role}} | {{user}} | <a style="color:white" href="/logout">Logout</a></div></header>

<div style="padding:14px 18px">
<span class="stat">FREE: <b>{{free_count}}</b></span>
<span class="stat">OCCUPIED/RESERVED: <b>{{busy_count}}</b></span>
<span class="stat">CARS TRACKED: <b>{{cars|length}}</b></span>
<span class="stat bad">PENALTIES: <b>${{total_penalties}}</b></span>
<form method="post" action="/sync"><button>SYNC FROM SIMULATOR</button></form>
</div>

<div class="wrap">
<div class="card"><h2>Parking Spots</h2><table>
<tr><th>Spot</th><th>Status</th><th>Type</th><th>Health</th></tr>
{% for s in spot_rows %}
<tr><td>{{s.name}}</td><td class="{{'busy' if s.busy else 'free'}}">{{'OCCUPIED/RESERVED' if s.busy else 'FREE'}}</td>
<td>{{s.type}}</td><td class="{{'warn' if not s.healthy else 'ok'}}">{{'HEALTHY' if s.healthy else 'UNAVAILABLE'}}</td></tr>
{% endfor %}</table></div>

<div class="card"><h2>Barrier Gates</h2>
{% for g in gate_rows %}
<p><b>{{g.name}}</b> — {{g.state}}
{% if g.broken %}<span class="bad">BROKEN</span>{% endif %}
<form method="post" action="/gate/{{g.name}}/open"><button>Open</button></form>
<form method="post" action="/gate/{{g.name}}/close"><button>Close</button></form></p>
{% endfor %}
<p class="small">Entry={{entry_gate}} | Exit={{exit_gate}}</p>

<h2>CO Zones</h2>
{% for z in zone_rows %}
<p><b>{{z.name}}</b> — <span class="co-{{z.risk|lower}}">{{z.risk}}</span></p>
{% endfor %}

<h2>Exhaust Fans</h2>
{% for f in fan_rows %}
<p class="small"><b>{{f.name}}</b> ({{f.zone}}) —
  {% if f.broken %}<span class="bad">BROKEN</span>
  {% elif f.is_on %}<span class="ok">ON</span>
  {% else %}OFF{% endif %}
</p>
{% endfor %}
</div>

<div class="card"><h2>Cars</h2><table>
<tr><th>Plate</th><th>Status</th><th>Spot</th><th>Payment</th><th>Decision</th><th>Test</th></tr>
{% for c in cars %}
<tr><td>{{c.plate}}</td><td>{{c.status}}</td><td>{{c.assigned_spot or '-'}}</td>
<td>{{c.payment_status}} / {{c.expected_amount}}</td><td class="small">{{c.decision or '-'}}</td>
<td>{% if c.status == 'PARKED' %}<form method="post" action="/car/{{c.plate}}/exit"><button>Send to Exit</button></form>{% endif %}</td></tr>
{% endfor %}</table></div>

<div class="card"><h2>Decision Timeline</h2>
{% for d in decisions %}<p class="small"><b>{{d.created_at}}</b> {{d.plate}} — {{d.action}} — {{d.detail}}</p>{% endfor %}
</div></div></body></html>
"""


# ------------------------------------------------------------------
# Routes
# ------------------------------------------------------------------

def require_login():
    return bool(session.get("user"))


@app.route("/webhook", methods=["POST"])
def webhook():
    data = request.get_json(silent=True) or {}
    print("\n[WEBHOOK]", data)

    if data.get("EventId") and not save_event(data):
        print("[WEBHOOK] Duplicate EventId ignored.")
        return jsonify({"status": "duplicate_ignored"}), 200

    threading.Thread(target=handle_event, args=(data,), daemon=True).start()
    return jsonify({"status": "received"}), 200


@app.route("/login", methods=["GET", "POST"])
def login():
    error = None
    if request.method == "POST":
        username = request.form.get("username", "")
        password = request.form.get("password", "")
        u = USERS.get(username)
        if u and u["password"] == password:
            session["user"] = username
            session["role"] = u["role"]
            return redirect(url_for("dashboard"))
        error = "Invalid login"
    return render_template_string(LOGIN_HTML, error=error)


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


@app.route("/")
def dashboard():
    if not require_login():
        return redirect(url_for("login"))

    with state.state_lock:
        spot_rows = []
        for name in sorted(state.spots.keys(), key=natural_spot_key):
            s = state.spots[name]
            busy = bool(s.get("occupied")) or name in state.reserved_spots
            spot_rows.append({
                "name": name,
                "busy": busy,
                "type": s.get("parkingForCarType", "Any"),
                "healthy": not s.get("broken") and not s.get("isUnderMaintenance")
            })

        gate_rows = [{
            "name": name,
            "state": g.get("state", "?"),
            "broken": g.get("broken", False)
        } for name, g in state.gates.items()]

        zone_rows = [{
            "name": name,
            "risk": z.get("co_risk", "Safe")
        } for name, z in state.zones.items()]

        fan_rows = [{
            "name": name,
            "zone": f.get("zone_parent", "-"),
            "is_on": bool(f.get("is_on")),
            "broken": bool(f.get("broken"))
        } for name, f in state.fans.items()]

    cars      = get_recent_cars()
    decisions = get_recent_decisions()
    total_penalties = get_total_penalties()

    free_count = sum(1 for s in spot_rows if not s["busy"] and s["healthy"])
    busy_count = len(spot_rows) - free_count

    return render_template_string(
        DASH_HTML,
        user=session["user"], role=session["role"],
        spot_rows=spot_rows, gate_rows=gate_rows,
        zone_rows=zone_rows, fan_rows=fan_rows,
        cars=cars, decisions=decisions,
        free_count=free_count, busy_count=busy_count,
        total_penalties=round(total_penalties, 2),
        entry_gate=ENTRY_GATE, exit_gate=EXIT_GATE
    )


@app.route("/sync", methods=["POST"])
def sync_route():
    if not require_login():
        return redirect(url_for("login"))
    try:
        sync_state()
    except Exception as e:
        log_decision("", "SYNC_ERROR", str(e))
    return redirect(url_for("dashboard"))


@app.route("/gate/<name>/<action>", methods=["POST"])
def manual_gate(name, action):
    if not require_login():
        return redirect(url_for("login"))
    try:
        if action == "open":
            open_gate(name)
        elif action == "close":
            close_gate(name)
    except Exception as e:
        log_decision("", "MANUAL_GATE_ERROR", str(e))
    return redirect(url_for("dashboard"))


@app.route("/car/<plate>/exit", methods=["POST"])
def manual_exit(plate):
    if not require_login():
        return redirect(url_for("login"))
    threading.Thread(target=send_to_exit, args=(plate,), daemon=True).start()
    return redirect(url_for("dashboard"))


# ------------------------------------------------------------------
# Startup
# ------------------------------------------------------------------
if __name__ == "__main__":
    init_db()
    load_state_from_db()

    try:
        sim_login()
        sync_state()
    except Exception as e:
        print("[STARTUP] Simulator sync not ready yet:", e)
        print("[STARTUP] This is OK. Load Level 1, then click SYNC on the dashboard.")

    print("\n============================================")
    print(" PARKMIND LEVEL 1")
    print(" Dashboard: http://127.0.0.1:8000")
    print(" Webhook:   http://127.0.0.1:8000/webhook")
    print(" Login:     admin/admin or operator/operator")
    print(f" Entry gate: {ENTRY_GATE}")
    print(f" Exit gate:  {EXIT_GATE}  <-- verify physically")
    print("============================================\n")

    app.run(host="0.0.0.0", port=WEB_PORT, threaded=True)
