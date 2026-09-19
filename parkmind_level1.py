"""
parkmind_level1.py — PARKMIND Level 1 Entry Point
Flask app: routes + startup only. All logic lives in core/.
"""
import threading
from flask import Flask, request, jsonify, render_template_string, redirect, url_for, session

from core.config import ENTRY_GATE, EXIT_GATE, WEB_PORT, GAME_SPEED_MULTIPLIER
from core import state
from core.database import (
    init_db, load_state_from_db, save_event,
    log_decision, get_recent_cars, get_recent_decisions, get_total_penalties
)
from core.simulator import sim_login, sync_state, open_gate, close_gate
from core.logic import natural_spot_key, send_to_exit
from core.events import handle_event

app = Flask(__name__)
app.secret_key = "pretty-little-hackers-level1"

USERS = {
    "admin":    {"password": "admin",    "role": "Admin"},
    "operator": {"password": "operator", "role": "Operator"},
}

# ─────────────────────────────────────────────────────────────────────────────
# HTML — full interactive dashboard (JS polls /api/status every 2s)
# ─────────────────────────────────────────────────────────────────────────────
LOGIN_HTML = """<!doctype html>
<html lang="en"><head><meta charset="UTF-8"><title>PARKMIND — Login</title>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:'Inter',sans-serif;background:radial-gradient(ellipse at 60% 40%,#0d1f3c,#050d1a);
  min-height:100vh;display:flex;align-items:center;justify-content:center}
.card{background:rgba(255,255,255,.04);backdrop-filter:blur(20px);border:1px solid rgba(255,255,255,.1);
  border-radius:24px;padding:48px 40px;width:360px;box-shadow:0 24px 64px rgba(0,0,0,.6)}
.logo{font-size:28px;font-weight:700;color:#fff;letter-spacing:-0.5px;margin-bottom:4px}
.logo span{color:#06b6d4}
.sub{color:rgba(255,255,255,.4);font-size:13px;margin-bottom:32px}
label{display:block;color:rgba(255,255,255,.5);font-size:12px;font-weight:500;
  text-transform:uppercase;letter-spacing:.8px;margin-bottom:6px}
input{width:100%;background:rgba(255,255,255,.06);border:1px solid rgba(255,255,255,.1);
  border-radius:10px;padding:12px 14px;color:#fff;font-size:14px;font-family:inherit;
  margin-bottom:16px;outline:none;transition:.2s}
input:focus{border-color:#06b6d4;background:rgba(6,182,212,.08)}
button{width:100%;background:linear-gradient(135deg,#0891b2,#06b6d4);border:none;
  border-radius:10px;padding:13px;color:#fff;font-size:14px;font-weight:600;
  font-family:inherit;cursor:pointer;margin-top:4px;transition:.2s;letter-spacing:.3px}
button:hover{transform:translateY(-1px);box-shadow:0 8px 24px rgba(6,182,212,.4)}
.err{color:#f87171;font-size:13px;margin-bottom:12px;
  background:rgba(248,113,113,.1);padding:10px 12px;border-radius:8px;
  border:1px solid rgba(248,113,113,.2)}
.hint{color:rgba(255,255,255,.25);font-size:12px;text-align:center;margin-top:20px}
</style></head><body>
<div class="card">
  <div class="logo">PARK<span>MIND</span></div>
  <div class="sub">Level 1 Control Centre &mdash; Pretty Little Hackers</div>
  {% if error %}<div class="err">⚠ {{error}}</div>{% endif %}
  <form method="post">
    <label>Username</label>
    <input name="username" placeholder="admin" autocomplete="off" required>
    <label>Password</label>
    <input name="password" type="password" placeholder="••••••" required>
    <button>Sign In &rarr;</button>
  </form>
  <p class="hint">admin / admin &nbsp;·&nbsp; operator / operator</p>
</div>
</body></html>"""

DASH_HTML = """<!doctype html>
<html lang="en"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>PARKMIND — Dashboard</title>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap" rel="stylesheet">
<style>
*{box-sizing:border-box;margin:0;padding:0}
:root{
  --bg:#050d1a;--surface:#0a1628;--card:#0e1e35;--border:rgba(255,255,255,.07);
  --cyan:#06b6d4;--cyan2:#0891b2;--green:#22c55e;--red:#ef4444;--orange:#f97316;
  --yellow:#eab308;--text:#e2e8f0;--muted:rgba(226,232,240,.45);
}
body{font-family:'Inter',sans-serif;background:var(--bg);color:var(--text);min-height:100vh;overflow-x:hidden}

/* ── Header ── */
header{
  display:flex;align-items:center;justify-content:space-between;
  padding:14px 24px;background:var(--surface);
  border-bottom:1px solid var(--border);position:sticky;top:0;z-index:100;
}
.logo{font-size:20px;font-weight:800;letter-spacing:-0.5px}
.logo span{color:var(--cyan)}
.header-stats{display:flex;gap:8px;align-items:center}
.hstat{background:rgba(255,255,255,.05);border:1px solid var(--border);border-radius:10px;
  padding:6px 14px;font-size:13px;font-weight:500}
.hstat b{color:var(--cyan)}
.hstat.penalty b{color:var(--red)}
.hstat.revenue b{color:var(--green)}
.user-area{display:flex;align-items:center;gap:12px;font-size:13px;color:var(--muted)}
.user-area a{color:var(--cyan);text-decoration:none;font-weight:500}
.badge{background:rgba(6,182,212,.15);color:var(--cyan);font-size:11px;font-weight:600;
  padding:3px 8px;border-radius:6px;text-transform:uppercase;letter-spacing:.5px}

/* ── Layout ── */
.layout{display:grid;grid-template-columns:1fr 280px;grid-template-rows:auto auto;gap:16px;padding:18px;max-width:1600px;margin:0 auto}

/* ── Cards ── */
.card{background:var(--card);border:1px solid var(--border);border-radius:16px;padding:18px}
.card h2{font-size:13px;font-weight:600;text-transform:uppercase;letter-spacing:.8px;
  color:var(--muted);margin-bottom:14px;display:flex;align-items:center;gap:6px}
.card h2 i{width:6px;height:6px;border-radius:50%;display:inline-block}
.card h2 i.c{background:var(--cyan)}
.card h2 i.g{background:var(--green)}
.card h2 i.r{background:var(--red)}
.card h2 i.o{background:var(--orange)}
.card h2 i.y{background:var(--yellow)}

/* ── Spot grid ── */
.spot-grid{display:flex;flex-wrap:wrap;gap:6px}
.spot{width:52px;height:52px;border-radius:10px;display:flex;flex-direction:column;
  align-items:center;justify-content:center;font-size:11px;font-weight:600;
  cursor:default;transition:.3s;border:1.5px solid transparent;position:relative}
.spot.free{background:rgba(34,197,94,.12);border-color:rgba(34,197,94,.3);color:var(--green)}
.spot.occupied{background:rgba(239,68,68,.12);border-color:rgba(239,68,68,.3);color:var(--red)}
.spot.reserved{background:rgba(234,179,8,.12);border-color:rgba(234,179,8,.3);color:var(--yellow)}
.spot.broken{background:rgba(100,116,139,.08);border-color:rgba(100,116,139,.2);color:#64748b}
.spot-icon{font-size:16px;line-height:1}
.spot-name{font-size:9px;opacity:.8;margin-top:2px}

/* ── Status badge ── */
.sb{display:inline-flex;align-items:center;gap:4px;padding:3px 8px;border-radius:6px;
  font-size:11px;font-weight:600;text-transform:uppercase;letter-spacing:.4px}
.sb.ok{background:rgba(34,197,94,.12);color:var(--green)}
.sb.bad{background:rgba(239,68,68,.12);color:var(--red)}
.sb.warn{background:rgba(234,179,8,.12);color:var(--yellow)}
.sb.info{background:rgba(6,182,212,.12);color:var(--cyan)}
.sb.idle{background:rgba(100,116,139,.1);color:#94a3b8}
.sb::before{content:'';width:5px;height:5px;border-radius:50%;background:currentColor}

/* ── Gate card ── */
.gate-item{display:flex;align-items:center;gap:12px;padding:10px 0;border-bottom:1px solid var(--border)}
.gate-item:last-child{border-bottom:none}
.gate-name{font-weight:600;font-size:13px;flex:1}
.gate-btns{display:flex;gap:6px}
.btn{padding:5px 12px;border:none;border-radius:7px;font-size:12px;font-weight:600;
  cursor:pointer;font-family:inherit;transition:.15s}
.btn.open{background:rgba(34,197,94,.15);color:var(--green);border:1px solid rgba(34,197,94,.3)}
.btn.open:hover{background:rgba(34,197,94,.25)}
.btn.close{background:rgba(239,68,68,.12);color:var(--red);border:1px solid rgba(239,68,68,.2)}
.btn.close:hover{background:rgba(239,68,68,.2)}
.btn.sync{background:rgba(6,182,212,.12);color:var(--cyan);border:1px solid rgba(6,182,212,.25);padding:7px 16px}
.btn.sync:hover{background:rgba(6,182,212,.2)}
.btn.exit-btn{background:rgba(249,115,22,.12);color:var(--orange);border:1px solid rgba(249,115,22,.25);padding:4px 10px;font-size:11px}
.btn.exit-btn:hover{background:rgba(249,115,22,.22)}

/* ── CO bars ── */
.co-row{display:flex;align-items:center;gap:10px;margin-bottom:10px}
.co-name{font-size:12px;font-weight:600;width:60px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.co-bar-wrap{flex:1;background:rgba(255,255,255,.05);border-radius:20px;height:8px;overflow:hidden}
.co-bar{height:100%;border-radius:20px;transition:width .5s ease,background .5s}
.co-safe .co-bar{background:var(--green);width:15%}
.co-low .co-bar{background:var(--yellow);width:40%}
.co-moderate .co-bar{background:var(--orange);width:70%}
.co-high .co-bar{background:var(--red);width:100%;animation:pulse .8s ease-in-out infinite}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.6}}
.co-label{font-size:10px;font-weight:600;width:55px;text-align:right}
.co-safe .co-label{color:var(--green)}
.co-low .co-label{color:var(--yellow)}
.co-moderate .co-label{color:var(--orange)}
.co-high .co-label{color:var(--red)}

/* ── Fan row ── */
.fan-row{display:flex;align-items:center;gap:8px;padding:7px 0;border-bottom:1px solid var(--border);font-size:12px}
.fan-row:last-child{border-bottom:none}
.fan-icon{font-size:16px}
.fan-on .fan-icon{animation:spin 1.5s linear infinite}
@keyframes spin{to{transform:rotate(360deg)}}
.fan-name{flex:1;font-weight:500}
.fan-zone{color:var(--muted);font-size:11px}

/* ── Cars table ── */
.cars-table{width:100%;border-collapse:collapse;font-size:12px}
.cars-table th{color:var(--muted);font-weight:500;text-transform:uppercase;
  font-size:10px;letter-spacing:.6px;padding:6px 8px;text-align:left;border-bottom:1px solid var(--border)}
.cars-table td{padding:8px 8px;border-bottom:1px solid rgba(255,255,255,.04);vertical-align:middle}
.cars-table tr:last-child td{border-bottom:none}
.plate{font-family:monospace;font-weight:700;font-size:12px;letter-spacing:.5px;color:var(--cyan)}

/* ── Decision log ── */
.log-wrap{max-height:220px;overflow-y:auto;scrollbar-width:thin;scrollbar-color:rgba(255,255,255,.1) transparent}
.log-row{display:flex;gap:10px;padding:6px 0;border-bottom:1px solid rgba(255,255,255,.04);font-size:11px}
.log-row:last-child{border-bottom:none}
.log-time{color:var(--muted);white-space:nowrap;font-variant-numeric:tabular-nums;font-size:10px;padding-top:1px}
.log-plate{color:var(--cyan);font-family:monospace;font-weight:700;width:70px;white-space:nowrap;overflow:hidden}
.log-action{font-weight:600;width:90px;white-space:nowrap;overflow:hidden;font-size:10px}
.log-detail{color:var(--muted);flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}

/* ── Misc ── */
.top-row{grid-column:1/-1;display:flex;gap:12px}
.stat-card{flex:1;background:var(--card);border:1px solid var(--border);border-radius:14px;
  padding:16px 20px;display:flex;align-items:center;gap:14px}
.stat-icon{font-size:28px;line-height:1}
.stat-val{font-size:26px;font-weight:800;line-height:1;margin-bottom:2px}
.stat-lbl{font-size:11px;color:var(--muted);font-weight:500;text-transform:uppercase;letter-spacing:.5px}
.stat-card.green .stat-val{color:var(--green)}
.stat-card.red .stat-val{color:var(--red)}
.stat-card.cyan .stat-val{color:var(--cyan)}
.stat-card.orange .stat-val{color:var(--orange)}

.pulse-dot{width:7px;height:7px;border-radius:50%;background:var(--green);
  animation:blink 1.4s ease-in-out infinite;display:inline-block}
@keyframes blink{0%,100%{opacity:1}50%{opacity:.2}}
.live-badge{display:flex;align-items:center;gap:6px;font-size:11px;color:var(--muted)}

#last-update{font-size:10px;color:var(--muted)}
</style>
</head>
<body>
<header>
  <div style="display:flex;align-items:center;gap:16px">
    <div class="logo">PARK<span>MIND</span></div>
    <div class="live-badge"><span class="pulse-dot"></span> Live</div>
  </div>
  <div class="header-stats">
    <div class="hstat">Speed: <b>{{game_speed}}x</b></div>
    <div class="hstat">Entry: <b>{{entry_gate}}</b></div>
    <div class="hstat">Exit: <b>{{exit_gate}}</b></div>
  </div>
  <div class="user-area">
    <span id="last-update">Updating...</span>
    <span class="badge">{{role}}</span>
    <span>{{user}}</span>
    <a href="/logout">Logout</a>
  </div>
</header>

<div class="layout">
  <!-- ── Top stats row ── -->
  <div class="top-row">
    <div class="stat-card green">
      <div class="stat-icon">🟢</div>
      <div><div class="stat-val" id="s-free">–</div><div class="stat-lbl">Free Spots</div></div>
    </div>
    <div class="stat-card red">
      <div class="stat-icon">🔴</div>
      <div><div class="stat-val" id="s-busy">–</div><div class="stat-lbl">Occupied / Reserved</div></div>
    </div>
    <div class="stat-card cyan">
      <div class="stat-icon">🚗</div>
      <div><div class="stat-val" id="s-cars">–</div><div class="stat-lbl">Cars Tracked</div></div>
    </div>
    <div class="stat-card green">
      <div class="stat-icon">💰</div>
      <div><div class="stat-val" id="s-revenue">$–</div><div class="stat-lbl">Revenue Collected</div></div>
    </div>
    <div class="stat-card red">
      <div class="stat-icon">⚠️</div>
      <div><div class="stat-val" id="s-penalty">$–</div><div class="stat-lbl">Penalties</div></div>
    </div>
    <div class="stat-card cyan">
      <div class="stat-icon">👻</div>
      <div><div class="stat-val" id="s-ghosts">–</div><div class="stat-lbl">Ghost Cars</div></div>
    </div>
  </div>

  <!-- ── Left panel: spots + gates ── -->
  <div>
    <div class="card" style="margin-bottom:14px">
      <h2><i class="g"></i> Parking Spots</h2>
      <div class="spot-grid" id="spot-grid">Loading…</div>
    </div>

    <div class="card" style="margin-bottom:14px">
      <h2><i class="c"></i> Barrier Gates</h2>
      <div id="gate-list">Loading…</div>
      <div style="margin-top:12px">
        <form method="post" action="/sync" style="display:inline">
          <button type="submit" class="btn sync">↻ Sync from Simulator</button>
        </form>
      </div>
    </div>

    <!-- Cars table -->
    <div class="card">
      <h2><i class="c"></i> Active Cars</h2>
      <div style="overflow-x:auto">
        <table class="cars-table">
          <thead><tr>
            <th>Plate</th><th>Status</th><th>Spot</th>
            <th>Payment</th><th>Expected</th><th>Action</th>
          </tr></thead>
          <tbody id="cars-body"><tr><td colspan="6" style="color:var(--muted);text-align:center;padding:20px">Loading…</td></tr></tbody>
        </table>
      </div>
    </div>
  </div>

  <!-- ── Right panel ── -->
  <div>
    <div class="card" style="margin-bottom:14px">
      <h2><i class="o"></i> CO Zones</h2>
      <div id="zone-list">Loading…</div>
    </div>

    <div class="card" style="margin-bottom:14px">
      <h2><i class="y"></i> Exhaust Fans</h2>
      <div id="fan-list">Loading…</div>
    </div>

    <div class="card">
      <h2><i class="c"></i> Decision Log</h2>
      <div class="log-wrap" id="log-list">Loading…</div>
    </div>
  </div>
</div>

<script>
const STATUS_MAP = {
  'WAITING':'info','ASSIGNED':'warn','PARKED':'ok','TO_EXIT':'warn',
  'PAYMENT_PENDING':'warn','PAID':'ok','LEFT':'idle','LEFT_FULL':'bad',
  'PAYMENT_PENDING':'warn','NEW':'idle'
};
const PAY_MAP = {'PAID':'ok','REQUESTED':'warn','INVALID':'bad','NONE':'idle'};

function sb(text, cls){ return `<span class="sb ${cls||'idle'}">${text}</span>`; }

function renderSpots(spots){
  if(!spots.length) return '<span style="color:var(--muted);font-size:12px">No spots loaded — click Sync</span>';
  return spots.map(s=>{
    let cls = s.broken ? 'broken' : s.busy ? 'occupied' : s.reserved ? 'reserved' : 'free';
    let icon = s.broken ? '🔧' : s.busy ? '🚗' : s.reserved ? '⏳' : '✓';
    return `<div class="spot ${cls}" title="${s.name} — ${cls}">
      <span class="spot-icon">${icon}</span>
      <span class="spot-name">${s.name}</span>
    </div>`;
  }).join('');
}

function renderGates(gates){
  if(!gates.length) return '<span style="color:var(--muted);font-size:12px">No gates loaded</span>';
  return gates.map(g=>{
    let stcls = g.state==='Open'?'ok':g.state==='Closed'?'bad':'warn';
    return `<div class="gate-item">
      <div class="gate-name">${g.name}${g.broken?' <span style="color:var(--red);font-size:10px">BROKEN</span>':''}</div>
      ${sb(g.state||'?', stcls)}
      <div class="gate-btns">
        <form method="post" action="/gate/${g.name}/open" style="display:inline">
          <button class="btn open" type="submit">Open</button></form>
        <form method="post" action="/gate/${g.name}/close" style="display:inline">
          <button class="btn close" type="submit">Close</button></form>
      </div>
    </div>`;
  }).join('');
}

function renderZones(zones){
  if(!zones.length) return '<span style="color:var(--muted);font-size:12px">No zones loaded — click Sync</span>';
  return zones.map(z=>{
    let risk = (z.risk||'Safe');
    let riskLow = risk.toLowerCase().replace(' ','');
    return `<div class="co-row co-${riskLow}">
      <span class="co-name">${z.name}</span>
      <div class="co-bar-wrap"><div class="co-bar"></div></div>
      <span class="co-label">${risk}</span>
    </div>`;
  }).join('');
}

function renderFans(fans){
  if(!fans.length) return '<span style="color:var(--muted);font-size:12px">No fans loaded — click Sync</span>';
  return fans.map(f=>{
    let icon = f.broken ? '🔧' : f.is_on ? '🌀' : '💨';
    let stcls = f.broken ? 'bad' : f.is_on ? 'ok' : 'idle';
    let stlbl = f.broken ? 'BROKEN' : f.is_on ? 'ON' : 'OFF';
    return `<div class="fan-row ${f.is_on?'fan-on':''}">
      <span class="fan-icon">${icon}</span>
      <span class="fan-name">${f.name}</span>
      <span class="fan-zone">${f.zone||''}</span>
      ${sb(stlbl, stcls)}
    </div>`;
  }).join('');
}

function renderCars(cars){
  if(!cars.length) return '<tr><td colspan="6" style="color:var(--muted);text-align:center;padding:20px">No cars yet</td></tr>';
  return cars.map(c=>{
    let stcls = STATUS_MAP[c.status]||'idle';
    let paycls = PAY_MAP[c.payment_status]||'idle';
    let action = c.status==='PARKED'
      ? `<form method="post" action="/car/${encodeURIComponent(c.plate)}/exit" style="display:inline">
           <button class="btn exit-btn" type="submit">→ Exit</button></form>`
      : '';
    return `<tr>
      <td><span class="plate">${c.plate}</span></td>
      <td>${sb(c.status, stcls)}</td>
      <td>${c.assigned_spot||'—'}</td>
      <td>${sb(c.payment_status||'NONE', paycls)}</td>
      <td style="color:var(--cyan);font-weight:600">${c.expected_amount?'$'+Number(c.expected_amount).toFixed(2):'—'}</td>
      <td>${action}</td>
    </tr>`;
  }).join('');
}

const ACTION_COLORS = {
  'ARRIVAL':'#06b6d4','PARKED':'#22c55e','DEPARTED':'#94a3b8',
  'CHARGE':'#a78bfa','PAYMENT_OK':'#22c55e','PAYMENT_REJECTED':'#ef4444',
  'PENALTY':'#ef4444','FAN_ON':'#eab308','FAN_OFF':'#94a3b8',
  'AUTO_REPAIR':'#f97316','COMPONENT_BROKEN':'#ef4444','COMPONENT_FIXED':'#22c55e',
  'GATE_OPEN':'#22c55e','GATE_CLOSE':'#ef4444','ZONE_CO':'#f97316',
  'ASSIGN':'#06b6d4','TIMER':'#94a3b8','SYNC':'#94a3b8'
};

function renderLog(decisions){
  if(!decisions.length) return '<span style="color:var(--muted);font-size:12px">No events yet</span>';
  return decisions.map(d=>{
    let col = ACTION_COLORS[d.action] || 'var(--muted)';
    let time = (d.created_at||'').split(' ')[1]||'';
    return `<div class="log-row">
      <span class="log-time">${time}</span>
      <span class="log-plate">${d.plate||''}</span>
      <span class="log-action" style="color:${col}">${d.action}</span>
      <span class="log-detail" title="${d.detail||''}">${d.detail||''}</span>
    </div>`;
  }).join('');
}

async function refresh(){
  try{
    const res = await fetch('/api/status');
    const d   = await res.json();

    // Stats
    document.getElementById('s-free').textContent    = d.stats.free_spots;
    document.getElementById('s-busy').textContent    = d.stats.occupied_spots;
    document.getElementById('s-cars').textContent    = d.stats.total_cars;
    document.getElementById('s-revenue').textContent = '$'+d.stats.total_revenue.toFixed(2);
    document.getElementById('s-penalty').textContent = '$'+d.stats.total_penalties.toFixed(2);
    document.getElementById('s-ghosts').textContent  = d.stats.ghost_cars;
    document.getElementById('last-update').textContent = 'Updated ' + new Date().toLocaleTimeString();

    // Sections
    document.getElementById('spot-grid').innerHTML = renderSpots(d.spots);
    document.getElementById('gate-list').innerHTML = renderGates(d.gates);
    document.getElementById('zone-list').innerHTML = renderZones(d.zones);
    document.getElementById('fan-list').innerHTML  = renderFans(d.fans);
    document.getElementById('cars-body').innerHTML = renderCars(d.cars);
    document.getElementById('log-list').innerHTML  = renderLog(d.decisions);

  }catch(e){
    document.getElementById('last-update').textContent = '⚠ Connection error';
  }
}

refresh();
setInterval(refresh, 2000);
</script>
</body></html>"""


# ─────────────────────────────────────────────────────────────────────────────
# API: status endpoint (polled by dashboard JS)
# ─────────────────────────────────────────────────────────────────────────────
@app.route("/api/status")
def api_status():
    if not session.get("user"):
        return jsonify({"error": "unauthorized"}), 401

    from core.database import db as _db, get_total_penalties
    conn = _db()
    cars_raw = [dict(r) for r in conn.execute(
        "SELECT * FROM cars ORDER BY COALESCE(entry_time,'') DESC LIMIT 60"
    ).fetchall()]
    decisions_raw = [dict(r) for r in conn.execute(
        "SELECT * FROM decisions ORDER BY id DESC LIMIT 40"
    ).fetchall()]
    revenue = conn.execute(
        "SELECT COALESCE(SUM(expected_amount),0) as t FROM cars WHERE payment_status='PAID'"
    ).fetchone()["t"] or 0
    ghost_cars = conn.execute(
        "SELECT count(*) as c FROM cars WHERE status IN ('PAYMENT_PENDING', 'TO_EXIT') AND parked_time < datetime('now', 'localtime', '-5 minutes')"
    ).fetchone()["c"] or 0
    conn.close()

    with state.state_lock:
        spot_rows = []
        for name in sorted(state.spots.keys(), key=natural_spot_key):
            s    = state.spots[name]
            busy = bool(s.get("occupied"))
            rsv  = name in state.reserved_spots
            spot_rows.append({
                "name":    name,
                "busy":    busy,
                "reserved": rsv and not busy,
                "broken":  bool(s.get("broken")) or bool(s.get("isUnderMaintenance")),
                "type":    s.get("parkingForCarType", "Any"),
            })

        gate_rows = [{"name": n, "state": g.get("state","?"), "broken": bool(g.get("broken"))}
                     for n, g in state.gates.items()]
        zone_rows = [{"name": n, "risk": z.get("co_risk","Safe")}
                     for n, z in state.zones.items()]
        fan_rows  = [{"name": n, "zone": f.get("zone_parent",""), "is_on": bool(f.get("is_on")), "broken": bool(f.get("broken"))}
                     for n, f in state.fans.items()]

    free_count = sum(1 for s in spot_rows if not s["busy"] and not s["reserved"] and not s["broken"])
    busy_count = len(spot_rows) - free_count

    return jsonify({
        "spots":     spot_rows,
        "gates":     gate_rows,
        "zones":     zone_rows,
        "fans":      fan_rows,
        "cars":      cars_raw,
        "decisions": decisions_raw,
        "stats": {
            "free_spots":      free_count,
            "occupied_spots":  busy_count,
            "total_cars":      len(cars_raw),
            "total_revenue":   float(revenue),
            "total_penalties": float(get_total_penalties()),
            "ghost_cars":      ghost_cars,
        }
    })


# ─────────────────────────────────────────────────────────────────────────────
# Routes
# ─────────────────────────────────────────────────────────────────────────────

def require_login():
    return bool(session.get("user"))


@app.route("/webhook", methods=["POST"])
def webhook():
    data = request.get_json(silent=True) or {}
    if data.get("EventId") and not save_event(data):
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
        error = "Invalid credentials"
    return render_template_string(LOGIN_HTML, error=error)


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


@app.route("/")
def dashboard():
    if not require_login():
        return redirect(url_for("login"))
    return render_template_string(
        DASH_HTML,
        user=session["user"], role=session["role"],
        entry_gate=ENTRY_GATE, exit_gate=EXIT_GATE,
        game_speed=GAME_SPEED_MULTIPLIER
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


# ─────────────────────────────────────────────────────────────────────────────
# Startup
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    init_db()
    load_state_from_db()

    try:
        sim_login()
        sync_state()
    except Exception as e:
        print("[STARTUP] Simulator not ready:", e)
        print("[STARTUP] This is OK — click SYNC on the dashboard once simulator is running.")

    print("\n============================================")
    print(" PARKMIND LEVEL 1")
    print(f" Dashboard: http://127.0.0.1:{WEB_PORT}")
    print(f" Webhook:   http://127.0.0.1:{WEB_PORT}/webhook")
    print(" Login:     admin/admin  or  operator/operator")
    print(f" Entry gate: {ENTRY_GATE}  |  Exit gate: {EXIT_GATE}")
    print(f" Game speed: {GAME_SPEED_MULTIPLIER}x")
    print("============================================\n")

    app.run(host="0.0.0.0", port=WEB_PORT, threaded=True)
