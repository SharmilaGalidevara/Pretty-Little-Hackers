from flask import (
    Blueprint,
    render_template_string,
    session,
    redirect,
    flash,
    request,
)
import sqlite3
from pathlib import Path
import os
import json
import requests
from datetime import datetime
from urllib.parse import quote

# ============================================================
# PARKMIND LEVEL 2
# COMPACT MAINTENANCE + MANUAL CONTROL DASHBOARD
# ============================================================

maintenance_bp = Blueprint(
    "maintenance",
    __name__,
    url_prefix="/maintenance"
)

APP_DIR = Path(__file__).resolve().parent

DB_PATH = Path(
    os.getenv(
        "PARKMIND_LEVEL2_DB",
        str(APP_DIR / "parkmind_level2.db")
    )
)

SIM_BASE = os.getenv(
    "PARKMIND_SIM_BASE",
    "http://127.0.0.1:9898/api/v1"
)

SIM_USER = os.getenv("PARKMIND_SIM_USER", "admin")
SIM_PASSWORD = os.getenv("PARKMIND_SIM_PASSWORD", "admin")

TOKEN = None


# ============================================================
# HTML
# ============================================================

HTML = r"""
<!doctype html>
<html>
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">

<title>PARKMIND · Maintenance</title>

<style>

*{
    box-sizing:border-box;
}

body{
    margin:0;
    background:#07111f;
    color:#e5e7eb;
    font-family:Inter,"Segoe UI",Arial,sans-serif;
}

header{
    position:sticky;
    top:0;
    z-index:50;
    min-height:68px;
    padding:10px 22px;
    background:#0b1729;
    border-bottom:1px solid #26364e;

    display:flex;
    align-items:center;
    justify-content:space-between;
    gap:15px;
}

.brand{
    font-size:20px;
    font-weight:900;
    color:#67e8f9;
}

.sub{
    color:#94a3b8;
    font-size:10px;
    margin-top:3px;
}

.header-right{
    display:flex;
    align-items:center;
    gap:8px;
    flex-wrap:wrap;
}

button{
    border:0;
    border-radius:8px;
    padding:8px 11px;
    font-size:10px;
    font-weight:800;
    cursor:pointer;
    color:white;
}

button:hover{
    filter:brightness(1.12);
}

button:disabled{
    opacity:.4;
    cursor:not-allowed;
}

.btn-blue{background:#0284c7}
.btn-green{background:#047857}
.btn-red{background:#b91c1c}
.btn-yellow{background:#a16207}
.btn-gray{background:#475569}
.btn-dark{background:#1e293b}

a{
    color:#67e8f9;
    text-decoration:none;
}

.page{
    max-width:1450px;
    margin:auto;
    padding:16px;
}

/* ============================================================
   STATUS
   ============================================================ */

.statusbar{
    display:grid;
    grid-template-columns:repeat(4,1fr);
    gap:9px;
    margin-bottom:12px;
}

.status{
    background:#101b2d;
    border:1px solid #26364e;
    border-radius:12px;
    padding:12px;
}

.status .number{
    font-size:23px;
    font-weight:900;
}

.status .label{
    color:#94a3b8;
    font-size:9px;
    text-transform:uppercase;
    letter-spacing:.5px;
    margin-top:3px;
}

.good{
    color:#86efac;
}

.warn{
    color:#fbbf24;
}

.bad{
    color:#f87171;
}

.cyan{
    color:#67e8f9;
}

.info{
    color:#7dd3fc;
}

/* ============================================================
   BANNERS
   ============================================================ */

.banner{
    padding:10px 12px;
    border-radius:10px;
    margin-bottom:10px;
    font-size:10px;
    line-height:1.5;
}

.banner.good{
    background:#0d2c23;
    border:1px solid #047857;
}

.banner.bad{
    background:#281116;
    border:1px solid #7f1d1d;
}

.banner.info{
    background:#082f49;
    border:1px solid #0e7490;
}

.flash{
    padding:9px 11px;
    border-radius:8px;
    margin-bottom:9px;
    background:#2a210d;
    border:1px solid #92400e;
    color:#fde68a;
    font-size:10px;
}

/* ============================================================
   MAIN SECTION
   ============================================================ */

.section{
    background:#101b2d;
    border:1px solid #26364e;
    border-radius:14px;
    margin-bottom:11px;
    overflow:hidden;
}

.section-header{
    padding:13px 15px;
    display:flex;
    align-items:center;
    justify-content:space-between;
    cursor:pointer;
    background:#0f1a2c;
}

.section-header:hover{
    background:#132137;
}

.section-title{
    display:flex;
    align-items:center;
    gap:9px;
    font-size:12px;
    font-weight:900;
    text-transform:uppercase;
    letter-spacing:.5px;
}

.section-summary{
    font-size:10px;
    color:#94a3b8;
}

.chevron{
    font-size:14px;
    transition:.2s;
}

.section.open .chevron{
    transform:rotate(180deg);
}

.section-body{
    display:none;
    padding:12px;
    border-top:1px solid #26364e;
}

.section.open .section-body{
    display:block;
}

/* ============================================================
   COMPONENT GRID
   ============================================================ */

.component-grid{
    display:grid;
    grid-template-columns:repeat(auto-fit,minmax(270px,1fr));
    gap:9px;
}

.component{
    background:#0b1525;
    border:1px solid #26364e;
    border-radius:11px;
    overflow:hidden;
}

.component-head{
    padding:11px;
    cursor:pointer;
}

.component-head:hover{
    background:#101d31;
}

.component-top{
    display:flex;
    align-items:center;
    justify-content:space-between;
    gap:8px;
}

.component-name{
    font-size:12px;
    font-weight:900;
}

.component-meta{
    color:#94a3b8;
    font-size:9px;
    margin-top:3px;
}

.component-state{
    margin-top:8px;
    display:flex;
    align-items:center;
    justify-content:space-between;
    gap:5px;
}

.component-details{
    display:none;
    padding:11px;
    border-top:1px solid #26364e;
    background:#091321;
}

.component.open .component-details{
    display:block;
}

.component-arrow{
    color:#64748b;
    transition:.2s;
}

.component.open .component-arrow{
    transform:rotate(180deg);
}

/* ============================================================
   BADGES
   ============================================================ */

.badge{
    display:inline-block;
    padding:3px 7px;
    border-radius:999px;
    background:#1e293b;
    font-size:9px;
    font-weight:900;
}

.badge.good{
    background:#0f2f25;
    color:#86efac;
}

.badge.bad{
    background:#3a171b;
    color:#fca5a5;
}

.badge.warn{
    background:#3b2a10;
    color:#fde68a;
}

.badge.cyan{
    background:#082f49;
    color:#7dd3fc;
}

.badge.gray{
    color:#cbd5e1;
}

/* ============================================================
   DETAILS
   ============================================================ */

.detail-grid{
    display:grid;
    grid-template-columns:1fr 1fr;
    gap:7px;
    margin-bottom:10px;
}

.detail{
    padding:8px;
    border:1px solid #26364e;
    border-radius:7px;
    background:#101b2d;
}

.detail-label{
    font-size:8px;
    color:#64748b;
    text-transform:uppercase;
}

.detail-value{
    margin-top:3px;
    font-size:10px;
    font-weight:800;
}

.reason{
    padding:8px;
    border-radius:7px;
    background:#111c2d;
    border:1px solid #26364e;
    font-size:9px;
    line-height:1.4;
    margin-bottom:8px;
}

/* ============================================================
   CONTROLS
   ============================================================ */

.controls{
    display:flex;
    gap:6px;
    flex-wrap:wrap;
    margin-top:9px;
}

.control-label{
    font-size:8px;
    color:#64748b;
    text-transform:uppercase;
    margin-bottom:5px;
}

.control-group{
    margin-top:9px;
}

.manual-warning{
    padding:7px;
    border-radius:7px;
    font-size:9px;
    background:#281116;
    border:1px solid #7f1d1d;
    color:#fecaca;
}

.manual-info{
    padding:7px;
    border-radius:7px;
    font-size:9px;
    background:#082f49;
    border:1px solid #0e7490;
    color:#bae6fd;
}

/* ============================================================
   CO
   ============================================================ */

.co-grid{
    display:grid;
    grid-template-columns:repeat(auto-fit,minmax(250px,1fr));
    gap:9px;
}

.zone{
    padding:11px;
    border-radius:10px;
    background:#0b1525;
    border:1px solid #26364e;
}

.zone.danger{
    border-color:#7f1d1d;
    background:#281116;
}

.zone-title{
    display:flex;
    justify-content:space-between;
    gap:10px;
}

.co-value{
    font-size:19px;
    font-weight:900;
    margin-top:8px;
}

/* ============================================================
   TABLES
   ============================================================ */

.table-wrap{
    overflow:auto;
}

table{
    width:100%;
    border-collapse:collapse;
    font-size:9px;
}

th,td{
    padding:7px;
    border-bottom:1px solid #26364e;
    text-align:left;
}

th{
    color:#94a3b8;
    font-size:8px;
    text-transform:uppercase;
}

.empty{
    padding:20px;
    text-align:center;
    color:#64748b;
}

/* ============================================================
   MOBILE
   ============================================================ */

@media(max-width:800px){

    header{
        position:relative;
        align-items:flex-start;
        flex-direction:column;
    }

    .statusbar{
        grid-template-columns:1fr 1fr;
    }

    .component-grid{
        grid-template-columns:1fr;
    }

}

</style>
</head>

<body>

<header>

<div>
    <div class="brand">PARKMIND · Maintenance</div>
    <div class="sub">
        Live maintenance · manual controls · safety interlocks
    </div>
</div>

<div class="header-right">

    <span class="sub">
        Simulator:
        <b class="{{'good' if online else 'bad'}}">
            {{'ONLINE' if online else 'OFFLINE'}}
        </b>
    </span>

    <form method="post" action="/maintenance/refresh">
        <button class="btn-gray">
            Refresh
        </button>
    </form>

    <a href="/logout">Logout</a>

</div>

</header>


<div class="page">

{% with messages = get_flashed_messages() %}
    {% for message in messages %}
        <div class="flash">{{message}}</div>
    {% endfor %}
{% endwith %}


<div class="banner {{'good' if online else 'bad'}}">

{% if online %}

<b>Simulator connected.</b>
Last snapshot:
{{last_refresh or 'not recorded'}}

{% else %}

<b>Simulator unavailable.</b>
Start the Level 2 simulator and press Refresh.

{% endif %}

</div>


<!-- ========================================================
     KPI
======================================================== -->

<div class="statusbar">

    <div class="status">
        <div class="number bad">{{stats.broken}}</div>
        <div class="label">Broken</div>
    </div>

    <div class="status">
        <div class="number warn">{{stats.preventive}}</div>
        <div class="label">Maintenance Due</div>
    </div>

    <div class="status">
        <div class="number cyan">{{stats.locked}}</div>
        <div class="label">Locked</div>
    </div>

    <div class="status">
        <div class="number good">{{stats.fans_on}}</div>
        <div class="label">Fans Running</div>
    </div>

</div>


<!-- ========================================================
     FANS
======================================================== -->

<div class="section open">

<div class="section-header" onclick="toggleSection(this.parentElement)">

    <div>
        <div class="section-title">
            🌀 Exhaust Fans
        </div>

        <div class="section-summary">
            {{stats.fan_total}} fans ·
            {{stats.fans_on}} running ·
            {{stats.fans_broken}} broken
        </div>
    </div>

    <div class="chevron">▼</div>

</div>


<div class="section-body">

<div class="component-grid">

{% for c in fans %}

<div class="component">

<div class="component-head"
     onclick="toggleComponent(this.parentElement)">

<div class="component-top">

<div>
    <div class="component-name">{{c.name}}</div>
    <div class="component-meta">
        {{c.zone or 'Unknown zone'}}
    </div>
</div>

<div class="component-arrow">▼</div>

</div>


<div class="component-state">

{% if c.broken %}

<span class="badge bad">BROKEN</span>

{% elif c.locked %}

<span class="badge cyan">MAINTENANCE</span>

{% elif c.is_on %}

<span class="badge good">ON</span>

{% else %}

<span class="badge gray">OFF</span>

{% endif %}


{% if c.co_danger %}

<span class="badge bad">CO RISK</span>

{% endif %}

</div>

</div>


<div class="component-details">

<div class="detail-grid">

<div class="detail">
    <div class="detail-label">Zone</div>
    <div class="detail-value">{{c.zone or '-'}}</div>
</div>

<div class="detail">
    <div class="detail-label">State</div>
    <div class="detail-value">{{'ON' if c.is_on else 'OFF'}}</div>
</div>

<div class="detail">
    <div class="detail-label">CO Risk</div>
    <div class="detail-value">{{c.risk or 'Unknown'}}</div>
</div>

<div class="detail">
    <div class="detail-label">Detected CO</div>
    <div class="detail-value">{{c.co_level if c.co_level is not none else '-'}}</div>
</div>

</div>


{% if c.co_danger %}

<div class="manual-warning">
    ⚠ CO safety mode active.
    Keep ventilation available.
    Manual OFF is blocked while this fan is required for active CO safety.
</div>

{% endif %}


{% if c.locked %}

<div class="manual-warning">
    🔒 This fan is under maintenance.
    Manual operation is disabled until maintenance is complete.
</div>

{% else %}

<div class="control-group">

<div class="control-label">
    Manual fan control
</div>

<div class="controls">

<form method="post"
      action="/maintenance/manual/fan/{{c.name}}/on">

<button
    class="btn-green"
    {% if c.is_on %}disabled{% endif %}>
    ▶ MANUAL ON
</button>

</form>


<form method="post"
      action="/maintenance/manual/fan/{{c.name}}/off">

<button
    class="btn-red"
    {% if not c.is_on or c.co_danger %}disabled{% endif %}>
    ■ MANUAL OFF
</button>

</form>

</div>

</div>

{% endif %}


{% if c.broken %}

<div class="control-group">

<div class="control-label">
    Maintenance
</div>

<form method="post"
      action="/maintenance/repair/fan/{{c.name}}">

<button class="btn-yellow">
    🔧 Repair Fan
</button>

</form>

</div>

{% endif %}

</div>

</div>

{% else %}

<div class="empty">
    No exhaust fans returned by simulator.
</div>

{% endfor %}

</div>

</div>

</div>


<!-- ========================================================
     LIGHTS
======================================================== -->

<div class="section">

<div class="section-header"
     onclick="toggleSection(this.parentElement)">

<div>

<div class="section-title">
    💡 Lights
</div>

<div class="section-summary">
    {{stats.light_total}} lights ·
    {{stats.lights_on}} currently ON
</div>

</div>

<div class="chevron">▼</div>

</div>


<div class="section-body">

<div class="component-grid">

{% for c in lights %}

<div class="component">

<div class="component-head"
     onclick="toggleComponent(this.parentElement)">

<div class="component-top">

<div>

<div class="component-name">
    {{c.name}}
</div>

<div class="component-meta">
    {{c.zone or '-'}}
    {% if c.group_name %}
    · {{c.group_name}}
    {% endif %}
</div>

</div>

<div class="component-arrow">▼</div>

</div>


<div class="component-state">

{% if c.is_on %}

<span class="badge good">ON</span>

{% else %}

<span class="badge gray">OFF</span>

{% endif %}

{% if c.daytime %}

<span class="badge warn">DAYTIME</span>

{% endif %}

</div>

</div>


<div class="component-details">

<div class="detail-grid">

<div class="detail">
<div class="detail-label">Zone</div>
<div class="detail-value">{{c.zone or '-'}}</div>
</div>

<div class="detail">
<div class="detail-label">Group</div>
<div class="detail-value">{{c.group_name or '-'}}</div>
</div>

<div class="detail">
<div class="detail-label">State</div>
<div class="detail-value">{{'ON' if c.is_on else 'OFF'}}</div>
</div>

<div class="detail">
<div class="detail-label">Simulator Time</div>
<div class="detail-value">{{sim_time or '-'}}</div>
</div>

</div>


{% if c.daytime and c.is_on %}

<div class="manual-warning">
    ⚡ Electricity rule:
    this light is ON during simulator daytime.
    Turn it OFF to comply with the operating rule.
</div>

{% else %}

<div class="manual-info">
    Manual ON is permitted only when the simulator is outside
    the configured daytime period.
</div>

{% endif %}


<div class="control-group">

<div class="control-label">
    Manual light control
</div>

<div class="controls">

<form method="post"
      action="/maintenance/manual/light/{{c.name}}/on">

<button
    class="btn-green"
    {% if c.is_on or c.daytime %}disabled{% endif %}>
    💡 ON
</button>

</form>


<form method="post"
      action="/maintenance/manual/light/{{c.name}}/off">

<button
    class="btn-red"
    {% if not c.is_on %}disabled{% endif %}>
    OFF
</button>

</form>

</div>

</div>

</div>

</div>

{% else %}

<div class="empty">
    No lights returned by simulator.
</div>

{% endfor %}

</div>

</div>

</div>


<!-- ========================================================
     GATES
======================================================== -->

<div class="section">

<div class="section-header"
     onclick="toggleSection(this.parentElement)">

<div>

<div class="section-title">
    🚧 Gates
</div>

<div class="section-summary">
    {{stats.gate_total}} gates ·
    {{stats.gates_broken}} broken
</div>

</div>

<div class="chevron">▼</div>

</div>


<div class="section-body">

<div class="component-grid">

{% for c in gates %}

<div class="component">

<div class="component-head"
     onclick="toggleComponent(this.parentElement)">

<div class="component-top">

<div>

<div class="component-name">
    {{c.name}}
</div>

<div class="component-meta">
    {{c.zone or 'Entrance / Exit'}}
</div>

</div>

<div class="component-arrow">▼</div>

</div>


<div class="component-state">

{% if c.broken %}

<span class="badge bad">BROKEN</span>

{% elif c.locked %}

<span class="badge cyan">MAINTENANCE</span>

{% else %}

<span class="badge good">
    {{c.state}}
</span>

{% endif %}

</div>

</div>


<div class="component-details">

<div class="detail-grid">

<div class="detail">
<div class="detail-label">State</div>
<div class="detail-value">{{c.state}}</div>
</div>

<div class="detail">
<div class="detail-label">Zone</div>
<div class="detail-value">{{c.zone or '-'}}</div>
</div>

</div>


{% if c.locked %}

<div class="manual-warning">
    🔒 Gate is under maintenance.
</div>

{% else %}

<div class="control-group">

<div class="control-label">
    Manual gate control
</div>

<div class="controls">

<form method="post"
      action="/maintenance/manual/gate/{{c.name}}/open">

<button class="btn-green"
        {% if c.state|lower == 'open' %}disabled{% endif %}>
    ↑ OPEN
</button>

</form>

<form method="post"
      action="/maintenance/manual/gate/{{c.name}}/close">

<button class="btn-blue"
        {% if c.state|lower == 'closed' %}disabled{% endif %}>
    ↓ CLOSE
</button>

</form>

</div>

</div>

{% endif %}


{% if c.broken %}

<div class="control-group">

<form method="post"
      action="/maintenance/repair/gate/{{c.name}}">

<button class="btn-yellow">
    🔧 Repair Gate
</button>

</form>

</div>

{% endif %}

</div>

</div>

{% else %}

<div class="empty">
    No gates returned by simulator.
</div>

{% endfor %}

</div>

</div>

</div>


<!-- ========================================================
     PARKING SPOTS
======================================================== -->

<div class="section">

<div class="section-header"
     onclick="toggleSection(this.parentElement)">

<div>

<div class="section-title">
    🅿 Parking Spots
</div>

<div class="section-summary">
    {{stats.spot_total}} spots ·
    {{stats.occupied}} occupied ·
    {{stats.spots_broken}} broken
</div>

</div>

<div class="chevron">▼</div>

</div>


<div class="section-body">

<div class="component-grid">

{% for c in spots %}

<div class="component">

<div class="component-head"
     onclick="toggleComponent(this.parentElement)">

<div class="component-top">

<div>

<div class="component-name">
    {{c.name}}
</div>

<div class="component-meta">
    {{c.zone or '-'}}
</div>

</div>

<div class="component-arrow">▼</div>

</div>


<div class="component-state">

{% if c.broken %}

<span class="badge bad">BROKEN</span>

{% elif c.detected_cars > 0 %}

<span class="badge warn">OCCUPIED</span>

{% else %}

<span class="badge good">FREE</span>

{% endif %}

</div>

</div>


<div class="component-details">

<div class="detail-grid">

<div class="detail">
<div class="detail-label">Purpose</div>
<div class="detail-value">{{c.purpose or '-'}}</div>
</div>

<div class="detail">
<div class="detail-label">Detected Cars</div>
<div class="detail-value">{{c.detected_cars}}</div>
</div>

<div class="detail">
<div class="detail-label">Zone</div>
<div class="detail-value">{{c.zone or '-'}}</div>
</div>

<div class="detail">
<div class="detail-label">Maintenance</div>
<div class="detail-value">
{{'LOCKED' if c.locked else 'AVAILABLE'}}
</div>
</div>

</div>


{% if c.detected_cars > 0 %}

<div class="manual-warning">
    🚗 Repair blocked because the simulator currently detects
    {{c.detected_cars}} car(s).
</div>

{% endif %}


{% if c.broken and not c.locked and c.detected_cars == 0 %}

<form method="post"
      action="/maintenance/repair/spot/{{c.name}}">

<button class="btn-yellow">
    🔧 Repair Spot
</button>

</form>

{% endif %}

</div>

</div>

{% else %}

<div class="empty">
    No parking spots returned by simulator.
</div>

{% endfor %}

</div>

</div>

</div>


<!-- ========================================================
     CO SAFETY
======================================================== -->

<div class="section">

<div class="section-header"
     onclick="toggleSection(this.parentElement)">

<div>

<div class="section-title">
    ☣ CO Safety
</div>

<div class="section-summary">
    {{zones|length}} monitored zones
</div>

</div>

<div class="chevron">▼</div>

</div>


<div class="section-body">

<div class="co-grid">

{% for z in zones %}

<div class="zone {{'danger' if z.is_danger else ''}}">

<div class="zone-title">

<b>{{z.name}}</b>

<span class="badge {{'bad' if z.is_danger else 'good'}}">
    {{z.risk}}
</span>

</div>

<div class="co-value">
    CO {{z.co_level if z.co_level is not none else '-'}}
</div>

<div class="component-meta">
    Simulator gasCarbonMonoxideLevel
</div>


<div style="margin-top:10px">

{% for f in z.fans %}

<div style="padding:7px 0;border-bottom:1px solid #26364e">

<div style="display:flex;justify-content:space-between">

<span>{{f.name}}</span>

{% if f.is_on %}

<span class="good">ON</span>

{% else %}

<span>OFF</span>

{% endif %}

</div>

</div>

{% else %}

<div class="component-meta">
    No fan associated with this zone.
</div>

{% endfor %}

</div>

</div>

{% else %}

<div class="empty">
    No zone data available.
</div>

{% endfor %}

</div>

</div>

</div>


<!-- ========================================================
     MAINTENANCE QUEUE
======================================================== -->

<div class="section">

<div class="section-header"
     onclick="toggleSection(this.parentElement)">

<div>

<div class="section-title">
    🔧 Maintenance Queue
</div>

<div class="section-summary">
    {{queue|length}} components require attention
</div>

</div>

<div class="chevron">▼</div>

</div>


<div class="section-body">

<div class="component-grid">

{% for c in queue %}

<div class="component">

<div class="component-head"
     onclick="toggleComponent(this.parentElement)">

<div class="component-top">

<div>

<div class="component-name">
    {{c.name}}
</div>

<div class="component-meta">
    {{c.kind}} · {{c.zone or '-'}}
</div>

</div>

<div class="component-arrow">▼</div>

</div>


<div class="component-state">

{% if c.broken %}

<span class="badge bad">URGENT</span>

{% elif c.alarm_problem %}

<span class="badge warn">PREVENTIVE</span>

{% else %}

<span class="badge cyan">LOCKED</span>

{% endif %}

</div>

</div>


<div class="component-details">

<div class="detail-grid">

<div class="detail">
<div class="detail-label">State</div>
<div class="detail-value">{{c.state}}</div>
</div>

<div class="detail">
<div class="detail-label">Zone</div>
<div class="detail-value">{{c.zone or '-'}}</div>
</div>

</div>


{% if c.alarm_problem %}

<div class="reason">
<b>Predictive signal</b><br>
{{c.alarm_problem}}
</div>

{% endif %}


{% if c.locked %}

<div class="manual-warning">
    🔒 {{c.lock_reason}}
</div>

{% elif c.safe %}

<div class="manual-info">
    ✓ Repair safety interlock passed.
</div>

{% else %}

<div class="manual-warning">
    ⛔ Repair blocked.<br>
    {{c.safe_reason}}
</div>

{% endif %}


{% if c.safe and not c.locked and c.kind in ['spot','gate','fan'] %}

<form method="post"
      action="/maintenance/repair/{{c.kind}}/{{c.name}}">

<button class="btn-yellow">
    🔧
    {{'Repair Broken' if c.broken else 'Preventive Repair'}}
</button>

</form>

{% endif %}

</div>

</div>

{% else %}

<div class="empty">
    No maintenance required.
</div>

{% endfor %}

</div>

</div>

</div>


<!-- ========================================================
     REPAIR HISTORY
======================================================== -->

<div class="section">

<div class="section-header"
     onclick="toggleSection(this.parentElement)">

<div>

<div class="section-title">
    📋 Repair History
</div>

<div class="section-summary">
    Last {{jobs|length}} repair commands
</div>

</div>

<div class="chevron">▼</div>

</div>


<div class="section-body">

<div class="table-wrap">

<table>

<tr>
<th>Component</th>
<th>Type</th>
<th>Started</th>
<th>Status</th>
<th>Completed</th>
</tr>

{% for j in jobs %}

<tr>

<td>
<b>{{j.component}}</b>
<br>
<span class="component-meta">{{j.kind}}</span>
</td>

<td>{{j.repair_type}}</td>

<td>{{j.start_sim_time or j.started_at}}</td>

<td>

<span class="badge
{{'good' if j.status == 'COMPLETED'
else 'bad' if j.status == 'FAILED'
else 'warn'}}">

{{j.status}}

</span>

</td>

<td>{{j.completed_sim_time or '-'}}</td>

</tr>

{% else %}

<tr>
<td colspan="5" class="empty">
No repair history.
</td>
</tr>

{% endfor %}

</table>

</div>

</div>

</div>


<!-- ========================================================
     MANUAL CONTROL AUDIT
======================================================== -->

<div class="section">

<div class="section-header"
     onclick="toggleSection(this.parentElement)">

<div>

<div class="section-title">
    🛡 Manual Operation Audit
</div>

<div class="section-summary">
    Last {{manual_logs|length}} manual commands
</div>

</div>

<div class="chevron">▼</div>

</div>


<div class="section-body">

<div class="table-wrap">

<table>

<tr>
<th>Time</th>
<th>User</th>
<th>Component</th>
<th>Action</th>
<th>Result</th>
</tr>

{% for a in manual_logs %}

<tr>

<td>{{a.created_at}}</td>

<td>{{a.username or '-'}}</td>

<td>{{a.component}}</td>

<td>{{a.action}}</td>

<td>

<span class="badge
{{'good' if a.result == 'SUCCESS' else 'bad'}}">

{{a.result}}

</span>

</td>

</tr>

{% else %}

<tr>
<td colspan="5" class="empty">
No manual operations recorded.
</td>
</tr>

{% endfor %}

</table>

</div>

</div>

</div>


</div>


<script>

function toggleSection(el){
    el.classList.toggle("open");
}

function toggleComponent(el){
    el.classList.toggle("open");
}

</script>

</body>
</html>
"""


# ============================================================
# DATABASE
# ============================================================

def db():
    conn = sqlite3.connect(
        str(DB_PATH),
        timeout=10
    )

    conn.row_factory = sqlite3.Row

    return conn


def table_exists(conn, name):
    return bool(
        conn.execute(
            """
            SELECT 1
            FROM sqlite_master
            WHERE type='table'
            AND name=?
            """,
            (name,)
        ).fetchone()
    )


def column_names(conn, table):
    if not table_exists(conn, table):
        return set()

    return {
        row[1]
        for row in conn.execute(
            f"PRAGMA table_info({table})"
        ).fetchall()
    }


def init_maintenance_tables():

    conn = db()

    conn.executescript("""

    CREATE TABLE IF NOT EXISTS maintenance_api_snapshot(

        name TEXT,
        kind TEXT,
        zone TEXT,
        state TEXT,

        broken INTEGER,
        api_under_maintenance INTEGER,

        detected_cars INTEGER DEFAULT 0,

        purpose TEXT,
        group_name TEXT,

        is_on INTEGER DEFAULT 0,

        raw_json TEXT,
        synced_at TEXT,

        PRIMARY KEY(name,kind)
    );


    CREATE TABLE IF NOT EXISTS maintenance_alarm_snapshot(

        name TEXT PRIMARY KEY,
        problem TEXT,
        raw_json TEXT,
        synced_at TEXT

    );


    CREATE TABLE IF NOT EXISTS maintenance_zone_snapshot(

        name TEXT PRIMARY KEY,

        co_level REAL,
        risk TEXT,

        raw_json TEXT,
        synced_at TEXT

    );


    CREATE TABLE IF NOT EXISTS maintenance_meta(

        key TEXT PRIMARY KEY,
        value TEXT

    );


    CREATE TABLE IF NOT EXISTS maintenance_dashboard_jobs(

        id INTEGER PRIMARY KEY AUTOINCREMENT,

        component TEXT,
        kind TEXT,

        repair_type TEXT,

        started_at TEXT,
        start_sim_time TEXT,

        status TEXT,

        completed_sim_time TEXT,

        detail TEXT

    );


    CREATE TABLE IF NOT EXISTS maintenance_manual_log(

        id INTEGER PRIMARY KEY AUTOINCREMENT,

        created_at TEXT,

        username TEXT,

        role TEXT,

        component TEXT,

        kind TEXT,

        action TEXT,

        result TEXT,

        detail TEXT

    );

    """)

    # Migration for databases created using older version.

    cols = column_names(
        conn,
        "maintenance_api_snapshot"
    )

    if "is_on" not in cols:
        conn.execute(
            """
            ALTER TABLE maintenance_api_snapshot
            ADD COLUMN is_on INTEGER DEFAULT 0
            """
        )

    conn.commit()
    conn.close()


# ============================================================
# META
# ============================================================

def set_meta(key, value):

    conn = db()

    conn.execute(
        """
        INSERT INTO maintenance_meta(key,value)
        VALUES(?,?)

        ON CONFLICT(key)
        DO UPDATE SET value=excluded.value
        """,
        (key, str(value))
    )

    conn.commit()
    conn.close()


def get_meta(key, default=""):

    conn = db()

    row = conn.execute(
        """
        SELECT value
        FROM maintenance_meta
        WHERE key=?
        """,
        (key,)
    ).fetchone()

    conn.close()

    if row:
        return row["value"]

    return default


# ============================================================
# SIMULATOR TIME
# ============================================================

def get_sim_time(conn=None):

    own = False

    if conn is None:
        conn = db()
        own = True

    value = ""

    if table_exists(conn, "system_state"):

        row = conn.execute(
            """
            SELECT value
            FROM system_state
            WHERE key='last_simulator_time'
            """
        ).fetchone()

        if row:
            value = row["value"] or ""

    if own:
        conn.close()

    return value


def is_daytime(sim_time):

    """
    Lights should not operate during simulator daytime.

    If simulator time cannot be parsed, return False rather than
    inventing a daytime state.
    """

    if not sim_time:
        return False

    text = str(sim_time).strip()

    import re

    match = re.search(
        r'(\d{1,2}):(\d{2})',
        text
    )

    if not match:
        return False

    hour = int(match.group(1))

    # PARKMIND electricity rule:
    # daytime = 06:00 through 17:59
    return 6 <= hour < 18


# ============================================================
# SIMULATOR AUTH
# ============================================================

def sim_login():

    global TOKEN

    response = requests.post(
        f"{SIM_BASE}/auth/login",
        json={
            "email": SIM_USER,
            "password": SIM_PASSWORD
        },
        timeout=7
    )

    response.raise_for_status()

    body = response.json()

    TOKEN = (
        body.get("token")
        or body.get("accessToken")
        or body.get("access_token")
    )

    if not TOKEN:
        raise RuntimeError(
            "Simulator authentication returned no token"
        )

    return TOKEN


def sim_request(method, path, **kwargs):

    global TOKEN

    if not TOKEN:
        sim_login()

    headers = kwargs.pop("headers", {})

    headers["Authorization"] = f"Bearer {TOKEN}"

    response = requests.request(
        method,
        f"{SIM_BASE}{path}",
        headers=headers,
        timeout=8,
        **kwargs
    )

    if response.status_code == 401:

        sim_login()

        headers["Authorization"] = f"Bearer {TOKEN}"

        response = requests.request(
            method,
            f"{SIM_BASE}{path}",
            headers=headers,
            timeout=8,
            **kwargs
        )

    response.raise_for_status()

    return response


# ============================================================
# UTILITIES
# ============================================================

def detected_count(value):

    if isinstance(value, list):
        return len(value)

    try:
        return int(value or 0)
    except Exception:
        return 0


def risk_requires_fan(risk):

    return str(risk or "").strip().lower() in {
        "mid",
        "moderate",
        "high",
        "critical",
        "danger",
        "unsafe"
    }


def record_manual(
    component,
    kind,
    action,
    result,
    detail=""
):

    conn = db()

    conn.execute(
        """
        INSERT INTO maintenance_manual_log(
            created_at,
            username,
            role,
            component,
            kind,
            action,
            result,
            detail
        )
        VALUES(?,?,?,?,?,?,?,?)
        """,
        (
            datetime.now().strftime(
                "%Y-%m-%d %H:%M:%S"
            ),

            session.get("user", ""),

            session.get("role", ""),

            component,

            kind,

            action,

            result,

            detail
        )
    )

    conn.commit()
    conn.close()


# ============================================================
# SNAPSHOT
# ============================================================

def refresh_snapshot():

    now = datetime.now().strftime(
        "%Y-%m-%d %H:%M:%S"
    )

    endpoints = {

        "spot":
            "/list-parking-spots",

        "gate":
            "/list-barriers",

        "light":
            "/list-lights",

        "fan":
            "/list-exhaust-fans",
    }

    pulled = {}

    for kind, endpoint in endpoints.items():

        data = sim_request(
            "GET",
            endpoint
        ).json()

        pulled[kind] = (
            data
            if isinstance(data, list)
            else []
        )


    alarms = sim_request(
        "GET",
        "/list-alarms"
    ).json()

    if not isinstance(alarms, list):
        alarms = []


    zones = sim_request(
        "GET",
        "/list-zones"
    ).json()

    if not isinstance(zones, list):
        zones = []


    conn = db()

    conn.execute(
        "DELETE FROM maintenance_api_snapshot"
    )

    conn.execute(
        "DELETE FROM maintenance_alarm_snapshot"
    )

    conn.execute(
        "DELETE FROM maintenance_zone_snapshot"
    )


    for kind, items in pulled.items():

        for item in items:

            name = str(
                item.get("name") or ""
            ).strip()

            if not name:
                continue


            zone = str(
                item.get("zoneParent") or ""
            ).strip()


            purpose = str(
                item.get("purpose") or ""
            ).strip()


            if kind == "light":

                broken = None
                under = None

            else:

                broken = int(
                    bool(
                        item.get(
                            "broken",
                            False
                        )
                    )
                )

                under = int(
                    bool(
                        item.get(
                            "isUnderMaintenance",
                            False
                        )
                    )
                )


            detected = 0

            is_on = False


            if kind == "spot":

                detected = detected_count(
                    item.get("detectedCars")
                )

                if purpose == "Park":

                    state = (
                        "Occupied"
                        if detected > 0
                        else "Free"
                    )

                else:

                    state = (
                        purpose
                        or "Sensor"
                    )


            elif kind == "gate":

                state = str(
                    item.get(
                        "state",
                        "Unknown"
                    )
                )


            elif kind in ("fan", "light"):

                is_on = bool(
                    item.get(
                        "isOn",
                        False
                    )
                )

                state = (
                    "On"
                    if is_on
                    else "Off"
                )

            else:

                state = "Unknown"


            conn.execute(
                """
                INSERT INTO maintenance_api_snapshot(
                    name,
                    kind,
                    zone,
                    state,
                    broken,
                    api_under_maintenance,
                    detected_cars,
                    purpose,
                    group_name,
                    is_on,
                    raw_json,
                    synced_at
                )
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    name,
                    kind,
                    zone,
                    state,
                    broken,
                    under,
                    detected,
                    purpose,
                    str(
                        item.get("group") or ""
                    ),
                    int(is_on),
                    json.dumps(item),
                    now
                )
            )


    for alarm in alarms:

        name = str(
            alarm.get("name")
            or alarm.get("Name")
            or ""
        ).strip()

        if not name:
            continue

        problem = str(
            alarm.get("problem")
            or alarm.get("Problem")
            or "Require Maintenance"
        )

        conn.execute(
            """
            INSERT INTO maintenance_alarm_snapshot(
                name,
                problem,
                raw_json,
                synced_at
            )
            VALUES(?,?,?,?)
            """,
            (
                name,
                problem,
                json.dumps(alarm),
                now
            )
        )


    for zone in zones:

        name = str(
            zone.get("name") or ""
        ).strip()

        if not name:
            continue

        conn.execute(
            """
            INSERT INTO maintenance_zone_snapshot(
                name,
                co_level,
                risk,
                raw_json,
                synced_at
            )
            VALUES(?,?,?,?,?)
            """,
            (
                name,
                zone.get(
                    "gasCarbonMonoxideLevel"
                ),
                str(
                    zone.get(
                        "risk",
                        "Unknown"
                    )
                ),
                json.dumps(zone),
                now
            )
        )


    conn.commit()
    conn.close()

    set_meta(
        "last_refresh",
        now
    )

    set_meta(
        "last_refresh_ok",
        "1"
    )

    return True


# ============================================================
# LIVE COMPONENT
# ============================================================

def live_component(kind, name):

    endpoints = {

        "spot":
            "/list-parking-spots",

        "gate":
            "/list-barriers",

        "fan":
            "/list-exhaust-fans",

        "light":
            "/list-lights",
    }

    endpoint = endpoints.get(kind)

    if not endpoint:
        return None

    items = sim_request(
        "GET",
        endpoint
    ).json()

    if not isinstance(items, list):
        return None

    for item in items:

        if str(
            item.get("name") or ""
        ) == name:

            return item

    return None


def live_zone(zone_name):

    zones = sim_request(
        "GET",
        "/list-zones"
    ).json()

    if not isinstance(zones, list):
        return None

    for zone in zones:

        if str(
            zone.get("name") or ""
        ) == zone_name:

            return zone

    return None


# ============================================================
# MANUAL COMMAND
#
# These are the actual simulator commands.
#
# If your simulator uses different endpoint names,
# set environment variables:
#
# PARKMIND_FAN_ON_ENDPOINT
# PARKMIND_FAN_OFF_ENDPOINT
# PARKMIND_LIGHT_ON_ENDPOINT
# PARKMIND_LIGHT_OFF_ENDPOINT
# PARKMIND_GATE_OPEN_ENDPOINT
# PARKMIND_GATE_CLOSE_ENDPOINT
#
# Supported placeholders:
# {name}
#
# Example:
#
# /exhaust-fans/{name}/on
# ============================================================

def manual_endpoint(kind, action, name):

    encoded = quote(
        name,
        safe=""
    )

    custom_names = {

        ("fan", "on"):
            "PARKMIND_FAN_ON_ENDPOINT",

        ("fan", "off"):
            "PARKMIND_FAN_OFF_ENDPOINT",

        ("light", "on"):
            "PARKMIND_LIGHT_ON_ENDPOINT",

        ("light", "off"):
            "PARKMIND_LIGHT_OFF_ENDPOINT",

        ("gate", "open"):
            "PARKMIND_GATE_OPEN_ENDPOINT",

        ("gate", "close"):
            "PARKMIND_GATE_CLOSE_ENDPOINT",
    }


    env_name = custom_names.get(
        (kind, action)
    )

    if env_name:

        template = os.getenv(
            env_name,
            ""
        ).strip()

        if template:

            return template.replace(
                "{name}",
                encoded
            )


    # Default Level 2 endpoint conventions.
    defaults = {

        ("fan", "on"):
            f"/exhaust-fans/{encoded}/on",

        ("fan", "off"):
            f"/exhaust-fans/{encoded}/off",

        ("light", "on"):
            f"/lights/{encoded}/on",

        ("light", "off"):
            f"/lights/{encoded}/off",

        ("gate", "open"):
            f"/barrier-gates/{encoded}/open",

        ("gate", "close"):
            f"/barrier-gates/{encoded}/close",
    }


    return defaults.get(
        (kind, action)
    )


# ============================================================
# MANUAL CONTROL SAFETY
# ============================================================

def manual_control(
    kind,
    name,
    action
):

    # --------------------------------------------------------
    # GET LIVE COMPONENT
    # --------------------------------------------------------

    item = live_component(
        kind,
        name
    )

    if not item:

        return False, (
            f"{name} was not found "
            "in the live simulator API."
        )


    # --------------------------------------------------------
    # MAINTENANCE LOCK
    # --------------------------------------------------------

    if bool(
        item.get(
            "isUnderMaintenance",
            False
        )
    ):

        return False, (
            f"{name} is currently under "
            "maintenance. Manual operation "
            "is disabled."
        )


    # --------------------------------------------------------
    # FAN
    # --------------------------------------------------------

    if kind == "fan":

        zone = str(
            item.get(
                "zoneParent",
                ""
            )
        )

        is_on = bool(
            item.get(
                "isOn",
                False
            )
        )


        zone_data = live_zone(
            zone
        )


        risk = ""

        if zone_data:

            risk = str(
                zone_data.get(
                    "risk",
                    ""
                )
            )


        # Turning OFF during dangerous CO conditions
        # is blocked.

        if (
            action == "off"
            and risk_requires_fan(risk)
        ):

            return False, (
                f"Manual OFF blocked: "
                f"{zone} has CO risk "
                f"{risk}. Healthy ventilation "
                "must remain available."
            )


        if action == "on" and is_on:

            return False, (
                f"{name} is already ON."
            )


        if action == "off" and not is_on:

            return False, (
                f"{name} is already OFF."
            )


    # --------------------------------------------------------
    # LIGHT
    # --------------------------------------------------------

    if kind == "light":

        is_on = bool(
            item.get(
                "isOn",
                False
            )
        )

        sim_time = get_sim_time()

        daytime = is_daytime(
            sim_time
        )


        if (
            action == "on"
            and daytime
        ):

            return False, (
                "Manual light ON blocked: "
                "lights should not operate "
                "during simulator daytime."
            )


        if action == "on" and is_on:

            return False, (
                f"{name} is already ON."
            )


        if action == "off" and not is_on:

            return False, (
                f"{name} is already OFF."
            )


    # --------------------------------------------------------
    # GATE
    # --------------------------------------------------------

    if kind == "gate":

        state = str(
            item.get(
                "state",
                ""
            )
        ).lower()


        if action == "open" and state == "open":

            return False, (
                f"{name} is already OPEN."
            )


        if action == "close" and state == "closed":

            return False, (
                f"{name} is already CLOSED."
            )


    # --------------------------------------------------------
    # SEND COMMAND
    # --------------------------------------------------------

    endpoint = manual_endpoint(
        kind,
        action,
        name
    )


    if not endpoint:

        return False, (
            f"No manual {action} endpoint "
            f"is configured for {kind}."
        )


    try:

        response = sim_request(
            "POST",
            endpoint
        )

    except requests.HTTPError as exc:

        status = ""

        if exc.response is not None:

            status = (
                f"HTTP {exc.response.status_code}"
            )


        return False, (
            f"Simulator rejected manual "
            f"{action} command for {name}"
            f"{' (' + status + ')' if status else ''}."
        )


    except Exception as exc:

        return False, (
            f"Manual command failed: {exc}"
        )


    return True, (
        f"Simulator accepted "
        f"{action.upper()} command for {name}."
    )


# ============================================================
# SIGNED EVENT RECONCILIATION
# ============================================================

def reconcile_signed_events():

    conn = db()

    if not table_exists(
        conn,
        "events"
    ):

        conn.close()
        return


    cols = column_names(
        conn,
        "events"
    )

    if not {
        "event_class",
        "payload"
    }.issubset(cols):

        conn.close()
        return


    rows = conn.execute(
        """
        SELECT
            event_class,
            server_time,
            payload
        FROM events
        WHERE event_class IN(
            'component_broken',
            'component_fixed'
        )
        ORDER BY id ASC
        """
    ).fetchall()


    latest = {}


    for row in rows:

        try:

            payload = json.loads(
                row["payload"] or "{}"
            )

        except Exception:

            continue


        name = str(
            payload.get("Name")
            or ""
        ).strip()


        if not name:
            continue


        latest[name] = {

            "event_class":
                row["event_class"],

            "server_time":
                row["server_time"]
                or payload.get(
                    "ServerDateTime"
                )
                or "",
        }


    for name, event in latest.items():

        if event["event_class"] == "component_broken":

            conn.execute(
                """
                UPDATE maintenance_api_snapshot
                SET broken=1
                WHERE name=?
                AND broken IS NOT NULL
                """,
                (name,)
            )


        elif event["event_class"] == "component_fixed":

            conn.execute(
                """
                UPDATE maintenance_api_snapshot
                SET
                    broken=0,
                    api_under_maintenance=0
                WHERE name=?
                AND broken IS NOT NULL
                """,
                (name,)
            )


            conn.execute(
                """
                DELETE FROM maintenance_alarm_snapshot
                WHERE name=?
                """,
                (name,)
            )


            conn.execute(
                """
                UPDATE maintenance_dashboard_jobs
                SET
                    status='COMPLETED',
                    completed_sim_time=?
                WHERE component=?
                AND status='COMMAND_SENT'
                """,
                (
                    event["server_time"],
                    name
                )
            )


    conn.commit()
    conn.close()


# ============================================================
# REPAIR SAFETY
# ============================================================

def safe_decision(
    component,
    zones_by_name
):

    if component["locked"]:

        return (
            False,
            component["lock_reason"]
        )


    kind = component["kind"]


    if kind == "spot":

        if component["purpose"] != "Park":

            return (
                False,
                "This is not a normal parking bay."
            )


        if int(
            component["detected_cars"] or 0
        ) > 0:

            return (
                False,
                "Parking spot is occupied."
            )


        return (
            True,
            "Parking spot is free."
        )


    if kind == "gate":

        if component["broken"]:

            return (
                True,
                "Gate is broken."
            )


        if str(
            component["state"]
        ).lower() != "closed":

            return (
                False,
                "Gate must be CLOSED before preventive repair."
            )


        return (
            True,
            "Gate is safely CLOSED."
        )


    if kind == "fan":

        if component["broken"]:

            return (
                True,
                "Fan is broken."
            )


        zone = zones_by_name.get(
            component["zone"]
        )


        if (
            zone
            and
            risk_requires_fan(
                zone["risk"]
            )
        ):

            return (
                False,
                f"CO risk is {zone['risk']}; ventilation must remain available."
            )


        if component["is_on"]:

            return (
                False,
                "Fan must be OFF before preventive repair."
            )


        return (
            True,
            "Fan is OFF and ventilation is not currently required."
        )


    return (
        False,
        "No documented repair operation."
    )


# ============================================================
# ROUTES
# ============================================================

@maintenance_bp.route(
    "/refresh",
    methods=["POST"]
)
def refresh():

    if not session.get("user"):
        return redirect("/login")


    if session.get("role") not in {
        "Maintenance",
        "Admin"
    }:
        return redirect("/")


    try:

        refresh_snapshot()

        flash(
            "Maintenance snapshot refreshed from simulator."
        )

    except Exception as exc:

        set_meta(
            "last_refresh_ok",
            "0"
        )

        flash(
            f"Simulator refresh failed: {exc}"
        )


    return redirect(
        "/maintenance/"
    )


# ============================================================
# MANUAL FAN
# ============================================================

@maintenance_bp.route(
    "/manual/fan/<name>/<action>",
    methods=["POST"]
)
def manual_fan(name, action):

    if not session.get("user"):
        return redirect("/login")


    if session.get("role") not in {
        "Maintenance",
        "Admin"
    }:
        return redirect("/")


    if action not in {
        "on",
        "off"
    }:

        flash(
            "Invalid fan command."
        )

        return redirect(
            "/maintenance/"
        )


    success, detail = manual_control(
        "fan",
        name,
        action
    )


    record_manual(
        name,
        "fan",
        f"MANUAL_{action.upper()}",
        "SUCCESS" if success else "FAILED",
        detail
    )


    if success:

        flash(
            f"🌀 {detail}"
        )

        # Refresh real state immediately.
        try:
            refresh_snapshot()
        except Exception:
            pass

    else:

        flash(
            f"⛔ {detail}"
        )


    return redirect(
        "/maintenance/"
    )


# ============================================================
# MANUAL LIGHT
# ============================================================

@maintenance_bp.route(
    "/manual/light/<name>/<action>",
    methods=["POST"]
)
def manual_light(name, action):

    if not session.get("user"):
        return redirect("/login")


    if session.get("role") not in {
        "Maintenance",
        "Admin"
    }:
        return redirect("/")


    if action not in {
        "on",
        "off"
    }:

        flash(
            "Invalid light command."
        )

        return redirect(
            "/maintenance/"
        )


    success, detail = manual_control(
        "light",
        name,
        action
    )


    record_manual(
        name,
        "light",
        f"MANUAL_{action.upper()}",
        "SUCCESS" if success else "FAILED",
        detail
    )


    if success:

        flash(
            f"💡 {detail}"
        )

        try:
            refresh_snapshot()
        except Exception:
            pass

    else:

        flash(
            f"⛔ {detail}"
        )


    return redirect(
        "/maintenance/"
    )


# ============================================================
# MANUAL GATE
# ============================================================

@maintenance_bp.route(
    "/manual/gate/<name>/<action>",
    methods=["POST"]
)
def manual_gate(name, action):

    if not session.get("user"):
        return redirect("/login")


    if session.get("role") not in {
        "Maintenance",
        "Admin"
    }:
        return redirect("/")


    if action not in {
        "open",
        "close"
    }:

        flash(
            "Invalid gate command."
        )

        return redirect(
            "/maintenance/"
        )


    success, detail = manual_control(
        "gate",
        name,
        action
    )


    record_manual(
        name,
        "gate",
        f"MANUAL_{action.upper()}",
        "SUCCESS" if success else "FAILED",
        detail
    )


    if success:

        flash(
            f"🚧 {detail}"
        )

        try:
            refresh_snapshot()
        except Exception:
            pass

    else:

        flash(
            f"⛔ {detail}"
        )


    return redirect(
        "/maintenance/"
    )


# ============================================================
# REPAIR
# ============================================================

@maintenance_bp.route(
    "/repair/<kind>/<name>",
    methods=["POST"]
)
def repair(kind, name):

    if not session.get("user"):
        return redirect("/login")


    if session.get("role") not in {
        "Maintenance",
        "Admin"
    }:
        return redirect("/")


    if kind not in {
        "spot",
        "gate",
        "fan"
    }:

        flash(
            "Repair blocked: unsupported component."
        )

        return redirect(
            "/maintenance/"
        )


    try:

        item = live_component(
            kind,
            name
        )


        if not item:

            flash(
                f"Repair blocked: {name} was not found."
            )

            return redirect(
                "/maintenance/"
            )


        if bool(
            item.get(
                "isUnderMaintenance",
                False
            )
        ):

            flash(
                f"Repair blocked: {name} is already under maintenance."
            )

            return redirect(
                "/maintenance/"
            )


        broken = bool(
            item.get(
                "broken",
                False
            )
        )


        zone = str(
            item.get(
                "zoneParent",
                ""
            )
        )


        if kind == "spot":

            purpose = str(
                item.get(
                    "purpose",
                    ""
                )
            )

            cars = detected_count(
                item.get(
                    "detectedCars"
                )
            )


            if purpose != "Park":

                flash(
                    f"Repair blocked: {name} is {purpose}."
                )

                return redirect(
                    "/maintenance/"
                )


            if cars > 0:

                flash(
                    f"Repair blocked: {name} has {cars} detected car(s)."
                )

                return redirect(
                    "/maintenance/"
                )


        elif kind == "gate":

            state = str(
                item.get(
                    "state",
                    ""
                )
            )


            if (
                not broken
                and
                state.lower() != "closed"
            ):

                flash(
                    f"Repair blocked: {name} must be CLOSED."
                )

                return redirect(
                    "/maintenance/"
                )


        elif kind == "fan":

            is_on = bool(
                item.get(
                    "isOn",
                    False
                )
            )


            if not broken:

                zone_data = live_zone(
                    zone
                )


                if (
                    zone_data
                    and
                    risk_requires_fan(
                        zone_data.get("risk")
                    )
                ):

                    flash(
                        f"Repair blocked: {zone} requires ventilation."
                    )

                    return redirect(
                        "/maintenance/"
                    )


                if is_on:

                    flash(
                        f"Repair blocked: {name} is ON. Turn it OFF first."
                    )

                    return redirect(
                        "/maintenance/"
                    )


        endpoint = {

            "spot":
                f"/parking-spots/{quote(name, safe='')}/repair",

            "gate":
                f"/barrier-gates/{quote(name, safe='')}/repair",

            "fan":
                f"/exhaust-fans/{quote(name, safe='')}/repair",

        }[kind]


        conn = db()


        alarm = conn.execute(
            """
            SELECT problem
            FROM maintenance_alarm_snapshot
            WHERE name=?
            """,
            (name,)
        ).fetchone()


        sim_time = get_sim_time(
            conn
        )


        repair_type = (
            "CORRECTIVE"
            if broken
            else
            "PREVENTIVE"
            if alarm
            else
            "MANUAL"
        )


        existing = conn.execute(
            """
            SELECT 1
            FROM maintenance_dashboard_jobs
            WHERE component=?
            AND status='COMMAND_SENT'
            """,
            (name,)
        ).fetchone()


        if existing:

            conn.close()

            flash(
                f"Repair already pending for {name}."
            )

            return redirect(
                "/maintenance/"
            )


        conn.close()


        sim_request(
            "POST",
            endpoint
        )


        conn = db()


        conn.execute(
            """
            INSERT INTO maintenance_dashboard_jobs(
                component,
                kind,
                repair_type,
                started_at,
                start_sim_time,
                status,
                detail
            )
            VALUES(?,?,?,?,?,?,?)
            """,
            (
                name,
                kind,
                repair_type,

                datetime.now().strftime(
                    "%Y-%m-%d %H:%M:%S"
                ),

                sim_time or None,

                "COMMAND_SENT",

                "Simulator accepted repair command"
            )
        )


        conn.commit()
        conn.close()


        flash(
            f"🔧 {repair_type.title()} repair command accepted for {name}."
        )


    except Exception as exc:

        flash(
            f"Repair command failed: {exc}"
        )


    return redirect(
        "/maintenance/"
    )


# ============================================================
# DASHBOARD
# ============================================================

@maintenance_bp.route("/")
def dashboard():

    if not session.get("user"):
        return redirect("/login")


    if session.get("role") not in {
        "Maintenance",
        "Admin"
    }:
        return redirect("/")


    init_maintenance_tables()


    conn = db()


    snapshot_count = conn.execute(
        """
        SELECT COUNT(*)
        FROM maintenance_api_snapshot
        """
    ).fetchone()[0]


    conn.close()


    if snapshot_count == 0:

        try:

            refresh_snapshot()

        except Exception:

            set_meta(
                "last_refresh_ok",
                "0"
            )


    reconcile_signed_events()


    conn = db()


    sim_time = get_sim_time(
        conn
    )


    rows = [

        dict(row)

        for row in conn.execute(
            """
            SELECT
                s.*,
                a.problem AS alarm_problem

            FROM maintenance_api_snapshot s

            LEFT JOIN maintenance_alarm_snapshot a
                ON a.name=s.name

            ORDER BY
                s.kind,
                s.zone,
                s.name
            """
        ).fetchall()
    ]


    zones = [

        dict(row)

        for row in conn.execute(
            """
            SELECT *
            FROM maintenance_zone_snapshot
            ORDER BY name
            """
        ).fetchall()
    ]


    zones_by_name = {
        z["name"]: z
        for z in zones
    }


    pending_names = {

        row["component"]

        for row in conn.execute(
            """
            SELECT component
            FROM maintenance_dashboard_jobs
            WHERE status='COMMAND_SENT'
            """
        ).fetchall()
    }


    components = []


    for c in rows:

        c["broken"] = (
            None
            if c["broken"] is None
            else bool(c["broken"])
        )


        c["api_under_maintenance"] = (

            None
            if c["api_under_maintenance"] is None

            else bool(
                c["api_under_maintenance"]
            )
        )


        c["is_on"] = bool(
            c.get("is_on", 0)
        )


        c["locked"] = (

            bool(
                c["api_under_maintenance"]
            )

            or
            c["name"] in pending_names
        )


        if c["api_under_maintenance"]:

            c["lock_reason"] = (
                "Simulator reports component "
                "under maintenance."
            )

        elif c["name"] in pending_names:

            c["lock_reason"] = (
                "Repair command accepted; "
                "waiting for component_fixed."
            )

        else:

            c["lock_reason"] = ""


        zone = zones_by_name.get(
            c["zone"]
        )


        c["co_level"] = (
            zone.get("co_level")
            if zone
            else None
        )


        c["risk"] = (
            zone.get("risk")
            if zone
            else "Unknown"
        )


        c["co_danger"] = (
            c["kind"] == "fan"
            and
            risk_requires_fan(
                c["risk"]
            )
        )


        safe, safe_reason = safe_decision(
            c,
            zones_by_name
        )


        c["safe"] = safe
        c["safe_reason"] = safe_reason


        components.append(c)


    spots = [
        c
        for c in components
        if c["kind"] == "spot"
    ]


    gates = [
        c
        for c in components
        if c["kind"] == "gate"
    ]


    fans = [
        c
        for c in components
        if c["kind"] == "fan"
    ]


    lights = [
        c
        for c in components
        if c["kind"] == "light"
    ]


    current_daytime = is_daytime(
        sim_time
    )


    for light in lights:

        light["daytime"] = current_daytime


    queue = [

        c

        for c in components

        if (
            c["broken"]
            or
            c["alarm_problem"]
            or
            c["locked"]
        )
    ]


    queue.sort(
        key=lambda c: (
            0 if c["broken"]
            else
            1 if c["alarm_problem"]
            else 2,

            c["name"]
        )
    )


    zone_cards = []


    for z in zones:

        z = dict(z)

        z["is_danger"] = (
            risk_requires_fan(
                z["risk"]
            )
        )


        z["fans"] = [

            f

            for f in fans

            if f["zone"] == z["name"]
        ]


        zone_cards.append(z)


    jobs = [

        dict(row)

        for row in conn.execute(
            """
            SELECT *
            FROM maintenance_dashboard_jobs
            ORDER BY id DESC
            LIMIT 30
            """
        ).fetchall()
    ]


    manual_logs = [

        dict(row)

        for row in conn.execute(
            """
            SELECT *
            FROM maintenance_manual_log
            ORDER BY id DESC
            LIMIT 30
            """
        ).fetchall()
    ]


    stats = {

        "broken":
            sum(
                1
                for c in components
                if c["broken"] is True
            ),

        "preventive":
            sum(
                1
                for c in components
                if (
                    c["alarm_problem"]
                    and
                    not c["broken"]
                    and
                    not c["locked"]
                )
            ),

        "locked":
            sum(
                1
                for c in components
                if c["locked"]
            ),

        "fans_on":
            sum(
                1
                for c in fans
                if c["is_on"]
            ),

        "fan_total":
            len(fans),

        "fans_broken":
            sum(
                1
                for c in fans
                if c["broken"]
            ),

        "light_total":
            len(lights),

        "lights_on":
            sum(
                1
                for c in lights
                if c["is_on"]
            ),

        "gate_total":
            len(gates),

        "gates_broken":
            sum(
                1
                for c in gates
                if c["broken"]
            ),

        "spot_total":
            len(spots),

        "occupied":
            sum(
                1
                for c in spots
                if int(
                    c["detected_cars"] or 0
                ) > 0
            ),

        "spots_broken":
            sum(
                1
                for c in spots
                if c["broken"]
            ),
    }


    conn.close()


    return render_template_string(

        HTML,

        online=(
            get_meta(
                "last_refresh_ok",
                "0"
            ) == "1"
        ),

        last_refresh=get_meta(
            "last_refresh",
            ""
        ),

        sim_time=sim_time,

        stats=stats,

        fans=fans,

        lights=lights,

        gates=gates,

        spots=spots,

        zones=zone_cards,

        queue=queue,

        jobs=jobs,

        manual_logs=manual_logs,
    )