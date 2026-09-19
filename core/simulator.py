# core/simulator.py  — REST API client + simulator state sync
import requests
from urllib.parse import quote
from core.config import SIM_BASE, SIM_USER, SIM_PASSWORD
from core import state
from core.database import log_decision, upsert_fan, upsert_zone

_token = None


def sim_login():
    global _token
    r = requests.post(
        f"{SIM_BASE}/auth/login",
        json={"Email": SIM_USER, "Password": SIM_PASSWORD},
        timeout=5
    )
    r.raise_for_status()
    _token = r.json()["token"]
    print("[API] Logged in to simulator.")


def sim_request(method, path, **kwargs):
    global _token
    if not _token:
        sim_login()

    headers = kwargs.pop("headers", {})
    headers["Authorization"] = f"Bearer {_token}"

    r = requests.request(
        method,
        f"{SIM_BASE}{path}",
        headers=headers,
        timeout=8,
        **kwargs
    )

    if r.status_code == 401:
        sim_login()
        headers["Authorization"] = f"Bearer {_token}"
        r = requests.request(
            method,
            f"{SIM_BASE}{path}",
            headers=headers,
            timeout=8,
            **kwargs
        )

    if r.status_code >= 400:
        print(f"[API ERROR] {method} {path} -> {r.status_code} {r.text}")
        r.raise_for_status()

    return r


def _detected_count(value):
    if isinstance(value, int):
        return value
    if isinstance(value, list):
        return len(value)
    try:
        return int(value or 0)
    except Exception:
        return 0


def sync_state():
    """Fetch latest spots, gates, fans, zones from simulator and persist."""
    with state.state_lock:
        park_data = sim_request("GET", "/list-parking-spots").json()
        barrier_data = sim_request("GET", "/list-barriers").json()

        state.spots.clear()
        for s in park_data:
            if s.get("purpose") == "Park":
                state.spots[s["name"]] = {
                    **s,
                    "occupied": _detected_count(s.get("detectedCars")) > 0
                }

        state.gates.clear()
        for g in barrier_data:
            state.gates[g["name"]] = dict(g)

        # Fans
        try:
            fan_data = sim_request("GET", "/list-exhaust-fans").json()
            for f in fan_data:
                upsert_fan(
                    f["name"],
                    zone_parent=f.get("zoneParent", ""),
                    is_on=f.get("isOn", False),
                    broken=f.get("broken", False)
                )
        except Exception as e:
            print(f"[SYNC] Could not load fans: {e}")

        # Zones / CO
        try:
            zone_data = sim_request("GET", "/list-zones").json()
            for z in zone_data:
                upsert_zone(z["name"], z.get("risk", "Safe"))
        except Exception as e:
            print(f"[SYNC] Could not load zones: {e}")

    print(f"[SYNC] {len(state.spots)} spots, {len(state.gates)} gates, "
          f"{len(state.fans)} fans, {len(state.zones)} zones.")
    log_decision("", "SYNC",
                 f"Loaded {len(state.spots)} spots, {len(state.gates)} gates, "
                 f"{len(state.fans)} fans, {len(state.zones)} zones")


# ------------------------------------------------------------------
# Gate helpers
# ------------------------------------------------------------------

def gate_safe(name):
    g = state.gates.get(name)
    if not g:
        return True
    return not g.get("broken", False) and not g.get("isUnderMaintenance", False)


def open_gate(name):
    if not gate_safe(name):
        log_decision("", "BLOCKED", f"Refused to open {name}: broken/maintenance")
        return False
    sim_request("POST", f"/barrier-gates/{quote(name, safe='')}/open")
    log_decision("", "GATE_OPEN", name)
    return True


def close_gate(name):
    if not gate_safe(name):
        log_decision("", "BLOCKED", f"Refused to close {name}: broken/maintenance")
        return False
    sim_request("POST", f"/barrier-gates/{quote(name, safe='')}/close")
    log_decision("", "GATE_CLOSE", name)
    return True


def send_car(plate, destination):
    plate_path = quote(plate.replace(" ", ""), safe="")
    dest_path = quote(destination, safe="")
    sim_request("POST", f"/car/{plate_path}/goto/{dest_path}")
    log_decision(plate, "CAR_GOTO", destination)


def charge_car(plate, parking_cost, charging_cost):
    plate_path = quote(plate.replace(" ", ""), safe="")
    sim_request(
        "POST",
        f"/car/{plate_path}/charge",
        params={
            "parkingCost": parking_cost,
            "chargingCost": charging_cost
        }
    )
    log_decision(plate, "CHARGE", f"parking={parking_cost}, charging={charging_cost}")
