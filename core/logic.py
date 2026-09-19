# core/logic.py  — parking decisions, queuing, scheduling, charge calculation
import re
import math
import threading
from datetime import datetime
from core.config import ENTRY_GATE, EXIT_GATE
from core import state
from core.database import (
    log_decision, upsert_car, get_car
)
from core.simulator import (
    open_gate, close_gate, send_car, charge_car, sync_state
)


def natural_spot_key(name):
    m = re.search(r"(\d+)$", name)
    return int(m.group(1)) if m else 999999


def compatible(spot, car_type):
    target = str(spot.get("parkingForCarType", "Any")).lower()
    car = str(car_type or "Normal").lower()
    if target == "any":
        return True
    if "electric" in car and target == "electric":
        return True
    if "accessible" in car and target == "accessible":
        return True
    return False


def choose_spot(car_type):
    candidates = []
    for name, s in state.spots.items():
        if s.get("occupied") or s.get("broken") or s.get("isUnderMaintenance"):
            continue
        if name in state.reserved_spots or not compatible(s, car_type):
            continue
        candidates.append(name)
    candidates.sort(key=natural_spot_key)
    return candidates[0] if candidates else None


def process_entry_queue():
    with state.state_lock:
        if state.entry_active is not None or not state.entry_queue:
            return

        if not state.spots:
            try:
                sync_state()
            except Exception as e:
                print("[SYNC ERROR]", e)
                return

        item = state.entry_queue.pop(0)
        plate = item["plate"]
        car_type = item["car_type"]
        spot = choose_spot(car_type)

        if not spot:
            upsert_car(plate, status="LEFT_FULL")
            log_decision(plate, "NO_SPACE", "No safe compatible parking spot. Sending car away.")
            try:
                send_car(plate, "leavepark")
            except Exception as e:
                log_decision(plate, "ERROR", f"leavepark failed: {e}")
            threading.Timer(0.2, process_entry_queue).start()
            return

        state.reserved_spots.add(spot)
        state.entry_active = {"plate": plate, "spot": spot, "sent": False}
        reason = f"{spot} selected: FREE + HEALTHY + COMPATIBLE"
        upsert_car(plate, assigned_spot=spot, status="ASSIGNED", decision=reason)
        log_decision(plate, "ASSIGN", reason)

        gate_st = state.gates.get(ENTRY_GATE, {}).get("state")
        try:
            if gate_st == "Open":
                state.entry_active["sent"] = True
                send_car(plate, spot)
            else:
                open_gate(ENTRY_GATE)
        except Exception as e:
            log_decision(plate, "ERROR", f"Entry handling failed: {e}")


def schedule_exit(plate, planned_minutes):
    seconds = max(1, int(planned_minutes)) * 60
    log_decision(plate, "TIMER", f"Exit scheduled in {seconds} seconds")
    timer = threading.Timer(seconds, send_to_exit, args=(plate,))
    timer.daemon = True
    timer.start()


def send_to_exit(plate):
    car = get_car(plate)
    if not car or car["status"] not in ("PARKED", "ASSIGNED"):
        return
    try:
        send_car(plate, "exit")
        upsert_car(plate, status="TO_EXIT")
    except Exception as e:
        log_decision(plate, "ERROR", f"Could not send to exit: {e}")


def calculate_charge(plate, exit_time_str):
    car = get_car(plate)
    if not car:
        return (1, 1.0, 0.0)

    parked_str = car.get("parked_time")
    minutes = 1
    try:
        parked = datetime.strptime(parked_str, "%Y-%m-%d %H:%M:%S")
        exit_time = datetime.strptime(exit_time_str, "%Y-%m-%d %H:%M:%S")
        minutes = max(1, math.ceil((exit_time - parked).total_seconds() / 60))
    except Exception:
        minutes = max(1, int(car.get("planned_minutes") or 1))

    parking_cost = float(minutes)
    charging_cost = float(minutes) if "electric" in str(car.get("car_type", "")).lower() else 0.0
    return minutes, parking_cost, charging_cost


def process_exit_queue():
    with state.state_lock:
        if state.exit_active is not None or not state.exit_queue:
            return

        plate = state.exit_queue.pop(0)
        state.exit_active = {"plate": plate, "sent": False}
        gate_st = state.gates.get(EXIT_GATE, {}).get("state")

        try:
            if gate_st == "Open":
                state.exit_active["sent"] = True
                send_car(plate, "leavepark")
            else:
                open_gate(EXIT_GATE)
        except Exception as e:
            log_decision(plate, "ERROR", f"Exit release failed: {e}")
