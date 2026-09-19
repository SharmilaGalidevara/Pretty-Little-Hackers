# core/logic.py  — parking decisions, queuing, scheduling, charge calculation
import re
import math
import threading
from datetime import datetime
from core.config import ENTRY_GATE, EXIT_GATE, GAME_SPEED_MULTIPLIER
from core import state
from core.database import log_decision, upsert_car, get_car
from core.simulator import open_gate, close_gate, send_car, charge_car, sync_state


def natural_spot_key(name):
    m = re.search(r"(\d+)$", name)
    return int(m.group(1)) if m else 999999


def compatible(spot, car_type):
    target = str(spot.get("parkingForCarType", "Any")).lower()
    car    = str(car_type or "Normal").lower()
    if target == "any":
        return True
    if "electric" in car and target == "electric":
        return True
    if "accessible" in car and target == "accessible":
        return True
    return False


def choose_spot(car_type):
    """Return the best available compatible spot name, or None if full."""
    candidates = []
    target_car = str(car_type or "Normal").lower()
    
    for name, s in state.spots.items():
        if s.get("occupied") or s.get("broken") or s.get("isUnderMaintenance"):
            continue
        if name in state.reserved_spots or not compatible(s, car_type):
            continue
        candidates.append((name, s))
        
    if not candidates:
        return None

    # Sort logic: 
    # 1. Exact match (e.g., Electric car -> Electric spot) preferred over 'Any'
    # 2. Then by natural spot key (closest spot)
    def sort_key(item):
        name, s = item
        spot_type = str(s.get("parkingForCarType", "Any")).lower()
        exact_match = (spot_type == target_car) and (spot_type != "any")
        
        # We want exact_match=True to sort before exact_match=False
        return (not exact_match, natural_spot_key(name))

    candidates.sort(key=sort_key)
    return candidates[0][0]


def process_entry_queue():
    """Process next car from entry queue. Can be called from any thread."""
    with state.state_lock:
        if state.entry_active is not None or not state.entry_queue:
            return

        if not state.spots:
            try:
                sync_state()
            except Exception as e:
                print("[SYNC ERROR]", e)
                return

        item     = state.entry_queue.pop(0)
        plate    = item["plate"]
        car_type = item["car_type"]
        spot     = choose_spot(car_type)

        if not spot:
            upsert_car(plate, status="LEFT_FULL")
            log_decision(plate, "NO_SPACE", "No compatible spot. Car turned away.")
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
                # Gate is already open — send car directly and clear active
                # so the next car can be processed without waiting for gate close
                state.entry_active["sent"] = True
                send_car(plate, spot)
                # Schedule next car after a short delay to avoid overwhelming the sim
                threading.Timer(1.5, _clear_entry_active_and_continue, args=(plate,)).start()
            else:
                open_gate(ENTRY_GATE)
        except Exception as e:
            log_decision(plate, "ERROR", f"Entry handling failed: {e}")
            state.entry_active = None
            state.reserved_spots.discard(spot)


def _clear_entry_active_and_continue(plate):
    """Called after car is sent when gate is permanently open."""
    with state.state_lock:
        if state.entry_active and state.entry_active["plate"] == plate:
            state.entry_active = None
    process_entry_queue()



def schedule_exit(plate, planned_minutes):
    """Schedule car to move to exit. Respects GameSpeedMultiplier."""
    real_seconds = max(5, int(planned_minutes * 60 / max(0.1, GAME_SPEED_MULTIPLIER)))
    log_decision(plate, "TIMER",
                 f"Exit in {real_seconds}s (planned={planned_minutes}min, speed={GAME_SPEED_MULTIPLIER}x)")
    timer = threading.Timer(real_seconds, send_to_exit, args=(plate,))
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
    minutes    = 1
    try:
        parked    = datetime.strptime(parked_str, "%Y-%m-%d %H:%M:%S")
        exit_time = datetime.strptime(exit_time_str, "%Y-%m-%d %H:%M:%S")
        
        real_seconds = (exit_time - parked).total_seconds()
        game_seconds = real_seconds * GAME_SPEED_MULTIPLIER
        planned      = float(car.get("planned_minutes") or 1)
        
        # The simulator expects the exact parking duration prorated per minute, rounded to 2 decimal places.
        game_minutes = game_seconds / 60.0
        minutes = max(1.0, round(game_minutes, 2))
    except Exception:
        minutes = max(1.0, float(car.get("planned_minutes") or 1))

    parking_cost  = float(minutes) * 1.0
    charging_cost = float(minutes) * 1.0 if "electric" in str(car.get("car_type", "")).lower() else 0.0
    return minutes, parking_cost, charging_cost


def process_exit_queue():
    with state.state_lock:
        if state.exit_active is not None or not state.exit_queue:
            return

        plate            = state.exit_queue.pop(0)
        state.exit_active = {"plate": plate, "sent": False}
        gate_st          = state.gates.get(EXIT_GATE, {}).get("state")

        try:
            if gate_st == "Open":
                state.exit_active["sent"] = True
                send_car(plate, "leavepark")
            else:
                open_gate(EXIT_GATE)
        except Exception as e:
            log_decision(plate, "ERROR", f"Exit release failed: {e}")

