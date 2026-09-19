# core/events.py  — webhook event handler
import threading
from datetime import datetime
from urllib.parse import quote
from core.config import ENTRY_GATE, EXIT_GATE
from core import state
from core.database import (
    log_decision, upsert_car, get_car,
    upsert_zone, upsert_fan, log_penalty
)
from core.simulator import (
    open_gate, close_gate, send_car, charge_car,
    sim_request, set_zone_lights
)
from core.logic import (
    process_entry_queue, process_exit_queue,
    calculate_charge, schedule_exit, send_to_exit
)


def _zone_has_occupied_spots(zone_name):
    """Check if any parking spot in a zone is occupied."""
    return any(
        s.get("occupied")
        for s in state.spots.values()
        if s.get("zoneParent") == zone_name
    )


def handle_event(data):
    event_class = data.get("EventClass")

    try:
        # ── Gate action ──────────────────────────────────────────────────────
        if event_class == "gate_action":
            name   = data.get("Name")
            action = data.get("Action")

            with state.state_lock:
                if name not in state.gates:
                    state.gates[name] = {"name": name}
                state.gates[name]["state"] = action

                if name == ENTRY_GATE and action == "Open":
                    if state.entry_active and not state.entry_active["sent"]:
                        state.entry_active["sent"] = True
                        send_car(state.entry_active["plate"], state.entry_active["spot"])

                if name == ENTRY_GATE and action == "Closed":
                    if state.entry_active is None:
                        threading.Thread(target=process_entry_queue, daemon=True).start()

                if name == EXIT_GATE and action == "Open":
                    if state.exit_active and not state.exit_active["sent"]:
                        state.exit_active["sent"] = True
                        send_car(state.exit_active["plate"], "leavepark")

                if name == EXIT_GATE and action == "Closed":
                    if state.exit_active is None:
                        threading.Thread(target=process_exit_queue, daemon=True).start()

        # ── Car spot movement ────────────────────────────────────────────────
        elif event_class == "car_spot_action":
            plate       = data.get("CarPlateNumber")
            car_type    = data.get("CarType") or "Normal"
            spot_name   = data.get("SpotName")
            spot_type   = data.get("SpotType")
            direction   = data.get("Direction")
            server_time = data.get("ServerDateTime") or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            planned     = int(data.get("PlannedParkingDurationInMinutes") or 0)

            if spot_type == "EntrySpot" and direction == "CarIn":
                upsert_car(
                    plate, car_type=car_type, planned_minutes=planned,
                    entry_time=server_time, status="WAITING"
                )
                log_decision(plate, "ARRIVAL", f"Arrived at {spot_name}; planned {planned} min")

                with state.state_lock:
                    already_queued = any(x["plate"] == plate for x in state.entry_queue)
                    already_active = state.entry_active and state.entry_active["plate"] == plate
                    if not already_queued and not already_active:
                        state.entry_queue.append({"plate": plate, "car_type": car_type, "planned": planned})
                process_entry_queue()

            elif spot_type == "Park":
                with state.state_lock:
                    if spot_name in state.spots:
                        state.spots[spot_name]["occupied"] = (direction == "CarIn")
                    zone = state.spots.get(spot_name, {}).get("zoneParent", "")

                if direction == "CarIn":
                    state.reserved_spots.discard(spot_name)
                    upsert_car(plate, assigned_spot=spot_name, parked_time=server_time, status="PARKED")
                    log_decision(plate, "PARKED", spot_name)

                    # Turn on zone lights when first car parks
                    if zone:
                        threading.Thread(target=set_zone_lights, args=(zone, True), daemon=True).start()

                    planned_minutes = get_car(plate).get("planned_minutes") or planned or 1
                    schedule_exit(plate, planned_minutes)

                    with state.state_lock:
                        if state.entry_active and state.entry_active["plate"] == plate:
                            state.entry_active = None
                            gate_st = state.gates.get(ENTRY_GATE, {}).get("state")
                            if gate_st != "Open":
                                try:
                                    close_gate(ENTRY_GATE)
                                except Exception as e:
                                    log_decision(plate, "ERROR", f"Close entry gate failed: {e}")
                            threading.Timer(0.5, process_entry_queue).start()

                elif direction == "CarOut":
                    log_decision(plate, "LEFT_SPOT", spot_name)
                    # Turn off zone lights if zone is now empty
                    if zone:
                        with state.state_lock:
                            zone_still_busy = _zone_has_occupied_spots(zone)
                        if not zone_still_busy:
                            threading.Thread(target=set_zone_lights, args=(zone, False), daemon=True).start()

            elif spot_type == "ExitSpot" and direction == "CarIn":
                car = get_car(plate)
                if car and car.get("payment_status") == "REQUESTED":
                    return

                minutes, parking_cost, charging_cost = calculate_charge(plate, server_time)
                expected = parking_cost + charging_cost

                upsert_car(
                    plate, exit_arrival_time=server_time, expected_amount=expected,
                    billed_minutes=minutes,
                    payment_status="REQUESTED", status="PAYMENT_PENDING"
                )
                log_decision(plate, "AT_EXIT", f"{minutes} min; total=${expected:.2f}")
                # Charge the car immediately to avoid escaped without paying penalties
                charge_car(plate, parking_cost, charging_cost)

            elif spot_type == "ExitSpot" and direction == "CarOut":
                car = get_car(plate)
                duration = 0
                if car and car.get("entry_time"):
                    try:
                        from datetime import datetime
                        t1 = datetime.strptime(car["entry_time"], "%Y-%m-%d %H:%M:%S")
                        t2 = datetime.strptime(server_time, "%Y-%m-%d %H:%M:%S")
                        duration = (t2 - t1).total_seconds()
                    except Exception:
                        pass
                        
                upsert_car(plate, departure_time=server_time, status="LEFT", real_duration_seconds=duration)
                log_decision(plate, "DEPARTED", f"Car left parking (real_duration: {duration}s)")

                with state.state_lock:
                    if state.exit_active and state.exit_active["plate"] == plate:
                        state.exit_active = None
                    try:
                        close_gate(EXIT_GATE)
                    except Exception as e:
                        log_decision(plate, "ERROR", f"Close exit gate failed: {e}")
                    threading.Timer(1.0, process_exit_queue).start()

        # ── Payment received ─────────────────────────────────────────────────
        elif event_class == "payment_made":
            plate    = data.get("CarPlateNumber")
            received = float(data.get("Amount") or 0)
            car      = get_car(plate)

            if not car:
                log_decision(plate, "PAYMENT_REJECTED", "Unknown car")
                return

            expected = float(car.get("expected_amount") or 0)

            if abs(received - expected) < 0.01:
                upsert_car(plate, payment_status="PAID", status="PAID")
                log_decision(plate, "PAYMENT_OK", f"Expected ${expected:.2f}, got ${received:.2f}")

                with state.state_lock:
                    if plate not in state.exit_queue and not (
                        state.exit_active and state.exit_active["plate"] == plate
                    ):
                        state.exit_queue.append(plate)
                process_exit_queue()
            else:
                upsert_car(plate, payment_status="INVALID")
                log_decision(
                    plate, "PAYMENT_REJECTED",
                    f"Expected ${expected:.2f}, got ${received:.2f}. Gate closed."
                )

        # ── Component broken — mark + auto-repair ────────────────────────────
        elif event_class == "component_broken":
            name = data.get("Name")
            with state.state_lock:
                if name in state.spots:  state.spots[name]["broken"] = True
                if name in state.gates:  state.gates[name]["broken"] = True
                if name in state.fans:   state.fans[name]["broken"]  = True
            if name in state.fans:
                upsert_fan(name, broken=True)
            log_decision("", "COMPONENT_BROKEN", str(name))

            try:
                comp_type = data.get("ComponentType", "").lower()
                if name in state.gates or "gate" in comp_type or "barrier" in comp_type:
                    sim_request("POST", f"/barrier-gates/{quote(name, safe='')}/repair")
                    log_decision("", "AUTO_REPAIR", f"Gate {name} repaired")
                elif name in state.fans or "fan" in comp_type:
                    sim_request("POST", f"/exhaust-fans/{quote(name, safe='')}/repair")
                    log_decision("", "AUTO_REPAIR", f"Fan {name} repaired")
                elif name in state.spots or "spot" in comp_type:
                    sim_request("POST", f"/parking-spots/{quote(name, safe='')}/repair")
                    log_decision("", "AUTO_REPAIR", f"Spot {name} repaired")
                else:
                    for ep in [
                        f"/barrier-gates/{quote(name, safe='')}/repair",
                        f"/exhaust-fans/{quote(name, safe='')}/repair",
                        f"/parking-spots/{quote(name, safe='')}/repair",
                    ]:
                        try:
                            sim_request("POST", ep)
                            log_decision("", "AUTO_REPAIR", f"{name} repaired via {ep}")
                            break
                        except Exception:
                            pass
            except Exception as e:
                log_decision("", "REPAIR_ERROR", f"{name}: {e}")

        # ── Component fixed ──────────────────────────────────────────────────
        elif event_class == "component_fixed":
            name = data.get("Name")
            with state.state_lock:
                if name in state.spots:  state.spots[name]["broken"] = False
                if name in state.gates:  state.gates[name]["broken"] = False
                if name in state.fans:   state.fans[name]["broken"]  = False
            if name in state.fans:
                upsert_fan(name, broken=False)
            log_decision("", "COMPONENT_FIXED", str(name))

        # ── CO / Zone status — auto fan + lighting ───────────────────────────
        elif event_class in ("zone_status", "co_alert"):
            zone_name = data.get("ZoneName") or data.get("Name")
            risk      = data.get("Risk") or data.get("CoRisk", "Safe")
            if zone_name:
                upsert_zone(zone_name, risk)
                log_decision("", "ZONE_CO", f"{zone_name} -> {risk}")

            if risk in ("High", "Moderate"):
                with state.state_lock:
                    zone_fans = [n for n, f in state.fans.items()
                                 if f.get("zone_parent") == zone_name and not f.get("broken")]
                for fan_name in zone_fans:
                    try:
                        sim_request("POST", f"/exhaust-fans/{quote(fan_name, safe='')}/on")
                        upsert_fan(fan_name, is_on=True)
                        log_decision("", "FAN_ON", f"{fan_name} ON — {risk} CO in {zone_name}")
                    except Exception as e:
                        log_decision("", "FAN_ERROR", f"{fan_name}: {e}")

            elif risk == "Safe":
                with state.state_lock:
                    zone_fans = [n for n, f in state.fans.items()
                                 if f.get("zone_parent") == zone_name
                                 and f.get("is_on") and not f.get("broken")]
                for fan_name in zone_fans:
                    try:
                        sim_request("POST", f"/exhaust-fans/{quote(fan_name, safe='')}/off")
                        upsert_fan(fan_name, is_on=False)
                        log_decision("", "FAN_OFF", f"{fan_name} OFF — {zone_name} safe")
                    except Exception as e:
                        log_decision("", "FAN_ERROR", f"{fan_name}: {e}")

        # ── Penalty received ─────────────────────────────────────────────────
        elif event_class == "penalty":
            reason = data.get("Reason", "Unknown")
            fine   = data.get("FineAmount", 0)
            plate  = data.get("CarPlateNumber", "")
            log_penalty(reason, fine, plate)
            log_decision(plate, "PENALTY", f"{reason} | fine=${fine}")

    except Exception as e:
        print("[EVENT ERROR]", e)
        log_decision("", "ERROR", f"{event_class}: {e}")

