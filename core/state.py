# core/state.py  — shared in-memory state (single source of truth for hot data)
import threading

# Live simulator state — write-through cached from DB and simulator sync
spots  = {}   # name -> spot dict
gates  = {}   # name -> gate dict
fans   = {}   # name -> fan dict
zones  = {}   # name -> {co_risk, updated_at}

# Spot reservation set (prevents double-booking between assign and CarIn event)
reserved_spots = set()

# Entry processing: one car at a time through the entry gate
entry_queue  = []   # list of {"plate", "car_type", "planned"}
entry_active = None  # currently being processed: {"plate", "spot", "sent"}

# Exit processing: one car at a time through the exit gate
exit_queue  = []    # list of plates
exit_active = None  # currently being processed: {"plate", "sent"}

# Single reentrant lock protecting all of the above
state_lock = threading.RLock()
