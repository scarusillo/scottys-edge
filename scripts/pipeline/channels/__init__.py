"""
Channel modules for v26.0 Phase 4.

Each channel is a self-contained pick generator extracted from the per-game
loop body. They are called by `pipeline/per_game.py` and by future orchestrator
code in Phase 6. Each module exposes a single `try_<channel>()` function that
returns a list of picks (and optionally a control flag).
"""
