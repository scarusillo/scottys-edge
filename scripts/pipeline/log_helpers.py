"""
Block-logger helpers for shadow_blocked_picks tracking.

`_log_park_veto` and `_log_divergence_block` write rows to the
`shadow_blocked_picks` table for post-hoc tracking of channel-specific
filter actions (PARK_GATE veto, DIVERGENCE_GATE block).

Extracted from model_engine.py in v26.0 Phase 7.

Re-exported from model_engine for back-compat: `from model_engine import
_log_park_veto, _log_divergence_block` keeps working.
"""
from datetime import datetime


def _log_park_veto(conn, sport, event_id, selection, park_adj, park_ctx):
    """Log a pick vetoed by park gate to shadow_blocked_picks for tracking."""
    try:
        conn.execute("""
            INSERT INTO shadow_blocked_picks (created_at, sport, event_id, selection,
                market_type, line, odds, edge_pct, units, reason)
            VALUES (?, ?, ?, ?, 'TOTAL', NULL, NULL, NULL, NULL, ?)
        """, (datetime.now().isoformat(), sport, event_id, selection,
              f"PARK_GATE ({park_ctx})"))
        conn.commit()
    except Exception:
        pass




def _log_divergence_block(conn, sport, event_id, home, away, model_spread, market_spread, reason_detail):
    """v26.0 Phase 4: thin wrapper kept for backwards compatibility.

    Implementation moved to pipeline.gates.log_divergence_block.
    """
    from pipeline.gates import log_divergence_block
    log_divergence_block(conn, sport, event_id, home, away,
                          model_spread, market_spread, reason_detail)



