"""
Step 6b — Model B shadow tagging (cross-book disagreement analysis).

After picks are merged, tag each with the Model B engine's read so we can
later compare Model A (Walters/Elo edge) decisions against Model B
(cross-book median consensus). Logs to `model_b_shadow` table for
historical tracking. Returns the human-readable shadow report string for
inclusion in the picks email.

Extracted from main.py cmd_run() Step 6b in v26.0 Phase 8.
"""


def tag_with_model_b(conn, all_picks):
    """Tag picks with Model B's view + log to shadow table.

    Mutates `all_picks` to add model_b_agrees / model_b_level / model_b_edge /
    model_b_reason fields.

    Returns: str — the shadow report text for the picks email.
    """
    _model_b_report = ""
    if not all_picks:
        return _model_b_report

    # Step 6b: Model B shadow tagging — cross-book disagreement analysis
    _model_b_report = ""
    try:
        from market_model import tag_picks_with_model_b, generate_shadow_report
        _mb_summary = tag_picks_with_model_b(conn, all_picks)
        _model_b_report = generate_shadow_report(all_picks)
        print(f"  Model B: {_mb_summary['agree']} agree, {_mb_summary['disagree']} disagree, {_mb_summary['unknown']} unknown")
        # Log Model B tags to DB for historical tracking
        try:
            conn.execute("""CREATE TABLE IF NOT EXISTS model_b_shadow (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT, event_id TEXT, selection TEXT, sport TEXT,
                model_a_edge REAL, model_b_agrees INTEGER, model_b_level TEXT,
                model_b_edge REAL, model_b_reason TEXT
            )""")
            from datetime import datetime as _dt
            _now = _dt.now().isoformat()
            for _p in all_picks:
                _mb_val = 1 if _p.get('model_b_agrees') is True else (0 if _p.get('model_b_agrees') is False else -1)
                conn.execute("""INSERT INTO model_b_shadow
                    (created_at, event_id, selection, sport, model_a_edge, model_b_agrees, model_b_level, model_b_edge, model_b_reason)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (_now, _p.get('event_id',''), _p.get('selection','')[:80], _p.get('sport',''),
                     _p.get('edge_pct',0), _mb_val, _p.get('model_b_level',''), _p.get('model_b_edge',0), _p.get('model_b_reason','')[:200]))
            conn.commit()
        except Exception:
            pass
    except Exception as e:
        print(f"  Model B: {e}")

    return _model_b_report
