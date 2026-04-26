"""
cmd_grade post-grade — line-trajectory feature backfill.

After picks are graded, retroactively compute the multi-point line-movement
features (v25.83 Layer 1 SHAPE + v25.84 Layer 2 ORIGINATOR) on bets that
fired without them. Both feature sets are static once a bet is fired so
running them post-hoc is safe and only hits bets from the last 14 days
(older bets had odds rows pruned by retention pre-v25.79 archive).

Updates `bets` columns:
  late_move_share, n_steps, max_overshoot          (Layer 1)
  originator_book, move_breadth, sharp_movers,
  soft_movers, sharp_soft_divergence, move_class   (Layer 2)

Extracted from main.py cmd_grade() in v26.0 Phase 8.
"""
import os
import sqlite3


def backfill_trajectory_features(db):
    """Compute + persist line-trajectory features for recent bets."""
    # ═══ TRAJECTORY BACKFILL — multi-point line-movement features ═══
    # v25.83 (2026-04-23): Layer 1 SHAPE (late_move_share, n_steps, max_overshoot)
    # v25.84 (2026-04-23): Layer 2 ORIGINATOR (move_class, originator_book,
    #   move_breadth, sharp_movers, soft_movers, sharp_soft_divergence)
    # Both run BEFORE retention so odds history is still available.
    # Trajectory + per-book are static once a bet is fired; safe to compute post-hoc.
    try:
        import sys as _sys
        from model_engine import SCRIPTS_DIR as _scripts_dir
        if _scripts_dir not in _sys.path:
            _sys.path.insert(0, _scripts_dir)
        from line_trajectory import compute_trajectory
        from per_book_trajectory import compute_per_book_trajectory, classify_move
        try:
            from archive_db import attach_archive
        except Exception:
            attach_archive = None
        _traj_conn = sqlite3.connect(db, timeout=30)
        if attach_archive is not None:
            try:
                attach_archive(_traj_conn)
            except Exception:
                pass
        _traj_cur = _traj_conn.cursor()
        # Limit to bets from the last 14 days — older bets had odds DELETE'd
        # before v25.79 archive shipped, so trajectory is unrecoverable.
        _traj_cur.execute("""
            SELECT id, created_at, event_id, market_type, side_type, selection
            FROM bets
            WHERE market_type IN ('SPREAD','TOTAL')
              AND opener_move IS NOT NULL
              AND (late_move_share IS NULL OR move_class IS NULL)
              AND DATE(created_at) >= DATE('now', '-14 days')
        """)
        _traj_targets = _traj_cur.fetchall()
        _l1_ok = _l2_ok = 0
        for _bid, _ct, _eid, _mt, _st, _sel in _traj_targets:
            _f = compute_trajectory(_traj_conn, _eid, _mt, _ct,
                                    side_type=_st, selection=_sel)
            if _f:
                _traj_cur.execute(
                    "UPDATE bets SET late_move_share=?, n_steps=?, max_overshoot=? WHERE id=?",
                    (_f['late_move_share'], _f['n_steps'], _f['max_overshoot'], _bid))
                _l1_ok += 1
            _pb = compute_per_book_trajectory(_traj_conn, _eid, _mt, _ct,
                                              side_type=_st, selection=_sel)
            if _pb:
                _cls = classify_move(_pb)
                if _cls:
                    _traj_cur.execute("""
                        UPDATE bets SET originator_book=?, move_breadth=?,
                                        sharp_movers=?, soft_movers=?,
                                        sharp_soft_divergence=?, move_class=?
                        WHERE id=?
                    """, (_cls['originator_book'], _cls['move_breadth'],
                          _cls['sharp_movers'], _cls['soft_movers'],
                          _cls['sharp_soft_divergence'], _cls['classification'], _bid))
                    _l2_ok += 1
        _traj_conn.commit()
        _traj_conn.close()
        if _traj_targets:
            print(f"  📈 Trajectory backfill: L1 {_l1_ok}/{len(_traj_targets)}  "
                  f"L2 {_l2_ok}/{len(_traj_targets)}")
    except Exception as e:
        print(f"  Trajectory backfill: {e}")

