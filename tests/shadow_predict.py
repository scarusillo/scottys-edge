"""
Shadow harness for the generate_predictions() pipeline refactor (v26.0 Phase 0).

Purpose: protect the refactor with golden-output regression tests. The current
generate_predictions() function in model_engine.py (lines 1362-3551, ~2,190
lines) is being decomposed into pipeline stages. Every phase of the refactor
must produce byte-equivalent picks vs. a captured baseline on the same input.

Three capabilities:

    capture_golden(conn, sport, label) -> path
        Runs the live generate_predictions() once and saves the output to
        tests/golden/{label}/{sport}.json. Stable ordering, normalized.

    replay_and_compare(conn, sport, label) -> DiffReport
        Runs generate_predictions() against the same conn, loads the golden,
        compares picks. Returns DiffReport (added, removed, changed).

    picks_equivalent(a, b) -> bool
        Semantic comparison ignoring noise (timestamps, db ids,
        sub-cent floating point). Compares: selection, line, odds, book,
        units, edge%, side_type, market_tier, market_type, sport.

Usage:

    # Capture current production behavior for all sports:
    PYTHONIOENCODING=utf-8 python tests/shadow_predict.py capture --label baseline

    # After a refactor phase, replay and confirm zero diff:
    PYTHONIOENCODING=utf-8 python tests/shadow_predict.py replay --label baseline

    # Determinism check (runs twice, expects identical):
    PYTHONIOENCODING=utf-8 python tests/shadow_predict.py determinism

This is the safety net for v26.0. If golden capture itself is unstable
(replay differs from baseline on the same DB), the function has hidden
non-determinism that must be addressed before refactor begins.
"""

import os
import shutil
import sys
import json
import sqlite3
import datetime as _dt_module
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Tuple, Optional, Any
from contextlib import contextmanager

# Wire up imports so we can call into scripts/
HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
SCRIPTS = ROOT / 'scripts'
GOLDEN_ROOT = HERE / 'golden'
GOLDEN_ROOT.mkdir(parents=True, exist_ok=True)
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

DEFAULT_DB = ROOT / 'data' / 'betting_model.db'

# Fields that count as the "identity" of a pick for equivalence
IDENTITY_FIELDS = (
    'sport', 'event_id', 'market_type', 'selection', 'book',
)

# Fields that count as the "value" of a pick for equivalence (rounded)
VALUE_FIELDS = (
    'line', 'odds', 'units', 'edge_pct', 'side_type',
    'market_tier', 'confidence',
)

# Fields ignored for equivalence (timestamps, ids, derived display)
IGNORED_FIELDS = {
    'created_at', 'id', 'graded_at', 'bet_id', 'profit', 'result', 'clv',
    'closing_line', 'closing_odds', 'clv_line', 'clv_odds_pct',
    # Display-only / informational
    'home', 'away', 'commence_time',
}


def _normalize_pick(pick: Dict[str, Any]) -> Dict[str, Any]:
    """Project a pick dict onto comparable fields with stable rounding."""
    out = {}
    for k in IDENTITY_FIELDS:
        v = pick.get(k)
        out[k] = '' if v is None else str(v).strip()
    for k in VALUE_FIELDS:
        v = pick.get(k)
        if v is None:
            out[k] = None
        elif isinstance(v, float):
            out[k] = round(v, 2)
        else:
            out[k] = v
    # Context factors: keep as-is but newline-normalize (engine sometimes uses |)
    ctx = pick.get('context_factors') or ''
    out['context_factors'] = ctx.strip() if isinstance(ctx, str) else ''
    return out


def _pick_key(pick: Dict[str, Any]) -> Tuple:
    """Stable dict key for matching picks across runs."""
    n = _normalize_pick(pick)
    return tuple(n[k] for k in IDENTITY_FIELDS)


def serialize_picks(picks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Normalize and sort picks for stable JSON dump."""
    normalized = [_normalize_pick(p) for p in picks]
    normalized.sort(key=lambda p: tuple(p.get(k, '') for k in IDENTITY_FIELDS))
    return normalized


def picks_equivalent(a: List[Dict], b: List[Dict]) -> Tuple[bool, Dict[str, Any]]:
    """
    Semantic equality on two pick lists.

    Returns (equal, report) where report has added/removed/changed lists.
    """
    by_key_a = {_pick_key(p): _normalize_pick(p) for p in a}
    by_key_b = {_pick_key(p): _normalize_pick(p) for p in b}

    keys_a = set(by_key_a)
    keys_b = set(by_key_b)

    only_in_a = sorted(keys_a - keys_b)  # in golden, not in replay = removed
    only_in_b = sorted(keys_b - keys_a)  # in replay, not in golden = added
    changed = []
    for k in sorted(keys_a & keys_b):
        if by_key_a[k] != by_key_b[k]:
            diffs = {
                f: (by_key_a[k].get(f), by_key_b[k].get(f))
                for f in set(by_key_a[k]) | set(by_key_b[k])
                if by_key_a[k].get(f) != by_key_b[k].get(f)
            }
            changed.append({'key': k, 'diffs': diffs})

    equal = not only_in_a and not only_in_b and not changed
    return equal, {
        'a_count': len(a),
        'b_count': len(b),
        'removed': [{'key': k, 'pick': by_key_a[k]} for k in only_in_a],
        'added':   [{'key': k, 'pick': by_key_b[k]} for k in only_in_b],
        'changed': changed,
    }


def _all_active_sports(conn: sqlite3.Connection) -> List[str]:
    """Return distinct sports with games in market_consensus today + tomorrow."""
    rows = conn.execute(
        """
        SELECT DISTINCT sport FROM market_consensus
        WHERE DATE(commence_time) >= DATE('now')
          AND DATE(commence_time) <= DATE('now', '+1 day')
        ORDER BY sport
        """
    ).fetchall()
    return [r[0] for r in rows]


def _make_frozen_datetime(frozen):
    """Build a datetime subclass whose .now() / .utcnow() return `frozen`."""
    class FrozenDatetime(_dt_module.datetime):
        @classmethod
        def now(cls, tz=None):
            if tz is None:
                return frozen.replace(tzinfo=None) if frozen.tzinfo else frozen
            if frozen.tzinfo is None:
                from datetime import timezone as _tz
                return frozen.replace(tzinfo=_tz.utc).astimezone(tz)
            return frozen.astimezone(tz)

        @classmethod
        def utcnow(cls):
            if frozen.tzinfo is None:
                return frozen
            from datetime import timezone as _tz
            return frozen.astimezone(_tz.utc).replace(tzinfo=None)
    return FrozenDatetime


_FROZEN_PATCH_MODULES = (
    'model_engine', 'main',
    'pipeline.score_helpers', 'pipeline.gates', 'pipeline.stage_1_fetch',
)


@contextmanager
def _freeze_wall_clock(frozen_iso):
    """Monkey-patch module-level `datetime` in pipeline-relevant modules so
    `datetime.now()` returns `frozen_iso`. Restores on exit.

    Local-scope `from datetime import datetime` calls inside individual
    functions are NOT patched — their bindings happen at call time and we
    can't intercept them. Those are typically used for shadow_blocked_picks
    timestamps (do not affect pick output) or scoreboard logging.
    """
    if frozen_iso is None:
        yield
        return
    if isinstance(frozen_iso, str):
        # Accept ISO with or without TZ
        try:
            frozen = _dt_module.datetime.fromisoformat(frozen_iso)
        except ValueError:
            frozen = _dt_module.datetime.fromisoformat(frozen_iso.replace('Z', '+00:00'))
    else:
        frozen = frozen_iso

    Frozen = _make_frozen_datetime(frozen)
    saved = {}
    for mod_path in _FROZEN_PATCH_MODULES:
        try:
            mod = __import__(mod_path, fromlist=[''])
        except ImportError:
            continue
        if hasattr(mod, 'datetime'):
            saved[mod_path] = mod.datetime
            mod.datetime = Frozen
    try:
        yield
    finally:
        for mod_path, original in saved.items():
            try:
                mod = __import__(mod_path, fromlist=[''])
                mod.datetime = original
            except ImportError:
                pass


def _snapshot_db(src: Path, dst: Path) -> Path:
    """
    Copy the live DB to a frozen snapshot so old + new pipelines run against
    identical input. Critical for deterministic replay — without this, the
    hourly pipeline can update odds/market_consensus mid-test and produce
    spurious diffs.

    Uses sqlite3 backup() API (handles in-flight writes safely) when possible,
    falls back to file copy.
    """
    dst.parent.mkdir(parents=True, exist_ok=True)
    try:
        src_conn = sqlite3.connect(str(src))
        dst_conn = sqlite3.connect(str(dst))
        with dst_conn:
            src_conn.backup(dst_conn)
        src_conn.close(); dst_conn.close()
    except Exception:
        shutil.copy2(str(src), str(dst))
    return dst


def capture_golden(label: str, db_path: Optional[Path] = None,
                   sport: Optional[str] = None,
                   snapshot: bool = True) -> Path:
    """
    Run generate_predictions() and save its output to tests/golden/{label}/{sport}.json.

    Args:
        label: directory name under tests/golden/ (e.g. 'baseline', 'phase1').
        db_path: defaults to data/betting_model.db.
        sport: capture this sport only. If None, all active sports today.
        snapshot: if True (default), copy the DB to a frozen snapshot under
                  tests/golden/{label}/_snapshot.db and run against THAT.
                  Replay against the same label uses the same snapshot, so
                  it tests the function's behavior, not ambient DB drift.

    Returns:
        Path to the captured golden directory.
    """
    db_path = db_path or DEFAULT_DB
    out_dir = GOLDEN_ROOT / label
    out_dir.mkdir(parents=True, exist_ok=True)

    # Freeze DB state for reproducible replay
    if snapshot:
        snap_path = out_dir / '_snapshot.db'
        _snapshot_db(db_path, snap_path)
        active_db = snap_path
        print(f"  DB snapshot: {snap_path} ({snap_path.stat().st_size//1024//1024} MB)")
    else:
        active_db = db_path

    conn = sqlite3.connect(str(active_db))
    try:
        from model_engine import generate_predictions
    except Exception as e:
        raise RuntimeError(f"Could not import generate_predictions: {e}")

    if sport:
        sports = [sport]
    else:
        sports = _all_active_sports(conn)

    captured_at = datetime.now()  # frozen for replay
    print(f"  Capturing {len(sports)} sport(s) → {out_dir}")
    print(f"  Wall clock: {captured_at.isoformat()}")
    summary = {
        'label': label,
        'db_source': str(db_path),
        'db_used': str(active_db),
        'snapshotted': snapshot,
        'captured_at': captured_at.isoformat(),
        'wall_clock_frozen_at': captured_at.isoformat(),
        'sports': {},
    }
    # Accumulator for the post-merge pipeline capture
    all_game_picks = []

    # Wrap all capture work in the freeze context. Note: even the FIRST capture
    # uses the freeze, which means generate_predictions sees a stable now()
    # equal to wall-clock-at-call. Subsequent replays see the same frozen value.
    _freeze_ctx = _freeze_wall_clock(captured_at)
    _freeze_ctx.__enter__()
    for sp in sports:
        print(f"    {sp}...", end=' ', flush=True)
        try:
            picks = generate_predictions(conn, sport=sp)
        except Exception as e:
            print(f"FAILED ({e})")
            summary['sports'][sp] = {'error': str(e), 'count': 0}
            continue
        normalized = serialize_picks(picks or [])
        out_path = out_dir / f"{sp}.json"
        with open(out_path, 'w', encoding='utf-8') as f:
            json.dump(normalized, f, indent=2, default=str, sort_keys=True)
        print(f"{len(normalized)} picks")
        summary['sports'][sp] = {'count': len(normalized), 'path': str(out_path)}
        all_game_picks.extend(picks or [])

    # v26.0 Phase 2.5: also capture POST-MERGE output so gates in
    # main._merge_and_select._passes_filter become harness-testable.
    # Props are passed empty — merge filter applies gates to game-line picks
    # the same way regardless. Future expansion can include real props.
    try:
        from main import _merge_and_select
        merged = _merge_and_select(all_game_picks, [], conn=conn)
        merged_normalized = serialize_picks(merged or [])
        merged_path = out_dir / '_merged.json'
        with open(merged_path, 'w', encoding='utf-8') as f:
            json.dump(merged_normalized, f, indent=2, default=str, sort_keys=True)
        print(f"  Post-merge: {len(merged_normalized)} picks → {merged_path.name}")
        summary['post_merge'] = {'count': len(merged_normalized), 'path': str(merged_path)}
    except Exception as e:
        print(f"  Post-merge capture FAILED: {e}")
        summary['post_merge'] = {'error': str(e)}

    summary_path = out_dir / '_summary.json'
    with open(summary_path, 'w', encoding='utf-8') as f:
        json.dump(summary, f, indent=2)

    _freeze_ctx.__exit__(None, None, None)
    conn.close()
    return out_dir


def replay_and_compare(label: str, db_path: Optional[Path] = None,
                       sport: Optional[str] = None,
                       use_snapshot: bool = True) -> Dict[str, Any]:
    """
    Run generate_predictions() and compare against a saved golden.

    Args:
        use_snapshot: if True (default) and the label has a _snapshot.db,
                      replay against that frozen DB. Required for deterministic
                      regression checks during the refactor.
    """
    db_path = db_path or DEFAULT_DB
    in_dir = GOLDEN_ROOT / label
    if not in_dir.exists():
        raise FileNotFoundError(f"No golden snapshot at {in_dir}")

    snap_path = in_dir / '_snapshot.db'
    if use_snapshot and snap_path.exists():
        active_db = snap_path
        print(f"  Replaying against snapshot: {snap_path}")
    else:
        active_db = db_path
        if use_snapshot:
            print(f"  WARNING: no snapshot found, replaying against live DB — may show ambient drift")

    # Read the frozen wall-clock time from the golden's summary (v26.0 harness)
    frozen_iso = None
    summary_path = in_dir / '_summary.json'
    if summary_path.exists():
        try:
            with open(summary_path, encoding='utf-8') as f:
                summary = json.load(f)
            frozen_iso = summary.get('wall_clock_frozen_at')
            if frozen_iso:
                print(f"  Wall clock frozen at: {frozen_iso}")
        except Exception:
            pass

    conn = sqlite3.connect(str(active_db))
    try:
        from model_engine import generate_predictions
    except Exception as e:
        raise RuntimeError(f"Could not import generate_predictions: {e}")

    if sport:
        sports = [sport]
    else:
        sports = sorted(p.stem for p in in_dir.glob('*.json') if not p.stem.startswith('_'))

    overall = {'label': label, 'sports': {}, 'total_diffs': 0}
    print(f"  Replaying {len(sports)} sport(s) vs golden in {in_dir}")
    all_game_picks = []
    _replay_freeze = _freeze_wall_clock(frozen_iso)
    _replay_freeze.__enter__()
    for sp in sports:
        golden_path = in_dir / f"{sp}.json"
        if not golden_path.exists():
            print(f"    {sp}: NO GOLDEN — skip")
            continue
        with open(golden_path, encoding='utf-8') as f:
            golden = json.load(f)
        try:
            new = generate_predictions(conn, sport=sp)
        except Exception as e:
            print(f"    {sp}: FAILED ({e})")
            overall['sports'][sp] = {'error': str(e)}
            overall['total_diffs'] += 1
            continue
        new_norm = serialize_picks(new or [])
        equal, report = picks_equivalent(golden, new_norm)
        overall['sports'][sp] = report
        if equal:
            print(f"    {sp}: ✅ {len(golden)} picks identical")
        else:
            n_diff = len(report['added']) + len(report['removed']) + len(report['changed'])
            overall['total_diffs'] += n_diff
            print(f"    {sp}: ❌ {len(golden)}→{len(new_norm)} picks | "
                  f"+{len(report['added'])} -{len(report['removed'])} ~{len(report['changed'])}")
        all_game_picks.extend(new or [])

    # v26.0 Phase 2.5: post-merge replay
    merged_golden_path = in_dir / '_merged.json'
    if merged_golden_path.exists() and not sport:  # only when replaying all sports
        try:
            from main import _merge_and_select
            with open(merged_golden_path, encoding='utf-8') as f:
                merged_golden = json.load(f)
            merged_new = _merge_and_select(all_game_picks, [], conn=conn)
            merged_new_norm = serialize_picks(merged_new or [])
            equal, report = picks_equivalent(merged_golden, merged_new_norm)
            overall['post_merge'] = report
            if equal:
                print(f"    POST-MERGE: ✅ {len(merged_golden)} picks identical")
            else:
                n_diff = len(report['added']) + len(report['removed']) + len(report['changed'])
                overall['total_diffs'] += n_diff
                print(f"    POST-MERGE: ❌ {len(merged_golden)}→{len(merged_new_norm)} picks | "
                      f"+{len(report['added'])} -{len(report['removed'])} ~{len(report['changed'])}")
        except Exception as e:
            print(f"    POST-MERGE: FAILED ({e})")

    _replay_freeze.__exit__(None, None, None)
    conn.close()
    return overall


def determinism_check(db_path: Optional[Path] = None) -> Dict[str, Any]:
    """
    Capture twice against a single frozen snapshot. Any diff = the function
    has internal non-determinism (random, dict order, time-of-day). Without
    the snapshot, ambient DB drift contaminates the test.
    """
    db_path = db_path or DEFAULT_DB
    print("Determinism check — capturing twice against frozen DB snapshot...")

    # Single snapshot for both runs — eliminates ambient DB drift
    shared_snap = GOLDEN_ROOT / '_det_shared' / '_snapshot.db'
    shared_snap.parent.mkdir(parents=True, exist_ok=True)
    _snapshot_db(db_path, shared_snap)
    print(f"  Frozen snapshot: {shared_snap}")

    # Both runs use the snapshot directly (no per-run snapshot)
    capture_golden('_det_run1', db_path=shared_snap, snapshot=False)
    capture_golden('_det_run2', db_path=shared_snap, snapshot=False)

    run1_dir = GOLDEN_ROOT / '_det_run1'
    run2_dir = GOLDEN_ROOT / '_det_run2'
    sports = sorted(p.stem for p in run1_dir.glob('*.json') if not p.stem.startswith('_'))

    overall = {'sports': {}, 'total_diffs': 0}
    for sp in sports:
        with open(run1_dir / f"{sp}.json", encoding='utf-8') as f:
            a = json.load(f)
        if not (run2_dir / f"{sp}.json").exists():
            print(f"  {sp}: missing in run2")
            continue
        with open(run2_dir / f"{sp}.json", encoding='utf-8') as f:
            b = json.load(f)
        equal, report = picks_equivalent(a, b)
        overall['sports'][sp] = report
        if equal:
            print(f"  {sp}: ✅ deterministic ({len(a)} picks)")
        else:
            n_diff = len(report['added']) + len(report['removed']) + len(report['changed'])
            overall['total_diffs'] += n_diff
            print(f"  {sp}: ❌ NON-DETERMINISTIC | +{len(report['added'])} -{len(report['removed'])} ~{len(report['changed'])}")

    return overall


def main():
    """CLI entry: capture | replay | determinism."""
    import argparse
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest='cmd', required=True)

    cp = sub.add_parser('capture', help='Capture a new golden snapshot')
    cp.add_argument('--label', default='baseline', help='Snapshot label')
    cp.add_argument('--sport', default=None, help='Single sport, default = all active')
    cp.add_argument('--db', default=None, help='DB path override')

    rp = sub.add_parser('replay', help='Replay against an existing golden')
    rp.add_argument('--label', default='baseline')
    rp.add_argument('--sport', default=None)
    rp.add_argument('--db', default=None)

    sub.add_parser('determinism', help='Run capture twice, expect identical')

    args = ap.parse_args()
    db = Path(args.db) if getattr(args, 'db', None) else None

    if args.cmd == 'capture':
        out = capture_golden(args.label, db_path=db, sport=args.sport)
        print(f"\n  Saved: {out}")
    elif args.cmd == 'replay':
        report = replay_and_compare(args.label, db_path=db, sport=args.sport)
        print(f"\n  Total diffs: {report['total_diffs']}")
        if report['total_diffs'] == 0:
            print("  ✅ Byte-equivalent — refactor safe to advance")
        else:
            print("  ❌ Diffs detected — investigate before advancing")
            sys.exit(1)
    elif args.cmd == 'determinism':
        report = determinism_check()
        print(f"\n  Determinism diffs: {report['total_diffs']}")
        if report['total_diffs'] == 0:
            print("  ✅ Function is deterministic — refactor proceeds")
        else:
            print("  ❌ Non-determinism detected — must address before refactor")
            sys.exit(1)


if __name__ == '__main__':
    main()
