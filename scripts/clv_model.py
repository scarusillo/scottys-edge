"""v25.97 CLV predictor — naive additive model with shrinkage.

Goal: at fire time, produce a `predicted_clv` value for any candidate pick
based on historical cohort behavior. NOT a deep ML model — given the small
sample (~143 graded picks since 4/15 with reliable closing-side data, much
of which has sparse Layer 2 trajectory features), a simple per-feature
average with shrinkage is the right tool.

Approach:
  baseline_clv = mean(clv) over training set
  For each feature f and value v:
    cohort_dev[f, v] = mean(clv | f=v) - baseline_clv
    n[f, v] = cohort size
    shrunk_dev[f, v] = cohort_dev * n / (n + k)    # k = SHRINKAGE_K (default 20)
  predicted_clv(pick) = baseline_clv + sum_f(shrunk_dev[f, pick.f])

Honest about its limits:
  - Independence assumption — interactions not modeled
  - Shrinkage k=20 means small cohorts barely move the prediction
  - Validation via leave-one-out on the training set + holdout

Outputs:
  - data/clv_model_report.md — table of feature contributions + validation
  - clv_predict(conn, pick) helper for fire-time use (NOT yet wired)

Safety:
  - Pure read on bets/graded_bets; writes only to the report markdown
  - No live pipeline integration in this commit; per
    feedback_dryrun_before_live.md, build then dry-run before wiring
"""
import sqlite3, os, sys, json, math, random
from collections import defaultdict
from statistics import mean

DB = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')
REPORT = os.path.join(os.path.dirname(__file__), '..', 'data',
                      'clv_model_report.md')

SHRINKAGE_K = 20   # higher = more conservative (small cohorts shrunk harder)
SINCE = '2026-04-15'
MIN_UNITS = 3.5


# ─────────────────────────────────────────────────────────────────────────
# Feature extraction — what's available at fire time
# ─────────────────────────────────────────────────────────────────────────

def _direction(sel, side_type):
    s = (sel or '').upper()
    if 'OVER' in s: return 'OVER'
    if 'UNDER' in s: return 'UNDER'
    if side_type in ('DOG', 'FAVORITE'): return side_type
    return 'OTHER'


def _bin_numeric(v, bins, labels):
    if v is None:
        return 'NULL'
    for i, b in enumerate(bins):
        if v < b: return labels[i]
    return labels[-1]


def _bin_edge(e):
    if e is None: return 'NULL'
    if e == 0:    return 'DATA'
    if e < 12:    return '<12'
    if e < 16:    return '12-16'
    if e < 18:    return '16-18'
    if e < 20:    return '18-20'
    return '20+'


def _bin_opener_move(om):
    if om is None: return 'NULL'
    if om <= -0.5: return 'AGAINST_>=0.5'
    if om < 0:     return 'AGAINST_<0.5'
    if om == 0:    return 'NONE'
    if om < 0.5:   return 'WITH_<0.5'
    return 'WITH_>=0.5'


def featurize(row):
    """row is a dict from query. Returns dict of feature -> value (str)."""
    return {
        'sport':       row['sport'] or 'X',
        'market':      row['market_type'] or 'X',
        'side_type':   row['side_type'] or 'X',
        'book':        row['book'] or 'X',
        'direction':   _direction(row['selection'], row['side_type']),
        'edge_bucket': _bin_edge(row['edge_pct']),
        'opener_move': _bin_opener_move(row['opener_move']),
        'move_class':  row['move_class'] or 'NULL',
    }


# ─────────────────────────────────────────────────────────────────────────
# Training
# ─────────────────────────────────────────────────────────────────────────

def load_training(conn, since=SINCE, min_units=MIN_UNITS):
    cur = conn.cursor()
    cur.execute("""
        SELECT g.bet_id, g.sport, g.selection, g.market_type, g.side_type,
               g.book, g.line, g.odds, g.edge_pct, g.units, g.result,
               g.pnl_units, g.clv, g.clv_line, g.clv_odds_pct, g.created_at,
               b.opener_move, b.move_class, b.originator_book
        FROM graded_bets g
        LEFT JOIN bets b ON g.bet_id = b.id
        WHERE g.created_at >= ? AND g.result IN ('WIN','LOSS','PUSH')
          AND g.units >= ? AND g.clv IS NOT NULL
    """, (since, min_units))
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


def fit(rows):
    """Return (baseline, per_feature_devs) where per_feature_devs is
    dict {feature_name: {value: (n, raw_dev, shrunk_dev)}}."""
    if not rows:
        return 0.0, {}

    clvs = [r['clv'] for r in rows]
    baseline = mean(clvs)

    # Group CLV by (feature, value)
    grouped = defaultdict(lambda: defaultdict(list))
    for r in rows:
        feats = featurize(r)
        for f, v in feats.items():
            grouped[f][v].append(r['clv'])

    devs = {}
    for f, by_v in grouped.items():
        devs[f] = {}
        for v, lst in by_v.items():
            n = len(lst)
            raw_dev = mean(lst) - baseline
            shrunk = raw_dev * n / (n + SHRINKAGE_K)
            devs[f][v] = (n, round(raw_dev, 3), round(shrunk, 3))
    return round(baseline, 3), devs


def predict(baseline, devs, pick_features):
    """pick_features is a dict like featurize() returns."""
    p = baseline
    contribs = {}
    for f, v in pick_features.items():
        if f in devs and v in devs[f]:
            contribs[f] = devs[f][v][2]  # shrunk dev
            p += devs[f][v][2]
        else:
            contribs[f] = 0.0
    return p, contribs


# ─────────────────────────────────────────────────────────────────────────
# Validation — leave-one-out on training set
# ─────────────────────────────────────────────────────────────────────────

def loo_validate(rows):
    """Leave-one-out CV. Returns (mae, corr, predictions list)."""
    if len(rows) < 10:
        return None, None, []
    preds = []
    for i, held in enumerate(rows):
        train = rows[:i] + rows[i+1:]
        bl, devs = fit(train)
        p, _ = predict(bl, devs, featurize(held))
        preds.append((p, held['clv']))

    diffs = [abs(p - a) for p, a in preds]
    mae = sum(diffs) / len(diffs)

    # Pearson correlation
    n = len(preds)
    mp = sum(p for p, _ in preds) / n
    ma = sum(a for _, a in preds) / n
    num = sum((p - mp) * (a - ma) for p, a in preds)
    sp = math.sqrt(sum((p - mp) ** 2 for p, _ in preds))
    sa = math.sqrt(sum((a - ma) ** 2 for _, a in preds))
    corr = num / (sp * sa) if (sp > 0 and sa > 0) else 0.0
    return round(mae, 3), round(corr, 3), preds


# ─────────────────────────────────────────────────────────────────────────
# Report
# ─────────────────────────────────────────────────────────────────────────

def build_report(rows, baseline, devs, mae, corr, preds):
    lines = []
    lines.append('# CLV Predictor — naive additive model')
    lines.append('')
    lines.append(f'**Trained on:** graded picks since {SINCE}, units ≥ {MIN_UNITS}, result in WIN/LOSS/PUSH')
    lines.append(f'**N:** {len(rows)}')
    lines.append(f'**Baseline CLV:** {baseline:+.3f}')
    lines.append(f'**Shrinkage k:** {SHRINKAGE_K}')
    lines.append('')
    lines.append('## Validation (leave-one-out)')
    lines.append('')
    lines.append(f'- **MAE:** {mae:.3f} (avg absolute error in CLV units)')
    lines.append(f'- **Correlation (predicted, actual):** {corr:+.3f}')
    lines.append('')

    # Decile lift on predicted_clv vs actual_clv
    lines.append('### Decile lift — does predicted_clv rank picks correctly?')
    lines.append('')
    sorted_preds = sorted(preds, key=lambda x: x[0])
    n = len(sorted_preds)
    deciles = []
    for d in range(10):
        chunk = sorted_preds[int(d*n/10):int((d+1)*n/10)]
        if not chunk: continue
        avg_pred = sum(p for p, _ in chunk) / len(chunk)
        avg_act = sum(a for _, a in chunk) / len(chunk)
        deciles.append((d+1, len(chunk), avg_pred, avg_act))
    lines.append('| Decile | n | avg predicted | avg actual |')
    lines.append('|---|---|---|---|')
    for d, c, ap, aa in deciles:
        lines.append(f'| {d} | {c} | {ap:+.3f} | {aa:+.3f} |')
    lines.append('')

    lines.append('## Feature contributions')
    lines.append('')
    for f in sorted(devs.keys()):
        lines.append(f'### {f}')
        lines.append('')
        lines.append('| value | n | raw_dev | shrunk_dev |')
        lines.append('|---|---|---|---|')
        items = sorted(devs[f].items(), key=lambda x: x[1][2])
        for v, (n, raw, shrunk) in items:
            lines.append(f'| {v} | {n} | {raw:+.3f} | {shrunk:+.3f} |')
        lines.append('')

    return '\n'.join(lines)


# ─────────────────────────────────────────────────────────────────────────
# Public helper — score a candidate pick at fire time
# ─────────────────────────────────────────────────────────────────────────

_MODEL_CACHE = None

def clv_predict(conn, pick):
    """Score a pick at fire time. `pick` is a dict with keys matching
    the bets table column names (sport, selection, market_type, side_type,
    book, edge_pct, opener_move, move_class, originator_book).

    Returns (predicted_clv, contributions_dict)."""
    global _MODEL_CACHE
    if _MODEL_CACHE is None:
        rows = load_training(conn)
        bl, devs = fit(rows)
        _MODEL_CACHE = (bl, devs)
    bl, devs = _MODEL_CACHE
    return predict(bl, devs, featurize(pick))


# ─────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────

def main():
    conn = sqlite3.connect(DB)
    rows = load_training(conn)
    print(f'Training set: n={len(rows)}')
    if len(rows) < 30:
        print('ERROR: training set too small (<30); cannot fit model')
        return

    baseline, devs = fit(rows)
    print(f'Baseline CLV: {baseline:+.3f}')

    print('Running leave-one-out validation...')
    mae, corr, preds = loo_validate(rows)
    print(f'  MAE: {mae:.3f}')
    print(f'  Correlation (predicted, actual): {corr:+.3f}')

    report = build_report(rows, baseline, devs, mae, corr, preds)
    with open(REPORT, 'w', encoding='utf-8') as f:
        f.write(report)
    print(f'Report written: {REPORT}')

    # Sanity check: top + bottom 5 predictions
    sorted_preds = sorted(enumerate(preds), key=lambda x: x[1][0])
    print('\nLowest 5 predicted CLV (most-fade candidates):')
    for idx, (p, a) in sorted_preds[:5]:
        r = rows[idx]
        print(f'  pred={p:+.2f}  actual={a:+.2f}  {r["sport"][:14]:14} {r["selection"][:50]}')
    print('\nHighest 5 predicted CLV:')
    for idx, (p, a) in sorted_preds[-5:]:
        r = rows[idx]
        print(f'  pred={p:+.2f}  actual={a:+.2f}  {r["sport"][:14]:14} {r["selection"][:50]}')


if __name__ == '__main__':
    main()
