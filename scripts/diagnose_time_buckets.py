"""For each cold sport x time-bucket in the 14-day window, dissect the losses:
  - Steam signal (SHARP_OPPOSES / NO_MOVEMENT / SHARP_CONFIRMS)
  - CLV (negative = line moved against us after fire)
  - Context-factor risk tags ([SHADOW], Midweek, Coors, unknown ERA, etc.)
  - Close-game variance (lost by <= 1 total run/point)
  - Would recent gates catch it retroactively?
"""
import os, sys, sqlite3
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from steam_engine import get_steam_signal

DB = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')

# Cold slices to dissect, plus hot for comparison
SLICES = [
    ('NCAA BB midday',     'baseball_ncaa', (12, 14)),
    ('MLB morning',        'baseball_mlb',  (9, 11)),
    ('MLB midday',         'baseball_mlb',  (12, 14)),
    ('NCAA BB morning',    'baseball_ncaa', (9, 11)),
    ('NBA afternoon',      'basketball_nba', (15, 17)),
    ('MLB evening (hot)',  'baseball_mlb',  (18, 20)),
    ('NBA early (hot)',    'basketball_nba', (6, 8)),
    ('NHL early (neutral)', 'icehockey_nhl', (6, 8)),
]


def classify_risk_tags(ctx):
    """Scan context_factors for known risk signals."""
    tags = []
    if not ctx: return tags
    c = ctx
    if '[SHADOW]' in c: tags.append('SHADOW')
    if 'sharp opposes' in c: tags.append('SHARP_OPPOSES')
    if 'sharp confirms' in c: tags.append('SHARP_CONFIRMS')
    if 'Midweek' in c: tags.append('Midweek')
    if 'Coors' in c: tags.append('Coors')
    if 'Chase' in c: tags.append('Chase')
    if '?.??' in c: tags.append('unknown_ERA')
    if 'Division familiarity' in c: tags.append('Division')
    if 'blowout' in c.lower() or 'BLOWOUT' in c: tags.append('Blowout')
    if 'SCRUB' in c: tags.append('Scrubbed')
    return tags


def classify_loss(pick):
    """Given a graded_bet dict, return the most likely loss cause(s)."""
    (sport, mtype, sel, line, odds, result, pnl, clv, ctx, model_spread) = pick
    if result != 'LOSS':
        return []
    reasons = []
    # 1. Steam signal — was there sharp action against us?
    #    (We can't recompute easily here; rely on context_factors tag.)
    if ctx and 'sharp opposes' in ctx:
        reasons.append('SHARP_OPPOSES (line moved against us)')
    # 2. Negative CLV — line moved against us after we fired
    if clv is not None and clv < -1.5:
        reasons.append(f'Bad CLV ({clv:+.1f}%)')
    elif clv is not None and clv < 0:
        reasons.append(f'Slight neg CLV ({clv:+.1f}%)')
    # 3. Shadow factor applied
    tags = classify_risk_tags(ctx)
    for t in ['SHADOW','Midweek','Coors','unknown_ERA','Division']:
        if t in tags:
            reasons.append(f'Risk tag: {t}')
    # 4. MLB_SIDE_CONVICTION retroactive check
    if mtype == 'SPREAD' and model_spread is not None and abs(model_spread) < 0.5 and sport == 'baseball_mlb':
        reasons.append(f'Would hit MLB_SIDE_CONVICTION_GATE (|ms|={abs(model_spread):.2f})')
    # 5. Blowout gate retroactive check — can't without actual_margin; skip
    return reasons


def main():
    conn = sqlite3.connect(DB)
    print("=" * 95)
    print("GRANULAR LOSS DIAGNOSIS — last 14 days")
    print("=" * 95)

    summary = {}

    for label, sport, (lo_hr, hi_hr) in SLICES:
        rows = conn.execute("""
            SELECT id, event_id, market_type, side_type, selection, line, odds,
                   units, result, pnl_units, clv, context_factors, model_spread,
                   strftime('%H', created_at) hr, created_at,
                   strftime('%w', created_at) dow
            FROM graded_bets
            WHERE sport = ? AND DATE(created_at) >= '2026-04-06'
              AND result IN ('WIN','LOSS','PUSH')
              AND CAST(strftime('%H', created_at) AS INT) BETWEEN ? AND ?
            ORDER BY created_at
        """, (sport, lo_hr, hi_hr)).fetchall()

        if not rows:
            continue

        print()
        print("-" * 95)
        print(f"SLICE: {label}  ({sport} {lo_hr:02d}-{hi_hr:02d}h)")
        print("-" * 95)

        steam_counter = {'SHARP_OPPOSES': 0, 'NO_MOVEMENT': 0, 'SHARP_CONFIRMS': 0, 'ERROR': 0}
        clv_buckets = {'pos': [], 'flat': [], 'neg': []}
        tag_counter = defaultdict(int)
        would_block = defaultdict(int)
        loss_details = []
        win_details = []

        for r in rows:
            (_id, eid, mt, st, sel, ln, od, un, res, pnl, clv, ctx, ms, hr, ca, dow) = r
            # Steam retrospective
            side_hint = (st or '').upper()
            if mt == 'TOTAL':
                ss = 'OVER' if 'OVER' in side_hint or 'over' in (sel or '').lower() else 'UNDER'
            elif mt == 'SPREAD':
                ss = side_hint if side_hint in ('FAVORITE','DOG') else ('FAVORITE' if (ln or 0) < 0 else 'DOG')
            else:
                ss = None
            steam_sig = 'skip'
            if ss and eid and ln is not None:
                try:
                    steam_sig, _info = get_steam_signal(conn, sport, eid, mt, ss, ln, od)
                    steam_counter[steam_sig] = steam_counter.get(steam_sig, 0) + 1
                except Exception:
                    steam_counter['ERROR'] += 1

            # CLV bucketing
            if clv is not None:
                if clv > 1: clv_buckets['pos'].append((pnl, clv))
                elif clv < -1: clv_buckets['neg'].append((pnl, clv))
                else: clv_buckets['flat'].append((pnl, clv))

            # Risk tags
            for t in classify_risk_tags(ctx):
                tag_counter[t] += 1

            # Retroactive gate checks
            if mt == 'SPREAD' and ms is not None and abs(ms) < 0.5 and sport == 'baseball_mlb':
                would_block['MLB_SIDE_CONVICTION_GATE'] += 1
            if sport in ('icehockey_nhl','baseball_ncaa') and steam_sig == 'SHARP_OPPOSES':
                would_block['SHARP_OPPOSES_BLOCK (v25.35)'] += 1

            # Detail per pick
            if res == 'LOSS':
                loss_details.append((ca[:10], sel[:50], ln, pnl, clv, steam_sig, classify_risk_tags(ctx), ctx or ''))
            elif res == 'WIN':
                win_details.append((ca[:10], sel[:50], ln, pnl, clv, steam_sig))

        # Aggregate
        total_pnl = sum(r[9] or 0 for r in rows)
        n = len(rows); w = sum(1 for r in rows if r[8]=='WIN'); l = sum(1 for r in rows if r[8]=='LOSS'); p = sum(1 for r in rows if r[8]=='PUSH')
        print(f"  Record: {w}W-{l}L-{p}P  P/L {total_pnl:+.2f}u  ({n} picks)")
        print()
        print(f"  Steam dist: {dict(steam_counter)}")
        print(f"  CLV dist: pos={len(clv_buckets['pos'])}  flat={len(clv_buckets['flat'])}  neg={len(clv_buckets['neg'])}")
        if clv_buckets['neg']:
            pnl_neg = sum(p for p,_ in clv_buckets['neg'])
            print(f"    negative-CLV picks P/L: {pnl_neg:+.2f}u")
        if tag_counter:
            print(f"  Risk tags: {dict(tag_counter)}")
        if would_block:
            print(f"  Retro-blocked by existing gates: {dict(would_block)}")

        # Print losses detail
        if loss_details:
            print()
            print(f"  LOSSES ({len(loss_details)}):")
            for dt, sel, ln, pnl, clv, sig, tags, ctx in loss_details:
                clv_str = f'{clv:+.1f}%' if clv is not None else '—'
                tag_str = ','.join(tags) if tags else '—'
                print(f"    {dt}  {sel:<52} ln={ln or 0:<5}  CLV={clv_str:>6}  steam={sig:<15} tags={tag_str}")

        summary[label] = {
            'n': n, 'w': w, 'l': l, 'pnl': total_pnl,
            'steam': dict(steam_counter),
            'would_block': dict(would_block),
            'neg_clv_pnl': sum(p for p,_ in clv_buckets['neg']) if clv_buckets['neg'] else 0.0,
        }

    # === FINAL SUMMARY TABLE ===
    print()
    print("=" * 95)
    print("SUMMARY — what would EXISTING gates catch, and what's left?")
    print("=" * 95)
    print(f"  {'Slice':<26} {'N':>3} {'W-L':>6} {'P/L':>8} {'SHARP_OPP':>10} {'NegCLV$':>10} {'Retro-caught':>15}")
    for label, s in summary.items():
        so = s['steam'].get('SHARP_OPPOSES', 0)
        rb = sum(s['would_block'].values())
        print(f"  {label:<26} {s['n']:>3} {s['w']}-{s['l']:<4} {s['pnl']:>+7.2f}u {so:>10} {s['neg_clv_pnl']:>+8.2f}u {rb:>15}")


if __name__ == '__main__':
    main()
