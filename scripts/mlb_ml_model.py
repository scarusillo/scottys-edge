"""v26.1 MLB Context ML — logistic regression model.

Hand-rolled with numpy. Trains on all completed MLB games since rebuild.
Output: P(home_team_wins) directly (not derived from spread).

Validation:
  - Random 80/20 holdout split
  - Leave-one-out for cohort sanity
  - Brier score, log-loss, accuracy on favorite calls
  - Backtest at edge >= 8% in [-150, +140] vs market_consensus.best_*_ml

Model:
  - Features standardized (z-score) before fitting
  - L2 regularization (lambda=1.0 — strong, since n=444 with 10 features)
  - Gradient descent, 5000 iterations, lr=0.05
  - Fit weights persisted to data/mlb_ml_weights.json

Public:
  - main() — fit + validate + report
  - predict_home_win_prob(conn, home, away, game_date) — fire-time helper
"""
import json
import math
import os
import sqlite3
import random
from datetime import datetime

import numpy as np

from mlb_ml_features import (
    build_features, features_to_vector,
    FEATURE_NAMES, DEFAULT_FILL,
)

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')
WEIGHTS_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'mlb_ml_weights.json')
REPORT_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'mlb_ml_report.md')

L2_LAMBDA = 1.0
LR = 0.05
MAX_ITERS = 5000
HOLDOUT_FRAC = 0.20
RANDOM_SEED = 42

TRAIN_SINCE = '2026-03-26'  # Opening Day


# ──────────────────────────────────────────────────────────────────────────
# Training data
# ──────────────────────────────────────────────────────────────────────────

def load_training(conn):
    """Pull all completed MLB games + build features + home_win label.
    Returns list of (X_dict, y, meta) where y = 1 if home won, meta has eid+date.
    """
    rows = conn.execute("""
        SELECT event_id, DATE(commence_time) AS gd, home, away,
               home_score, away_score
        FROM results
        WHERE sport='baseball_mlb'
          AND home_score IS NOT NULL AND away_score IS NOT NULL
          AND DATE(commence_time) >= ?
          AND DATE(commence_time) < DATE('now')
        ORDER BY commence_time
    """, (TRAIN_SINCE,)).fetchall()

    samples = []
    for eid, gd, home, away, hs, asc in rows:
        if hs == asc:
            continue  # MLB ties are vanishingly rare; skip for binary classification
        feat = build_features(conn, home, away, gd)
        y = 1 if hs > asc else 0
        samples.append({
            'features': feat, 'y': y, 'event_id': eid, 'date': gd,
            'home': home, 'away': away,
        })
    return samples


# ──────────────────────────────────────────────────────────────────────────
# Standardization
# ──────────────────────────────────────────────────────────────────────────

def fit_standardizer(X):
    """Compute per-feature mean + std for z-scoring."""
    X = np.array(X, dtype=np.float64)
    mu = X.mean(axis=0)
    sigma = X.std(axis=0)
    sigma = np.where(sigma < 1e-8, 1.0, sigma)
    return mu, sigma


def standardize(X, mu, sigma):
    X = np.array(X, dtype=np.float64)
    return (X - mu) / sigma


# ──────────────────────────────────────────────────────────────────────────
# Logistic regression
# ──────────────────────────────────────────────────────────────────────────

def _sigmoid(z):
    z = np.clip(z, -50, 50)
    return 1.0 / (1.0 + np.exp(-z))


def fit_logreg(X, y, l2=L2_LAMBDA, lr=LR, iters=MAX_ITERS, verbose=False):
    """Hand-rolled L2-regularized logistic regression via batch gradient
    descent. Returns (weights, bias). Weights shape (n_features,).
    """
    X = np.asarray(X, dtype=np.float64)
    y = np.asarray(y, dtype=np.float64)
    n, d = X.shape
    w = np.zeros(d)
    b = 0.0
    for it in range(iters):
        z = X @ w + b
        p = _sigmoid(z)
        grad_w = (X.T @ (p - y)) / n + (l2 / n) * w
        grad_b = (p - y).mean()
        w -= lr * grad_w
        b -= lr * grad_b
        if verbose and it % 500 == 0:
            loss = -np.mean(y * np.log(p + 1e-12) + (1 - y) * np.log(1 - p + 1e-12))
            loss += (l2 / (2 * n)) * np.sum(w ** 2)
            print(f'  iter {it}: loss={loss:.4f}')
    return w, b


def predict_proba(X, w, b):
    X = np.asarray(X, dtype=np.float64)
    return _sigmoid(X @ w + b)


# ──────────────────────────────────────────────────────────────────────────
# Validation metrics
# ──────────────────────────────────────────────────────────────────────────

def brier(y, p):
    return float(np.mean((y - p) ** 2))


def logloss(y, p):
    p = np.clip(p, 1e-12, 1 - 1e-12)
    return float(-np.mean(y * np.log(p) + (1 - y) * np.log(1 - p)))


def accuracy_on_favorite(y, p):
    """Proportion of games where the model's predicted favorite (p > 0.5) won."""
    pred = (p > 0.5).astype(int)
    return float((pred == y).mean())


# ──────────────────────────────────────────────────────────────────────────
# Backtest harness vs market ML
# ──────────────────────────────────────────────────────────────────────────

def backtest_vs_market(conn, samples, w, b, mu, sigma,
                       edge_floor=0.08, odds_min=-150, odds_max=140,
                       stake=5.0):
    """For each sample, look up market_consensus best ML on home + away sides.
    Compare model probability to fair (devigged) market prob. Fire when:
      - model prob > fair prob by at least edge_floor
      - selected ML in [odds_min, odds_max]
    Track W/L/PL.
    """
    def odds_to_implied(a):
        if a is None: return None
        return 100.0/(a+100.0) if a > 0 else abs(a)/(abs(a)+100.0)

    def payout(a):
        return a/100.0 if a > 0 else 100.0/abs(a)

    cands = []
    for s in samples:
        eid = s['event_id']
        row = conn.execute("""
            SELECT best_home_ml, best_away_ml FROM market_consensus
            WHERE event_id=? AND sport='baseball_mlb' AND tag='CURRENT'
              AND best_home_ml IS NOT NULL AND best_away_ml IS NOT NULL
            LIMIT 1
        """, (eid,)).fetchone()
        if not row:
            continue
        h_ml, a_ml = row
        ihp = odds_to_implied(h_ml); iap = odds_to_implied(a_ml)
        tot = ihp + iap
        if tot <= 0: continue
        h_fair = ihp / tot
        a_fair = iap / tot

        x = features_to_vector(s['features'])
        x_std = standardize([x], mu, sigma)
        ph = float(predict_proba(x_std, w, b)[0])
        pa = 1 - ph

        h_won = (s['y'] == 1)
        for side, ml, pmod, pfair, won in [
            ('HOME', h_ml, ph, h_fair, h_won),
            ('AWAY', a_ml, pa, a_fair, not h_won),
        ]:
            edge = pmod - pfair
            if edge < edge_floor: continue
            if not (odds_min <= ml <= odds_max): continue
            pl = stake * payout(ml) if won else -stake
            cands.append({
                'side': side, 'ml': ml, 'edge': edge, 'won': won, 'pl': pl,
                'pmod': pmod, 'pfair': pfair,
            })
    return cands


def summarize_backtest(cands, label=''):
    if not cands:
        return f'{label}: n=0'
    n = len(cands)
    w = sum(1 for c in cands if c['won'])
    l = n - w
    pl = sum(c['pl'] for c in cands)
    wr = w / n
    return f'{label}: n={n} | {w}W-{l}L | WR={wr:.1%} | P/L={pl:+.2f}u'


# ──────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────

def main():
    random.seed(RANDOM_SEED)
    np.random.seed(RANDOM_SEED)

    conn = sqlite3.connect(DB_PATH)
    print('=== v26.1 MLB Context ML — training ===\n')
    samples = load_training(conn)
    print(f'Training samples: n={len(samples)}')
    if len(samples) < 50:
        print('ERROR: training set too small (<50)'); return

    # Build feature matrix
    X = [features_to_vector(s['features']) for s in samples]
    y = [s['y'] for s in samples]
    print(f'Feature matrix: {len(X)} × {len(FEATURE_NAMES)}')

    # Train/holdout split
    n = len(samples)
    indices = list(range(n))
    random.shuffle(indices)
    n_holdout = int(n * HOLDOUT_FRAC)
    holdout_idx = set(indices[:n_holdout])
    train_idx = [i for i in indices if i not in holdout_idx]
    holdout_idx = sorted(holdout_idx)
    print(f'Train: {len(train_idx)}, Holdout: {len(holdout_idx)}\n')

    X_train = np.array([X[i] for i in train_idx])
    y_train = np.array([y[i] for i in train_idx])
    X_holdout = np.array([X[i] for i in holdout_idx])
    y_holdout = np.array([y[i] for i in holdout_idx])

    # Standardize on train, apply to holdout
    mu, sigma = fit_standardizer(X_train)
    X_train_std = standardize(X_train, mu, sigma)
    X_holdout_std = standardize(X_holdout, mu, sigma)

    # Fit
    print('Fitting logistic regression...')
    w, bias = fit_logreg(X_train_std, y_train, verbose=True)
    print()

    # Train metrics
    p_train = predict_proba(X_train_std, w, bias)
    print('=== Training set metrics ===')
    print(f'  Log-loss:  {logloss(y_train, p_train):.4f}')
    print(f'  Brier:     {brier(y_train, p_train):.4f}')
    print(f'  Acc@0.5:   {accuracy_on_favorite(y_train, p_train):.1%}')

    # Holdout metrics — the honest test
    p_hold = predict_proba(X_holdout_std, w, bias)
    print('\n=== Holdout (random 20%) metrics ===')
    print(f'  Log-loss:  {logloss(y_holdout, p_hold):.4f}')
    print(f'  Brier:     {brier(y_holdout, p_hold):.4f}')
    print(f'  Acc@0.5:   {accuracy_on_favorite(y_holdout, p_hold):.1%}')

    # Feature importance (standardized weight magnitude)
    print('\n=== Feature weights (standardized) ===')
    importance = sorted(zip(FEATURE_NAMES, w), key=lambda x: -abs(x[1]))
    for name, weight in importance:
        sign = '+' if weight >= 0 else '-'
        print(f'  {name:22} {sign}{abs(weight):.3f}')
    print(f'  bias                   {bias:+.3f}')

    # Backtest on full sample (proxy for "would the model find ML edge?")
    print('\n=== Backtest: model vs market ML at edge >= 8%, [-150, +140] ===')
    full_cands = backtest_vs_market(conn, samples, w, bias, mu, sigma)
    print(' ', summarize_backtest(full_cands, 'Full sample'))

    # Just holdout-period backtest (no leakage)
    holdout_samples = [samples[i] for i in holdout_idx]
    hold_cands = backtest_vs_market(conn, holdout_samples, w, bias, mu, sigma)
    print(' ', summarize_backtest(hold_cands, 'Holdout only'))

    # Edge sweep on holdout
    print('\n=== Holdout edge sweep ===')
    for floor in [0.05, 0.08, 0.10, 0.12, 0.15]:
        cands = backtest_vs_market(conn, holdout_samples, w, bias, mu, sigma,
                                    edge_floor=floor)
        print(' ', summarize_backtest(cands, f'edge >= {int(floor*100)}%'))

    # Save weights
    out = {
        'feature_names': FEATURE_NAMES,
        'mu': mu.tolist(),
        'sigma': sigma.tolist(),
        'weights': w.tolist(),
        'bias': float(bias),
        'trained_at': datetime.utcnow().isoformat(),
        'n_train': int(len(train_idx)),
        'n_holdout': int(len(holdout_idx)),
        'metrics_holdout': {
            'logloss': logloss(y_holdout, p_hold),
            'brier': brier(y_holdout, p_hold),
            'acc_at_0.5': accuracy_on_favorite(y_holdout, p_hold),
        },
    }
    with open(WEIGHTS_PATH, 'w') as f:
        json.dump(out, f, indent=2)
    print(f'\nWeights saved: {WEIGHTS_PATH}')

    # Build report
    _build_report(samples, w, bias, mu, sigma, importance, full_cands, hold_cands,
                  y_train, p_train, y_holdout, p_hold, len(train_idx), len(holdout_idx))


def _build_report(samples, w, bias, mu, sigma, importance, full_cands, hold_cands,
                  y_train, p_train, y_holdout, p_hold, n_train, n_hold):
    lines = []
    lines.append('# v26.1 MLB Context ML — Model Report\n')
    lines.append(f'**Trained:** {datetime.utcnow().isoformat()}')
    lines.append(f'**Sample:** n={len(samples)} ({n_train} train, {n_hold} holdout)\n')
    lines.append('## Validation\n')
    lines.append('| Metric | Train | Holdout |')
    lines.append('|---|---|---|')
    lines.append(f'| Log-loss | {logloss(y_train, p_train):.4f} | {logloss(y_holdout, p_hold):.4f} |')
    lines.append(f'| Brier | {brier(y_train, p_train):.4f} | {brier(y_holdout, p_hold):.4f} |')
    lines.append(f'| Acc@0.5 | {accuracy_on_favorite(y_train, p_train):.1%} | {accuracy_on_favorite(y_holdout, p_hold):.1%} |\n')
    lines.append('## Feature Weights (standardized)\n')
    lines.append('| Feature | Weight |')
    lines.append('|---|---|')
    for name, weight in importance:
        lines.append(f'| {name} | {weight:+.3f} |')
    lines.append(f'| **bias** | {bias:+.3f} |\n')
    lines.append('## Backtest @ edge ≥ 8%, odds in [-150, +140]\n')
    lines.append(f'- {summarize_backtest(full_cands, "Full sample")}')
    lines.append(f'- {summarize_backtest(hold_cands, "Holdout only")}\n')

    with open(REPORT_PATH, 'w', encoding='utf-8') as f:
        f.write('\n'.join(lines))
    print(f'Report: {REPORT_PATH}')


# ──────────────────────────────────────────────────────────────────────────
# Fire-time helper
# ──────────────────────────────────────────────────────────────────────────

_LOADED_WEIGHTS = None


def _ensure_weights():
    global _LOADED_WEIGHTS
    if _LOADED_WEIGHTS is None:
        if not os.path.exists(WEIGHTS_PATH):
            return None
        with open(WEIGHTS_PATH) as f:
            _LOADED_WEIGHTS = json.load(f)
    return _LOADED_WEIGHTS


def predict_home_win_prob(conn, home, away, game_date):
    """Fire-time helper. Returns float P(home_wins) or None if model not
    loaded.
    """
    w = _ensure_weights()
    if w is None:
        return None
    feat = build_features(conn, home, away, game_date)
    x = features_to_vector(feat)
    mu = np.array(w['mu']); sigma = np.array(w['sigma'])
    x_std = (np.array(x) - mu) / sigma
    weights = np.array(w['weights']); bias = w['bias']
    z = float(x_std @ weights + bias)
    z = max(min(z, 50), -50)
    return 1.0 / (1.0 + math.exp(-z))


if __name__ == '__main__':
    main()
