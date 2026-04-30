# v26.1 MLB Context ML — Model Report

**Trained:** 2026-04-29T16:30:59.908021
**Sample:** n=440 (352 train, 88 holdout)

## Validation

| Metric | Train | Holdout |
|---|---|---|
| Log-loss | 0.6769 | 0.7333 |
| Brier | 0.2420 | 0.2687 |
| Acc@0.5 | 56.2% | 50.0% |

## Feature Weights (standardized)

| Feature | Weight |
|---|---|
| rest_days_home | +0.275 |
| rest_days_away | -0.142 |
| batting_form_diff | +0.132 |
| park_factor | -0.130 |
| bullpen_era_diff | +0.124 |
| recent_rd_diff | -0.104 |
| starter_k9_diff | -0.084 |
| starter_era_diff | +0.007 |
| home_advantage | -0.000 |
| injury_impact_diff | +0.000 |
| **bias** | +0.233 |

## Backtest @ edge ≥ 8%, odds in [-150, +140]

- Full sample: n=96 | 52W-44L | WR=54.2% | P/L=+47.06u
- Holdout only: n=20 | 12W-8L | WR=60.0% | P/L=+24.20u
