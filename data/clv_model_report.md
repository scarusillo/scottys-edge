# CLV Predictor — naive additive model

**Trained on:** graded picks since 2026-04-15, units ≥ 3.5, result in WIN/LOSS/PUSH
**N:** 143
**Baseline CLV:** +0.259
**Shrinkage k:** 20

## Validation (leave-one-out)

- **MAE:** 0.847 (avg absolute error in CLV units)
- **Correlation (predicted, actual):** +0.253

### Decile lift — does predicted_clv rank picks correctly?

| Decile | n | avg predicted | avg actual |
|---|---|---|---|
| 1 | 14 | -0.909 | -0.143 |
| 2 | 14 | -0.680 | -0.250 |
| 3 | 14 | -0.483 | +0.000 |
| 4 | 15 | -0.239 | +0.233 |
| 5 | 14 | +0.075 | +0.107 |
| 6 | 14 | +0.311 | -0.071 |
| 7 | 15 | +0.628 | +0.747 |
| 8 | 14 | +0.897 | +0.929 |
| 9 | 14 | +1.297 | +0.693 |
| 10 | 15 | +1.653 | +0.313 |

## Feature contributions

### book

| value | n | raw_dev | shrunk_dev |
|---|---|---|---|
| FanDuel | 25 | -0.279 | -0.155 |
| Caesars | 19 | -0.265 | -0.129 |
| DraftKings | 41 | -0.147 | -0.099 |
| Fanatics | 16 | +0.147 | +0.065 |
| BetMGM | 12 | +0.224 | +0.084 |
| BetRivers | 30 | +0.434 | +0.260 |

### direction

| value | n | raw_dev | shrunk_dev |
|---|---|---|---|
| DOG | 7 | -0.402 | -0.104 |
| UNDER | 73 | -0.072 | -0.056 |
| OTHER | 3 | -0.093 | -0.012 |
| OVER | 60 | +0.139 | +0.104 |

### edge_bucket

| value | n | raw_dev | shrunk_dev |
|---|---|---|---|
| 12-16 | 14 | -0.345 | -0.142 |
| 20+ | 86 | -0.083 | -0.067 |
| <12 | 5 | -0.259 | -0.052 |
| NULL | 3 | -0.259 | -0.034 |
| 18-20 | 5 | +0.021 | +0.004 |
| DATA | 22 | +0.263 | +0.138 |
| 16-18 | 8 | +1.016 | +0.290 |

### market

| value | n | raw_dev | shrunk_dev |
|---|---|---|---|
| TOTAL | 86 | -0.137 | -0.111 |
| SPREAD | 8 | -0.322 | -0.092 |
| MONEYLINE | 2 | -0.259 | -0.024 |
| PROP | 47 | +0.317 | +0.222 |

### move_class

| value | n | raw_dev | shrunk_dev |
|---|---|---|---|
| STABLE | 62 | -0.268 | -0.202 |
| SHARP_LEAD | 4 | -1.009 | -0.168 |
| SOFT_LEAD | 13 | +0.010 | +0.004 |
| MIXED | 2 | +0.741 | +0.067 |
| STEAM | 1 | +1.741 | +0.083 |
| DIVERGENT | 6 | +0.657 | +0.152 |
| NULL | 55 | +0.242 | +0.178 |

### opener_move

| value | n | raw_dev | shrunk_dev |
|---|---|---|---|
| NONE | 18 | -0.398 | -0.189 |
| WITH_>=0.5 | 5 | -0.459 | -0.092 |
| WITH_<0.5 | 10 | -0.209 | -0.070 |
| AGAINST_<0.5 | 40 | -0.034 | -0.023 |
| AGAINST_>=0.5 | 16 | -0.041 | -0.018 |
| NULL | 54 | +0.252 | +0.184 |

### side_type

| value | n | raw_dev | shrunk_dev |
|---|---|---|---|
| UNDER | 37 | -0.408 | -0.265 |
| DOG | 7 | -0.402 | -0.104 |
| PROP_BOOK_ARB | 5 | -0.259 | -0.052 |
| OVER | 28 | -0.081 | -0.047 |
| PROP_UNDER | 20 | -0.074 | -0.037 |
| BOOK_ARB | 2 | -0.259 | -0.024 |
| PROP_CAREER_FADE | 2 | -0.259 | -0.024 |
| DATA_SPREAD | 1 | -0.259 | -0.012 |
| SPREAD_FADE_FLIP | 2 | -0.009 | -0.001 |
| DATA_TOTAL | 19 | +0.320 | +0.156 |
| PROP_FADE_FLIP | 11 | +0.850 | +0.301 |
| PROP_OVER | 9 | +0.985 | +0.306 |

### sport

| value | n | raw_dev | shrunk_dev |
|---|---|---|---|
| baseball_ncaa | 35 | -0.331 | -0.211 |
| icehockey_nhl | 20 | -0.289 | -0.145 |
| tennis_atp_madrid_open | 5 | -0.459 | -0.092 |
| baseball_mlb | 29 | -0.111 | -0.066 |
| soccer_spain_la_liga | 3 | -0.259 | -0.034 |
| soccer_italy_serie_a | 2 | -0.259 | -0.024 |
| tennis_wta_madrid_open | 2 | -0.259 | -0.024 |
| basketball_nba | 47 | +0.526 | +0.369 |
