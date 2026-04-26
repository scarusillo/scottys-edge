## Gate Health Card — 2026-04-25

### Fires (graded)
| Sport | n | Units | P/L |
|---|---|---|---|
| basketball_nba | 6 | 30.0u | -20.45u |
| baseball_ncaa | 3 | 15.0u | -5.24u |
| baseball_mlb | 3 | 15.0u | +4.45u |
| soccer_spain_la_liga | 1 | 5.0u | +4.85u |
| icehockey_nhl | 1 | 5.0u | -5.00u |
| **TOTAL** | **14** | **70.0u** | **-21.39u** |

### Fires (pending grade)
| Sport | n | Units |
|---|---|---|
| baseball_mlb | 1 | 5.0u |

### Block volume by sport
| Sport | Distinct events | Total log entries |
|---|---|---|
| soccer_usa_mls | 14 | 413 |
| baseball_ncaa | 13 | 91 |
| baseball_mlb | 11 | 437 |
| soccer_germany_bundesliga | 6 | 30 |
| basketball_nba | 6 | 669 |
| tennis_wta_madrid_open | 4 | 24 |
| tennis_atp_madrid_open | 4 | 44 |
| soccer_spain_la_liga | 4 | 34 |
| soccer_epl | 4 | 28 |
| soccer_italy_serie_a | 3 | 30 |
| soccer_france_ligue_one | 3 | 32 |
| icehockey_nhl | 3 | 31 |

### Blocks by gate
| Gate | Total log entries | Distinct events |
|---|---|---|
| `CONTEXT_TOTAL_P2_SHADOW_INSUFFICIENT_SAMPLE` | 465 | 30 |
| `PROP_DIVERGENCE_GATE` | 355 | 13 |
| `PROP_CAREER_FADE_FLIP` | 297 | 5 |
| `DIVERGENCE_GATE` | 226 | 15 |
| `MLB_SIDE_CONVICTION_GATE` | 144 | 6 |
| `ERA_RELIABILITY_GATE` | 140 | 3 |
| `PARK_GATE` | 126 | 5 |
| `NCAA_ERA_RELIABILITY_GATE` | 43 | 5 |
| `NHL_PACE_OVER_GATE` | 26 | 1 |
| `PROP_FADE_FLIP` | 23 | 4 |
| `SHARP_OPPOSES_BLOCK` | 6 | 3 |
| `HARD_VETO_DK_NCAA_BB_UNDERS` | 4 | 3 |
| `PACE_GATE` | 4 | 1 |
| `GAME_CAP` | 2 | 1 |
| `LINE_AGAINST_GATE` | 2 | 1 |

**Approx pass rate:** 15 fires / (15 fires + 75 distinct-blocked events) = **16.7%**

_Note: pass-rate is approximate — same event can be evaluated multiple times across the hourly pipeline; this counts distinct event_id only._

---

### Gate Block Summary (last 7 days)

**Total blocks logged: 9904** across 27 gate types


| Gate | Total | Distinct events | Avg/day |
|---|---|---|---|
| `PROP_DIVERGENCE_GATE` | 3370 | 63 | 481.4 |
| `BLOWOUT_GATE` | 1473 | 8 | 210.4 |
| `MLB_SIDE_CONVICTION_GATE` | 930 | 48 | 132.9 |
| `CONTEXT_TOTAL_P2_SHADOW_INSUFFICIENT_SAMPLE` | 822 | 54 | 117.4 |
| `ERA_RELIABILITY_GATE` | 738 | 23 | 105.4 |
| `PARK_GATE` | 630 | 33 | 90.0 |
| `DIVERGENCE_GATE` | 590 | 69 | 84.3 |
| `PROP_CAREER_FADE_FLIP` | 513 | 11 | 73.3 |
| `PROP_FADE_FLIP` | 425 | 24 | 60.7 |
| `NCAA_ERA_RELIABILITY_GATE` | 109 | 17 | 15.6 |
| `NHL_PACE_OVER_GATE` | 56 | 2 | 8.0 |
| `SHARP_OPPOSES_BLOCK` | 56 | 10 | 8.0 |
| `GAME_CAP` | 43 | 5 | 6.1 |
| `PACE_GATE` | 38 | 4 | 5.4 |
| `SPREAD_FADE_FLIP_DUAL_MODEL_VETO` | 27 | 1 | 3.9 |

### Daily breakdown — top 10 gates

| Gate | 04-20 | 04-21 | 04-22 | 04-23 | 04-24 | 04-25 | 04-26 |
|---|---|---|---|---|---|---|---|
| `PROP_DIVERGENCE_GATE` | 493 | 667 | 354 | 380 | 389 | 355 | 33 |
| `BLOWOUT_GATE` | 174 | 87 | 324 | 356 | 184 | · | · |
| `MLB_SIDE_CONVICTION_GATE` | 62 | 246 | 208 | 35 | 166 | 144 | · |
| `CONTEXT_TOTAL_P2_SHADOW_INSUFF` | · | · | 269 | 23 | 65 | 465 | · |
| `ERA_RELIABILITY_GATE` | 42 | 180 | 192 | 76 | 52 | 140 | · |
| `PARK_GATE` | 44 | 124 | 136 | 43 | 110 | 126 | · |
| `DIVERGENCE_GATE` | 17 | 100 | 92 | 26 | 122 | 226 | 7 |
| `PROP_CAREER_FADE_FLIP` | · | · | · | · | 167 | 297 | 49 |
| `PROP_FADE_FLIP` | 80 | 135 | 60 | 10 | 12 | 23 | · |
| `NCAA_ERA_RELIABILITY_GATE` | · | 13 | · | · | 50 | 43 | · |
