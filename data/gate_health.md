## Gate Health Card — 2026-04-26

### Fires (graded)
| Sport | n | Units | P/L |
|---|---|---|---|
| baseball_ncaa | 4 | 20.0u | -0.48u |
| icehockey_nhl | 3 | 15.0u | -10.00u |
| basketball_nba | 3 | 15.0u | +3.90u |
| baseball_mlb | 3 | 15.0u | +3.90u |
| tennis_atp_madrid_open | 1 | 5.0u | +3.85u |
| soccer_spain_la_liga | 1 | 5.0u | +3.85u |
| soccer_italy_serie_a | 1 | 5.0u | -5.00u |
| **TOTAL** | **16** | **80.0u** | **+0.02u** |

### Block volume by sport
| Sport | Distinct events | Total log entries |
|---|---|---|
| baseball_ncaa | 15 | 29 |
| baseball_mlb | 15 | 783 |
| soccer_france_ligue_one | 5 | 34 |
| basketball_nba | 5 | 827 |
| tennis_wta_madrid_open | 4 | 16 |
| soccer_spain_la_liga | 4 | 31 |
| tennis_atp_madrid_open | 3 | 19 |
| soccer_italy_serie_a | 3 | 24 |
| icehockey_nhl | 3 | 30 |
| soccer_germany_bundesliga | 2 | 8 |
| soccer_usa_mls | 1 | 22 |

### Blocks by gate
| Gate | Total log entries | Distinct events |
|---|---|---|
| `BLOWOUT_GATE` | 459 | 2 |
| `PROP_CAREER_FADE_FLIP` | 410 | 5 |
| `PROP_DIVERGENCE_GATE` | 329 | 14 |
| `MLB_SIDE_CONVICTION_GATE` | 119 | 9 |
| `CONTEXT_TOTAL_P2_SHADOW_INSUFFICIENT_SAMPLE` | 111 | 14 |
| `ERA_RELIABILITY_GATE` | 78 | 4 |
| `DIVERGENCE_GATE` | 74 | 11 |
| `PROP_PLAYOFF_ROLE_GATE_SHADOW` | 73 | 2 |
| `PARK_GATE` | 71 | 5 |
| `SPREAD_FADE_FLIP_DUAL_MODEL_VETO` | 31 | 2 |
| `NHL_PACE_OVER_GATE` | 26 | 1 |
| `NCAA_ERA_RELIABILITY_GATE` | 14 | 6 |
| `HARD_VETO_DK_NCAA_BB_UNDERS` | 10 | 5 |
| `PROP_BOOK_ARB_SHADOW` | 4 | 2 |
| `PROP_FADE_FLIP` | 4 | 2 |
| _+5 more gates with smaller volume_ | | |

**Approx pass rate:** 16 fires / (16 fires + 60 distinct-blocked events) = **21.1%**

_Note: pass-rate is approximate — same event can be evaluated multiple times across the hourly pipeline; this counts distinct event_id only._

---

### Gate Block Summary (last 7 days)

**Total blocks logged: 10326** across 28 gate types


| Gate | Total | Distinct events | Avg/day |
|---|---|---|---|
| `PROP_DIVERGENCE_GATE` | 2976 | 61 | 425.1 |
| `BLOWOUT_GATE` | 1584 | 8 | 226.3 |
| `MLB_SIDE_CONVICTION_GATE` | 980 | 47 | 140.0 |
| `CONTEXT_TOTAL_P2_SHADOW_INSUFFICIENT_SAMPLE` | 933 | 68 | 133.3 |
| `PROP_CAREER_FADE_FLIP` | 874 | 11 | 124.9 |
| `ERA_RELIABILITY_GATE` | 760 | 23 | 108.6 |
| `DIVERGENCE_GATE` | 657 | 73 | 93.9 |
| `PARK_GATE` | 654 | 31 | 93.4 |
| `PROP_FADE_FLIP` | 324 | 23 | 46.3 |
| `NCAA_ERA_RELIABILITY_GATE` | 120 | 21 | 17.1 |
| `PROP_PLAYOFF_ROLE_GATE_SHADOW` | 90 | 2 | 12.9 |
| `NHL_PACE_OVER_GATE` | 82 | 3 | 11.7 |
| `SHARP_OPPOSES_BLOCK` | 59 | 13 | 8.4 |
| `SPREAD_FADE_FLIP_DUAL_MODEL_VETO` | 58 | 3 | 8.3 |
| `GAME_CAP` | 43 | 5 | 6.1 |

### Daily breakdown — top 10 gates

| Gate | 04-21 | 04-22 | 04-23 | 04-24 | 04-25 | 04-26 | 04-27 |
|---|---|---|---|---|---|---|---|
| `PROP_DIVERGENCE_GATE` | 667 | 354 | 380 | 389 | 355 | 329 | 9 |
| `BLOWOUT_GATE` | 87 | 324 | 356 | 184 | · | 459 | · |
| `MLB_SIDE_CONVICTION_GATE` | 246 | 208 | 35 | 166 | 144 | 119 | · |
| `CONTEXT_TOTAL_P2_SHADOW_INSUFF` | · | 269 | 23 | 65 | 465 | 111 | · |
| `PROP_CAREER_FADE_FLIP` | · | · | · | 167 | 297 | 410 | · |
| `ERA_RELIABILITY_GATE` | 180 | 192 | 76 | 52 | 140 | 78 | · |
| `DIVERGENCE_GATE` | 100 | 92 | 26 | 122 | 226 | 74 | · |
| `PARK_GATE` | 124 | 136 | 43 | 110 | 126 | 71 | · |
| `PROP_FADE_FLIP` | 135 | 60 | 10 | 12 | 23 | 4 | · |
| `NCAA_ERA_RELIABILITY_GATE` | 13 | · | · | 50 | 43 | 14 | · |
