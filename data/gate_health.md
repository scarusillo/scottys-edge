## Gate Health Card — 2026-04-24

### Fires (graded)
| Sport | n | Units | P/L |
|---|---|---|---|
| basketball_nba | 6 | 30.0u | +7.08u |
| tennis_atp_madrid_open | 2 | 7.0u | +6.36u |
| baseball_ncaa | 2 | 10.0u | -0.58u |
| tennis_wta_madrid_open | 1 | 3.5u | -3.50u |
| icehockey_nhl | 1 | 5.0u | +4.55u |
| baseball_mlb | 1 | 5.0u | +0.00u |
| **TOTAL** | **13** | **60.5u** | **+13.91u** |

### Block volume by sport
| Sport | Distinct events | Total log entries |
|---|---|---|
| baseball_ncaa | 16 | 83 |
| baseball_mlb | 12 | 540 |
| tennis_wta_madrid_open | 7 | 53 |
| basketball_nba | 5 | 549 |
| tennis_atp_madrid_open | 4 | 16 |
| icehockey_nhl | 3 | 87 |
| soccer_spain_la_liga | 1 | 18 |
| soccer_italy_serie_a | 1 | 18 |
| soccer_germany_bundesliga | 1 | 16 |
| soccer_france_ligue_one | 1 | 18 |
| soccer_epl | 1 | 18 |

### Blocks by gate
| Gate | Total log entries | Distinct events |
|---|---|---|
| `PROP_DIVERGENCE_GATE` | 389 | 12 |
| `BLOWOUT_GATE` | 184 | 1 |
| `PROP_CAREER_FADE_FLIP` | 167 | 3 |
| `MLB_SIDE_CONVICTION_GATE` | 166 | 6 |
| `DIVERGENCE_GATE` | 122 | 15 |
| `PARK_GATE` | 110 | 4 |
| `CONTEXT_TOTAL_P2_SHADOW_INSUFFICIENT_SAMPLE` | 65 | 4 |
| `ERA_RELIABILITY_GATE` | 52 | 1 |
| `NCAA_ERA_RELIABILITY_GATE` | 50 | 8 |
| `NHL_PACE_OVER_GATE` | 30 | 1 |
| `SPREAD_FADE_FLIP_DUAL_MODEL_VETO` | 27 | 1 |
| `PROP_FADE_FLIP` | 12 | 4 |
| `SHARP_OPPOSES_BLOCK` | 12 | 3 |
| `CLV_MICRO_EDGE_BORDERLINE` | 10 | 3 |
| `LINE_AGAINST_GATE` | 5 | 1 |
| _+7 more gates with smaller volume_ | | |

**Approx pass rate:** 13 fires / (13 fires + 52 distinct-blocked events) = **20.0%**

_Note: pass-rate is approximate — same event can be evaluated multiple times across the hourly pipeline; this counts distinct event_id only._

---

### Gate Block Summary (last 7 days)

**Total blocks logged: 11540** across 28 gate types


| Gate | Total | Distinct events | Avg/day |
|---|---|---|---|
| `PROP_DIVERGENCE_GATE` | 4716 | 66 | 673.7 |
| `BLOWOUT_GATE` | 1821 | 9 | 260.1 |
| `MLB_SIDE_CONVICTION_GATE` | 973 | 58 | 139.0 |
| `ERA_RELIABILITY_GATE` | 694 | 23 | 99.1 |
| `PARK_GATE` | 669 | 41 | 95.6 |
| `CONTEXT_TOTAL_P2_SHADOW_INSUFFICIENT_SAMPLE` | 657 | 54 | 93.9 |
| `PROP_FADE_FLIP` | 650 | 27 | 92.9 |
| `DIVERGENCE_GATE` | 576 | 72 | 82.3 |
| `PROP_CAREER_FADE_FLIP` | 359 | 7 | 51.3 |
| `NCAA_ERA_RELIABILITY_GATE` | 93 | 18 | 13.3 |
| `SHARP_OPPOSES_BLOCK` | 55 | 10 | 7.9 |
| `NHL_PACE_OVER_GATE` | 54 | 3 | 7.7 |
| `PACE_GATE` | 44 | 5 | 6.3 |
| `GAME_CAP` | 41 | 4 | 5.9 |
| `SPREAD_FADE_FLIP_DUAL_MODEL_VETO` | 27 | 1 | 3.9 |

### Daily breakdown — top 10 gates

| Gate | 04-19 | 04-20 | 04-21 | 04-22 | 04-23 | 04-24 | 04-25 |
|---|---|---|---|---|---|---|---|
| `PROP_DIVERGENCE_GATE` | 699 | 493 | 667 | 354 | 380 | 389 | 212 |
| `BLOWOUT_GATE` | 348 | 174 | 87 | 324 | 356 | 184 | · |
| `MLB_SIDE_CONVICTION_GATE` | 69 | 62 | 246 | 208 | 35 | 166 | 90 |
| `ERA_RELIABILITY_GATE` | 56 | 42 | 180 | 192 | 76 | 52 | 96 |
| `PARK_GATE` | 47 | 44 | 124 | 136 | 43 | 110 | 78 |
| `CONTEXT_TOTAL_P2_SHADOW_INSUFF` | · | · | · | 269 | 23 | 65 | 300 |
| `PROP_FADE_FLIP` | 105 | 80 | 135 | 60 | 10 | 12 | 22 |
| `DIVERGENCE_GATE` | · | 17 | 100 | 92 | 26 | 122 | 144 |
| `PROP_CAREER_FADE_FLIP` | · | · | · | · | · | 167 | 192 |
| `NCAA_ERA_RELIABILITY_GATE` | 3 | · | 13 | · | · | 50 | 24 |
