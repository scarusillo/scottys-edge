## Gate Health Card — 2026-04-28

### Fires (graded)
| Sport | n | Units | P/L |
|---|---|---|---|
| baseball_ncaa | 5 | 25.0u | +2.81u |
| basketball_nba | 4 | 20.0u | -1.47u |
| baseball_mlb | 3 | 15.0u | -5.73u |
| **TOTAL** | **12** | **60.0u** | **-4.39u** |

### Block volume by sport
| Sport | Distinct events | Total log entries |
|---|---|---|
| baseball_mlb | 14 | 679 |
| baseball_ncaa | 11 | 79 |
| basketball_nba | 6 | 1241 |
| tennis_atp_madrid_open | 3 | 7 |
| soccer_uefa_champs_league | 2 | 17 |
| icehockey_nhl | 2 | 30 |

### Blocks by gate
| Gate | Total log entries | Distinct events |
|---|---|---|
| `PROP_DIVERGENCE_GATE` | 447 | 12 |
| `BLOWOUT_GATE` | 439 | 3 |
| `PROP_PLAYOFF_ROLE_GATE` | 359 | 3 |
| `PROP_CAREER_FADE_RECENCY_VETO` | 200 | 3 |
| `PROP_PLAYOFF_ROLE_GATE_SHADOW` | 174 | 6 |
| `PARK_GATE` | 87 | 7 |
| `MLB_SIDE_CONVICTION_GATE` | 70 | 6 |
| `DIVERGENCE_GATE` | 60 | 7 |
| `ERA_RELIABILITY_GATE` | 58 | 3 |
| `RAW_EDGE_FLIP` | 30 | 5 |
| `SPREAD_FADE_FLIP_DUAL_MODEL_VETO` | 28 | 2 |
| `PROP_CAREER_FADE_FLIP` | 26 | 6 |
| `CONTEXT_TOTAL_P2_SHADOW_INSUFFICIENT_SAMPLE` | 17 | 2 |
| `NHL_PACE_OVER_GATE` | 15 | 1 |
| `PROP_FADE_FLIP` | 15 | 3 |
| _+6 more gates with smaller volume_ | | |

**Approx pass rate:** 12 fires / (12 fires + 38 distinct-blocked events) = **24.0%**

_Note: pass-rate is approximate — same event can be evaluated multiple times across the hourly pipeline; this counts distinct event_id only._

---

### Gate Block Summary (last 7 days)

**Total blocks logged: 11487** across 31 gate types


| Gate | Total | Distinct events | Avg/day |
|---|---|---|---|
| `PROP_DIVERGENCE_GATE` | 2561 | 62 | 365.9 |
| `BLOWOUT_GATE` | 1840 | 11 | 262.9 |
| `PROP_CAREER_FADE_FLIP` | 1076 | 17 | 153.7 |
| `CONTEXT_TOTAL_P2_SHADOW_INSUFFICIENT_SAMPLE` | 983 | 74 | 140.4 |
| `PROP_PLAYOFF_ROLE_GATE_SHADOW` | 955 | 8 | 136.4 |
| `MLB_SIDE_CONVICTION_GATE` | 797 | 43 | 113.9 |
| `ERA_RELIABILITY_GATE` | 674 | 23 | 96.3 |
| `DIVERGENCE_GATE` | 670 | 84 | 95.7 |
| `PARK_GATE` | 655 | 36 | 93.6 |
| `PROP_PLAYOFF_ROLE_GATE` | 381 | 3 | 54.4 |
| `PROP_CAREER_FADE_RECENCY_VETO` | 207 | 3 | 29.6 |
| `PROP_FADE_FLIP` | 146 | 21 | 20.9 |
| `NCAA_ERA_RELIABILITY_GATE` | 107 | 19 | 15.3 |
| `SPREAD_FADE_FLIP_DUAL_MODEL_VETO` | 102 | 7 | 14.6 |
| `NHL_PACE_OVER_GATE` | 97 | 4 | 13.9 |

### Daily breakdown — top 10 gates

| Gate | 04-23 | 04-24 | 04-25 | 04-26 | 04-27 | 04-28 | 04-29 |
|---|---|---|---|---|---|---|---|
| `PROP_DIVERGENCE_GATE` | 380 | 389 | 355 | 329 | 288 | 447 | 19 |
| `BLOWOUT_GATE` | 356 | 184 | · | 459 | · | 439 | 78 |
| `PROP_CAREER_FADE_FLIP` | · | 167 | 297 | 410 | 176 | 26 | · |
| `CONTEXT_TOTAL_P2_SHADOW_INSUFF` | 23 | 65 | 465 | 111 | 33 | 17 | · |
| `PROP_PLAYOFF_ROLE_GATE_SHADOW` | · | · | · | 73 | 708 | 174 | · |
| `MLB_SIDE_CONVICTION_GATE` | 35 | 166 | 144 | 119 | 55 | 70 | · |
| `ERA_RELIABILITY_GATE` | 76 | 52 | 140 | 78 | 78 | 58 | · |
| `DIVERGENCE_GATE` | 26 | 122 | 226 | 74 | 70 | 60 | · |
| `PARK_GATE` | 43 | 110 | 126 | 71 | 82 | 87 | · |
| `PROP_PLAYOFF_ROLE_GATE` | · | · | · | · | · | 359 | 22 |
