## Gate Health Card — 2026-04-29

### Fires (graded)
| Sport | n | Units | P/L |
|---|---|---|---|
| basketball_nba | 5 | 25.0u | -15.61u |
| baseball_mlb | 5 | 25.0u | -5.90u |
| tennis_atp_madrid_open | 1 | 5.0u | -5.00u |
| baseball_ncaa | 1 | 5.0u | -5.00u |
| **TOTAL** | **12** | **60.0u** | **-31.51u** |

### Block volume by sport
| Sport | Distinct events | Total log entries |
|---|---|---|
| baseball_mlb | 17 | 573 |
| basketball_nba | 5 | 885 |
| tennis_atp_madrid_open | 1 | 5 |
| soccer_uefa_champs_league | 1 | 9 |
| icehockey_nhl | 1 | 1 |
| baseball_ncaa | 1 | 10 |

### Blocks by gate
| Gate | Total log entries | Distinct events |
|---|---|---|
| `PROP_PLAYOFF_ROLE_GATE` | 649 | 5 |
| `BLOWOUT_GATE` | 339 | 2 |
| `PROP_DIVERGENCE_GATE` | 138 | 6 |
| `MLB_SIDE_CONVICTION_GATE` | 80 | 7 |
| `ERA_RELIABILITY_GATE` | 64 | 3 |
| `PARK_GATE` | 63 | 6 |
| `PROP_CAREER_FADE_RECENCY_VETO` | 45 | 3 |
| `DIVERGENCE_GATE` | 20 | 3 |
| `MLB_CONTEXT_ML_SHADOW` | 18 | 6 |
| `SPREAD_FADE_FLIP_DUAL_MODEL_VETO` | 15 | 2 |
| `PROP_FADE_FLIP` | 12 | 2 |
| `CONTEXT_TOTAL_P2_SHADOW_INSUFFICIENT_SAMPLE` | 9 | 1 |
| `PROP_CAREER_FADE_FLIP` | 9 | 1 |
| `CONTEXT_DAILY_SPORT_CAP` | 8 | 1 |
| `PROP_EVENT_CAP` | 4 | 1 |
| _+4 more gates with smaller volume_ | | |

**Approx pass rate:** 12 fires / (12 fires + 26 distinct-blocked events) = **31.6%**

_Note: pass-rate is approximate — same event can be evaluated multiple times across the hourly pipeline; this counts distinct event_id only._

---

### Gate Block Summary (last 7 days)

**Total blocks logged: 11201** across 33 gate types


| Gate | Total | Distinct events | Avg/day |
|---|---|---|---|
| `PROP_DIVERGENCE_GATE` | 2335 | 56 | 333.6 |
| `BLOWOUT_GATE` | 1777 | 10 | 253.9 |
| `PROP_CAREER_FADE_FLIP` | 1085 | 18 | 155.0 |
| `PROP_PLAYOFF_ROLE_GATE` | 1023 | 6 | 146.1 |
| `PROP_PLAYOFF_ROLE_GATE_SHADOW` | 955 | 8 | 136.4 |
| `CONTEXT_TOTAL_P2_SHADOW_INSUFFICIENT_SAMPLE` | 723 | 58 | 103.3 |
| `MLB_SIDE_CONVICTION_GATE` | 669 | 42 | 95.6 |
| `DIVERGENCE_GATE` | 598 | 74 | 85.4 |
| `PARK_GATE` | 582 | 37 | 83.1 |
| `ERA_RELIABILITY_GATE` | 546 | 21 | 78.0 |
| `PROP_CAREER_FADE_RECENCY_VETO` | 247 | 6 | 35.3 |
| `SPREAD_FADE_FLIP_DUAL_MODEL_VETO` | 117 | 9 | 16.7 |
| `NCAA_ERA_RELIABILITY_GATE` | 107 | 19 | 15.3 |
| `PROP_FADE_FLIP` | 98 | 17 | 14.0 |
| `NHL_PACE_OVER_GATE` | 97 | 4 | 13.9 |

### Daily breakdown — top 10 gates

| Gate | 04-24 | 04-25 | 04-26 | 04-27 | 04-28 | 04-29 | 04-30 |
|---|---|---|---|---|---|---|---|
| `PROP_DIVERGENCE_GATE` | 389 | 355 | 329 | 288 | 447 | 138 | 9 |
| `BLOWOUT_GATE` | 184 | · | 459 | · | 439 | 339 | · |
| `PROP_CAREER_FADE_FLIP` | 167 | 297 | 410 | 176 | 26 | 9 | · |
| `PROP_PLAYOFF_ROLE_GATE` | · | · | · | · | 359 | 649 | 15 |
| `PROP_PLAYOFF_ROLE_GATE_SHADOW` | · | · | 73 | 708 | 174 | · | · |
| `CONTEXT_TOTAL_P2_SHADOW_INSUFF` | 65 | 465 | 111 | 33 | 17 | 9 | · |
| `MLB_SIDE_CONVICTION_GATE` | 166 | 144 | 119 | 55 | 70 | 80 | · |
| `DIVERGENCE_GATE` | 122 | 226 | 74 | 70 | 60 | 20 | · |
| `PARK_GATE` | 110 | 126 | 71 | 82 | 87 | 63 | · |
| `ERA_RELIABILITY_GATE` | 52 | 140 | 78 | 78 | 58 | 64 | · |
