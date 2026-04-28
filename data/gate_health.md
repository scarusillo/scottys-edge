## Gate Health Card — 2026-04-27

### Fires (graded)
| Sport | n | Units | P/L |
|---|---|---|---|
| basketball_nba | 5 | 25.0u | +12.68u |
| tennis_atp_madrid_open | 1 | 5.0u | -5.00u |
| baseball_ncaa | 1 | 5.0u | +4.42u |
| **TOTAL** | **7** | **35.0u** | **+12.10u** |

### Block volume by sport
| Sport | Distinct events | Total log entries |
|---|---|---|
| baseball_mlb | 8 | 254 |
| tennis_wta_madrid_open | 7 | 35 |
| tennis_atp_madrid_open | 5 | 19 |
| basketball_nba | 4 | 1187 |
| soccer_italy_serie_a | 2 | 15 |
| soccer_spain_la_liga | 1 | 9 |
| soccer_epl | 1 | 9 |

### Blocks by gate
| Gate | Total log entries | Distinct events |
|---|---|---|
| `PROP_PLAYOFF_ROLE_GATE_SHADOW` | 708 | 4 |
| `PROP_DIVERGENCE_GATE` | 288 | 7 |
| `PROP_CAREER_FADE_FLIP` | 176 | 3 |
| `PARK_GATE` | 82 | 6 |
| `ERA_RELIABILITY_GATE` | 78 | 3 |
| `DIVERGENCE_GATE` | 70 | 14 |
| `MLB_SIDE_CONVICTION_GATE` | 55 | 4 |
| `CONTEXT_TOTAL_P2_SHADOW_INSUFFICIENT_SAMPLE` | 33 | 4 |
| `PROP_FADE_FLIP` | 22 | 4 |
| `SPREAD_FADE_FLIP_DUAL_MODEL_VETO` | 16 | 2 |

**Approx pass rate:** 7 fires / (7 fires + 28 distinct-blocked events) = **20.0%**

_Note: pass-rate is approximate — same event can be evaluated multiple times across the hourly pipeline; this counts distinct event_id only._

---

### Gate Block Summary (last 7 days)

**Total blocks logged: 11075** across 28 gate types


| Gate | Total | Distinct events | Avg/day |
|---|---|---|---|
| `PROP_DIVERGENCE_GATE` | 2782 | 62 | 397.4 |
| `BLOWOUT_GATE` | 1497 | 9 | 213.9 |
| `PROP_CAREER_FADE_FLIP` | 1060 | 14 | 151.4 |
| `MLB_SIDE_CONVICTION_GATE` | 973 | 46 | 139.0 |
| `CONTEXT_TOTAL_P2_SHADOW_INSUFFICIENT_SAMPLE` | 966 | 72 | 138.0 |
| `PROP_PLAYOFF_ROLE_GATE_SHADOW` | 836 | 5 | 119.4 |
| `ERA_RELIABILITY_GATE` | 796 | 24 | 113.7 |
| `DIVERGENCE_GATE` | 710 | 85 | 101.4 |
| `PARK_GATE` | 692 | 34 | 98.9 |
| `PROP_FADE_FLIP` | 270 | 24 | 38.6 |
| `NCAA_ERA_RELIABILITY_GATE` | 120 | 21 | 17.1 |
| `NHL_PACE_OVER_GATE` | 82 | 3 | 11.7 |
| `SPREAD_FADE_FLIP_DUAL_MODEL_VETO` | 74 | 5 | 10.6 |
| `SHARP_OPPOSES_BLOCK` | 59 | 13 | 8.4 |
| `GAME_CAP` | 43 | 5 | 6.1 |

### Daily breakdown — top 10 gates

| Gate | 04-22 | 04-23 | 04-24 | 04-25 | 04-26 | 04-27 | 04-28 |
|---|---|---|---|---|---|---|---|
| `PROP_DIVERGENCE_GATE` | 354 | 380 | 389 | 355 | 329 | 288 | 20 |
| `BLOWOUT_GATE` | 324 | 356 | 184 | · | 459 | · | 87 |
| `PROP_CAREER_FADE_FLIP` | · | · | 167 | 297 | 410 | 176 | 10 |
| `MLB_SIDE_CONVICTION_GATE` | 208 | 35 | 166 | 144 | 119 | 55 | · |
| `CONTEXT_TOTAL_P2_SHADOW_INSUFF` | 269 | 23 | 65 | 465 | 111 | 33 | · |
| `PROP_PLAYOFF_ROLE_GATE_SHADOW` | · | · | · | · | 73 | 708 | 55 |
| `ERA_RELIABILITY_GATE` | 192 | 76 | 52 | 140 | 78 | 78 | · |
| `DIVERGENCE_GATE` | 92 | 26 | 122 | 226 | 74 | 70 | · |
| `PARK_GATE` | 136 | 43 | 110 | 126 | 71 | 82 | · |
| `PROP_FADE_FLIP` | 60 | 10 | 12 | 23 | 4 | 22 | 4 |
