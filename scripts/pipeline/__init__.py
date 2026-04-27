"""
Pipeline stages for v26.0 generate_predictions() refactor.

Stages:
  1. fetch    — load ratings, market_consensus games, Elo, sport config
  2. score    — compute model spreads/totals/MLs (orchestrator.py, per_game.py)
  3. gate     — apply ~25 vetoes, log to shadow_blocked_picks (gates.py, stage_5_merge.py)
  4. channels — fade-flips, Context own-picks, book-arb, prop divergence (channels/, arb_scanner.py)
  5. route    — book selection, odds caps (stage_5_merge.py)
  6. merge    — concentration caps, final stake sizing (stage_5_merge.py)
"""
