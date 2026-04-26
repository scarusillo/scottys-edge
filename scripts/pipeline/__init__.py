"""
Pipeline stages for v26.0 generate_predictions() refactor.

Stages:
  1. fetch    — load ratings, market_consensus games, Elo, sport config
  2. score    — compute model spreads/totals/MLs (TODO)
  3. gate     — apply ~25 vetoes, log to shadow_blocked_picks (TODO)
  4. channels — fade-flips, Context own-picks, book-arb, prop divergence (TODO)
  5. route    — book selection, odds caps (TODO)
  6. merge    — concentration caps, final stake sizing (TODO)
"""
