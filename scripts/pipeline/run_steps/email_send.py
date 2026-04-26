"""
Steps 9 + email — generate HTML/PNG cards, run sanity checks, print step
timings, send the picks email + the captions/playbook email, and the no-edge
fallback card on scheduled-only runs.

Returns (skip_remainder, png_card_path, png_card_paths) so the caller can:
  - short-circuit the rest of cmd_run when no picks fired on a non-scheduled
    hour (skip_remainder=True),
  - feed the PNG card paths into the Step 9c social-post stage.

Extracted from main.py cmd_run() Step 9 in v26.0 Phase 8.
"""
from datetime import datetime


def prepare_and_send_email(conn, all_picks, do_email, run_type,
                           total_odds_fetched, _step_timings, _step6_breakdown,
                           research_brief, _model_b_report, _log, _mark):
    """Run the full Step 9 email-prep + send sequence.

    Args:
        conn:              open sqlite3.Connection (passed to sanity checks +
                           captions section)
        all_picks:         list[dict] — final picks to email
        do_email:          bool — `--email` flag
        run_type:          'Morning' / 'Afternoon' / 'Evening'
        total_odds_fetched:int — used in the no-edge alert email body
        _step_timings:     dict from cmd_run for the timing summary
        _step6_breakdown:  list of (sport, dur, n_picks) tuples for Step 6 timing
        research_brief:    string from Step 5c (or None)
        _model_b_report:   string from Step 6b (or empty)
        _log:              logging.Logger
        _mark:             timing closure (called once after this stage)

    Returns: (skip_remainder, png_card_path, png_card_paths)
        skip_remainder (bool): True → caller should `return` from cmd_run
                               without running Step 9c (social post).
        png_card_path (str|None): primary card path for social post.
        png_card_paths (list[str]): all card paths.
    """
    # Lazy imports for module-level helpers used inside the body.
    from main import (
        _generate_html_card, _social_media_card, _scan_arbs,
    )
    from model_engine import picks_to_text

    # Step 9: Email (with inline HTML card)
    html_content = None
    if all_picks:
        try:
            html_path, html_content = _generate_html_card(all_picks)
        except Exception as e:
            html_path = None
            html_content = None
            print(f"  HTML card: {e}")

    # Generate PNG card
    png_card_path = None
    png_card_paths = []
    if all_picks:
        try:
            from card_image import generate_card_image
            result = generate_card_image(all_picks)
            if isinstance(result, list):
                png_card_paths = result
                png_card_path = result[0]  # Primary card for email attachment
            else:
                png_card_path = result
                png_card_paths = [result]
        except Exception as e:
            print(f"  PNG card: {e}")

    # v25: Pipeline sanity check — flag risky picks before emailing
    _warnings = []
    if all_picks:
        try:
            _today_str = datetime.now().strftime('%Y-%m-%d')

            # Check 1: Same player lost on ANY prop recently (not just exact selection)
            # Catches: Abrams lost on RUNS yesterday, now firing on RBIS today
            for p in all_picks:
                if p.get('market_type') == 'PROP':
                    import re as _re
                    _pm = _re.match(r'^(.+?)\s+(OVER|UNDER)\s+', p.get('selection', ''))
                    _player_name = _pm.group(1) if _pm else p.get('selection', '')
                    _recent_loss = conn.execute("""
                        SELECT selection, pnl_units, clv, DATE(created_at) as dt
                        FROM graded_bets
                        WHERE selection LIKE ? AND result = 'LOSS'
                        AND DATE(created_at) >= DATE('now', '-7 days')
                        ORDER BY created_at DESC LIMIT 1
                    """, (f"{_player_name}%",)).fetchone()
                    if _recent_loss:
                        _clv_info = f", CLV:{_recent_loss[2]:+.1f}%" if _recent_loss[2] is not None else ""
                        _warnings.append(
                            f"REPEAT LOSS: {p['selection'][:40]} — {_player_name} lost {_recent_loss[1]:+.1f}u on {_recent_loss[3]} ({_recent_loss[0][:30]}){_clv_info}")

            # Check 2: Single game with 3+ picks (concentration)
            _game_picks = {}
            for p in all_picks:
                eid = p.get('event_id', '')
                if eid:
                    _game_picks[eid] = _game_picks.get(eid, 0) + 1
            for eid, cnt in _game_picks.items():
                if cnt >= 3:
                    _warnings.append(f"CONCENTRATION: {cnt} picks on same game (event {eid[:12]}...)")

            # Check 3: Total exposure today >30u
            _today_units = conn.execute("""
                SELECT COALESCE(SUM(units), 0) FROM bets
                WHERE DATE(created_at) = ? AND result IS NULL
            """, (_today_str,)).fetchone()[0]
            _new_units = sum(p.get('units', 0) for p in all_picks)
            if _today_units + _new_units > 30:
                _warnings.append(f"EXPOSURE: {_today_units + _new_units:.0f}u total today (existing {_today_units:.0f}u + new {_new_units:.0f}u)")

            if _warnings:
                print(f"\n  ⚠️ SANITY CHECK WARNINGS:")
                for _w in _warnings:
                    print(f"    {_w}")
        except Exception as e:
            print(f"  Sanity check: {e}")

    _mark('step8_merge_dedup')

    # v25.34: per-step timing summary — helps identify which step is eating
    # time in slow runs. Print before email so it's visible in the log.
    print("\n⏱️  Per-step timing:")
    _total = sum(_step_timings.values())
    for _k, _v in _step_timings.items():
        pct = (_v / _total * 100) if _total > 0 else 0
        print(f"    {_k:30s} {_v:6.1f}s  {pct:5.1f}%")
    print(f"    {'TOTAL (pre-email)':30s} {_total:6.1f}s")

    # Per-sport breakdown of Step 6 (predictions) — reveals which sport is slow
    if _step6_breakdown:
        print("\n⏱️  Step 6 per-sport (sorted slowest first):")
        for _sp, _dur, _n_picks in sorted(_step6_breakdown, key=lambda x: -x[1]):
            if _dur < 1.0: continue  # skip trivial sports
            print(f"    {_sp:30s} {_dur:6.1f}s  ({_n_picks} picks)")

    if do_email:
        print("\n📧 Step 9: Sending email...")
        if all_picks:
            from emailer import send_picks_email, send_email
            text = picks_to_text(all_picks, f"{run_type} Picks")
            # Append sanity check warnings to email
            if _warnings:
                text += "\n\n" + "⚠️ " * 10 + "\n"
                text += "  SANITY CHECK WARNINGS\n"
                text += "⚠️ " * 10 + "\n\n"
                for _w in _warnings:
                    text += f"  {_w}\n"
            # Append research intel to picks email
            if research_brief:
                text += "\n\n" + "═" * 50 + "\n"
                text += "  PRE-GAME INTEL\n"
                text += "═" * 50 + "\n\n"
                text += research_brief
            # Append Model B shadow report
            if _model_b_report:
                text += "\n\n" + _model_b_report
            social = _social_media_card(all_picks)
            full_text = text + "\n\n" + social
            email_ok = send_picks_email(full_text, run_type, html_body=html_content,
                            attachment_path=png_card_path,
                            attachment_paths=png_card_paths if len(png_card_paths) > 1 else None)
            if not email_ok:
                print("  ❌ EMAIL FAILED — picks were saved but not delivered. Check GMAIL_APP_PASSWORD env var.")

            # Separate caption email (plain text, copyable from phone)
            # v25.3: Twitter caption + threads removed — account suspended April 2026.
            try:
                from card_image import generate_caption, generate_pick_writeups
                ig_caption = generate_caption(all_picks)
                if ig_caption:
                    # Per-pick write-ups for engagement posts
                    writeups = generate_pick_writeups(all_picks)

                    caption_text = "INSTAGRAM CAPTION:\n" + "="*40 + "\n" + ig_caption
                    if writeups:
                        caption_text += "\n\n" + "INDIVIDUAL PICK POSTS (copy-paste for engagement):\n" + "="*40 + writeups

                    # v17: Growth playbook — accounts to engage, ready-to-post content
                    _season = conn.execute("""
                        SELECT SUM(CASE WHEN result='WIN' THEN 1 ELSE 0 END),
                               SUM(CASE WHEN result='LOSS' THEN 1 ELSE 0 END),
                               SUM(pnl_units)
                        FROM graded_bets WHERE DATE(created_at) >= '2026-03-04'
                        AND result NOT IN ('DUPLICATE','PENDING','TAINTED') AND units >= 3.5
                    """).fetchone()
                    _sw, _sl, _sp = _season[0] or 0, _season[1] or 0, _season[2] or 0
                    _wr = _sw/(_sw+_sl)*100 if (_sw+_sl) > 0 else 0
                    # Use all today's picks (including earlier runs) for the caption playbook
                    _all_today = conn.execute("""
                        SELECT selection, odds, units FROM bets
                        WHERE created_at >= ? AND result IS NULL ORDER BY units DESC
                    """, (today,)).fetchall() if 'today' in dir() else []
                    _best_pick = _all_today[0] if _all_today else (all_picks[0] if all_picks else None)
                    _bp_sel = _best_pick[0] if _best_pick and isinstance(_best_pick, tuple) else (_best_pick['selection'] if _best_pick else 'Check card')
                    _bp_odds_raw = _best_pick[1] if _best_pick and isinstance(_best_pick, tuple) else (_best_pick.get('odds') if _best_pick else None)
                    _bp_odds = f"({_bp_odds_raw:+.0f})" if _bp_odds_raw else ''

                    # v24: Reddit post — standalone for r/sportsbetting + thread comments
                    _pick_lines = []
                    for _p in sorted(all_picks, key=lambda x: x.get('units', 0), reverse=True):
                        if _p.get('units', 0) >= 3.5:
                            _odds_str = f"({_p['odds']:+.0f})" if _p.get('odds') else ''
                            _u = _p.get('units', 0)
                            _tag = ' — MAX PLAY' if _u >= 5.0 else ''
                            _pick_lines.append(f"**{_p['selection']}** {_odds_str} {_u:.0f}u{_tag}")
                    if _pick_lines:
                        _today_str = datetime.now().strftime('%A %m/%d')
                        _reddit_body = f"Title: {_today_str} Picks — {_sw}W-{_sl}L ({_wr:.0f}%) season, all tracked\n\n"
                        _reddit_body += "Body:\n\n"
                        _reddit_body += f"{len(_pick_lines)} plays for {datetime.now().strftime('%A')}. Full transparency — every pick graded, every loss shown.\n\n"
                        _reddit_body += "\n\n".join(_pick_lines)
                        _reddit_body += f"\n\nSeason: {_sw}-{_sl} ({_wr:.0f}%) | {_sp:+.0f}u\n\n"
                        _reddit_body += "All picks tracked at scottys_edge on IG. Discord: discord.gg/JQ6rRfuN\n\n"
                        _reddit_body += "---\nPost in: r/sportsbetting (standalone), r/sportsbook (daily thread comment)"
                        caption_text += "\n\n" + "REDDIT POST (r/sportsbetting):\n" + "="*40 + "\n" + _reddit_body

                    # v25.3: Twitter sections removed from growth playbook —
                    # @Scottys_Edge suspended April 2026. IG + Discord + Reddit only.
                    growth_section = f"""

    GROWTH PLAYBOOK
    {'='*40}

    ACCOUNTS TO TAG (on your image, not caption):
      IG: @actionnetworkhq @baborofficial @bettingcappers @vegasinsider

    ACCOUNTS TO COMMENT ON (within 30 min of their posts):
      @ActionNetworkHQ @ESPNBet @BleacherReport — reply with your model's take

    TONIGHT'S CHECKLIST:
    {'='*40}
    [ ] Post picks card to IG feed + story (tag 4 accounts ON image)
    [ ] Comment on 2 big account posts (within 30 min)
    [ ] After wins hit: post results card + "Called it" story
    """
                    # v25: Reddit engagement comments (for team subs + betting subs)
                    from card_image import generate_engagement_comments
                    _eng_comments = generate_engagement_comments(all_picks)
                    if _eng_comments:
                        _reddit_comments = [c for c in _eng_comments if c['platform'] == 'reddit']
                        if _reddit_comments:
                            caption_text += "\n\n" + "REDDIT COMMENTS (team subs + betting subs):\n" + "=" * 40
                            _seen_targets = set()
                            for _rc in _reddit_comments:
                                _key = (_rc['target'], _rc['pick'])
                                if _key in _seen_targets:
                                    continue
                                _seen_targets.add(_key)
                                caption_text += f"\n\n{_rc['target']} — {_rc['game']} ({_rc['sport']}):\n"
                                caption_text += f"{_rc['comment']}"

                    caption_text += growth_section

                    # v24: Timing confidence tags — early picks historically capture better CLV
                    try:
                        _current_hour = datetime.now().hour
                        _timing_lines = []
                        for _p in all_picks:
                            if _p.get('units', 0) < 3.5:
                                continue
                            _sel = _p.get('selection', '')
                            if _current_hour < 8:
                                _timing_lines.append(f"  EARLY LINE CAPTURE: {_sel} — historically +1.04 avg CLV, 61% WR before 8am")
                            elif _current_hour < 11:
                                _timing_lines.append(f"  MORNING CAPTURE: {_sel} — lines still settling, monitor for movement")
                            elif _current_hour >= 17:
                                _timing_lines.append(f"  LATE ENTRY: {_sel} — evening picks historically 52% WR, lines fully baked")
                        if _timing_lines:
                            caption_text += f"\n\nTIMING INSIGHT\n{'='*40}\n"
                            caption_text += '\n'.join(_timing_lines)
                            if _current_hour < 11:
                                caption_text += "\n\n  Early/morning picks capture the most CLV. These are your highest-conviction windows."
                            elif _current_hour >= 17:
                                caption_text += "\n\n  Evening picks have thinner edges — market has had all day to settle. Size conservatively."
                    except Exception:
                        pass

                    # v24: Arb scanner — find cross-book arbitrage opportunities
                    try:
                        arb_section = _scan_arbs(conn)
                        if arb_section:
                            caption_text += arb_section
                    except Exception as _arb_e:
                        print(f"  Arb scan: {_arb_e}")

                    today = datetime.now().strftime('%Y-%m-%d')
                    send_email(f"Social Captions - {run_type} {today}", caption_text)
                    print("  Captions + pick write-ups email sent")

                    # Save engagement comments JSON for Cowork automation
                    try:
                        from card_image import save_engagement_comments
                        _cw_path = save_engagement_comments(all_picks)
                        if _cw_path:
                            _cw_count = len(generate_engagement_comments(all_picks))
                            print(f"  Cowork comments saved: {_cw_path} ({_cw_count} comments)")
                    except Exception as _cw_e:
                        print(f"  Cowork comments: {_cw_e}")
            except Exception as e:
                print(f"  Captions: {e}")
        else:
            # No picks found — send no-edge card + captions on the 11am and 5:30pm
            # scheduled runs only. Other hours (8am opener, ad-hoc) skip to avoid clutter.
            _hour = datetime.now().hour
            if _hour not in (10, 11, 17):
                print("  No new picks — skipping email (only 11am/5:30pm runs send no-edge cards)")
                return (True, None, [])
            from emailer import send_picks_email, send_email
            from datetime import datetime
            try:
                from card_image import generate_card_image, generate_caption
                no_edge_path = generate_card_image([])  # Generates no-edge card
                today = datetime.now().strftime('%Y-%m-%d')
                no_edge_msg = "No plays today — model didn't find enough edge."
                if total_odds_fetched == 0:
                    no_edge_msg += "\n\nNote: Zero odds data was available — this may indicate an API outage rather than a genuine no-edge day."
                email_ok = send_picks_email(no_edge_msg, run_type, attachment_path=no_edge_path)
                if not email_ok:
                    print("  ❌ EMAIL FAILED — picks were saved but not delivered. Check GMAIL_APP_PASSWORD env var.")
                # Caption email
                caption = generate_caption([])
                if caption:
                    caption_text = "INSTAGRAM CAPTION:\n" + "="*40 + "\n" + caption + "\n\n" + "TWITTER CAPTION:\n" + "="*40 + "\nNo plays tonight.\n\nDiscipline is the edge. We only bet when the data says to bet.\n\nBack tomorrow.\n\n#SportsBetting #FreePicks #BettingCommunity"
                    send_email(f"Social Captions - {run_type} {today}", caption_text)
                print("  No-edge card + caption sent")
            except Exception as e:
                print(f"  No-edge email: {e}")

    _log.info(f"Step 9: Email {'sent' if do_email else 'skipped'}")

    _mark('step9_email')
    return (False, png_card_path, png_card_paths)
