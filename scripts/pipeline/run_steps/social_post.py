"""
Step 9c — Auto-post picks to Discord + Instagram.

Discord webhook always fires (low cost, no rate limit concerns). Instagram
posts each PNG card as a story (instagrapi). Twitter/X removed in v25.3
after the @Scottys_Edge account was suspended (April 2026).

Extracted from main.py cmd_run() Step 9c in v26.0 Phase 8.
"""


def post_to_social(all_picks, png_card_path, png_card_paths):
    """Post final picks to Discord + Instagram stories.

    Args:
        all_picks:        list[dict] — final picks (no-op if empty)
        png_card_path:    primary card path (used as fallback when paths empty)
        png_card_paths:   list of card paths for multi-card stories

    Returns: None. All errors caught + printed.
    """
    if not all_picks:
        return

    try:
        from social_media import post_picks_social
        post_picks_social(all_picks)
    except Exception as e:
        print(f"  Social media: {e}")

    try:
        from social_media import post_picks_to_instagram
        if png_card_paths:
            post_picks_to_instagram(png_card_paths, all_picks)
        elif png_card_path:
            post_picks_to_instagram([png_card_path], all_picks)
    except Exception as e:
        print(f"  Instagram: {e}")
