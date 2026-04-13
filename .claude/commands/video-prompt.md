Generate the daily Sora video prompt based on today's graded results.

Steps:
1. Query graded_bets for the most recently graded day's results (usually yesterday)
2. Calculate per-sport W-L records and the day's total W-L
3. Get the PUBLIC season record — this uses the post-rebuild filter (units >= 3.5) NOT raw DB totals. Query: `SELECT ... FROM graded_bets WHERE units >= 3.5`. Confirm with the user if the number looks off.
4. Generate the prompt in this EXACT scene-by-scene format — do NOT deviate:

```
Cinematic vertical video (9:16), 12 seconds. Dark sports broadcast studio with green neon lighting.

Scene 1 (0-3s): Camera enters a dark premium sports studio. A large neon sign on the wall reads "SCOTTY'S EDGE" with EDGE glowing bright green neon and SCOTTY'S in white neon tubes. The sign flickers on dramatically. Green neon light reflects off glossy black floors. Slow cinematic camera push toward the sign. A LED scoreboard below the sign shows "[DAY RECORD]" in large white numbers.

Scene 2-4 (3-9s): One scene per sport that had picks. Each scene has an athlete from that sport, well-lit with a sport-specific colored spotlight. LED scoreboard behind each shows that sport's W-L record. Green numbers if winning record, red if losing, white if .500.

Scene 5 (final 3s): Camera slowly pulls back to reveal the full studio. The neon "SCOTTY'S EDGE" sign glows on the wall. Scoreboard shows large green glowing numbers "[SEASON W-L]" with "[WIN%]%" below it pulsing green. All green neon lighting pulses slowly. Premium broadcast sign-off. Cinematic fade.

Style: Dark ESPN SportsCenter studio. Athletes are fully visible and well-lit with sport-specific colored lighting, NOT silhouettes. Green neon is the signature accent color. Numbers appear on LED scoreboards within the studio. Smooth slow camera movements. Premium sports broadcast quality.
```

5. Save the prompt to data/cards/kling_prompt.txt (legacy filename, still used)
6. Display the full prompt to the user so they can copy it into Sora

Key rules:
- Duration is 12 seconds (NOT 15)
- Tool is Sora (NOT Kling — filename is legacy)
- Format is 9:16 vertical
- All text is ON-SCREEN (never voiceover)
- Sport-specific spotlights: baseball=red, hockey=blue, soccer=golden, basketball=orange
- Adjust scene timing to fit the number of sports (more sports = shorter per scene)
- Result values in DB are WIN/LOSS/PUSH (not W/L/P)