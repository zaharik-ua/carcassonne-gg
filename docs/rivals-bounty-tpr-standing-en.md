# How the Rivals standings are calculated: Bounty TPR

## About — Standings

You earn standings points for every victory.
Each defeated opponent gives you their **Bounty** — a point value based on their GG Elo and tournament results, with stronger opponents worth more points.
Your total score is the sum of the Bounties of all opponents you defeated plus your own Bounty. 
See **Rules** for full calculation details.

## 1. Short explanation

**1. Every victory adds points to your standings score.** For each opponent you defeat, you receive points equal to their current **Bounty**. You do not receive an opponent's Bounty when you lose.

**2. Bounty is the current estimate of a player's strength, expressed as a point value.** It determines how many points a victory over that player is worth. The system calculates Bounty using their **GG Elo** and results in the current tournament. At the beginning, GG Elo carries more weight, but tournament results become increasingly important with every match played. After 10 matches, the evaluation is based entirely on tournament performance. The stronger the system considers an opponent, the higher their Bounty and the more points you earn for defeating them.

**3. How standings points are calculated.** Your total standings score is the sum of the current Bounties of all opponents you defeated plus your own current Bounty. Your own Bounty is added once, regardless of how many matches you played or won:

`Points = Σ Bounties of defeated opponents + your own Bounty`

Bounty is not locked in at the time of a victory; it is recalculated throughout the tournament. If an opponent you defeated performs better later, their Bounty increases and your victory becomes worth more Points. If their evaluation decreases, the value of your victory decreases as well. Final Points are determined after all tournament matches have been completed, and the player with the highest total wins.
