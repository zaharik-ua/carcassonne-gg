import test from "node:test";
import assert from "node:assert/strict";

import {
  calculateAdjustedTpr,
  calculateBounty,
  calculateBountyPoints,
  calculateSmoothedWinRate,
  calculateTpr,
  calculateTprConfidence,
  percentileInclusive,
} from "./bounty-tpr.js";

test("smoothed win rate follows the configured +0.5 formula", () => {
  assert.equal(calculateSmoothedWinRate(10, 10), 10.5 / 11);
  assert.equal(calculateSmoothedWinRate(9, 10), 9.5 / 11);
  assert.equal(calculateSmoothedWinRate(5, 10), 0.5);
});

test("TPR matches the target expected score against played opponents", () => {
  const tpr = calculateTpr([1500, 1600, 1700], 0.5);
  assert.ok(Math.abs(tpr - 1600) < 0.000001);
});

test("confidence reaches 100 percent at the target match count", () => {
  assert.equal(calculateTprConfidence(1, 10), 0.1);
  assert.equal(calculateTprConfidence(5, 10), 0.5);
  assert.equal(calculateTprConfidence(10, 10), 1);
  assert.equal(calculateTprConfidence(12, 10), 1);
});

test("adjusted TPR blends current Elo and raw TPR", () => {
  assert.equal(calculateAdjustedTpr(1500, 1700, 0.2), 1540);
  assert.equal(calculateAdjustedTpr(1500, 1700, 1), 1700);
});

test("inclusive percentile and bounty use the Elo curve", () => {
  assert.equal(percentileInclusive([1000, 1200, 1400, 1600], 0.75), 1450);
  assert.equal(calculateBounty(1450, 1450), 0.5);
  assert.ok(calculateBounty(1650, 1450) > 0.5);
});

test("points include current defeated-opponent bounties and the player's own bounty", () => {
  assert.deepEqual(calculateBountyPoints(0.6, [0.45, 0.62, 0.78]), {
    opponentsPoints: 1.85,
    points: 2.45,
  });
});
