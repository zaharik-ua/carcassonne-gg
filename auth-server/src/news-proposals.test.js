import assert from "node:assert/strict";
import test from "node:test";
import {
  isNewsProposalSubmitter,
  isOwnEditableNewsProposal,
} from "./news-proposals.js";

const proposalAccess = {
  isAdmin: false,
  isNewsEditor: false,
  profileId: "player-1",
};

test("only linked non-editor users use the news proposal workflow", () => {
  assert.equal(isNewsProposalSubmitter(proposalAccess), true);
  assert.equal(isNewsProposalSubmitter({ ...proposalAccess, profileId: "" }), false);
  assert.equal(isNewsProposalSubmitter({ ...proposalAccess, isNewsEditor: true }), false);
  assert.equal(isNewsProposalSubmitter({ ...proposalAccess, isAdmin: true }), false);
});

test("proposal users can edit only their own active draft proposal", () => {
  const ownDraft = {
    is_proposal: 1,
    created_by: "player-1",
    status: "Draft",
  };

  assert.equal(isOwnEditableNewsProposal(proposalAccess, ownDraft), true);
  assert.equal(isOwnEditableNewsProposal(proposalAccess, { ...ownDraft, created_by: "player-2" }), false);
  assert.equal(isOwnEditableNewsProposal(proposalAccess, { ...ownDraft, status: "Published" }), false);
  assert.equal(isOwnEditableNewsProposal(proposalAccess, { ...ownDraft, is_proposal: 0 }), false);
  assert.equal(isOwnEditableNewsProposal({ ...proposalAccess, isNewsEditor: true }, ownDraft), false);
});
