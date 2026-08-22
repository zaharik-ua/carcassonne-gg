export function isNewsProposalSubmitter(access) {
  return !!String(access?.profileId || "").trim()
    && access?.isAdmin !== true
    && access?.isNewsEditor !== true;
}

export function isOwnEditableNewsProposal(access, newsItem) {
  if (!isNewsProposalSubmitter(access) || !newsItem) return false;
  return Number(newsItem.is_proposal) === 1
    && String(newsItem.created_by || "").trim() === String(access.profileId || "").trim()
    && String(newsItem.status || "").trim().toLowerCase() === "draft";
}
