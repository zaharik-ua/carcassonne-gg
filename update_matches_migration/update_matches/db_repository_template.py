from __future__ import annotations

from .models import MatchUpdateRequest, MatchUpdateResult
from .repository import MatchRepository


class DatabaseMatchRepository(MatchRepository):
    """Template adapter for the target project.

    Replace this class with the real DB implementation after moving the folder.
    The expected behavior mirrors the old Google Sheets flow:

    - `target="ongoing"`:
      select started matches that are still in progress and not fully scored
    - `target="empty_finished"`:
      select started matches that are already finished but still have empty score/result fields
    """

    def __init__(self, db_session) -> None:
        self.db_session = db_session

    def fetch_matches_to_update(self, *, target: str, limit: int) -> list[MatchUpdateRequest]:
        raise NotImplementedError("Implement DB query that returns MatchUpdateRequest objects")

    def save_match_result(self, match: MatchUpdateRequest, result: MatchUpdateResult) -> None:
        raise NotImplementedError("Implement DB write for successful result update")

    def save_match_error(self, match: MatchUpdateRequest, message: str) -> None:
        raise NotImplementedError("Implement DB write for failed result update")
