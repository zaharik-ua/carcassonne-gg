"""JSON-only CLI used by the duel result editor; does not write to SQLite."""
import argparse
import contextlib
import json
import sys

from dotenv import load_dotenv
from update_matches.game_result import GameResultNotFound, fetch_game_result


def main():
    load_dotenv()
    parser = argparse.ArgumentParser()
    parser.add_argument("--table-id", required=True)
    parser.add_argument("--player-1-id", required=True)
    parser.add_argument("--player-2-id", required=True)
    args = parser.parse_args()
    try:
        with contextlib.redirect_stdout(sys.stderr):
            game = fetch_game_result(args.table_id, args.player_1_id, args.player_2_id)
        result = {"ok": True, "game": game}
    except GameResultNotFound as error:
        result = {"ok": False, "not_found": True, "message": str(error)}
    except Exception:
        result = {"ok": False, "message": "Could not load the BGA result. Please try again."}
    print(json.dumps(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
