from __future__ import annotations

import argparse
import json

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover
    def load_dotenv() -> None:
        return None

from update_matches.carcassonne_lab import generate_carcassonne_lab_url


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate a CarcassonneLab URL from a completed BGA table.")
    parser.add_argument("--table-id", required=True, help="Completed Board Game Arena table number")
    return parser.parse_args()


def main() -> int:
    load_dotenv()
    args = parse_args()
    try:
        url = generate_carcassonne_lab_url(args.table_id)
        payload = {"ok": True, "table_id": str(args.table_id), "url": url}
    except Exception as exc:
        payload = {"ok": False, "table_id": str(args.table_id), "message": str(exc)}
    print(json.dumps(payload, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
