"""One-off conversion of raw addresses in api_requests to client pseudonyms.

Run with ``PYTHONPATH=app python -m pseudonymize_request_log --dry-run`` first.
The cursor stops at the maximum ID present at startup, so concurrent new
requests cannot keep the migration running indefinitely.
"""

import argparse
from ipaddress import ip_address

from client_ip import pseudonymize_client_ip
from database import create_connection


def _is_ip_address(value: str) -> bool:
    try:
        ip_address(value)
    except ValueError:
        return False
    return True


def convert_request_log(
    *, dry_run: bool, batch_size: int = 500
) -> tuple[int, int, int]:
    """Return scanned, converted (or convertible), and skipped row counts."""
    if batch_size < 1:
        raise ValueError("batch_size must be positive")

    connection = create_connection()
    if connection is None:
        raise RuntimeError("Cannot connect to the request log database")

    cursor = connection.cursor()
    scanned = matched = skipped = 0
    last_id = 0
    try:
        cursor.execute("SELECT COALESCE(MAX(id), 0) FROM api_requests")
        max_id = cursor.fetchone()[0]
        while last_id < max_id:
            cursor.execute(
                "SELECT id, client_ip FROM api_requests "
                "WHERE id > %s AND id <= %s ORDER BY id LIMIT %s",
                (last_id, max_id, batch_size),
            )
            rows = cursor.fetchall()
            if not rows:
                break
            for row_id, value in rows:
                scanned += 1
                if not _is_ip_address(value):
                    skipped += 1
                    continue
                if not dry_run:
                    pseudonym = pseudonymize_client_ip(value)[:40]
                    cursor.execute(
                        "UPDATE api_requests SET client_ip = %s "
                        "WHERE id = %s AND client_ip = %s",
                        (pseudonym, row_id, value),
                    )
                    if cursor.rowcount != 1:
                        raise RuntimeError(
                            f"Request log row {row_id} changed during conversion"
                        )
                matched += 1
            if not dry_run:
                connection.commit()
            last_id = rows[-1][0]
        return scanned, matched, skipped
    except Exception:
        connection.rollback()
        raise
    finally:
        cursor.close()
        connection.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--batch-size", type=int, default=500)
    args = parser.parse_args()
    scanned, matched, skipped = convert_request_log(
        dry_run=args.dry_run, batch_size=args.batch_size
    )
    action = "convertible" if args.dry_run else "converted"
    print(f"scanned={scanned} {action}={matched} skipped_non_ip={skipped}")


if __name__ == "__main__":
    main()
