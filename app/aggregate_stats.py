"""
Aggregate raw API request logs into daily stats and purge old rows.

Every run aggregates ALL past days missing from daily_stats (auto-backfill),
so if cron was down for a few days, next run catches up automatically.

Run daily via cron:
  0 2 * * * docker exec <container> python app/aggregate_stats.py

One-time recompute of already aggregated days from raw rows (e.g. after new
counter columns were added), from the given date through yesterday:
  docker exec <container> python app/aggregate_stats.py --recompute-since YYYY-MM-DD
It refuses a date whose raw rows may already be purged, does not purge, and
leaves degraded_count untouched (raw rows before its writer cannot tell).
"""

import argparse
import datetime
import sys
import os

# Ensure app directory is on the path so config/database imports work
sys.path.insert(0, os.path.dirname(__file__))

from database import create_connection


# (column, expression over the raw rows of one group)
DAILY_COUNTERS = (
    ("request_count", "COUNT(*)"),
    ("unique_ips", "COUNT(DISTINCT client_ip)"),
    ("avg_response_time_ms", "ROUND(AVG(response_time_ms))"),
    ("error_count", "SUM(status_code >= 400)"),
    ("server_error_count", "SUM(status_code >= 500)"),
    ("degraded_count", "SUM(degraded_reason IS NOT NULL)"),
)
# Raw rows written before the degraded-reason writer carry NULL for every
# request, so a recompute would turn "not measured" into a measured zero. It
# leaves `degraded_count` as it is (NULL on days aggregated before it existed).
RECOMPUTE_COUNTERS = tuple(
    counter for counter in DAILY_COUNTERS if counter[0] != "degraded_count"
)

# (endpoint, application, extra GROUP BY). Per-application and overall totals
# keep unique clients correct across endpoints.
GROUPINGS = (
    ("endpoint", "application", "endpoint, application"),
    ("endpoint", "'all'", "endpoint"),
    ("'_total_'", "application", "application"),
    ("'_total_'", "'all'", None),
)


def _aggregate_statement(endpoint, application, group_by, counters) -> str:
    columns = ", ".join(name for name, _ in counters)
    values = ", ".join(expression for _, expression in counters)
    updates = ",\n        ".join(f"{name} = VALUES({name})" for name, _ in counters)
    group = "DATE(created_at)" + (f", {group_by}" if group_by else "")
    return f"""
    INSERT INTO api_request_daily_stats
        (date, endpoint, application, {columns})
    SELECT DATE(created_at), {endpoint}, {application}, {values}
    FROM api_requests
    WHERE DATE(created_at) = %s
    GROUP BY {group}
    ON DUPLICATE KEY UPDATE
        {updates}
"""


AGGREGATE_STATEMENTS = tuple(
    _aggregate_statement(*grouping, DAILY_COUNTERS) for grouping in GROUPINGS
)
RECOMPUTE_STATEMENTS = tuple(
    _aggregate_statement(*grouping, RECOMPUTE_COUNTERS) for grouping in GROUPINGS
)


def _aggregate_day(cursor, day, statements) -> None:
    for statement in statements:
        cursor.execute(statement, (day,))


def recompute_since(since: datetime.date) -> None:
    """Re-aggregate every day from `since` through yesterday from raw rows.

    Every counter except `degraded_count` is recomputed (see
    `RECOMPUTE_COUNTERS`).

    Only days whose raw rows are provably complete are accepted: raw rows are
    purged by age, so a raw row older than `since` proves that nothing from
    `since` onwards has been purged yet. Anything else would overwrite a
    complete daily row with a partial count, so it is refused.
    """
    connection = create_connection()
    if connection is None:
        sys.exit("ERROR: could not connect to database")

    cursor = connection.cursor()
    try:
        cursor.execute("SELECT CURDATE(), MIN(created_at) FROM api_requests")
        today, earliest_raw = cursor.fetchone()
        if since >= today:
            raise ValueError(
                f"{since} is not a complete past day (today is {today})"
            )
        if earliest_raw is None or earliest_raw >= datetime.datetime.combine(
            since, datetime.time.min
        ):
            raise ValueError(
                f"raw rows for {since} may be incomplete: the earliest raw "
                f"row is {earliest_raw}; choose a later date"
            )
        cursor.execute(
            """
            SELECT DISTINCT DATE(created_at) AS d
            FROM api_requests
            WHERE created_at >= %s AND DATE(created_at) < CURDATE()
            ORDER BY d
            """,
            (since,),
        )
        dates = [row[0] for row in cursor.fetchall()]
        for d in dates:
            _aggregate_day(cursor, d, RECOMPUTE_STATEMENTS)
        connection.commit()
        print(f"Recomputed {len(dates)} day(s) since {since}")
    except Exception as e:
        connection.rollback()
        sys.exit(f"ERROR: {e}")
    finally:
        cursor.close()
        connection.close()


def aggregate_and_purge():
    connection = create_connection()
    if connection is None:
        print("ERROR: could not connect to database")
        sys.exit(1)

    cursor = connection.cursor()
    try:
        # Find all past dates needing aggregation or missing _total_ row
        cursor.execute("""
            SELECT DISTINCT DATE(created_at) AS d
            FROM api_requests
            WHERE DATE(created_at) < CURDATE()
              AND (
                  DATE(created_at) NOT IN (
                      SELECT DISTINCT date FROM api_request_daily_stats
                      WHERE endpoint = '_total_' AND application IN ('all', 'unknown')
                  )
              )
            ORDER BY d
        """)
        dates = [row[0] for row in cursor.fetchall()]

        if not dates:
            print("All past days already aggregated")
        else:
            for d in dates:
                _aggregate_day(cursor, d, AGGREGATE_STATEMENTS)
            print(f"Aggregated {len(dates)} day(s)")

        # Purge raw rows older than 14 days
        cursor.execute("""
            DELETE FROM api_requests
            WHERE created_at < NOW() - INTERVAL 14 DAY
        """)
        purged = cursor.rowcount
        if purged:
            print(f"Purged {purged} raw rows older than 14 days")

        connection.commit()
    except Exception as e:
        connection.rollback()
        print(f"ERROR: {e}")
        sys.exit(1)
    finally:
        cursor.close()
        connection.close()


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0].strip())
    parser.add_argument(
        "--recompute-since",
        type=datetime.date.fromisoformat,
        metavar="YYYY-MM-DD",
        help="re-aggregate already aggregated days from raw rows, then exit",
    )
    args = parser.parse_args(argv)
    if args.recompute_since is not None:
        recompute_since(args.recompute_since)
    else:
        aggregate_and_purge()


if __name__ == "__main__":
    main()
