"""
Aggregate raw API request logs into daily stats and purge old rows.

Every run aggregates ALL past days missing from daily_stats (auto-backfill),
so if cron was down for a few days, next run catches up automatically.

Run daily via cron:
  0 2 * * * docker exec <container> python app/aggregate_stats.py

One-time recompute of already aggregated days from raw rows (e.g. after new
counter columns were added), from the given date through yesterday:
  docker exec <container> python app/aggregate_stats.py --recompute-since YYYY-MM-DD
It refuses a date whose raw rows may already be purged, and does not purge.
"""

import argparse
import datetime
import sys
import os

# Ensure app directory is on the path so config/database imports work
sys.path.insert(0, os.path.dirname(__file__))

from database import create_connection


AGGREGATE_SQL = """
    INSERT INTO api_request_daily_stats
        (date, endpoint, application, request_count, unique_ips, avg_response_time_ms,
         error_count, server_error_count, degraded_count)
    SELECT
        DATE(created_at)         AS date,
        endpoint,
        application,
        COUNT(*)                 AS request_count,
        COUNT(DISTINCT client_ip) AS unique_ips,
        ROUND(AVG(response_time_ms)) AS avg_response_time_ms,
        SUM(status_code >= 400)  AS error_count,
        SUM(status_code >= 500)  AS server_error_count,
        SUM(degraded_reason IS NOT NULL) AS degraded_count
    FROM api_requests
    WHERE DATE(created_at) = %s
    GROUP BY DATE(created_at), endpoint, application
    ON DUPLICATE KEY UPDATE
        request_count       = VALUES(request_count),
        unique_ips          = VALUES(unique_ips),
        avg_response_time_ms = VALUES(avg_response_time_ms),
        error_count         = VALUES(error_count),
        server_error_count  = VALUES(server_error_count),
        degraded_count      = VALUES(degraded_count)
"""

AGGREGATE_OVERALL_ENDPOINT_SQL = """
    INSERT INTO api_request_daily_stats
        (date, endpoint, application, request_count, unique_ips, avg_response_time_ms,
         error_count, server_error_count, degraded_count)
    SELECT DATE(created_at), endpoint, 'all', COUNT(*),
           COUNT(DISTINCT client_ip), ROUND(AVG(response_time_ms)),
           SUM(status_code >= 400), SUM(status_code >= 500),
           SUM(degraded_reason IS NOT NULL)
    FROM api_requests
    WHERE DATE(created_at) = %s
    GROUP BY DATE(created_at), endpoint
    ON DUPLICATE KEY UPDATE
        request_count = VALUES(request_count),
        unique_ips = VALUES(unique_ips),
        avg_response_time_ms = VALUES(avg_response_time_ms),
        error_count = VALUES(error_count),
        server_error_count = VALUES(server_error_count),
        degraded_count = VALUES(degraded_count)
"""

# Per-application and overall totals keep unique clients correct across endpoints.
AGGREGATE_APP_TOTAL_SQL = """
    INSERT INTO api_request_daily_stats
        (date, endpoint, application, request_count, unique_ips, avg_response_time_ms,
         error_count, server_error_count, degraded_count)
    SELECT
        DATE(created_at)         AS date,
        '_total_'                AS endpoint,
        application,
        COUNT(*)                 AS request_count,
        COUNT(DISTINCT client_ip) AS unique_ips,
        ROUND(AVG(response_time_ms)) AS avg_response_time_ms,
        SUM(status_code >= 400)  AS error_count,
        SUM(status_code >= 500)  AS server_error_count,
        SUM(degraded_reason IS NOT NULL) AS degraded_count
    FROM api_requests
    WHERE DATE(created_at) = %s
    GROUP BY DATE(created_at), application
    ON DUPLICATE KEY UPDATE
        request_count       = VALUES(request_count),
        unique_ips          = VALUES(unique_ips),
        avg_response_time_ms = VALUES(avg_response_time_ms),
        error_count         = VALUES(error_count),
        server_error_count  = VALUES(server_error_count),
        degraded_count      = VALUES(degraded_count)
"""

AGGREGATE_TOTAL_SQL = """
    INSERT INTO api_request_daily_stats
        (date, endpoint, application, request_count, unique_ips, avg_response_time_ms,
         error_count, server_error_count, degraded_count)
    SELECT
        DATE(created_at), '_total_', 'all', COUNT(*),
        COUNT(DISTINCT client_ip), ROUND(AVG(response_time_ms)),
        SUM(status_code >= 400), SUM(status_code >= 500),
        SUM(degraded_reason IS NOT NULL)
    FROM api_requests
    WHERE DATE(created_at) = %s
    GROUP BY DATE(created_at)
    ON DUPLICATE KEY UPDATE
        request_count = VALUES(request_count),
        unique_ips = VALUES(unique_ips),
        avg_response_time_ms = VALUES(avg_response_time_ms),
        error_count = VALUES(error_count),
        server_error_count = VALUES(server_error_count),
        degraded_count = VALUES(degraded_count)
"""


def _aggregate_day(cursor, day) -> None:
    cursor.execute(AGGREGATE_SQL, (day,))
    cursor.execute(AGGREGATE_OVERALL_ENDPOINT_SQL, (day,))
    cursor.execute(AGGREGATE_APP_TOTAL_SQL, (day,))
    cursor.execute(AGGREGATE_TOTAL_SQL, (day,))


def recompute_since(since: datetime.date) -> None:
    """Re-aggregate every day from `since` through yesterday from raw rows.

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
            _aggregate_day(cursor, d)
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
                _aggregate_day(cursor, d)
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
