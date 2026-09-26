"""
Aggregate raw API request logs into daily stats and purge old rows.

Every run aggregates ALL past days missing from daily_stats (auto-backfill),
so if cron was down for a few days, next run catches up automatically.

Run daily via cron:
  0 2 * * * docker exec <container> python app/aggregate_stats.py
"""

import sys
import os

# Ensure app directory is on the path so config/database imports work
sys.path.insert(0, os.path.dirname(__file__))

from database import create_connection


AGGREGATE_SQL = """
    INSERT INTO api_request_daily_stats
        (date, endpoint, application, request_count, unique_ips, avg_response_time_ms, error_count)
    SELECT
        DATE(created_at)         AS date,
        endpoint,
        application,
        COUNT(*)                 AS request_count,
        COUNT(DISTINCT client_ip) AS unique_ips,
        ROUND(AVG(response_time_ms)) AS avg_response_time_ms,
        SUM(status_code >= 400)  AS error_count
    FROM api_requests
    WHERE DATE(created_at) = %s
    GROUP BY DATE(created_at), endpoint, application
    ON DUPLICATE KEY UPDATE
        request_count       = VALUES(request_count),
        unique_ips          = VALUES(unique_ips),
        avg_response_time_ms = VALUES(avg_response_time_ms),
        error_count         = VALUES(error_count)
"""

AGGREGATE_OVERALL_ENDPOINT_SQL = """
    INSERT INTO api_request_daily_stats
        (date, endpoint, application, request_count, unique_ips, avg_response_time_ms, error_count)
    SELECT DATE(created_at), endpoint, 'all', COUNT(*),
           COUNT(DISTINCT client_ip), ROUND(AVG(response_time_ms)),
           SUM(status_code >= 400)
    FROM api_requests
    WHERE DATE(created_at) = %s
    GROUP BY DATE(created_at), endpoint
    ON DUPLICATE KEY UPDATE
        request_count = VALUES(request_count),
        unique_ips = VALUES(unique_ips),
        avg_response_time_ms = VALUES(avg_response_time_ms),
        error_count = VALUES(error_count)
"""

# Per-application and overall totals keep unique clients correct across endpoints.
AGGREGATE_APP_TOTAL_SQL = """
    INSERT INTO api_request_daily_stats
        (date, endpoint, application, request_count, unique_ips, avg_response_time_ms, error_count)
    SELECT
        DATE(created_at)         AS date,
        '_total_'                AS endpoint,
        application,
        COUNT(*)                 AS request_count,
        COUNT(DISTINCT client_ip) AS unique_ips,
        ROUND(AVG(response_time_ms)) AS avg_response_time_ms,
        SUM(status_code >= 400)  AS error_count
    FROM api_requests
    WHERE DATE(created_at) = %s
    GROUP BY DATE(created_at), application
    ON DUPLICATE KEY UPDATE
        request_count       = VALUES(request_count),
        unique_ips          = VALUES(unique_ips),
        avg_response_time_ms = VALUES(avg_response_time_ms),
        error_count         = VALUES(error_count)
"""

AGGREGATE_TOTAL_SQL = """
    INSERT INTO api_request_daily_stats
        (date, endpoint, application, request_count, unique_ips, avg_response_time_ms, error_count)
    SELECT
        DATE(created_at), '_total_', 'all', COUNT(*),
        COUNT(DISTINCT client_ip), ROUND(AVG(response_time_ms)),
        SUM(status_code >= 400)
    FROM api_requests
    WHERE DATE(created_at) = %s
    GROUP BY DATE(created_at)
    ON DUPLICATE KEY UPDATE
        request_count = VALUES(request_count),
        unique_ips = VALUES(unique_ips),
        avg_response_time_ms = VALUES(avg_response_time_ms),
        error_count = VALUES(error_count)
"""


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
                cursor.execute(AGGREGATE_SQL, (d,))
                cursor.execute(AGGREGATE_OVERALL_ENDPOINT_SQL, (d,))
                cursor.execute(AGGREGATE_APP_TOTAL_SQL, (d,))
                cursor.execute(AGGREGATE_TOTAL_SQL, (d,))
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


if __name__ == "__main__":
    aggregate_and_purge()
