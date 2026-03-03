"""
Aggregate yesterday's raw API request logs into daily stats and purge old rows.

Run daily via cron:
  0 2 * * * docker exec public-api python app/aggregate_stats.py
"""

import sys
import os

# Ensure app directory is on the path so config/database imports work
sys.path.insert(0, os.path.dirname(__file__))

from database import create_connection


def aggregate_and_purge():
    connection = create_connection()
    if connection is None:
        print("ERROR: could not connect to database")
        sys.exit(1)

    cursor = connection.cursor()
    try:
        # Aggregate yesterday's data
        cursor.execute("""
            INSERT INTO api_request_daily_stats
                (date, endpoint, request_count, unique_ips, avg_response_time_ms, error_count)
            SELECT
                DATE(created_at)         AS date,
                endpoint,
                COUNT(*)                 AS request_count,
                COUNT(DISTINCT client_ip) AS unique_ips,
                ROUND(AVG(response_time_ms)) AS avg_response_time_ms,
                SUM(status_code >= 400)  AS error_count
            FROM api_requests
            WHERE DATE(created_at) = CURDATE() - INTERVAL 1 DAY
            GROUP BY DATE(created_at), endpoint
            ON DUPLICATE KEY UPDATE
                request_count       = VALUES(request_count),
                unique_ips          = VALUES(unique_ips),
                avg_response_time_ms = VALUES(avg_response_time_ms),
                error_count         = VALUES(error_count)
        """)
        aggregated = cursor.rowcount
        print(f"Aggregated {aggregated} endpoint rows for yesterday")

        # Purge raw rows older than 14 days
        cursor.execute("""
            DELETE FROM api_requests
            WHERE created_at < NOW() - INTERVAL 14 DAY
        """)
        purged = cursor.rowcount
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
