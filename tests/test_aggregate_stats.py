import datetime
from unittest.mock import Mock

import pytest

import aggregate_stats

STATEMENTS = (
    aggregate_stats.AGGREGATE_SQL,
    aggregate_stats.AGGREGATE_OVERALL_ENDPOINT_SQL,
    aggregate_stats.AGGREGATE_APP_TOTAL_SQL,
    aggregate_stats.AGGREGATE_TOTAL_SQL,
)
TODAY = datetime.date(2026, 9, 27)


@pytest.mark.parametrize("statement", STATEMENTS)
def test_every_statement_writes_the_server_error_and_degraded_counters(statement):
    insert, update = statement.split("ON DUPLICATE KEY UPDATE")
    assert "error_count, server_error_count, degraded_count)" in insert
    assert "SUM(status_code >= 500)" in insert
    assert "SUM(degraded_reason IS NOT NULL)" in insert
    assert "VALUES(server_error_count)" in update
    assert "VALUES(degraded_count)" in update


def _database(monkeypatch, earliest_raw, days=()):
    connection = Mock()
    cursor = connection.cursor.return_value
    cursor.fetchone.return_value = (TODAY, earliest_raw)
    cursor.fetchall.return_value = [(day,) for day in days]
    monkeypatch.setattr(aggregate_stats, "create_connection", lambda: connection)
    return connection, cursor


def test_recompute_reaggregates_every_raw_day_and_never_purges(monkeypatch, capsys):
    days = (datetime.date(2026, 9, 25), datetime.date(2026, 9, 26))
    connection, cursor = _database(
        monkeypatch, datetime.datetime(2026, 9, 13, 2, 0), days
    )

    aggregate_stats.main(["--recompute-since", "2026-09-25"])

    queries = [call.args[0] for call in cursor.execute.call_args_list]
    assert queries[2:] == list(STATEMENTS) * 2
    assert [call.args[1] for call in cursor.execute.call_args_list[2:]] == [
        (day,) for day in days for _ in STATEMENTS
    ]
    assert cursor.execute.call_args_list[1].args[1] == (datetime.date(2026, 9, 25),)
    assert not any("DELETE" in query for query in queries)
    connection.commit.assert_called_once_with()
    assert "Recomputed 2 day(s) since 2026-09-25" in capsys.readouterr().out


@pytest.mark.parametrize(
    ("since", "earliest_raw", "message"),
    [
        ("2026-09-27", datetime.datetime(2026, 9, 13), "not a complete past day"),
        # The age-based purge cut this day in the middle: its earlier rows
        # are gone, so the oldest raw day itself must be refused.
        ("2026-09-13", datetime.datetime(2026, 9, 13, 2, 0), "may be incomplete"),
        ("2026-09-13", datetime.datetime(2026, 9, 13, 0, 0), "may be incomplete"),
        ("2026-09-20", None, "may be incomplete"),
    ],
)
def test_recompute_refuses_a_day_raw_rows_cannot_prove_complete(
    monkeypatch, capsys, since, earliest_raw, message
):
    connection, cursor = _database(monkeypatch, earliest_raw)

    with pytest.raises(SystemExit) as exit_info:
        aggregate_stats.main(["--recompute-since", since])

    assert message in str(exit_info.value.code)
    assert cursor.execute.call_count == 1
    connection.commit.assert_not_called()
    connection.rollback.assert_called_once_with()


def test_recompute_rejects_a_malformed_date():
    with pytest.raises(SystemExit) as exit_info:
        aggregate_stats.main(["--recompute-since", "27.09.2026"])
    assert exit_info.value.code == 2
