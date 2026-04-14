from src.common.config import load_config
from src.sessionization.build_sessions import build_sessionized_events
from tests.conftest import NORMALIZED_EVENT_TEST_SCHEMA


def test_sessionization_splits_on_timeout(spark, normalized_event_factory):
    config = load_config("local")
    session_timeout_seconds = int(config.sessionization["timeout_seconds"])

    rows = [
        normalized_event_factory(
            event_time="2026-01-01T10:00:00+00:00",
            path="/a",
            path_template="/a",
            line_hash="a",
        ),
        normalized_event_factory(
            event_time="2026-01-01T10:05:00+00:00",
            path="/b",
            path_template="/b",
            raw_path="/b",
            line_hash="b",
        ),
        normalized_event_factory(
            event_time="2026-01-01T10:40:01+00:00",
            path="/c",
            path_template="/c",
            raw_path="/c",
            line_hash="c",
        ),
    ]

    df = spark.createDataFrame(rows, schema=NORMALIZED_EVENT_TEST_SCHEMA)
    out = build_sessionized_events(
        normalized_df=df,
        session_timeout_seconds=session_timeout_seconds,
    )

    assert out.count() == 3
    assert out.select("session_id").distinct().count() == 2

    ordered = (
        out.orderBy("event_time")
        .select("event_order_in_session", "session_event_count", "is_new_session")
        .collect()
    )

    assert ordered[0]["event_order_in_session"] == 1
    assert ordered[1]["event_order_in_session"] == 2
    assert ordered[2]["event_order_in_session"] == 1
    assert ordered[0]["is_new_session"] == 1
    assert ordered[1]["is_new_session"] == 0
    assert ordered[2]["is_new_session"] == 1