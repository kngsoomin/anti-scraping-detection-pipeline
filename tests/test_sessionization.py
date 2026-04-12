from src.schemas.normalized_events import NORMALIZED_EVENTS_SCHEMA
from src.sessionization.build_sessions import build_sessionized_events



def test_sessionization_splits_on_timeout(spark, normalized_event_factory):
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

    df = spark.createDataFrame(rows, schema=NORMALIZED_EVENTS_SCHEMA)
    out = build_sessionized_events(df)

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