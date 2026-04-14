from src.features.build_session_features import build_session_features
from tests.conftest import SESSIONIZED_EVENT_TEST_SCHEMA


def test_build_session_features_basic(spark, sessionized_event_factory):
    rows = [
        sessionized_event_factory(
            event_time="2026-01-01T10:00:00+00:00",
            prev_event_time=None,
            gap_from_prev_request_sec=None,
            event_order_in_session=1,
            path="/a",
            path_template="/a",
            referer=None,
            is_asset=False,
            asset_type="html",
            status_code=200,
            line_hash="h1",
        ),
        sessionized_event_factory(
            event_time="2026-01-01T10:05:00+00:00",
            prev_event_time="2026-01-01T10:00:00+00:00",
            gap_from_prev_request_sec=300,
            is_new_session=0,
            event_order_in_session=2,
            path="/img1",
            raw_path="/img1",
            path_template="/img/{id}",
            referer="https://example.com",
            is_asset=True,
            asset_type="image",
            status_code=200,
            line_hash="h2",
        ),
        sessionized_event_factory(
            event_time="2026-01-01T10:10:00+00:00",
            prev_event_time="2026-01-01T10:05:00+00:00",
            gap_from_prev_request_sec=300,
            is_new_session=0,
            event_order_in_session=3,
            path="/missing",
            raw_path="/missing",
            path_template="/missing",
            referer=None,
            is_asset=False,
            asset_type="html",
            status_code=404,
            line_hash="h3",
        ),
    ]

    df = spark.createDataFrame(rows, schema=SESSIONIZED_EVENT_TEST_SCHEMA)
    out = build_session_features(df)

    row = out.collect()[0]

    assert row["session_id"] == "s1"
    assert row["request_count"] == 3
    assert row["session_duration_sec"] == 600
    assert row["requests_per_minute"] == 0.3
    assert row["unique_paths"] == 3
    assert row["unique_path_templates"] == 3
    assert row["missing_referer_ratio"] == 2 / 3
    assert row["status_4xx_ratio"] == 1 / 3
    assert row["mean_inter_request_gap"] == 300.0
    assert row["html_to_asset_ratio"] == 2.0