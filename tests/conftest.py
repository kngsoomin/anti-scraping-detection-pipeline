import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("anti-scraping-tests")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    yield spark
    spark.stop()


@pytest.fixture
def normalized_event_factory():
    def _make(**overrides):
        row = {
            "event_time": "2026-01-01T10:00:00+00:00",
            "src_ip": "1.1.1.1",
            "method": "GET",
            "raw_path": "/a",
            "path": "/a",
            "path_template": "/a",
            "query_string": None,
            "protocol": "HTTP/1.1",
            "status_code": 200,
            "bytes_sent": 100,
            "referer": None,
            "user_agent": "UA1",
            "is_asset": False,
            "asset_type": "html",
            "is_known_bot_ua": False,
            "is_robots_request": False,
            "ingested_at": "2026-01-01T10:00:00+00:00",
            "source_file": "test.log",
            "line_hash": "hash-a",
        }
        row.update(overrides)
        return row
    return _make


@pytest.fixture
def sessionized_event_factory():
    def _make(**overrides):
        row = {
            "session_id": "s1",
            "src_ip": "1.1.1.1",
            "user_agent": "UA1",
            "event_time": "2026-01-01T10:00:00+00:00",
            "prev_event_time": None,
            "gap_from_prev_request_sec": None,
            "is_new_session": 1,
            "session_sequence": 1,
            "event_order_in_session": 1,
            "session_start_ts": "2026-01-01T10:00:00+00:00",
            "session_end_ts": "2026-01-01T10:10:00+00:00",
            "session_duration_sec": 600,
            "session_event_count": 3,
            "method": "GET",
            "raw_path": "/a",
            "path": "/a",
            "path_template": "/a",
            "query_string": None,
            "protocol": "HTTP/1.1",
            "status_code": 200,
            "bytes_sent": 100,
            "referer": None,
            "is_asset": False,
            "asset_type": "html",
            "is_known_bot_ua": False,
            "is_robots_request": False,
            "ingested_at": "2026-01-01T10:00:00+00:00",
            "source_file": "test.log",
            "line_hash": "hash-a",
        }
        row.update(overrides)
        return row
    return _make


@pytest.fixture
def session_feature_factory():
    def _make(**overrides):
        row = {
            "session_id": "s1",
            "src_ip": "1.1.1.1",
            "user_agent": "UA1",
            "request_count": 10,
            "requests_per_minute": 5.0,
            "session_duration_sec": 120,
            "unique_paths": 8,
            "unique_path_templates": 6,
            "missing_referer_ratio": 0.5,
            "html_to_asset_ratio": 1.5,
            "status_4xx_ratio": 0.0,
            "mean_inter_request_gap": 5.0,
        }
        row.update(overrides)
        return row
    return _make