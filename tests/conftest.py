import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    LongType,
    DoubleType,
    BooleanType,
)


NORMALIZED_EVENT_TEST_SCHEMA = StructType([
    StructField("event_time", StringType(), True),
    StructField("src_ip", StringType(), True),
    StructField("method", StringType(), True),
    StructField("raw_path", StringType(), True),
    StructField("path", StringType(), True),
    StructField("path_template", StringType(), True),
    StructField("query_string", StringType(), True),
    StructField("protocol", StringType(), True),
    StructField("status_code", IntegerType(), True),
    StructField("bytes_sent", LongType(), True),
    StructField("referer", StringType(), True),
    StructField("user_agent", StringType(), True),
    StructField("is_asset", BooleanType(), True),
    StructField("asset_type", StringType(), True),
    StructField("is_robots_request", BooleanType(), True),
    StructField("ingested_at", StringType(), True),
    StructField("source_file", StringType(), True),
    StructField("line_hash", StringType(), True),
])

SESSIONIZED_EVENT_TEST_SCHEMA = StructType([
    StructField("session_id", StringType(), True),
    StructField("src_ip", StringType(), True),
    StructField("user_agent", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("session_date", StringType(), True),
    StructField("prev_event_time", StringType(), True),
    StructField("gap_from_prev_request_sec", IntegerType(), True),
    StructField("is_new_session", IntegerType(), True),
    StructField("session_sequence", IntegerType(), True),
    StructField("event_order_in_session", IntegerType(), True),
    StructField("session_event_count", IntegerType(), True),
    StructField("session_start_ts", StringType(), True),
    StructField("session_end_ts", StringType(), True),
    StructField("session_duration_sec", LongType(), True),
    StructField("method", StringType(), True),
    StructField("raw_path", StringType(), True),
    StructField("path", StringType(), True),
    StructField("path_template", StringType(), True),
    StructField("query_string", StringType(), True),
    StructField("protocol", StringType(), True),
    StructField("status_code", IntegerType(), True),
    StructField("bytes_sent", LongType(), True),
    StructField("referer", StringType(), True),
    StructField("is_asset", BooleanType(), True),
    StructField("asset_type", StringType(), True),
    StructField("is_robots_request", BooleanType(), True),
    StructField("ingested_at", StringType(), True),
    StructField("source_file", StringType(), True),
    StructField("line_hash", StringType(), True),
])

SESSION_FEATURE_TEST_SCHEMA = StructType([
    StructField("session_id", StringType(), True),
    StructField("session_date", StringType(), True),
    StructField("src_ip", StringType(), True),
    StructField("user_agent", StringType(), True),
    StructField("session_start_ts", StringType(), True),
    StructField("request_count", LongType(), True),
    StructField("requests_per_minute", DoubleType(), True),
    StructField("session_duration_sec", LongType(), True),
    StructField("unique_paths", LongType(), True),
    StructField("unique_path_templates", LongType(), True),
    StructField("missing_referer_ratio", DoubleType(), True),
    StructField("html_to_asset_ratio", DoubleType(), True),
    StructField("status_4xx_ratio", DoubleType(), True),
    StructField("mean_inter_request_gap", DoubleType(), True),
    StructField("hostname", StringType(), True),
    StructField("has_hostname", BooleanType(), True),
    StructField("is_known_bot_domain", BooleanType(), True),
    StructField("is_known_bot_ua", BooleanType(), True),
])


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
            "session_date": "2026-01-01",
            "prev_event_time": None,
            "gap_from_prev_request_sec": None,
            "is_new_session": 1,
            "session_sequence": 1,
            "event_order_in_session": 1,
            "session_event_count": 3,
            "session_start_ts": "2026-01-01T10:00:00+00:00",
            "session_end_ts": "2026-01-01T10:10:00+00:00",
            "session_duration_sec": 600,
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
            "session_date": "2026-01-01",
            "src_ip": "1.1.1.1",
            "user_agent": "UA1",
            "session_start_ts": "2026-01-01T10:00:00+00:00",
            "request_count": 10,
            "requests_per_minute": 5.0,
            "session_duration_sec": 120,
            "unique_paths": 8,
            "unique_path_templates": 6,
            "missing_referer_ratio": 0.5,
            "html_to_asset_ratio": 1.5,
            "status_4xx_ratio": 0.0,
            "mean_inter_request_gap": 5.0,
            "hostname": None,
            "has_hostname": False,
            "is_known_bot_domain": False,
            "is_known_bot_ua": False,
        }
        row.update(overrides)
        return row
    return _make