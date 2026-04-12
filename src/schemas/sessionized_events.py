from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    BooleanType,
)


SESSIONIZED_EVENTS_SCHEMA = StructType([
    StructField("session_id", StringType(), True),

    StructField("src_ip", StringType(), True),
    StructField("user_agent", StringType(), True),

    StructField("event_time", StringType(), True),
    StructField("prev_event_time", StringType(), True),
    StructField("gap_from_prev_request_sec", LongType(), True),

    StructField("is_new_session", LongType(), True),
    StructField("session_sequence", LongType(), True),
    StructField("event_order_in_session", LongType(), True),

    StructField("session_start_ts", StringType(), True),
    StructField("session_end_ts", StringType(), True),
    StructField("session_duration_sec", LongType(), True),
    StructField("session_event_count", LongType(), True),

    # request-level fields (carry-over from normalized)
    StructField("method", StringType(), True),
    StructField("raw_path", StringType(), True),
    StructField("path", StringType(), True),
    StructField("path_template", StringType(), True),
    StructField("query_string", StringType(), True),
    StructField("protocol", StringType(), True),

    StructField("status_code", LongType(), True),
    StructField("bytes_sent", LongType(), True),

    StructField("referer", StringType(), True),

    StructField("is_asset", BooleanType(), True),
    StructField("asset_type", StringType(), True),

    StructField("is_known_bot_ua", BooleanType(), True),
    StructField("is_robots_request", BooleanType(), True),

    StructField("ingested_at", StringType(), True),
    StructField("source_file", StringType(), True),

    StructField("line_hash", StringType(), True),
])