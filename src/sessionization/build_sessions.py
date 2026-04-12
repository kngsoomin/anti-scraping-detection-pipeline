from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window




def build_sessionized_events(normalized_df, session_timeout_seconds: int):
    """
    Input:
        normalized_df: spark dataframe

    Output:
        sessionized_df: spark dataframe
    """
    df = normalized_df.withColumn("event_time", F.to_timestamp("event_time"))

    # partition by session key (src_ip + user_agent)
    actor_window = Window.partitionBy("src_ip", "user_agent").orderBy("event_time", "line_hash")

    # previous event time
    df = df.withColumn("prev_event_time", F.lag("event_time").over(actor_window))

    # gap: current event time - previous event time
    df = df.withColumn(
        "gap_from_prev_request_sec",
        F.unix_timestamp("event_time") - F.unix_timestamp("prev_event_time")
    )

    # is this new session? (gap > session_timeout_threshold)
    df = df.withColumn(
        "is_new_session",
        F.when(F.col("prev_event_time").isNull(), F.lit(1))
         .when(F.col("gap_from_prev_request_sec") > session_timeout_seconds, F.lit(1))
         .otherwise(F.lit(0))
    )

    # generate session sequence within actor (src_ip + user_agent)
    df = df.withColumn(
        "session_sequence",
        F.sum("is_new_session").over(actor_window)
    )

    # replayable / deterministic session_id
    df = df.withColumn(
        "session_id",
        F.sha2(
            F.concat_ws(
                "||",
                F.col("src_ip"),
                F.col("user_agent"),
                F.col("session_sequence").cast("string"),
            ),
            256
        )
    )

    session_order_window = Window.partitionBy("session_id").orderBy("event_time", "line_hash")
    session_all_window = Window.partitionBy("session_id")

    df = df.withColumn(
        "event_order_in_session",
        F.row_number().over(session_order_window) # ~ current row
    )

    df = df.withColumn(
        "session_event_count",
        F.count("*").over(session_all_window)
    )

    df = df.withColumn(
        "session_start_ts",
        F.min("event_time").over(session_all_window)
    )

    df = df.withColumn(
        "session_end_ts",
        F.max("event_time").over(session_all_window)
    )

    df = df.withColumn(
        "session_date",
        F.to_date("session_start_ts")
    )

    df = df.withColumn(
        "session_duration_sec",
        F.unix_timestamp("session_end_ts") - F.unix_timestamp("session_start_ts")
    )

    return df.select(
        "session_id",
        "src_ip",
        "user_agent",
        "event_time",
        "session_date",
        "prev_event_time",
        "gap_from_prev_request_sec",
        "is_new_session",
        "session_sequence",
        "event_order_in_session",
        "session_event_count",
        "session_start_ts",
        "session_end_ts",
        "session_duration_sec",

        # keep normalized layer columns
        "method",
        "raw_path",
        "path",
        "path_template",
        "query_string",
        "protocol",
        "status_code",
        "bytes_sent",
        "referer",
        "is_asset",
        "asset_type",
        "is_known_bot_ua",
        "is_robots_request",
        "ingested_at",
        "source_file",
        "line_hash",
    )