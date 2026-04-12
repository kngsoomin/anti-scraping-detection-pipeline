from __future__ import annotations

import sys

from pyspark.sql import functions as F

from src.common.config import load_config
from src.common.paths import resolve_output_path
from src.common.spark import get_spark
from src.detection.build_rule_based_detection import build_rule_based_detection


def main(env_name: str, process_date: str) -> None:
    config = load_config(env_name)
    spark = get_spark("detection", config)
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    input_path = resolve_output_path(config, "session_features")
    output_path = resolve_output_path(config, "suspicious_sessions")

    session_features_df = spark.read.parquet(input_path).filter(
            F.col("session_date") == process_date
        )

    suspicious_sessions_df = build_rule_based_detection(
        session_features_df=session_features_df,
        thresholds=config.detection["thresholds"],
        risk_bands=config.detection["risk_bands"],
    )

    suspicious_sessions_df.write \
        .mode("overwrite") \
        .partitionBy("session_date") \
        .parquet(output_path)

    print(f"Suspicious sessions for session_date={process_date} written to: {output_path}/session_date={process_date}")
    spark.stop()


if __name__ == "__main__":
    env_name = sys.argv[1] if len(sys.argv) > 1 else "local"
    process_date = sys.argv[2] if len(sys.argv) > 2 else None
    main(env_name, process_date)