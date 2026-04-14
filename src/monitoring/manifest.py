from __future__ import annotations

from datetime import datetime, timezone
from pyspark.sql import SparkSession, DataFrame



def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def build_run_id(process_date: str, started_at: str) -> str:
    """
    Example:
      2019-01-22__2026-04-13T04:14:02.731447+00:00
    """
    return f"{process_date}__{started_at}"


def duration_seconds(started_at: str, completed_at: str) -> int:
    started_dt = datetime.fromisoformat(started_at)
    completed_dt = datetime.fromisoformat(completed_at)
    return int((completed_dt - started_dt).total_seconds())


def build_run_manifest(
    process_date: str,
    status: str,
    started_at: str,
    completed_at: str,
    metrics: dict,
    dq_passed: bool,
    validation_errors: list[str],
    validation_warnings: list[str],
    error_message: str | None = None,
) -> dict:
    manifest = {
        "run_id": build_run_id(process_date, started_at),
        "process_date": process_date,
        "status": status,
        "started_at": started_at,
        "completed_at": completed_at,
        "duration_sec": duration_seconds(started_at, completed_at),
        "dq_passed": dq_passed,
        "validation_errors": ",".join(validation_errors) if validation_errors else "",
        "validation_warnings": ",".join(validation_warnings) if validation_warnings else "",
        "error_message": error_message if error_message else "",
        **metrics,
    }

    return manifest


def build_manifest_df(spark: SparkSession, manifest: dict) -> DataFrame:
    # for k, v in manifest.items():
    #     try:
    #         spark.createDataFrame([{k: v}]).show()
    #         print(f"OK: {k} -> {type(v)}")
    #     except Exception as e:
    #         print(f"ERROR at field: {k}")
    #         print(f"value: {v}")
    #         print(f"type: {type(v)}")
    #         print(e)
    #         break
    return spark.createDataFrame([manifest])


def write_manifest(
    manifest_df: DataFrame,
    manifest_output_path: str,
    process_date: str,
) -> None:
    (
        manifest_df.coalesce(1)
        .write
        .mode("overwrite")
        .json(f"{manifest_output_path}/process_date={process_date}")
    )