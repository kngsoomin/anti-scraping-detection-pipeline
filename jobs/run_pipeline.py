from __future__ import annotations

import subprocess
import sys

from jobs.cli_utils import parse_args, validate_cli_args, build_process_dates

from src.common.config import load_config
from src.common.paths import resolve_output_path
from src.common.spark import get_spark

from src.monitoring.metrics import collect_run_metrics
from src.monitoring.validation import validate_run_metrics
from src.monitoring.manifest import (
    build_run_manifest,
    build_manifest_df,
    write_manifest,
    utc_now_iso,
)

JOB_MODULES = [
    "jobs.run_parse",
    "jobs.run_sessions",
    "jobs.run_features",
    "jobs.run_enrichment",
    "jobs.run_detection",
    "jobs.run_daily_summary",
    "jobs.run_parse_failure_summary",
]



def run_single_date(env_name: str, process_date: str) -> None:
    started_at = utc_now_iso()
    error_message = None
    status = "success"

    try:
        for module_name in JOB_MODULES:
            print(f"\n=== Running {module_name} ({env_name}, {process_date}) ===")
            cmd = [
                sys.executable,
                "-m",
                module_name,
                env_name,
                "--process-date",
                process_date]
            subprocess.run(cmd, check=True)

    except Exception as exc:
        status = "failed"
        error_message = str(exc)

    completed_at = utc_now_iso()

    config = load_config(env_name)
    manifest_output_path = resolve_output_path(config, "manifests")

    spark = get_spark("pipeline-manifest", config)

    metrics: dict = {}
    dq_passed = False
    validation_errors: list[str] = []
    validation_warnings: list[str] = []

    try:
        metrics = collect_run_metrics(spark, env_name, process_date)
        dq_passed, validation_errors, validation_warnings = validate_run_metrics(metrics)

        if status == "success" and not dq_passed:
            status = "failed"
            error_message = "run-level validation failed"

    except Exception as metrics_exc:
        if status == "success":
            status = "failed"
            error_message = f"metrics/validation failed: {metrics_exc}"

    manifest = build_run_manifest(
        process_date=process_date,
        status=status,
        started_at=started_at,
        completed_at=completed_at,
        metrics=metrics,
        dq_passed=dq_passed,
        validation_errors=validation_errors,
        validation_warnings=validation_warnings,
        error_message=error_message,
    )

    manifest_df = build_manifest_df(spark, manifest)
    write_manifest(manifest_df, manifest_output_path, process_date)

    spark.stop()

    if status == "failed":
        raise RuntimeError(error_message)


def main() -> None:
    args = parse_args()
    validate_cli_args(args)

    process_dates = build_process_dates(
        process_date=args.process_date,
        start_date=args.start_date,
        end_date=args.end_date,
    )

    print(f"Running pipeline for {len(process_dates)} date(s): {process_dates}")

    failed_dates: list[tuple[str, str]] = []

    for process_date in process_dates:
        print(f"\n{'=' * 80}")
        print(f"START PROCESS DATE: {process_date}")
        print(f"{'=' * 80}")

        try:
            run_single_date(args.env_name, process_date)
        except Exception as exc:
            failed_dates.append((process_date, str(exc)))
            print(f"FAILED: {process_date} -> {exc}")

    if failed_dates:
        print(f"\n{len(failed_dates)} date(s) failed:")
        for process_date, error in failed_dates:
            print(f"- {process_date}: {error}")
        raise RuntimeError("One or more process dates failed.")

    print("\nAll process dates completed successfully.")


if __name__ == "__main__":
    main()