import argparse
from datetime import datetime, timedelta


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run anti-scraping pipeline for a single date or date range."
    )
    parser.add_argument(
        "env_name",
        help="Environment name, e.g. local or ec2",
    )
    parser.add_argument(
        "--process-date",
        dest="process_date",
        help="Single processing date in YYYY-MM-DD format",
    )
    parser.add_argument(
        "--start-date",
        dest="start_date",
        help="Start date in YYYY-MM-DD format",
    )
    parser.add_argument(
        "--end-date",
        dest="end_date",
        help="End date in YYYY-MM-DD format",
    )
    return parser.parse_args()


def validate_cli_args(args: argparse.Namespace) -> None:
    has_process_date = args.process_date is not None
    has_range = args.start_date is not None or args.end_date is not None

    if has_process_date and has_range:
        raise ValueError("Use either --process-date or --start-date/--end-date, not both.")

    if not has_process_date and not has_range:
        raise ValueError("You must provide either --process-date or --start-date/--end-date.")

    if has_range and not (args.start_date and args.end_date):
        raise ValueError("Both --start-date and --end-date are required for range execution.")

    if args.process_date:
        _parse_date(args.process_date)

    if args.start_date:
        start_dt = _parse_date(args.start_date)
        end_dt = _parse_date(args.end_date)
        if start_dt > end_dt:
            raise ValueError("--start-date must be earlier than or equal to --end-date.")


def _parse_date(date_str: str) -> datetime.date:
    return datetime.strptime(date_str, "%Y-%m-%d").date()


def build_process_dates(
    process_date: str | None,
    start_date: str | None,
    end_date: str | None,
) -> list[str]:
    if process_date:
        return [process_date]

    start_dt = _parse_date(start_date)
    end_dt = _parse_date(end_date)

    dates: list[str] = []
    current = start_dt
    while current <= end_dt:
        dates.append(current.isoformat())
        current += timedelta(days=1)

    return dates
