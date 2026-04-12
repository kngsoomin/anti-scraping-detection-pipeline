from __future__ import annotations

from datetime import datetime, timedelta


def get_prev_date(process_date: str) -> str:
    dt = datetime.strptime(process_date, "%Y-%m-%d").date()
    return (dt - timedelta(days=1)).isoformat()

def get_lookback_start_ts(process_date: str, session_timeout_seconds: int) -> str:
    process_dt = datetime.strptime(process_date, "%Y-%m-%d")
    lookback_start = process_dt - timedelta(seconds=session_timeout_seconds)
    return lookback_start.strftime("%Y-%m-%d %H:%M:%S")