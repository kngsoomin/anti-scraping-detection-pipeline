from __future__ import annotations

import subprocess
import sys

JOB_MODULES = [
    "jobs.run_parse",
    "jobs.run_sessions",
    "jobs.run_features",
    "jobs.run_detection",
    "jobs.run_daily_summary",
]


def main(env_name: str, process_date: str | None = None) -> None:
    for module_name in JOB_MODULES:
        print(f"\n=== Running {module_name} ({env_name}, {process_date}) ===")

        cmd = [sys.executable, "-m", module_name, env_name]

        if process_date:
            cmd.append(process_date)

        subprocess.run(cmd, check=True)


if __name__ == "__main__":
    env_name = sys.argv[1] if len(sys.argv) > 1 else "local"
    process_date = sys.argv[2] if len(sys.argv) > 2 else None

    main(env_name, process_date)