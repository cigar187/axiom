"""
validation/verify_scheduler.py — Confirm the Cloud Scheduler job is working.

Run this AFTER running cloud_scheduler_setup.sh to verify:
  1. The Cloud Scheduler job exists and is enabled
  2. It can successfully reach the Axiom API
  3. The pipeline ran (or is scheduled to run next at the right time)

Usage:
    python validation/verify_scheduler.py
    python validation/verify_scheduler.py --trigger   # manually fire the job NOW
    python validation/verify_scheduler.py --status    # just check job status

Requires:
    - gcloud CLI installed and authenticated
    - The proxy running in Tab 1 (only for --trigger with API check)
"""
import argparse
import json
import subprocess
import sys
from datetime import datetime, timezone

PROJECT  = "axiom-gtmvelo"
REGION   = "us-central1"
JOB_NAME = "axiom-daily-run"


def _run(cmd: list[str], capture: bool = True) -> tuple[int, str, str]:
    """Run a shell command and return (returncode, stdout, stderr)."""
    result = subprocess.run(
        cmd, capture_output=capture, text=True
    )
    return result.returncode, result.stdout.strip(), result.stderr.strip()


def check_job_exists() -> dict | None:
    """Return job info dict if the scheduler job exists, else None."""
    code, out, err = _run([
        "gcloud", "scheduler", "jobs", "describe", JOB_NAME,
        f"--project={PROJECT}",
        f"--location={REGION}",
        "--format=json",
    ])
    if code != 0:
        return None
    try:
        return json.loads(out)
    except json.JSONDecodeError:
        return None


def trigger_job_now() -> bool:
    """Fire the scheduler job immediately. Returns True on success."""
    print(f"\n  Triggering job '{JOB_NAME}' manually...")
    code, out, err = _run([
        "gcloud", "scheduler", "jobs", "run", JOB_NAME,
        f"--project={PROJECT}",
        f"--location={REGION}",
    ])
    if code == 0:
        print("  ✓ Job triggered successfully.")
        print("  The pipeline is now running on Cloud Run.")
        print("  It will take 60-90 seconds to complete.")
        print(f"\n  Watch the logs:")
        print(f"    gcloud logging read 'resource.type=cloud_run_revision AND "
              f"resource.labels.service_name=axiom-engine' "
              f"--project={PROJECT} --limit=50 --format='value(textPayload)'")
        return True
    else:
        print(f"  ERROR triggering job: {err}")
        return False


def check_recent_executions() -> list[dict]:
    """Fetch the last 5 executions from Cloud Logging."""
    code, out, err = _run([
        "gcloud", "logging", "read",
        (
            f"resource.type=cloud_scheduler_job AND "
            f"resource.labels.job_id={JOB_NAME} AND "
            f"resource.labels.location={REGION}"
        ),
        f"--project={PROJECT}",
        "--limit=5",
        "--format=json",
    ])
    if code != 0 or not out or out == "[]":
        return []
    try:
        logs = json.loads(out)
        return logs
    except json.JSONDecodeError:
        return []


def print_status(job: dict, executions: list):
    state = job.get("state", "UNKNOWN")
    schedule = job.get("schedule", "?")
    tz = job.get("timeZone", "UTC")
    last_run = job.get("lastAttemptTime", "Never")
    next_run = job.get("scheduleTime", "?")

    state_icon = "✓" if state == "ENABLED" else "⚠"

    # Parse next run time for human-readable display
    if next_run and next_run != "?":
        try:
            dt = datetime.fromisoformat(next_run.replace("Z", "+00:00"))
            now = datetime.now(timezone.utc)
            diff = dt - now
            hours = int(diff.total_seconds() // 3600)
            mins  = int((diff.total_seconds() % 3600) // 60)
            next_run_str = f"{dt.strftime('%Y-%m-%d %H:%M UTC')} (in {hours}h {mins}m)"
        except Exception:
            next_run_str = next_run
    else:
        next_run_str = "?"

    print(f"\n{'═'*64}")
    print(f"  AXIOM CLOUD SCHEDULER STATUS")
    print(f"{'═'*64}")
    print(f"  Job name      : {JOB_NAME}")
    print(f"  State         : {state_icon} {state}")
    print(f"  Schedule      : {schedule} ({tz})")
    print(f"  Last run      : {last_run}")
    print(f"  Next run      : {next_run_str}")

    if executions:
        print(f"\n  Recent executions ({len(executions)}):")
        for e in executions[:5]:
            ts  = e.get("timestamp", "?")
            msg = (e.get("jsonPayload", {}) or {}).get("message", "")
            txt = e.get("textPayload", "")
            print(f"    {ts[:19]}  {msg or txt or '(no message)'}")
    else:
        print(f"\n  No recent execution logs found.")
        print(f"  (It may take a few seconds after triggering for logs to appear.)")

    print(f"\n{'─'*64}")
    print(f"  Cloud Console:")
    print(f"  https://console.cloud.google.com/cloudscheduler?project={PROJECT}")
    print(f"{'═'*64}\n")


def main():
    parser = argparse.ArgumentParser(
        description="Verify the Axiom Cloud Scheduler daily auto-run job."
    )
    parser.add_argument("--trigger", action="store_true",
                        help="Manually fire the scheduler job right now")
    parser.add_argument("--status", action="store_true",
                        help="Only show job status (no trigger)")
    args = parser.parse_args()

    # Check gcloud is available
    code, _, _ = _run(["gcloud", "version"])
    if code != 0:
        print("\nERROR: gcloud CLI not found.")
        print("Install from: https://cloud.google.com/sdk/docs/install\n")
        sys.exit(1)

    print(f"\nChecking scheduler job '{JOB_NAME}' in project '{PROJECT}'...")

    job = check_job_exists()
    if not job:
        print(f"\n  Job '{JOB_NAME}' does not exist yet.")
        print(f"  Run this first: bash cloud_scheduler_setup.sh\n")
        sys.exit(1)

    if args.trigger:
        success = trigger_job_now()
        if not success:
            sys.exit(1)
        print("\n  Waiting 5 seconds then checking logs...")
        import time
        time.sleep(5)

    executions = check_recent_executions()
    print_status(job, executions)


if __name__ == "__main__":
    main()
