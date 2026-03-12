"""
main.py – Scripty Job Runner
Simulates job creation, validation, dispatch, and retry logic.
Reads configuration from .env via python-dotenv.
"""

import os
import sys
import time
import logging
from pathlib import Path

from dotenv import load_dotenv

# ── Load environment ───────────────────────────────────────────────────────────
load_dotenv()  # reads .env from the project root

# ── Logging setup ─────────────────────────────────────────────────────────────

LOG_LEVEL     = os.getenv("LOG_LEVEL", "INFO").upper()
LOG_TO_FILE   = os.getenv("LOG_TO_FILE", "false").lower() == "true"
LOG_FILE_PATH = os.getenv("LOG_FILE_PATH", "logs/scripty.log")

def configure_logging() -> logging.Logger:
    """Set up root logger with console (and optionally file) handlers."""
    fmt = logging.Formatter(
        fmt="%(asctime)s  [%(levelname)-8s]  %(name)s – %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    logger = logging.getLogger("scripty")
    logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))

    # Console handler
    console_h = logging.StreamHandler(sys.stdout)
    console_h.setFormatter(fmt)
    logger.addHandler(console_h)

    # Optional file handler
    if LOG_TO_FILE:
        log_path = Path(LOG_FILE_PATH)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        file_h = logging.FileHandler(log_path)
        file_h.setFormatter(fmt)
        logger.addHandler(file_h)
        logger.info("File logging enabled → %s", log_path)

    return logger


log = configure_logging()

# ── Imports from utils ─────────────────────────────────────────────────────────
from utils.helpers import (
    build_job_payload,
    validate_job_config,
    format_job_result,
    simulate_worker_call,
)

# ── Config from env ────────────────────────────────────────────────────────────
MAX_RETRIES  = int(os.getenv("JOB_MAX_RETRIES", 3))
RETRY_DELAY  = float(os.getenv("JOB_RETRY_DELAY_SECONDS", 5))
APP_ENV      = os.getenv("APP_ENV", "development")
APP_NAME     = os.getenv("APP_NAME", "scripty-job-runner")


# ── Core runner ────────────────────────────────────────────────────────────────

def dispatch_job(payload: dict) -> dict:
    """
    Attempt to dispatch a single job, retrying on transient failures.
    Returns the final formatted result dict.
    """
    job_id   = payload["job_id"]
    job_type = payload["job_type"]

    log.info("Dispatching job  job_id=%s  type=%s  priority=%s",
             job_id, job_type, payload.get("priority"))

    for attempt in range(1, MAX_RETRIES + 1):
        log.debug("  attempt %d/%d …", attempt, MAX_RETRIES)

        response = simulate_worker_call(payload)

        log.debug("  worker responded  status=%s  latency=%.3fs",
                  response["status_code"], response["latency_s"])

        if response["success"]:
            log.info("  ✓ accepted by worker  queue_pos=%s",
                     response["body"].get("queue_position"))
            return format_job_result(
                job_id=job_id,
                status="accepted",
                output=response["body"],
            )

        log.warning("  ✗ worker error  status=%s  reason=%s",
                    response["status_code"],
                    response["body"].get("error", "unknown"))

        if attempt < MAX_RETRIES:
            log.info("  retrying in %.1fs …", RETRY_DELAY)
            time.sleep(RETRY_DELAY)

    log.error("Job failed after %d attempts  job_id=%s", MAX_RETRIES, job_id)
    return format_job_result(job_id=job_id, status="failed")


def run_jobs(job_definitions: list[dict]) -> None:
    """
    Iterate over a list of job definitions, validate, and dispatch each one.

    Args:
        job_definitions: List of dicts with keys: job_type, parameters, priority.
    """
    log.info("═" * 60)
    log.info("%s  |  env=%s  |  %d job(s) queued",
             APP_NAME, APP_ENV, len(job_definitions))
    log.info("═" * 60)

    results: list[dict] = []

    for idx, jdef in enumerate(job_definitions, start=1):
        log.info("── Job %d/%d ─────────────────────────", idx, len(job_definitions))

        # Build payload
        payload = build_job_payload(
            job_type=jdef["job_type"],
            parameters=jdef.get("parameters", {}),
            priority=jdef.get("priority", 5),
        )

        # Validate
        is_valid, errors = validate_job_config(payload)
        if not is_valid:
            log.error("Invalid job config – skipping  errors=%s", errors)
            results.append(format_job_result(payload["job_id"], "invalid"))
            continue

        log.debug("Payload validated  keys=%s", list(payload.keys()))

        # Dispatch
        result = dispatch_job(payload)
        results.append(result)

        log.info("Result  job_id=%s  status=%s", result["job_id"], result["status"])

    # ── Summary ────────────────────────────────────────────────────────────────
    log.info("═" * 60)
    accepted = sum(1 for r in results if r["status"] == "accepted")
    failed   = sum(1 for r in results if r["status"] == "failed")
    invalid  = sum(1 for r in results if r["status"] == "invalid")

    log.info("Summary  total=%d  accepted=%d  failed=%d  invalid=%d",
             len(results), accepted, failed, invalid)
    log.info("═" * 60)


# ── Entry point ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    # Simulated job queue – no real API involved
    sample_jobs = [
        {
            "job_type": "generate_report",
            "priority": 2,
            "parameters": {
                "report_id": "RPT-001",
                "format": "pdf",
                "date_range": {"from": "2026-01-01", "to": "2026-03-01"},
            },
        },
        {
            "job_type": "send_notification",
            "priority": 4,
            "parameters": {
                "recipient": "ops@example.com",
                "channel": "email",
                "template": "weekly_digest",
            },
        },
        {
            "job_type": "sync_inventory",
            "priority": 1,
            "parameters": {
                "warehouse_id": "WH-42",
                "full_sync": True,
            },
        },
        {
            "job_type": "resize_images",
            "priority": 7,
            "parameters": {
                "bucket": "assets-bucket",
                "prefix": "uploads/2026/03/",
                "target_widths": [320, 768, 1280],
            },
        },
    ]

    run_jobs(sample_jobs)
