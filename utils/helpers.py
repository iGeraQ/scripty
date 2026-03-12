"""
utils/helpers.py
Utility functions for job building, validation, and simulated worker calls.
"""

import os
import time
import random
import uuid
from datetime import datetime, timezone


# ──────────────────────────────────────────────────────────────────────────────
# Job building
# ──────────────────────────────────────────────────────────────────────────────

def build_job_payload(
    job_type: str,
    parameters: dict,
    priority: int = 5,
) -> dict:
    """
    Construct a standardised job payload dict.

    Args:
        job_type:   Identifier for the kind of job (e.g. 'resize_image').
        parameters: Arbitrary key/value pairs the job needs to run.
        priority:   1 (highest) – 10 (lowest). Defaults to 5.

    Returns:
        Fully populated job payload ready to be dispatched.
    """
    if not (1 <= priority <= 10):
        raise ValueError(f"Priority must be 1–10, got {priority}")

    return {
        "job_id": str(uuid.uuid4()),
        "job_type": job_type,
        "priority": priority,
        "parameters": parameters,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "status": "pending",
        "retries": 0,
        "max_retries": int(os.getenv("JOB_MAX_RETRIES", 3)),
    }


# ──────────────────────────────────────────────────────────────────────────────
# Validation
# ──────────────────────────────────────────────────────────────────────────────

REQUIRED_JOB_KEYS = {"job_id", "job_type", "parameters", "status"}


def validate_job_config(payload: dict) -> tuple[bool, list[str]]:
    """
    Validate that a job payload contains the required fields.

    Returns:
        (is_valid, list_of_errors)
    """
    errors: list[str] = []

    missing = REQUIRED_JOB_KEYS - payload.keys()
    if missing:
        errors.append(f"Missing required keys: {', '.join(sorted(missing))}")

    if "priority" in payload and not (1 <= payload["priority"] <= 10):
        errors.append(f"Invalid priority value: {payload['priority']}")

    if "job_type" in payload and not payload["job_type"].strip():
        errors.append("job_type cannot be empty")

    return (len(errors) == 0, errors)


# ──────────────────────────────────────────────────────────────────────────────
# Result formatting
# ──────────────────────────────────────────────────────────────────────────────

def format_job_result(job_id: str, status: str, output: dict | None = None) -> dict:
    """
    Wrap a job result into a uniform response envelope.
    """
    return {
        "job_id": job_id,
        "status": status,
        "output": output or {},
        "finished_at": datetime.now(timezone.utc).isoformat(),
    }


# ──────────────────────────────────────────────────────────────────────────────
# Simulated worker call  (no real HTTP request)
# ──────────────────────────────────────────────────────────────────────────────

def simulate_worker_call(payload: dict) -> dict:
    """
    Pretend to POST a job to a remote worker endpoint.

    Simulates:
      - Network latency  (random 0.1–0.8 s)
      - 15 % chance of transient failure to exercise retry logic
      - Successful response with a mock output

    Returns:
        dict with keys: success (bool), status_code (int), body (dict)
    """
    base_url = os.getenv("WORKER_BASE_URL", "http://localhost:8080")
    api_key  = os.getenv("WORKER_API_KEY", "")

    # Simulate latency
    latency = round(random.uniform(0.1, 0.8), 3)
    time.sleep(latency)

    # Simulate occasional failure
    if random.random() < 0.15:
        return {
            "success": False,
            "status_code": 503,
            "body": {"error": "Service temporarily unavailable (simulated)"},
            "latency_s": latency,
            "endpoint": f"{base_url}/jobs",
        }

    return {
        "success": True,
        "status_code": 202,
        "body": {
            "accepted": True,
            "queue_position": random.randint(1, 20),
            "estimated_wait_s": random.randint(1, 10),
        },
        "latency_s": latency,
        "endpoint": f"{base_url}/jobs",
        "auth_used": bool(api_key),
    }
