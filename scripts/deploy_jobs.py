#!/usr/bin/env python3
"""Submits both Flink 2.2 streaming jobs (SQL pipeline + Java DataStream sessioniser)
and waits until both reach RUNNING with at least one completed RocksDB checkpoint on MinIO S3.
"""

from __future__ import annotations

import argparse
import subprocess
import time
from typing import Any

import requests


def list_running_jobs(flink_url: str) -> dict[str, str]:
    resp = requests.get(f"{flink_url}/jobs/overview", timeout=10)
    resp.raise_for_status()
    return {j["name"]: j["jid"] for j in resp.json().get("jobs", []) if j["state"] == "RUNNING"}


def submit_flink_job(cmd_args: list[str]) -> str:
    full_cmd = ["docker", "exec", "flink-jobmanager", "/opt/flink/bin/flink", "run", "-d", *cmd_args]
    try:
        out = subprocess.check_output(full_cmd, text=True, stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as exc:
        print(f"[flink] ERROR (exit={exc.returncode}) output:\n{exc.output}", flush=True)
        raise
    print(out.strip())
    return out


def wait_for_completed_checkpoint(
    flink_url: str, jid: str, name: str, min_completed: int = 2, timeout_sec: int = 90
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_sec
    while time.monotonic() < deadline:
        resp = requests.get(f"{flink_url}/jobs/{jid}/checkpoints", timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            completed = int(data.get("counts", {}).get("completed", 0))
            if completed >= min_completed:
                latest = data.get("latest", {}).get("completed", {}) or {}
                print(
                    f"[flink] Job {name} ({jid}) RUNNING | completed_checkpoints={completed} "
                    f"| external_path={latest.get('external_path', 'n/a')}"
                )
                return data
        time.sleep(1.5)
    raise TimeoutError(f"Timed out waiting for completed checkpoint on Flink job {name} ({jid})")


def wait_for_task_slots(flink_url: str, min_slots: int = 12, timeout_sec: int = 60) -> None:
    deadline = time.monotonic() + timeout_sec
    while time.monotonic() < deadline:
        resp = requests.get(f"{flink_url}/overview", timeout=10)
        if resp.status_code == 200:
            ov = resp.json()
            slots = int(ov.get("slots-total", 0))
            if slots >= min_slots:
                print(f"[flink] Cluster ready: taskmanagers={ov.get('taskmanagers')} slots_total={slots}")
                return
        time.sleep(1.0)
    raise TimeoutError(f"Timed out waiting for >= {min_slots} Flink task slots at {flink_url}/overview")


def main() -> None:
    p = argparse.ArgumentParser(description="Deploy Flink SQL and DataStream jobs")
    p.add_argument("--flink-url", default="http://localhost:8081")
    p.add_argument("--parallelism", default="3")
    p.add_argument("--job-version", default="v1")
    args = p.parse_args()

    wait_for_task_slots(args.flink_url, min_slots=12)
    running = list_running_jobs(args.flink_url)

    sql_name = "clickstream-realtime-sql-pipeline"
    if sql_name not in running:
        print(f"[flink] Submitting {sql_name}...")
        submit_flink_job(
            [
                "-c",
                "io.clickstream.flink.SqlPipelineRunner",
                "/opt/flink/usrlib/clickstream-flink-jobs.jar",
                "--sql-file",
                "/opt/flink/sql/01_pipeline.sql",
                "--pipeline-name",
                sql_name,
                "--parallelism",
                args.parallelism,
            ]
        )
    else:
        print(f"[flink] {sql_name} already RUNNING ({running[sql_name]})")

    ds_prefix = "clickstream-datastream-sessions-"
    if not any(k.startswith(ds_prefix) for k in running):
        ds_name = f"{ds_prefix}{args.job_version}"
        print(f"[flink] Submitting {ds_name}...")
        submit_flink_job(
            [
                "-c",
                "io.clickstream.flink.SessionAnalyticsJob",
                "/opt/flink/usrlib/clickstream-flink-jobs.jar",
                "--parallelism",
                args.parallelism,
                "--job-version",
                args.job_version,
            ]
        )
    else:
        print("[flink] DataStream sessioniser job already RUNNING")

    # Wait for both jobs to enter RUNNING and complete >=1 S3 RocksDB checkpoint
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        running = list_running_jobs(args.flink_url)
        if sql_name in running and any(k.startswith(ds_prefix) for k in running):
            break
        time.sleep(1.0)
    else:
        raise RuntimeError(f"Expected both Flink jobs RUNNING, got: {running}")

    for name, jid in running.items():
        wait_for_completed_checkpoint(args.flink_url, jid, name)


if __name__ == "__main__":
    main()
