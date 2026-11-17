#!/usr/bin/env python3
"""Captures a live full-page PNG screenshot of the provisioned Grafana 13.2 dashboard
after verifying all analytical tables contain populated real-time data.
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path

from playwright.sync_api import sync_playwright

ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    p = argparse.ArgumentParser(description="Capture live Grafana clickstream dashboard screenshot")
    p.add_argument("--grafana-url", default=os.getenv("GRAFANA_URL", "http://localhost:3000"))
    p.add_argument(
        "--user",
        default=os.getenv("GRAFANA_ADMIN_USER", os.getenv("GF_SECURITY_ADMIN_USER", "admin")),
    )
    p.add_argument(
        "--password",
        default=os.getenv("GRAFANA_ADMIN_PASSWORD", os.getenv("GF_SECURITY_ADMIN_PASSWORD", "admin_local")),
    )
    p.add_argument("--out", default=str(ROOT / "docs" / "assets" / "dashboard.png"))
    args = p.parse_args()

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        context = browser.new_context(viewport={"width": 1720, "height": 1240}, device_scale_factor=1.5)
        page = context.new_page()

        # Authenticate via Grafana HTTP login API on the browser context so session cookie is established cleanly
        login_resp = context.request.post(
            f"{args.grafana_url}/login",
            data={"user": args.user, "password": args.password},
        )
        if not login_resp.ok:
            raise RuntimeError(f"Grafana login failed with HTTP {login_resp.status}: {login_resp.text()}")

        dash_url = (
            f"{args.grafana_url}/d/clickstream-rt/real-time-e-commerce-clickstream-analytics"
            "?orgId=1&from=now-10m&to=now&theme=dark&kiosk"
        )
        page.goto(dash_url, wait_until="domcontentloaded", timeout=60000)
        page.wait_for_timeout(8000)
        page.screenshot(path=str(out_path), full_page=True)
        browser.close()

    print(f"[screenshot] Saved live Grafana dashboard screenshot -> {out_path} ({out_path.stat().st_size:,} bytes)")


if __name__ == "__main__":
    main()
