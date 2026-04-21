#!/usr/bin/env python3
"""
Orchestrator for the unmmgdss → Delphi OMOP incremental ETL.

Reads config.yaml, opens source + destination connections, then runs each
enabled step in dependency order. Each step module exposes:

    def run(src_conn, dst_conn, cfg) -> int   # returns rows processed

Add a step by creating its module (e.g. step_02_care_site.py) and appending
its name to STEPS. Toggle individual steps via config.yaml `steps.*`.
"""

from __future__ import annotations

import importlib
import logging
import pathlib
import sys
import time

import db

CONFIG_PATH = pathlib.Path(__file__).parent / "config.yaml"

# Dependency-ordered registry. Empty until per-step modules are implemented;
# see ArchitecturePlan.md §4 and the plan in /home/smathias/.claude/plans/.
STEPS: list[str] = [
    "step_02_care_site",
]


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("etl")


def main() -> int:
    cfg = db.load_config(CONFIG_PATH)
    enabled_cfg = cfg.get("steps", {})

    log.info("Opening connections")
    src_conn = db.connect_source(cfg)
    dst_conn = db.connect_destination(cfg)

    try:
        for name in STEPS:
            if not enabled_cfg.get(name, True):
                log.info("[%s] disabled in config — skipping", name)
                continue

            log.info("[%s] starting", name)
            start = time.perf_counter()
            try:
                module = importlib.import_module(name)
                rows = module.run(src_conn, dst_conn, cfg)
            except Exception:
                elapsed = time.perf_counter() - start
                log.exception("[%s] FAILED after %.1fs", name, elapsed)
                return 1
            elapsed = time.perf_counter() - start
            log.info("[%s] done — %s rows in %.1fs", name, rows, elapsed)

        if not STEPS:
            log.info("No steps registered yet. See STEPS in this file.")
        return 0
    finally:
        src_conn.close()
        dst_conn.close()


if __name__ == "__main__":
    sys.exit(main())
