#!/usr/bin/env python3
"""Step 3 check: the pipeline's cleaned events, clicks and links must equal the
plain-Python oracle's, row for row.

Usage: python tests/check_clean.py DATA_DIR...   (e.g. fixtures/*/input data)
"""
import json
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "pipeline"))
sys.path.insert(0, os.path.dirname(__file__))

import oracle  # noqa: E402
import pipeline as pp  # noqa: E402


def pipeline_sets(data_dir):
    with open(os.path.join(data_dir, "manifest.json")) as f:
        start, end = pp.window_ms(json.load(f))
    campaigns = pp.read_table(os.path.join(data_dir, "campaigns.csv"), pp.CAMPAIGN_COLS)
    return {
        "events": set(pp.clean_events(data_dir, start, end).collect().rows()),
        "clicks": set(pp.clean_clicks(data_dir, start, end, campaigns).collect().rows()),
        "links": set(pp.clean_links(data_dir).collect().rows()),
    }


def oracle_sets(data_dir):
    return {
        "events": oracle.clean_events(data_dir),
        "clicks": oracle.clean_clicks(data_dir),
        "links": oracle.clean_links(data_dir),
    }


def main():
    failed = 0
    for data_dir in sys.argv[1:]:
        t0 = time.time()
        got = pipeline_sets(data_dir)
        t1 = time.time()
        want = oracle_sets(data_dir)
        problems = []
        for table in want:
            extra, missing = got[table] - want[table], want[table] - got[table]
            if extra or missing:
                problems.append(f"{table}: {len(extra)} extra, {len(missing)} missing; "
                                f"extra e.g. {sorted(extra)[:3]}, missing e.g. {sorted(missing)[:3]}")
        counts = ", ".join(f"{t} {len(want[t])}" for t in want)
        if problems:
            failed += 1
            print(f"DIFF {data_dir} ({counts})")
            for p in problems:
                print("   ", p)
        else:
            print(f"ok   {data_dir} ({counts}; pipeline {t1 - t0:.2f}s, oracle {time.time() - t1:.2f}s)")
    print(f"\n{len(sys.argv) - 1 - failed}/{len(sys.argv) - 1} match")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
