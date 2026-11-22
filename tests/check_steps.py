#!/usr/bin/env python3
"""Step-by-step check: each finished pipeline step must equal the plain-Python
oracle's result, row for row.

Step 3: cleaned events, clicks, links. Step 4: each linked ID's person, and the
person of every event and click. Step 5: the sessions and the user_daily table.
Step 6: the campaign_daily table.

Usage: python tests/check_steps.py DATA_DIR...   (e.g. fixtures/*/input data)
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
    events = pp.clean_events(data_dir, start, end)
    clicks = pp.clean_clicks(data_dir, start, end, campaigns)
    names = pp.person_names(pp.clean_links(data_dir))
    products = pp.read_table(os.path.join(data_dir, "products.csv"), pp.PRODUCT_COLS)
    sessionized = pp.sessionize(pp.with_person(events, names)).collect()
    sessions = pp.session_table(sessionized.lazy()).collect()
    clicks_p = pp.with_person(clicks, names).collect()
    campaigns_daily = pp.campaign_daily(pp.credit(sessions, clicks_p), pp.assists(sessions, clicks_p), campaigns)
    return {
        "campaign_daily": set(campaigns_daily.rows()),
        "sessions": set(sessions.select("person", "start", "day", "events", "revenue_cents", "purchases").rows()),
        "user_daily": set(pp.user_daily(sessionized.lazy(), sessions.lazy(), products).collect().rows()),
        "events": set(events.collect().rows()),
        "clicks": set(clicks.collect().rows()),
        "links": set(pp.clean_links(data_dir).collect().rows()),
        "people": set(names.rows()),
        "event_person": set(pp.with_person(events, names).select("event_id", "person").collect().rows()),
        "click_person": set(pp.with_person(clicks, names).select("click_id", "person").collect().rows()),
    }


def oracle_sets(data_dir):
    events, clicks = oracle.clean_events(data_dir), oracle.clean_clicks(data_dir)
    people = oracle.people(data_dir)
    return {
        "campaign_daily": oracle.campaign_daily(data_dir),
        "sessions": {(s["person"], s["start"], s["day"], len(s["events"]), sum(e[5] for e in s["events"]),
                      sum(e[3] == "purchase" for e in s["events"])) for s in oracle.sessions(data_dir)},
        "user_daily": oracle.user_daily(data_dir),
        "events": events,
        "clicks": clicks,
        "links": oracle.clean_links(data_dir),
        "people": set(people.items()),
        "event_person": {(e[0], people.get(e[1], e[1])) for e in events},
        "click_person": {(c[0], people.get(c[1], c[1])) for c in clicks},
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
