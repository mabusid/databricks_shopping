"""A slow, literal, plain-Python reading of rules/rules.md, used only to test the
Polars pipeline. It shares no code with pipeline/pipeline.py on purpose: different
tools (str.split, datetime, Python ints) so the two do not share the same mistakes.

So far it covers step 3, cleaning: events rules 1-4, clicks rules 1-3, links rule 1.
"""
import glob
import json
import os
import re
from datetime import datetime, timedelta, timezone

DAY_MS = 86_400_000
EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def lines_of(path):
    """Data lines of one CSV file: split on \\n only, header skipped."""
    with open(path, "rb") as f:
        text = f.read().decode("utf-8")
    lines = text.split("\n")[1:]
    if lines and lines[-1] == "":
        lines.pop()
    return lines


def fields_of(line, n):
    """The n field values of a line (None for empty), or None if the line is malformed."""
    parts = line.split(",")
    if len(parts) != n:
        return None  # wrong field count; an empty line has 1 field
    values = []
    for part in parts:
        if '"' in part:
            if len(part) >= 2 and part[0] == '"' and part[-1] == '"' and '"' not in part[1:-1]:
                part = part[1:-1]
            else:
                return None  # a quote anywhere except around the whole field
        values.append(part if part != "" else None)
    return values


def rows(data_dir, folder, n):
    for path in sorted(glob.glob(os.path.join(data_dir, folder, "*.csv"))):
        for line in lines_of(path):
            values = fields_of(line, n)
            if values is not None:
                yield values


ISO = re.compile(r"([0-9]{4})-([0-9]{2})-([0-9]{2})T([0-9]{2}):([0-9]{2}):([0-9]{2})(Z|([+-])([0-9]{2}):([0-9]{2}))")


def ts_ms(s):
    """F1, F2 or F3 -> ms since epoch; None if bad (window not checked here)."""
    if s is None:
        return None
    m = ISO.fullmatch(s)
    if m:
        y, mo, d, h, mi, se = (int(m.group(i)) for i in range(1, 7))
        try:
            local = datetime(y, mo, d, h, mi, se, tzinfo=timezone.utc)  # rejects Feb 30, hour 24, second 60
        except ValueError:
            return None
        offset = timedelta(0)
        if m.group(7) != "Z":
            oh, om = int(m.group(9)), int(m.group(10))
            if oh > 23 or om > 59:
                return None
            offset = timedelta(hours=oh, minutes=om) * (-1 if m.group(8) == "-" else 1)
        return (local - offset - EPOCH) // timedelta(milliseconds=1)
    if re.fullmatch(r"[0-9]+", s):
        return int(s)  # Python ints never overflow; huge values just fall outside the window
    return None


def window(data_dir):
    with open(os.path.join(data_dir, "manifest.json")) as f:
        manifest = json.load(f)
    return ts_ms(manifest["window_start"]), ts_ms(manifest["window_end"])


def clean_events(data_dir):
    """Set of (event_id, user_id, ts, event_type, product_id, price_cents)."""
    start, end = window(data_dir)
    kept = {}  # event_id -> the copy with the smallest ingest_ts
    for event_id, user, ts, ingest, etype, product, price in rows(data_dir, "events", 7):
        if event_id is None:
            continue
        ingest_ms = ts_ms(ingest)
        if event_id not in kept or ingest_ms < kept[event_id][0]:
            kept[event_id] = (ingest_ms, user, ts, etype, product, price)
    out = set()
    for event_id, (ingest_ms, user, ts, etype, product, price) in kept.items():
        t = ts_ms(ts)
        if user is None or t is None or not (start <= t < end):
            continue
        if etype not in ("view", "cart", "purchase") or product is None:
            continue
        value = 0
        if etype == "purchase":
            if price is None or not re.fullmatch(r"[0-9]+", price) or not (1 <= int(price) <= 10_000_000):
                continue
            value = int(price)
        if ingest_ms - t > DAY_MS:
            continue  # late
        out.add((event_id, user, t, etype, product, value))
    return out


def campaign_ids(data_dir):
    path = os.path.join(data_dir, "campaigns.csv")
    return {v[0] for v in (fields_of(line, 2) for line in lines_of(path)) if v is not None}


def clean_clicks(data_dir):
    """Set of (click_id, user_id, campaign_id, ts)."""
    start, end = window(data_dir)
    campaigns = campaign_ids(data_dir)
    kept = {}
    for click_id, user, campaign, ts in rows(data_dir, "clicks", 4):
        if click_id is not None and click_id not in kept:
            kept[click_id] = (user, campaign, ts)
    out = set()
    for click_id, (user, campaign, ts) in kept.items():
        t = ts_ms(ts)
        if user is None or t is None or not (start - 7 * DAY_MS <= t < end) or campaign not in campaigns:
            continue
        out.add((click_id, user, campaign, t))
    return out


def clean_links(data_dir):
    """Set of (user_id_a, user_id_b)."""
    return {(a, b) for a, b in rows(data_dir, "links", 2) if a is not None and b is not None}


def people(data_dir):
    """Linked IDs rules 2-3 by union-find: {user_id: person} for every ID in a kept link.
    An ID in no link is its own person; callers use people.get(id, id)."""
    parent = {}

    def find(x):
        parent.setdefault(x, x)
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    for a, b in clean_links(data_dir):
        root_a, root_b = find(a), find(b)
        if root_a != root_b:
            parent[root_a] = root_b
    members = {}
    for x in list(parent):
        members.setdefault(find(x), []).append(x)
    return {x: min(group) for group in members.values() for x in group}  # str min is byte-wise for UTF-8
