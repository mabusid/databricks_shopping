"""A slow, literal, plain-Python reading of rules/rules.md, used only to test the
Polars pipeline. It shares no code with pipeline/pipeline.py on purpose: different
tools (str.split, datetime, Python ints) so the two do not share the same mistakes.

It covers steps 3-6: cleaning, linked IDs, sessions and user_daily, and click
credit, assists and campaign_daily.
"""
import bisect
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


GAP_MS = 30 * 60 * 1000


def day_of(ms):
    return (EPOCH + timedelta(milliseconds=ms)).date()


def sessions(data_dir):
    """Events rules 5-6. List of dicts: person, start, day, events (the event tuples)."""
    names = people(data_dir)
    by_person = {}
    for event in clean_events(data_dir):
        by_person.setdefault(names.get(event[1], event[1]), []).append(event)
    out = []
    for person, events in by_person.items():
        # Walk the person's distinct timestamps; each one joins the session the
        # previous distinct one is in, unless the gap is over 30 minutes.
        start_of = {}  # ts -> its session's start
        start = previous = None
        for t in sorted({e[2] for e in events}):
            if previous is None or t - previous > GAP_MS:
                start = t
            start_of[t] = start
            previous = t
        grouped = {}
        for event in events:
            grouped.setdefault(start_of[event[2]], []).append(event)
        for start, members in grouped.items():
            out.append({"person": person, "start": start, "day": day_of(start), "events": members})
    return out


def products(data_dir):
    path = os.path.join(data_dir, "products.csv")
    return dict(v for v in (fields_of(line, 2) for line in lines_of(path)) if v is not None)


def user_daily(data_dir):
    """Set of (user_id, day, sessions, events, revenue_cents, top_category)."""
    category = products(data_dir)
    days = {}
    for s in sessions(data_dir):
        d = days.setdefault((s["person"], s["day"]), {"sessions": 0, "events": 0, "revenue": 0, "spend": {}})
        d["sessions"] += 1
        d["events"] += len(s["events"])
        for _, _, _, etype, product, price in s["events"]:
            if etype == "purchase":
                d["revenue"] += price
                c = category.get(product, "unknown")
                d["spend"][c] = d["spend"].get(c, 0) + price
    out = set()
    for (person, day), d in days.items():
        # Highest spend; a tie goes to the byte-wise smallest category.
        top = min(d["spend"].items(), key=lambda kv: (-kv[1], kv[0]))[0] if d["spend"] else None
        out.add((person, day, d["sessions"], d["events"], d["revenue"], top))
    return out


def campaign_daily(data_dir):
    """Clicks rules 4-7, brute force per session. Set of (campaign_id, channel, day,
    sessions, purchasing_sessions, revenue_cents, assisted_sessions)."""
    names = people(data_dir)
    path = os.path.join(data_dir, "campaigns.csv")
    channel = dict(v for v in (fields_of(line, 2) for line in lines_of(path)) if v is not None)
    clicks_of = {}  # person -> sorted [(ts, click_id, campaign)]
    for click_id, user, campaign, t in clean_clicks(data_dir):
        clicks_of.setdefault(names.get(user, user), []).append((t, click_id, campaign))
    times_of = {}
    for person, clicks in clicks_of.items():
        clicks.sort()
        times_of[person] = [c[0] for c in clicks]
    table = {}
    for s in sessions(data_dir):
        clicks, times = clicks_of.get(s["person"], []), times_of.get(s["person"], [])
        # The person's clicks with start - 7 days <= ts <= start.
        window = clicks[bisect.bisect_left(times, s["start"] - 7 * DAY_MS):bisect.bisect_right(times, s["start"])]
        if window:
            latest = max(c[0] for c in window)
            credited = min((c[1], c[2]) for c in window if c[0] == latest)[1]  # tie: smallest click_id
        else:
            credited = "organic"
        row = table.setdefault((credited, s["day"]), [0, 0, 0, 0])
        row[0] += 1
        row[1] += any(e[3] == "purchase" for e in s["events"])
        row[2] += sum(e[5] for e in s["events"])
        for campaign in {c[2] for c in window}:  # each campaign assists once
            table.setdefault((campaign, s["day"]), [0, 0, 0, 0])[3] += 1
    return {(c, channel.get(c, "organic"), d, *row) for (c, d), row in table.items()}
