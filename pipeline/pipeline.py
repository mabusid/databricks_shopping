#!/usr/bin/env python3
"""
Shopping Sessions pipeline (Polars). The rules are in rules/rules.md.

Reads  $DATA_DIR    (default /data)
Writes $OUTPUT_DIR  (default /output): user_daily/ and campaign_daily/ as Parquet
Temp   $SCRATCH_DIR (default /scratch)

Steps (README plan): 3 clean the logs, 4 join each person's IDs, 5 build
sessions and user_daily, 6 credit clicks, count assists, campaign_daily.
"""
import glob
import json
import mmap
import os
from datetime import datetime

NCPU = os.environ.get("NCPU")
if NCPU:
    # Must be set before polars is imported.
    os.environ.setdefault("POLARS_MAX_THREADS", NCPU)

import polars as pl  # noqa: E402

DATA = os.environ.get("DATA_DIR", "/data")
OUT = os.environ.get("OUTPUT_DIR", "/output")
SCRATCH = os.environ.get("SCRATCH_DIR", "/scratch")

DAY_MS = 86_400_000
WEEK_MS = 7 * DAY_MS
SESSION_GAP_MS = 30 * 60 * 1000
MAX_PRICE = 10_000_000

EVENT_COLS = ["event_id", "user_id", "ts", "ingest_ts", "event_type", "product_id", "price_cents"]
CLICK_COLS = ["click_id", "user_id", "campaign_id", "ts"]
LINK_COLS = ["user_id_a", "user_id_b"]
PRODUCT_COLS = ["product_id", "category"]
CAMPAIGN_COLS = ["campaign_id", "channel"]
EVENT_TYPES = ["view", "cart", "purchase"]


# --- Reading lines -------------------------------------------------------------
# Polars reads each line as one string column. It must never split a line, so
# we use a separator byte the data does not hold, and no quote character.
# Polars' reader strips a trailing \r, but the rules say \r is an ordinary
# character, and it might split on a line holding the separator. So any file
# holding either byte is split exactly in Python instead (the generator never
# writes a \r, so in practice that is only fixture E20).
SEP = "\x1f"


def needs_exact_split(path):
    """True if Polars' line reader could change this file's lines."""
    with open(path, "rb") as f:
        if os.fstat(f.fileno()).st_size == 0:
            return False
        with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
            return mm.find(b"\r") != -1 or mm.find(SEP.encode()) != -1


def exact_lines(path):
    """The file's lines after the header, split on \\n only, nothing stripped."""
    with open(path, "rb") as f:
        lines = f.read().decode("utf-8").split("\n")[1:]
    if lines and lines[-1] == "":
        lines.pop()  # the file's final \n ends the last line; it does not start a new one
    return lines


def read_lines(paths):
    """Every data line of every file, unchanged, as a LazyFrame with one column `line`.

    Each file's first line is its header and is skipped. A later line equal to
    the header is data (fixture L3). An empty line comes through as null, and
    the field check drops it, as the rules drop empty lines.
    """
    paths = [p for p in paths if os.path.getsize(p) > 0]  # an empty file has no data lines
    fast = [p for p in paths if not needs_exact_split(p)]
    exact = [p for p in paths if p not in fast]
    frames = []
    if fast:
        # skip_lines=1 skips each file's header line (checked on all fixtures and the dev set).
        frames.append(pl.scan_csv(fast, has_header=False, skip_lines=1, separator=SEP,
                                  quote_char=None, schema={"line": pl.String}))
    for path in exact:
        frames.append(pl.LazyFrame({"line": exact_lines(path)}, schema={"line": pl.String}))
    if not frames:
        return pl.LazyFrame(schema={"line": pl.String})
    return pl.concat(frames)


# --- CSV dialect ---------------------------------------------------------------
# A field is either wrapped in quotes with no quote or comma inside, or holds no
# quote at all. Values never hold a comma, so every comma separates fields.
FIELD = '(?:"[^",]*"|[^",]*)'


def split_fields(lines, names):
    """Drop malformed lines and split the rest into string columns `names`.

    A line is malformed unless it has exactly len(names) fields, each either
    fully quoted or without quotes. Quotes are syntax, so they are removed.
    An empty field (`` or `""`) becomes null. Spaces are kept.
    """
    pattern = "^" + FIELD + "(?:," + FIELD + "){" + str(len(names) - 1) + "}$"
    return (
        lines.filter(pl.col("line").str.contains(pattern))
        .select(pl.col("line").str.split_exact(",", len(names) - 1).struct.rename_fields(names))
        .unnest("line")
        .with_columns(pl.col(c).str.strip_chars('"') for c in names)
        .with_columns(pl.when(pl.col(c) != "").then(pl.col(c)).alias(c) for c in names)
    )


# --- Timestamps ----------------------------------------------------------------
# F1 2026-10-03T23:58:10Z, F2 2026-10-03T19:58:10-04:00, F3 epoch milliseconds.
# Our own checks and arithmetic, so we control exactly what is accepted.
ISO_RE = r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:Z|[+-][0-9]{2}:[0-9]{2})$"
DIGITS_RE = r"^[0-9]+$"


def _int(s, start, end):
    """Characters [start, end) of s as an integer (null if not a number)."""
    return s.str.slice(start, end - start).cast(pl.Int64, strict=False)


def digits_value(s, max_len):
    """The value of a digits-only string with leading zeros ignored, or null if it
    has more than max_len significant digits. Call only where s is digits only."""
    stripped = s.str.strip_chars_start("0")
    return (pl.when(stripped == "").then(0)
            .when(stripped.str.len_bytes() <= max_len).then(stripped.cast(pl.Int64, strict=False)))


def parse_ts(s):
    """Timestamp string -> integer ms since the epoch, UTC; null if it is bad.

    The window check is separate. A value with more than 18 significant F3
    digits is null: at least 10^18 ms, it is far outside any window.
    """
    year, month, day = _int(s, 0, 4), _int(s, 5, 7), _int(s, 8, 10)
    hour, minute, second = _int(s, 11, 13), _int(s, 14, 16), _int(s, 17, 19)
    off_hour, off_minute = _int(s, 20, 22), _int(s, 23, 25)
    is_utc = s.str.slice(19, 1) == "Z"
    sign = pl.when(s.str.slice(19, 1) == "-").then(-1).otherwise(1)
    offset_s = pl.when(is_utc).then(0).otherwise(sign * (off_hour * 3600 + off_minute * 60))

    leap = ((year % 4 == 0) & (year % 100 != 0)) | (year % 400 == 0)
    days_in_month = (pl.when(month == 2).then(pl.when(leap).then(29).otherwise(28))
                     .when(month.is_in([4, 6, 9, 11])).then(30)
                     .otherwise(31))
    iso_ok = (
        s.str.contains(ISO_RE)
        & month.is_between(1, 12) & day.is_between(1, days_in_month)
        & (hour <= 23) & (minute <= 59) & (second <= 59)
        & (is_utc | ((off_hour <= 23) & (off_minute <= 59)))
    )
    # Days since 1970-01-01 for a proleptic Gregorian date (H. Hinnant's days_from_civil).
    y = year - (month <= 2).cast(pl.Int64)
    era = y // 400
    yoe = y - era * 400
    doy = (153 * ((month + 9) % 12) + 2) // 5 + day - 1
    doe = yoe * 365 + yoe // 4 - yoe // 100 + doy
    days = era * 146097 + doe - 719468
    iso_ms = (days * 86400 + hour * 3600 + minute * 60 + second - offset_s) * 1000

    return (pl.when(iso_ok).then(iso_ms)
            .when(s.str.contains(DIGITS_RE)).then(digits_value(s, 18)))


def window_ms(manifest):
    """(window_start, window_end) from manifest.json, in ms since the epoch."""
    def ms(text):
        return int(datetime.fromisoformat(text).timestamp()) * 1000
    return ms(manifest["window_start"]), ms(manifest["window_end"])


# --- Cleaning (README step 3) --------------------------------------------------
def csv_files(folder):
    return sorted(glob.glob(os.path.join(folder, "*.csv")))


def clean_events(data_dir, start, end):
    """Events rules 1-4: parse, dedup, validate, drop late.

    Columns: event_id, user_id, ts (ms), event_type, product_id,
    price_cents (the price for a purchase, 0 otherwise).
    """
    events = (
        split_fields(read_lines(csv_files(os.path.join(data_dir, "events"))), EVENT_COLS)
        .filter(pl.col("event_id").is_not_null())
        .with_columns(pl.col("ingest_ts").pipe(parse_ts).alias("ingest_ms"))
    )
    # Dedup before validation: keep the copy with the smallest ingest_ts.
    # Copies with equal ingest_ts are identical, so any one of them will do.
    events = (
        events.filter(pl.col("ingest_ms") == pl.col("ingest_ms").min().over("event_id"))
        .unique(subset="event_id", keep="any")
    )
    is_purchase = pl.col("event_type") == "purchase"
    price = pl.col("price_cents")
    price_value = pl.when(price.str.contains(DIGITS_RE)).then(digits_value(price, 8))
    events = events.with_columns(
        pl.col("ts").pipe(parse_ts).alias("ts_ms"),
        pl.when(is_purchase).then(price_value).otherwise(0).alias("price_value"),
    )
    return (
        events.filter(
            pl.col("user_id").is_not_null()
            & pl.col("ts_ms").is_between(start, end, closed="left")
            & pl.col("event_type").is_in(EVENT_TYPES)
            & pl.col("product_id").is_not_null()
            & (~is_purchase | pl.col("price_value").is_between(1, MAX_PRICE))
            & (pl.col("ingest_ms") - pl.col("ts_ms") <= DAY_MS)  # late: strictly over 24 h
        )
        .select("event_id", "user_id", pl.col("ts_ms").alias("ts"), "event_type", "product_id",
                pl.col("price_value").alias("price_cents"))
    )


def clean_clicks(data_dir, start, end, campaigns):
    """Clicks rules 1-3: parse, dedup, validate. Columns: click_id, user_id, campaign_id, ts (ms)."""
    clicks = (
        split_fields(read_lines(csv_files(os.path.join(data_dir, "clicks"))), CLICK_COLS)
        .filter(pl.col("click_id").is_not_null())
        .unique(subset="click_id", keep="any")  # copies are identical
        .with_columns(pl.col("ts").pipe(parse_ts))
    )
    return clicks.filter(
        pl.col("user_id").is_not_null()
        & pl.col("ts").is_between(start - WEEK_MS, end, closed="left")
        & pl.col("campaign_id").is_in(campaigns["campaign_id"].implode())
    ).select(CLICK_COLS)


def clean_links(data_dir):
    """Linked IDs rule 1: parse, drop lines with an empty side. Columns: user_id_a, user_id_b."""
    links = split_fields(read_lines(csv_files(os.path.join(data_dir, "links"))), LINK_COLS)
    return links.drop_nulls()


def read_table(path, names):
    """products.csv or campaigns.csv: clean files, read with the same dialect rules."""
    return split_fields(read_lines([path]), names).collect()


# --- Linked IDs (README step 4) ------------------------------------------------
def person_names(links):
    """Linked IDs rules 2-3: for every ID in a kept link, user_id -> person.

    A person is a connected component of the link graph, however long its
    chains, and is named by its byte-wise smallest ID. An ID in no link is a
    person on its own; with_person handles that.
    """
    import numpy as np
    from scipy.sparse import coo_matrix
    from scipy.sparse.csgraph import connected_components

    links = links.collect()
    # Sorted, so the order of the ID numbers is the IDs' byte order (Polars sorts strings byte-wise).
    ids = pl.concat([links["user_id_a"], links["user_id_b"]]).unique().sort()
    if ids.len() == 0:
        return pl.DataFrame(schema={"user_id": pl.String, "person": pl.String})
    a = ids.search_sorted(links["user_id_a"]).to_numpy()
    b = ids.search_sorted(links["user_id_b"]).to_numpy()
    # int32 weights: repeated links are summed, and a narrower type could overflow to 0.
    graph = coo_matrix((np.ones(len(a), dtype=np.int32), (a, b)), shape=(ids.len(), ids.len()))
    _, component = connected_components(graph, directed=False)
    # The first ID of each component in byte order is its smallest, so it is the name.
    _, first = np.unique(component, return_index=True)
    return pl.DataFrame({"user_id": ids, "person": ids.gather(first[component])})


def with_person(frame, names):
    """Replace user_id with person: the linked name, or the ID itself if it is in no link."""
    return (
        frame.join(names.lazy(), on="user_id", how="left")
        .with_columns(pl.coalesce("person", "user_id").alias("person"))
        .drop("user_id")
    )


# --- Sessions (README step 5) --------------------------------------------------
def sessionize(events):
    """Events rules 5-6: give each event its session (an id), start (ms) and day.

    Sorted by person and ts, a session starts at a person's first event and
    wherever the gap from the previous event is over 30 minutes. Events with
    equal ts have a gap of 0, so they always share a session; when ts changes,
    the previous event holds the previous distinct ts, so the gap is measured
    between distinct timestamps, as the rules say.
    """
    ts, person = pl.col("ts"), pl.col("person")
    new_session = ((person != person.shift(1)) | (ts - ts.shift(1) > SESSION_GAP_MS)).fill_null(True)
    return (
        events.sort("person", "ts")
        .with_columns(new_session.cum_sum().alias("session"))
        .with_columns(ts.min().over("session").alias("start"))
        .with_columns(utc_date(pl.col("start")).alias("day"))  # rule 6: the start's UTC date
    )


def utc_date(ms):
    """ms since the epoch -> its UTC calendar date."""
    return (ms // DAY_MS).cast(pl.Int32).cast(pl.Date)


def session_table(sessionized):
    """One row per session: person, start, day, events, revenue_cents, purchases."""
    return sessionized.group_by("session").agg(
        pl.col("person").first(),
        pl.col("start").first(),
        pl.col("day").first(),
        pl.len().cast(pl.Int64).alias("events"),
        pl.col("price_cents").sum().alias("revenue_cents"),  # 0 for views and carts
        (pl.col("event_type") == "purchase").sum().cast(pl.Int64).alias("purchases"),
    )


def user_daily(sessionized, sessions, products):
    """The user_daily table: one row per person and day with a session."""
    totals = sessions.group_by("person", "day").agg(
        pl.len().cast(pl.Int64).alias("sessions"),
        pl.col("events").sum(),
        pl.col("revenue_cents").sum(),
    )
    # Rule 7: a product not in products.csv is `unknown`, which competes like any category.
    # top_category: the highest summed purchase price; a tie goes to the byte-wise smallest.
    top = (
        sessionized.filter(pl.col("event_type") == "purchase")
        .join(products.lazy(), on="product_id", how="left")
        .with_columns(pl.col("category").fill_null("unknown"))
        .group_by("person", "day", "category").agg(pl.col("price_cents").sum().alias("total"))
        .group_by("person", "day").agg(
            pl.col("category").sort_by("total", "category", descending=[True, False]).first()
            .alias("top_category"))
    )
    return (
        totals.join(top, on=["person", "day"], how="left")  # null top_category: no purchases
        .select(pl.col("person").alias("user_id"), "day", "sessions", "events", "revenue_cents",
                "top_category")
    )


# --- Clicks: credit and assists (README step 6) --------------------------------
def credit(sessions, clicks):
    """Clicks rules 4-5: add each session's credited campaign_id.

    The person's latest click with click.ts <= start and start - click.ts <= 7 days;
    on a tie in ts, the smallest click_id. No such click: `organic`.
    """
    # One click per (person, ts): the smallest click_id, so the tie is settled before the join.
    latest = (
        clicks.group_by("person", "ts")
        .agg(pl.col("campaign_id").sort_by("click_id").first())
        .rename({"ts": "click_ts"})
        .sort("click_ts")
    )
    return (
        sessions.sort("start")
        # Both sides are sorted on the join key just above; Polars can't verify that with `by`.
        .join_asof(latest, left_on="start", right_on="click_ts", by="person", strategy="backward",
                   check_sortedness=False)
        .with_columns(
            pl.when(pl.col("start") - pl.col("click_ts") <= WEEK_MS)
            .then(pl.col("campaign_id")).otherwise(pl.lit("organic")).alias("campaign_id"))
        .drop("click_ts")
    )


def assists(sessions, clicks):
    """Clicks rule 7: assisted_sessions per (campaign_id, day).

    A session is assisted by a campaign if the person clicked it at some ts with
    start - 7 days <= ts <= start, that is, if its start lies in [ts, ts + 7 days].
    Listing every (session, campaign) pair would be huge for heavy users, so we
    count instead:
      1. Per (person, campaign), merge the clicks' [ts, ts + 7 days] spans into
         disjoint runs (a new run starts where the gap between clicks is over 7 days),
         so each session is counted at most once per campaign.
      2. Cut each run at day boundaries, keeping only days the person has sessions on.
      3. Count the person's sessions in each piece from a running count of their
         sessions in start order, looked up with join_asof.
    """
    person, campaign, ts = pl.col("person"), pl.col("campaign_id"), pl.col("ts")
    new_run = ((person != person.shift(1)) | (campaign != campaign.shift(1))
               | (ts - ts.shift(1) > WEEK_MS)).fill_null(True)
    runs = (
        clicks.select("person", "campaign_id", "ts").sort("person", "campaign_id", "ts")
        .with_columns(new_run.cum_sum().alias("run"))
        .group_by("run").agg(
            person.first(), campaign.first(),
            ts.min().alias("run_from"), (ts.max() + WEEK_MS).alias("run_to"))
    )
    day_from = pl.col("day").cast(pl.Int64) * DAY_MS  # first ms of the day
    person_days = sessions.select("person", "day").unique().with_columns(day_from.alias("day_from"))
    pieces = (
        runs.join(person_days, on="person")
        .filter((pl.col("day_from") <= pl.col("run_to")) & (pl.col("day_from") + DAY_MS > pl.col("run_from")))
        .with_columns(
            pl.max_horizontal("run_from", "day_from").alias("piece_from"),
            pl.min_horizontal("run_to", pl.col("day_from") + DAY_MS - 1).alias("piece_to"))
    )
    # How many of the person's sessions start at or before t, for any t.
    running = (sessions.select("person", "start")
               .with_columns(pl.col("start").rank("ordinal").over("person").cast(pl.Int64).alias("count"))
               .sort("start"))

    def sessions_up_to(frame, t, name):
        # Both sides are sorted on the join key; Polars can't verify that with `by`.
        frame = frame.sort(t).join_asof(running, left_on=t, right_on="start", by="person",
                                        strategy="backward", check_sortedness=False)
        return frame.with_columns(pl.col("count").fill_null(0).alias(name)).drop("start", "count")

    pieces = sessions_up_to(pieces.with_columns((pl.col("piece_from") - 1).alias("before")),
                            "before", "n_before")
    pieces = sessions_up_to(pieces, "piece_to", "n_to")
    return (
        pieces.with_columns((pl.col("n_to") - pl.col("n_before")).alias("n"))
        .filter(pl.col("n") > 0)
        .group_by("campaign_id", "day").agg(pl.col("n").sum().cast(pl.Int64).alias("assisted_sessions"))
    )


def campaign_daily(credited, assisted, campaigns):
    """The campaign_daily table: one row per (campaign_id, day) credited or assisted."""
    by_credit = credited.group_by("campaign_id", "day").agg(
        pl.len().cast(pl.Int64).alias("sessions"),
        (pl.col("purchases") > 0).sum().cast(pl.Int64).alias("purchasing_sessions"),
        pl.col("revenue_cents").sum(),
    )
    counts = ["sessions", "purchasing_sessions", "revenue_cents", "assisted_sessions"]
    return (
        by_credit.join(assisted, on=["campaign_id", "day"], how="full", coalesce=True)
        .with_columns(pl.col(c).fill_null(0) for c in counts)  # assisted only: 0 credited; organic: 0 assisted
        .join(campaigns, on="campaign_id", how="left")
        .with_columns(pl.col("channel").fill_null("organic"))  # only organic is not in campaigns.csv
        .select("campaign_id", "channel", "day", *counts)
    )


# --- Output --------------------------------------------------------------------
USER_DAILY = {
    "user_id": pl.String, "day": pl.Date, "sessions": pl.Int64,
    "events": pl.Int64, "revenue_cents": pl.Int64, "top_category": pl.String,
}
CAMPAIGN_DAILY = {
    "campaign_id": pl.String, "channel": pl.String, "day": pl.Date,
    "sessions": pl.Int64, "purchasing_sessions": pl.Int64, "revenue_cents": pl.Int64,
    "assisted_sessions": pl.Int64,
}


def main():
    with open(os.path.join(DATA, "manifest.json")) as f:
        manifest = json.load(f)
    start, end = window_ms(manifest)
    print("window:", manifest["window_start"], "to", manifest["window_end"])
    print("polars threads:", pl.thread_pool_size())

    products = read_table(os.path.join(DATA, "products.csv"), PRODUCT_COLS)
    campaigns = read_table(os.path.join(DATA, "campaigns.csv"), CAMPAIGN_COLS)
    names = person_names(clean_links(DATA))
    events = with_person(clean_events(DATA, start, end), names)
    clicks = with_person(clean_clicks(DATA, start, end, campaigns), names).collect()

    sessionized = sessionize(events).collect()
    sessions = session_table(sessionized.lazy()).collect()
    write_table(user_daily(sessionized.lazy(), sessions.lazy(), products).collect(), "user_daily", USER_DAILY)

    credited = credit(sessions, clicks)
    write_table(campaign_daily(credited, assists(sessions, clicks), campaigns), "campaign_daily", CAMPAIGN_DAILY)
    print("wrote user_daily and campaign_daily")


def write_table(frame, name, schema):
    """Write one output table, with exactly the columns and types of `schema`."""
    folder = os.path.join(OUT, name)
    os.makedirs(folder, exist_ok=True)
    frame.select(pl.col(c).cast(t) for c, t in schema.items()).write_parquet(
        os.path.join(folder, "part-0.parquet"))


if __name__ == "__main__":
    main()
