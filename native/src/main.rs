//! Shopping Sessions: the whole pipeline as one native program. Rules: rules/rules.md.
//!
//! Reads  $DATA_DIR   (default /data)
//! Writes $OUTPUT_DIR (default /output): user_daily/ and campaign_daily/ as Parquet
//!
//! The work runs in a few parallel passes over 256 hash buckets:
//!   1. events: parse every copy, work out whether it is valid, bucket by event_id
//!   2. per event_id bucket: keep each event's earliest copy (dedup before
//!      validation), drop invalid ones, re-bucket by person
//!   3. clicks: parse and validate, bucket by person
//!   4. per person bucket: sessions, user_daily rows, click credit, assists
//! Every field is a slice of the memory-mapped input; nothing is copied.

mod input;
mod output;
mod parse;

use foldhash::fast::FixedState;
use hashbrown::hash_map::Entry;
use hashbrown::{HashMap, HashTable};
use output::Column;
use parse::{ts_ms, BAD};
use rayon::prelude::*;
use std::hash::BuildHasher;
use std::path::{Path, PathBuf};
use std::time::Instant;

const DAY: i64 = 86_400_000;
const WEEK: i64 = 7 * DAY;
const SESSION_GAP: i64 = 30 * 60 * 1000;
/// log2 of the number of hash buckets, set once from the input size so each
/// bucket's tables stay small enough for a core's L2 cache (~16k events).
static BUCKET_BITS: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(8);
const CHUNK_BYTES: usize = 2 << 20;

const VIEW: u8 = 0;
const CART: u8 = 1;
const PURCHASE: u8 = 2;
const BAD_TYPE: u8 = 3;

type Str = &'static [u8];
type Map<K, V> = HashMap<K, V, FixedState>;

fn hasher() -> FixedState {
    FixedState::with_seed(0x5eed_5e55)
}

#[inline]
fn hash(key: Str) -> u64 {
    hasher().hash_one(key)
}

/// Each key is hashed once; the hash's top bits pick its bucket
/// and is reused in the bucket's hash table.
#[inline]
fn bucket(h: u64) -> usize {
    (h >> (64 - BUCKET_BITS.load(std::sync::atomic::Ordering::Relaxed))) as usize
}

fn buckets() -> usize {
    1 << BUCKET_BITS.load(std::sync::atomic::Ordering::Relaxed)
}

/// Run `work` with `acc()`, which gives the calling rayon worker its own
/// accumulator (no other thread ever touches it, so the lock is never contended),
/// and return all the accumulators.
fn per_thread<A: Send>(threads: usize, new: impl Fn() -> A, work: impl for<'a> FnOnce(&'a (dyn Fn() -> std::sync::MutexGuard<'a, A> + Sync))) -> Vec<A> {
    let accs: Vec<std::sync::Mutex<A>> = (0..threads).map(|_| std::sync::Mutex::new(new())).collect();
    let acc = || accs[rayon::current_thread_index().unwrap_or(0) % threads].lock().unwrap();
    work(&acc);
    accs.into_iter().map(|m| m.into_inner().unwrap()).collect()
}

fn new_buckets<T>(capacity: usize) -> Vec<Vec<T>> {
    (0..buckets()).map(|_| Vec::with_capacity(capacity)).collect()
}

/// One copy of an event, as parsed. `ok`: rules 3-4 hold for this copy. For a
/// copy that is ok, its person and category are looked up while parsing,
/// when the line is still in cache; for one that isn't they are left empty.
struct EventCopy {
    h: u64, // hash of event_id
    event_id: Str,
    person: Str,
    hp: u64, // hash of person
    ts: i64,
    ingest: i64,
    price: i32,
    category: u16,
    event_type: u8,
    ok: bool,
}

/// Equal slices: the same bytes at the same place are equal without reading them
/// (every ID of a linked person points at one shared name).
#[inline]
fn same(a: Str, b: Str) -> bool {
    (a.as_ptr() == b.as_ptr() && a.len() == b.len()) || a == b
}

/// What parsing an event needs: the window and the lookups.
struct Lookups<'a> {
    start: i64,
    end: i64,
    links: &'a Links,
    product_category: &'a Map<Str, u16>,
    unknown: u16,
}

/// A surviving event, with its person.
struct Event {
    person: Str,
    hp: u64, // hash of person
    ts: i64,
    price: i32,
    category: u16,
    event_type: u8,
}

/// A valid click, with its person.
struct Click {
    person: Str,
    hp: u64, // hash of person
    ts: i64,
    click_id: Str,
    campaign: u32,
}

/// One user_daily row.
struct UserRow {
    person: Str,
    day: i32,
    sessions: i64,
    events: i64,
    revenue: i64,
    top: Option<u16>,
}

/// Events rules 1 and 3-4 for one well-formed line's fields: None if the event_id is empty.
#[inline]
fn parse_event(fields: [Str; 7], lk: &Lookups) -> Option<EventCopy> {
    let (start, end) = (lk.start, lk.end);
    let [event_id, user_id, ts, ingest, event_type, product_id, price] = fields;
    if event_id.is_empty() {
        return None;
    }
    let ingest = ts_ms(ingest);
    if ingest == BAD {
        return None; // never happens: ingest_ts is always valid F1
    }
    let ts = ts_ms(ts);
    let event_type = match event_type {
        b"view" => VIEW,
        b"cart" => CART,
        b"purchase" => PURCHASE,
        _ => BAD_TYPE, // case-sensitive, untrimmed
    };
    let price = if event_type == PURCHASE { parse::price(price) } else { Some(0) };
    let ok = !user_id.is_empty()
        && ts >= start
        && ts < end
        && event_type != BAD_TYPE
        && !product_id.is_empty()
        && price.is_some()
        && ingest - ts <= DAY; // late: strictly over 24 h (ts is in the window here, so no overflow)
    let (person, hp, category) = if ok {
        let (person, hp) = lk.links.person_of(user_id);
        let category = if event_type == PURCHASE {
            lk.product_category.get(product_id).copied().unwrap_or(lk.unknown)
        } else {
            0
        };
        (person, hp, category)
    } else {
        (&[][..], 0, 0)
    };
    Some(EventCopy {
        h: hash(event_id), event_id, person, hp, ts, ingest, price: price.unwrap_or(0) as i32, category, event_type, ok,
    })
}

/// Each linked ID's person: the name and its hash.
struct Links {
    table: HashTable<(u64, u32)>, // (hash of ID, index into ids)
    ids: Vec<Str>,
    person: Vec<(Str, u64)>,
}

impl Links {
    /// The person an ID belongs to, and the hash of the person's name. An ID in
    /// no link is a person on its own, named by itself.
    #[inline]
    fn person_of(&self, id: Str) -> (Str, u64) {
        let h = hash(id);
        match self.table.find(h, |&(eh, i)| eh == h && self.ids[i as usize] == id) {
            Some(&(_, i)) => self.person[i as usize],
            None => (id, h),
        }
    }
}

/// Linked IDs rules 1-3: for every ID in a kept link line, the person's name
/// (the byte-wise smallest ID joined to it, however many links apart).
fn person_names(links_dir: &Path) -> Links {
    let pairs: Vec<(Str, Str)> = input::dir_chunks(links_dir, CHUNK_BYTES)
        .par_iter()
        .flat_map_iter(|&chunk| {
            let mut pairs = Vec::new();
            input::for_each_record::<2>(chunk, |rec| {
                if let Some([a, b]) = rec {
                    if !a.is_empty() && !b.is_empty() {
                        pairs.push((a, b));
                    }
                }
            });
            pairs
        })
        .collect();
    let mut index: Map<Str, u32> = Map::with_capacity_and_hasher(pairs.len() * 2, hasher());
    let mut ids: Vec<Str> = Vec::new();
    let mut parent: Vec<u32> = Vec::new();
    let mut intern = |id: Str, ids: &mut Vec<Str>, parent: &mut Vec<u32>| -> u32 {
        *index.entry(id).or_insert_with(|| {
            ids.push(id);
            parent.push(parent.len() as u32);
            (ids.len() - 1) as u32
        })
    };
    fn find(parent: &mut [u32], mut x: u32) -> u32 {
        while parent[x as usize] != x {
            let up = parent[parent[x as usize] as usize];
            parent[x as usize] = up; // path halving
            x = up;
        }
        x
    }
    for &(a, b) in &pairs {
        let ia = intern(a, &mut ids, &mut parent);
        let ib = intern(b, &mut ids, &mut parent);
        let (ra, rb) = (find(&mut parent, ia), find(&mut parent, ib));
        if ra != rb {
            parent[ra as usize] = rb;
        }
    }
    let roots: Vec<u32> = (0..ids.len() as u32).map(|i| find(&mut parent, i)).collect();
    let mut name: Vec<Str> = vec![&[]; ids.len()];
    for (i, &r) in roots.iter().enumerate() {
        let r = r as usize;
        if name[r].is_empty() || ids[i] < name[r] {
            name[r] = ids[i]; // byte-wise smallest ID of the person
        }
    }
    let person: Vec<(Str, u64)> = roots.iter().map(|&r| (name[r as usize], hash(name[r as usize]))).collect();
    let mut table = HashTable::with_capacity(ids.len());
    for (i, &id) in ids.iter().enumerate() {
        table.insert_unique(hash(id), (hash(id), i as u32), |e: &(u64, u32)| e.0);
    }
    Links { table, ids, person }
}

fn window(data: &Path) -> (i64, i64) {
    let text = std::fs::read_to_string(data.join("manifest.json")).expect("read manifest.json");
    let value = |key: &str| -> i64 {
        let at = text.find(&format!("\"{key}\"")).unwrap_or_else(|| panic!("manifest has no {key}"));
        let rest = &text[at + key.len() + 2..];
        let open = rest.find('"').expect("manifest value") + 1;
        let close = open + rest[open..].find('"').expect("manifest value");
        let ms = ts_ms(rest[open..close].as_bytes());
        assert!(ms != BAD, "bad {key} in manifest.json");
        ms
    };
    (value("window_start"), value("window_end"))
}

/// Per-thread results of the person pass.
struct Tally {
    users: Vec<UserRow>,
    /// [(campaign or organic) * days + day] -> (sessions, purchasing sessions, revenue)
    credited: Vec<[i64; 3]>,
    /// [campaign * days + day] -> assisted sessions
    assisted: Vec<i64>,
}

impl Tally {
    fn new(campaigns: usize, days: usize) -> Tally {
        Tally { users: Vec::new(), credited: vec![[0; 3]; (campaigns + 1) * days], assisted: vec![0; campaigns * days] }
    }
    fn merge(mut self, other: Tally) -> Tally {
        self.users.extend(other.users);
        for (a, b) in self.credited.iter_mut().zip(&other.credited) {
            for k in 0..3 {
                a[k] += b[k];
            }
        }
        for (a, b) in self.assisted.iter_mut().zip(&other.assisted) {
            *a += b;
        }
        self
    }
}

/// A session of one person.
struct Session {
    start: i64,
    day: usize,
    purchases: i64,
    revenue: i64,
}

/// Everything the person pass needs to know about the inputs.
struct Context {
    base_day: i64,
    days: usize,
    campaigns: usize,
    categories: usize,
    /// Category indices in byte order of their names, for the top_category tie-break.
    category_order: Vec<u16>,
}

/// Events rules 5-7 and clicks rules 4-7 for the people of one bucket.
///
/// People are numbered (exact name comparison), then events and clicks are
/// placed person by person with a counting sort (two linear passes), so the
/// only sorting left is by time within each person.
fn person_bucket(events: &[Vec<Vec<Event>>], clicks: &[Vec<Vec<Click>>], bi: usize, cx: &Context, tally: &mut Tally) {
    let n_events: usize = events.iter().map(|e| e[bi].len()).sum();
    let mut number: HashTable<(u64, u32)> = HashTable::with_capacity(n_events / 4 + 16);
    let mut names: Vec<Str> = Vec::new();
    let mut counts: Vec<u32> = Vec::new();
    let mut ids: Vec<u32> = Vec::with_capacity(n_events);
    for e in events.iter().flat_map(|e| &e[bi]) {
        let id = match number.entry(e.hp, |&(h, i)| h == e.hp && same(names[i as usize], e.person), |x| x.0) {
            hashbrown::hash_table::Entry::Occupied(o) => o.get().1,
            hashbrown::hash_table::Entry::Vacant(v) => {
                v.insert((e.hp, names.len() as u32));
                names.push(e.person);
                counts.push(0);
                (names.len() - 1) as u32
            }
        };
        counts[id as usize] += 1;
        ids.push(id);
    }
    let people = names.len();
    // Counting sort of events by person: (ts, price, category, event_type).
    let mut ev_at = vec![0u32; people + 1];
    for p in 0..people {
        ev_at[p + 1] = ev_at[p] + counts[p];
    }
    let mut fill: Vec<u32> = ev_at[..people].to_vec();
    let mut evs: Vec<(i64, i32, u16, u8)> = vec![(0, 0, 0, 0); n_events];
    for (e, &id) in events.iter().flat_map(|e| &e[bi]).zip(&ids) {
        let at = &mut fill[id as usize];
        evs[*at as usize] = (e.ts, e.price, e.category, e.event_type);
        *at += 1;
    }
    drop(ids);
    // Clicks of people with events, the same way: (ts, click_id, campaign).
    let mut click_ids: Vec<(u32, u32)> = Vec::new(); // (person, index into the flat click list)
    let flat_clicks: Vec<&Click> = clicks.iter().flat_map(|c| &c[bi]).collect();
    let mut ck_count = vec![0u32; people];
    for (k, c) in flat_clicks.iter().enumerate() {
        if let Some(&(_, id)) = number.find(c.hp, |&(h, i)| h == c.hp && same(names[i as usize], c.person)) {
            ck_count[id as usize] += 1;
            click_ids.push((id, k as u32));
        }
    }
    let mut ck_at = vec![0u32; people + 1];
    for p in 0..people {
        ck_at[p + 1] = ck_at[p] + ck_count[p];
    }
    let mut fill: Vec<u32> = ck_at[..people].to_vec();
    let mut cks: Vec<(i64, Str, u32)> = vec![(0, &[][..], 0); click_ids.len()];
    for &(id, k) in &click_ids {
        let c = flat_clicks[k as usize];
        let at = &mut fill[id as usize];
        cks[*at as usize] = (c.ts, c.click_id, c.campaign);
        *at += 1;
    }
    drop(click_ids);

    let organic = cx.campaigns;
    let mut sessions: Vec<Session> = Vec::new();
    let mut runs: Vec<(u32, i64)> = Vec::new();
    let mut starts: Vec<i64> = Vec::new();
    let mut day_end: Vec<usize> = Vec::new();
    let mut spend = vec![0i64; cx.categories];
    for p in 0..people {
        let evs = &mut evs[ev_at[p] as usize..ev_at[p + 1] as usize];
        evs.sort_unstable_by_key(|e| e.0);
        // Rule 5: a new session where the gap from the previous event is over 30 min.
        // Equal timestamps have a gap of 0, so they always share a session.
        sessions.clear();
        let mut prev = i64::MIN;
        let mut row: Option<UserRow> = None;
        for &(ts, price, category, event_type) in evs.iter() {
            let price = price as i64;
            if sessions.is_empty() || ts - prev > SESSION_GAP {
                let day = (ts.div_euclid(DAY) - cx.base_day) as usize; // rule 6: the start's UTC date
                if row.as_ref().is_some_and(|r| r.day as i64 != cx.base_day + day as i64) {
                    tally.users.push(finish_row(row.take().unwrap(), &mut spend, cx));
                }
                let r = row.get_or_insert(UserRow {
                    person: names[p],
                    day: (cx.base_day + day as i64) as i32,
                    sessions: 0,
                    events: 0,
                    revenue: 0,
                    top: None,
                });
                r.sessions += 1;
                sessions.push(Session { start: ts, day, purchases: 0, revenue: 0 });
            }
            prev = ts;
            let r = row.as_mut().unwrap();
            r.events += 1;
            if event_type == PURCHASE {
                r.revenue += price;
                spend[category as usize] += price;
                let s = sessions.last_mut().unwrap();
                s.purchases += 1;
                s.revenue += price;
            }
        }
        tally.users.push(finish_row(row.take().unwrap(), &mut spend, cx));

        let clicks = &mut cks[ck_at[p] as usize..ck_at[p + 1] as usize];
        if clicks.is_empty() {
            for s in &sessions {
                let c = &mut tally.credited[organic * cx.days + s.day];
                c[0] += 1;
                c[1] += (s.purchases > 0) as i64;
                c[2] += s.revenue;
            }
            continue;
        }
        clicks.sort_unstable(); // by (ts, click_id, campaign)

        // Rules 4-5: credit each session to the latest click with ts <= start and
        // start - ts <= 7 days; on a tie in ts, the smallest click_id; else organic.
        let mut k = 0;
        for s in &sessions {
            while k < clicks.len() && clicks[k].0 <= s.start {
                k += 1;
            }
            let mut campaign = organic;
            if k > 0 && s.start - clicks[k - 1].0 <= WEEK {
                let t = clicks[k - 1].0;
                let mut g = k - 1;
                while g > 0 && clicks[g - 1].0 == t {
                    g -= 1; // sorted by click_id within a ts: the first is the smallest
                }
                campaign = clicks[g].2 as usize;
            }
            let c = &mut tally.credited[campaign * cx.days + s.day];
            c[0] += 1;
            c[1] += (s.purchases > 0) as i64;
            c[2] += s.revenue;
        }

        // Rule 7: a campaign assists a session if one of its clicks has
        // start - 7 days <= ts <= start, i.e. the start is in [ts, ts + 7 days].
        // Per campaign, merge those spans into disjoint runs, then count the
        // sessions in each run day by day (each counted once per campaign).
        runs.clear();
        runs.extend(clicks.iter().map(|c| (c.2, c.0)));
        runs.sort_unstable();
        starts.clear();
        starts.extend(sessions.iter().map(|s| s.start));
        day_end.clear();
        day_end.resize(sessions.len(), 0); // first index after this session's day
        for x in (0..sessions.len()).rev() {
            day_end[x] = if x + 1 < sessions.len() && sessions[x + 1].day == sessions[x].day { day_end[x + 1] } else { x + 1 };
        }
        let mut count_run = |campaign: u32, from: i64, last_click: i64| {
            let to = last_click + WEEK;
            let lo = starts.partition_point(|&x| x < from);
            let hi = starts.partition_point(|&x| x <= to);
            let mut x = lo;
            while x < hi {
                let end = day_end[x].min(hi);
                tally.assisted[campaign as usize * cx.days + sessions[x].day] += (end - x) as i64;
                x = end;
            }
        };
        let (mut campaign, mut from, mut last) = (runs[0].0, runs[0].1, runs[0].1);
        for &(c, ts) in &runs[1..] {
            if c == campaign && ts - last <= WEEK {
                last = ts;
            } else {
                count_run(campaign, from, last);
                (campaign, from, last) = (c, ts, ts);
            }
        }
        count_run(campaign, from, last);
    }
}

/// Close a user_daily row: top_category is the highest spend, ties to the byte-wise smallest name.
fn finish_row(mut row: UserRow, spend: &mut [i64], cx: &Context) -> UserRow {
    let mut best = 0;
    for &c in &cx.category_order {
        if spend[c as usize] > best {
            best = spend[c as usize];
            row.top = Some(c);
        }
    }
    spend.iter_mut().for_each(|s| *s = 0);
    row
}

fn main() {
    if let Some(n) = std::env::var("NCPU").ok().and_then(|n| n.parse::<usize>().ok()) {
        rayon::ThreadPoolBuilder::new().num_threads(n).build_global().ok();
    }
    // SHOPPING_REPEAT=N runs the whole pipeline N times: only for profiling.
    let repeat = std::env::var("SHOPPING_REPEAT").ok().and_then(|n| n.parse().ok()).unwrap_or(1);
    for _ in 0..repeat {
        run();
    }
}

fn run() {
    let t0 = Instant::now();
    let log = |what: &str| eprintln!("{:8.3}s  {what}", t0.elapsed().as_secs_f64());
    let data = PathBuf::from(std::env::var("DATA_DIR").unwrap_or_else(|_| "/data".into()));
    let out = PathBuf::from(std::env::var("OUTPUT_DIR").unwrap_or_else(|_| "/output".into()));

    let (start, end) = window(&data);
    let base_day = start.div_euclid(DAY);
    let days = ((end - 1).div_euclid(DAY) - base_day + 1) as usize;

    // Rule 7: categories from products.csv; a product not in it is `unknown`.
    let products = input::small_table::<2>(&data.join("products.csv"));
    let mut category_names: Vec<Str> = Vec::new();
    let mut category_index: Map<Str, u16> = Map::with_hasher(hasher());
    let mut product_category: Map<Str, u16> = Map::with_capacity_and_hasher(products.len(), hasher());
    for [product, category] in &products {
        let c = *category_index.entry(category).or_insert_with(|| {
            category_names.push(category);
            (category_names.len() - 1) as u16
        });
        product_category.entry(product).or_insert(c);
    }
    let unknown = category_names.len() as u16;
    category_names.push(b"unknown");
    let mut category_order: Vec<u16> = (0..category_names.len() as u16).collect();
    category_order.sort_by_key(|&c| category_names[c as usize]);

    let campaign_rows = input::small_table::<2>(&data.join("campaigns.csv"));
    let mut campaign_index: Map<Str, u32> = Map::with_capacity_and_hasher(campaign_rows.len(), hasher());
    let mut campaign_ids: Vec<Str> = Vec::new();
    let mut channels: Vec<Str> = Vec::new();
    for [campaign, channel] in &campaign_rows {
        if let Entry::Vacant(e) = campaign_index.entry(campaign) {
            e.insert(campaign_ids.len() as u32);
            campaign_ids.push(campaign);
            channels.push(channel);
        }
    }
    let campaigns = campaign_ids.len();

    let links = person_names(&data.join("links"));
    log("small tables and linked IDs");

    // Pass 1: parse every event copy, bucketed by event_id.
    let lookups = Lookups { start, end, links: &links, product_category: &product_category, unknown };
    let event_chunks = input::dir_chunks(&data.join("events"), CHUNK_BYTES);
    let event_bytes: usize = event_chunks.iter().map(|c| c.len()).sum();
    // ~16k events per bucket (at ~90 bytes a line), between 256 and 4096 buckets.
    let bits = (event_bytes / 90 / 16_384).max(1).next_power_of_two().trailing_zeros().clamp(8, 12);
    // SHOPPING_BUCKET_BITS overrides it: only for testing other bucket counts.
    let bits = std::env::var("SHOPPING_BUCKET_BITS").ok().and_then(|b| b.parse().ok()).unwrap_or(bits);
    BUCKET_BITS.store(bits, std::sync::atomic::Ordering::Relaxed);
    let (nb, threads) = (buckets(), rayon::current_num_threads());
    // Bucket lists are kept per worker thread, not per chunk, so their number
    // depends on the threads, not on the input size.
    let copies: Vec<Vec<Vec<EventCopy>>> = per_thread(threads, || new_buckets(event_bytes / 80 / nb / threads + 16), |acc| {
        event_chunks.par_iter().for_each(|&chunk| {
            let mut b = acc();
            input::for_each_record::<7>(chunk, |rec| {
                if let Some(c) = rec.and_then(|f| parse_event(f, &lookups)) {
                    b[bucket(c.h)].push(c);
                }
            });
        });
    });
    log("events parsed");

    // Pass 2: dedup before validation (each event_id's earliest copy, valid or
    // not), drop invalid events, re-bucket the survivors by person.
    let events: Vec<Vec<Vec<Event>>> = per_thread(threads, || new_buckets(event_bytes / 90 / nb / threads + 16), |acc| {
        (0..nb).into_par_iter().for_each(|bi| {
            let mut b = acc();
            let n: usize = copies.iter().map(|c| c[bi].len()).sum();
            let mut first: HashTable<(u64, u32)> = HashTable::with_capacity(n); // (hash, index into kept)
            let mut kept: Vec<&EventCopy> = Vec::with_capacity(n);
            for c in copies.iter().flat_map(|c| &c[bi]) {
                match first.entry(c.h, |&(h, k)| h == c.h && same(kept[k as usize].event_id, c.event_id), |e| e.0) {
                    hashbrown::hash_table::Entry::Occupied(e) => {
                        let k = e.get().1 as usize;
                        if c.ingest < kept[k].ingest {
                            kept[k] = c; // copies with equal ingest_ts are identical
                        }
                    }
                    hashbrown::hash_table::Entry::Vacant(e) => {
                        e.insert((c.h, kept.len() as u32));
                        kept.push(c);
                    }
                }
            }
            for c in kept.into_iter().filter(|c| c.ok) {
                b[bucket(c.hp)].push(Event {
                    person: c.person, hp: c.hp, ts: c.ts, price: c.price, category: c.category, event_type: c.event_type,
                });
            }
        });
    });
    // Big tables are never freed: freeing costs time (madvise/munmap) and the process ends soon.
    std::mem::forget(copies);
    log("events deduped and validated");

    // Pass 3: clicks. Copies of a click are identical, so dedup can change
    // neither credit (same ts and click_id) nor assists (each campaign counts
    // once): it is skipped.
    let click_chunks = input::dir_chunks(&data.join("clicks"), CHUNK_BYTES);
    let click_bytes: usize = click_chunks.iter().map(|c| c.len()).sum();
    let clicks: Vec<Vec<Vec<Click>>> = per_thread(threads, || new_buckets(click_bytes / 45 / nb / threads + 16), |acc| {
        click_chunks.par_iter().for_each(|&chunk| {
            let mut b = acc();
            input::for_each_record::<4>(chunk, |rec| {
                let Some([click_id, user_id, campaign, ts]) = rec else { return };
                if click_id.is_empty() || user_id.is_empty() {
                    return;
                }
                let ts = ts_ms(ts);
                if ts < start - WEEK || ts >= end {
                    return; // the click window, a week longer than the event window
                }
                let Some(&campaign) = campaign_index.get(campaign) else { return };
                let (person, hp) = links.person_of(user_id);
                b[bucket(hp)].push(Click { person, hp, ts, click_id, campaign });
            });
        });
    });
    log("clicks parsed");

    // Pass 4: per person bucket.
    let cx = Context { base_day, days, campaigns, categories: category_names.len(), category_order };
    let tally = (0..nb)
        .into_par_iter()
        .fold(
            || Tally::new(campaigns, days),
            |mut tally, bi| {
                person_bucket(&events, &clicks, bi, &cx, &mut tally);
                tally
            },
        )
        .reduce(|| Tally::new(campaigns, days), Tally::merge);
    log("sessions, credit and assists");

    // Output.
    let users = &tally.users;
    let mut cols = (Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new());
    for c in 0..=campaigns {
        for d in 0..days {
            let [sessions, purchasing, revenue] = tally.credited[c * days + d];
            let assisted = if c < campaigns { tally.assisted[c * days + d] } else { 0 };
            if sessions > 0 || assisted > 0 {
                cols.0.push(if c < campaigns { campaign_ids[c] } else { b"organic" });
                cols.1.push(if c < campaigns { channels[c] } else { b"organic" });
                cols.2.push((base_day + d as i64) as i32);
                cols.3.push(sessions);
                cols.4.push(purchasing);
                cols.5.push(revenue);
                cols.6.push(assisted);
            }
        }
    }
    // user_daily is large: one part file per thread, written in parallel.
    let part_rows = users.len().div_ceil(rayon::current_num_threads()).max(1);
    rayon::join(
        || {
            let parts: Vec<&[UserRow]> = if users.is_empty() { vec![&users[..]] } else { users.chunks(part_rows).collect() };
            parts.par_iter().enumerate().for_each(|(part, users)| {
                output::write_table(&out, "user_daily", part, output::USER_DAILY, vec![
                    Column::Str(users.iter().map(|u| u.person).collect()),
                    Column::Date(users.iter().map(|u| u.day).collect()),
                    Column::I64(users.iter().map(|u| u.sessions).collect()),
                    Column::I64(users.iter().map(|u| u.events).collect()),
                    Column::I64(users.iter().map(|u| u.revenue).collect()),
                    Column::OptStr(users.iter().map(|u| u.top.map(|c| category_names[c as usize])).collect()),
                ])
            });
        },
        || {
            output::write_table(&out, "campaign_daily", 0, output::CAMPAIGN_DAILY, vec![
                Column::Str(cols.0), Column::Str(cols.1), Column::Date(cols.2), Column::I64(cols.3),
                Column::I64(cols.4), Column::I64(cols.5), Column::I64(cols.6),
            ])
        },
    );
    log(&format!("wrote {} user_daily rows", users.len()));
    std::mem::forget(tally);
    std::mem::forget(events);
    std::mem::forget(clicks);
}
