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
/// log2 of the number of hash buckets (256), set once at start.
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

/// A second, independent hash (foldhash's other variant, another seed).
#[inline]
fn hash2(key: Str) -> u64 {
    foldhash::quality::FixedState::with_seed(0x9e37_79b9_7f4a_7c15).hash_one(key)
}

/// A person's identity: (hash, hash2) of their name, 128 bits. People are told
/// apart by it without reading their names, which for one-ID people are scattered
/// across the input. Two different names would share it about once in 10^26 runs
/// (the user's decision; see DECISIONS.md).
type PersonKey = (u64, u64);

#[inline]
fn key_of(name: Str) -> PersonKey {
    (hash(name), hash2(name))
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
    key: PersonKey, // the person's 128-bit identity
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
    key: PersonKey, // the person's 128-bit identity
    ts: i64,
    price: i32,
    category: u16,
    event_type: u8,
}

/// A valid click, with its person.
struct Click {
    key: PersonKey, // the person's 128-bit identity
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
    let (person, key, category) = if ok {
        let (person, key) = lk.links.person_of(user_id);
        let category = if event_type == PURCHASE {
            lk.product_category.get(product_id).copied().unwrap_or(lk.unknown)
        } else {
            0
        };
        (person, key, category)
    } else {
        (&[][..], (0, 0), 0)
    };
    Some(EventCopy {
        h: hash(event_id), event_id, person, key, ts, ingest, price: price.unwrap_or(0) as i32, category, event_type, ok,
    })
}

/// One linked ID: its key, and its person's name and key, all in the table
/// entry so a lookup is one memory access.
struct LinkEntry {
    id: PersonKey,
    name: Str,
    person: PersonKey,
}

/// The linked IDs are split into this many parts by key, so building them runs in parallel.
const LINK_PARTS: usize = 64;

#[inline]
fn link_part(key: PersonKey) -> usize {
    (key.1 >> 58) as usize // hash2's top bits: the tables hash by key.0
}

struct Links {
    parts: Vec<HashTable<LinkEntry>>,
    /// A Bloom filter of every linked ID's key: 8+ bits per ID, 2 bits set each.
    /// A clear bit means "in no link" for sure, so most one-ID people skip the
    /// big table (which at 20M is far larger than a Xeon core's cache).
    bloom: Vec<u64>,
    bloom_mask: u64, // number of 512-bit blocks - 1
}

impl Links {
    /// The two bits of a key, both in the same 512-bit block (one cache line),
    /// so a check reads memory once.
    #[inline]
    fn bloom_bits(&self, key: PersonKey) -> (usize, usize) {
        let block = ((key.1 >> 18) & self.bloom_mask) as usize * 512;
        (block + (key.1 & 511) as usize, block + ((key.1 >> 9) & 511) as usize)
    }

    #[inline]
    fn maybe_linked(&self, key: PersonKey) -> bool {
        let (a, b) = self.bloom_bits(key);
        (self.bloom[a / 64] >> (a % 64)) & (self.bloom[b / 64] >> (b % 64)) & 1 == 1
    }

    /// The person an ID belongs to, and the person's key. An ID in no link is a
    /// person on its own, named by itself. IDs are matched by their 128-bit key.
    #[inline]
    fn person_of(&self, id: Str) -> (Str, PersonKey) {
        let key = key_of(id);
        if !self.maybe_linked(key) {
            return (id, key);
        }
        match self.parts[link_part(key)].find(key.0, |e| e.id == key) {
            Some(e) => (e.name, e.person),
            None => (id, key),
        }
    }
}

/// Linked IDs rules 1-3: for every ID in a kept link line, the person's name
/// (the byte-wise smallest ID joined to it, however many links apart).
/// Every step runs in parallel:
///   1. number the distinct IDs, part by part (IDs matched by key);
///   2. join them with a lock-free union-find (a root is only ever linked under
///      a smaller root, so the parent links can never form a loop);
///   3. pick each person's byte-wise smallest ID, with an atomic compare-and-swap;
///   4. build each part's lookup table.
fn person_names(links_dir: &Path) -> Links {
    use std::sync::atomic::{AtomicU32, Ordering::{AcqRel, Acquire, Relaxed}};
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
    let side = |o: u32| -> Str { let (a, b) = pairs[(o / 2) as usize]; if o % 2 == 0 { a } else { b } };

    // 1. Each occurrence o = 2 * pair + side goes to its ID's part; each part numbers its IDs.
    const CHUNK: usize = 4096;
    let threads = rayon::current_num_threads();
    let scattered: Vec<Vec<Vec<(PersonKey, u32)>>> =
        per_thread(threads, || (0..LINK_PARTS).map(|_| Vec::new()).collect::<Vec<Vec<_>>>(), |acc| {
            pairs.par_chunks(CHUNK).enumerate().for_each(|(ci, chunk)| {
                let mut b = acc();
                for (j, &(x, y)) in chunk.iter().enumerate() {
                    let o = ((ci * CHUNK + j) * 2) as u32;
                    for (k, occ) in [(key_of(x), o), (key_of(y), o + 1)] {
                        b[link_part(k)].push((k, occ));
                    }
                }
            });
        });
    // Per part: the distinct keys, the first occurrence of each, and every occurrence's number.
    let numbered: Vec<(Vec<PersonKey>, Vec<u32>, Vec<(u32, u32)>)> = (0..LINK_PARTS)
        .into_par_iter()
        .map(|p| {
            let mut table: HashTable<(PersonKey, u32)> = HashTable::new();
            let (mut keys, mut first, mut numbers) = (Vec::new(), Vec::new(), Vec::new());
            for &(k, o) in scattered.iter().flat_map(|t| &t[p]) {
                let local = match table.entry(k.0, |e| e.0 == k, |e| e.0.0) {
                    hashbrown::hash_table::Entry::Occupied(e) => e.get().1,
                    hashbrown::hash_table::Entry::Vacant(v) => {
                        v.insert((k, keys.len() as u32));
                        keys.push(k);
                        first.push(o);
                        (keys.len() - 1) as u32
                    }
                };
                numbers.push((o, local));
            }
            (keys, first, numbers)
        })
        .collect();
    let mut offset = vec![0u32; LINK_PARTS + 1];
    for p in 0..LINK_PARTS {
        offset[p + 1] = offset[p] + numbered[p].0.len() as u32;
    }
    let n = offset[LINK_PARTS] as usize;
    let number: Vec<AtomicU32> = (0..pairs.len() * 2).map(|_| AtomicU32::new(0)).collect();
    numbered.par_iter().enumerate().for_each(|(p, (_, _, numbers))| {
        for &(o, local) in numbers {
            number[o as usize].store(offset[p] + local, Relaxed);
        }
    });
    let ids: Vec<Str> = numbered
        .par_iter()
        .map(|(_, first, _)| first.iter().map(|&o| side(o)).collect::<Vec<Str>>())
        .collect::<Vec<_>>()
        .concat();

    // 2. Union-find.
    let parent: Vec<AtomicU32> = (0..n as u32).map(AtomicU32::new).collect();
    fn find(parent: &[AtomicU32], mut x: u32) -> u32 {
        loop {
            let p = parent[x as usize].load(Acquire);
            if p == x {
                return x;
            }
            let g = parent[p as usize].load(Acquire);
            if g != p {
                let _ = parent[x as usize].compare_exchange_weak(p, g, AcqRel, Acquire); // path halving
            }
            x = p;
        }
    }
    (0..pairs.len()).into_par_iter().for_each(|i| {
        let (a, b) = (number[2 * i].load(Relaxed), number[2 * i + 1].load(Relaxed));
        loop {
            let (ra, rb) = (find(&parent, a), find(&parent, b));
            if ra == rb {
                break;
            }
            let (hi, lo) = if ra > rb { (ra, rb) } else { (rb, ra) };
            if parent[hi as usize].compare_exchange(hi, lo, AcqRel, Acquire).is_ok() {
                break;
            }
        }
    });
    let roots: Vec<u32> = (0..n as u32).into_par_iter().map(|i| find(&parent, i)).collect();

    // 3. Each person's byte-wise smallest ID (distinct IDs never have equal bytes).
    const NONE: u32 = u32::MAX;
    let best: Vec<AtomicU32> = (0..n).map(|_| AtomicU32::new(NONE)).collect();
    (0..n as u32).into_par_iter().for_each(|i| {
        let slot = &best[roots[i as usize] as usize];
        let mut cur = slot.load(Acquire);
        while cur == NONE || ids[i as usize] < ids[cur as usize] {
            match slot.compare_exchange_weak(cur, i, AcqRel, Acquire) {
                Ok(_) => break,
                Err(now) => cur = now,
            }
        }
    });

    // 4. The lookup tables.
    let parts: Vec<HashTable<LinkEntry>> = (0..LINK_PARTS)
        .into_par_iter()
        .map(|p| {
            let keys = &numbered[p].0;
            let mut table = HashTable::with_capacity(keys.len());
            for (local, &k) in keys.iter().enumerate() {
                let g = offset[p] as usize + local;
                let name = ids[best[roots[g] as usize].load(Relaxed) as usize];
                table.insert_unique(k.0, LinkEntry { id: k, name, person: key_of(name) }, |e: &LinkEntry| e.id.0);
            }
            table
        })
        .collect();
    // Bloom filter: at least 8 bits per ID (a power of two), at least 2^16 bits.
    let bits = (n * 8).max(1 << 16).next_power_of_two();
    let mut links = Links { parts, bloom: vec![0u64; bits / 64], bloom_mask: (bits / 512 - 1) as u64 };
    for p in 0..LINK_PARTS {
        for &k in &numbered[p].0 {
            let (a, b) = links.bloom_bits(k);
            links.bloom[a / 64] |= 1 << (a % 64);
            links.bloom[b / 64] |= 1 << (b % 64);
        }
    }
    links
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
    /// user_daily rows with a top_category, and without (kept apart so each
    /// Parquet part's null markers are one run, cheap to encode).
    users: Vec<UserRow>,
    users_no_top: Vec<UserRow>,
    /// [(campaign or organic) * days + day] -> (sessions, purchasing sessions, revenue)
    credited: Vec<[i64; 3]>,
    /// [campaign * days + day] -> assisted sessions
    assisted: Vec<i64>,
}

impl Tally {
    fn new(campaigns: usize, days: usize) -> Tally {
        Tally {
            users: Vec::new(),
            users_no_top: Vec::new(),
            credited: vec![[0; 3]; (campaigns + 1) * days],
            assisted: vec![0; campaigns * days],
        }
    }
    fn push_user(&mut self, row: UserRow) {
        if row.top.is_some() { self.users.push(row) } else { self.users_no_top.push(row) }
    }
    /// Add another thread's campaign counts to these (its user rows stay where they are).
    fn add_counts(&mut self, other: &Tally) {
        for (a, b) in self.credited.iter_mut().zip(&other.credited) {
            for k in 0..3 {
                a[k] += b[k];
            }
        }
        for (a, b) in self.assisted.iter_mut().zip(&other.assisted) {
            *a += b;
        }
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
/// People are numbered by their 128-bit key (no name is read), then events and clicks are
/// placed person by person with a counting sort (two linear passes), so the
/// only sorting left is by time within each person.
fn person_bucket(events: &[Vec<Vec<Event>>], clicks: &[Vec<Vec<Click>>], bi: usize, cx: &Context, tally: &mut Tally) {
    let n_events: usize = events.iter().map(|e| e[bi].len()).sum();
    let mut number: HashTable<(PersonKey, u32)> = HashTable::with_capacity(n_events / 4 + 16);
    let mut names: Vec<Str> = Vec::new();
    let mut counts: Vec<u32> = Vec::new();
    let mut ids: Vec<u32> = Vec::with_capacity(n_events);
    for e in events.iter().flat_map(|e| &e[bi]) {
        let id = match number.entry(e.key.0, |&(k, _)| k == e.key, |x| x.0.0) {
            hashbrown::hash_table::Entry::Occupied(o) => o.get().1,
            hashbrown::hash_table::Entry::Vacant(v) => {
                v.insert((e.key, names.len() as u32));
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
        if let Some(&(_, id)) = number.find(c.key.0, |&(k, _)| k == c.key) {
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
                    tally.push_user(finish_row(row.take().unwrap(), &mut spend, cx));
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
        tally.push_user(finish_row(row.take().unwrap(), &mut spend, cx));

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
    // 256 buckets. Scaling them with the input (2048 at 20M, to keep each dedup
    // table in L2) made attempt #6 slower: the wider scatter while parsing cost
    // more. SHOPPING_BUCKET_BITS overrides it, only for testing other counts.
    let bits = std::env::var("SHOPPING_BUCKET_BITS").ok().and_then(|b| b.parse().ok()).unwrap_or(8);
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
                b[bucket(c.key.0)].push(Event {
                    person: c.person, key: c.key, ts: c.ts, price: c.price, category: c.category, event_type: c.event_type,
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
                let (_, key) = links.person_of(user_id);
                b[bucket(key.0)].push(Click { key, ts, click_id, campaign });
            });
        });
    });
    log("clicks parsed");

    // Pass 4: per person bucket.
    let cx = Context { base_day, days, campaigns, categories: category_names.len(), category_order };
    // One tally per worker thread (rayon's fold made one per work split, and
    // merging dozens of them cost ~3%); only the campaign counts are merged.
    let tallies: Vec<Tally> = per_thread(threads, || Tally::new(campaigns, days), |acc| {
        (0..nb).into_par_iter().for_each(|bi| person_bucket(&events, &clicks, bi, &cx, &mut acc()));
    });
    let mut tally = Tally::new(campaigns, days);
    for t in &tallies {
        tally.add_counts(t);
    }
    log("sessions, credit and assists");

    // Output.
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
    // user_daily is large: each thread's rows with and without a top_category
    // are separate part files, written in parallel.
    let mut parts: Vec<&[UserRow]> =
        tallies.iter().flat_map(|t| [&t.users[..], &t.users_no_top[..]]).filter(|p| !p.is_empty()).collect();
    if parts.is_empty() {
        parts.push(&[]); // still write one (empty) file with the schema
    }
    let user_rows: usize = parts.iter().map(|p| p.len()).sum();
    rayon::join(
        || {
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
    log(&format!("wrote {user_rows} user_daily rows"));
    std::mem::forget(tallies);
    std::mem::forget(events);
    std::mem::forget(clicks);
}
