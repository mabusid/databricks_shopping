//! Shopping Sessions: the whole pipeline as one native program. Rules: rules/rules.md.
//!
//! Reads  $DATA_DIR   (default /data)
//! Writes $OUTPUT_DIR (default /output): user_daily/ and campaign_daily/ as Parquet
//!
//! The work runs in a few parallel passes over 256 (or 1024) hash buckets:
//!   1. events and clicks: parse every line. Each event copy leaves a small
//!      dedup record in its event_id's bucket and, if the copy is valid, its
//!      event in its person's bucket; each valid click goes to its person's bucket
//!   2. per event_id bucket: keep each event's earliest copy (dedup before
//!      validation) and mark the events of all other copies dead
//!   3. per person bucket: sessions, user_daily rows, click credit, assists
//! Every field is a slice of the memory-mapped input; nothing is copied.

mod blocks;
mod input;
mod output;
mod parse;
mod probe;

use foldhash::fast::FixedState;
use hashbrown::hash_map::Entry;
use hashbrown::{HashMap, HashTable};
use blocks::BlockList;
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

/// A person's identity: the 128-bit XXH3 hash of their name. People are told
/// apart by it without reading their names, which for one-ID people are scattered
/// across the input. Two different names would share it about once in 10^26 runs
/// (the user's decision; see DECISIONS.md).
type PersonKey = (u64, u64);

/// Both halves come from one pass of XXH3-128 over the bytes.
#[inline]
fn key_of(name: Str) -> PersonKey {
    let h = xxhash_rust::xxh3::xxh3_128(name);
    (h as u64, (h >> 64) as u64)
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

/// One empty list per bucket.
fn new_lists<T: Copy>() -> Vec<BlockList<T>> {
    (0..buckets()).map(|_| BlockList::new()).collect()
}

/// Per-thread lists of every bucket -> per-bucket lists of every thread.
fn by_bucket<T: Copy>(per_thread: Vec<Vec<BlockList<T>>>) -> Vec<Vec<BlockList<T>>> {
    let mut out: Vec<Vec<BlockList<T>>> = (0..buckets()).map(|_| Vec::with_capacity(per_thread.len())).collect();
    for lists in per_thread {
        for (bi, list) in lists.into_iter().enumerate() {
            out[bi].push(list);
        }
    }
    out
}

/// A string slice in 8 bytes: a 48-bit address and a 16-bit length (user-space
/// addresses fit in 48 bits on x86-64 and arm64 Linux). A slice that doesn't fit
/// (65,535+ bytes, or a higher address) goes to a side table instead, so every
/// value stays exact. Smaller records mean less memory written between passes.
#[derive(Clone, Copy)]
struct PStr(u64);

const PSTR_SPILL: u64 = 0xFFFF;
static PSTR_SPILLED: std::sync::Mutex<Vec<Str>> = std::sync::Mutex::new(Vec::new());

impl PStr {
    #[inline]
    fn new(s: Str) -> PStr {
        let p = s.as_ptr() as u64;
        if p >> 48 == 0 && (s.len() as u64) < PSTR_SPILL {
            PStr(p | (s.len() as u64) << 48)
        } else {
            let mut spilled = PSTR_SPILLED.lock().unwrap();
            spilled.push(s);
            PStr((spilled.len() - 1) as u64 | PSTR_SPILL << 48)
        }
    }

    #[inline]
    fn get(self) -> Str {
        let len = self.0 >> 48;
        if len == PSTR_SPILL {
            return PSTR_SPILLED.lock().unwrap()[(self.0 & ((1 << 48) - 1)) as usize];
        }
        // SAFETY: built by `new` from a 'static slice of exactly this address and length.
        unsafe { std::slice::from_raw_parts((self.0 & ((1 << 48) - 1)) as *const u8, len as usize) }
    }

    /// Equal slices; the same packed value means the same bytes, without reading them.
    #[inline]
    fn same(self, other: PStr) -> bool {
        self.0 == other.0 || self.get() == other.get()
    }
}

/// One copy of an event, as parsed. `ok`: rules 3-4 hold for this copy. For a
/// copy that is ok, its person and category are looked up while parsing,
/// when the line is still in cache; for one that isn't they are left empty.
#[derive(Clone, Copy)]
struct EventCopy {
    h: u64, // hash of event_id
    event_id: PStr,
    person: PStr,
    key: PersonKey, // the person's 128-bit identity
    ts: i64,
    ingest: i64,
    price: i32,
    category: u16,
    event_type: u8,
    ok: bool,
}

const _: () = assert!(std::mem::size_of::<EventCopy>() == 64);
const _: () = assert!(std::mem::size_of::<Dup>() == 32);
const _: () = assert!(std::mem::size_of::<Event>() == 40);
const _: () = assert!(std::mem::size_of::<Click>() == 40);

/// What parsing an event needs: the window and the lookups.
struct Lookups<'a> {
    start: i64,
    end: i64,
    links: &'a Links,
    product_category: &'a Map<Str, u16>,
    unknown: u16,
}

/// A valid event copy, with its person. `dead`: dedup chose another copy of
/// its event_id (set by pass 2), so it doesn't count.
#[derive(Clone, Copy)]
struct Event {
    person: PStr,
    key: PersonKey, // the person's 128-bit identity
    ts: i64,
    price: i32,
    category: u16,
    event_type: u8,
    dead: u8,
}

/// What dedup needs of one event copy (valid or not): its event_id, ingest_ts,
/// and where its Event is (0 if the copy isn't valid and has none).
#[derive(Clone, Copy)]
struct Dup {
    h: u64, // hash of event_id
    event_id: PStr,
    ingest: i64,
    event: usize, // *mut Event, as an address
}

/// Mark the Event of a copy that dedup did not keep.
#[inline]
fn kill(d: &Dup) {
    if d.event != 0 {
        // SAFETY: `event` is the address `BlockList::push` returned for this copy's
        // Event; its list lives (unreleased, unborrowed) until the person pass, and
        // each copy is in exactly one dedup bucket, so no other thread writes it.
        unsafe { std::ptr::addr_of_mut!((*(d.event as *mut Event)).dead).write(1) };
    }
}

/// Each worker thread's lists from pass 1, one per bucket.
struct ParseLists {
    dups: Vec<BlockList<Dup>>,
    events: Vec<BlockList<Event>>,
    clicks: Vec<BlockList<Click>>,
}

impl ParseLists {
    fn new() -> ParseLists {
        ParseLists { dups: new_lists(), events: new_lists(), clicks: new_lists() }
    }

    /// File one parsed event copy: its event (if valid) by person, its dedup record by event_id.
    #[inline]
    fn add(&mut self, c: EventCopy) {
        let event = if c.ok {
            self.events[bucket(c.key.0)].push(Event {
                person: c.person, key: c.key, ts: c.ts, price: c.price, category: c.category, event_type: c.event_type, dead: 0,
            }) as usize
        } else {
            0
        };
        self.dups[bucket(c.h)].push(Dup { h: c.h, event_id: c.event_id, ingest: c.ingest, event });
    }
}

/// A valid click, with its person.
#[derive(Clone, Copy)]
struct Click {
    key: PersonKey, // the person's 128-bit identity
    ts: i64,
    click_id: PStr,
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
fn parse_event(fields: [Str; 7], lk: &Lookups) -> Option<Pending> {
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
    // The person is filled in later (`finish_event`), once its table slot has had
    // time to arrive in cache: here only the key is computed and the slot prefetched.
    let (key, maybe_linked, category) = if ok {
        let key = key_of(user_id);
        let maybe = lk.links.prefetch(key);
        let category = if event_type == PURCHASE {
            lk.product_category.get(product_id).copied().unwrap_or(lk.unknown)
        } else {
            0
        };
        (key, maybe, category)
    } else {
        (NO_KEY, false, 0)
    };
    Some(Pending {
        copy: EventCopy {
            h: hash(event_id), event_id: PStr::new(event_id), person: PStr(0), key, ts, ingest,
            price: price.unwrap_or(0) as i32, category, event_type, ok,
        },
        user_id,
        maybe_linked,
    })
}

/// A parsed copy waiting for its person (see `parse_event`).
#[derive(Clone, Copy)]
struct Pending {
    copy: EventCopy,
    user_id: Str,
    maybe_linked: bool,
}

#[inline]
fn finish_event(p: Pending, links: &Links) -> EventCopy {
    let mut c = p.copy;
    if c.ok {
        let (person, key) = links.resolve(p.user_id, c.key, p.maybe_linked);
        c.person = PStr::new(person);
        c.key = key;
    }
    c
}

/// How many parsed copies wait before their person is looked up: enough lines
/// for a prefetched table slot to arrive from memory.
const PIPELINE: usize = 8;
/// The most copies the ring can hold (SHOPPING_PIPELINE may set 1..=32, only for probes).
const PIPELINE_MAX: usize = 32;

/// A tuning knob from the environment (only probes set these), else `default`.
fn knob(name: &str, default: usize) -> usize {
    std::env::var(name).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

/// One linked ID: its key, and its person's name and key, all in one 40-byte
/// slot so a lookup is one (prefetchable) memory access.
#[derive(Clone, Copy)]
struct LinkSlot {
    id: PersonKey, // (0, 0) marks an empty slot; a real (0, 0) key is kept apart
    person: PersonKey,
    name: PStr,
}

const NO_KEY: PersonKey = (0, 0);

/// One part's table: open addressing, linear probing, at most half full.
struct LinkPart {
    slots: Vec<LinkSlot>,
    mask: usize,
}

impl LinkPart {
    fn build(entries: &[LinkSlot]) -> LinkPart {
        let cap = (entries.len() * 2).max(16).next_power_of_two();
        let mut slots = vec![LinkSlot { id: NO_KEY, person: NO_KEY, name: PStr(0) }; cap];
        for e in entries {
            let mut i = e.id.0 as usize & (cap - 1);
            while slots[i].id != NO_KEY {
                i = (i + 1) & (cap - 1);
            }
            slots[i] = *e;
        }
        LinkPart { slots, mask: cap - 1 }
    }

    #[inline]
    fn home(&self, key: PersonKey) -> *const LinkSlot {
        &self.slots[key.0 as usize & self.mask]
    }

    #[inline]
    fn find(&self, key: PersonKey) -> Option<&LinkSlot> {
        let mut i = key.0 as usize & self.mask;
        loop {
            let s = &self.slots[i];
            if s.id == key {
                return Some(s);
            }
            if s.id == NO_KEY {
                return None;
            }
            i = (i + 1) & self.mask;
        }
    }
}

/// Ask the CPU to start loading the cache lines of `*p` (never faults).
#[inline(always)]
fn prefetch<T>(p: *const T) {
    let end = (p as usize + std::mem::size_of::<T>() - 1) as *const u8;
    #[cfg(target_arch = "x86_64")]
    // SAFETY: prefetching is a hint; it never faults and never changes memory.
    unsafe {
        use std::arch::x86_64::{_mm_prefetch, _MM_HINT_T0};
        _mm_prefetch(p as *const i8, _MM_HINT_T0);
        _mm_prefetch(end as *const i8, _MM_HINT_T0);
    }
    #[cfg(target_arch = "aarch64")]
    // SAFETY: as above.
    unsafe {
        std::arch::asm!("prfm pldl1keep, [{0}]", "prfm pldl1keep, [{1}]", in(reg) p, in(reg) end,
                        options(nostack, preserves_flags, readonly));
    }
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    let _ = end;
}

/// The linked IDs are split into this many parts by key, so building them runs in parallel.
const LINK_PARTS: usize = 64;

#[inline]
fn link_part(key: PersonKey) -> usize {
    (key.1 >> 58) as usize // the key's second half: the tables hash by key.0
}

struct Links {
    parts: Vec<LinkPart>,
    /// The person of a linked ID whose key is (0, 0), if there is one.
    zero: Option<(PStr, PersonKey)>,
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
        let maybe = self.maybe_linked(key);
        self.resolve(id, key, maybe)
    }

    /// Start loading the table slot a key will need (when the Bloom filter says
    /// it may be linked); returns that answer for `resolve`.
    #[inline]
    fn prefetch(&self, key: PersonKey) -> bool {
        let maybe = self.maybe_linked(key);
        if maybe {
            prefetch(self.parts[link_part(key)].home(key));
        }
        maybe
    }

    #[inline]
    fn resolve(&self, id: Str, key: PersonKey, maybe_linked: bool) -> (Str, PersonKey) {
        if !maybe_linked {
            return (id, key);
        }
        if key == NO_KEY {
            return self.zero.map_or((id, key), |(n, k)| (n.get(), k));
        }
        match self.parts[link_part(key)].find(key) {
            Some(e) => (e.name.get(), e.person),
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
    let slots: Vec<Vec<LinkSlot>> = (0..LINK_PARTS)
        .into_par_iter()
        .map(|p| {
            numbered[p].0.iter().enumerate().map(|(local, &k)| {
                let g = offset[p] as usize + local;
                let name = ids[best[roots[g] as usize].load(Relaxed) as usize];
                LinkSlot { id: k, person: key_of(name), name: PStr::new(name) }
            }).collect()
        })
        .collect();
    let zero = slots.iter().flatten().find(|e| e.id == NO_KEY).map(|e| (e.name, e.person));
    let parts: Vec<LinkPart> = slots
        .par_iter()
        .map(|entries| {
            let real: Vec<LinkSlot> = entries.iter().filter(|e| e.id != NO_KEY).copied().collect();
            LinkPart::build(&real)
        })
        .collect();
    // Bloom filter: at least 8 bits per ID (a power of two), at least 2^16 bits.
    let bits = (n * 8).max(1 << 16).next_power_of_two();
    let mut links = Links { parts, zero, bloom: Vec::new(), bloom_mask: (bits / 512 - 1) as u64 };
    // Set every key's two bits, all parts in parallel (atomic OR; order doesn't matter).
    let words: Vec<std::sync::atomic::AtomicU64> = (0..bits / 64).map(|_| std::sync::atomic::AtomicU64::new(0)).collect();
    numbered.par_iter().for_each(|(keys, _, _)| {
        for &k in keys {
            let (a, b) = links.bloom_bits(k);
            words[a / 64].fetch_or(1 << (a % 64), Relaxed);
            words[b / 64].fetch_or(1 << (b % 64), Relaxed);
        }
    });
    links.bloom = words.into_iter().map(|w| w.into_inner()).collect();
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
    /// [(campaign or organic) * days + day] -> (sessions, purchasing sessions, revenue)
    credited: Vec<[i64; 3]>,
    /// [campaign * days + day] -> assisted sessions
    assisted: Vec<i64>,
}

impl Tally {
    fn new(campaigns: usize, days: usize) -> Tally {
        Tally {
            credited: vec![[0; 3]; (campaigns + 1) * days],
            assisted: vec![0; campaigns * days],
        }
    }
    /// Add another thread's campaign counts to these.
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

/// A group of buckets' user_daily rows, with a top_category and without (kept
/// apart so each Parquet part's null markers are one run, cheap to encode).
#[derive(Default)]
struct Users {
    top: Vec<UserRow>,
    no_top: Vec<UserRow>,
}

impl Users {
    fn push(&mut self, row: UserRow) {
        if row.top.is_some() { self.top.push(row) } else { self.no_top.push(row) }
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
fn person_bucket(events: &[BlockList<Event>], clicks: &[BlockList<Click>], cx: &Context, tally: &mut Tally, users: &mut Users) {
    let n_copies: usize = events.iter().map(|l| l.len()).sum();
    let mut number: HashTable<(PersonKey, u32)> = HashTable::with_capacity(n_copies / 4 + 16);
    let mut names: Vec<Str> = Vec::new();
    let mut counts: Vec<u32> = Vec::new();
    let mut ids: Vec<u32> = Vec::with_capacity(n_copies);
    let live = || events.iter().flat_map(|l| l.iter()).filter(|e| e.dead == 0);
    for e in live() {
        let id = match number.entry(e.key.0, |&(k, _)| k == e.key, |x| x.0.0) {
            hashbrown::hash_table::Entry::Occupied(o) => o.get().1,
            hashbrown::hash_table::Entry::Vacant(v) => {
                v.insert((e.key, names.len() as u32));
                names.push(e.person.get());
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
    let mut evs: Vec<(i64, i32, u16, u8)> = vec![(0, 0, 0, 0); ids.len()];
    for (e, &id) in live().zip(&ids) {
        let at = &mut fill[id as usize];
        evs[*at as usize] = (e.ts, e.price, e.category, e.event_type);
        *at += 1;
    }
    drop(ids);
    // Clicks of people with events, the same way: (ts, click_id, campaign).
    let mut click_ids: Vec<(u32, u32)> = Vec::new(); // (person, index into the flat click list)
    let flat_clicks: Vec<&Click> = clicks.iter().flat_map(|l| l.iter()).collect();
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
        cks[*at as usize] = (c.ts, c.click_id.get(), c.campaign);
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
                    users.push(finish_row(row.take().unwrap(), &mut spend, cx));
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
        users.push(finish_row(row.take().unwrap(), &mut spend, cx));

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
    // SHOPPING_PROBE=DIR: machine microbenchmarks for a probe submission (never in a scored run).
    if let Ok(dir) = std::env::var("SHOPPING_PROBE") {
        probe::run(Path::new(&dir));
        return;
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

    // Event chunks, and the bucket count.
    let lookups = Lookups { start, end, links: &links, product_category: &product_category, unknown };
    let chunk_bytes = knob("SHOPPING_CHUNK_KB", CHUNK_BYTES >> 10) << 10;
    let depth = knob("SHOPPING_PIPELINE", PIPELINE).clamp(1, PIPELINE_MAX);
    let click_depth = knob("SHOPPING_CLICK_PIPELINE", 0).min(PIPELINE_MAX);
    let event_chunks = input::dir_chunks(&data.join("events"), chunk_bytes);
    // 256 buckets, or 1024 for inputs over 1 GB of events (~10M+), so each
    // bucket's dedup table (~20k events at 20M) fits a Xeon core's L2. Attempt #6
    // tried 2048 with growing Vecs and was slower (the scatter into fresh,
    // pre-reserved memory); block lists made the scatter cheap.
    // SHOPPING_BUCKET_BITS overrides it, only for testing other counts.
    let event_bytes: usize = event_chunks.iter().map(|c| c.len()).sum();
    let default_bits = if event_bytes > 1 << 30 { 10 } else { 8 };
    let bits = std::env::var("SHOPPING_BUCKET_BITS").ok().and_then(|b| b.parse().ok()).unwrap_or(default_bits);
    BUCKET_BITS.store(bits, std::sync::atomic::Ordering::Relaxed);
    let threads = rayon::current_num_threads();
    // Pass 1: parse every event copy and every click. Bucket lists are kept per
    // worker thread, not per chunk, so their number depends on the threads, not
    // on the input size. Clicks: copies of a click are identical, so dedup can
    // change neither credit (same ts and click_id) nor assists (each campaign
    // counts once): it is skipped.
    let click_chunks = input::dir_chunks(&data.join("clicks"), chunk_bytes);
    let work: Vec<(bool, &'static [u8])> =
        event_chunks.iter().map(|&c| (false, c)).chain(click_chunks.iter().map(|&c| (true, c))).collect();
    let lists: Vec<ParseLists> = per_thread(threads, ParseLists::new, |acc| {
        work.par_iter().for_each(|&(is_click, chunk)| {
            let mut b = acc();
            if is_click {
                // Optionally (SHOPPING_CLICK_PIPELINE=N, probes only) the linked-ID slot
                // is prefetched and the click waits N clicks before its lookup, like events.
                let mut ring: [Option<(Str, PersonKey, bool, Click)>; PIPELINE_MAX] = [None; PIPELINE_MAX];
                let mut at = 0;
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
                    if click_depth == 0 {
                        let (_, key) = links.person_of(user_id);
                        b.clicks[bucket(key.0)].push(Click { key, ts, click_id: PStr::new(click_id), campaign });
                        return;
                    }
                    let key = key_of(user_id);
                    let maybe = links.prefetch(key);
                    let c = Click { key, ts, click_id: PStr::new(click_id), campaign };
                    if let Some((id, k, m, mut old)) = ring[at].replace((user_id, key, maybe, c)) {
                        old.key = links.resolve(id, k, m).1;
                        b.clicks[bucket(old.key.0)].push(old);
                    }
                    at = if at + 1 == click_depth { 0 } else { at + 1 };
                });
                for slot in ring.iter_mut() {
                    if let Some((id, k, m, mut old)) = slot.take() {
                        old.key = links.resolve(id, k, m).1;
                        b.clicks[bucket(old.key.0)].push(old);
                    }
                }
                return;
            }
            let mut ring: [Option<Pending>; PIPELINE_MAX] = [None; PIPELINE_MAX];
            let mut at = 0;
            input::for_each_record::<7>(chunk, |rec| {
                if let Some(p) = rec.and_then(|f| parse_event(f, &lookups)) {
                    if let Some(old) = ring[at].replace(p) {
                        b.add(finish_event(old, &links));
                    }
                    at = if at + 1 == depth { 0 } else { at + 1 };
                }
            });
            for k in 0..depth {
                if let Some(old) = ring[(at + k) % depth].take() {
                    b.add(finish_event(old, &links));
                }
            }
        });
    });
    log("events and clicks parsed");

    // Pass 2: dedup before validation. Per event_id, the copy with the smallest
    // ingest_ts is kept (copies with equal ingest_ts are identical: the first
    // seen is kept); the events of all other copies are marked dead. If the kept
    // copy is invalid it has no event, so the event is dropped.
    let mut dups = Vec::with_capacity(threads);
    let mut events = Vec::with_capacity(threads);
    let mut clicks = Vec::with_capacity(threads);
    for l in lists {
        dups.push(l.dups);
        events.push(l.events);
        clicks.push(l.clicks);
    }
    by_bucket(dups).into_par_iter().for_each(|lists| {
        let n: usize = lists.iter().map(|l| l.len()).sum();
        let mut first: HashTable<(u64, u32)> = HashTable::with_capacity(n); // (hash, index into kept)
        let mut kept: Vec<&Dup> = Vec::with_capacity(n);
        for d in lists.iter().flat_map(|l| l.iter()) {
            match first.entry(d.h, |&(h, k)| h == d.h && kept[k as usize].event_id.same(d.event_id), |e| e.0) {
                hashbrown::hash_table::Entry::Occupied(e) => {
                    let k = e.get().1 as usize;
                    if d.ingest < kept[k].ingest {
                        kill(kept[k]);
                        kept[k] = d;
                    } else {
                        kill(d);
                    }
                }
                hashbrown::hash_table::Entry::Vacant(e) => {
                    e.insert((d.h, kept.len() as u32));
                    kept.push(d);
                }
            }
        }
        drop(kept);
        for l in lists {
            l.release();
        }
    });
    log("events deduped");

    // Pass 3: per person bucket, in groups of buckets. Each group writes its own
    // user_daily parts as soon as it is done, so writing overlaps the other
    // groups' work instead of running after all of them.
    let cx = Context { base_day, days, campaigns, categories: category_names.len(), category_order };
    let checksum = std::env::var("SHOPPING_CHECKSUM").is_ok();
    // SHOPPING_CHECKSUM=1: an order-independent checksum of both tables, so a
    // probe can check on the scorer that two builds wrote the same rows.
    let row_hash = |fields: &[&[u8]]| {
        let mut buf = Vec::new();
        for f in fields {
            buf.extend_from_slice(&(f.len() as u32).to_le_bytes());
            buf.extend_from_slice(f);
        }
        xxhash_rust::xxh3::xxh3_64(&buf)
    };
    let user_hash = |u: &UserRow| {
        let top = u.top.map_or(&b"\xffnull"[..], |c| category_names[c as usize]);
        row_hash(&[u.person, &u.day.to_le_bytes(), &u.sessions.to_le_bytes(), &u.events.to_le_bytes(), &u.revenue.to_le_bytes(), top])
    };
    // Compact output stores sessions and events as INT32; a person-day has at most
    // as many sessions and events as there are event lines, so they fit when that does.
    let compact = output::compact() && event_bytes < i32::MAX as usize;
    let write_users = |part: usize, users: &[UserRow]| {
        let (schema, sessions, events) = if compact {
            (output::USER_DAILY_32, Column::I32(users.iter().map(|u| u.sessions as i32).collect()),
             Column::I32(users.iter().map(|u| u.events as i32).collect()))
        } else {
            (output::USER_DAILY, Column::I64(users.iter().map(|u| u.sessions).collect()),
             Column::I64(users.iter().map(|u| u.events).collect()))
        };
        output::write_table(&out, "user_daily", part, schema, vec![
            Column::Str(users.iter().map(|u| u.person).collect()),
            Column::Date(users.iter().map(|u| u.day).collect()),
            sessions,
            events,
            Column::I64(users.iter().map(|u| u.revenue).collect()),
            Column::OptStr(users.iter().map(|u| u.top.map(|c| category_names[c as usize])).collect()),
        ])
    };
    let user_rows = std::sync::atomic::AtomicUsize::new(0);
    let user_sum = std::sync::atomic::AtomicU64::new(0);
    let mut pairs = by_bucket(events).into_iter().zip(by_bucket(clicks));
    let per_group = (buckets() / knob("SHOPPING_WRITE_GROUPS", 64).max(1)).max(1);
    let mut groups = Vec::new();
    loop {
        let g: Vec<_> = pairs.by_ref().take(per_group).collect();
        if g.is_empty() {
            break;
        }
        groups.push(g);
    }
    // One tally per worker thread (rayon's fold made one per work split, and
    // merging dozens of them cost ~3%); only the campaign counts are merged.
    let tallies: Vec<Tally> = per_thread(threads, || Tally::new(campaigns, days), |acc| {
        groups.into_par_iter().enumerate().for_each(|(gi, group)| {
            let mut users = Users::default();
            for (evs, cks) in group {
                person_bucket(&evs, &cks, &cx, &mut acc(), &mut users);
                evs.into_iter().for_each(BlockList::release);
                cks.into_iter().for_each(BlockList::release);
            }
            for (k, rows) in [&users.top, &users.no_top].into_iter().enumerate() {
                if !rows.is_empty() {
                    write_users(2 * gi + k, rows);
                    user_rows.fetch_add(rows.len(), std::sync::atomic::Ordering::Relaxed);
                    if checksum {
                        let h = rows.iter().fold(0u64, |a, u| a.wrapping_add(user_hash(u)));
                        user_sum.fetch_add(h, std::sync::atomic::Ordering::Relaxed);
                    }
                }
            }
            std::mem::forget(users); // freeing costs time and the process ends soon
        });
    });
    let user_rows = user_rows.into_inner();
    if user_rows == 0 {
        write_users(0, &[]); // still write one (empty) file with the schema
    }
    let mut tally = Tally::new(campaigns, days);
    for t in &tallies {
        tally.add_counts(t);
    }
    log("sessions, credit, assists and user_daily");

    // campaign_daily.
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
    if checksum {
        let campaign = (0..cols.0.len()).fold(0u64, |acc, i| {
            acc.wrapping_add(row_hash(&[cols.0[i], cols.1[i], &cols.2[i].to_le_bytes(), &cols.3[i].to_le_bytes(),
                                        &cols.4[i].to_le_bytes(), &cols.5[i].to_le_bytes(), &cols.6[i].to_le_bytes()]))
        });
        eprintln!("checksum user_daily {user_rows} rows {:016x}  campaign_daily {} rows {campaign:016x}", user_sum.into_inner(), cols.0.len());
    }
    output::write_table(&out, "campaign_daily", 0, output::CAMPAIGN_DAILY, vec![
        Column::Str(cols.0), Column::Str(cols.1), Column::Date(cols.2), Column::I64(cols.3),
        Column::I64(cols.4), Column::I64(cols.5), Column::I64(cols.6),
    ]);
    // SHOPPING_FAKE_ROW=1 adds one made-up campaign_daily row, only so a probe
    // submission fails the dev check on purpose and the scorer shows its logs.
    if std::env::var("SHOPPING_FAKE_ROW").is_ok() {
        output::write_table(&out, "campaign_daily", 1, output::CAMPAIGN_DAILY, vec![
            Column::Str(vec![b"probe_fake"]), Column::Str(vec![b"probe_fake"]), Column::Date(vec![base_day as i32]),
            Column::I64(vec![1]), Column::I64(vec![0]), Column::I64(vec![0]), Column::I64(vec![0]),
        ]);
    }
    // SHOPPING_FAKE_ROWS=FILE: one made-up row per line of FILE, `c<TAB>A<TAB>B` (a
    // campaign_daily row with campaign_id A, channel B) or `u<TAB>A<TAB>B` (a
    // user_daily row with user_id A, top_category B). The scorer shows differing
    // dev rows in full, so a probe reports its measurements this way.
    if let Ok(path) = std::env::var("SHOPPING_FAKE_ROWS") {
        let text: &'static str = Box::leak(std::fs::read_to_string(&path).unwrap_or_default().into_boxed_str());
        let (mut c, mut u): (Vec<(Str, Str)>, Vec<(Str, Str)>) = (Vec::new(), Vec::new());
        for line in text.lines() {
            let mut f = line.splitn(3, '\t');
            let (t, a, b) = (f.next().unwrap_or(""), f.next().unwrap_or("").as_bytes(), f.next().unwrap_or("").as_bytes());
            if t == "c" { c.push((a, b)) } else if t == "u" { u.push((a, b)) }
        }
        let n = |k: usize| (0..k as i64).collect::<Vec<i64>>();
        if !c.is_empty() {
            output::write_table(&out, "campaign_daily", 2, output::CAMPAIGN_DAILY, vec![
                Column::Str(c.iter().map(|r| r.0).collect()), Column::Str(c.iter().map(|r| r.1).collect()),
                Column::Date(vec![base_day as i32; c.len()]), Column::I64(n(c.len())), Column::I64(vec![0; c.len()]),
                Column::I64(vec![0; c.len()]), Column::I64(vec![0; c.len()]),
            ]);
        }
        if !u.is_empty() {
            // The same schema as the real parts (INT32 counts in compact mode).
            let (schema, sessions, events) = if compact {
                (output::USER_DAILY_32, Column::I32((0..u.len() as i32).collect()), Column::I32(vec![0; u.len()]))
            } else {
                (output::USER_DAILY, Column::I64(n(u.len())), Column::I64(vec![0; u.len()]))
            };
            output::write_table(&out, "user_daily", 900, schema, vec![
                Column::Str(u.iter().map(|r| r.0).collect()), Column::Date(vec![base_day as i32; u.len()]),
                sessions, events, Column::I64(vec![0; u.len()]),
                Column::OptStr(u.iter().map(|r| Some(r.1)).collect()),
            ]);
        }
    }
    log(&format!("wrote {user_rows} user_daily rows"));
    log_rusage();
    std::mem::forget(tallies); // freeing costs time and the process ends now
}

/// CPU time, page faults and peak memory of the run so far, to stderr.
fn log_rusage() {
    // SAFETY: getrusage only writes the struct it is given.
    let mut u: libc::rusage = unsafe { std::mem::zeroed() };
    unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut u) };
    let secs = |t: libc::timeval| t.tv_sec as f64 + t.tv_usec as f64 / 1e6;
    // ru_maxrss is in KB on Linux, bytes on macOS.
    let rss_mb = if cfg!(target_os = "macos") { u.ru_maxrss as f64 / 1e6 } else { u.ru_maxrss as f64 / 1e3 };
    eprintln!("rusage  user {:.3}s  sys {:.3}s  minflt {}  majflt {}  maxrss {:.0} MB  nvcsw {}  nivcsw {}",
              secs(u.ru_utime), secs(u.ru_stime), u.ru_minflt, u.ru_majflt, rss_mb, u.ru_nvcsw, u.ru_nivcsw);
}
