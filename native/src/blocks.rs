//! Append-only lists made of fixed-size blocks, carved from one big reserved arena.
//!
//! On Linux, writing to fresh memory is expensive (page faults, zeroing), and
//! scattering into many lists is expensive when each push touches several cache
//! lines. So:
//!   - all blocks come from one virtual reservation (a lock-free bump pointer,
//!     2 MB huge pages advised), so any element can also be named by a 32-bit
//!     reference (its offset from the arena base, in 8-byte words);
//!   - a `Scatter` keeps only one tail pointer per list on the hot path: a block is
//!     full when the tail reaches its (block-aligned) end.
//! Nothing is ever returned to the OS (the process ends soon after).

use std::marker::PhantomData;
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};

/// Bytes per block. Every element type stored here is `Copy`, at most 64-byte aligned.
/// Small, because every list's last block is partly empty and there are
/// thousands of lists (buckets x threads x kinds).
pub const BLOCK_BYTES: usize = 4 << 10;
/// Each thread takes the arena in chunks of this many bytes and cuts its blocks
/// from them, so its blocks share its own huge pages and the shared counter is rare.
const CHUNK_BYTES: usize = 1 << 20;

static BASE: AtomicUsize = AtomicUsize::new(0);
static NEXT: AtomicUsize = AtomicUsize::new(0);
static ARENA_BYTES: AtomicUsize = AtomicUsize::new(0);

/// Reserve the arena (once, before any block is taken): `bytes` of address
/// space (only touched pages use memory), at most 32 GB so 32-bit references work.
pub fn init(bytes: usize) {
    if BASE.load(Relaxed) != 0 {
        return;
    }
    const ALIGN: usize = 2 << 20;
    let bytes = bytes.next_multiple_of(ALIGN).clamp(64 << 20, 32 << 30);
    // SAFETY: a fresh private anonymous reservation, never unmapped.
    let p = unsafe {
        libc::mmap(std::ptr::null_mut(), bytes + ALIGN, libc::PROT_READ | libc::PROT_WRITE,
                   libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | libc::MAP_NORESERVE, -1, 0)
    };
    assert!(p != libc::MAP_FAILED, "reserve arena");
    let base = (p as usize + ALIGN - 1) & !(ALIGN - 1);
    // Ask for transparent huge pages (needed where the kernel's THP mode is "madvise").
    #[cfg(target_os = "linux")]
    // SAFETY: the range lies inside the reservation; advice never changes its contents.
    unsafe {
        // SHOPPING_THP=0 (probes): 4 KB pages instead (no compaction stalls for huge pages).
        let advice = if std::env::var("SHOPPING_THP").is_ok_and(|v| v == "0") { libc::MADV_NOHUGEPAGE } else { libc::MADV_HUGEPAGE };
        libc::madvise(base as *mut libc::c_void, bytes, advice);
    }
    POPULATE.store(std::env::var("SHOPPING_POPULATE").is_ok_and(|v| v == "1") as usize, Relaxed);
    ARENA_BYTES.store(bytes, Relaxed);
    BASE.store(base, Relaxed);
}

static POPULATE: AtomicUsize = AtomicUsize::new(0);

/// SHOPPING_POPULATE=1 (probes): map each new chunk's pages with one call
/// (MADV_POPULATE_WRITE, Linux 5.14+) instead of one page fault per page.
#[inline]
fn populate(addr: usize, len: usize) {
    #[cfg(target_os = "linux")]
    if POPULATE.load(Relaxed) == 1 {
        const MADV_POPULATE_WRITE: i32 = 23;
        // SAFETY: the range lies inside the arena; on error (old kernel) the pages just fault later.
        unsafe { libc::madvise(addr as *mut libc::c_void, len, MADV_POPULATE_WRITE) };
    }
    let _ = (addr, len);
}

thread_local! {
    /// This thread's current chunk: (next free address, end).
    static CHUNK: std::cell::Cell<(usize, usize)> = const { std::cell::Cell::new((0, 0)) };
}

#[inline]
fn take_block() -> *mut u8 {
    CHUNK.with(|c| {
        let (mut next, mut end) = c.get();
        if next == end {
            let off = NEXT.fetch_add(CHUNK_BYTES, Relaxed);
            assert!(off + CHUNK_BYTES <= ARENA_BYTES.load(Relaxed), "arena full");
            next = BASE.load(Relaxed) + off;
            end = next + CHUNK_BYTES;
            populate(next, CHUNK_BYTES);
        }
        c.set((next + BLOCK_BYTES, end));
        next as *mut u8
    })
}

/// A 32-bit name for an element in the arena (8-byte aligned, below 32 GB).
#[inline]
pub fn ref_of<T>(p: *mut T) -> u32 {
    ((p as usize - BASE.load(Relaxed)) >> 3) as u32
}

/// The element a `ref_of` names.
#[inline]
pub fn at<T>(r: u32) -> *mut T {
    (BASE.load(Relaxed) + ((r as usize) << 3)) as *mut T
}

/// `n` zeroed `T`s in their own mapping, advised to use 2 MB huge pages (big
/// random-access tables: far fewer TLB misses). Never freed.
///
/// # Safety
/// All-zero bytes must be a valid `T`.
pub unsafe fn huge_zeroed<T>(n: usize) -> &'static mut [T] {
    const ALIGN: usize = 2 << 20;
    let bytes = (n * std::mem::size_of::<T>()).max(1);
    // SAFETY: a fresh private anonymous mapping (zero-filled), never unmapped;
    // 2 MB more than needed so the table can start on a huge-page boundary.
    let raw = unsafe {
        libc::mmap(std::ptr::null_mut(), bytes + ALIGN, libc::PROT_READ | libc::PROT_WRITE, libc::MAP_PRIVATE | libc::MAP_ANONYMOUS, -1, 0)
    };
    assert!(raw != libc::MAP_FAILED, "mmap table");
    let p = ((raw as usize + ALIGN - 1) & !(ALIGN - 1)) as *mut libc::c_void;
    #[cfg(target_os = "linux")]
    // SAFETY: the range is exactly the mapping just made.
    unsafe {
        libc::madvise(p, bytes, libc::MADV_HUGEPAGE);
    }
    // SAFETY: page-aligned (so aligned for T), zeroed, and zero is a valid T (caller).
    unsafe { std::slice::from_raw_parts_mut(p as *mut T, n) }
}

/// One finished list of `T` (read side).
pub struct BlockList<T: Copy> {
    blocks: Vec<*mut T>,
    in_last: usize,
    len: usize,
    _t: PhantomData<T>,
}

// SAFETY: a list owns its blocks exclusively; T is plain data.
unsafe impl<T: Copy + Send> Send for BlockList<T> {}
unsafe impl<T: Copy + Sync> Sync for BlockList<T> {}

impl<T: Copy> BlockList<T> {
    const PER_BLOCK: usize = BLOCK_BYTES / std::mem::size_of::<T>();

    pub fn len(&self) -> usize {
        self.len
    }

    /// The elements, block by block.
    pub fn slices(&self) -> impl Iterator<Item = &[T]> + '_ {
        let last = self.blocks.len().wrapping_sub(1);
        self.blocks.iter().enumerate().map(move |(i, &b)| {
            let n = if i == last { self.in_last } else { Self::PER_BLOCK };
            // SAFETY: the first n elements of every block were written by push.
            unsafe { std::slice::from_raw_parts(b, n) }
        })
    }

    pub fn iter(&self) -> impl Iterator<Item = &T> + '_ {
        self.slices().flatten()
    }

    /// Done with the list (its memory is not reused).
    pub fn release(self) {}
}

/// `n` append-only lists written by one thread: one tail pointer per list.
pub struct Scatter<T: Copy> {
    tails: Vec<*mut T>,
    blocks: Vec<Vec<*mut T>>,
}

// SAFETY: as BlockList.
unsafe impl<T: Copy + Send> Send for Scatter<T> {}

impl<T: Copy> Scatter<T> {
    const SIZE: usize = std::mem::size_of::<T>();

    pub fn new(n: usize) -> Self {
        assert!(std::mem::align_of::<T>() <= 64 && Self::SIZE > 0 && Self::SIZE <= BLOCK_BYTES);
        assert!(BASE.load(Relaxed) != 0, "blocks::init first");
        Scatter { tails: vec![std::ptr::null_mut(); n], blocks: (0..n).map(|_| Vec::new()).collect() }
    }

    /// Append `v` to list `b`; returns where it is stored (stable for the program's life).
    #[inline(always)]
    pub fn push(&mut self, b: usize, v: T) -> *mut T {
        // SAFETY: b < n (callers pass a bucket index); a tail is either null, or
        // points into its last block with room for one more element unless its
        // offset in the (block-aligned) block says otherwise.
        unsafe {
            let slot = self.tails.get_unchecked_mut(b);
            let mut t = *slot;
            let off = t as usize & (BLOCK_BYTES - 1);
            if off == 0 || off + Self::SIZE > BLOCK_BYTES {
                t = Self::grow(&mut self.blocks, b);
            }
            t.write(v);
            *self.tails.get_unchecked_mut(b) = t.add(1);
            t
        }
    }

    #[cold]
    #[inline(never)]
    fn grow(blocks: &mut [Vec<*mut T>], b: usize) -> *mut T {
        let p = take_block() as *mut T;
        blocks[b].push(p);
        p
    }

    /// The finished lists, one per index.
    pub fn into_lists(self) -> Vec<BlockList<T>> {
        self.blocks
            .into_iter()
            .zip(self.tails)
            .map(|(blocks, tail)| {
                let in_last = match blocks.last() {
                    Some(&last) => (tail as usize - last as usize) / Self::SIZE,
                    None => 0,
                };
                let len = blocks.len().saturating_sub(1) * BlockList::<T>::PER_BLOCK + in_last;
                BlockList { blocks, in_last, len, _t: PhantomData }
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn push_iterate_refs() {
        init(1 << 30);
        let mut s: Scatter<u64> = Scatter::new(3);
        let n = BLOCK_BYTES / 8 * 3 + 7;
        let mut last = std::ptr::null_mut();
        for i in 0..n as u64 {
            last = s.push((i % 2) as usize, i * 3);
        }
        assert_eq!(unsafe { *at::<u64>(ref_of(last)) }, (n as u64 - 1) * 3);
        let lists = s.into_lists();
        assert_eq!(lists[0].len() + lists[1].len(), n);
        assert_eq!(lists[2].len(), 0);
        assert!(lists[0].iter().copied().eq((0..n as u64).filter(|i| i % 2 == 0).map(|i| i * 3)));
        assert!(lists[1].iter().copied().eq((0..n as u64).filter(|i| i % 2 == 1).map(|i| i * 3)));
        // A size that does not divide the block.
        let mut t: Scatter<[u64; 3]> = Scatter::new(1);
        for i in 0..50_000u64 {
            t.push(0, [i, i + 1, i + 2]);
        }
        let l = t.into_lists();
        assert!(l[0].iter().copied().eq((0..50_000u64).map(|i| [i, i + 1, i + 2])));
    }
}
