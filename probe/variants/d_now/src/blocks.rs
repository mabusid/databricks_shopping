//! Append-only lists made of fixed-size blocks from one shared pool.
//!
//! On Linux, writing to fresh memory is expensive: every 4 KB page faults and
//! the kernel zeroes it (measured: pushing parsed event copies into fresh
//! memory took 0.19 s on 2M events, into reused memory 0.11 s). So lists here
//! never reallocate, and a list that is done with gives its blocks back to the
//! pool, where the next lists (events, clicks) pick them up already mapped.
//! Nothing is ever returned to the OS.

use std::alloc::{alloc, Layout};
use std::marker::PhantomData;
use std::sync::Mutex;

/// Bytes per block. Every element type stored here is `Copy`, at most 64-byte aligned.
const BLOCK_BYTES: usize = 64 << 10;
/// New blocks are carved from 16 MB slabs, 2 MB aligned so the kernel can back
/// them with 2 MB huge pages (far fewer page faults).
const SLAB_BLOCKS: usize = 256;
const SLAB_ALIGN: usize = 2 << 20;

/// Free blocks, as addresses (a `usize` is `Send`, a raw pointer is not).
static POOL: Mutex<Vec<usize>> = Mutex::new(Vec::new());

fn take_block() -> *mut u8 {
    let mut free = POOL.lock().unwrap();
    if let Some(b) = free.pop() {
        return b as *mut u8;
    }
    // SAFETY: a non-zero size and a valid power-of-two alignment.
    let slab = unsafe { alloc(Layout::from_size_align(BLOCK_BYTES * SLAB_BLOCKS, SLAB_ALIGN).unwrap()) };
    assert!(!slab.is_null(), "out of memory");
    // Ask for transparent huge pages (needed where the kernel's THP mode is "madvise").
    // SAFETY: the range is exactly the slab just allocated; advice never changes its contents.
    #[cfg(target_os = "linux")]
    unsafe {
        libc::madvise(slab as *mut libc::c_void, BLOCK_BYTES * SLAB_BLOCKS, libc::MADV_HUGEPAGE);
    }
    free.extend((1..SLAB_BLOCKS).map(|i| slab as usize + i * BLOCK_BYTES));
    slab
}

/// An append-only list of `T` in pool blocks.
pub struct BlockList<T: Copy> {
    blocks: Vec<*mut T>,
    in_last: usize, // elements in the last block
    len: usize,
    _t: PhantomData<T>,
}

// SAFETY: a list owns its blocks exclusively; T is plain data.
unsafe impl<T: Copy + Send> Send for BlockList<T> {}
unsafe impl<T: Copy + Sync> Sync for BlockList<T> {}

impl<T: Copy> BlockList<T> {
    const PER_BLOCK: usize = BLOCK_BYTES / std::mem::size_of::<T>();

    pub fn new() -> Self {
        assert!(std::mem::align_of::<T>() <= 64 && std::mem::size_of::<T>() > 0);
        BlockList { blocks: Vec::new(), in_last: 0, len: 0, _t: PhantomData }
    }

    /// Append `v`; returns where it is stored. Blocks never move, so the address
    /// stays valid until the list is released.
    #[inline]
    pub fn push(&mut self, v: T) -> *mut T {
        if self.blocks.is_empty() || self.in_last == Self::PER_BLOCK {
            self.blocks.push(take_block() as *mut T);
            self.in_last = 0;
        }
        // SAFETY: the last block has room for PER_BLOCK elements and in_last < PER_BLOCK.
        let at = unsafe { self.blocks.last().unwrap().add(self.in_last) };
        unsafe { at.write(v) };
        self.in_last += 1;
        self.len += 1;
        at
    }

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

    /// Give the blocks back to the pool for later lists. Anything borrowed from
    /// this list must be gone, which taking `self` by value guarantees.
    pub fn release(self) {
        if !self.blocks.is_empty() {
            POOL.lock().unwrap().extend(self.blocks.iter().map(|&b| b as usize));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn push_iterate_release_reuse() {
        let mut a: BlockList<u64> = BlockList::new();
        let n = BlockList::<u64>::PER_BLOCK * 3 + 7;
        for i in 0..n as u64 {
            a.push(i * 3);
        }
        assert_eq!(a.len(), n);
        assert!(a.iter().copied().eq((0..n as u64).map(|i| i * 3)));
        a.release();
        // A list of another type reuses the blocks and still reads back exactly.
        let mut b: BlockList<(u32, u16)> = BlockList::new();
        for i in 0..50_000u32 {
            b.push((i, (i % 7) as u16));
        }
        assert!(b.iter().copied().eq((0..50_000u32).map(|i| (i, (i % 7) as u16))));
        let empty: BlockList<u8> = BlockList::new();
        assert_eq!(empty.iter().count(), 0);
        empty.release();
    }
}
