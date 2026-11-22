//! Reading the CSV inputs exactly as rules.md's "CSV dialect" says.
//!
//! Files are memory-mapped and leaked, so every field is a `&'static [u8]`
//! slice into the mapped bytes: no copies, no allocation per field.

use memchr::{memchr, memchr_iter};
use memmap2::Mmap;
use std::fs::File;
use std::path::{Path, PathBuf};

/// Every `*.csv` file directly in `dir`, sorted (names starting with `.` are skipped, like a shell glob).
pub fn csv_files(dir: &Path) -> Vec<PathBuf> {
    let mut files: Vec<PathBuf> = match std::fs::read_dir(dir) {
        Ok(entries) => entries
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| {
                let name = p.file_name().and_then(|n| n.to_str()).unwrap_or("");
                name.ends_with(".csv") && !name.starts_with('.') && p.is_file()
            })
            .collect(),
        Err(_) => Vec::new(),
    };
    files.sort();
    files
}

/// The whole file, memory-mapped for the life of the program.
pub fn map(path: &Path) -> &'static [u8] {
    let file = File::open(path).unwrap_or_else(|e| panic!("open {}: {e}", path.display()));
    if file.metadata().map(|m| m.len()).unwrap_or(0) == 0 {
        return &[];
    }
    let mmap = unsafe { Mmap::map(&file) }.unwrap_or_else(|e| panic!("mmap {}: {e}", path.display()));
    let mmap: &'static Mmap = Box::leak(Box::new(mmap));
    &mmap[..]
}

/// The data lines' bytes: everything after the header line. A later line equal
/// to the header is data (fixture L3); a file with no newline has no data lines.
pub fn body(data: &'static [u8]) -> &'static [u8] {
    match memchr(b'\n', data) {
        Some(i) => &data[i + 1..],
        None => &[],
    }
}

/// Cut the bodies of all files into pieces of about `target` bytes, each made
/// of whole lines, so they can be parsed in parallel.
pub fn chunks(bodies: &[&'static [u8]], target: usize) -> Vec<&'static [u8]> {
    let mut out = Vec::new();
    for &body in bodies {
        let mut start = 0;
        while start < body.len() {
            let mut end = (start + target).min(body.len());
            if end < body.len() {
                end = match memchr(b'\n', &body[end..]) {
                    Some(i) => end + i + 1,
                    None => body.len(),
                };
            }
            out.push(&body[start..end]);
            start = end;
        }
    }
    out
}

/// All chunks of all `*.csv` files in `dir`.
pub fn dir_chunks(dir: &Path, target: usize) -> Vec<&'static [u8]> {
    let bodies: Vec<&'static [u8]> = csv_files(dir).iter().map(|p| body(map(p))).collect();
    chunks(&bodies, target)
}

/// Call `f` on every line of a chunk. Only `\n` ends a line; a `\r` stays in
/// the line as an ordinary character. A final line without `\n` is a line.
#[inline]
pub fn for_each_line(chunk: &'static [u8], mut f: impl FnMut(&'static [u8])) {
    let mut start = 0;
    for nl in memchr_iter(b'\n', chunk) {
        f(&chunk[start..nl]);
        start = nl + 1;
    }
    if start < chunk.len() {
        f(&chunk[start..]);
    }
}

/// Split a line into exactly N fields, or None if it is malformed.
///
/// A line is malformed unless it has exactly N comma-separated fields, each
/// either holding no quote or wrapped in quotes with none inside. Quotes are
/// syntax and are removed. An empty field (`` or `""`) is an empty slice,
/// which callers treat as null. Spaces are kept. An empty line has 1 field.
#[inline]
pub fn fields<const N: usize>(line: &'static [u8]) -> Option<[&'static [u8]; N]> {
    let mut out: [&'static [u8]; N] = [&[]; N];
    let mut n = 0;
    let mut start = 0;
    for comma in memchr_iter(b',', line) {
        if n + 1 >= N {
            return None; // too many fields
        }
        out[n] = &line[start..comma];
        n += 1;
        start = comma + 1;
    }
    if n + 1 != N {
        return None; // too few fields
    }
    out[n] = &line[start..];
    if memchr(b'"', line).is_some() {
        for f in out.iter_mut() {
            if memchr(b'"', f).is_some() {
                let k = f.len();
                if k >= 2 && f[0] == b'"' && f[k - 1] == b'"' && memchr(b'"', &f[1..k - 1]).is_none() {
                    *f = &f[1..k - 1];
                } else {
                    return None; // a quote anywhere except around the whole field
                }
            }
        }
    }
    Some(out)
}

// --- One pass over a chunk: lines and fields together, 8 bytes at a time ---

const LOW7: u64 = 0x7f7f_7f7f_7f7f_7f7f;
const ONES: u64 = 0x0101_0101_0101_0101;

/// 0x80 in every byte of `w` that equals `b`, 0 elsewhere (exact: no carries between bytes).
#[inline(always)]
fn eq_mask(w: u64, b: u8) -> u64 {
    let x = w ^ (ONES * b as u64);
    !(((x & LOW7).wrapping_add(LOW7)) | x | LOW7)
}

/// Bitmasks, one bit per byte, of the commas, quotes and newlines in a 64-byte
/// block (simdjson's "stage 1"). SSE2 on x86-64 (every x86-64 CPU has it),
/// NEON on arm64, plain 8-byte words elsewhere.
#[cfg(target_arch = "x86_64")]
#[inline(always)]
fn masks64(b: &[u8; 64]) -> (u64, u64, u64) {
    use std::arch::x86_64::*;
    // SAFETY: SSE2 is part of the x86-64 baseline; loads are unaligned and in bounds.
    unsafe {
        let (comma, quote, newline) = (_mm_set1_epi8(b',' as i8), _mm_set1_epi8(b'"' as i8), _mm_set1_epi8(b'\n' as i8));
        let (mut c, mut q, mut n) = (0u64, 0u64, 0u64);
        for k in 0..4 {
            let v = _mm_loadu_si128(b.as_ptr().add(16 * k) as *const __m128i);
            c |= (_mm_movemask_epi8(_mm_cmpeq_epi8(v, comma)) as u32 as u64) << (16 * k);
            q |= (_mm_movemask_epi8(_mm_cmpeq_epi8(v, quote)) as u32 as u64) << (16 * k);
            n |= (_mm_movemask_epi8(_mm_cmpeq_epi8(v, newline)) as u32 as u64) << (16 * k);
        }
        (c, q, n)
    }
}

#[cfg(target_arch = "aarch64")]
#[inline(always)]
fn masks64(b: &[u8; 64]) -> (u64, u64, u64) {
    use std::arch::aarch64::*;
    // SAFETY: NEON is part of the arm64 baseline; loads are in bounds.
    unsafe {
        const BIT: [u8; 16] = [1, 2, 4, 8, 16, 32, 64, 128, 1, 2, 4, 8, 16, 32, 64, 128];
        let bit = vld1q_u8(BIT.as_ptr());
        let v = [vld1q_u8(b.as_ptr()), vld1q_u8(b.as_ptr().add(16)), vld1q_u8(b.as_ptr().add(32)), vld1q_u8(b.as_ptr().add(48))];
        // Compare, keep one distinct bit per byte, then add neighbouring bytes
        // three times: byte j of the result is the mask of bytes 8j..8j+8.
        let mask = |x: u8| -> u64 {
            let x = vdupq_n_u8(x);
            let t = [vandq_u8(vceqq_u8(v[0], x), bit), vandq_u8(vceqq_u8(v[1], x), bit),
                     vandq_u8(vceqq_u8(v[2], x), bit), vandq_u8(vceqq_u8(v[3], x), bit)];
            let s = vpaddq_u8(vpaddq_u8(t[0], t[1]), vpaddq_u8(t[2], t[3]));
            vgetq_lane_u64(vreinterpretq_u64_u8(vpaddq_u8(s, s)), 0)
        };
        (mask(b','), mask(b'"'), mask(b'\n'))
    }
}

#[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
#[inline(always)]
fn masks64(b: &[u8; 64]) -> (u64, u64, u64) {
    let (mut c, mut q, mut n) = (0u64, 0u64, 0u64);
    for k in 0..8 {
        let w = u64::from_le_bytes(b[8 * k..8 * k + 8].try_into().unwrap());
        // 0x80 per matching byte -> one bit per byte.
        let squash = |m: u64| ((m >> 7).wrapping_mul(0x0102_0408_1020_4080) >> 56) as u64;
        c |= squash(eq_mask(w, b',')) << (8 * k);
        q |= squash(eq_mask(w, b'"')) << (8 * k);
        n |= squash(eq_mask(w, b'\n')) << (8 * k);
    }
    (c, q, n)
}

/// Call `f(fields)` for every line of `chunk`: Some if the line has exactly N
/// fields, each fully quoted or quote-free (quotes removed), None if it is
/// malformed. Same rules as `fields::<N>(line)`.
///
/// Commas, newlines and quotes are found 8 bytes at a time with bitmasks.
/// Commas are recorded; quotes are only counted. At the end of a line, the line
/// is valid exactly when its quote count equals 2 for every field that starts
/// and ends with a quote: any other quote makes the count too high.
#[inline]
#[allow(unused_assignments)] // the last end_line! resets state nobody reads
pub fn for_each_record<const N: usize>(chunk: &'static [u8], mut f: impl FnMut(Option<[&'static [u8]; N]>)) {
    let n = chunk.len();
    let mut commas = [0usize; N]; // positions of the line's first N-1 commas
    let mut nc = 0usize; // commas in the current line
    let mut quotes = 0u32; // quotes in the current line
    let mut line_start = 0usize;

    macro_rules! comma {
        ($pos:expr) => {{
            if nc < N - 1 {
                commas[nc] = $pos;
            }
            nc += 1;
        }};
    }
    macro_rules! end_line {
        ($end:expr) => {{
            let end = $end;
            if nc + 1 == N {
                let mut out: [&'static [u8]; N] = [&[]; N];
                let mut s = line_start;
                let mut boundary = 0u32;
                for k in 0..N {
                    let e = if k + 1 < N { commas[k] } else { end };
                    let (mut fs, mut fe) = (s, e);
                    if quotes != 0 && e - s >= 2 && chunk[s] == b'"' && chunk[e - 1] == b'"' {
                        fs += 1;
                        fe -= 1;
                        boundary += 2;
                    }
                    out[k] = &chunk[fs..fe];
                    s = e + 1;
                }
                f(if boundary == quotes { Some(out) } else { None });
            } else {
                f(None); // wrong number of fields (an empty line has 1)
            }
            nc = 0;
            quotes = 0;
            line_start = end + 1;
        }};
    }

    let mut i = 0;
    // 64 bytes at a time with SIMD; the rest 8 bytes at a time, then byte by byte.
    while i + 64 <= n {
        let (mut c, mut q, mut nl) = masks64(chunk[i..i + 64].try_into().unwrap());
        while nl != 0 {
            // Everything before this newline belongs to the current line.
            let k = nl.trailing_zeros();
            let below = (1u64 << k) - 1;
            let mut cb = c & below;
            while cb != 0 {
                comma!(i + cb.trailing_zeros() as usize);
                cb &= cb - 1;
            }
            quotes += (q & below).count_ones();
            c &= !below;
            q &= !below;
            end_line!(i + k as usize);
            nl &= nl - 1;
        }
        while c != 0 {
            comma!(i + c.trailing_zeros() as usize);
            c &= c - 1;
        }
        quotes += q.count_ones();
        i += 64;
    }
    while i + 8 <= n {
        let w = u64::from_le_bytes(chunk[i..i + 8].try_into().unwrap());
        let mut c = eq_mask(w, b',');
        let mut q = eq_mask(w, b'"');
        let mut nl = eq_mask(w, b'\n');
        while nl != 0 {
            // Everything before this newline belongs to the current line.
            let k = nl.trailing_zeros();
            let below = (1u64 << k) - 1;
            let mut cb = c & below;
            while cb != 0 {
                comma!(i + (cb.trailing_zeros() / 8) as usize);
                cb &= cb - 1;
            }
            quotes += (q & below).count_ones();
            c &= !below;
            q &= !below;
            end_line!(i + (k / 8) as usize);
            nl &= nl - 1;
        }
        while c != 0 {
            comma!(i + (c.trailing_zeros() / 8) as usize);
            c &= c - 1;
        }
        quotes += q.count_ones();
        i += 8;
    }
    while i < n {
        match chunk[i] {
            b',' => comma!(i),
            b'"' => quotes += 1,
            b'\n' => end_line!(i),
            _ => {}
        }
        i += 1;
    }
    if line_start < n {
        end_line!(n); // a final line without '\n'
    }
}

/// Every well-formed line of a small, clean file (products.csv, campaigns.csv).
pub fn small_table<const N: usize>(path: &Path) -> Vec<[&'static [u8]; N]> {
    let mut rows = Vec::new();
    if path.is_file() {
        for_each_line(body(map(path)), |line| {
            if let Some(f) = fields::<N>(line) {
                rows.push(f);
            }
        });
    }
    rows
}

#[cfg(test)]
mod tests {
    use super::*;

    fn old_way<const N: usize>(chunk: &'static [u8]) -> Vec<Option<[&'static [u8]; N]>> {
        let mut v = Vec::new();
        for_each_line(chunk, |line| v.push(fields::<N>(line)));
        v
    }

    fn new_way<const N: usize>(chunk: &'static [u8]) -> Vec<Option<[&'static [u8]; N]>> {
        let mut v = Vec::new();
        for_each_record::<N>(chunk, |r| v.push(r));
        v
    }

    fn check<const N: usize>(chunk: &'static [u8]) {
        assert_eq!(old_way::<N>(chunk), new_way::<N>(chunk), "N={N} chunk={:?}", String::from_utf8_lossy(chunk));
    }

    #[test]
    fn same_as_line_then_fields() {
        for s in ["", "\n", "\n\n", "a,b\n", "a,b", "a,b,\n", ",\n", "\"a\",\"\"\n", "\"a\"b,c\n", "a,\"b\n",
                  "\"\",\"\"", "a,b\r\nc,d\n", "\"\"\"\",x\n", "a,b,c\n", "x,\"y\"\n\n,\n"] {
            let s: &'static [u8] = Box::leak(s.as_bytes().to_vec().into_boxed_slice());
            check::<2>(s);
            check::<3>(s);
        }
        // Random chunks over a small alphabet that hits every case.
        let alphabet = b"ab,\"\n \r";
        let mut x: u64 = 0x2545_f491_4f6c_dd1d;
        for _ in 0..50_000 {
            let len = (x % 300) as usize;
            let mut buf = Vec::with_capacity(len);
            for _ in 0..len {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                buf.push(alphabet[(x % alphabet.len() as u64) as usize]);
            }
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            let s: &'static [u8] = Box::leak(buf.into_boxed_slice());
            check::<2>(s);
            check::<4>(s);
            check::<7>(s);
        }
    }
}
