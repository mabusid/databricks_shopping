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

/// Call `f(line, fields)` for every line of `chunk`: `fields` is Some if the
/// line has exactly N fields, each fully quoted or quote-free (quotes removed),
/// None if it is malformed. Same rules as `fields::<N>(line)`.
#[inline]
pub fn for_each_record<const N: usize>(chunk: &'static [u8], mut f: impl FnMut(Option<[&'static [u8]; N]>)) {
    let n = chunk.len();
    let mut out: [&'static [u8]; N] = [&[]; N];
    let mut nf = 0usize; // fields closed so far in this line
    let mut bad = false; // line already known malformed
    let mut field_start = 0usize;
    let mut q_count = 0u32; // quotes in the current field
    let mut q_first = 0usize;
    let mut q_last = 0usize;

    // Close the field [field_start, end): check its quotes, store it.
    macro_rules! close_field {
        ($end:expr) => {{
            let end = $end;
            if !bad {
                if nf >= N {
                    bad = true;
                } else {
                    let mut s = field_start;
                    let mut e = end;
                    if q_count != 0 {
                        if q_count == 2 && q_first == field_start && q_last + 1 == end && end - field_start >= 2 {
                            s += 1;
                            e -= 1;
                        } else {
                            bad = true;
                        }
                    }
                    out[nf] = &chunk[s..e];
                    nf += 1;
                }
            }
            #[allow(unused_assignments)]
            { q_count = 0; }
        }};
    }
    macro_rules! end_line {
        ($end:expr) => {{
            close_field!($end);
            f(if !bad && nf == N { Some(out) } else { None });
            nf = 0;
            bad = false;
            field_start = $end + 1;
        }};
    }
    macro_rules! delim {
        ($pos:expr, $byte:expr) => {{
            let pos = $pos;
            match $byte {
                b',' => {
                    close_field!(pos);
                    field_start = pos + 1;
                }
                b'\n' => end_line!(pos),
                _ => {
                    // a quote
                    if q_count == 0 {
                        q_first = pos;
                    }
                    q_last = pos;
                    q_count += 1;
                }
            }
        }};
    }

    let mut i = 0;
    while i + 8 <= n {
        let w = u64::from_le_bytes(chunk[i..i + 8].try_into().unwrap());
        let mut m = eq_mask(w, b',') | eq_mask(w, b'\n') | eq_mask(w, b'"');
        while m != 0 {
            let k = (m.trailing_zeros() / 8) as usize;
            delim!(i + k, chunk[i + k]);
            m &= m - 1;
        }
        i += 8;
    }
    while i < n {
        let b = chunk[i];
        if b == b',' || b == b'\n' || b == b'"' {
            delim!(i, b);
        }
        i += 1;
    }
    if field_start < n || nf > 0 {
        // a final line without '\n'
        close_field!(n);
        f(if !bad && nf == N { Some(out) } else { None });
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
        for _ in 0..20_000 {
            let len = (x % 90) as usize;
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
