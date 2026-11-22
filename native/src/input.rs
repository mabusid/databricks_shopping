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
