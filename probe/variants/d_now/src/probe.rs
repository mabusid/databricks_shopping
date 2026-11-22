//! Probe mode (`SHOPPING_PROBE=<dataset dir>`): microbenchmarks of the machine,
//! printed to stderr. Only for probe submissions, which fail the dev check on
//! purpose so the scorer shows these logs (see DECISIONS.md). Never used in a scored run.

use rayon::prelude::*;
use std::path::Path;
use std::time::Instant;

fn line(what: &str, secs: f64, extra: String) {
    eprintln!("probe {what:<34} {:8.4}s  {extra}", secs);
}

/// An anonymous mapping of `bytes`, with `advice` applied (0 = none).
unsafe fn anon(bytes: usize, flags: i32, advice: i32) -> *mut u8 {
    let p = unsafe {
        libc::mmap(std::ptr::null_mut(), bytes, libc::PROT_READ | libc::PROT_WRITE, libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | flags, -1, 0)
    };
    assert!(p != libc::MAP_FAILED, "mmap");
    #[cfg(target_os = "linux")]
    if advice != 0 {
        unsafe { libc::madvise(p, bytes, advice) };
    }
    let _ = advice;
    p as *mut u8
}

/// Touch one byte per 4 KB page of [p, p + bytes) with all threads.
fn touch(p: *mut u8, bytes: usize) {
    let addr = p as usize;
    (0..bytes / 4096).into_par_iter().with_min_len(256).for_each(|i| unsafe { *((addr + i * 4096) as *mut u8) = 1 });
}

fn fresh_memory(name: &str, flags: i32, advice: i32) {
    const GB: usize = 1 << 30;
    let t = Instant::now();
    let p = unsafe { anon(GB, flags, advice) };
    touch(p, GB);
    let fault = t.elapsed().as_secs_f64();
    let t = Instant::now();
    unsafe { libc::munmap(p as *mut libc::c_void, GB) };
    line(&format!("fresh 1GB {name}"), fault, format!("munmap {:.4}s", t.elapsed().as_secs_f64()));
}

pub fn run(dataset: &Path) {
    eprintln!("probe threads {}", rayon::current_num_threads());
    #[cfg(target_arch = "x86_64")]
    eprintln!(
        "probe cpu avx2={} bmi1={} bmi2={} popcnt={} lzcnt={} avx512f={} avx512bw={}",
        is_x86_feature_detected!("avx2"), is_x86_feature_detected!("bmi1"), is_x86_feature_detected!("bmi2"),
        is_x86_feature_detected!("popcnt"), is_x86_feature_detected!("lzcnt"), is_x86_feature_detected!("avx512f"),
        is_x86_feature_detected!("avx512bw")
    );

    // Reading the input: mmap + touch every page, vs read() into a buffer.
    let mut files = crate::input::csv_files(&dataset.join("events"));
    files.extend(crate::input::csv_files(&dataset.join("clicks")));
    let total: u64 = files.iter().map(|f| std::fs::metadata(f).map(|m| m.len()).unwrap_or(0)).sum();
    let gb = total as f64 / 1e9;
    for round in 0..2 {
        let t = Instant::now();
        let maps: Vec<memmap2::Mmap> = files.par_iter().map(|f| unsafe { memmap2::Mmap::map(&std::fs::File::open(f).unwrap()).unwrap() }).collect();
        let sum: u64 = maps.par_iter().map(|m| m.chunks(4096).map(|c| c[0] as u64).sum::<u64>()).sum();
        let s = t.elapsed().as_secs_f64();
        let t = Instant::now();
        drop(maps);
        line(&format!("mmap+touch {gb:.2}GB r{round}"), s, format!("{:.1} GB/s, unmap {:.4}s ({sum})", gb / s, t.elapsed().as_secs_f64()));
    }
    for round in 0..2 {
        let t = Instant::now();
        let sum: u64 = files.par_iter().map(|f| {
            use std::io::Read;
            let mut v = Vec::new();
            std::fs::File::open(f).unwrap().read_to_end(&mut v).unwrap();
            v.len() as u64
        }).sum();
        let s = t.elapsed().as_secs_f64();
        line(&format!("read() {gb:.2}GB r{round}"), s, format!("{:.1} GB/s ({sum})", gb / s));
    }
    // read() into one reused, pre-faulted buffer per thread (no page faults at all).
    {
        let t = Instant::now();
        let sum: u64 = files.par_iter().map_init(|| { let mut b = vec![0u8; 64 << 20]; b.iter_mut().step_by(4096).for_each(|x| *x = 1); b }, |buf, f| {
            use std::io::Read;
            let mut file = std::fs::File::open(f).unwrap();
            let mut n = 0u64;
            loop {
                let k = file.read(&mut buf[..]).unwrap();
                if k == 0 { break }
                n += k as u64;
            }
            n
        }).sum();
        let s = t.elapsed().as_secs_f64();
        line(&format!("read() into reused 64MB {gb:.2}GB"), s, format!("{:.1} GB/s ({sum})", gb / s));
    }

    // Fresh anonymous memory: 4 KB pages, transparent huge pages, MAP_POPULATE.
    #[cfg(target_os = "linux")]
    {
        fresh_memory("4KB (NOHUGEPAGE)", 0, libc::MADV_NOHUGEPAGE);
        fresh_memory("default", 0, 0);
        fresh_memory("MADV_HUGEPAGE", 0, libc::MADV_HUGEPAGE);
        fresh_memory("MAP_POPULATE", libc::MAP_POPULATE, 0);
        fresh_memory("MAP_POPULATE+HUGEPAGE", libc::MAP_POPULATE, libc::MADV_HUGEPAGE);
    }

    // Memory read bandwidth over a touched 1 GB buffer, 1 thread and all threads.
    let buf: Vec<u64> = vec![1u64; (1 << 30) / 8];
    for all in [false, true] {
        let t = Instant::now();
        let s: u64 = if all { buf.par_chunks(1 << 16).map(|c| c.iter().sum::<u64>()).sum() } else { buf.iter().sum() };
        let e = t.elapsed().as_secs_f64();
        line(&format!("read bandwidth 1GB {}", if all { "all threads" } else { "1 thread" }), e, format!("{:.1} GB/s ({s})", 1.073741824 / e));
    }
    // Scattered writes into 1024 streams of fresh-but-touched memory (like a bucket scatter).
    drop(buf);

    // Random-access latency: a pointer chase through one random cycle.
    for mb in [1usize, 8, 64, 256] {
        let n = mb * (1 << 20) / 4;
        let mut next: Vec<u32> = (0..n as u32).collect();
        let mut x: u64 = 0x9E37_79B9_7F4A_7C15;
        for i in (1..n).rev() {
            x ^= x << 13; x ^= x >> 7; x ^= x << 17;
            next.swap(i, (x % i as u64) as usize); // Sattolo: one big cycle
        }
        let steps = 4_000_000usize;
        let t = Instant::now();
        let mut at = 0u32;
        for _ in 0..steps {
            at = next[at as usize];
        }
        let e = t.elapsed().as_secs_f64();
        line(&format!("pointer chase {mb}MB"), e, format!("{:.1} ns/load ({at})", e * 1e9 / steps as f64));
    }
}
