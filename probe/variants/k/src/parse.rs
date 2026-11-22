//! Timestamps, prices and digits, exactly as rules.md says. ASCII digits only.

/// A bad timestamp. It fails every window check.
pub const BAD: i64 = i64::MIN;

/// The value of a digits-only string with leading zeros ignored, or None if it
/// is empty, holds a non-digit, or has more than `max_len` significant digits.
#[inline]
pub fn digits_value(s: &[u8], max_len: usize) -> Option<i64> {
    if s.is_empty() || !s.iter().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let first = s.iter().position(|&b| b != b'0').unwrap_or(s.len());
    let sig = &s[first..];
    if sig.len() > max_len {
        return None;
    }
    Some(sig.iter().fold(0i64, |v, &b| v * 10 + (b - b'0') as i64))
}

/// Days from 1970-01-01 to a proleptic Gregorian date (H. Hinnant's days_from_civil).
#[inline]
pub fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = (m + 9) % 12; // March = 0
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

#[inline]
fn days_in_month(y: i64, m: i64) -> i64 {
    match m {
        2 => if (y % 4 == 0 && y % 100 != 0) || y % 400 == 0 { 29 } else { 28 },
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    }
}

/// F1 `2026-10-03T23:58:10Z`, F2 `2026-10-03T19:58:10-04:00` or F3 epoch ms
/// -> ms since the epoch, UTC; BAD for anything else. An F3 value with more
/// than 18 significant digits is BAD: at least 10^18 ms, it is outside any window.
#[inline]
pub fn ts_ms(s: &[u8]) -> i64 {
    let n = s.len();
    // An F1/F2 string has '-' at 4, so it can't be digits only: decide by shape first.
    if (n == 20 || n == 25) && s[4] == b'-' {
        return iso_swar(s);
    }
    if n == 13 {
        return f3_13(s); // the usual F3 length (2001 to 2286)
    }
    digits_value(s, 18).unwrap_or(BAD)
}

/// The same as `ts_ms`, with the plain (reference) parsers; tests compare the two.
#[cfg(test)]
pub fn ts_ms_ref(s: &[u8]) -> i64 {
    let n = s.len();
    if (n == 20 || n == 25) && s[4] == b'-' {
        return iso_ms(s).unwrap_or(BAD);
    }
    digits_value(s, 18).unwrap_or(BAD)
}

#[inline(always)]
fn load8(s: &[u8], i: usize) -> u64 {
    u64::from_le_bytes(s[i..i + 8].try_into().unwrap())
}

/// 0 if all 8 bytes are ASCII digits, else non-zero (exact: a carry or borrow
/// between bytes only starts at a byte that is itself flagged).
#[inline(always)]
fn non_digits(x: u64) -> u64 {
    (x.wrapping_sub(0x3030_3030_3030_3030) | x.wrapping_add(0x4646_4646_4646_4646)) & 0x8080_8080_8080_8080
}

/// The value of 8 ASCII digits, the first in the lowest byte (Lemire's SWAR method).
#[inline(always)]
fn parse8(x: u64) -> u64 {
    let x = x.wrapping_sub(0x3030_3030_3030_3030);
    let x = x.wrapping_mul(10).wrapping_add(x >> 8); // byte 2k: two-digit pair k
    ((x & 0x0000_00FF_0000_00FF).wrapping_mul(100 + (1_000_000 << 32))
        .wrapping_add(((x >> 16) & 0x0000_00FF_0000_00FF).wrapping_mul(1 + (10_000 << 32))))
        >> 32
}

/// F3 with exactly 13 characters: digits only, any value (leading zeros included).
#[inline(always)]
fn f3_13(s: &[u8]) -> i64 {
    let (a, b) = (load8(s, 0), load8(s, 5)); // bytes 0..8 and 5..13
    if non_digits(a) | non_digits(b) != 0 {
        return BAD;
    }
    (parse8(a) * 100_000 + parse8(b) % 100_000) as i64
}

/// F1 or F2 (length 20 or 25) with word-at-a-time loads: every separator,
/// digit and range check folded into one flag. Same results as `iso_ms`.
#[inline(always)]
fn iso_swar(s: &[u8]) -> i64 {
    // a: "YYYY-MM-" (bytes 0..8), b: "DDTHH:MM" (8..16), c: "H:MM:SSZ" (12..20).
    let (a, b, c) = (load8(s, 0), load8(s, 8), load8(s, 12));
    const SEP_A: u64 = 0xFF00_00FF_0000_0000; // bytes 4, 7
    const SEP_B: u64 = 0x0000_FF00_00FF_0000; // bytes 10, 13
    const SEP_C: u64 = 0x0000_00FF_0000_0000; // byte 16 (byte 4 of c)
    let mut bad = ((a & SEP_A) != 0x2D00_002D_0000_0000) | ((b & SEP_B) != 0x0000_3A00_0054_0000) | ((c & SEP_C) != 0x0000_003A_0000_0000);
    // Separators replaced by '0', so each word is digits only when the stamp is well formed.
    let ad = (a & !SEP_A) | 0x3000_0030_0000_0000;
    let bd = (b & !SEP_B) | 0x0000_3000_0030_0000;
    let cd = (c & 0x00FF_FF00_0000_0000) | 0x3000_0030_3030_3030; // seconds at bytes 5, 6
    bad |= (non_digits(ad) | non_digits(bd) | non_digits(cd)) != 0;
    let pairs = |x: u64| {
        let x = x.wrapping_sub(0x3030_3030_3030_3030);
        x.wrapping_mul(10).wrapping_add(x >> 8) // byte k: digit k * 10 + digit k+1
    };
    let (pa, pb, pc) = (pairs(ad), pairs(bd), pairs(cd));
    let byte = |x: u64, k: u32| ((x >> (8 * k)) & 0xFF) as i64;
    let year = byte(pa, 0) * 100 + byte(pa, 2);
    let (month, day, hour, minute, second) = (byte(pa, 5), byte(pb, 0), byte(pb, 3), byte(pb, 6), byte(pc, 5));
    bad |= !(1..=12).contains(&month) | (day < 1) | (hour > 23) | (minute > 59) | (second > 59);
    let offset_s = if s.len() == 20 {
        bad |= s[19] != b'Z';
        0
    } else {
        // e: "SS+HH:MM" (bytes 17..25): sign at 2, hours at 3-4, ':' at 5, minutes at 6-7.
        let e = load8(s, 17);
        let sign = s[19];
        bad |= ((e >> 40) & 0xFF) != b':' as u64;
        let ed = (e & 0xFFFF_00FF_FF00_0000) | 0x0000_3000_0030_3030;
        bad |= non_digits(ed) != 0 || (sign != b'+') & (sign != b'-');
        let pe = pairs(ed);
        let (oh, om) = (byte(pe, 3), byte(pe, 6));
        bad |= (oh > 23) | (om > 59);
        (if sign == b'-' { -1 } else { 1 }) * (oh * 3600 + om * 60)
    };
    if bad || day > days_in_month(year, month) {
        return BAD;
    }
    (days_from_civil(year, month, day) * 86400 + hour * 3600 + minute * 60 + second - offset_s) * 1000
}

/// F1 or F2 (length 20 or 25). Every check is folded into one flag, so there
/// is a single branch instead of one per digit.
#[cfg(test)]
fn iso_ms(s: &[u8]) -> Option<i64> {
    let n = s.len();
    let b: &[u8; 20] = s[..20].try_into().ok()?;
    let v = |i: usize| b[i].wrapping_sub(b'0'); // a digit's value; > 9 if not a digit
    let mut bad = (b[4] != b'-') | (b[7] != b'-') | (b[10] != b'T') | (b[13] != b':') | (b[16] != b':');
    for i in [0, 1, 2, 3, 5, 6, 8, 9, 11, 12, 14, 15, 17, 18] {
        bad |= v(i) > 9;
    }
    let two = |i: usize| v(i) as i64 * 10 + v(i + 1) as i64;
    let year = two(0) * 100 + two(2);
    let (month, day, hour, minute, second) = (two(5), two(8), two(11), two(14), two(17));
    bad |= !(1..=12).contains(&month) | (day < 1) | (hour > 23) | (minute > 59) | (second > 59);
    let offset_s = if n == 20 {
        bad |= b[19] != b'Z';
        0
    } else {
        let o: &[u8; 5] = s[20..25].try_into().ok()?;
        let w = |i: usize| o[i].wrapping_sub(b'0');
        bad |= (w(0) > 9) | (w(1) > 9) | (o[2] != b':') | (w(3) > 9) | (w(4) > 9) | ((b[19] != b'+') & (b[19] != b'-'));
        let (oh, om) = (w(0) as i64 * 10 + w(1) as i64, w(3) as i64 * 10 + w(4) as i64);
        bad |= (oh > 23) | (om > 59);
        (if b[19] == b'-' { -1 } else { 1 }) * (oh * 3600 + om * 60)
    };
    if bad || day > days_in_month(year, month) {
        return None;
    }
    let days = days_from_civil(year, month, day);
    Some((days * 86400 + hour * 3600 + minute * 60 + second - offset_s) * 1000)
}

/// A purchase's price: digits only, 1 to 10,000,000 (`0250` is 250). None if invalid.
#[inline]
pub fn price(s: &[u8]) -> Option<i64> {
    digits_value(s, 8).filter(|v| (1..=10_000_000).contains(v))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timestamps() {
        assert_eq!(ts_ms(b"2026-10-03T23:58:10Z"), 1791071890000);
        assert_eq!(ts_ms(b"2026-10-03T19:58:10-04:00"), 1791071890000);
        assert_eq!(ts_ms(b"1791071890000"), 1791071890000);
        assert_eq!(ts_ms(b"0001791071890000"), 1791071890000);
        assert_eq!(ts_ms(b"2026-10-03T19:58:10-00:00"), ts_ms(b"2026-10-03T19:58:10Z"));
        for bad in [&b""[..], b"2026-10-03 10:00:00", b"2026-10-03T10:00:00z", b"2026-10-03t10:00:00Z",
                    b"2026-10-03T10:00:00.000Z", b"2026-02-30T10:00:00Z", b"2027-02-29T10:00:00Z",
                    b"2026-13-01T10:00:00Z", b"2026-10-03T24:00:00Z", b"2026-10-03T10:00:60Z",
                    b"2026-10-03T10:00:00+24:00", b"2026-10-03T10:00:00+05:60", b"2026-10-03T10:00:00+0200",
                    b"1791071890000.0", b"1.79e12", b"-5", b"2026-10-03T10:00:00", b"1791071890000\r",
                    "\u{0661}\u{0667}".as_bytes(), b"0000000000000000000001791071890000"] {
            assert_eq!(ts_ms(bad), if bad == b"0000000000000000000001791071890000" { 1791071890000 } else { BAD },
                       "{:?}", String::from_utf8_lossy(bad));
        }
        assert_eq!(ts_ms(b"2028-02-29T00:00:00Z"), days_from_civil(2028, 2, 29) * 86_400_000);
        assert_eq!(ts_ms(b"1234567890123456789012"), BAD); // 22 significant digits
    }

    /// The word-at-a-time parsers give exactly what the plain ones give.
    #[test]
    fn swar_equals_reference() {
        let mut x: u64 = 0x1234_5678_9abc_def1;
        let mut rnd = |m: u64| { x ^= x << 13; x ^= x >> 7; x ^= x << 17; x % m };
        let alphabet = b"0123456789-:TZ+ zt.\r\xff/";
        let mut checked = 0;
        for i in 0..1_500_000u64 {
            // A valid stamp of a random form and date, then 0-3 random byte edits.
            let (y, mo, d) = (rnd(10_000), 1 + rnd(13), 1 + rnd(31));
            let (h, mi, se) = (rnd(25), rnd(61), rnd(61));
            let mut v: Vec<u8> = match i % 4 {
                0 | 1 => format!("{y:04}-{mo:02}-{d:02}T{h:02}:{mi:02}:{se:02}Z").into_bytes(),
                2 => format!("{y:04}-{mo:02}-{d:02}T{h:02}:{mi:02}:{se:02}{}{:02}:{:02}",
                             if rnd(2) == 0 { '+' } else { '-' }, rnd(26), rnd(62)).into_bytes(),
                _ => format!("{:013}", rnd(10_000_000_000_000)).into_bytes(),
            };
            for _ in 0..rnd(4) {
                let k = rnd(v.len() as u64) as usize;
                v[k] = alphabet[rnd(alphabet.len() as u64) as usize];
            }
            assert_eq!(ts_ms(&v), ts_ms_ref(&v), "{:?}", String::from_utf8_lossy(&v));
            checked += (ts_ms(&v) != BAD) as u32;
        }
        assert!(checked > 100_000, "too few valid stamps: {checked}");
    }

    #[test]
    fn prices() {
        assert_eq!(price(b"0250"), Some(250));
        assert_eq!(price(b"10000000"), Some(10_000_000));
        for bad in [&b"0"[..], b"", b"19.99", b"-5", b"+5", b"10000001", b"1000\r", "２５０".as_bytes()] {
            assert_eq!(price(bad), None);
        }
    }
}
