//! Timestamps, prices and digits, exactly as rules.md says. ASCII digits only.

/// A bad timestamp. It fails every window check.
pub const BAD: i64 = i64::MIN;

#[inline]
fn digit(b: u8) -> Option<i64> {
    if b.is_ascii_digit() { Some((b - b'0') as i64) } else { None }
}

#[inline]
fn two(s: &[u8], i: usize) -> Option<i64> {
    Some(digit(s[i])? * 10 + digit(s[i + 1])?)
}

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
    if let Some(v) = digits_value(s, 18) {
        return v;
    }
    iso_ms(s).unwrap_or(BAD)
}

#[inline]
fn iso_ms(s: &[u8]) -> Option<i64> {
    let n = s.len();
    if !(n == 20 || n == 25) {
        return None;
    }
    if s[4] != b'-' || s[7] != b'-' || s[10] != b'T' || s[13] != b':' || s[16] != b':' {
        return None;
    }
    let year = two(s, 0)? * 100 + two(s, 2)?;
    let (month, day) = (two(s, 5)?, two(s, 8)?);
    let (hour, minute, second) = (two(s, 11)?, two(s, 14)?, two(s, 17)?);
    if !(1..=12).contains(&month) || day < 1 || day > days_in_month(year, month)
        || hour > 23 || minute > 59 || second > 59 {
        return None;
    }
    let offset_s = if n == 20 {
        if s[19] != b'Z' {
            return None;
        }
        0
    } else {
        let sign = match s[19] {
            b'+' => 1,
            b'-' => -1,
            _ => return None,
        };
        if s[22] != b':' {
            return None;
        }
        let (oh, om) = (two(s, 20)?, two(s, 23)?);
        if oh > 23 || om > 59 {
            return None;
        }
        sign * (oh * 3600 + om * 60)
    };
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

    #[test]
    fn prices() {
        assert_eq!(price(b"0250"), Some(250));
        assert_eq!(price(b"10000000"), Some(10_000_000));
        for bad in [&b"0"[..], b"", b"19.99", b"-5", b"+5", b"10000001", b"1000\r", "２５０".as_bytes()] {
            assert_eq!(price(bad), None);
        }
    }
}
