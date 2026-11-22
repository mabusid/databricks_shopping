//! Writing the two output tables as Parquet, with exactly the columns rules.md lists.
//!
//! Our own minimal writer: one row group, one PLAIN data page (v1) per column,
//! no compression, no statistics. Required columns have no level data; the
//! nullable column's definition levels are RLE runs. The file is built in memory
//! and written with one call. Metadata uses Thrift's compact protocol.

use std::fs::create_dir_all;
use std::path::Path;

/// One column's values, in row order.
pub enum Column {
    Str(Vec<&'static [u8]>),
    /// Nullable string: None is null.
    OptStr(Vec<Option<&'static [u8]>>),
    /// Days since 1970-01-01 (Parquet DATE).
    Date(Vec<i32>),
    I64(Vec<i64>),
    /// An integer column stored as INT32 (the caller checked every value fits).
    I32(Vec<i32>),
}

/// INT32 counts in user_daily (SHOPPING_COMPACT_OUT=1; probes only).
pub fn compact() -> bool {
    std::env::var("SHOPPING_COMPACT_OUT").is_ok_and(|v| v == "1")
}

pub const USER_DAILY: &[&str] = &["user_id", "day", "sessions", "events", "revenue_cents", "top_category"];
pub const USER_DAILY_32: &[&str] = USER_DAILY;
pub const CAMPAIGN_DAILY: &[&str] =
    &["campaign_id", "channel", "day", "sessions", "purchasing_sessions", "revenue_cents", "assisted_sessions"];

// Parquet enum values.
const T_INT32: i32 = 1;
const T_INT64: i32 = 2;
const T_BYTE_ARRAY: i32 = 6;
const REQUIRED: i32 = 0;
const OPTIONAL: i32 = 1;
const CONVERTED_UTF8: i32 = 0;
const CONVERTED_DATE: i32 = 6;
const ENC_PLAIN: i32 = 0;
const ENC_RLE: i32 = 3;

// Thrift compact protocol type ids.
const CT_I32: u8 = 5;
const CT_I64: u8 = 6;
const CT_BINARY: u8 = 8;
const CT_LIST: u8 = 9;
const CT_STRUCT: u8 = 12;

/// A Thrift compact-protocol writer (just what the Parquet footer needs).
struct Thrift<'a> {
    buf: &'a mut Vec<u8>,
    last: Vec<i16>, // the last field id written, per open struct
}

impl<'a> Thrift<'a> {
    fn new(buf: &'a mut Vec<u8>) -> Self {
        Thrift { buf, last: vec![0] }
    }
    fn varint(&mut self, mut v: u64) {
        while v >= 0x80 {
            self.buf.push(v as u8 | 0x80);
            v >>= 7;
        }
        self.buf.push(v as u8);
    }
    fn zigzag(&mut self, v: i64) {
        self.varint(((v << 1) ^ (v >> 63)) as u64);
    }
    fn field(&mut self, id: i16, ty: u8) {
        let last = self.last.last_mut().unwrap();
        let delta = id - *last;
        if (1..=15).contains(&delta) {
            self.buf.push((delta as u8) << 4 | ty);
        } else {
            self.buf.push(ty);
            let z = ((id as i64) << 1) ^ ((id as i64) >> 63);
            *last = id;
            self.varint(z as u64);
            return;
        }
        *last = id;
    }
    fn i32(&mut self, id: i16, v: i32) {
        self.field(id, CT_I32);
        self.zigzag(v as i64);
    }
    fn i64(&mut self, id: i16, v: i64) {
        self.field(id, CT_I64);
        self.zigzag(v);
    }
    fn binary(&mut self, id: i16, b: &[u8]) {
        self.field(id, CT_BINARY);
        self.varint(b.len() as u64);
        self.buf.extend_from_slice(b);
    }
    fn list(&mut self, id: i16, elem: u8, n: usize) {
        self.field(id, CT_LIST);
        if n < 15 {
            self.buf.push((n as u8) << 4 | elem);
        } else {
            self.buf.push(0xF0 | elem);
            self.varint(n as u64);
        }
    }
    /// A struct-typed field: write its fields, then `end`.
    fn begin(&mut self, id: i16) {
        self.field(id, CT_STRUCT);
        self.last.push(0);
    }
    /// A struct as a list element (no field header).
    fn begin_elem(&mut self) {
        self.last.push(0);
    }
    fn end(&mut self) {
        self.buf.push(0); // stop
        self.last.pop();
    }
}

/// One column's physical type, repetition and annotations.
fn kind(c: &Column) -> (i32, i32, Option<i32>) {
    match c {
        Column::Str(_) => (T_BYTE_ARRAY, REQUIRED, Some(CONVERTED_UTF8)),
        Column::OptStr(_) => (T_BYTE_ARRAY, OPTIONAL, Some(CONVERTED_UTF8)),
        Column::Date(_) => (T_INT32, REQUIRED, Some(CONVERTED_DATE)),
        Column::I64(_) => (T_INT64, REQUIRED, None),
        Column::I32(_) => (T_INT32, REQUIRED, None),
    }
}

fn rows(c: &Column) -> usize {
    match c {
        Column::Str(v) => v.len(),
        Column::OptStr(v) => v.len(),
        Column::Date(v) | Column::I32(v) => v.len(),
        Column::I64(v) => v.len(),
    }
}

/// The page body: definition levels (nullable column only), then PLAIN values.
fn page_body(c: &Column, out: &mut Vec<u8>) {
    match c {
        Column::Str(v) => {
            for s in v {
                out.extend_from_slice(&(s.len() as u32).to_le_bytes());
                out.extend_from_slice(s);
            }
        }
        Column::OptStr(v) => {
            // RLE/bit-packed hybrid, bit width 1, as RLE runs only; 4-byte length first.
            let at = out.len();
            out.extend_from_slice(&[0; 4]);
            let mut i = 0;
            while i < v.len() {
                let present = v[i].is_some();
                let mut j = i + 1;
                while j < v.len() && v[j].is_some() == present {
                    j += 1;
                }
                let mut header = ((j - i) as u64) << 1;
                while header >= 0x80 {
                    out.push(header as u8 | 0x80);
                    header >>= 7;
                }
                out.push(header as u8);
                out.push(present as u8);
                i = j;
            }
            let n = (out.len() - at - 4) as u32;
            out[at..at + 4].copy_from_slice(&n.to_le_bytes());
            for s in v.iter().flatten() {
                out.extend_from_slice(&(s.len() as u32).to_le_bytes());
                out.extend_from_slice(s);
            }
        }
        Column::Date(v) | Column::I32(v) => {
            for x in v {
                out.extend_from_slice(&x.to_le_bytes());
            }
        }
        Column::I64(v) => {
            for x in v {
                out.extend_from_slice(&x.to_le_bytes());
            }
        }
    }
}

fn body_size(c: &Column) -> usize {
    match c {
        Column::Str(v) => v.iter().map(|s| 4 + s.len()).sum(),
        Column::OptStr(v) => 64 + v.iter().map(|s| s.map_or(0, |s| 4 + s.len())).sum::<usize>(),
        Column::Date(v) | Column::I32(v) => 4 * v.len(),
        Column::I64(v) => 8 * v.len(),
    }
}

/// Write `columns` (named by `names`, same order) to `<out>/<table>/part-<part>.parquet`.
/// A table may be split over several part files; the checker reads them all.
pub fn write_table(out: &Path, table: &str, part: usize, names: &[&str], columns: Vec<Column>) {
    assert_eq!(names.len(), columns.len());
    let folder = out.join(table);
    create_dir_all(&folder).expect("create output folder");
    let n = columns.first().map_or(0, rows);
    assert!(columns.iter().all(|c| rows(c) == n), "columns of unequal length");

    let mut buf: Vec<u8> = Vec::with_capacity(columns.iter().map(body_size).sum::<usize>() + 4096);
    buf.extend_from_slice(b"PAR1");
    // (data_page_offset, total size incl. header) per column
    let mut chunks: Vec<(i64, i64)> = Vec::with_capacity(columns.len());
    if n > 0 {
        let mut body = Vec::new();
        for c in &columns {
            body.clear();
            page_body(c, &mut body);
            let offset = buf.len() as i64;
            let mut t = Thrift::new(&mut buf);
            t.i32(1, 0); // DATA_PAGE
            t.i32(2, body.len() as i32); // uncompressed size
            t.i32(3, body.len() as i32); // compressed size
            t.begin(5); // DataPageHeader
            t.i32(1, n as i32);
            t.i32(2, ENC_PLAIN);
            t.i32(3, ENC_RLE);
            t.i32(4, ENC_RLE);
            t.end();
            t.end(); // stop of the PageHeader
            buf.extend_from_slice(&body);
            chunks.push((offset, buf.len() as i64 - offset));
        }
    }

    // FileMetaData.
    let footer_at = buf.len();
    {
        let mut t = Thrift::new(&mut buf);
        t.i32(1, 1); // version
        t.list(2, CT_STRUCT, columns.len() + 1); // schema
        t.begin_elem();
        t.binary(4, b"schema");
        t.i32(5, columns.len() as i32);
        t.end();
        for (c, name) in columns.iter().zip(names) {
            let (ty, rep, converted) = kind(c);
            t.begin_elem();
            t.i32(1, ty);
            t.i32(3, rep);
            t.binary(4, name.as_bytes());
            if let Some(conv) = converted {
                t.i32(6, conv);
                t.begin(10); // LogicalType union: STRING (1) or DATE (6)
                t.begin(if conv == CONVERTED_DATE { 6 } else { 1 });
                t.end();
                t.end();
            }
            t.end();
        }
        t.i64(3, n as i64); // num_rows
        t.list(4, CT_STRUCT, if n > 0 { 1 } else { 0 }); // row_groups
        if n > 0 {
            t.begin_elem();
            t.list(1, CT_STRUCT, columns.len()); // columns
            for ((c, name), &(offset, size)) in columns.iter().zip(names).zip(&chunks) {
                let (ty, rep, _) = kind(c);
                t.begin_elem(); // ColumnChunk
                t.i64(2, offset); // file_offset
                t.begin(3); // ColumnMetaData
                t.i32(1, ty);
                if rep == OPTIONAL {
                    t.list(2, CT_I32, 2);
                    t.zigzag(ENC_PLAIN as i64);
                    t.zigzag(ENC_RLE as i64);
                } else {
                    t.list(2, CT_I32, 1);
                    t.zigzag(ENC_PLAIN as i64);
                }
                t.list(3, CT_BINARY, 1); // path_in_schema
                t.varint(name.len() as u64);
                t.buf.extend_from_slice(name.as_bytes());
                t.i32(4, 0); // UNCOMPRESSED
                t.i64(5, n as i64); // num_values
                t.i64(6, size); // total_uncompressed_size
                t.i64(7, size); // total_compressed_size
                t.i64(9, offset); // data_page_offset
                t.end();
                t.end();
            }
            t.i64(2, chunks.iter().map(|c| c.1).sum()); // total_byte_size
            t.i64(3, n as i64); // num_rows
            t.end();
        }
        t.binary(6, b"shopping-sessions native"); // created_by
        t.end(); // stop of FileMetaData
    }
    let footer_len = (buf.len() - footer_at) as u32;
    buf.extend_from_slice(&footer_len.to_le_bytes());
    buf.extend_from_slice(b"PAR1");
    std::fs::write(folder.join(format!("part-{part}.parquet")), &buf).expect("write parquet file");
}
