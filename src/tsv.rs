//! Reading ClickHouse's `TabSeparatedWithNamesAndTypes` as it arrives.
//!
//! The format writes every value as the server's own text (what `clickhouse-client` shows),
//! escaped so that a tab and a line end only ever separate values and rows: `\t`, `\n`, `\\`
//! and the others below inside a value, `\N` for NULL. Undoing that escape gives back the
//! server's text exactly.

use spelaion_driver_api::Cell;

/// Complete lines out of bytes that arrive in pieces.
#[derive(Default)]
pub struct Lines {
    buf: Vec<u8>,
    start: usize,
}

impl Lines {
    pub fn push(&mut self, bytes: &[u8]) {
        if self.start > 0 && self.start == self.buf.len() {
            self.buf.clear();
            self.start = 0;
        } else if self.start > 64 * 1024 {
            self.buf.drain(..self.start);
            self.start = 0;
        }
        self.buf.extend_from_slice(bytes);
    }

    /// The next complete line, without its `\n`.
    pub fn next_line(&mut self) -> Option<&[u8]> {
        let rest = &self.buf[self.start..];
        let end = rest.iter().position(|&b| b == b'\n')?;
        let line = &self.buf[self.start..self.start + end];
        self.start += end + 1;
        Some(line)
    }

    /// Gives back the last `count` bytes taken, so the next line starts there again.
    pub fn unread(&mut self, count: usize) {
        self.start = self.start.saturating_sub(count);
    }

    /// What arrived after the last complete line.
    pub fn rest(&self) -> &[u8] {
        &self.buf[self.start..]
    }
}

/// The fields of a line.
pub fn fields(line: &[u8]) -> impl Iterator<Item = &[u8]> {
    line.split(|&b| b == b'\t')
}

/// A field's text, `None` for NULL. Text that is not UTF-8 (ClickHouse's `String` holds any
/// bytes) has each invalid sequence replaced with U+FFFD.
pub fn unescape(field: &[u8]) -> Option<String> {
    if field == b"\\N" {
        return None;
    }
    if !field.contains(&b'\\') {
        return Some(String::from_utf8_lossy(field).into_owned());
    }
    unescape_bytes(field).map(|out| String::from_utf8_lossy(&out).into_owned())
}

/// A field's bytes, `None` for NULL: what [`escape_bytes`] wrote.
pub fn unescape_bytes(field: &[u8]) -> Option<Vec<u8>> {
    if field == b"\\N" {
        return None;
    }
    let mut out = Vec::with_capacity(field.len());
    let mut bytes = field.iter().copied();
    while let Some(b) = bytes.next() {
        if b != b'\\' {
            out.push(b);
            continue;
        }
        match bytes.next() {
            Some(b'b') => out.push(0x08),
            Some(b'f') => out.push(0x0c),
            Some(b'r') => out.push(b'\r'),
            Some(b'n') => out.push(b'\n'),
            Some(b't') => out.push(b'\t'),
            Some(b'0') => out.push(0),
            Some(b'a') => out.push(0x07),
            Some(b'v') => out.push(0x0b),
            Some(b'e') => out.push(0x1b),
            // `\\`, `\'`, `\"` and anything else: the character itself.
            Some(other) => out.push(other),
            None => out.push(b'\\'),
        }
    }
    Some(out)
}

/// A value as a `TabSeparated` field for an `INSERT … FORMAT TabSeparated` body: `\N` for NULL,
/// and the characters that would end the field, the row or an escape written as escapes, so no
/// value can make another field or row. [`unescape`] reads it back.
pub fn escape(value: Option<&str>, out: &mut Vec<u8>) {
    match value {
        None => out.extend_from_slice(b"\\N"),
        Some(text) => escape_bytes(text.as_bytes(), out),
    }
}

/// Bytes as a `TabSeparated` field (ClickHouse's `String` holds any bytes, SDB-464): each byte
/// that would end the field, the row or an escape written as an escape, every other as it is.
/// Text is escaped the same way, byte by byte: every escaped character is ASCII, and no byte of
/// another UTF-8 character is one of them. [`unescape_bytes`] reads it back.
pub fn escape_bytes(value: &[u8], out: &mut Vec<u8>) {
    for &byte in value {
        match byte {
            b'\\' => out.extend_from_slice(b"\\\\"),
            b'\t' => out.extend_from_slice(b"\\t"),
            b'\n' => out.extend_from_slice(b"\\n"),
            b'\r' => out.extend_from_slice(b"\\r"),
            0 => out.extend_from_slice(b"\\0"),
            0x08 => out.extend_from_slice(b"\\b"),
            0x0c => out.extend_from_slice(b"\\f"),
            other => out.push(other),
        }
    }
}

/// A row as a line of a `TabSeparated` body: its values escaped ([`escape`], [`escape_bytes`]),
/// a tab between them and a line end after the last.
pub fn push_row<'a>(values: impl Iterator<Item = Option<Cell<'a>>>, out: &mut Vec<u8>) {
    for (i, value) in values.enumerate() {
        if i > 0 {
            out.push(b'\t');
        }
        match value {
            None => out.extend_from_slice(b"\\N"),
            Some(Cell::Text(text)) => escape_bytes(text.as_bytes(), out),
            Some(Cell::Bytes(bytes)) => escape_bytes(bytes, out),
        }
    }
    out.push(b'\n');
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    type Row = Vec<Option<String>>;

    /// `rows` as the body of an insert.
    fn body(rows: &[Row]) -> Vec<u8> {
        let mut out = Vec::new();
        for row in rows {
            push_row(row.iter().map(|v| v.as_deref().map(Cell::Text)), &mut out);
        }
        out
    }

    /// A body read back as the server reads it: a row per line, a value per field.
    fn read_back(body: &[u8]) -> Vec<Row> {
        let mut lines = Lines::default();
        lines.push(body);
        let mut rows = Vec::new();
        while let Some(line) = lines.next_line() {
            rows.push(fields(line).map(unescape).collect());
        }
        assert!(lines.rest().is_empty(), "a row without its line end");
        rows
    }

    /// Text with every character that means something in the format, and any other.
    fn text() -> impl Strategy<Value = String> {
        let special = prop::sample::select(vec![
            '\\', '\t', '\n', '\r', '\0', '\u{7}', '\u{8}', '\u{b}', '\u{c}', '\u{1b}', 'N', 't',
            'n', '0', '\'', '"',
        ]);
        prop::collection::vec(prop_oneof![3 => special, 1 => any::<char>()], 0..12)
            .prop_map(|chars| chars.into_iter().collect())
            .boxed()
            .prop_union(any::<String>().boxed())
    }

    fn rows() -> impl Strategy<Value = Vec<Row>> {
        (1usize..6).prop_flat_map(|width| {
            prop::collection::vec(prop::collection::vec(prop::option::of(text()), width), 0..8)
        })
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(2048))]

        /// Whatever the values, the body reads back as the same rows: as many rows, as many
        /// values in each, each value the same text or NULL.
        #[test]
        fn any_rows_read_back_as_written(rows in rows()) {
            let back = read_back(&body(&rows));
            prop_assert_eq!(back, rows);
        }

        /// Whatever the bytes (SDB-464), they stay one field of one line and read back as the
        /// same bytes.
        #[test]
        fn any_bytes_read_back_as_written(
            values in prop::collection::vec(prop::option::of(any::<Vec<u8>>()), 1..6)
        ) {
            let mut line = Vec::new();
            push_row(values.iter().map(|v| v.as_deref().map(Cell::Bytes)), &mut line);
            let mut lines = Lines::default();
            lines.push(&line);
            let row = lines.next_line().expect("one line");
            let back: Vec<Option<Vec<u8>>> = fields(row).map(unescape_bytes).collect();
            prop_assert_eq!(back, values);
            prop_assert!(lines.next_line().is_none());
        }
    }

    #[test]
    fn values_that_look_like_the_format_stay_values() {
        let rows: Vec<Row> = vec![
            // A value ending in a backslash, before a tab and before the line end.
            vec![Some("ends\\".into()), Some("x\\".into()), Some("\\".into())],
            // NULL, and the text `\N`, and one that ends in a backslash before an N.
            vec![None, Some("\\N".into()), Some("\\\\N".into())],
            // Empty values, and a lone line end or tab.
            vec![Some(String::new()), Some("\n".into()), Some("\t".into())],
            // Escapes written out as text.
            vec![
                Some("\\t\\n".into()),
                Some("\\0".into()),
                Some("a\r\nb".into()),
            ],
        ];
        assert_eq!(read_back(&body(&rows)), rows);
        // A single empty value is an empty line, not no row.
        let one: Vec<Row> = vec![vec![Some(String::new())], vec![None]];
        assert_eq!(body(&one), b"\n\\N\n");
        assert_eq!(read_back(&body(&one)), one);
    }

    #[test]
    fn lines_come_out_whole_whatever_the_pieces() {
        let mut lines = Lines::default();
        lines.push(b"a\tb\nc");
        assert_eq!(lines.next_line(), Some(&b"a\tb"[..]));
        assert_eq!(lines.next_line(), None);
        lines.push(b"d\n\ne");
        assert_eq!(lines.next_line(), Some(&b"cd"[..]));
        assert_eq!(lines.next_line(), Some(&b""[..]));
        assert_eq!(lines.next_line(), None);
        assert_eq!(lines.rest(), b"e");
    }

    #[test]
    fn escapes_come_undone() {
        assert_eq!(unescape(b"\\N"), None);
        assert_eq!(unescape(b"\\\\N").as_deref(), Some("\\N"));
        assert_eq!(unescape(b"plain").as_deref(), Some("plain"));
        assert_eq!(
            unescape(b"a\\tb\\nc\\\\d\\'e\\0f").as_deref(),
            Some("a\tb\nc\\d'e\0f")
        );
        assert_eq!(unescape(b"['a\\\\'b']").as_deref(), Some("['a\\'b']"));
        assert_eq!(unescape(b"").as_deref(), Some(""));
        assert_eq!(unescape(b"\xff").as_deref(), Some("\u{fffd}"));
    }

    #[test]
    fn fields_split_on_tabs_only() {
        let got: Vec<&[u8]> = fields(b"1\t\\N\t").collect();
        assert_eq!(got, [&b"1"[..], b"\\N", b""]);
    }

    #[test]
    fn escaped_values_stay_one_field_and_read_back() {
        for value in [
            None,
            Some(""),
            Some("plain"),
            Some("\\N"),
            Some("tab\there"),
            Some("two\nlines\r\n"),
            Some("back\\slash \\t not a tab"),
            Some("nul\0 bell\u{7} bs\u{8} ff\u{c}"),
            Some("ñandú 字 🦀 'quoted' \"too\""),
        ] {
            let mut field = Vec::new();
            escape(value, &mut field);
            assert!(
                !field.contains(&b'\t') && !field.contains(&b'\n'),
                "{field:?}"
            );
            assert_eq!(fields(&field).count(), 1, "{field:?}");
            assert_eq!(unescape(&field).as_deref(), value, "{field:?}");
        }
    }
}
