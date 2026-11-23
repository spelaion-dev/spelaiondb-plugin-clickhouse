//! Reading ClickHouse's `TabSeparatedWithNamesAndTypes` as it arrives.
//!
//! The format writes every value as the server's own text (what `clickhouse-client` shows),
//! escaped so that a tab and a line end only ever separate values and rows: `\t`, `\n`, `\\`
//! and the others below inside a value, `\N` for NULL. Undoing that escape gives back the
//! server's text exactly.

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
    Some(String::from_utf8_lossy(&out).into_owned())
}

/// A value as a `TabSeparated` field for an `INSERT … FORMAT TabSeparated` body: `\N` for NULL,
/// and the characters that would end the field, the row or an escape written as escapes, so no
/// value can make another field or row. [`unescape`] reads it back.
pub fn escape(value: Option<&str>, out: &mut String) {
    let Some(value) = value else {
        out.push_str("\\N");
        return;
    };
    for c in value.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '\t' => out.push_str("\\t"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\0' => out.push_str("\\0"),
            '\u{8}' => out.push_str("\\b"),
            '\u{c}' => out.push_str("\\f"),
            other => out.push(other),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
            let mut field = String::new();
            escape(value, &mut field);
            assert!(!field.contains('\t') && !field.contains('\n'), "{field:?}");
            assert_eq!(fields(field.as_bytes()).count(), 1, "{field:?}");
            assert_eq!(unescape(field.as_bytes()).as_deref(), value, "{field:?}");
        }
    }
}
