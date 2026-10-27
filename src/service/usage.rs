use axum::http::{HeaderMap, header};
use serde::Deserialize;

use crate::store::Usage;

const MAX_JSON_BYTES: usize = 1024 * 1024;
const MAX_SSE_BYTES: usize = 64 * 1024;

#[derive(Clone, Copy)]
pub(super) enum UsageKind {
    Chat,
    Embeddings,
    Models,
}

pub(super) struct UsageTracker {
    parser: Parser,
    usage_kind: UsageKind,
    usage: Option<Usage>,
    usage_final: bool,
}

enum Parser {
    Disabled,
    Json(Vec<u8>),
    Sse(SseParser),
}

impl UsageTracker {
    pub(super) fn new(headers: &HeaderMap, usage_kind: UsageKind) -> Self {
        if matches!(usage_kind, UsageKind::Models) {
            return Self {
                parser: Parser::Disabled,
                usage_kind,
                usage: Some(Usage {
                    prompt_tokens: 0,
                    completion_tokens: 0,
                    total_tokens: 0,
                }),
                usage_final: true,
            };
        }
        let identity_encoding = headers
            .get_all(header::CONTENT_ENCODING)
            .iter()
            .all(|value| {
                value.to_str().is_ok_and(|value| {
                    value
                        .split(',')
                        .all(|encoding| encoding.trim().eq_ignore_ascii_case("identity"))
                })
            });
        let mut content_types = headers.get_all(header::CONTENT_TYPE).iter();
        let content_type = content_types.next().and_then(|value| value.to_str().ok());
        let parser = if !identity_encoding || content_types.next().is_some() {
            Parser::Disabled
        } else {
            match content_type.map(|value| value.split(';').next().unwrap_or("").trim()) {
                Some(value) if value.eq_ignore_ascii_case("text/event-stream") => {
                    Parser::Sse(SseParser::new(usage_kind))
                }
                Some(value) if is_json_media_type(value) => Parser::Json(Vec::new()),
                _ => Parser::Disabled,
            }
        };
        Self {
            parser,
            usage_kind,
            usage: None,
            usage_final: false,
        }
    }

    pub(super) fn feed(&mut self, bytes: &[u8]) {
        match &mut self.parser {
            Parser::Disabled => {}
            Parser::Json(buffer) => {
                if bytes.len() > MAX_JSON_BYTES - buffer.len() {
                    self.parser = Parser::Disabled;
                } else {
                    buffer.extend_from_slice(bytes);
                }
            }
            Parser::Sse(parser) => parser.feed(bytes, &mut self.usage),
        }
    }

    pub(super) fn finish(&mut self) -> Option<Usage> {
        match std::mem::replace(&mut self.parser, Parser::Disabled) {
            Parser::Json(buffer) => {
                self.usage = parse_usage(&buffer, self.usage_kind);
                self.usage_final = self.usage.is_some();
            }
            Parser::Sse(parser) => {
                // EOF cannot promote an earlier snapshot if later usage was
                // malformed, discarded, or left in an incomplete event.
                self.usage_final = self.usage.is_some()
                    && parser.final_report
                    && parser.line_len == 0
                    && parser.event_len == 0
                    && !parser.discard_event;
            }
            Parser::Disabled => {}
        }
        self.usage
    }

    pub(super) fn observed(&self) -> Option<Usage> {
        self.usage
    }

    /// Meaningful only after clean upstream EOF and `finish()`.
    pub(super) fn is_final(&self) -> bool {
        self.usage_final
    }
}

fn is_json_media_type(value: &str) -> bool {
    let Some((kind, subtype)) = value.split_once('/') else {
        return false;
    };
    kind.eq_ignore_ascii_case("application")
        && !subtype.is_empty()
        && subtype
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&byte))
        && (subtype.eq_ignore_ascii_case("json")
            || (subtype.len() > 5 && subtype[subtype.len() - 5..].eq_ignore_ascii_case("+json")))
}

fn parse_usage(bytes: &[u8], usage_kind: UsageKind) -> Option<Usage> {
    #[derive(Deserialize)]
    struct Envelope<'a> {
        #[serde(borrow)]
        usage: Option<&'a serde_json::value::RawValue>,
    }
    #[derive(Deserialize)]
    struct Report {
        prompt_tokens: i64,
        completion_tokens: i64,
        total_tokens: i64,
    }
    #[derive(Deserialize)]
    struct EmbeddingReport {
        prompt_tokens: i64,
        #[serde(default)]
        completion_tokens: i64,
        total_tokens: i64,
    }
    // Ignore completion text and provider metadata without allocating a Value
    // tree for every streamed event.
    if bytes.iter().find(|byte| !byte.is_ascii_whitespace()) != Some(&b'{') {
        return None;
    }
    let raw = serde_json::from_slice::<Envelope<'_>>(bytes).ok()?.usage?;
    if !raw.get().starts_with('{') {
        return None;
    }
    let report = match usage_kind {
        UsageKind::Chat => serde_json::from_str::<Report>(raw.get()).ok()?,
        UsageKind::Embeddings => {
            let report = serde_json::from_str::<EmbeddingReport>(raw.get()).ok()?;
            if report.completion_tokens != 0 {
                return None;
            }
            Report {
                prompt_tokens: report.prompt_tokens,
                completion_tokens: 0,
                total_tokens: report.total_tokens,
            }
        }
        UsageKind::Models => return None,
    };
    let Report {
        prompt_tokens,
        completion_tokens,
        total_tokens,
    } = report;
    if prompt_tokens < 0 || completion_tokens < 0 || total_tokens < 0 {
        return None;
    }
    if prompt_tokens.checked_add(completion_tokens)? != total_tokens {
        return None;
    }
    Some(Usage {
        prompt_tokens,
        completion_tokens,
        total_tokens,
    })
}

struct SseParser {
    usage_kind: UsageKind,
    line: Vec<u8>,
    data: Vec<u8>,
    line_len: usize,
    event_len: usize,
    discard_event: bool,
    skip_lf: bool,
    preamble: usize,
    final_report: bool,
}

impl SseParser {
    fn new(usage_kind: UsageKind) -> Self {
        Self {
            usage_kind,
            line: Vec::new(),
            data: Vec::new(),
            line_len: 0,
            event_len: 0,
            discard_event: false,
            skip_lf: false,
            preamble: 0,
            final_report: false,
        }
    }

    fn feed(&mut self, bytes: &[u8], usage: &mut Option<Usage>) {
        const BOM: [u8; 3] = [0xef, 0xbb, 0xbf];
        for &byte in bytes {
            // SSE permits a single UTF-8 BOM at the beginning of the stream.
            if self.preamble < BOM.len() {
                if byte == BOM[self.preamble] {
                    self.preamble += 1;
                    continue;
                }
                let buffered = self.preamble;
                self.preamble = BOM.len();
                for &prefix in &BOM[..buffered] {
                    self.push(prefix, usage);
                }
            }
            self.push(byte, usage);
        }
    }

    fn push(&mut self, byte: u8, usage: &mut Option<Usage>) {
        if self.skip_lf {
            self.skip_lf = false;
            if byte == b'\n' {
                return;
            }
        }
        match byte {
            b'\r' | b'\n' => {
                self.end_line(usage);
                self.skip_lf = byte == b'\r';
            }
            _ => {
                self.line_len = (self.line_len + 1).min(MAX_SSE_BYTES + 1);
                self.event_len = (self.event_len + 1).min(MAX_SSE_BYTES + 1);
                if self.line_len > MAX_SSE_BYTES || self.event_len > MAX_SSE_BYTES {
                    self.discard();
                }
                if !self.discard_event {
                    self.line.push(byte);
                }
            }
        }
    }

    fn end_line(&mut self, usage: &mut Option<Usage>) {
        if self.line_len == 0 {
            if !self.discard_event {
                if let Some(report) = parse_usage(&self.data, self.usage_kind) {
                    // Reports are cumulative snapshots, not event increments.
                    *usage = Some(report);
                    self.final_report = true;
                } else if !self.data.is_empty() && self.data != b"[DONE]\n" {
                    self.final_report = false;
                }
            }
            self.line.clear();
            self.data.clear();
            self.event_len = 0;
            self.discard_event = false;
            return;
        }

        self.event_len = (self.event_len + 1).min(MAX_SSE_BYTES + 1);
        if self.event_len > MAX_SSE_BYTES {
            self.discard();
        }
        if !self.discard_event {
            let colon = self.line.iter().position(|&byte| byte == b':');
            let (field, value) = match colon {
                Some(index) => {
                    let value = &self.line[index + 1..];
                    (
                        &self.line[..index],
                        value.strip_prefix(b" ").unwrap_or(value),
                    )
                }
                None => (self.line.as_slice(), &[][..]),
            };
            if field == b"data" {
                self.data.extend_from_slice(value);
                self.data.push(b'\n');
            }
        }
        self.line.clear();
        self.line_len = 0;
    }

    fn discard(&mut self) {
        self.discard_event = true;
        self.final_report = false;
        self.line.clear();
        self.data.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const REPORT: &[u8] =
        br#"{"usage":{"prompt_tokens":3,"completion_tokens":2,"total_tokens":5}}"#;

    fn tracker(content_type: &str) -> UsageTracker {
        let mut headers = HeaderMap::new();
        headers.insert(header::CONTENT_TYPE, content_type.parse().unwrap());
        UsageTracker::new(&headers, UsageKind::Chat)
    }

    fn expected(prompt_tokens: i64, completion_tokens: i64) -> Option<Usage> {
        Some(Usage {
            prompt_tokens,
            completion_tokens,
            total_tokens: prompt_tokens + completion_tokens,
        })
    }

    fn event(tracker: &mut UsageTracker, report: &[u8]) {
        tracker.feed(b"data: ");
        tracker.feed(report);
        tracker.feed(b"\n\n");
    }
    #[test]
    fn embeddings_allow_only_zero_or_absent_completion_tokens() {
        for (bytes, prompt) in [
            (
                br#"{"usage":{"prompt_tokens":3,"total_tokens":3}}"#.as_slice(),
                3,
            ),
            (
                br#"{"usage":{"prompt_tokens":3,"completion_tokens":0,"total_tokens":3}}"#
                    .as_slice(),
                3,
            ),
            (
                br#"{"usage":{"prompt_tokens":0,"total_tokens":0}}"#.as_slice(),
                0,
            ),
        ] {
            for content_type in ["application/json", "text/event-stream"] {
                let mut headers = HeaderMap::new();
                headers.insert(header::CONTENT_TYPE, content_type.parse().unwrap());
                let mut parser = UsageTracker::new(&headers, UsageKind::Embeddings);
                if content_type == "text/event-stream" {
                    event(&mut parser, bytes);
                } else {
                    parser.feed(bytes);
                }
                assert_eq!(parser.finish(), expected(prompt, 0));
                assert!(parser.is_final());
            }
        }
        assert_eq!(
            parse_usage(
                br#"{"usage":{"prompt_tokens":3,"total_tokens":3}}"#,
                UsageKind::Chat,
            ),
            None,
            "chat still requires completion_tokens",
        );
        assert_eq!(parse_usage(REPORT, UsageKind::Chat), expected(3, 2));
    }

    #[test]
    fn malformed_embeddings_usage_remains_unknown() {
        for bytes in [
            REPORT,
            br#"{"usage":{"prompt_tokens":3,"completion_tokens":-1,"total_tokens":2}}"#,
            br#"{"usage":{"prompt_tokens":3,"completion_tokens":null,"total_tokens":3}}"#,
            br#"{"usage":{"prompt_tokens":3,"completion_tokens":0.0,"total_tokens":3}}"#,
            br#"{"usage":{"prompt_tokens":3,"total_tokens":4}}"#,
            br#"{"usage":{"prompt_tokens":-3,"total_tokens":-3}}"#,
            br#"{"usage":{"prompt_tokens":3}}"#,
            br#"{"usage":{"total_tokens":3}}"#,
            br#"{"usage":{"prompt_tokens":9223372036854775808,"total_tokens":9223372036854775808}}"#,
            b"invalid",
        ] {
            for content_type in ["application/json", "text/event-stream"] {
                let mut headers = HeaderMap::new();
                headers.insert(header::CONTENT_TYPE, content_type.parse().unwrap());
                let mut parser = UsageTracker::new(&headers, UsageKind::Embeddings);
                if content_type == "text/event-stream" {
                    event(&mut parser, bytes);
                } else {
                    parser.feed(bytes);
                }
                assert_eq!(parser.observed(), None);
                assert_eq!(parser.finish(), None);
                assert!(!parser.is_final());
            }
        }
    }

    #[test]
    fn models_ignore_headers_bodies_and_upstream_usage() {
        for content_type in ["application/json", "text/event-stream", "text/plain"] {
            for encoding in ["identity", "gzip"] {
                let mut headers = HeaderMap::new();
                headers.insert(header::CONTENT_TYPE, content_type.parse().unwrap());
                headers.insert(header::CONTENT_ENCODING, encoding.parse().unwrap());
                let mut parser = UsageTracker::new(&headers, UsageKind::Models);
                assert_eq!(parser.observed(), expected(0, 0));
                assert!(parser.is_final());
                parser.feed(REPORT);
                event(&mut parser, REPORT);
                parser.feed(b"malformed");
                assert_eq!(parser.observed(), expected(0, 0));
                assert_eq!(parser.finish(), expected(0, 0));
                assert!(parser.is_final());
            }
        }
    }

    #[test]
    fn only_final_usage_refunds_the_uncertain_stream_remainder() {
        for (tail, final_report) in [
            (b"".as_slice(), true),
            (b"data: [DONE]\n\n".as_slice(), true),
            (b": keepalive\n\n".as_slice(), true),
            (b"data: {\"usage\":null}\n\n".as_slice(), false),
            (b"data: {\"choices\":[{}]}\n\n".as_slice(), false),
            (b"data: malformed\n\n".as_slice(), false),
            (b"data: {\"usage\":{\"prompt_tokens\":3,\"completion_tokens\":7,\"total_tokens\":10}}\n".as_slice(), false),
        ] {
            let mut parser = tracker("text/event-stream");
            event(&mut parser, REPORT);
            parser.feed(tail);
            assert_eq!(parser.observed(), expected(3, 2));
            assert!(!parser.is_final(), "no final report before clean EOF");
            assert_eq!(parser.finish(), expected(3, 2));
            assert_eq!(parser.is_final(), final_report, "{tail:?}");
        }

        let mut parser = tracker("text/event-stream");
        event(&mut parser, REPORT);
        parser.feed(b"data: ");
        parser.feed(&vec![b'x'; MAX_SSE_BYTES + 1]);
        parser.feed(b"\n\n");
        assert_eq!(parser.finish(), expected(3, 2));
        assert!(
            !parser.is_final(),
            "discarded later usage cannot refund escrow"
        );

        let mut parser = tracker("text/event-stream");
        event(&mut parser, REPORT);
        event(&mut parser, b"invalid");
        event(
            &mut parser,
            br#"{"usage":{"prompt_tokens":3,"completion_tokens":7,"total_tokens":10}}"#,
        );
        assert_eq!(parser.finish(), expected(3, 7));
        assert!(
            parser.is_final(),
            "a later final cumulative report reconciles uncertainty"
        );
    }

    #[test]
    fn json_requires_clean_eof_and_preserves_zero() {
        let mut parser = tracker("application/json; charset=utf-8");
        for byte in REPORT {
            parser.feed(std::slice::from_ref(byte));
            assert_eq!(parser.observed(), None);
        }
        assert_eq!(parser.finish(), expected(3, 2));
        assert_eq!(parser.finish(), expected(3, 2));

        let mut zero = tracker("Application/Problem+JSON");
        zero.feed(br#"{"usage":{"prompt_tokens":0,"completion_tokens":0,"total_tokens":0}}"#);
        assert_eq!(zero.finish(), expected(0, 0));
    }

    #[test]
    fn truncated_or_malformed_json_is_unknown() {
        for bytes in [
            &REPORT[..REPORT.len() - 1],
            b"not json".as_slice(),
            b"{}".as_slice(),
            b"{\"usage\":null}".as_slice(),
            br#"{"usage":[3,2,5]}"#.as_slice(),
            br#"[{"prompt_tokens":3,"completion_tokens":2,"total_tokens":5}]"#.as_slice(),
        ] {
            let mut parser = tracker("application/json");
            parser.feed(bytes);
            assert_eq!(parser.observed(), None);
            assert_eq!(parser.finish(), None);
        }
    }

    #[test]
    fn sse_handles_fragmented_bom_multiline_and_line_endings() {
        let mut parser = tracker("text/event-stream; charset=utf-8");
        let wire = b"\xef\xbb\xbf: comment\r\nevent: message\r\ndata: {\r\ndata: \"usage\": {\"prompt_tokens\":3,\"completion_tokens\":2,\"total_tokens\":5}}\r\n\r\n";
        for byte in &wire[..wire.len() - 2] {
            parser.feed(std::slice::from_ref(byte));
        }
        assert_eq!(parser.observed(), None);
        for byte in &wire[wire.len() - 2..] {
            parser.feed(std::slice::from_ref(byte));
        }
        assert_eq!(parser.observed(), expected(3, 2));
        parser.feed(b"data: {\"usage\": {\"prompt_tokens\":4,\"completion_tokens\":2,\"total_tokens\":6}}\r\r");
        assert_eq!(parser.observed(), expected(4, 2));
        assert_eq!(parser.finish(), expected(4, 2));
    }

    #[test]
    fn cumulative_reports_are_not_added_and_last_valid_wins() {
        let mut parser = tracker("text/event-stream");
        event(&mut parser, REPORT);
        event(&mut parser, REPORT);
        assert_eq!(parser.observed(), expected(3, 2));
        event(
            &mut parser,
            br#"{"usage":{"prompt_tokens":3,"completion_tokens":7,"total_tokens":10}}"#,
        );
        event(&mut parser, br#"{"usage":null}"#);
        event(&mut parser, b"[DONE]");
        assert_eq!(parser.finish(), expected(3, 7));
    }

    #[test]
    fn sse_does_not_dispatch_unterminated_events_at_eof() {
        for suffix in [b"".as_slice(), b"\n", b"\r\n"] {
            let mut parser = tracker("text/event-stream");
            parser.feed(b"data: ");
            parser.feed(REPORT);
            parser.feed(suffix);
            assert_eq!(parser.observed(), None);
            assert_eq!(parser.finish(), None);
        }
        let mut parser = tracker("text/event-stream");
        event(&mut parser, REPORT);
        parser.feed(b"data: {\"usage\":");
        assert_eq!(parser.observed(), expected(3, 2));
        assert_eq!(parser.finish(), expected(3, 2));
    }

    #[test]
    fn invalid_usage_is_ignored_without_losing_valid_report() {
        let invalid = [
            br#"{"usage":{"prompt_tokens":-1,"completion_tokens":2,"total_tokens":1}}"#.as_slice(),
            br#"{"usage":{"prompt_tokens":3.0,"completion_tokens":2,"total_tokens":5}}"#,
            br#"{"usage":{"prompt_tokens":3,"completion_tokens":2,"total_tokens":6}}"#,
            br#"{"usage":{"prompt_tokens":3,"completion_tokens":2}}"#,
            br#"{"usage":{"prompt_tokens":9223372036854775807,"completion_tokens":1,"total_tokens":0}}"#,
            br#"{"usage":{"prompt_tokens":9223372036854775808,"completion_tokens":0,"total_tokens":9223372036854775808}}"#,
            br#"{"usage":{"prompt_tokens":"3","completion_tokens":2,"total_tokens":5}}"#,
            br#"{"usage":[3,2,5]}"#,
            br#"[{"prompt_tokens":3,"completion_tokens":2,"total_tokens":5}]"#,
            b"broken json",
        ];
        let mut parser = tracker("text/event-stream");
        for report in invalid {
            event(&mut parser, report);
            assert_eq!(parser.observed(), None);
        }
        event(&mut parser, REPORT);
        assert_eq!(parser.observed(), expected(3, 2));
        for report in invalid {
            event(&mut parser, report);
            assert_eq!(parser.observed(), expected(3, 2));
        }
        assert_eq!(parser.finish(), expected(3, 2));
    }

    #[test]
    fn oversized_line_discards_whole_event_then_recovers() {
        let mut parser = tracker("text/event-stream");
        parser.feed(b"data: ");
        parser.feed(&vec![b'x'; MAX_SSE_BYTES + 1]);
        // A valid data line in the oversized event must not rescue it.
        parser.feed(b"\ndata: ");
        parser.feed(REPORT);
        parser.feed(b"\n\n");
        assert_eq!(parser.observed(), None);
        event(&mut parser, b"bad json");
        event(&mut parser, REPORT);
        assert_eq!(parser.finish(), expected(3, 2));
    }

    #[test]
    fn oversized_event_with_short_lines_recovers_at_blank_line() {
        let mut parser = tracker("text/event-stream");
        parser.feed(b"data: ");
        parser.feed(REPORT);
        parser.feed(b"\n");
        let comment = vec![b':'; 1023];
        for _ in 0..65 {
            parser.feed(&comment);
            parser.feed(b"\n");
        }
        parser.feed(b"\n");
        assert_eq!(parser.observed(), None);
        event(&mut parser, REPORT);
        assert_eq!(parser.finish(), expected(3, 2));
    }

    #[test]
    fn oversized_json_is_unknown_even_with_valid_prefix() {
        let mut parser = tracker("application/json");
        parser.feed(REPORT);
        parser.feed(&vec![b' '; MAX_JSON_BYTES]);
        assert_eq!(parser.observed(), None);
        assert_eq!(parser.finish(), None);

        let mut at_limit = tracker("application/json");
        at_limit.feed(REPORT);
        at_limit.feed(&vec![b' '; MAX_JSON_BYTES - REPORT.len()]);
        assert_eq!(at_limit.finish(), expected(3, 2));
    }

    #[test]
    fn unsupported_or_encoded_bodies_are_unknown() {
        for content_type in [
            "text/plain",
            "application/+json",
            "application/json/extra",
            "",
        ] {
            let mut parser = tracker(content_type);
            parser.feed(REPORT);
            assert_eq!(parser.finish(), None);
        }
        for encoding in ["gzip", "br", "identity, gzip", ""] {
            let mut headers = HeaderMap::new();
            headers.insert(header::CONTENT_TYPE, "application/json".parse().unwrap());
            headers.insert(header::CONTENT_ENCODING, encoding.parse().unwrap());
            let mut parser = UsageTracker::new(&headers, UsageKind::Chat);
            parser.feed(REPORT);
            assert_eq!(parser.finish(), None);
        }
        let mut headers = HeaderMap::new();
        headers.insert(header::CONTENT_TYPE, "application/json".parse().unwrap());
        headers.insert(header::CONTENT_ENCODING, "Identity".parse().unwrap());
        let mut parser = UsageTracker::new(&headers, UsageKind::Chat);
        parser.feed(REPORT);
        assert_eq!(parser.finish(), expected(3, 2));
    }
}
