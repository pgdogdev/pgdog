use memchr::memmem;

fn is_edge_whitespace(c: char) -> bool {
    matches!(c, ' ' | '\t' | '\n' | '\r')
}

/// Strip a leading `/* ... */` block comment, allowing whitespace on either
/// side. Returns `(rest, comment)`.
pub(super) fn leading_block_comment(q: &str) -> Option<(&str, &str)> {
    let trimmed = q.trim_start_matches(is_edge_whitespace);
    if !trimmed.starts_with("/*") {
        return None;
    }
    // Match the nearest `*/` after the opening `/*`; that's the correct
    // (shortest) comment. Searching from byte 2 avoids matching a `*/` that
    // overlaps with the opening `/*`, e.g. in `/*/abc*/`.
    let body_end = memmem::find(&trimmed.as_bytes()[2..], b"*/")?;
    let comment_end = 2 + body_end + 2;
    let comment = &trimmed[..comment_end];
    let rest = trimmed[comment_end..].trim_start_matches(is_edge_whitespace);
    Some((rest, comment))
}

/// Strip a trailing `/* ... */` block comment, allowing whitespace on either
/// side. Returns `(rest, comment)`.
pub(super) fn trailing_block_comment(q: &str) -> Option<(&str, &str)> {
    let trimmed = q.trim_end_matches(is_edge_whitespace);
    if !trimmed.ends_with("*/") {
        return None;
    }
    let inner = &trimmed[..trimmed.len() - 2];
    let start = memmem::rfind(inner.as_bytes(), b"/*")?;
    // If the body contains `*/`, the trailing `*/` pairs with an earlier
    // `/*` we can't see from here — reject.
    if memmem::find(inner[start + 2..].as_bytes(), b"*/").is_some() {
        return None;
    }
    let comment = &trimmed[start..];
    let rest = q[..start].trim_end_matches(is_edge_whitespace);
    Some((rest, comment))
}
