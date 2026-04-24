use memchr::memmem;

fn is_edge_whitespace(c: char) -> bool {
    matches!(c, ' ' | '\t' | '\n' | '\r')
}

/// Strip all consecutive leading `/* ... */` block comments, allowing
/// whitespace on either side and between them. Returns `(rest, comment)`
/// where `comment` is a single slice spanning from the first `/*` to the
/// last `*/` (with any inter-comment whitespace included).
pub(super) fn leading_block_comment(q: &str) -> Option<(&str, &str)> {
    let start = "/*";
    let end = "*/";

    let trimmed = q.trim_start_matches(is_edge_whitespace);
    let mut last_end: Option<usize> = None;
    let mut pos = 0;

    loop {
        let remaining = trimmed[pos..].trim_start_matches(is_edge_whitespace);
        if !remaining.starts_with(start) {
            break;
        }
        // Match the nearest `*/` after the opening `/*`; that's the correct
        // (shortest) comment. Searching past the opening `/*` avoids matching
        // a `*/` that overlaps with it, e.g. in `/*/abc*/`.
        let Some(body_end) = memmem::find(&remaining.as_bytes()[start.len()..], end.as_bytes())
        else {
            break;
        };
        let remaining_offset = trimmed.len() - remaining.len();
        let comment_end = remaining_offset + start.len() + body_end + end.len();
        last_end = Some(comment_end);
        pos = comment_end;
    }

    let last_end = last_end?;
    let comment = &trimmed[..last_end];
    let rest = trimmed[last_end..].trim_start_matches(is_edge_whitespace);
    Some((rest, comment))
}

/// Strip all consecutive trailing `/* ... */` block comments, allowing
/// whitespace on either side and between them. Returns `(rest, comment)`
/// where `comment` is a single slice spanning from the first `/*` to the
/// last `*/`.
pub(super) fn trailing_block_comment(q: &str) -> Option<(&str, &str)> {
    let start = "/*";
    let end = "*/";

    let trimmed = q.trim_end_matches(is_edge_whitespace);
    let mut first_start: Option<usize> = None;
    let mut cursor = trimmed.len();

    loop {
        let before = trimmed[..cursor].trim_end_matches(is_edge_whitespace);
        if !before.ends_with(end) {
            break;
        }
        let inner = &before[..before.len() - end.len()];
        let Some(s) = memmem::rfind(inner.as_bytes(), start.as_bytes()) else {
            break;
        };
        // If the body contains `*/`, the trailing `*/` pairs with an earlier
        // `/*` we can't see from here — stop.
        if memmem::find(inner[s + start.len()..].as_bytes(), end.as_bytes()).is_some() {
            break;
        }
        first_start = Some(s);
        cursor = s;
    }

    let start_idx = first_start?;
    let comment = &trimmed[start_idx..];
    let rest = q[..start_idx].trim_end_matches(is_edge_whitespace);
    Some((rest, comment))
}
