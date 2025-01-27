use std::mem::take;

/// Convert escape characters into SQL-safe entities.
pub fn unescape(s: &str) -> String {
    let mut result = Vec::new();
    let mut buffer = String::with_capacity(s.len());

    let mut escape = false;
    for c in s.chars() {
        if escape {
            result.push(format!("'{}'", take(&mut buffer)));
            escape = false;
            match c {
                'n' => {
                    result.push(r#"E'\n'"#.into());
                }

                't' => {
                    result.push(r#"E'\t'"#.into());
                }

                '\\' => {
                    result.push(r#"'\\'"#.into());
                }

                _ => {
                    result.push(format!("'{}'", c));
                }
            }
        } else if c == '\\' {
            escape = true;
        } else {
            buffer.push(c);
        }
    }
    if !buffer.is_empty() {
        result.push(format!("'{}'", take(&mut buffer)));
    }
    result.join(" || ")
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_new_line() {
        let s = "hello\\nworld\\n;";
        let result = unescape(s);
        assert_eq!(result, "'hello' || E'\\n' || 'world' || E'\\n' || ';'");
    }

    #[test]
    fn test_unescape() {
        let s = "hello\\n\\tworld\\\\";
        let result = unescape(s);
        assert_eq!(result, r#"'hello' || E'\n" || E'\t' || 'world' || E'\\'"#)
    }

    #[test]
    fn test_unscape_normal() {
        let s = "hello world";
        let result = unescape(s);
        assert_eq!(result, "'hello world'");
    }
}
