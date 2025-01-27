use std::mem::take;

pub fn escape(s: &str, quote: char) -> String {
    let mut result = String::with_capacity(s.len());
    for c in s.chars() {
        if c == quote {
            result.push(quote);
            result.push(c);
        } else {
            result.push(c);
        }
    }

    result
}

/// Convert escape characters into SQL-safe entities.
pub fn unescape(s: &str) -> String {
    let mut result = Vec::new();
    let mut buffer = String::with_capacity(s.len());

    let mut escape = false;
    for c in s.chars() {
        if escape {
            if !buffer.is_empty() {
                result.push(format!("'{}'", take(&mut buffer)));
            }

            escape = false;
            match c {
                'n' => {
                    result.push(r#"E'\n'"#.into());
                }

                't' => {
                    result.push(r#"E'\t'"#.into());
                }

                '\\' => {
                    result.push(r#"E'\\'"#.into());
                }

                '\'' => {
                    result.push(r#"E'\''"#.into());
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
        let s = r#"hello\nworld\n;"#;
        let result = unescape(s);
        assert_eq!(result, r#"'hello' || E'\n' || 'world' || E'\n' || ';'"#);
    }

    #[test]
    fn test_unescape() {
        let s = r#"hello\n\tworld\\"#;
        let result = unescape(s);
        assert_eq!(result, r#"'hello' || E'\n' || E'\t' || 'world' || E'\\'"#)
    }

    #[test]
    fn test_unscape_normal() {
        let s = "hello world";
        let result = unescape(s);
        assert_eq!(result, "'hello world'");
    }

    #[test]
    fn test_escape() {
        let s = r#"hello"drop table x;"#;
        let result = escape(s, '"');
        assert_eq!(result, r#"hello""drop table x;"#);
    }
}
