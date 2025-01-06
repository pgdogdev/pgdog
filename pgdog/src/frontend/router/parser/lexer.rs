//! SQL lexer.

use std::mem::take;

use super::{Error, Token};

/// Lexer.
pub struct Lexer<'a> {
    sql: &'a str,
    string: bool,
    entity: bool,
}

impl<'a> Lexer<'a> {
    pub fn new(sql: &'a str) -> Self {
        Self {
            sql,
            string: false,
            entity: false,
        }
    }

    pub fn lex(mut self) -> Result<Vec<Token>, Error> {
        let mut tokens = vec![];
        let mut buffer = String::new();
        let mut iter = self.sql.chars().peekable();

        while let Some(c) = iter.next() {
            match c {
                '\'' => {
                    if !self.string {
                        self.string = true;
                        continue;
                    }

                    let next = iter.peek();

                    match next {
                        // Escape
                        Some('\'') => {
                            let _ = iter.next();
                            buffer.push('\'');
                        }

                        Some(' ') | Some(',') | Some(';') | None => {
                            self.string = false;
                            let _ = iter.next();
                            tokens.push(Token::String(take(&mut buffer)));
                        }

                        _ => continue,
                    }
                }

                '"' => {
                    if !self.entity {
                        self.entity = true;
                        continue;
                    }

                    let next = iter.peek();

                    match next {
                        // Escape
                        Some('"') => {
                            let _ = iter.next();
                            buffer.push('"');
                        }

                        Some(' ') | Some(',') | Some(';') | None => {
                            self.entity = false;
                            let _ = iter.next();
                            tokens.push(Token::Entity(take(&mut buffer)));
                        }

                        _ => continue,
                    }
                }

                ' ' | ';' | ',' => {
                    if self.entity || self.string {
                        buffer.push(c);
                    } else {
                        let token = match buffer.as_str() {
                            "select" => Token::Select,
                            "with" => Token::With,
                            "from" => Token::From,
                            "*" => Token::Star,
                            "" => continue,
                            _ => Token::Entity(take(&mut buffer)),
                        };
                        tokens.push(token);
                        let token = match c {
                            ' ' => Token::Space,
                            ',' => Token::Comma,
                            ';' => Token::End,
                            _ => unreachable!(),
                        };
                        tokens.push(token);
                        buffer.clear();
                    }
                }

                _ => {
                    if self.string || self.entity {
                        buffer.push(c);
                    } else {
                        buffer.push(c.to_ascii_lowercase());
                    }
                }
            }
        }

        Ok(tokens)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    #[ignore]
    fn test_basic() {
        let sql = r#"select a,b, cross_apple,"column a,!" from test;"#;
        let lexer = Lexer::new(sql);
        let tokens = lexer.lex().unwrap();
        panic!("{:?}", tokens);
    }
}
