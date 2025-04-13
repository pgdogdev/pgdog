//! Configuration errors.

use thiserror::Error;

/// Configuration error.
#[derive(Debug, Error)]
pub enum Error {
    #[error("{0}")]
    Io(#[from] std::io::Error),

    #[error("{0}")]
    Deser(#[from] toml::de::Error),

    #[error("{0}, line {1}")]
    MissingField(String, usize),

    #[error("{0}")]
    Url(#[from] url::ParseError),

    #[error("{0}")]
    Json(#[from] serde_json::Error),

    #[error("incomplete startup")]
    IncompleteStartup,
}

impl Error {
    pub fn config(source: &str, err: toml::de::Error) -> Self {
        let span = err.span();
        let message = err.message();

        let span = if let Some(span) = span {
            span
        } else {
            return Self::MissingField(message.into(), 0);
        };

        let mut lines = vec![];
        let mut line = 1;
        for (i, c) in source.chars().enumerate() {
            if c == '\n' {
                lines.push((line, i));
                line += 1;
            }
        }

        let mut lines = lines.into_iter().peekable();

        while let Some(line) = lines.next() {
            if span.start < line.1 {
                if let Some(next) = lines.peek() {
                    if next.1 > span.start {
                        return Self::MissingField(message.into(), line.0);
                    }
                }
            }
        }

        Self::MissingField(message.into(), 0)
    }
}
