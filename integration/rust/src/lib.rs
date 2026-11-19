#[macro_export]
macro_rules! unsafe_sql {
    ($($arg:tt)*) => {
        ::sqlx::SqlSafeStr::into_sql_str(::sqlx::AssertSqlSafe(::std::format!($($arg)*)))
    };
}

pub mod setup;
pub mod utils;
