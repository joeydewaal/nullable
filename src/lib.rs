mod context;
mod expr;
mod func;
mod nullable;
mod query;
mod select;
mod state;
mod statement;
mod table;

use sqlparser::dialect::{Dialect, PostgreSqlDialect, SQLiteDialect};
pub use state::NullableState;
pub use table::*;

pub enum SqlFlavour {
    Postgres,
    Sqlite,
}

impl SqlFlavour {
    fn to_dialect(&self) -> &'static dyn Dialect {
        match self {
            SqlFlavour::Postgres => &PostgreSqlDialect {},
            SqlFlavour::Sqlite => &SQLiteDialect {},
        }
    }
}
