mod context;
mod cte;
mod expr;
mod func;
mod join;
mod join_resolver;
mod nullable;
mod query;
mod select;
mod state;
mod statement;
mod table;
mod wal;
mod where_;
mod values;
mod insert;
mod source;
mod params;
mod delete;
mod select_item;
mod set_expr;

use sqlparser::dialect::{Dialect, PostgreSqlDialect, SQLiteDialect};
pub use state::NullableState;
pub use table::*;
pub use source::Source;

#[derive(Debug, Clone, Copy)]
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
