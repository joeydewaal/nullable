use sqlparser::ast::Query;

use crate::{expr::nullable_from_expr, nullable::StatementNullable, Source};

pub fn nullable_from_query(query: &Query, source: &Source) -> StatementNullable {
    nullable_from_expr(&query.body, source)
}
