use sqlparser::ast::Query;

use crate::{context::Context, expr::nullable_from_expr, nullable::StatementNullable};

pub fn nullable_from_query(
    query: &Query,
    context: &mut Context,
) -> anyhow::Result<StatementNullable> {
    nullable_from_expr(&query.body, context)
}
