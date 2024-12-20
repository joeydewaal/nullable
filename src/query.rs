use sqlparser::ast::Query;

use crate::{context::Context, expr::nullable_from_set_expr, nullable::StatementNullable};

pub fn nullable_from_query(
    query: &Query,
    context: &mut Context,
) -> anyhow::Result<StatementNullable> {
    if let Some(with) = &query.with {
        context.add_with(&with);
    }

    dbg!(&context.tables);
    nullable_from_set_expr(&query.body, context)
}
