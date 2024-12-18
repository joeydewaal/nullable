use sqlparser::ast::{Ident, Query};

use crate::{context::Context, expr::nullable_from_expr, nullable::StatementNullable};

pub fn nullable_from_query(
    query: &Query,
    context: &mut Context,
) -> anyhow::Result<StatementNullable> {
    if let Some(with) = &query.with {
        context.add_with(&with);
    }

    nullable_from_expr(&query.body, context)
}
