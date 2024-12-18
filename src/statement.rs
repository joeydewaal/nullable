use sqlparser::ast::Statement;

use crate::{context::Context, nullable::StatementNullable, query::nullable_from_query};

pub fn nullable_from_statement(
    statement: &Statement,
    context: &mut Context,
) -> anyhow::Result<StatementNullable> {
    match statement {
        Statement::Query(query) => nullable_from_query(query, context),
        _ => unimplemented!("{statement:?}"),
    }
}
