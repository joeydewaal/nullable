use sqlparser::ast::Statement;

use crate::{
    context::Context,
    nullable::{Nullable, StatementNullable},
    query::nullable_from_query,
    select::visit_select_item,
};

pub fn nullable_from_statement(
    statement: &Statement,
    context: &mut Context,
) -> anyhow::Result<StatementNullable> {
    match statement {
        Statement::Query(query) => nullable_from_query(query, context),
        Statement::CreateTable(_)
        | Statement::CreateView { .. }
        | Statement::CreateIndex(_)
        | Statement::CreateType { .. }
        | Statement::CreateExtension { .. }
        | Statement::CreateRole { .. }
        | Statement::CreateSchema { .. } => Ok(StatementNullable::new()),
        Statement::Update {
            table, returning, ..
        } => {
            let mut nullable = Nullable::empty();

            if let Some(returning) = returning {
                context.add_active_tables(table);
                for item in returning {
                    nullable.add(visit_select_item(item, context)?);
                }
            }
            Ok(nullable.into())
        }
        _ => unimplemented!("{statement:?}"),
    }
}
