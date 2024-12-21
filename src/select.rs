use sqlparser::ast::{Select, SelectItem};

use crate::{
    context::Context,
    expr::visit_expr,
    nullable::{GetNullable, Nullable, NullableResult},
};

impl GetNullable for Select {
    fn nullable_for(
        context: &mut Context,
        select: &Self,
    ) -> anyhow::Result<crate::nullable::StatementNullable> {
        for table in &select.from {
            context.add_active_tables(table)?;
        }

        context.update_from_join(select)?;
        context.update_from_where(select)?;
        dbg!(&context.tables);
        dbg!(&context.wal);

        let n: Vec<_> = select
            .projection
            .iter()
            .map(|c| visit_select_item(c, context).unwrap())
            .flatten()
            .collect();

        Ok(Nullable::new(n).into())
    }
}

pub fn visit_select_item(
    select_item: &SelectItem,
    context: &mut Context,
) -> anyhow::Result<Vec<NullableResult>> {
    match select_item {
        SelectItem::UnnamedExpr(expr) => Ok(vec![visit_expr(&expr, None, context)?]),
        SelectItem::ExprWithAlias { expr, alias } => {
            Ok(vec![visit_expr(&expr, Some(alias.clone()), context)?])
        }
        SelectItem::Wildcard(_wildcard) => {
            let mut results = Vec::new();

            for table in context.iter_tables() {
                for column in table.columns.iter() {
                    results.push(context.nullable_for_idents(&[column.column_name.clone()])?);
                }
            }
            Ok(results)
        }
        SelectItem::QualifiedWildcard(table_name, _wildcard) => {
            let mut results = Vec::new();

            let table = context.find_table_by_idents_table(&table_name.0).unwrap();

            dbg!(&table);

            for column in &table.columns {
                results.push(context.nullable_for_table_col(table, column)?);
            }

            Ok(results)
        }
    }
}
