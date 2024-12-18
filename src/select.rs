use sqlparser::ast::{Select, SelectItem};

use crate::{
    context::Context,
    expr::visit_expr,
    nullable::{Nullable, NullableResult},
};

pub fn nullable_from_select(select: &Select, context: &mut Context) -> anyhow::Result<Nullable> {
    for table in &select.from {
        context.add_active_tables(table);
    }

    dbg!(&context.tables);
    context.update_nullable_from_select_joins(select);
    context.update_nullable_from_select_where(select)?;
    dbg!(&context.tables);

    let n: Vec<_> = select
        .projection
        .iter()
        .map(|c| visit_select_item(c, context).unwrap())
        .collect();
    Ok(Nullable::new(n))
}

pub fn visit_select_item(
    select_item: &SelectItem,
    context: &mut Context,
) -> anyhow::Result<NullableResult> {
    match select_item {
        SelectItem::UnnamedExpr(expr) => visit_expr(&expr, None, context),
        SelectItem::ExprWithAlias { expr, alias } => {
            visit_expr(&expr, Some(alias.clone()), context)
        }
        _ => unimplemented!("{select_item:?}"),
    }
}
