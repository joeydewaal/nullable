use sqlparser::ast::{Select, SelectItem};

use crate::{expr::visit_expr, nullable::Nullable, Source, Tables};

pub fn nullable_from_select(select: &Select, source: &Source) -> Nullable {
    let mut active_tables = Tables::new();

    for table in &select.from {
        active_tables.visit_join_active_table(table, source);
    }

    active_tables.update_nullable_from_select(select);

    println!("{active_tables:#?}");

    let n: Vec<_> = select
        .projection
        .iter()
        .map(|c| visit_select_item(c, &active_tables).unwrap_or(true))
        .collect();

    Nullable::new(n)
}

pub fn visit_select_item(select_item: &SelectItem, tables: &Tables) -> Option<bool> {
    match select_item {
        SelectItem::UnnamedExpr(expr) => visit_expr(&expr, None, tables),
        SelectItem::ExprWithAlias { expr, alias } => {
            visit_expr(&expr, Some(alias.value.clone()), tables)
        }
        _ => None,
    }
}
