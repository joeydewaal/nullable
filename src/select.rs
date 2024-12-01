use sqlparser::ast::Select;

use crate::{nullable::Nullable, statement::StatementExpr, ColumnExprs, Source, Tables};

pub fn nullable_from_select(select: &Select, source: &Source) -> Nullable {
    let mut active_tables = Tables::new();
    let mut active_cols = ColumnExprs::new();

    for table in &select.from {
        active_tables.visit_join_active_table(table, source);
    }

    for column in &select.projection {
        let c = active_tables.push_select_item(&column);
        active_cols.push(c);
    }

    let mut x = StatementExpr {
        tables: active_tables,
        cols: active_cols,
    };

    x.update_nullable_from_select(select);

    let mut inferred_nullable = x.get_nullable(source);
    for (i, col) in x.cols.iter().enumerate() {
        inferred_nullable[i] = inferred_nullable[i] || col.get_nullable(&x, source);
    }

    Nullable::new(inferred_nullable)
}
