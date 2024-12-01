use sqlparser::ast::{SetExpr, SetOperator};

use crate::{nullable::StatementNullable, select::nullable_from_select, Source};

pub fn nullable_from_expr(expr: &SetExpr, source: &Source) -> StatementNullable {
    match expr {
        SetExpr::Select(ref select) => nullable_from_select(select, source).into(),
        SetExpr::SetOperation {
            op: SetOperator::Union,
            set_quantifier: _,
            left,
            right,
        } => {
            let mut nullable = StatementNullable::new();
            nullable.combine(nullable_from_expr(&left, source));
            nullable.combine(nullable_from_expr(&right, source));
            nullable
        }
        _ => StatementNullable::new(),
    }
}
