use sqlparser::ast::{CastKind, Expr, SetExpr, SetOperator, Value};

use crate::{
    func::visit_func, nullable::StatementNullable, select::nullable_from_select, Source,
    Tables,
};

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

pub fn visit_expr(expr: &Expr, alias: Option<String>, tables: &Tables) -> Option<bool> {
    match expr {
        Expr::CompoundIdentifier(idents) => tables.nullable_for_ident(&idents),
        Expr::Identifier(col_name) => tables.nullable_for_ident(&[col_name.clone()]),
        Expr::Function(func) => visit_func(func, tables),
        Expr::Exists {
            subquery: _,
            negated: _,
        } => Some(false),
        Expr::Value(value) => match value {
            Value::Null => Some(true),
            _ => Some(false),
        },
        Expr::Cast {
            kind: CastKind::DoubleColon,
            expr,
            data_type: _,
            format: _,
        } => visit_expr(expr, alias, tables),
        _ => None,
    }
}
