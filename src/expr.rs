use anyhow::Context as _;
use sqlparser::ast::{BinaryOperator, CastKind, Expr, Ident, SetExpr, SetOperator, Value};

use crate::{
    context::Context,
    func::visit_func,
    nullable::{NullableResult, StatementNullable},
    query::nullable_from_query,
    select::nullable_from_select,
    TableColumn,
};

pub fn nullable_from_expr(
    expr: &SetExpr,
    context: &mut Context,
) -> anyhow::Result<StatementNullable> {
    match expr {
        SetExpr::Select(ref select) => nullable_from_select(select, context).map(|x| x.into()),
        SetExpr::SetOperation {
            op: SetOperator::Union,
            set_quantifier: _,
            left,
            right,
        } => {
            let mut nullable = StatementNullable::new();
            nullable.combine(nullable_from_expr(&left, context)?);
            nullable.combine(nullable_from_expr(&right, context)?);
            Ok(nullable)
        }
        _ => unimplemented!("{expr:?}"),
    }
}

pub fn visit_expr(
    expr: &Expr,
    alias: Option<Ident>,
    context: &mut Context,
) -> anyhow::Result<NullableResult> {
    match expr {
        Expr::CompoundIdentifier(idents) => {
            let value = context.tables.nullable_for_ident(&idents)?.set_alias(alias);
            Ok(value)
        }
        Expr::Identifier(col_name) => {
            let value = context
                .tables
                .nullable_for_ident(&[col_name.clone()])?
                .set_alias(alias);
            Ok(value)
        }
        Expr::Function(func) => {
            let o = visit_func(func, context)?.set_alias(alias);
            Ok(o)
        }
        Expr::Exists {
            subquery: _,
            negated: _,
        } => Ok(NullableResult::unnamed(Some(false))),
        Expr::Value(value) => match value {
            Value::Null => Ok(NullableResult::unnamed(Some(true)).set_alias(alias)),
            _ => Ok(NullableResult::unnamed(Some(false)).set_alias(alias)),
        },
        Expr::Cast {
            kind: CastKind::DoubleColon,
            expr,
            data_type: _,
            format: _,
        } => visit_expr(expr, alias, context),
        Expr::Tuple(_tuple) => Ok(NullableResult::unnamed(Some(false)).set_alias(alias)),
        Expr::Nested(nested) => visit_expr(&nested, alias, context),
        Expr::BinaryOp { left, op: _, right } => {
            let left_nullable = visit_expr(&left, alias.clone(), context)?;
            let right_nullable = visit_expr(&right, alias, context)?;

            if left_nullable.value == Some(false) && right_nullable.value == Some(false) {
                return Ok(NullableResult::unnamed(Some(false)));
            } else if left_nullable.value == Some(true) || right_nullable.value == Some(true) {
                return Ok(NullableResult::unnamed(Some(true)));
            } else {
                return Ok(NullableResult::unnamed(None));
            }
        }
        Expr::IsNotNull(_) => Ok(NullableResult::unnamed(None).set_alias(alias)),
        Expr::Subquery(query) => {
            dbg!(&query);
            let r = nullable_from_query(&query, context)
                .map(|r| r.get_nullable().iter().any(|n| *n == Some(true)))?;
            dbg!(&r);
            Ok(NullableResult::unnamed(Some(r)).set_alias(alias))
        }
        _ => unimplemented!("{:?}", expr),
    }
}

pub fn get_nullable_col(
    expr: &Expr,
    context: &mut Context,
) -> anyhow::Result<Vec<(TableColumn, Option<bool>, Option<bool>)>> {
    match expr {
        Expr::IsNotNull(not_null) => {
            if let Some(column) = get_column(&not_null, context)? {
                return Ok(vec![(column, Some(false), Some(false))]);
            }
            Ok(vec![])
        }
        Expr::BinaryOp { left, op, right } => {
            let mut x = vec![];

            println!("left_col:  {:?}", get_column(&left, context));
            println!("right: {:?}", visit_expr(&right, None, context));
            if let (Some(left_col), Some(false)) = (
                get_column(&left, context)?,
                visit_expr(&right, None, context)?.value,
            ) {
                x.push((left_col, Some(false), Some(false)));
            }

            println!("right_col:  {:?}", get_column(&right, context));
            println!("left:  {:?}", visit_expr(&left, None, context));
            if let (Some(right_col), Some(false)) = (
                get_column(&right, context)?,
                visit_expr(&left, None, context)?.value,
            ) {
                x.push((right_col, Some(false), Some(false)));
            }

            if *op != BinaryOperator::And {
                return Ok(x);
            }
            let mut left = get_nullable_col(left, context)?;
            let mut right = get_nullable_col(right, context)?;

            x.append(&mut left);
            x.append(&mut right);

            dbg!(&x);
            return Ok(x);
        }
        Expr::CompoundIdentifier(_) => Ok(vec![]),
        Expr::Identifier(_ident) => Ok(vec![]),
        Expr::Value(_) => Ok(vec![]),
        _ => unimplemented!("{expr:?}"),
    }
}

fn get_column(expr: &Expr, context: &mut Context) -> anyhow::Result<Option<TableColumn>> {
    match expr {
        Expr::CompoundIdentifier(idents) => {
            let (col, _table) = context
                .tables
                .find_col_by_idents(&idents)
                .context(format!("table not found: {expr:?}"))?;
            Ok(Some(col))
        }
        Expr::Identifier(ident) => {
            let (col, _table) = context
                .tables
                .find_col_by_idents(&[ident.clone()])
                .context(format!("table not found: {expr:?}"))?;
            Ok(Some(col))
        }
        _ => Ok(None),
    }
}
