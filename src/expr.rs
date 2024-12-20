use anyhow::{anyhow, Context as _};
use sqlparser::ast::{BinaryOperator, CastKind, Expr, Ident, Value};

use crate::{
    context::Context,
    func::visit_func,
    nullable::{Nullable, NullableResult},
    TableColumn,
};

pub fn visit_expr(
    expr: &Expr,
    alias: Option<Ident>,
    context: &mut Context,
) -> anyhow::Result<NullableResult> {
    match expr {
        Expr::CompoundIdentifier(idents) => {
            let value = context.nullable_for_ident(&idents)?.set_alias(alias);
            Ok(value)
        }
        Expr::Identifier(col_name) => {
            let value = context
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
            Value::Placeholder(param) => {
                Ok(NullableResult::unnamed(context.nullable_for_param(&param)?).set_alias(alias))
            }
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
            let r = context.nullable_for(query)
                .map(|r| r.get_nullable().iter().any(|n| *n == Some(true)))?;
            Ok(NullableResult::unnamed(Some(r)).set_alias(alias))
        }
        Expr::Array(array) => {
            let mut nullable = Nullable::empty();
            for expr in &array.elem {
                nullable.push(visit_expr(expr, None, context)?);
            }
            nullable.to_result().ok_or(anyhow!("Geen output gevonden"))
        }
        _ => unimplemented!("{:?}", expr),
    }
}

pub fn get_nullable_col(expr: &Expr, context: &mut Context) -> anyhow::Result<()> {
    match expr {
        Expr::IsNotNull(not_null) => {
            if let Some(column) = get_column(&not_null, context)? {
                context
                    .wal
                    .add_column(column.table_id, column.column_id, false);
                context.wal.add_table(column.table_id, false);
            }
            Ok(())
        }
        Expr::BinaryOp { left, op, right } => {
            if let (Some(left_col), Some(false)) = (
                get_column(&left, context)?,
                visit_expr(&right, None, context)?.value,
            ) {
                context
                    .wal
                    .add_column(left_col.table_id, left_col.column_id, false);
                context.wal.add_table(left_col.table_id, false);
            }

            if let (Some(right_col), Some(false)) = (
                get_column(&right, context)?,
                visit_expr(&left, None, context)?.value,
            ) {
                context
                    .wal
                    .add_column(right_col.table_id, right_col.column_id, false);
                context.wal.add_table(right_col.table_id, false);
            }

            if *op != BinaryOperator::And {
                return Ok(());
            }
            get_nullable_col(left, context)?;
            get_nullable_col(right, context)?;

            return Ok(());
        }
        Expr::CompoundIdentifier(_) => Ok(()),
        Expr::Identifier(_ident) => Ok(()),
        Expr::Value(_) => Ok(()),
        _ => unimplemented!("{expr:?}"),
    }
}

fn get_column(expr: &Expr, context: &mut Context) -> anyhow::Result<Option<TableColumn>> {
    match expr {
        Expr::CompoundIdentifier(idents) => {
            let (col, _table) = context
                .find_col_by_idents(&idents)
                .context(format!("table not found: {expr:?}"))?;
            Ok(Some(col))
        }
        Expr::Identifier(ident) => {
            let (col, _table) = context
                .find_col_by_idents(&[ident.clone()])
                .context(format!("table not found: {expr:?}"))?;
            Ok(Some(col))
        }
        _ => Ok(None),
    }
}
