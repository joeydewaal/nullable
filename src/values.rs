use sqlparser::ast::Values;

use crate::{
    context::Context,
    expr::visit_expr,
    nullable::{Nullable, StatementNullable},
};

impl Context {
    pub fn nullable_from_values(&mut self, values: &Values) -> anyhow::Result<StatementNullable> {
        let mut statement = StatementNullable::new();
        for row in &values.rows {
            let mut nullables = Nullable::empty();

            for col in row {
                let nullable = visit_expr(&col, None, self)?;
                nullables.push(nullable);
            }
            statement.push(nullables);
        }
        Ok(statement)
    }
}
