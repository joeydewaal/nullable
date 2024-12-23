use sqlparser::ast::Cte;

use crate::{context::Context, nullable::GetNullable};

impl GetNullable for Cte {
    fn nullable_for(
        context: &mut Context,
        cte: &Self,
    ) -> anyhow::Result<crate::nullable::StatementNullable> {
        let nullable = context.nullable_for(&cte.query)?.flatten();

        dbg!(&nullable);

        let table = nullable.clone().to_table(vec![cte.alias.name.clone()]);

        dbg!(&table);

        context.push(table.clone());
        context.source.push(table);
        Ok(nullable.into())
    }
}
