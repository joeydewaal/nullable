use anyhow::Result;
use sqlparser::ast::Cte;

use crate::{context::Context, query::nullable_from_query};

pub fn visit_cte(cte: &Cte, context: &mut Context) -> Result<()> {
    let nullable = nullable_from_query(&cte.query, context)?.flatten();

    let table = nullable.to_table(vec![cte.alias.name.clone()]);

    context.source.push(table);
    Ok(())
}
