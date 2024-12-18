use anyhow::Result;
use sqlparser::ast::Cte;

use crate::{context::Context, query::nullable_from_query, Table};

pub fn visit_cte(cte: &Cte, context: &mut Context) -> Result<()> {
    let table = Table::new(&cte.alias.name.value);

    let nullable = nullable_from_query(&cte.query, context)?.flatten();

    for _row in nullable.into_iter() {

    }

    context.tables.push(table);

    Ok(())
}
