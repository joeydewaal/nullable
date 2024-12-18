use anyhow::Result;
use sqlparser::ast::Cte;

use crate::{context::Context, query::nullable_from_query, Table};

pub fn visit_cte(cte: &Cte, context: &mut Context) -> Result<()> {
    let mut table = Table::new(cte.alias.name.value.clone());

    let x = nullable_from_query(&cte.query, context)?;

    dbg!(&x);

    Ok(())
}
