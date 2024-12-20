use anyhow::Context as _;
use sqlparser::ast::Insert;

use crate::{
    context::Context,
    nullable::{Nullable, StatementNullable},
    select::visit_select_item,
};

impl Context {
    pub fn nullables_from_insert(&mut self, insert: &Insert) -> anyhow::Result<StatementNullable> {
        let mut nullable = Nullable::empty();

        if let Some(returning) = &insert.returning {
            let table = self
                .source
                .find_by_original_name(&insert.table_name.0)
                .context("Could not find")?;
            self.push(table.clone());

            for item in returning {
                nullable.append(&mut visit_select_item(item, self)?);
            }
        }
        Ok(nullable.into())
    }
}
