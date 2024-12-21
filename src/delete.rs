use sqlparser::ast::{Delete, FromTable};

use crate::{
    context::Context,
    nullable::{Nullable, StatementNullable},
    select::visit_select_item,
};

impl Context {
    pub fn nullable_for_delete(&mut self, delete: &Delete) -> anyhow::Result<StatementNullable> {
        let mut nullable = Nullable::empty();

        match &delete.from {
            FromTable::WithFromKeyword(tables) => {
                for table in tables {
                    self.add_active_tables(table)?;
                }
            }
            other => unimplemented!("{other:?}"),
        }

        if let Some(returning) = &delete.returning {
            for item in returning {
                nullable.append(&mut visit_select_item(item, self)?);
            }
        }
        Ok(nullable.into())
    }
}
