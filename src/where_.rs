use sqlparser::ast::Select;

use crate::{context::Context, expr::get_nullable_col};

impl Context {
    pub fn update_from_where(&mut self, select: &Select) -> anyhow::Result<()> {
        let Some(ref selection) = select.selection else {
            return Ok(());
        };

        let x = get_nullable_col(selection, self)?;
        for (col, nullable_column, nullable_table) in x.iter() {
            if let Some(col_nullable) = nullable_column {
                self.wal
                    .add_column(col.table_id, col.column_id, *col_nullable);
            }

            if let Some(table_nullable) = nullable_table {
                self.wal.add_table(col.table_id, *table_nullable);
            }
        }

        // self.tables.apply(x);

        Ok(())
    }

}
