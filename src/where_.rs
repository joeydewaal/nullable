use sqlparser::ast::Select;

use crate::{context::Context, expr::get_nullable_col};

impl Context {
    pub fn update_from_where(&mut self, select: &Select) -> anyhow::Result<()> {
        let Some(ref selection) = select.selection else {
            return Ok(());
        };

        get_nullable_col(selection, self)?;
        Ok(())
    }
}
