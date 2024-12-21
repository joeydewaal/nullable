use sqlparser::ast::SelectItem;

use crate::{
    nullable::{GetNullable, Nullable},
    select::visit_select_item,
};

impl GetNullable for Vec<SelectItem> {
    fn nullable_for(
        context: &mut crate::context::Context,
        items: &Self,
    ) -> anyhow::Result<crate::nullable::StatementNullable> {
        let mut nullable = Nullable::empty();
        for item in items {
            nullable.append(&mut visit_select_item(item, context)?);
        }
        Ok(nullable.into())
    }
}
