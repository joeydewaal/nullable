use anyhow::anyhow;
use sqlparser::ast::{Expr, Ident, TableFactor};
use std::fmt::Debug;

#[derive(Debug, Clone)]
pub struct Source {
    tables: Vec<Table>,
}

impl Source {
    pub fn new(tables: Vec<Table>) -> Self {
        Source { tables }
    }

    pub fn empty() -> Self {
        Self { tables: Vec::new() }
    }

    pub fn find_by_original_name(&self, name: &[Ident]) -> Option<Table> {
        self.tables
            .iter()
            .find(|t| t.original_name == name)
            .cloned()
    }
}

#[derive(Default, Debug, Clone)]
pub struct Tables(pub Vec<Table>);

impl Tables {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn find_table_id(&self, table_id: TableId) -> Option<&Table> {
        self.0.iter().find(|t| t.table_id == table_id)
    }

    // pub fn apply(&mut self, data: Vec<(TableColumn, Option<bool>, Option<bool>)>) {
    //     for (col, nullable_column, nullable_table) in data.into_iter() {
    //         for t in self.0.iter_mut() {
    //             if t.table_id == col.table_id {
    //                 t.table_nullable = nullable_table;
    //                 for column in t.columns.iter_mut() {
    //                     if column.column_id == col.column_id {
    //                         column.inferred_nullable = nullable_column
    //                     }
    //                 }
    //             }
    //         }
    //     }
    // }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn get_index(&self, other: &Table) -> Option<usize> {
        self.0.iter().position(|t| t.equals(other))
    }

    pub fn push(&mut self, mut table: Table) {
        for cur_table in self.0.iter() {
            // don't insert duplicate tables
            if cur_table.equals(&table) {
                return;
            }
        }

        table.table_id = TableId::new(self.0.len());

        for col in table.columns.iter_mut() {
            col.table_id = table.table_id
        }

        self.0.push(table)
    }

    pub fn find_table_by_idents_table(&self, name: &[Ident]) -> Option<&Table> {
        self.0.iter().find(|t| t.table_name == name)
    }

    // pub fn nullable_for_ident(&self, name: &[Ident]) -> anyhow::Result<NullableResult> {
    //     let (col, table) = self.find_col_by_idents(name)?;

    //     if let Some(wal_nullable) = self.find_table_by_idents_table

    //     dbg!(&col, &table);

    //     if col.inferred_nullable.is_some() {
    //         return Ok(NullableResult::named(col.inferred_nullable, name));
    //     }

    //     if table.table_nullable.is_some() {
    //         return Ok(NullableResult::named(table.table_nullable, name));
    //     } else {
    //         return Ok(NullableResult::named(Some(col.get_nullable()), name));
    //     }
    // }

    pub fn find_col_by_idents(&self, name: &[Ident]) -> anyhow::Result<(TableColumn, &Table)> {
        // search for col
        if name.len() == 1 {
            for table in self.0.iter() {
                for col in &table.columns {
                    if col.column_name == name[0] {
                        return Ok((col.clone(), table));
                    }
                }
            }
        }

        // look for original name: `table_alias`.`col_name`
        if let Some(table) = self
            .0
            .iter()
            .find(|table| table.table_name == name[..name.len() - 1])
        {
            if let Some(col) = table
                .columns
                .iter()
                .find(|column| Some(&column.column_name) == name.last())
            {
                return Ok((col.clone(), table));
            }
        }

        // look for original name: `original_table_name`.`col_name`
        if let Some(table) = self
            .0
            .iter()
            .find(|table| table.original_name == name[..name.len() - 1])
        {
            if let Some(col) = table
                .columns
                .iter()
                .find(|column| Some(&column.column_name) == name.last())
            {
                return Ok((col.clone(), table));
            }
        }

        return Err(anyhow!("Not found"));
    }

    pub fn table_from_expr(
        &self,
        expr: &Expr,
        recursive_left: bool,
    ) -> anyhow::Result<(TableColumn, Table)> {
        match &expr {
            Expr::CompoundIdentifier(idents) => {
                self.find_col_by_idents(&idents).map(|t| (t.0, t.1.clone()))
            }
            Expr::BinaryOp { left, op: _, right } => {
                if recursive_left {
                    return self.table_from_expr(left, recursive_left);
                } else {
                    return self.table_from_expr(right, recursive_left);
                }
            }
            _ => Err(anyhow!("not found")),
        }
    }

    pub fn find_table_by_table_factor(&self, factor: &TableFactor) -> Option<Table> {
        match &factor {
            TableFactor::Table { name, alias, .. } => {
                if let Some(alias) = alias {
                    return self
                        .0
                        .iter()
                        .find(|t| t.table_name.as_ref() == [alias.name.clone()])
                        .cloned();
                }
                self.find_table_by_idents_table(&name.0).cloned()
            }
            _ => None,
        }
    }
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct Table {
    pub table_id: TableId,
    pub original_name: Vec<Ident>,
    pub table_name: Vec<Ident>,
    pub columns: Vec<TableColumn>,
}

impl Table {
    pub fn new(table_name: impl Into<String>) -> Self {
        let name = Ident::new(table_name);
        Self {
            table_id: TableId::new(0),
            table_name: vec![name.clone()],
            original_name: vec![name],
            columns: Vec::new(),
        }
    }

    pub fn push_column(mut self, column_name: impl Into<String>, catalog_nullable: bool) -> Self {
        self.columns.push(TableColumn::new(
            Ident::new(column_name),
            catalog_nullable,
            self.table_id,
            ColumnId::new(self.columns.len()),
        ));
        self
    }

    pub fn equals(&self, other: &Self) -> bool {
        self.table_name == other.table_name
    }

    pub fn add_alias(&mut self, alias: Option<&Ident>) {
        if let Some(alias) = alias {
            self.table_name = vec![alias.clone()];
        }
    }
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct TableColumn {
    pub column_name: Ident,
    pub catalog_nullable: bool,

    pub column_id: ColumnId,
    pub table_id: TableId,
}

#[derive(PartialEq, Eq, Hash, Clone, Copy)]
pub struct TableId(usize);

impl Debug for TableId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl TableId {
    pub fn new(d: usize) -> Self {
        Self(d)
    }
}

#[derive(PartialEq, Eq, Hash, Clone, Copy)]
pub struct ColumnId(usize);

impl Debug for ColumnId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl ColumnId {
    pub fn new(d: usize) -> Self {
        Self(d)
    }
}

impl TableColumn {
    pub fn new(
        column_name: Ident,
        catalog_nullable: bool,
        table_id: TableId,
        column_id: ColumnId,
    ) -> Self {
        Self {
            table_id,
            column_id,
            column_name,
            catalog_nullable,
        }
    }
}
