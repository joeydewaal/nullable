use anyhow::anyhow;
use sqlparser::ast::{Expr, Ident, TableAlias, TableFactor};
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
            .find(|t| t.original_name.as_deref() == Some(name))
            .cloned()
    }

    pub fn push(&mut self, table: Table) {
        self.tables.push(table);
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
        self.0
            .iter()
            .find(|t| t.table_name.as_deref() == Some(name))
    }
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
            .find(|table| table.table_name.as_deref() == Some(&name[..name.len() - 1][..]))
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
            .find(|table| table.original_name.as_deref() == Some(&name[..name.len() - 1][..]))
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

    pub fn find_cols_by_idents(&self, name: &[Ident]) -> Vec<(TableColumn,&Table)> {
        let mut tables = Vec::new();

        // search for col
        for table in self.0.iter() {
            for col in &table.columns {
                if col.column_name == name[0] {
                    tables.push((col.clone(), table));
                    continue;
                }
            }
        }
        tables
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
                        .find(|t| t.table_name.as_deref() == Some(&[alias.name.clone()][..]))
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
    pub original_name: Option<Vec<Ident>>,
    pub table_name: Option<Vec<Ident>>,
    pub columns: Vec<TableColumn>,
}

impl Table {
    pub fn new(table_name: impl ToOptName) -> Self {
        let table_name = table_name.to_op_name();
        Self {
            table_id: TableId::new(0),
            table_name: table_name.clone(),
            original_name: table_name,
            columns: Vec::new(),
        }
    }

    pub fn push_column2(mut self, column_name: Ident, catalog_nullable: bool) -> Self {
        self.columns.push(TableColumn::new(
            column_name,
            catalog_nullable,
            self.table_id,
            ColumnId::new(self.columns.len()),
        ));
        self
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

    pub fn add_alias(&mut self, alias: impl ToOptName) {
        let alias = alias.to_op_name();
        if let Some(alias) = alias {
            self.table_name = Some(alias);
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

pub trait ToOptName {
    fn to_op_name(self) -> Option<Vec<Ident>>;
}

impl ToOptName for &str {
    fn to_op_name(self) -> Option<Vec<Ident>> {
        Some(vec![Ident::new(self)])
    }
}

impl ToOptName for &Ident {
    fn to_op_name(self) -> Option<Vec<Ident>> {
        Some(vec![self.clone()])
    }
}

impl ToOptName for &[Ident] {
    fn to_op_name(self) -> Option<Vec<Ident>> {
        Some(self.to_vec())
    }
}

impl ToOptName for Vec<Ident> {
    fn to_op_name(self) -> Option<Vec<Ident>> {
        Some(self)
    }
}

impl ToOptName for Option<Ident> {
    fn to_op_name(self) -> Option<Vec<Ident>> {
        Some(vec![self?])
    }
}

impl ToOptName for &Option<TableAlias> {
    fn to_op_name(self) -> Option<Vec<Ident>> {
        self.as_ref().map(|f| f.name.clone()).to_op_name()
    }
}
