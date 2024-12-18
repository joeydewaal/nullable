use anyhow::anyhow;
use sqlparser::ast::{Expr, Ident, TableFactor};
use std::{fmt::Debug, slice::Iter};

use crate::nullable::NullableResult;

#[derive(Debug, Clone)]
pub struct Source {
    tables: Vec<Table>,
}

impl Source {
    pub fn new(tables: Vec<Table>) -> Self {
        Source { tables }
    }

    // pub fn find_table(&self, table_name: &str) -> Option<&Table> {
    //     self.tables.iter().find(|t| t.table_name == table_name)
    // }

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

    pub fn apply(&mut self, data: Vec<(TableColumn, Option<bool>, Option<bool>)>) {
        for (col, nullable_column, nullable_table) in data.into_iter() {
            for t in self.0.iter_mut() {
                if t.table_id == col.table_id {
                    t.table_nullable = nullable_table;
                    for column in t.columns.iter_mut() {
                        if column.column_id == col.column_id {
                            // println!("{}:{:?}", column.column_name, nullable);
                            column.inferred_nullable = nullable_column
                        }
                    }
                }
            }
        }
    }

    pub fn iter(&self) -> Iter<'_, Table> {
        self.0.iter()
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

    pub fn find_table(
        &self,
        col_name: &str,
        table_name: Option<&str>,
    ) -> anyhow::Result<(TableColumn, &Table)> {
        todo!()
        // let find_col = |table: &Table| {
        //     table
        //         .columns
        //         .iter()
        //         .find(move |c| c.column_name == col_name)
        //         .cloned()
        // };

        // if let Some(table_name) = table_name {
        //     let opt_table = self.0.iter().find(|table| table.table_name == table_name);

        //     return opt_table
        //         .map(|t| (find_col(&t).unwrap(), t))
        //         .ok_or(anyhow!("Not found"));
        // }
        // let mut iterator = self.0.iter().filter(|table| {
        //     table
        //         .columns
        //         .iter()
        //         .find(|col| col.column_name == col_name)
        //         .is_some()
        // });

        // let opt_table = iterator.next();
        // assert!(iterator.next().is_none());
        // return opt_table
        //     .map(|t| (find_col(&t).unwrap(), t))
        //     .ok_or(anyhow!("Not found"));
    }

    // pub fn find_table_by_alias(
    //     &self,
    //     col_name: &str,
    //     alias: &String,
    // ) -> anyhow::Result<(TableColumn, &Table)> {
    //     if let Some(table) = self.0.iter().find(|t| t.alias.as_deref() == Some(alias)) {
    //         if let Some(col) = table.columns.iter().find(|c| c.column_name == col_name) {
    //             return Ok((col.clone(), table));
    //         }
    //     };
    //     Err(anyhow!("error"))
    // }

    pub fn find_table_by_idents_table(&self, name: &[Ident]) -> Option<&Table> {
        self.0.iter().find(|t| t.table_name == name)
    }

    pub fn nullable_for_ident(&self, name: &[Ident]) -> anyhow::Result<NullableResult> {
        let (col, table) = self.find_col_by_idents(name)?;

        if col.inferred_nullable.is_some() {
            return Ok(NullableResult::named(col.inferred_nullable, name));
        }

        if table.table_nullable == Some(true) {
            return Ok(NullableResult::named(Some(true), name));
        } else {
            return Ok(NullableResult::named(Some(col.get_nullable()), name));
        }
    }

    pub fn find_col_by_idents(&self, name: &[Ident]) -> anyhow::Result<(TableColumn, &Table)> {
        // search for col
        if name.len() == 1 {
            for table in self.0.iter() {
                for col in &table.columns {
                    if col.column_name == name[0].value {
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
                .find(|column| column.column_name == name.last().unwrap().value)
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
                .find(|column| column.column_name == name.last().unwrap().value)
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

    pub fn set_table_nullable(&mut self, table_id: TableId, nullable: bool) {
        for i in 0..self.len() {
            if table_id == self.0[i].table_id {
                println!("Setting {:?} to {}", self.0[i].table_name, nullable);
                self.0[i].table_nullable = Some(nullable);

                println!(
                    "{:?} dependants {:?}",
                    self.0[i].table_name,
                    self.0[i].dependants.iter().map(|t| t).collect::<Vec<_>>()
                );

                if nullable {
                    for y in 0..self.0[i].dependants.len() {
                        let b = self.0[i].dependants[y].clone();
                        println!("recursive");
                        self.set_table_nullable(b, nullable)
                    }
                }
            }
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
    pub table_nullable: Option<bool>,
    pub original_name: Vec<Ident>,
    pub table_name: Vec<Ident>,
    pub columns: Vec<TableColumn>,
    pub dependants: Vec<TableId>,
}

impl Table {
    pub fn new(table_name: impl Into<String>) -> Self {
        let name = Ident::new(table_name);
        Self {
            table_id: TableId::new(0),
            table_name: vec![name.clone()],
            original_name: vec![name],
            columns: Vec::new(),
            table_nullable: None,
            dependants: Vec::new(),
        }
    }

    pub fn push_column(mut self, column_name: impl Into<String>, catalog_nullable: bool) -> Self {
        self.columns.push(TableColumn::new(
            column_name,
            catalog_nullable,
            self.table_id,
            ColumnId::new(self.columns.len()),
        ));
        self
    }

    pub fn equals(&self, other: &Self) -> bool {
        self.table_name == other.table_name
    }

    pub fn add_dependent(&mut self, other: &Self) {
        self.dependants.push(other.table_id)
    }

    pub fn add_alias(&mut self, alias: Option<&Ident>) {
        if let Some(alias) = alias {
            self.table_name = vec![alias.clone()];
        }
    }
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct TableColumn {
    pub column_name: String,
    pub catalog_nullable: bool,
    pub inferred_nullable: Option<bool>,

    pub column_id: ColumnId,
    pub table_id: TableId,
}

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub struct TableId(usize);

impl TableId {
    pub fn new(d: usize) -> Self {
        Self(d)
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub struct ColumnId(usize);

impl ColumnId {
    pub fn new(d: usize) -> Self {
        Self(d)
    }
}

impl TableColumn {
    pub fn new(
        column_name: impl Into<String>,
        catalog_nullable: bool,
        table_id: TableId,
        column_id: ColumnId,
    ) -> Self {
        Self {
            table_id,
            column_id,
            column_name: column_name.into(),
            catalog_nullable,
            inferred_nullable: None,
        }
    }

    pub fn get_nullable(&self) -> bool {
        self.inferred_nullable.unwrap_or(self.catalog_nullable)
    }
}
