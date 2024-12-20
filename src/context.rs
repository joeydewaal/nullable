use std::collections::HashSet;

use anyhow::anyhow;
use sqlparser::ast::{Expr, Ident, TableFactor, TableWithJoins, With};

use crate::{
    cte::visit_cte,
    nullable::NullableResult,
    wal::{Wal, WalEntry},
    Source, Table, TableColumn, TableId, Tables,
};

pub struct Context {
    pub tables: Tables,
    pub source: Source,
    pub wal: Wal,
}

impl Context {
    pub fn new(tables: Tables, source: Source, wal: Wal) -> Context {
        Self {
            tables,
            source,
            wal,
        }
    }

    pub fn add_active_tables(&mut self, table: &TableWithJoins) {
        self.visit_table_factor(&&table.relation);
        for join_table in &table.joins {
            self.visit_table_factor(&&join_table.relation);
        }
    }

    pub fn visit_table_factor(&mut self, table: &TableFactor) {
        match table {
            TableFactor::Table { name, alias, .. } => {
                let mut table = self.source.find_by_original_name(&name.0).unwrap();
                table.add_alias(alias.as_ref().map(|alias| &alias.name));
                self.push(table);
            }
            _ => (),
        }
    }

    pub fn find_table_by_table_factor(&self, factor: &TableFactor) -> Option<Table> {
        match &factor {
            TableFactor::Table { name, alias, .. } => {
                if let Some(alias) = alias {
                    return self
                        .tables
                        .0
                        .iter()
                        .find(|t| t.table_name == &[alias.name.clone()])
                        .cloned();
                }
                self.tables.find_table_by_idents_table(&name.0).cloned()
            }
            _ => None,
        }
    }

    pub fn recursive_find_joined_tables(&self, expr: &Expr, tables: &mut HashSet<Table>) {
        match expr {
            Expr::CompoundIdentifier(idents) => {
                let table = self.tables.find_col_by_idents(&idents).unwrap();

                tables.insert(table.1.clone());
            }
            Expr::BinaryOp { left, op: _, right } => {
                self.recursive_find_joined_tables(&left, tables);
                self.recursive_find_joined_tables(&right, tables);
            }
            Expr::Subscript { expr, subscript: _ } => {
                self.recursive_find_joined_tables(expr, tables)
            }
            Expr::Value(_) => (),
            others => unimplemented!("{others:?}"),
        }
    }

    pub fn add_with(&mut self, with: &With) {
        for cte in &with.cte_tables {
            let _ = visit_cte(cte, self);
        }
    }

    pub fn nullable_for_idents(&self, ident: &[Ident]) -> anyhow::Result<NullableResult> {
        self.nullable_for_ident(ident)
    }

    pub fn iter_tables(&self) -> impl Iterator<Item = &Table> {
        self.tables.0.iter()
    }

    pub fn find_table_by_idents_table(&self, name: &[Ident]) -> Option<&Table> {
        self.tables.0.iter().find(|t| t.table_name == name)
    }

    pub fn nullable_for_table_col(
        &self,
        table: &Table,
        col: &TableColumn,
    ) -> anyhow::Result<NullableResult> {
        let mut col_name = table.table_name.clone();
        col_name.push(col.column_name.clone());

        // check col nullable in wal
        if let Some(wal_nullable) = self.wal.nullable_for_col(table, col.column_id) {
            println!("found col null {} {col_name:?}", wal_nullable);
            return Ok(NullableResult::named(Some(wal_nullable), &col_name));
        }

        // check table nullable in wal
        if let Some(wal_nullable) = self.nullable_for_table(table) {
            println!(
                "found table null {} {col_name:?} {:?}",
                wal_nullable, table.table_id
            );
            if wal_nullable {
                return Ok(NullableResult::named(Some(wal_nullable), &col_name));
            }
        }

        Ok(NullableResult::named(Some(col.catalog_nullable), &col_name))
    }
    pub fn nullable_for_ident(&self, name: &[Ident]) -> anyhow::Result<NullableResult> {
        let (col, table) = self.find_col_by_idents(name)?;
        self.nullable_for_table_col(table, &col)
    }
    pub fn find_col_by_idents(&self, name: &[Ident]) -> anyhow::Result<(TableColumn, &Table)> {
        // search for col
        if name.len() == 1 {
            for table in self.tables.0.iter() {
                for col in &table.columns {
                    if col.column_name == name[0] {
                        return Ok((col.clone(), table));
                    }
                }
            }
        }

        // look for original name: `table_alias`.`col_name`
        if let Some(table) = self
            .tables
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
            .tables
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

    pub fn push(&mut self, mut table: Table) {
        for cur_table in self.tables.0.iter() {
            // don't insert duplicate tables
            if cur_table.equals(&table) {
                return;
            }
        }

        table.table_id = TableId::new(self.tables.len());

        for col in table.columns.iter_mut() {
            col.table_id = table.table_id
        }

        self.tables.0.push(table)
    }

    pub fn nullable_for_table(&self, table: &Table) -> Option<bool> {
        for row in self.wal.data.iter().rev() {
            match row {
                WalEntry::TableNullable { table_id, nullable } if *table_id == table.table_id => {
                    return Some(*nullable)
                }
                _ => continue,
            }
        }
        None
    }
}
