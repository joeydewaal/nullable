use std::collections::HashSet;

use anyhow::anyhow;
use sqlparser::ast::{
    Expr, Ident, JoinConstraint, JoinOperator, Select, TableFactor, TableWithJoins, With,
};

use crate::{
    cte::visit_cte,
    expr::get_nullable_col,
    join_resolver::JoinResolver,
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

    pub fn update_nullable_from_select_where(&mut self, select: &Select) -> anyhow::Result<()> {
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

        todo!();
        // self.tables.apply(x);

        Ok(())
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

    pub fn update_nullable_from_select_joins(&mut self, select: &Select) {
        for table in &select.from {
            for join in &table.joins {
                let base_table = self.find_table_by_table_factor(&table.relation).unwrap();
                let base_table_nullable = self.nullable_for_table(&base_table);
                let left_table = self.find_table_by_table_factor(&join.relation).unwrap();

                match &join.join_operator {
                    JoinOperator::LeftOuter(inner) => {
                        self.handle_join_constraint(
                            &inner,
                            &left_table,
                            |left_table, right_table| {
                                println!(
                                    "left joined {:?} on {:?}",
                                    &left_table.0.table_name,
                                    right_table
                                        .iter()
                                        .map(|t| t.0.table_name.clone())
                                        .collect::<Vec<_>>()
                                );
                                (Some(true), vec![None; right_table.len()])
                            },
                        );
                    }
                    JoinOperator::Inner(inner) => {
                        self.handle_join_constraint(
                            &inner,
                            &left_table,
                            |(left_table, left_nullable), right_table| {
                                println!(
                                    "inner joined {:?} on {:?}",
                                    &left_table.table_name,
                                    right_table
                                        .iter()
                                        .map(|t| t.0.table_name.clone())
                                        .collect::<Vec<_>>()
                                );

                                if let Some(index) =
                                    right_table.iter().enumerate().find_map(|(i, t)| {
                                        if t.0.equals(&base_table) {
                                            Some(i)
                                        } else {
                                            None
                                        }
                                    })
                                {
                                    println!("joined on base table");

                                    if base_table_nullable == Some(true) {
                                        println!(
                                            "base table: {:?} nullable",
                                            base_table.table_name
                                        );
                                    }
                                    let mut right_nullable = vec![None; right_table.len()];
                                    right_nullable[index] = Some(false);
                                    return (Some(false), right_nullable);
                                }
                                if right_table.iter().any(|t| t.1 == Some(true)) {
                                    (Some(true), vec![None; right_table.len()])
                                } else {
                                    (Some(false), vec![Some(false); right_table.len()])
                                }
                            },
                        );
                    }
                    JoinOperator::RightOuter(inner) => {
                        self.handle_join_constraint(
                            &inner,
                            &left_table,
                            |(left_table, _left_nullable), right_table| {
                                println!(
                                    "right joined {:?} on {:?}",
                                    &left_table.table_name,
                                    right_table
                                        .iter()
                                        .map(|t| t.0.table_name.clone())
                                        .collect::<Vec<_>>()
                                );

                                (
                                    Some(false),
                                    right_table
                                        .iter()
                                        .map(|t| {
                                            if t.0.equals(&left_table) {
                                                None
                                            } else {
                                                Some(true)
                                            }
                                        })
                                        .collect(),
                                )
                            },
                        );
                    }
                    _ => (),
                }
            }
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

    fn handle_join_constraint(
        &mut self,
        constraint: &JoinConstraint,
        left_joined_table: &Table,
        callback: impl Fn(
            &(Table, Option<bool>),
            &[(Table, Option<bool>)],
        ) -> (Option<bool>, Vec<Option<bool>>),
    ) {
        // println!("left_joined_col {:#?}", left_joined_table.table_name);
        match &constraint {
            JoinConstraint::On(expr) => {
                let mut t = HashSet::new();
                self.recursive_find_joined_tables(expr, &mut t);
                let right_tables: Vec<(Table, Option<bool>)> = t
                    .into_iter()
                    .map(|t| (t.clone(), self.nullable_for_table(&t)))
                    .collect();

                let left_table = right_tables
                    .iter()
                    .find_map(|(table, null)| {
                        if table.equals(&left_joined_table) {
                            Some((table.clone(), *null))
                        } else {
                            None
                        }
                    })
                    .unwrap();

                let (nullable1, nullable2) = (callback)(&left_table, &right_tables);

                for (nullable2, table) in nullable2.iter().zip(right_tables.clone()) {
                    if let Some(null2) = nullable2 {
                        self.wal.add_table(table.0.table_id, *null2);
                    }
                }
                if let Some(null1) = nullable1 {
                    self.wal.add_table(left_table.0.table_id, null1);
                }

                for right_t in right_tables.iter() {
                    for table in self.tables.0.iter_mut() {
                        if table.equals(&left_table.0)
                            && !right_t.0.equals(&left_table.0)
                            && !right_t.0.equals(&left_table.0)
                        {
                            println!(
                                "adding {:?} to {:?}",
                                right_t.0.table_name, table.table_name
                            );
                            table.add_dependent(&right_t.0)
                        }
                    }
                }
            }
            _ => (),
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
            _ => (),
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

    pub fn nullable_for_ident(&self, name: &[Ident]) -> anyhow::Result<NullableResult> {
        let (col, table) = self.find_col_by_idents(name)?;

        // check col nullable in wal
        if let Some(wal_nullable) = self.wal.nullable_for_col(table, col.column_id) {
            println!("found col null {} {name:?}", wal_nullable);
            if wal_nullable {
                return Ok(NullableResult::named(Some(wal_nullable), name));
            }
        }

        // check table nullable in wal
        if let Some(wal_nullable) = self.nullable_for_table(table) {
            println!(
                "found table null {} {name:?} {:?} {:?}",
                wal_nullable, table.table_id, table.dependants
            );
            if wal_nullable {
                return Ok(NullableResult::named(Some(wal_nullable), name));
            }
        }

        Ok(NullableResult::named(Some(col.catalog_nullable), name))
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
        if let Some(dep) = table.dependants.first() {
            let table = self.tables.find_table_id(*dep).unwrap();
            return self.nullable_for_table(table);
        }

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

    pub fn update_from_join(&mut self, select: &Select) {
        for table in &select.from {
            let base_table = self.find_table_by_table_factor(&table.relation).unwrap();

            let mut join_resolver = JoinResolver::from_base(base_table.table_id);

            for join in &table.joins {
                let left_table = self.find_table_by_table_factor(&join.relation).unwrap();

                match &join.join_operator {
                    JoinOperator::LeftOuter(inner) => {
                        self.handle_join_constraint2(
                            &mut join_resolver,
                            &inner,
                            &left_table,
                            |left_table, right_table, resolver| {
                                println!("left joined {:?} on {:?}", &left_table, right_table);
                                for r_table in right_table {
                                    if *r_table != left_table {
                                        resolver.set_nullable(*r_table, Some(true));
                                    }
                                }
                                resolver.set_nullable(left_table, None);
                            },
                        );
                    }
                    JoinOperator::Inner(inner) => {
                        self.handle_join_constraint2(
                            &mut join_resolver,
                            &inner,
                            &left_table,
                            |left_table, right_table, resolver| {
                                println!("inner joined {:?} on {:?}", &left_table, right_table);
                                for r_table in right_table {
                                    if *r_table != left_table {
                                        resolver.set_nullable(*r_table, None);
                                    }
                                }
                                resolver.set_nullable(left_table, None);
                            },
                        );
                    }
                    JoinOperator::RightOuter(inner) => {
                        self.handle_join_constraint2(
                            &mut join_resolver,
                            &inner,
                            &left_table,
                            |left_table, right_table, resolver| {
                                println!("right joined {:?} on {:?}", &left_table, right_table);
                                for r_table in right_table {
                                    if *r_table != left_table {
                                        resolver.set_nullable(*r_table, Some(true));
                                    }
                                }
                                resolver.set_nullable(left_table, Some(false));
                            },
                        );
                    }
                    _ => (),
                }
            }
            dbg!(&join_resolver);
            let alefkjealskfj = join_resolver.get_nullables();
            dbg!(&alefkjealskfj);
            for (table_id, nullable) in alefkjealskfj {
                self.wal.add_table(table_id, nullable);
            }
        }
    }

    fn handle_join_constraint2(
        &mut self,
        join_resolver: &mut JoinResolver,
        constraint: &JoinConstraint,
        left_joined_table: &Table,
        callback: impl Fn(TableId, &[TableId], &mut JoinResolver),
    ) {
        // println!("left_joined_col {:#?}", left_joined_table.table_name);
        match &constraint {
            JoinConstraint::On(expr) => {
                let mut t = HashSet::new();
                self.recursive_find_joined_tables(expr, &mut t);
                let right_tables: Vec<_> = t.into_iter().map(|t| t.table_id).collect();

                let left_table = right_tables
                    .iter()
                    .find(|table| **table == left_joined_table.table_id)
                    .unwrap();

                for right_table in &right_tables {
                    join_resolver.add_leaf(*right_table, *left_table, None);
                }

                let _ = (callback)(*left_table, &right_tables, join_resolver);
            }
            _ => (),
        }
    }
}
