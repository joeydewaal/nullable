use std::{collections::HashSet, ops::Deref};

use sqlparser::ast::{Expr, JoinConstraint, JoinOperator, Select, Statement};

use crate::{
    nullable::StatementNullable, query::nullable_from_query, state::ColumnExpr, ColumnExprs,
    Source, Table, Tables,
};

#[derive(Debug)]
pub struct StatementExpr {
    pub tables: Tables,
    pub cols: ColumnExprs,
}

impl StatementExpr {
    pub fn new() -> Self {
        Self {
            tables: Tables::new(),
            cols: ColumnExprs::new(),
        }
    }

    pub fn expr_nullable(&self, expr: &Expr) -> Option<Vec<Option<bool>>> {
        match expr {
            Expr::CompoundIdentifier(ident) => {
                let x = self.tables.find_table_by_idents(&ident)?;
                return None;
            }
            _ => None,
        }
    }

    pub fn get_nullable(&self, source: &Source) -> Vec<bool> {
        self.cols
            .iter()
            .map(|c| c.get_nullable(&self, source))
            .collect()
    }

    pub fn update_nullable_from_select(&mut self, select: &Select) {
        for table in &select.from {
            for join in &table.joins {
                let left_table = self
                    .tables
                    .find_table_by_table_factor(&join.relation)
                    .unwrap();

                match &join.join_operator {
                    JoinOperator::LeftOuter(inner) => {
                        self.handle_join_constraint(
                            &inner,
                            &left_table,
                            |left_table, right_table| {
                                println!(
                                    "{:?} left joined on {:?}",
                                    (&left_table.table_name, &left_table.alias),
                                    right_table
                                        .iter()
                                        .map(|t| (t.table_name.clone(), t.alias.clone()))
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
                            |left_table, right_table| {
                                println!(
                                    "{:?} inner joined on {:?}",
                                    (&left_table.table_name, &left_table.alias),
                                    right_table
                                        .iter()
                                        .map(|t| (t.table_name.clone(), t.alias.clone()))
                                        .collect::<Vec<_>>()
                                );
                                if right_table.iter().any(|t| t.table_nullable) {
                                    (Some(true), vec![None; right_table.len()])
                                } else {
                                    (Some(false), vec![None; right_table.len()])
                                }
                            },
                        );
                    }
                    _ => (),
                }
            }
        }
    }
    fn handle_join_constraint(
        &mut self,
        constraint: &JoinConstraint,
        left_joined_table: &Table,
        callback: impl Fn(&Table, &[Table]) -> (Option<bool>, Vec<Option<bool>>),
    ) {
        // println!("left_joined_col {:#?}", left_joined_table.table_name);
        match &constraint {
            JoinConstraint::On(expr) => {
                let mut t = HashSet::new();
                self.tables.recursive_find_joined_tables(expr, &mut t);
                let right_tables: Vec<Table> = t.into_iter().collect();

                let left_table = right_tables
                    .iter()
                    .find(|table| table.equals(&left_joined_table))
                    .unwrap();

                let (nullable1, nullable2) = (callback)(&left_table, &right_tables);

                if let Some(null1) = nullable1 {
                    self.set_table_nullable(&left_table, null1);
                    println!(
                        "after: {:?} is {}",
                        (&left_table.table_name, &left_table.alias),
                        null1,
                    );
                }

                for (nullable2, table) in nullable2.iter().zip(right_tables) {
                    if let Some(null2) = nullable2 {
                        self.set_table_nullable(&table, *null2);
                        println!(
                            "after: {:?} is {}",
                            (&table.table_name, &table.alias),
                            null2,
                        );
                    }
                }
            }
            _ => (),
        }
    }

    fn set_table_nullable(&mut self, table: &Table, nullable: bool) {
        self.tables.set_table_nullable(table, nullable);

        for col in self.cols.iter_mut() {
            if let ColumnExpr::Column(column) = col {
                if let Some(table1) = &column.table {
                    if table1.equals(&table) {
                        column.inferred_nullable = Some(nullable)
                    }
                }
            }
        }
    }
}

#[derive(Debug)]
pub struct StatementExprs(Vec<StatementExpr>);

impl Deref for StatementExprs {
    type Target = Vec<StatementExpr>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub fn nullable_from_statement(statement: &Statement, source: &Source) -> StatementNullable {
    match statement {
        Statement::Query(query) => nullable_from_query(query, source),
        _ => Default::default(),
    }
}
