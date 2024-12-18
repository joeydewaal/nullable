use std::collections::HashSet;

use sqlparser::ast::{
    Expr, JoinConstraint, JoinOperator, Select, TableFactor, TableWithJoins, With,
};

use crate::{cte::visit_cte, expr::get_nullable_col, Source, Table, Tables};

pub struct Context {
    pub tables: Tables,
    pub source: Source,
}

impl Context {
    pub fn new(tables: Tables, source: Source) -> Context {
        Self { tables, source }
    }

    pub fn update_nullable_from_select_where(&mut self, select: &Select) -> anyhow::Result<()> {
        let Some(ref selection) = select.selection else {
            return Ok(());
        };

        let x = get_nullable_col(selection, self)?;
        dbg!(&x);
        self.tables.apply(x);
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
                println!("{:?} {:?}", name, alias);
                let mut table = self.source.find_by_original_name(&name.0).unwrap();
                table.add_alias(alias.as_ref().map(|alias| &alias.name));
                self.tables.push(table);
            }
            _ => (),
        }
    }

    pub fn update_nullable_from_select_joins(&mut self, select: &Select) {
        for table in &select.from {
            for join in &table.joins {
                let base_table = self.find_table_by_table_factor(&table.relation).unwrap();
                let left_table = self.find_table_by_table_factor(&join.relation).unwrap();

                match &join.join_operator {
                    JoinOperator::LeftOuter(inner) => {
                        self.handle_join_constraint(
                            &inner,
                            &left_table,
                            |left_table, right_table| {
                                println!(
                                    "left joined {:?} on {:?}",
                                    (&left_table.table_name, &left_table.table_name),
                                    right_table
                                        .iter()
                                        .map(|t| (t.table_name.clone(), t.table_name.clone()))
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
                                    "inner joined {:?} on {:?}",
                                    (&left_table.table_name, &left_table.table_name),
                                    right_table
                                        .iter()
                                        .map(|t| (t.table_name.clone(), t.table_name.clone()))
                                        .collect::<Vec<_>>()
                                );

                                if let Some(index) =
                                    right_table.iter().enumerate().find_map(|(i, t)| {
                                        if t.equals(&base_table) {
                                            Some(i)
                                        } else {
                                            None
                                        }
                                    })
                                {
                                    println!("joined on base table");

                                    if base_table.table_nullable == Some(true) {
                                        println!(
                                            "base table: {:?} nullable",
                                            base_table.table_name
                                        );
                                    }
                                    let mut right_nullable = vec![None; right_table.len()];
                                    right_nullable[index] = Some(false);
                                    return (Some(false), right_nullable);
                                }

                                if right_table.iter().any(|t| t.table_nullable == Some(true)) {
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
                            |left_table, right_table| {
                                println!(
                                    "right joined {:?} on {:?}",
                                    (&left_table.table_name, &left_table.table_name),
                                    right_table
                                        .iter()
                                        .map(|t| (t.table_name.clone(), t.table_name.clone()))
                                        .collect::<Vec<_>>()
                                );

                                (
                                    Some(false),
                                    right_table
                                        .iter()
                                        .map(|t| {
                                            if t.equals(&left_table) {
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
        callback: impl Fn(&Table, &[Table]) -> (Option<bool>, Vec<Option<bool>>),
    ) {
        // println!("left_joined_col {:#?}", left_joined_table.table_name);
        match &constraint {
            JoinConstraint::On(expr) => {
                let mut t = HashSet::new();
                self.recursive_find_joined_tables(expr, &mut t);
                let right_tables: Vec<Table> = t.into_iter().collect();

                let left_table = right_tables
                    .iter()
                    .find(|table| table.equals(&left_joined_table))
                    .unwrap();

                let (nullable1, nullable2) = (callback)(&left_table, &right_tables);

                for (nullable2, table) in nullable2.iter().zip(right_tables.clone()) {
                    if let Some(null2) = nullable2 {
                        self.tables.set_table_nullable(table.table_id, *null2);
                    }
                }
                if let Some(null1) = nullable1 {
                    self.tables.set_table_nullable(left_table.table_id, null1);
                }

                for right_t in right_tables.iter() {
                    for table in self.tables.0.iter_mut() {
                        if table.equals(right_t) && !left_table.equals(&right_t) {
                            table.add_dependent(&left_table)
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
                println!("`{idents:?}`");
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
}
