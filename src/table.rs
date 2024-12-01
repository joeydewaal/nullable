use crate::{
    state::{ColumnExpr, Function},
    Column,
};
use sqlparser::ast::{CastKind, Expr, Ident, SelectItem, TableFactor, TableWithJoins, Value};
use std::{collections::HashSet, ops::Deref};

#[derive(Debug, Clone)]
pub struct Source {
    tables: Vec<Table>,
}

impl Source {
    pub fn new(tables: Vec<Table>) -> Self {
        Source { tables }
    }

    pub fn find_table(&self, table_name: &str) -> Option<&Table> {
        self.tables.iter().find(|t| t.table_name == table_name)
    }

    pub fn empty() -> Self {
        Self { tables: Vec::new() }
    }
}

#[derive(Default, Debug)]
pub struct Tables(Vec<Table>);

impl Deref for Tables {
    type Target = Vec<Table>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Tables {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn visit_join_active_table(&mut self, table: &TableWithJoins, source: &Source) {
        self.visit_table_factor(&&table.relation, source);
        for join_table in &table.joins {
            self.visit_table_factor(&&join_table.relation, source);
        }
    }

    pub fn push(&mut self, table: Table) {
        for cur_table in self.0.iter() {
            // don't insert duplicate tables
            if cur_table == &table {
                return;
            }
        }

        self.0.push(table)
    }

    pub fn recursive_find_joined_tables(&self, expr: &Expr, tables: &mut HashSet<Table>) {
        match expr {
            Expr::CompoundIdentifier(idents) => {
                let table = self.find_table_by_idents(&idents).unwrap();

                tables.insert(table.1.clone());
            }
            Expr::BinaryOp { left, op: _, right } => {
                self.recursive_find_joined_tables(&left, tables);
                self.recursive_find_joined_tables(&right, tables);
            }
            _ => (),
        }
    }

    pub fn visit_table_factor(&mut self, table: &TableFactor, sources: &Source) {
        match table {
            TableFactor::Table { name, alias, .. } => {
                for ident in name.0.iter() {
                    let mut table = sources
                        .find_table(&ident.value)
                        .cloned()
                        .expect("Could not find table in active tables");

                    if let Some(alias) = alias {
                        table.alias = Some(alias.name.value.clone());
                    }

                    self.push(table);
                }
            }
            _ => (),
        }
    }

    pub fn push_select_item(&self, items: &SelectItem) -> ColumnExpr {
        match items {
            SelectItem::UnnamedExpr(expr) => {
                return self.visit_expr(expr, None).unwrap_or_default()
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                return self
                    .visit_expr(expr, Some(alias.value.clone()))
                    .unwrap_or_default();
            }
            _ => (),
        }
        ColumnExpr::Unknown
    }

    pub fn visit_expr(&self, expr: &Expr, alias: Option<String>) -> Option<ColumnExpr> {
        match expr {
            Expr::CompoundIdentifier(idents) => {
                let (col_name, table) = self
                    .find_table_by_idents(&idents)
                    .expect(&format!("Could not find column for {:?}", &idents));
                return Some(ColumnExpr::Column(Column::new(
                    col_name.column_name,
                    None,
                    Some(table.clone()),
                )));
            }
            Expr::Identifier(col_name) => {
                let (_, table) = self.find_table(&col_name.value, None).unwrap();
                return Some(ColumnExpr::Column(Column::new(
                    &col_name.value,
                    None,
                    Some(table.clone()),
                )));
            }
            Expr::Function(func) => return Some(ColumnExpr::from_func(func, alias)),
            Expr::Exists {
                subquery: _,
                negated: _,
            } => {
                return Some(ColumnExpr::Function(Function::Known {
                    function_name: "exists".into(),
                    alias,
                    inferred_nullable: Some(false),
                }))
            }
            Expr::Value(value) => {
                let nullable = match value {
                    Value::Null => Some(true),
                    _ => Some(false),
                };
                return Some(ColumnExpr::from_value(alias, nullable));
            }
            Expr::Cast {
                kind: CastKind::DoubleColon,
                expr,
                data_type: _,
                format: _,
            } => {
                return self.visit_expr(expr, alias);
            }
            _ => None,
        }
    }

    pub fn find_table(
        &self,
        col_name: &str,
        table_name: Option<&str>,
    ) -> Option<(TableColumn, &Table)> {
        let find_col = |table: &Table| {
            table
                .columns
                .iter()
                .find(move |c| c.column_name == col_name)
                .cloned()
        };

        if let Some(table_name) = table_name {
            let opt_table = self.0.iter().find(|table| table.table_name == table_name);

            return opt_table.map(|t| (find_col(&t).unwrap(), t));
        }
        let mut iterator = self.0.iter().filter(|table| {
            table
                .columns
                .iter()
                .find(|col| col.column_name == col_name)
                .is_some()
        });

        let opt_table = iterator.next();
        assert!(iterator.next().is_none());
        return opt_table.map(|t| (find_col(&t).unwrap(), t));
    }

    pub fn find_table_by_alias(
        &self,
        col_name: &str,
        alias: &String,
    ) -> Option<(TableColumn, &Table)> {
        if let Some(table) = self.0.iter().find(|t| t.alias.as_deref() == Some(alias)) {
            if let Some(col) = table.columns.iter().find(|c| c.column_name == col_name) {
                return Some((col.clone(), table));
            }
        };
        None
    }

    pub fn find_table_by_idents_table(&self, name: &[Ident]) -> Option<&Table> {
        let table_name = name.first()?;

        self.0.iter().find(|t| t.table_name == table_name.value)
    }

    pub fn find_table_by_idents(&self, name: &[Ident]) -> Option<(TableColumn, &Table)> {
        let (col_name, table_name): (&str, Option<&String>) = {
            let first = name.first()?;
            let second = name.get(1);

            if second.is_none() {
                (&first.value, second.map(|c| &c.value))
            } else {
                (second.map(|c| &c.value)?, Some(&first.value))
            }
        };

        if let Some(opt_alias) = table_name {
            if let Some(x) = self.find_table(&col_name, table_name.map(|t| t.as_str())) {
                return Some(x);
            } else {
                return self.find_table_by_alias(&col_name, opt_alias);
            }
        }

        return self.find_table(&col_name, table_name.map(|t| t.as_str()));
    }

    pub fn table_from_expr(
        &self,
        expr: &Expr,
        recursive_left: bool,
    ) -> Option<(TableColumn, Table)> {
        match &expr {
            Expr::CompoundIdentifier(idents) => self
                .find_table_by_idents(&idents)
                .map(|t| (t.0, t.1.clone())),
            Expr::BinaryOp { left, op: _, right } => {
                if recursive_left {
                    return self.table_from_expr(left, recursive_left);
                } else {
                    return self.table_from_expr(right, recursive_left);
                }
            }
            _ => None,
        }
    }

    pub fn set_table_nullable(&mut self, table: &Table, nullable: bool) {
        for row in self.0.iter_mut() {
            if table.equals(row) {
                row.table_nullable = nullable;
            }
        }
    }

    pub fn find_table_by_table_factor(&self, factor: &TableFactor) -> Option<Table> {
        match &factor {
            TableFactor::Table { name, alias, .. } => {
                dbg!(&name, alias);
                if let Some(alias) = alias {
                    return self
                        .0
                        .iter()
                        .find(|t| t.alias.as_ref() == Some(&alias.name.value))
                        .cloned();
                }
                self.find_table_by_idents_table(&name.0).cloned()
            }
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Table {
    pub table_nullable: bool,
    pub table_name: String,
    pub alias: Option<String>,
    pub columns: Vec<TableColumn>,
}

impl Table {
    pub fn new(table_name: impl Into<String>) -> Self {
        Self {
            table_name: table_name.into(),
            columns: Vec::new(),
            table_nullable: false,
            alias: None,
        }
    }

    pub fn push_column(mut self, column_name: impl Into<String>, catalog_nullable: bool) -> Self {
        self.columns.push(TableColumn::new(
            column_name,
            catalog_nullable,
            &self.table_name,
        ));
        self
    }

    pub fn equals(&self, other: &Self) -> bool {
        if self.alias.is_none() && other.alias.is_none() {
            return self.table_name == other.table_name;
        }

        self.alias == other.alias && self.table_name == other.table_name
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TableColumn {
    pub column_name: String,
    pub catalog_nullable: bool,
    pub table_name: String,
}

impl TableColumn {
    pub fn new(
        column_name: impl Into<String>,
        catalog_nullable: bool,
        table_name: impl Into<String>,
    ) -> Self {
        Self {
            column_name: column_name.into(),
            catalog_nullable,
            table_name: table_name.into(),
        }
    }
}
