use std::{
    ops::{Deref, DerefMut},
    time::Instant,
};

use sqlparser::ast::{Function as SqlFunction, FunctionArg, FunctionArguments, ObjectName};

use sqlparser::{ast::Statement, dialect::PostgreSqlDialect, parser::Parser};

use crate::{
    statement::{nullable_from_statement, StatementExpr},
    table::Source,
    Table,
};

pub struct NullableState {
    parsed_query: Vec<Statement>,
    source: Source,
    started: Instant,
}

impl NullableState {
    pub fn new(query: &str, source: Source) -> Self {
        let query = Parser::parse_sql(&PostgreSqlDialect {}, query).unwrap();

        Self {
            parsed_query: query,
            source,
            started: Instant::now(),
        }
    }

    pub fn get_nullable(&mut self) -> Vec<bool> {
        dbg!(&self.parsed_query);
        let s = self.parsed_query.first().unwrap();
        let inferred_nullable = nullable_from_statement(&s, &self.source);
        println!("{:?}", self.started.elapsed());

        println!("{:#?}", inferred_nullable);
        inferred_nullable.get_nullable()
    }
}

#[derive(Debug, Default)]
pub struct ColumnExprs(Vec<ColumnExpr>);

impl ColumnExprs {
    pub fn new() -> Self {
        Self(vec![])
    }
}

impl Deref for ColumnExprs {
    type Target = Vec<ColumnExpr>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for ColumnExprs {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl ColumnExprs {}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum ColumnExpr {
    Column(Column),
    Function(Function),
    Value(ValueExpr),
    #[default]
    Unknown,
}

impl ColumnExpr {
    pub fn get_nullable(&self, statement: &StatementExpr, source: &Source) -> bool {
        match self {
            ColumnExpr::Column(c) => c.get_nullable(),
            ColumnExpr::Function(f) => f.get_nullable(statement, source).unwrap_or(true),
            ColumnExpr::Unknown => true,
            ColumnExpr::Value(v) => v.nullable.unwrap_or(true),
        }
    }

    pub fn from_func(func: &SqlFunction, alias: Option<String>) -> Self {
        Self::Function(Function::new(func, alias))
    }

    pub fn from_value(alias: Option<String>, nullable: Option<bool>) -> Self {
        Self::Value(ValueExpr::new(alias, nullable))
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Column {
    #[allow(unused)]
    pub name: String,
    pub inferred_nullable: Option<bool>,
    pub catalog_nullable: Option<bool>,
    pub table: Option<ColumnTable>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ColumnTable {
    pub table_name: String,
    pub table_alias: Option<String>,
}

impl ColumnTable {
    pub fn new(name: impl Into<String>, alias: Option<String>) -> Self {
        Self {
            table_name: name.into(),
            table_alias: alias,
        }
    }
    pub fn equals(&self, other: &Table) -> bool {
        if self.table_alias.is_none() && other.alias.is_none() {
            return self.table_name == other.table_name;
        }

        self.table_alias == other.alias && self.table_name == other.table_name
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ValueExpr {
    alias: Option<String>,
    nullable: Option<bool>,
}

impl ValueExpr {
    pub fn new(alias: Option<String>, nullable: Option<bool>) -> Self {
        Self { alias, nullable }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Function {
    Unknown {
        alias: Option<String>,
        func: SqlFunction,
    },
    Known {
        alias: Option<String>,
        function_name: String,
        inferred_nullable: Option<bool>,
    },
}

impl Function {
    pub fn new(func: &SqlFunction, alias: Option<String>) -> Self {
        if let Some(function_name) = Self::func_name(&func.name) {
            let inferred_nullable = match function_name.to_lowercase().as_ref() {
                "count" => Some(false),
                "current_user" => Some(false),
                "now" => Some(false),
                _ => None,
            };

            if let Some(nullable) = inferred_nullable {
                return Self::Known {
                    alias,
                    function_name: function_name.clone(),
                    inferred_nullable: Some(nullable),
                };
            }
        };

        Self::Unknown {
            func: func.clone(),
            alias,
        }
    }

    pub fn non_null_more_than_one(func: &SqlFunction) -> bool {
        if !Self::function_arg_len(&func.args)
            .map(|len| len >= 0)
            .unwrap_or(false)
        {
            return false;
        }
        return true;
    }

    pub fn get_nullable(&self, statement: &StatementExpr, source: &Source) -> Option<bool> {
        match &self {
            Function::Known {
                alias: _,
                function_name: _,
                inferred_nullable,
            } => *inferred_nullable,
            Function::Unknown { alias: _, func } => {
                let func_name = Self::func_name(&func.name)?;

                match func_name.to_lowercase().as_str() {
                    "avg" => {
                        let nullable = Self::args_nullable(&func.args);
                        let x = 3;
                    }
                    _ => (),
                }

                Some(true)
            }
        }
    }

    pub fn args_nullable(args: &FunctionArguments) -> Option<Vec<Option<bool>>> {
        match args {
            FunctionArguments::None => None,
            FunctionArguments::List(list) => {
                for arg in &list.args {}
                Some(vec![])
            }
            FunctionArguments::Subquery(query) => None,
        }
    }

    pub fn arg_nullable(arg: &FunctionArg, statement: &StatementExpr) -> Option<bool> {
        match arg {
            FunctionArg::Unnamed(expr) => match expr {
                sqlparser::ast::FunctionArgExpr::Expr(expr) => statement.expr_nullable(&expr)?.pop()?,
                _ => None,
            },
            _ => None,
        }
    }

    pub fn function_arg_len(args: &FunctionArguments) -> Option<usize> {
        match args {
            FunctionArguments::List(list) => Some(list.args.len()),
            _ => None,
        }
    }

    fn func_name(obj: &ObjectName) -> Option<String> {
        obj.0.first().map(|i| i.value.clone())
    }
}

impl Column {
    pub fn new(
        name: impl Into<String>,
        inferred_nullable: Option<bool>,
        table_name: Option<Table>,
    ) -> Self {
        let name = name.into();
        let catalog_nullable = table_name
            .as_ref()
            .map(|t| {
                t.columns.iter().find_map(|c| {
                    if c.column_name == name {
                        return Some(c.catalog_nullable);
                    }
                    None
                })
            })
            .flatten();
        Self {
            name,
            inferred_nullable,
            table: table_name.map(|t| ColumnTable::new(t.table_name, t.alias)),
            catalog_nullable,
        }
    }

    pub fn get_nullable(&self) -> bool {
        if self.catalog_nullable == Some(true) {
            return true;
        }

        if let Some(inferred) = self.inferred_nullable {
            return inferred;
        }

        false
    }
}
