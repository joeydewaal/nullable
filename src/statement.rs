use sqlparser::ast::Statement;

use crate::{nullable::StatementNullable, query::nullable_from_query, Source};

pub fn nullable_from_statement(statement: &Statement, source: &Source) -> StatementNullable {
    match statement {
        Statement::Query(query) => nullable_from_query(query, source),
        _ => Default::default(),
    }
}

