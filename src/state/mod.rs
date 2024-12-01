use std::time::Instant;

use sqlparser::{ast::Statement, dialect::PostgreSqlDialect, parser::Parser};

use crate::{statement::nullable_from_statement, table::Source};

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
        inferred_nullable.get_nullable()
    }
}
