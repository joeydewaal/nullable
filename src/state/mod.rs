use std::time::Instant;

use sqlparser::{ast::Statement, parser::Parser};

use crate::{
    context::Context, statement::nullable_from_statement, table::Source, SqlFlavour, Tables,
};

pub struct NullableState {
    parsed_query: Vec<Statement>,
    source: Source,
    started: Instant,
}

impl NullableState {
    pub fn new(query: &str, source: Source, flavour: SqlFlavour) -> Self {
        let query = Parser::parse_sql(flavour.to_dialect(), query).unwrap();

        Self {
            parsed_query: query,
            source,
            started: Instant::now(),
        }
    }

    pub fn get_nullable(&mut self, cols: &[&str]) -> Vec<bool> {
        dbg!(&self.parsed_query);
        let s = self.parsed_query.first().unwrap();

        let mut context = Context::new(Tables::new(), self.source.clone());

        let inferred_nullable = nullable_from_statement(&s, &mut context).unwrap();
        println!("{:?}", self.started.elapsed());
        inferred_nullable.get_nullable_final(cols)
    }
}
