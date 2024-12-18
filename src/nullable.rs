use sqlparser::ast::Ident;

#[derive(Debug)]
pub enum NullablePlace {
    Named { name: Vec<Ident> },
    Indexed { index: usize },
    Unnamed,
}

#[derive(Debug)]
pub struct NullableResult {
    pub place: NullablePlace,
    pub value: Option<bool>,
}

impl NullableResult {
    pub fn named(value: Option<bool>, _name: &[Ident]) -> Self {
        Self {
            place: NullablePlace::Named {
                name: _name.to_vec()
            },
            value,
        }
    }

    pub fn unnamed(value: Option<bool>) -> Self {
        Self {
            value,
            place: NullablePlace::Unnamed
        }
    }

    pub fn set_alias(self, alias: Option<Ident>) -> Self {
        if let Some(alias) = alias {
            Self {
                place: NullablePlace::Named {
                    name: vec![alias]
                },
                value: self.value,
            }
        } else {
            self
        }
    }
}

#[derive(Default, Debug)]
pub struct Nullable(Vec<NullableResult>);

impl Nullable {
    pub fn empty() -> Self {
        Self::new(vec![])
    }

    pub fn add(&mut self, nullable: NullableResult) {
        self.0.push(nullable);
    }

    pub fn new(inner: Vec<NullableResult>) -> Self {
        Self(inner)
    }
}

#[derive(Default, Debug)]
pub struct StatementNullable(Vec<Nullable>);

impl From<Nullable> for StatementNullable {
    fn from(value: Nullable) -> Self {
        Self(vec![value])
    }
}

impl StatementNullable {
    pub fn new() -> Self {
        Self(vec![])
    }

    pub fn combine(&mut self, mut null: Self) {
        self.0.append(&mut null.0)
    }

    pub fn get_nullable(mut self) -> Vec<Option<bool>> {
        let Some(mut inferred_nullable): Option<Vec<Option<bool>>> = self
            .0
            .pop()
            .map(|e| e.0.into_iter().map(|e| e.value).collect())
        else {
            return vec![];
        };

        for row in self.0.iter() {
            for (i, col) in row.0.iter().enumerate() {
                inferred_nullable[i] = match (inferred_nullable[i], col.value) {
                    (Some(first), Some(second)) => Some(first || second),
                    (Some(first), None) => Some(first),
                    (None, Some(second)) => Some(second),
                    (None, None) => None,
                };
            }
        }
        inferred_nullable
    }

    pub fn get_nullable_final(self) -> Vec<bool> {
        self.get_nullable()
            .into_iter()
            .map(|inferred| inferred.unwrap_or(true))
            .collect()
    }
}
