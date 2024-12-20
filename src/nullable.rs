use sqlparser::ast::Ident;

#[derive(Debug)]
pub enum NullablePlace {
    Named { name: Vec<Ident> },
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
                name: _name.to_vec(),
            },
            value,
        }
    }

    pub fn unnamed(value: Option<bool>) -> Self {
        Self {
            value,
            place: NullablePlace::Unnamed,
        }
    }

    pub fn set_alias(self, alias: Option<Ident>) -> Self {
        if let Some(alias) = alias {
            Self {
                place: NullablePlace::Named { name: vec![alias] },
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

    pub fn append(&mut self, nullable: &mut Vec<NullableResult>) {
        self.0.append(nullable);
    }

    pub fn new(inner: Vec<NullableResult>) -> Self {
        Self(inner)
    }

    pub fn into_iter(self) -> impl Iterator<Item = NullableResult> {
        self.0.into_iter()
    }
    pub fn nullable(&self, col_name: &str, index: usize) -> Option<bool> {
        let col_name = Ident::new(col_name);

        if let Some((left_index, left_nullable)) = self.l_find_index(&col_name) {
            if let Some((right_index, _right_nullable)) = self.r_find_index(&col_name) {
                if left_index == right_index {
                    return left_nullable;
                }
            }
        }
        self.0[index].value
    }

    fn l_find_index(&self, col_name: &Ident) -> Option<(usize, Option<bool>)> {
        self.0
            .iter()
            .enumerate()
            .find_map(|(index, nullable)| match &nullable.place {
                NullablePlace::Named { name } if name.last() == Some(col_name) => {
                    Some((index, nullable.value))
                }
                _ => None,
            })
    }

    fn r_find_index(&self, col_name: &Ident) -> Option<(usize, Option<bool>)> {
        let mut index = self.0.len();

        while index != 0 {
            index -= 1;

            match &self.0[index].place {
                NullablePlace::Named { name } if name.last() == Some(col_name) => {
                    return Some((index, self.0[index].value))
                }
                _ => (),
            }
        }
        None
    }
}

#[derive(Debug)]
pub struct StatementNullable {
    nullables: Vec<Nullable>,
}

impl From<Nullable> for StatementNullable {
    fn from(value: Nullable) -> Self {
        Self {
            nullables: vec![value],
        }
    }
}

impl StatementNullable {
    pub fn new() -> Self {
        Self {
            nullables: Vec::new(),
        }
    }

    pub fn combine(&mut self, mut null: Self) {
        self.nullables.append(&mut null.nullables)
    }

    pub fn get_nullable(mut self) -> Vec<Option<bool>> {
        let Some(mut inferred_nullable): Option<Vec<Option<bool>>> = self
            .nullables
            .pop()
            .map(|e| e.0.into_iter().map(|e| e.value).collect())
        else {
            return vec![];
        };

        for row in self.nullables.iter() {
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

    pub fn flatten(mut self) -> Nullable {
        let Some(mut first) = self.nullables.pop() else {
            return Nullable::empty();
        };

        for row in self.nullables.into_iter() {
            for (i, col) in row.0.into_iter().enumerate() {
                let value = match (first.0[i].value, col.value) {
                    (Some(first), Some(second)) => Some(first || second),
                    (Some(first), None) => Some(first),
                    (None, Some(second)) => Some(second),
                    (None, None) => None,
                };

                if let NullablePlace::Named { .. } = &first.0[i].place {
                    first.0[i].value = value;
                    continue;
                } else if let NullablePlace::Named { .. } = col.place {
                    first.0[i].place = col.place;
                    first.0[i].value = value;
                    continue;
                }
            }
        }
        first
    }

    pub fn get_nullable_final(self, cols: &[&str]) -> Vec<bool> {
        let nullables = self.flatten();

        let mut results = Vec::new();

        for (index, col) in cols.iter().enumerate() {
            results.push(nullables.nullable(col, index).unwrap_or(true));
        }

        results
    }
}
