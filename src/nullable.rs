#[derive(Default, Debug)]
pub struct Nullable(Vec<bool>);

impl Nullable {
    pub fn empty() -> Self {
        Self::new(vec![])
    }

    pub fn add(&mut self, nullable: Option<bool>) {
        self.0.push(nullable.unwrap_or(true));
    }

    pub fn new(inner: Vec<bool>) -> Self {
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


    pub fn get_nullable(mut self) -> Vec<bool> {
        let mut inferred_nullable: Vec<bool> = self
            .0
            .pop()
            .map(|x| x.0)
            .unwrap_or_default();

        for row in self.0.iter() {
            for (i, col) in row.0.iter().enumerate() {
                inferred_nullable[i] = inferred_nullable[i] || *col
            }
        }
        inferred_nullable
    }
}
