use crate::TableId;

#[derive(Debug)]
pub struct JoinResolver {
    data: JoinEntry,
    leafs: Vec<JoinResolver>,
}

#[derive(Debug)]
pub struct JoinEntry {
    table_id: TableId,
    nullable: Option<bool>,
}

impl JoinResolver {
    pub fn from_base(table_id: TableId) -> Self {
        Self {
            data: JoinEntry {
                table_id,
                nullable: Some(false),
            },
            leafs: Vec::new(),
        }
    }

    pub fn add_leaf(&mut self, base: TableId, leaf_id: TableId, leaf_nullable: Option<bool>) {
        if base == leaf_id {
            return;
        } else if self.data.table_id == base {
            self.leafs.push(JoinResolver {
                data: JoinEntry {
                    table_id: leaf_id,
                    nullable: leaf_nullable,
                },
                leafs: Vec::new(),
            });
        } else {
            for leaf in &mut self.leafs {
                leaf.add_leaf(base, leaf_id, leaf_nullable);
            }
        }
    }

    pub fn set_nullable(&mut self, table_id: TableId, nullable: Option<bool>) {
        self.recursive_set_nullable(table_id, nullable, 1);
    }

    pub fn recursive_set_nullable(
        &mut self,
        table_id: TableId,
        nullable: Option<bool>,
        depth: usize,
    ) {
        if self.data.table_id == table_id {
            if depth == 1 && nullable.is_some() {
                self.data.nullable = nullable;
            } else if depth != 1 {
                self.data.nullable = nullable;
            }
            return;
        }
        for leaf in &mut self.leafs {
            leaf.recursive_set_nullable(table_id, nullable, depth + 1);
        }
    }

    pub fn get_nullables(self) -> Vec<(TableId, bool)> {
        let mut nullables = Vec::new();
        let null = Self::null(self.data.nullable.unwrap(), self.data.nullable);
        nullables.push((self.data.table_id, null));

        for leaf in self.leafs {
            leaf.r_nullables(null, &mut nullables);
        }

        nullables
    }

    fn r_nullables(self, parent_nullable: bool, nullables: &mut Vec<(TableId, bool)>) {
        let null = Self::null(parent_nullable, self.data.nullable);
        nullables.push((self.data.table_id, null));

        for leaf in self.leafs {
            leaf.r_nullables(null, nullables);
        }
    }

    fn null(parent_nullable: bool, nullable: Option<bool>) -> bool {
        if let Some(inferred) = nullable {
            return inferred;
        }

        parent_nullable
    }
}

#[cfg(test)]
mod test_join {
    use crate::TableId;

    use super::JoinResolver;

    // #[test]
    // fn testing1() {
    //     // from 1
    //     // left join 2 on 2 = 1
    //     let base = TableId::new(1);

    //     let mut resolver = JoinResolver::from_base(base);
    //     resolver.add_leaf(base, TableId::new(2), None);

    //     dbg!(resolver);
    // }

    // #[test]
    // fn testing2() {
    //     // from 1
    //     // left join 2 on 2 = 1
    //     let table_1 = TableId::new(1);
    //     let table_2 = TableId::new(2);
    //     let table_3 = TableId::new(3);

    //     let mut resolver = JoinResolver::from_base(table_1);
    //     resolver.add_leaf(table_1, table_2, None);
    //     resolver.add_leaf(table_2, table_3, None);

    //     dbg!(&resolver);
    //     dbg!(resolver.get_nullables());
    // }

    // #[test]
    // fn testing3() {
    //     // from 1
    //     // left join 2 on 2 = 1
    //     let table_1 = TableId::new(1);
    //     let table_2 = TableId::new(2);
    //     let table_3 = TableId::new(3);

    //     let mut resolver = JoinResolver::from_base(table_1);
    //     resolver.add_leaf(table_1, table_2, None);
    //     resolver.add_leaf(table_2, table_3, None);

    //     resolver.set_nullable(table_1, Some(true));
    //     resolver.set_nullable(table_3, Some(false));

    //     dbg!(&resolver);
    //     dbg!(resolver.get_nullables());
    // }
    #[test]
    fn testing4() {
        // from 1 users
        // inner join 2 pets
        // on 2 = 1
        // right join 3 company
        // on 3 = 1
        let table_1 = TableId::new(1);
        let table_2 = TableId::new(2);
        let table_3 = TableId::new(3);

        let mut resolver = JoinResolver::from_base(table_1);
        resolver.add_leaf(table_1, table_2, None);
        resolver.add_leaf(table_1, table_3, None);

        resolver.set_nullable(table_1, Some(true));
        resolver.set_nullable(table_3, Some(false));

        dbg!(&resolver);
        dbg!(resolver.get_nullables());
    }
}
