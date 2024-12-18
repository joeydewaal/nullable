use mylib::{NullableState, Source, SqlFlavour, Table};

#[test]
pub fn func1() {
    let orders_table = Table::new("vote")
        .push_column("id", false)
        .push_column("user_id", false);

    let table = Table::new("user");
    let user_table = table.push_column("id", false);

    let source = Source::new(vec![user_table, orders_table]);

    let query = r#"SELECT user.id, (SELECT COUNT(vote.id) FROM vote WHERE vote.user_id = user.id) as votes FROM user WHERE user.id = ?
 "#;

    let mut state = NullableState::new(query, source, SqlFlavour::Sqlite);
    let nullable = state.get_nullable();
    println!("{:?}", nullable);
    assert!(nullable == [false, false])
}
