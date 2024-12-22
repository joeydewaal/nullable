use mylib::{NullableState, Source, SqlFlavour, Table};

#[test]
pub fn values() {
    let source = Source::empty();

    let query = r#"
        values (1, 2)
 "#;

    let mut state = NullableState::new(query, source, SqlFlavour::Postgres);
    let nullable = state.get_nullable(&["?column?", "?column"]);
    println!("{:?}", nullable);
    assert!(nullable == [false, false])
}

#[test]
pub fn query_1() {
    let source = Source::empty();

    let query = r#"
        (select 1)
 "#;

    let mut state = NullableState::new(query, source, SqlFlavour::Postgres);
    let nullable = state.get_nullable(&["?column?"]);
    println!("{:?}", nullable);
    assert!(nullable == [false])
}

#[ignore = "sqlparser does currently not support table commands"]
#[test]
pub fn table_1() {
    let user_table = Table::new("users")
        .push_column("id", false)
        .push_column("name", false)
        .push_column("emailadres", true);
    let source = Source::new(vec![user_table]);

    let query = r#"
        TABLE users;
 "#;

    let mut state = NullableState::new(query, source, SqlFlavour::Postgres);
    let nullable = state.get_nullable(&["id", "name", "emailadres"]);
    println!("{:?}", nullable);
    assert!(nullable == [false, false, true])
}
