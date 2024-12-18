use mylib::{NullableState, Source, SqlFlavour, Table};

#[test]
pub fn wildcard_1() {
    let foo_table = Table::new("foo")
        .push_column("id", false)
        .push_column("name", false);

    let source = Source::new(vec![foo_table]);

    let query = r#"
        SELECT * FROM foo
    "#;

    let mut state = NullableState::new(query, source, SqlFlavour::Postgres);
    let nullable = state.get_nullable(&[
        "id",
        "name"
    ]);
    println!("{:?}", nullable);
    assert!(nullable == [false, false])
}
