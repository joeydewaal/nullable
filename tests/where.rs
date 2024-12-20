use mylib::{NullableState, Source, SqlFlavour, Table};

#[test]
pub fn where1() {
    let user_table = Table::new("users")
        .push_column("user_id", false)
        .push_column("name", false)
        .push_column("emailadres", true);

    let source = Source::new(vec![user_table]);

    let query = r#"
        select
            u.user_id,
            u.name,
            u.emailadres
        from
            users u
        where
            u.emailadres is not null
 "#;

    let mut state = NullableState::new(query, source, SqlFlavour::Postgres);
    let nullable = state.get_nullable(&[
        "user_id",
        "name",
        "emailadres"
    ]);
    println!("{:?}", nullable);
    assert!(nullable == [false, false, false])
}

#[test]
pub fn where3() {
    let user_table = Table::new("users")
        .push_column("user_id", false)
        .push_column("name", false)
        .push_column("emailadres", true)
        .push_column("age", true);

    let orders_table = Table::new("agenda")
        .push_column("agenda_id", false)
        .push_column("startdate", false)
        .push_column("user_id", false);

    let source = Source::new(vec![user_table, orders_table]);

    let query = r#"
        select
            a.agenda_id,
            a.startdate,
            u.user_id,
			u.emailadres,
            u.age
        from
            agenda a
        left join
            users u on a.user_id = u.user_id
		where u.emailadres is not null
 "#;

    let mut state = NullableState::new(query, source, SqlFlavour::Postgres);
    let nullable = state.get_nullable(&[
        "agenda_id",
        "startdate",
        "user_id",
        "emailadres",
        "age"
    ]);
    println!("{:?}", nullable);
    assert!(nullable == [false, false, false, false, true])
}

#[test]
pub fn where4() {
    let user_table = Table::new("users")
        .push_column("user_id", false)
        .push_column("name", false)
        .push_column("emailadres", true)
        .push_column("age", true);

    let orders_table = Table::new("agenda")
        .push_column("agenda_id", false)
        .push_column("startdate", false)
        .push_column("user_id", false);

    let source = Source::new(vec![user_table, orders_table]);

    let query = r#"
        select
            a.agenda_id,
            a.startdate,
            u.user_id,
			u.emailadres,
            u.age
        from
            agenda a
        left join
            users u on a.user_id = u.user_id
		where u.emailadres is not null and age is not null

 "#;

    let mut state = NullableState::new(query, source, SqlFlavour::Postgres);
    let nullable = state.get_nullable(&[
        "agenda_id",
        "startdate",
        "user_id",
        "emailadres",
        "age"
    ]);
    println!("{:?}", nullable);
    assert!(nullable == [false, false, false, false, false])
}

#[test]
pub fn where5() {
    let user_table = Table::new("users")
        .push_column("user_id", false)
        .push_column("name", false)
        .push_column("emailadres", true)
        .push_column("age", true);

    let orders_table = Table::new("agenda")
        .push_column("agenda_id", false)
        .push_column("startdate", false)
        .push_column("user_id", false);

    let source = Source::new(vec![user_table, orders_table]);

    let query = r#"
        select
            a.agenda_id,
            a.startdate,
            u.user_id,
			u.emailadres,
            u.age
        from
            agenda a
        left join
            users u on a.user_id = u.user_id
		where u.emailadres is not null and age is not null and name is not null

 "#;

    let mut state = NullableState::new(query, source, SqlFlavour::Postgres);
    let nullable = state.get_nullable(&[
        "agenda_id",
        "startdate",
        "user_id",
        "emailadres",
        "age"
    ]);
    println!("{:?}", nullable);
    assert!(nullable == [false, false, false, false, false])
}

#[test]
pub fn where6() {
    let user_table = Table::new("users")
        .push_column("user_id", false)
        .push_column("name", false)
        .push_column("emailadres", true)
        .push_column("age", true);

    let orders_table = Table::new("agenda")
        .push_column("agenda_id", false)
        .push_column("startdate", false)
        .push_column("user_id", false);

    let source = Source::new(vec![user_table, orders_table]);

    let query = r#"
        select
            a.agenda_id,
            a.startdate,
            u.user_id,
			u.emailadres,
            u.age
        from
            agenda a
        left join
            users u on a.user_id = u.user_id
		where age < 15
     "#;

    let mut state = NullableState::new(query, source, SqlFlavour::Postgres);
    let nullable = state.get_nullable(&[
        "agenda_id",
        "startdate",
        "user_id",
        "emailadres",
        "age"
    ]);
    println!("{:?}", nullable);
    assert!(nullable == [false, false, false, true, false])
}

#[test]
pub fn where7() {
    let user_table = Table::new("users")
        .push_column("user_id", false)
        .push_column("name", false)
        .push_column("emailadres", true)
        .push_column("age", true);

    let orders_table = Table::new("agenda")
        .push_column("agenda_id", false)
        .push_column("startdate", false)
        .push_column("user_id", false);

    let source = Source::new(vec![user_table, orders_table]);

    let query = r#"
        select
            a.agenda_id,
            a.startdate,
            u.user_id,
			u.emailadres,
            u.age
        from
            agenda a
        left join
            users u on a.user_id = u.user_id
		where age < 15 or a.agenda_id = 1
     "#;

    let mut state = NullableState::new(query, source, SqlFlavour::Postgres);
    let nullable = state.get_nullable(&[
        "agenda_id",
        "startdate",
        "user_id",
        "emailadres",
        "age"
    ]);
    println!("{:?}", nullable);
    assert!(nullable == [false, false, true, true, true])
}

#[test]
pub fn where8() {
    let user_table = Table::new("users")
        .push_column("user_id", false)
        .push_column("name", false)
        .push_column("emailadres", true)
        .push_column("age", true);

    let orders_table = Table::new("agenda")
        .push_column("agenda_id", false)
        .push_column("startdate", false)
        .push_column("user_id", false);

    let source = Source::new(vec![user_table, orders_table]);

    let query = r#"
        select
            a.agenda_id,
            a.startdate,
            u.user_id,
			u.emailadres,
            u.age
        from
            agenda a
        left join
            users u on a.user_id = u.user_id
		where age < 15 and a.agenda_id = 1
     "#;

    let mut state = NullableState::new(query, source, SqlFlavour::Postgres);
    let nullable = state.get_nullable(&[
        "agenda_id",
        "startdate",
        "user_id",
        "emailadres",
        "age"
    ]);
    println!("{:?}", nullable);
    assert!(nullable == [false, false, false, true, false])
}
