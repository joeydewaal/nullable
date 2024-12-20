use mylib::{NullableState, Source, SqlFlavour, Table};

#[test]
pub fn join_1() {
    let user_table = Table::new("users")
        .push_column("id", false)
        .push_column("username", false)
        .push_column("emailadres", true)
        .push_column("pet_id", false);

    let pets_table = Table::new("pets")
        .push_column("pet_id", false)
        .push_column("pet_name", false);

    let source = Source::new(vec![user_table, pets_table]);

    let query = r#"
select
	users.id,
	users.username,
	pets.pet_id,
	pets.pet_name
from
	users
left join
	pets using (pet_id)
 "#;

    let mut state = NullableState::new(query, source, SqlFlavour::Postgres);
    let nullable = state.get_nullable(&["id", "username", "pet_id", "pet_name"]);
    println!("{:?}", nullable);
    assert!(nullable == [false, false, true, true])
}

#[test]
pub fn natural_join_1() {
    let user_table = Table::new("users")
        .push_column("id", false)
        .push_column("username", false)
        .push_column("emailadres", true)
        .push_column("pet_id", false);

    let pets_table = Table::new("pets")
        .push_column("pet_id", false)
        .push_column("pet_name", false);

    let source = Source::new(vec![user_table, pets_table]);

    let query = r#"
select
	users.id,
	users.username,
	pets.pet_id,
	pets.pet_name
from
	users
natural join
	pets
 "#;

    let mut state = NullableState::new(query, source, SqlFlavour::Postgres);
    let nullable = state.get_nullable(&["id", "username", "pet_id", "pet_name"]);
    println!("{:?}", nullable);
    assert!(nullable == [false, false, false, false])
}

#[test]
pub fn natural_join_2() {
    let user_table = Table::new("users")
        .push_column("id", false)
        .push_column("username", false)
        .push_column("emailadres", true)
        .push_column("pet_id", false);

    let pets_table = Table::new("pets")
        .push_column("pet_id", false)
        .push_column("pet_name", false);

    let source = Source::new(vec![user_table, pets_table]);

    let query = r#"
select
	users.id,
	users.username,
	pets.pet_id,
	pets.pet_name
from
	users
natural left join
	pets
 "#;

    let mut state = NullableState::new(query, source, SqlFlavour::Postgres);
    let nullable = state.get_nullable(&["id", "username", "pet_id", "pet_name"]);
    println!("{:?}", nullable);
    assert!(nullable == [false, false, true, true])
}

#[test]
pub fn join_full_outer_2() {
    let user_table = Table::new("users")
        .push_column("id", false)
        .push_column("username", false)
        .push_column("emailadres", true)
        .push_column("pet_id", false);

    let pets_table = Table::new("pets")
        .push_column("pet_id", false)
        .push_column("pet_name", false);

    let source = Source::new(vec![user_table, pets_table]);

    let query = r#"
select
	users.id,
	users.username,
	pets.pet_id,
	pets.pet_name
from
	users
full outer join
	pets using (pet_id)
 "#;

    let mut state = NullableState::new(query, source, SqlFlavour::Postgres);
    let nullable = state.get_nullable(&["id", "username", "pet_id", "pet_name"]);
    println!("{:?}", nullable);
    assert!(nullable == [true, true, true, true])
}
