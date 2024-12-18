// use mylib::{NullableState, Source, SqlFlavour};

// #[test]
// pub fn with_1() {
//     let source = Source::empty();

//     let query = r#"
// with user_id as (
//     select 1 as id
// )
// select
// 	id
// from
// 	user_id
//  "#;

//     let mut state = NullableState::new(query, source, SqlFlavour::Postgres);
//     let nullable = state.get_nullable();
//     println!("{:?}", nullable);
//     assert!(nullable == [false, false])
// }
