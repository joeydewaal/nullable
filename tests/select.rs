use mylib::{NullableState, Source, Table};

#[test]
pub fn basic_double_left_inner_join_union() {
    let user_table = Table::new("users")
        .push_column("id", false)
        .push_column("username", false)
        .push_column("emailadres", true)
        .push_column("pet_id", true);

    let pets_table = Table::new("pets")
        .push_column("pet_id", false)
        .push_column("pet_name", false)
        .push_column("plant_id", true);

    let plants_table = Table::new("plants")
        .push_column("plant_id", false)
        .push_column("plant_name", false);

    let source = Source::new(vec![user_table, pets_table, plants_table]);

    let query = r#"
        select
            users.id,
            users.username,
            pets.pet_id,
            pets.pet_name,
            plants.plant_id,
            plants.plant_name
        from
            users
        left join
            pets
        on
            pets.pet_id = users.pet_id
        inner join
            plants
        on
            plants.plant_id = pets.plant_id
        union
        select
            users.id,
            users.username,
            pets.pet_id,
            pets.pet_name,
            plants.plant_id,
            plants.plant_name
        from
            users
        left join
            pets
        on
            pets.pet_id = users.pet_id
        inner join
            plants
        on
            plants.plant_id = pets.plant_id
 "#;

    let mut state = NullableState::new(query, source);
    let nullable = state.get_nullable();
    println!("{:?}", nullable);
    assert!(nullable == [false, false, true, true, true, true])
}

#[test]
pub fn basic_double_left_inner_join_double_union() {
    let user_table = Table::new("users")
        .push_column("id", false)
        .push_column("username", false)
        .push_column("emailadres", true)
        .push_column("pet_id", true);

    let pets_table = Table::new("pets")
        .push_column("pet_id", false)
        .push_column("pet_name", false)
        .push_column("plant_id", true);

    let plants_table = Table::new("plants")
        .push_column("plant_id", false)
        .push_column("plant_name", false);

    let source = Source::new(vec![user_table, pets_table, plants_table]);

    let query = r#"
        select
            users.id,
            users.username,
            pets.pet_id,
            pets.pet_name,
            plants.plant_id,
            plants.plant_name
        from
            users
        left join
            pets
        on
            pets.pet_id = users.pet_id
        inner join
            plants
        on
            plants.plant_id = pets.plant_id
        union
        select
            users.id,
            users.username,
            pets.pet_id,
            pets.pet_name,
            plants.plant_id,
            plants.plant_name
        from
            users
        left join
            pets
        on
            pets.pet_id = users.pet_id
        inner join
            plants
        on
            plants.plant_id = pets.plant_id
        union
        select
            users.id,
            users.username,
            pets.pet_id,
            pets.pet_name,
            plants.plant_id,
            plants.plant_name
        from
            users
        left join
            pets
        on
            pets.pet_id = users.pet_id
        inner join
            plants
        on
            plants.plant_id = pets.plant_id
 "#;

    let mut state = NullableState::new(query, source);
    let nullable = state.get_nullable();
    println!("{:?}", nullable);
    assert!(nullable == [false, false, true, true, true, true])
}
#[test]
pub fn basic_select1() {
    let user_table = Table::new("users")
        .push_column("id", false)
        .push_column("username", false)
        .push_column("emailadres", true);

    let source = Source::new(vec![user_table]);

    let query = r#"
        select users.id, username, emailadres from users
    "#;

    let mut state = NullableState::new(query, source);
    let nullable = state.get_nullable();
    println!("{:?}", nullable);
    assert!(nullable == [false, false, true])
}

#[test]
pub fn select_static() {
    let user_table = Table::new("users")
        .push_column("id", false)
        .push_column("username", false)
        .push_column("emailadres", true);

    let source = Source::new(vec![user_table]);

    let query = r#"
     select
         1 as value

 "#;

    let mut state = NullableState::new(query, source);
    let nullable = state.get_nullable();
    println!("{:?}", nullable);
    assert!(nullable == [false])
}

#[test]
pub fn select_static_multiple() {
    let query = r#"
    select
        1 as value, null as value2
    union
    select
        2 as value, 3 as value2

"#;
    let mut state = NullableState::new(query, Source::empty());
    let nullable = state.get_nullable();
    println!("{:?}", nullable);
    assert!(nullable == [false, true])
}

#[test]
pub fn basic_left_join() {
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
            users.emailadres,
            pets.pet_id,
            pets.pet_name
        from
            users
        left join
            pets
        on
            pets.pet_id = users.pet_id
 "#;

    let mut state = NullableState::new(query, source);
    let nullable = state.get_nullable();
    println!("{:?}", nullable);
    assert!(nullable == [false, false, true, true, true])
}

#[test]
pub fn basic_inner_join() {
    let user_table = Table::new("users")
        .push_column("id", false)
        .push_column("username", false)
        .push_column("emailadres", true)
        .push_column("pet_id", true);

    let pets_table = Table::new("pets")
        .push_column("pet_id", false)
        .push_column("pet_name", false);

    let source = Source::new(vec![user_table, pets_table]);

    let query = r#"
        select
            users.id,
            users.username,
            users.emailadres,
            pets.pet_id,
            pets.pet_name
        from
            users
        inner join
            pets
        on
            pets.pet_id = users.pet_id
 "#;

    let mut state = NullableState::new(query, source);
    let nullable = state.get_nullable();
    println!("{:?}", nullable);
    assert!(nullable == [false, false, true, false, false])
}

#[test]
pub fn basic_double_left_join() {
    let user_table = Table::new("users")
        .push_column("id", false)
        .push_column("username", false)
        .push_column("emailadres", true)
        .push_column("pet_id", true)
        .push_column("plant_id", true);

    let pets_table = Table::new("pets")
        .push_column("pet_id", false)
        .push_column("pet_name", false);

    let plants_table = Table::new("plants")
        .push_column("plant_id", false)
        .push_column("plant_name", false);

    let source = Source::new(vec![user_table, pets_table, plants_table]);

    let query = r#"
        select
            users.id,
            users.username,
            pets.pet_id,
            pets.pet_name,
            plants.plant_id,
            plants.plant_name
        from
            users
        left join
            pets
        on
            pets.pet_id = users.pet_id
        left join
            plants
        on
            plants.plant_id = users.plant_id
 "#;

    let mut state = NullableState::new(query, source);
    let nullable = state.get_nullable();
    println!("{:?}", nullable);
    assert!(nullable == [false, false, true, true, true, true])
}

#[test]
pub fn basic_double_left_inner_join() {
    let user_table = Table::new("users")
        .push_column("id", false)
        .push_column("username", false)
        .push_column("emailadres", true)
        .push_column("pet_id", true);

    let pets_table = Table::new("pets")
        .push_column("pet_id", false)
        .push_column("pet_name", false)
        .push_column("plant_id", true);

    let plants_table = Table::new("plants")
        .push_column("plant_id", false)
        .push_column("plant_name", false);

    let source = Source::new(vec![user_table, pets_table, plants_table]);

    let query = r#"
        select
            users.id,
            users.username,
            pets.pet_id,
            pets.pet_name,
            plants.plant_id,
            plants.plant_name
        from
            users
        left join
            pets
        on
            pets.pet_id = users.pet_id
        inner join
            plants
        on
            plants.plant_id = pets.plant_id
 "#;

    let mut state = NullableState::new(query, source);
    let nullable = state.get_nullable();
    println!("{:?}", nullable);
    assert!(nullable == [false, false, true, true, true, true])
}

#[test]
pub fn select_count1() {
    let user_table = Table::new("users")
        .push_column("id", false)
        .push_column("username", false)
        .push_column("emailadres", true);

    let source = Source::new(vec![user_table]);

    let query = r#"
        select
            count(users.id)
        from
            users
 "#;

    let mut state = NullableState::new(query, source);
    let nullable = state.get_nullable();
    println!("{:?}", nullable);
    assert!(nullable == [false])
}

#[test]
pub fn select_hardcoded_value() {
    let source = Source::empty();

    let query = r#"
        SELECT 1
 "#;

    let mut state = NullableState::new(query, source);
    let nullable = state.get_nullable();
    println!("{:?}", nullable);
    assert!(nullable == [false])
}

#[test]
pub fn select_cast1() {
    let source = Source::empty();

    let query = r#"
        SELECT '123'::INTEGER;

 "#;

    let mut state = NullableState::new(query, source);
    let nullable = state.get_nullable();
    println!("{:?}", nullable);
    assert!(nullable == [false])
}

#[test]
pub fn select_cast2() {
    let source = Source::empty();

    let query = r#"
        SELECT NULL::INTEGER;

 "#;

    let mut state = NullableState::new(query, source);
    let nullable = state.get_nullable();
    println!("{:?}", nullable);
    assert!(nullable == [true])
}

#[test]
pub fn select_func() {
    let source = Source::empty();

    let query = r#"
        SELECT now();
 "#;

    let mut state = NullableState::new(query, source);
    let nullable = state.get_nullable();
    println!("{:?}", nullable);
    assert!(nullable == [false])
}

#[test]
pub fn basic_double_left_inner_join_union_mixed() {
    let user_table = Table::new("users")
        .push_column("id", false)
        .push_column("username", false)
        .push_column("pet_id", true);

    let pets_table = Table::new("pets")
        .push_column("pet_id", false)
        .push_column("pet_name", false)
        .push_column("plant_id", true);

    let plants_table = Table::new("plants")
        .push_column("plant_id", false)
        .push_column("plant_name", false);

    let source = Source::new(vec![user_table, pets_table, plants_table]);

    let query = r#"
        select
            users.id,
            users.username,
            pets.pet_id,
            pets.pet_name
        from
            users
        left join
            pets
        on
            pets.pet_id = users.pet_id
        union
        select
            pets.pet_name,
            users.username,
            pets.pet_id,
            users.id
        from
            users
        inner join
            pets
        on
            pets.pet_id = users.pet_id
 "#;

    let mut state = NullableState::new(query, source);
    let nullable = state.get_nullable();
    println!("{:?}", nullable);
    assert!(nullable == [false, false, true, true])
}

#[test]
pub fn sqlx_issue_3202() {
    let user_table = Table::new("songs")
        .push_column("id", false)
        .push_column("artist", false)
        .push_column("title", false);

    let pets_table = Table::new("top50")
        .push_column("id", false)
        .push_column("year", false)
        .push_column("week", false)
        .push_column("song_id", false)
        .push_column("position", false);

    let source = Source::new(vec![user_table, pets_table]);

    let query = r#"
        SELECT
            this_week.week as cur_week,
            this_week.position as cur_position,
            prev_week.position as prev_position,
            (prev_week.position - this_week.position) as delta,
            songs.artist as artist,
            songs.title as title
        FROM
            top50 AS this_week
        INNER JOIN
            songs ON songs.id=this_week.song_id
        LEFT OUTER JOIN
            top50 as prev_week ON prev_week.song_id=this_week.song_id AND prev_week.week = this_week.week - 1
        WHERE this_week.week = 15
        ORDER BY this_week.week, this_week.position
 "#;

    let mut state = NullableState::new(query, source);
    let nullable = state.get_nullable();
    println!("{:?}", nullable);
    assert!(nullable == [false, false, true, true, false, false])
}

#[test]
pub fn sqlx_issue_3408() {
    let department_table = Table::new("department")
        .push_column("id", false)
        .push_column("name", false);

    let employee_table = Table::new("employee")
        .push_column("name", false)
        .push_column("department_id", true);

    let source = Source::new(vec![department_table, employee_table]);

    let query = r#"
       select
            employee.name as employee_name,
            department.name as department_name
       from employee
       left join
            department
       on
            employee.department_id = department.id
            and employee.name = $1
 "#;

    let mut state = NullableState::new(query, source.clone());
    let nullable = state.get_nullable();
    println!("{:?}", nullable);
    assert!(nullable == [false, true]);

    let query = r#"
        select
            employee.name as employee_name,
            department.name as department_name
        from employee
        left join
            department
            on employee.department_id = department.id
        where employee.name = $1
 "#;

    let mut state = NullableState::new(query, source);
    let nullable = state.get_nullable();
    println!("{:?}", nullable);
    assert!(nullable == [false, true]);
}

#[test]
pub fn select_exists1() {
    let source = Source::empty();

    let query = r#"
        SELECT EXISTS (SELECT 1);
 "#;

    let mut state = NullableState::new(query, source);
    let nullable = state.get_nullable();
    println!("{:?}", nullable);
    assert!(nullable == [false])
}

#[test]
pub fn select_not_exists1() {
    let source = Source::empty();

    let query = r#"
        SELECT NOT EXISTS (SELECT 1);
 "#;

    let mut state = NullableState::new(query, source);
    let nullable = state.get_nullable();
    println!("{:?}", nullable);
    assert!(nullable == [false])
}

#[test]
pub fn select_func1() {
    let user_table = Table::new("users")
        .push_column("id", false)
        .push_column("username", false)
        .push_column("age", false);

    let source = Source::new(vec![user_table]);

    let query = r#"
        select
            avg(users.age)
        from
            users
 "#;

    let mut state = NullableState::new(query, source);
    let nullable = state.get_nullable();
    println!("{:?}", nullable);
    assert!(nullable == [false])
}

#[test]
pub fn select_func2() {
    let user_table = Table::new("users")
        .push_column("id", false)
        .push_column("username", false)
        .push_column("age", true);

    let source = Source::new(vec![user_table]);

    let query = r#"
        select
            avg(users.age), upper(username)
        from
            users
 "#;

    let mut state = NullableState::new(query, source);
    let nullable = state.get_nullable();
    println!("{:?}", nullable);
    assert!(nullable == [true, false])
}

#[test]
pub fn select_func3() {
    let source = Source::empty();

    let query = r#"
        select
            coalesce(null, 1),
            coalesce(null),
            coalesce()
 "#;

    let mut state = NullableState::new(query, source);
    let nullable = state.get_nullable();
    println!("{:?}", nullable);
    assert!(nullable == [false, true, true])
}

#[test]
pub fn basic_left_join_func1() {
    let user_table = Table::new("users")
        .push_column("id", false)
        .push_column("username", false)
        .push_column("emailadres", true)
        .push_column("pet_id", false);

    let pets_table = Table::new("pets")
        .push_column("pet_id", false)
        .push_column("pet_name", false)
        .push_column("age", false);

    let source = Source::new(vec![user_table, pets_table]);

    let query = r#"
        select
            users.id,
            users.username,
            users.emailadres,
            pets.pet_id,
            pets.pet_name,
            avg(pets.age)
        from
            users
        inner join
            pets
        on
            pets.pet_id = users.pet_id
 "#;

    let mut state = NullableState::new(query, source);
    let nullable = state.get_nullable();
    println!("{:?}", nullable);
    assert!(nullable == [false, false, true, false, false, false])
}

#[test]
pub fn basic_right_join_func1() {
    let user_table = Table::new("users")
        .push_column("id", false)
        .push_column("username", false)
        .push_column("emailadres", true)
        .push_column("pet_id", false);

    let pets_table = Table::new("pets")
        .push_column("pet_id", false)
        .push_column("pet_name", false)
        .push_column("age", false);

    let source = Source::new(vec![user_table, pets_table]);

    let query = r#"
        select
            users.id,
            users.username,
            users.emailadres,
            pets.pet_id,
            pets.pet_name,
            avg(pets.age)
        from
            users
        right join
            pets
        on
            pets.pet_id = users.pet_id
 "#;

    let mut state = NullableState::new(query, source);
    let nullable = state.get_nullable();
    println!("{:?}", nullable);
    assert!(nullable == [true, true, true, false, false, false])
}

#[test]
pub fn double_right_join() {
    let user_table = Table::new("users")
        .push_column("id", false)
        .push_column("name", false)
        .push_column("pet_id", true)
        .push_column("company_id", true);

    let pets_table = Table::new("pets")
        .push_column("pet_id", false)
        .push_column("pet_name", false);

    let company_table = Table::new("company")
        .push_column("id", false)
        .push_column("name", false);

    let source = Source::new(vec![user_table, pets_table, company_table]);

    let query = r#"
        select
            users.id,
            users.name,
            company.id,
            company.name,
            pets.pet_id,
            pets.pet_name
        from
            users
        inner join
            pets
        on
            pets.pet_id = users.pet_id
        right join
            company
        on
            company.id = users.company_id

 "#;

    let mut state = NullableState::new(query, source);
    let nullable = state.get_nullable();
    println!("{:?}", nullable);
    assert!(nullable == [true, true, false, false, true, true])
}
