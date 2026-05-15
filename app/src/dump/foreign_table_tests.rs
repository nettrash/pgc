use super::*;

fn make_column(name: &str, data_type: &str) -> ForeignTableColumn {
    ForeignTableColumn {
        name: name.to_string(),
        data_type: data_type.to_string(),
        is_nullable: true,
        column_default: None,
        options: Vec::new(),
    }
}

fn make_foreign_table() -> ForeignTable {
    ForeignTable::new(
        "public".to_string(),
        "ft_test".to_string(),
        "remote_server".to_string(),
        "postgres".to_string(),
        Vec::new(),
        vec![make_column("id", "integer"), make_column("name", "text")],
    )
}

#[test]
fn hash_populates_hash_field() {
    let ft = make_foreign_table();
    assert!(ft.hash.is_some());
}

#[test]
fn hash_is_consistent() {
    let ft1 = make_foreign_table();
    let ft2 = make_foreign_table();
    assert_eq!(ft1.hash, ft2.hash);
}

#[test]
fn hash_differs_with_different_server() {
    let ft1 = make_foreign_table();
    let mut ft2 = make_foreign_table();
    ft2.server = "other_server".to_string();
    ft2.hash();
    assert_ne!(ft1.hash, ft2.hash);
}

#[test]
fn get_script_creates_foreign_table() {
    let ft = make_foreign_table();
    let script = ft.get_script();
    assert!(script.contains("create foreign table public.ft_test"));
    assert!(script.contains("server remote_server"));
    assert!(script.contains("id integer"));
    assert!(script.contains("name text"));
}

#[test]
fn get_script_includes_not_null() {
    let mut ft = make_foreign_table();
    ft.columns[0].is_nullable = false;
    let script = ft.get_script();
    assert!(script.contains("id integer not null"));
}

#[test]
fn get_script_includes_column_options() {
    let mut ft = make_foreign_table();
    ft.columns[0].options = vec!["column_name 'remote_id'".to_string()];
    let script = ft.get_script();
    assert!(script.contains("options (column_name 'remote_id')"));
}

#[test]
fn get_script_includes_table_options() {
    let ft = ForeignTable::new(
        "public".to_string(),
        "ft_test".to_string(),
        "remote_server".to_string(),
        "postgres".to_string(),
        vec![
            "schema_name 'remote_schema'".to_string(),
            "table_name 'remote_table'".to_string(),
        ],
        vec![make_column("id", "integer")],
    );
    let script = ft.get_script();
    assert!(script.contains("options (schema_name 'remote_schema', table_name 'remote_table')"));
}

#[test]
fn get_script_includes_owner() {
    let ft = make_foreign_table();
    let script = ft.get_script();
    assert!(script.contains("alter foreign table public.ft_test owner to postgres;"));
}

#[test]
fn get_script_includes_comment() {
    let mut ft = make_foreign_table();
    ft.comment = Some("test comment".to_string());
    let script = ft.get_script();
    assert!(script.contains("comment on foreign table public.ft_test is 'test comment';"));
}

#[test]
fn get_drop_script() {
    let ft = make_foreign_table();
    let script = ft.get_drop_script();
    assert!(script.contains("drop foreign table if exists public.ft_test;"));
}

#[test]
fn get_alter_script_owner_change() {
    let ft1 = make_foreign_table();
    let mut ft2 = make_foreign_table();
    ft2.owner = "new_owner".to_string();
    let script = ft1.get_alter_script(&ft2, true);
    assert!(script.contains("alter foreign table public.ft_test owner to new_owner;"));
}

#[test]
fn get_alter_script_server_change_drops_recreates() {
    let ft1 = make_foreign_table();
    let mut ft2 = make_foreign_table();
    ft2.server = "new_server".to_string();
    let script = ft1.get_alter_script(&ft2, true);
    assert!(script.contains("drop foreign table if exists public.ft_test;"));
    assert!(script.contains("create foreign table public.ft_test"));
    assert!(script.contains("server new_server"));
}

#[test]
fn get_alter_script_add_column() {
    let ft1 = make_foreign_table();
    let mut ft2 = make_foreign_table();
    ft2.columns.push(make_column("email", "text"));
    let script = ft1.get_alter_script(&ft2, true);
    assert!(script.contains("alter foreign table public.ft_test add column email text;"));
}

#[test]
fn get_alter_script_drop_column() {
    let ft1 = make_foreign_table();
    let mut ft2 = make_foreign_table();
    ft2.columns.retain(|c| c.name != "name");
    let script = ft1.get_alter_script(&ft2, true);
    assert!(script.contains("alter foreign table public.ft_test drop column name;"));
}

#[test]
fn get_alter_script_change_column_type() {
    let ft1 = make_foreign_table();
    let mut ft2 = make_foreign_table();
    ft2.columns[0].data_type = "bigint".to_string();
    let script = ft1.get_alter_script(&ft2, true);
    assert!(script.contains("alter foreign table public.ft_test alter column id type bigint;"));
}

#[test]
fn get_alter_script_set_not_null() {
    let ft1 = make_foreign_table();
    let mut ft2 = make_foreign_table();
    ft2.columns[0].is_nullable = false;
    let script = ft1.get_alter_script(&ft2, true);
    assert!(script.contains("alter foreign table public.ft_test alter column id set not null;"));
}

#[test]
fn get_alter_script_drop_not_null() {
    let mut ft1 = make_foreign_table();
    ft1.columns[0].is_nullable = false;
    let ft2 = make_foreign_table();
    let script = ft1.get_alter_script(&ft2, true);
    assert!(script.contains("alter foreign table public.ft_test alter column id drop not null;"));
}

#[test]
fn get_alter_script_comment_change() {
    let ft1 = make_foreign_table();
    let mut ft2 = make_foreign_table();
    ft2.comment = Some("new comment".to_string());
    let script = ft1.get_alter_script(&ft2, true);
    assert!(script.contains("comment on foreign table public.ft_test is 'new comment';"));
}

#[test]
fn get_alter_script_no_changes() {
    let ft1 = make_foreign_table();
    let ft2 = make_foreign_table();
    let script = ft1.get_alter_script(&ft2, true);
    assert!(script.is_empty());
}

#[test]
fn get_alter_script_server_change_use_drop_false_comments_out() {
    let ft1 = make_foreign_table();
    let mut ft2 = make_foreign_table();
    ft2.server = "new_server".to_string();
    let script = ft1.get_alter_script(&ft2, false);
    assert!(script.contains("-- use_drop=false"));
    assert!(script.contains("-- drop foreign table"));
    assert!(script.contains("-- create foreign table"));
    assert!(!script.contains("\ndrop foreign table"));
}
