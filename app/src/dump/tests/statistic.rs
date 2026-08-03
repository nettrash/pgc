use super::*;

fn make_statistic() -> Statistic {
    Statistic::new(
        "public".to_string(),
        "my_stat".to_string(),
        "postgres".to_string(),
        "public".to_string(),
        "my_table".to_string(),
        vec!["ndistinct".to_string(), "dependencies".to_string()],
        vec!["col1".to_string(), "col2".to_string()],
        "CREATE STATISTICS public.my_stat (ndistinct, dependencies) ON col1, col2 FROM public.my_table".to_string(),
    )
}

#[test]
fn hash_populates_hash_field() {
    let stat = make_statistic();
    assert!(stat.hash.is_some());
}

#[test]
fn hash_is_consistent() {
    let s1 = make_statistic();
    let s2 = make_statistic();
    assert_eq!(s1.hash, s2.hash);
}

#[test]
fn hash_differs_with_different_name() {
    let s1 = make_statistic();
    let mut s2 = make_statistic();
    s2.name = "other_stat".to_string();
    s2.hash();
    assert_ne!(s1.hash, s2.hash);
}

#[test]
fn get_script_creates_statistics() {
    let stat = make_statistic();
    let script = stat.get_script();
    assert!(script.contains("create statistics public.my_stat"));
    assert!(script.contains("(ndistinct, dependencies)"));
    assert!(script.contains("on col1, col2"));
    assert!(script.contains("from public.my_table"));
}

#[test]
fn get_script_includes_owner() {
    let stat = make_statistic();
    let script = stat.get_script();
    assert!(script.contains("alter statistics public.my_stat owner to postgres;"));
}

#[test]
fn get_script_includes_comment() {
    let mut stat = make_statistic();
    stat.comment = Some("test comment".to_string());
    let script = stat.get_script();
    assert!(script.contains("comment on statistics public.my_stat is 'test comment';"));
}

#[test]
fn get_drop_script() {
    let stat = make_statistic();
    let script = stat.get_drop_script();
    assert!(script.contains("drop statistics if exists public.my_stat;"));
}

#[test]
fn get_alter_script_owner_change() {
    let s1 = make_statistic();
    let mut s2 = make_statistic();
    s2.owner = "new_owner".to_string();
    let script = s1.get_alter_script(&s2, true);
    assert!(script.contains("alter statistics public.my_stat owner to new_owner;"));
}

#[test]
fn get_alter_script_definition_change_drops_recreates() {
    let s1 = make_statistic();
    let mut s2 = make_statistic();
    s2.definition =
        "CREATE STATISTICS public.my_stat (mcv) ON col1, col2, col3 FROM public.my_table"
            .to_string();
    s2.kinds = vec!["mcv".to_string()];
    s2.columns = vec!["col1".to_string(), "col2".to_string(), "col3".to_string()];
    let script = s1.get_alter_script(&s2, true);
    assert!(script.contains("drop statistics if exists public.my_stat;"));
    assert!(script.contains("create statistics public.my_stat"));
}

#[test]
fn get_alter_script_comment_change() {
    let s1 = make_statistic();
    let mut s2 = make_statistic();
    s2.comment = Some("new comment".to_string());
    let script = s1.get_alter_script(&s2, true);
    assert!(script.contains("comment on statistics public.my_stat is 'new comment';"));
}

#[test]
fn get_alter_script_no_changes() {
    let s1 = make_statistic();
    let s2 = make_statistic();
    let script = s1.get_alter_script(&s2, true);
    assert!(script.is_empty());
}

#[test]
fn get_alter_script_definition_change_use_drop_false_comments_out() {
    let s1 = make_statistic();
    let mut s2 = make_statistic();
    s2.kinds = vec!["mcv".to_string()];
    s2.columns = vec!["col1".to_string(), "col2".to_string(), "col3".to_string()];
    let script = s1.get_alter_script(&s2, false);
    assert!(script.contains("-- use_drop=false"));
    assert!(script.contains("-- drop statistics"));
    assert!(script.contains("-- create statistics"));
    assert!(!script.contains("\ndrop statistics"));
}
