use super::*;

#[test]
fn test_publication_hash_populates() {
    let mut p = Publication {
        name: "\"my_pub\"".into(),
        owner: "postgres".into(),
        all_tables: false,
        publish: "insert,update,delete".into(),
        tables: vec!["public.orders".into()],
        comment: None,
        hash: None,
        ..Default::default()
    };
    p.hash();
    assert!(p.hash.is_some());
}

#[test]
fn test_publication_get_script() {
    let mut p = Publication {
        name: "\"my_pub\"".into(),
        owner: "postgres".into(),
        all_tables: false,
        publish: "insert,update,delete".into(),
        tables: vec!["public.orders".into()],
        comment: None,
        hash: None,
        ..Default::default()
    };
    p.hash();
    let s = p.get_script();
    assert!(s.contains("CREATE PUBLICATION"));
    assert!(s.contains("FOR TABLE public.orders"));
}

#[test]
fn test_subscription_get_script() {
    let mut s = Subscription {
        name: "\"my_sub\"".into(),
        owner: "postgres".into(),
        connection: "host=primary dbname=mydb".into(),
        server: None,
        publications: vec!["my_pub".into()],
        enabled: true,
        slot_name: SubscriptionSlot::Own,
        retain_dead_tuples: false,
        max_retention_duration: 0,
        wal_receiver_timeout: None,
        comment: None,
        hash: None,
    };
    s.hash();
    let script = s.get_script();
    assert!(script.contains("CREATE SUBSCRIPTION"));
    assert!(script.contains("my_pub"));
}

#[test]
fn alter_to_all_tables_use_drop_true_drops_and_recreates() {
    let from = Publication {
        name: "my_pub".into(),
        owner: "postgres".into(),
        all_tables: false,
        publish: "insert,update,delete".into(),
        tables: vec!["public.orders".into()],
        comment: None,
        hash: None,
        ..Default::default()
    };
    let to = Publication {
        name: "my_pub".into(),
        owner: "postgres".into(),
        all_tables: true,
        publish: "insert,update,delete".into(),
        tables: vec![],
        comment: None,
        hash: None,
        ..Default::default()
    };
    let script = from.get_alter_script(&to, true);
    assert!(
        script.contains("DROP PUBLICATION IF EXISTS my_pub;"),
        "use_drop=true must DROP, got: {script}"
    );
    assert!(
        script.contains("CREATE PUBLICATION my_pub FOR ALL TABLES"),
        "use_drop=true must CREATE with FOR ALL TABLES, got: {script}"
    );
}

#[test]
fn alter_to_all_tables_use_drop_false_comments_out() {
    let from = Publication {
        name: "my_pub".into(),
        owner: "postgres".into(),
        all_tables: false,
        publish: "insert,update,delete".into(),
        tables: vec!["public.orders".into()],
        comment: None,
        hash: None,
        ..Default::default()
    };
    let to = Publication {
        name: "my_pub".into(),
        owner: "postgres".into(),
        all_tables: true,
        publish: "insert,update,delete".into(),
        tables: vec![],
        comment: None,
        hash: None,
        ..Default::default()
    };
    let script = from.get_alter_script(&to, false);
    assert!(
        script.contains("-- use_drop=false"),
        "must include use_drop=false warning, got: {script}"
    );
    assert!(
        script.contains("-- DROP PUBLICATION IF EXISTS my_pub;"),
        "must have commented-out DROP, got: {script}"
    );
    assert!(
        script.contains("-- CREATE PUBLICATION my_pub FOR ALL TABLES"),
        "must have commented-out CREATE, got: {script}"
    );
    // Must NOT have uncommented DROP or CREATE
    let has_active_drop = script.lines().any(|l| {
        let t = l.trim_start();
        t.starts_with("DROP PUBLICATION") && !t.starts_with("--")
    });
    assert!(!has_active_drop, "must not have active DROP, got: {script}");
}

// ==== pg19:publications tests ====

// Fixtures for the tests below. `base` is a publication that uses none of the
// fields added after pgc 1.0.x.
fn base(name: &str) -> Publication {
    Publication {
        name: name.into(),
        owner: "postgres".into(),
        publish: "insert,update,delete,truncate".into(),
        ..Default::default()
    }
}

fn all_tables(name: &str) -> Publication {
    Publication {
        all_tables: true,
        ..base(name)
    }
}

fn all_sequences(name: &str) -> Publication {
    Publication {
        all_sequences: true,
        ..base(name)
    }
}

fn listing(name: &str, tables: &[&str]) -> Publication {
    Publication {
        tables: tables.iter().map(|t| t.to_string()).collect(),
        ..base(name)
    }
}

fn hashed(mut p: Publication) -> Publication {
    p.hash();
    p
}

fn pg19() -> PublicationAlterContext {
    PublicationAlterContext {
        set_all_supported: true,
        ..Default::default()
    }
}

fn gone(tables: &[&str], set_all_supported: bool) -> PublicationAlterContext {
    PublicationAlterContext {
        set_all_supported,
        gone_tables: tables.iter().map(|t| t.to_string()).collect(),
        ..Default::default()
    }
}

// ---- hash stability: publications without the new fields keep their 1.0.27 hash ----

// The digests below were read from a dump that the pgc 1.0.27 publication code
// wrote from PostgreSQL 18.6 (`CREATE PUBLICATION p_tab FOR ALL TABLES`,
// `CREATE PUBLICATION p_empty`, `CREATE PUBLICATION p_list FOR TABLE s.t1, s.t2
// WITH (publish = 'insert,update')` plus a comment). A FROM dump written by
// 1.0.x is compared against these verbatim, so they must never change.

#[test]
fn hash_of_all_tables_publication_matches_v1_0_27() {
    assert_eq!(
        hashed(all_tables("p_tab")).hash.as_deref(),
        Some("40cbc4da36e4b32910f5dcbbf3e5ea7f88ead8495802dd51445d1aadad90ca0b")
    );
}

#[test]
fn hash_of_empty_publication_matches_v1_0_27() {
    assert_eq!(
        hashed(base("p_empty")).hash.as_deref(),
        Some("dc236f59d6dc7da491a411fd8ea8364b0b93c5dabc67ba28693227c755982477")
    );
}

#[test]
fn hash_of_table_list_publication_with_comment_matches_v1_0_27() {
    let p = Publication {
        publish: "insert,update".into(),
        comment: Some("a 'quoted' comment".into()),
        ..listing("p_list", &["s.t1", "s.t2"])
    };
    assert_eq!(
        hashed(p).hash.as_deref(),
        Some("e7ed6d00b03b510b7b4540eae086b5867a7693f384976ed818c13adcba30b14e")
    );
}

#[test]
fn empty_table_spec_entry_does_not_change_the_hash() {
    let plain = hashed(listing("p", &["s.t1"]));
    let mut with_empty_spec = listing("p", &["s.t1"]);
    with_empty_spec
        .table_specs
        .insert("s.t1".into(), PublicationTableSpec::default());
    assert_eq!(plain.hash, hashed(with_empty_spec).hash);
}

#[test]
fn every_new_field_changes_the_hash_and_each_differently() {
    let spec = |columns: &[&str], row_filter: Option<&str>| PublicationTableSpec {
        columns: columns.iter().map(|c| c.to_string()).collect(),
        row_filter: row_filter.map(str::to_string),
    };
    let with_spec = |s: PublicationTableSpec| {
        let mut p = listing("p", &["s.t1"]);
        p.table_specs.insert("s.t1".into(), s);
        p
    };
    let variants = vec![
        listing("p", &["s.t1"]),
        Publication {
            all_sequences: true,
            ..listing("p", &["s.t1"])
        },
        Publication {
            all_tables: true,
            except_tables: vec!["s.t1".into()],
            ..base("p")
        },
        Publication {
            all_tables: true,
            ..base("p")
        },
        Publication {
            schemas: vec!["s".into()],
            ..listing("p", &["s.t1"])
        },
        with_spec(spec(&["id"], None)),
        with_spec(spec(&["id", "a"], None)),
        with_spec(spec(&[], Some("(id > 1)"))),
        with_spec(spec(&["id"], Some("(id > 1)"))),
        Publication {
            via_partition_root: true,
            ..listing("p", &["s.t1"])
        },
        Publication {
            publish_generated_columns: Some("stored".into()),
            ..listing("p", &["s.t1"])
        },
    ];
    let hashes: Vec<String> = variants
        .into_iter()
        .map(|p| hashed(p).hash.unwrap())
        .collect();
    let distinct: std::collections::HashSet<&String> = hashes.iter().collect();
    assert_eq!(distinct.len(), hashes.len(), "hash collision in {hashes:?}");
}

// ---- get_script ----

#[test]
fn get_script_is_unchanged_for_publications_without_new_fields() {
    let expected = "CREATE PUBLICATION p FOR TABLE s.a, s.b WITH (publish = 'insert,update,delete,truncate');\n\n\
                    ALTER PUBLICATION p OWNER TO postgres;\n\n";
    assert_eq!(listing("p", &["s.a", "s.b"]).get_script(), expected);
    assert_eq!(
        all_tables("p").get_script(),
        "CREATE PUBLICATION p FOR ALL TABLES WITH (publish = 'insert,update,delete,truncate');\n\n\
         ALTER PUBLICATION p OWNER TO postgres;\n\n"
    );
    assert_eq!(
        base("p").get_script(),
        "CREATE PUBLICATION p WITH (publish = 'insert,update,delete,truncate');\n\n\
         ALTER PUBLICATION p OWNER TO postgres;\n\n"
    );
}

#[test]
fn get_script_all_sequences() {
    assert!(all_sequences("p").get_script().starts_with(
        "CREATE PUBLICATION p FOR ALL SEQUENCES WITH (publish = 'insert,update,delete,truncate');"
    ));
    let both = Publication {
        all_tables: true,
        ..all_sequences("p")
    };
    assert!(
        both.get_script()
            .starts_with("CREATE PUBLICATION p FOR ALL TABLES, ALL SEQUENCES WITH (")
    );
}

#[test]
fn get_script_all_tables_except_uses_table_only_per_row() {
    let p = Publication {
        except_tables: vec!["s.a".into(), "s.\"B c\"".into()],
        ..all_tables("p")
    };
    assert!(p.get_script().starts_with(
        "CREATE PUBLICATION p FOR ALL TABLES EXCEPT (TABLE ONLY s.a, TABLE ONLY s.\"B c\") WITH ("
    ));
    let with_sequences = Publication {
        all_sequences: true,
        ..p
    };
    assert!(with_sequences.get_script().starts_with(
        "CREATE PUBLICATION p FOR ALL TABLES EXCEPT (TABLE ONLY s.a, TABLE ONLY s.\"B c\"), ALL SEQUENCES WITH ("
    ));
}

#[test]
fn get_script_members_with_column_lists_row_filters_and_schemas() {
    let mut p = Publication {
        schemas: vec!["s2".into(), "\"S 3\"".into()],
        ..listing("p", &["s.t1", "s.t2", "s.t3"])
    };
    p.table_specs.insert(
        "s.t1".into(),
        PublicationTableSpec {
            columns: vec!["a".into(), "id".into()],
            row_filter: Some("(a > 1)".into()),
        },
    );
    p.table_specs.insert(
        "s.t3".into(),
        PublicationTableSpec {
            columns: vec![],
            row_filter: Some("true".into()),
        },
    );
    assert!(p.get_script().starts_with(
        "CREATE PUBLICATION p FOR TABLE s.t1 (a, id) WHERE ((a > 1)), s.t2, s.t3 WHERE (true), \
         TABLES IN SCHEMA s2, \"S 3\" WITH ("
    ));

    let schemas_only = Publication {
        schemas: vec!["s2".into()],
        ..base("p")
    };
    assert!(
        schemas_only
            .get_script()
            .starts_with("CREATE PUBLICATION p FOR TABLES IN SCHEMA s2 WITH (")
    );
}

#[test]
fn get_script_emits_only_non_default_options() {
    let p = Publication {
        publish: "insert".into(),
        via_partition_root: true,
        publish_generated_columns: Some("stored".into()),
        ..listing("p", &["s.pt"])
    };
    assert!(p.get_script().starts_with(
        "CREATE PUBLICATION p FOR TABLE s.pt WITH (publish = 'insert', \
         publish_via_partition_root = true, publish_generated_columns = stored);"
    ));
}

// ---- get_alter_script: output that must stay as pgc 1.0.27 wrote it ----

#[test]
fn alter_list_to_list_is_unchanged() {
    let from = hashed(listing("p", &["s.a", "s.b"]));
    let to = hashed(listing("p", &["s.b"]));
    assert_eq!(
        from.get_alter_script(&to, true),
        "ALTER PUBLICATION p SET TABLE s.b;\n\n"
    );
}

#[test]
fn alter_publish_only_is_unchanged() {
    let from = listing("p", &["s.a"]);
    let to = Publication {
        publish: "insert".into(),
        ..listing("p", &["s.a"])
    };
    assert_eq!(
        from.get_alter_script(&to, false),
        "ALTER PUBLICATION p SET (publish = 'insert');\n\n"
    );
}

#[test]
fn alter_list_to_all_tables_before_pg19_is_unchanged() {
    let from = listing("p", &["s.a"]);
    let to = all_tables("p");
    let create = "CREATE PUBLICATION p FOR ALL TABLES WITH (publish = 'insert,update,delete,truncate');\n\n\
                  ALTER PUBLICATION p OWNER TO postgres;\n\n";
    assert_eq!(
        from.get_alter_script(&to, true),
        format!("DROP PUBLICATION IF EXISTS p;\n\n{create}")
    );
    let commented = "-- use_drop=false: publication p requires drop+recreate to switch to FOR ALL TABLES; statements commented out\n\
                     -- DROP PUBLICATION IF EXISTS p;\n\
                     -- \n\
                     -- CREATE PUBLICATION p FOR ALL TABLES WITH (publish = 'insert,update,delete,truncate');\n\
                     -- \n\
                     -- ALTER PUBLICATION p OWNER TO postgres;\n\
                     -- \n";
    assert_eq!(from.get_alter_script(&to, false), commented);
}

// ---- get_alter_script: the membership state machine ----

#[test]
fn alter_all_tables_to_list_recreates_instead_of_set_table() {
    // `SET TABLE` on a FOR ALL TABLES publication fails on every version and
    // was re-emitted on every run.
    let from = all_tables("p");
    let to = listing("p", &["s.a"]);
    let script = from.get_alter_script(&to, true);
    assert!(!script.contains("SET TABLE"), "got: {script}");
    assert!(script.starts_with("DROP PUBLICATION IF EXISTS p;"));
    assert!(script.contains("CREATE PUBLICATION p FOR TABLE s.a WITH ("));

    let commented = from.get_alter_script(&to, false);
    assert!(commented.starts_with(
        "-- use_drop=false: publication p requires drop+recreate to switch from FOR ALL TABLES; statements commented out\n"
    ));
    assert!(
        commented.lines().all(|l| l.starts_with("--")),
        "got: {commented}"
    );
}

#[test]
fn alter_all_tables_to_empty_recreates() {
    let script = all_tables("p").get_alter_script(&base("p"), true);
    assert!(
        script.starts_with("DROP PUBLICATION IF EXISTS p;"),
        "got: {script}"
    );
    assert!(
        script.contains("CREATE PUBLICATION p WITH (publish = 'insert,update,delete,truncate');")
    );
}

#[test]
fn alter_list_to_empty_drops_the_members() {
    let from = listing("p", &["s.a", "s.b"]);
    assert_eq!(
        from.get_alter_script(&base("p"), false),
        "ALTER PUBLICATION p DROP TABLE ONLY s.a, ONLY s.b;\n\n"
    );
    let schemas = Publication {
        schemas: vec!["s2".into()],
        ..listing("p", &["s.a"])
    };
    assert_eq!(
        schemas.get_alter_script(&base("p"), false),
        "ALTER PUBLICATION p DROP TABLE ONLY s.a;\n\n\
         ALTER PUBLICATION p DROP TABLES IN SCHEMA s2;\n\n"
    );
}

#[test]
fn alter_member_row_filter_or_column_list_uses_set_table() {
    let from = listing("p", &["s.a"]);
    let mut to = listing("p", &["s.a"]);
    to.table_specs.insert(
        "s.a".into(),
        PublicationTableSpec {
            columns: vec!["id".into()],
            row_filter: Some("(id > 5)".into()),
        },
    );
    assert_eq!(
        from.get_alter_script(&to, false),
        "ALTER PUBLICATION p SET TABLE s.a (id) WHERE ((id > 5));\n\n"
    );
    // ... and back.
    assert_eq!(
        to.get_alter_script(&from, false),
        "ALTER PUBLICATION p SET TABLE s.a;\n\n"
    );
}

#[test]
fn alter_list_to_schemas_uses_set() {
    let from = listing("p", &["s.a"]);
    let to = Publication {
        schemas: vec!["s2".into()],
        ..base("p")
    };
    assert_eq!(
        from.get_alter_script(&to, false),
        "ALTER PUBLICATION p SET TABLES IN SCHEMA s2;\n\n"
    );
    let mixed = Publication {
        schemas: vec!["s2".into()],
        ..listing("p", &["s.a"])
    };
    assert_eq!(
        to.get_alter_script(&mixed, false),
        "ALTER PUBLICATION p SET TABLE s.a, TABLES IN SCHEMA s2;\n\n"
    );
}

#[test]
fn alter_between_all_kinds_uses_set_all() {
    let both = Publication {
        all_tables: true,
        ..all_sequences("p")
    };
    // all_sequences on either side proves PG19 even without the context flag.
    assert_eq!(
        all_tables("p").get_alter_script(&both, false),
        "ALTER PUBLICATION p SET ALL TABLES, ALL SEQUENCES;\n\n"
    );
    assert_eq!(
        both.get_alter_script(&all_tables("p"), false),
        "ALTER PUBLICATION p SET ALL TABLES;\n\n"
    );
    assert_eq!(
        all_sequences("p").get_alter_script(&all_tables("p"), false),
        "ALTER PUBLICATION p SET ALL TABLES;\n\n"
    );
    assert_eq!(
        base("p").get_alter_script(&all_sequences("p"), false),
        "ALTER PUBLICATION p SET ALL SEQUENCES;\n\n"
    );
}

#[test]
fn alter_except_list_uses_set_all_and_keeps_all_sequences() {
    let from = Publication {
        except_tables: vec!["s.a".into()],
        ..all_tables("p")
    };
    let to = Publication {
        except_tables: vec!["s.b".into()],
        ..all_tables("p")
    };
    assert_eq!(
        from.get_alter_script(&to, false),
        "ALTER PUBLICATION p SET ALL TABLES EXCEPT (TABLE ONLY s.b);\n\n"
    );
    assert_eq!(
        from.get_alter_script(&all_tables("p"), false),
        "ALTER PUBLICATION p SET ALL TABLES;\n\n"
    );
    // SET replaces the whole spec, so ALL SEQUENCES must be repeated.
    let to_with_sequences = Publication {
        all_sequences: true,
        ..to
    };
    let from_with_sequences = Publication {
        all_sequences: true,
        ..from
    };
    assert_eq!(
        from_with_sequences.get_alter_script(&to_with_sequences, false),
        "ALTER PUBLICATION p SET ALL TABLES EXCEPT (TABLE ONLY s.b), ALL SEQUENCES;\n\n"
    );
}

#[test]
fn alter_list_to_all_on_pg19_empties_the_list_then_sets_all() {
    let from = Publication {
        schemas: vec!["s2".into()],
        ..listing("p", &["s.a", "s.b"])
    };
    let expected = "ALTER PUBLICATION p DROP TABLE ONLY s.a, ONLY s.b;\n\n\
                    ALTER PUBLICATION p DROP TABLES IN SCHEMA s2;\n\n\
                    ALTER PUBLICATION p SET ALL TABLES;\n\n";
    // In place, so the same with and without use_drop.
    assert_eq!(
        from.get_alter_script_with(&all_tables("p"), true, &pg19()),
        expected
    );
    assert_eq!(
        from.get_alter_script_with(&all_tables("p"), false, &pg19()),
        expected
    );
    // A list -> ALL SEQUENCES change is PG19 data, so no context flag is needed.
    assert_eq!(
        listing("p", &["s.a"]).get_alter_script(&all_sequences("p"), false),
        "ALTER PUBLICATION p DROP TABLE ONLY s.a;\n\n\
         ALTER PUBLICATION p SET ALL SEQUENCES;\n\n"
    );
    // Empty -> ALL TABLES on 19 is a single SET.
    assert_eq!(
        base("p").get_alter_script_with(&all_tables("p"), false, &pg19()),
        "ALTER PUBLICATION p SET ALL TABLES;\n\n"
    );
}

#[test]
fn alter_all_sequences_to_list_or_empty_recreates() {
    let script = all_sequences("p").get_alter_script(&listing("p", &["s.a"]), true);
    assert!(
        script.starts_with("DROP PUBLICATION IF EXISTS p;"),
        "got: {script}"
    );
    assert!(script.contains("CREATE PUBLICATION p FOR TABLE s.a WITH ("));

    let commented = all_sequences("p").get_alter_script_with(&base("p"), false, &pg19());
    assert!(commented.starts_with(
        "-- use_drop=false: publication p requires drop+recreate to switch from FOR ALL SEQUENCES; statements commented out\n"
    ));
    assert!(commented.contains("-- CREATE PUBLICATION p WITH ("));
    assert!(
        commented.lines().all(|l| l.starts_with("--")),
        "got: {commented}"
    );
}

#[test]
fn alter_skips_members_the_migration_already_dropped() {
    // DROP TABLE of a relation that no longer exists fails the whole statement.
    let from = listing("p", &["s.a", "s.b"]);
    assert_eq!(
        from.get_alter_script_with(&base("p"), true, &gone(&["s.a"], false)),
        "ALTER PUBLICATION p DROP TABLE ONLY s.b;\n\n"
    );
    assert_eq!(
        from.get_alter_script_with(&all_tables("p"), true, &gone(&["s.a", "s.b"], true)),
        "ALTER PUBLICATION p SET ALL TABLES;\n\n"
    );
    // Gone and not listed by the target either: SET TABLE is still emitted, as
    // pgc 1.0.x did.
    assert_eq!(
        from.get_alter_script_with(&listing("p", &["s.b"]), true, &gone(&["s.a"], false)),
        "ALTER PUBLICATION p SET TABLE s.b;\n\n"
    );
}

#[test]
fn alter_restores_members_the_migration_recreated() {
    // Equal publications, but s.a was dropped and recreated with its row.
    let from = hashed(listing("p", &["s.a", "s.b"]));
    let to = hashed(listing("p", &["s.a", "s.b"]));
    let ctx = gone(&["s.a"], false);
    assert!(from.references_any_table(&ctx.gone_tables));
    assert_eq!(
        from.get_alter_script_with(&to, true, &ctx),
        "ALTER PUBLICATION p SET TABLE s.a, s.b;\n\n"
    );

    let except = Publication {
        except_tables: vec!["s.a".into()],
        ..all_tables("p")
    };
    assert!(except.references_any_table(&ctx.gone_tables));
    assert_eq!(
        except.get_alter_script_with(&except.clone(), true, &ctx),
        "ALTER PUBLICATION p SET ALL TABLES EXCEPT (TABLE ONLY s.a);\n\n"
    );
    // Nothing gone, nothing to do.
    assert_eq!(
        from.get_alter_script_with(&to, true, &PublicationAlterContext::default()),
        ""
    );
    assert!(!from.references_any_table(&gone(&["s.c"], false).gone_tables));
}

#[test]
fn alter_options_are_one_set_ordered_around_the_membership_change() {
    let from = listing("p", &["s.pt"]);
    let mut to = Publication {
        publish: "insert".into(),
        via_partition_root: true,
        ..listing("p", &["s.pt"])
    };
    to.table_specs.insert(
        "s.pt".into(),
        PublicationTableSpec {
            columns: vec![],
            row_filter: Some("(v > 0)".into()),
        },
    );
    // A row filter on a partitioned table needs publish_via_partition_root
    // first ...
    assert_eq!(
        from.get_alter_script(&to, false),
        "ALTER PUBLICATION p SET (publish = 'insert', publish_via_partition_root = true);\n\n\
         ALTER PUBLICATION p SET TABLE s.pt WHERE ((v > 0));\n\n"
    );
    // ... and may only lose it after the filter is gone.
    assert_eq!(
        to.get_alter_script(&from, false),
        "ALTER PUBLICATION p SET TABLE s.pt;\n\n\
         ALTER PUBLICATION p SET (publish = 'insert,update,delete,truncate', publish_via_partition_root = false);\n\n"
    );
}

#[test]
fn alter_publish_generated_columns_sets_and_resets() {
    let from = base("p");
    let to = Publication {
        publish_generated_columns: Some("stored".into()),
        ..base("p")
    };
    assert_eq!(
        from.get_alter_script(&to, false),
        "ALTER PUBLICATION p SET (publish_generated_columns = stored);\n\n"
    );
    assert_eq!(
        to.get_alter_script(&from, false),
        "ALTER PUBLICATION p SET (publish_generated_columns = none);\n\n"
    );
}

#[test]
fn alter_set_all_keeps_the_option_owner_and_comment_changes() {
    let from = all_sequences("p");
    let to = Publication {
        all_tables: true,
        publish: "insert".into(),
        owner: "other".into(),
        comment: Some("c".into()),
        ..all_sequences("p")
    };
    assert_eq!(
        from.get_alter_script(&to, false),
        "ALTER PUBLICATION p SET ALL TABLES, ALL SEQUENCES;\n\n\
         ALTER PUBLICATION p SET (publish = 'insert');\n\n\
         ALTER PUBLICATION p OWNER TO other;\n\n\
         COMMENT ON PUBLICATION p IS 'c';\n\n"
    );
}

#[test]
fn legacy_publication_json_loads_with_defaults() {
    // A publication object as pgc 1.0.27 serialized it.
    let json = r#"{"name":"p","owner":"postgres","all_tables":false,"publish":"insert","tables":["s.a"],"hash":"x"}"#;
    let p: Publication = serde_json::from_str(json).expect("legacy publication must load");
    assert!(!p.all_sequences);
    assert!(p.table_specs.is_empty());
    assert!(p.schemas.is_empty());
    assert!(p.except_tables.is_empty());
    assert!(!p.via_partition_root);
    assert_eq!(p.publish_generated_columns, None);
    // ... and serializes back without any of the new keys.
    assert_eq!(serde_json::to_string(&p).unwrap(), json);
}

fn parents(tables: &[&str]) -> PublicationAlterContext {
    PublicationAlterContext {
        inheritance_parents: tables.iter().map(|t| t.to_string()).collect(),
        ..Default::default()
    }
}

#[test]
fn inheritance_parent_members_are_written_only() {
    // A parent created recursively with a row filter: one row for it and one
    // for its child, both filtered. Without ONLY on the parent the CREATE
    // fails ("conflicting or redundant WHERE clauses").
    let mut p = listing("p", &["s.child", "s.parent", "s.t"]);
    for t in ["s.child", "s.parent"] {
        p.table_specs.insert(
            t.into(),
            PublicationTableSpec {
                columns: vec!["id".into()],
                row_filter: Some("(id > 0)".into()),
            },
        );
    }
    let ctx = parents(&["s.parent"]);
    assert_eq!(
        p.get_script_with(&ctx),
        "CREATE PUBLICATION p FOR TABLE s.child (id) WHERE ((id > 0)), \
         ONLY s.parent (id) WHERE ((id > 0)), s.t \
         WITH (publish = 'insert,update,delete,truncate');\n\n\
         ALTER PUBLICATION p OWNER TO postgres;\n\n"
    );
    // Without inheritance the spelling is pgc 1.0.x's.
    assert!(p.get_script().starts_with(
        "CREATE PUBLICATION p FOR TABLE s.child (id) WHERE ((id > 0)), s.parent (id)"
    ));

    // SET TABLE, with or without a filter change: `SET TABLE s.parent` would
    // add the child as a member as well.
    let from = listing("p", &["s.parent"]);
    let mut to = listing("p", &["s.parent"]);
    to.table_specs.insert(
        "s.parent".into(),
        PublicationTableSpec {
            columns: vec![],
            row_filter: Some("(id > 0)".into()),
        },
    );
    assert_eq!(
        from.get_alter_script_with(&to, true, &ctx),
        "ALTER PUBLICATION p SET TABLE ONLY s.parent WHERE ((id > 0));\n\n"
    );
    assert_eq!(
        listing("p", &["s.t"]).get_alter_script_with(
            &listing("p", &["s.parent", "s.t"]),
            true,
            &ctx
        ),
        "ALTER PUBLICATION p SET TABLE ONLY s.parent, s.t;\n\n"
    );

    // The recreate that leaves FOR ALL TABLES, and a member restored after
    // its table was recreated.
    assert_eq!(
        all_tables("p").get_alter_script_with(&listing("p", &["s.parent"]), true, &ctx),
        "DROP PUBLICATION IF EXISTS p;\n\n\
         CREATE PUBLICATION p FOR TABLE ONLY s.parent WITH (publish = 'insert,update,delete,truncate');\n\n\
         ALTER PUBLICATION p OWNER TO postgres;\n\n"
    );
    let restore = PublicationAlterContext {
        gone_tables: ["s.parent".to_string()].into(),
        ..parents(&["s.parent"])
    };
    assert_eq!(
        from.get_alter_script_with(&from.clone(), true, &restore),
        "ALTER PUBLICATION p SET TABLE ONLY s.parent;\n\n"
    );
}

// ==== pg19:subscriptions-fdw tests ====

// ---- PG19: SERVER subscriptions, retain_dead_tuples, max_retention_duration,
// ---- wal_receiver_timeout ----

/// A CONNECTION subscription with every PG19 field at its default — the only
/// shape a PG14-18 server or a pgc 1.0.x dump can produce.
fn sub(name: &str) -> Subscription {
    Subscription {
        name: name.into(),
        owner: "postgres".into(),
        connection: "host=primary dbname=mydb".into(),
        server: None,
        publications: vec!["my_pub".into()],
        enabled: true,
        slot_name: SubscriptionSlot::Own,
        retain_dead_tuples: false,
        max_retention_duration: 0,
        wal_receiver_timeout: None,
        comment: None,
        hash: None,
    }
}

fn sub_on_server(name: &str, server: &str) -> Subscription {
    Subscription {
        connection: String::new(),
        server: Some(server.into()),
        ..sub(name)
    }
}

fn hash_of(mut s: Subscription) -> String {
    s.hash();
    s.hash.unwrap()
}

/// Every statement of `script`, in order, with the blank-line framing removed.
fn statements(script: &str) -> Vec<&str> {
    script
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .collect()
}

/// Every block of `script` — one statement or comment each, however many
/// lines it spans — in order, without the blank-line framing.
fn blocks(script: &str) -> Vec<&str> {
    script
        .split("\n\n")
        .map(str::trim)
        .filter(|b| !b.is_empty())
        .collect()
}

/// The block that waits for the workers of subscription `s` to exit, as
/// [`Subscription::worker_exit_wait_script`] writes it.
const WAIT_S: &str = "DO $pgc$
DECLARE
    deadline timestamptz := clock_timestamp() + interval '60 seconds';
BEGIN
    WHILE clock_timestamp() < deadline AND EXISTS (
        SELECT 1 FROM pg_catalog.pg_stat_subscription st
        JOIN pg_catalog.pg_subscription s ON s.oid = st.subid
        WHERE s.subname = 's'
          AND s.subdbid = (SELECT oid FROM pg_catalog.pg_database WHERE datname = current_database())
          AND st.pid IS NOT NULL
    ) LOOP
        PERFORM pg_catalog.pg_sleep(0.1);
    END LOOP;
END
$pgc$;";

#[test]
fn subscription_hash_without_pg19_fields_matches_pgc_1_0_x() {
    // Pinned digests of the pgc 1.0.27 algorithm: name, owner, connection,
    // publications, enabled, comment. Hashes are stored in dump files and
    // compared verbatim, so a 1.0.x dump against a 1.1 one must not diff.
    assert_eq!(
        hash_of(sub("\"my_sub\"")),
        "6d68a5e01b54c8245549a205c784c3dcbb7b5de538b81e798856f7714f5d1038"
    );
    let disabled = Subscription {
        enabled: false,
        ..sub("\"my_sub\"")
    };
    assert_eq!(
        hash_of(disabled),
        "11c0afb288b2117807b706b0b266220f1b29c3503916bb1e56d6f0e0658b4877"
    );
}

#[test]
fn subscription_hash_changes_with_each_pg19_field() {
    let base = hash_of(sub("s"));
    let variants = [
        sub_on_server("s", "srv"),
        Subscription {
            retain_dead_tuples: true,
            ..sub("s")
        },
        Subscription {
            max_retention_duration: 60000,
            ..sub("s")
        },
        Subscription {
            wal_receiver_timeout: Some("30s".into()),
            ..sub("s")
        },
    ];
    let hashes: Vec<String> = variants.into_iter().map(hash_of).collect();
    for h in &hashes {
        assert_ne!(h, &base, "a PG19 field must change the hash");
    }
    for (i, a) in hashes.iter().enumerate() {
        for b in &hashes[i + 1..] {
            assert_ne!(a, b, "distinct PG19 fields must hash differently");
        }
    }
    assert_ne!(
        hash_of(sub_on_server("s", "srv_a")),
        hash_of(sub_on_server("s", "srv_b")),
        "a server switch must change the hash"
    );
    assert_ne!(
        hash_of(Subscription {
            wal_receiver_timeout: Some("30s".into()),
            ..sub("s")
        }),
        hash_of(Subscription {
            wal_receiver_timeout: Some("10s".into()),
            ..sub("s")
        })
    );
}

#[test]
fn subscription_get_script_without_pg19_fields_is_unchanged() {
    assert_eq!(
        statements(&sub("\"my_sub\"").get_script()),
        vec![
            "CREATE SUBSCRIPTION \"my_sub\" CONNECTION 'host=primary dbname=mydb' PUBLICATION my_pub;",
            "ALTER SUBSCRIPTION \"my_sub\" OWNER TO postgres;",
        ]
    );
    // A disabled subscription is created without contacting its publisher
    // (r4-sub): pgc 1.0.x wrote WITH (enabled = false), which connected and
    // created a slot there.
    let disabled = Subscription {
        enabled: false,
        ..sub("s")
    };
    assert_eq!(
        statements(&disabled.get_script())[1],
        "CREATE SUBSCRIPTION s CONNECTION 'host=primary dbname=mydb' PUBLICATION my_pub \
         WITH (connect = false, enabled = false);"
    );
}

#[test]
fn subscription_get_script_on_server_emits_server_not_connection() {
    let script = sub_on_server("s", "\"Probe Srv\"").get_script();
    assert_eq!(
        statements(&script)[0],
        "CREATE SUBSCRIPTION s SERVER \"Probe Srv\" PUBLICATION my_pub;"
    );
    assert!(!script.contains("CONNECTION"), "got: {script}");
}

#[test]
fn subscription_get_script_emits_non_default_pg19_options_in_order() {
    let s = Subscription {
        enabled: false,
        retain_dead_tuples: true,
        max_retention_duration: 60000,
        wal_receiver_timeout: Some("30s".into()),
        ..sub("s")
    };
    assert_eq!(
        statements(&s.get_script())[1],
        "CREATE SUBSCRIPTION s CONNECTION 'host=primary dbname=mydb' PUBLICATION my_pub \
         WITH (connect = false, enabled = false, retain_dead_tuples = true, \
         max_retention_duration = 60000, wal_receiver_timeout = '30s');"
    );
    let only_timeout = Subscription {
        wal_receiver_timeout: Some("it's".into()),
        ..sub("s")
    };
    assert!(
        only_timeout
            .get_script()
            .contains(" WITH (wal_receiver_timeout = 'it''s');"),
        "quotes must be escaped and enabled = true left out"
    );
}

#[test]
fn subscription_alter_source_switches() {
    let conn = sub("s");
    let srv_a = sub_on_server("s", "srv_a");
    let srv_b = sub_on_server("s", "srv_b");

    assert_eq!(
        statements(&conn.get_alter_script(&srv_a, true)),
        vec!["ALTER SUBSCRIPTION s SERVER srv_a;"]
    );
    assert_eq!(
        statements(&srv_a.get_alter_script(&srv_b, true)),
        vec!["ALTER SUBSCRIPTION s SERVER srv_b;"]
    );
    assert_eq!(
        statements(&srv_a.get_alter_script(&conn, true)),
        vec!["ALTER SUBSCRIPTION s CONNECTION 'host=primary dbname=mydb';"]
    );
    assert!(srv_a.get_alter_script(&srv_a.clone(), true).is_empty());

    let other_conn = Subscription {
        connection: "host=o'ther".into(),
        ..sub("s")
    };
    assert_eq!(
        statements(&conn.get_alter_script(&other_conn, true)),
        vec!["ALTER SUBSCRIPTION s CONNECTION 'host=o''ther';"],
        "the pre-PG19 CONNECTION path must stay byte-identical"
    );
}

#[test]
fn subscription_alter_options_only_is_one_set_without_disable() {
    let from = sub("s");
    let to = Subscription {
        max_retention_duration: 10000,
        wal_receiver_timeout: Some("2min".into()),
        ..sub("s")
    };
    assert_eq!(
        statements(&from.get_alter_script(&to, true)),
        vec![
            "ALTER SUBSCRIPTION s SET (max_retention_duration = 10000, wal_receiver_timeout = '2min');"
        ],
        "max_retention_duration and wal_receiver_timeout are legal on an enabled subscription"
    );
}

#[test]
fn subscription_alter_wal_receiver_timeout_reset_emits_minus_one() {
    let from = Subscription {
        enabled: false,
        wal_receiver_timeout: Some("45s".into()),
        ..sub("s")
    };
    let to = Subscription {
        enabled: false,
        ..sub("s")
    };
    assert_eq!(
        statements(&from.get_alter_script(&to, true)),
        vec!["ALTER SUBSCRIPTION s SET (wal_receiver_timeout = '-1');"]
    );
    assert_eq!(
        statements(&from.get_alter_script(&to.clone(), true)),
        statements(&from.get_alter_script(&to, false)),
        "resetting an option is not a drop"
    );
}

#[test]
fn subscription_alter_retain_dead_tuples_on_enabled_bounces_the_subscription() {
    let from = sub("s");
    let to = Subscription {
        retain_dead_tuples: true,
        ..sub("s")
    };
    let script = from.get_alter_script(&to, true);
    let steps = blocks(&script);
    assert!(
        steps[0].starts_with("-- retain_dead_tuples can only change"),
        "got: {script}"
    );
    assert_eq!(
        steps[1..],
        [
            "ALTER SUBSCRIPTION s DISABLE;",
            WAIT_S,
            "ALTER SUBSCRIPTION s SET (retain_dead_tuples = true);",
            "ALTER SUBSCRIPTION s ENABLE;",
        ]
    );

    // Enabled -> disabled: the bounce's DISABLE is the final state.
    let to_disabled = Subscription {
        enabled: false,
        retain_dead_tuples: true,
        ..sub("s")
    };
    let script = from.get_alter_script(&to_disabled, true);
    assert_eq!(
        blocks(&script)[1..],
        [
            "ALTER SUBSCRIPTION s DISABLE;",
            WAIT_S,
            "ALTER SUBSCRIPTION s SET (retain_dead_tuples = true);",
        ]
    );
}

#[test]
fn subscription_alter_retain_dead_tuples_on_disabled_needs_no_bounce() {
    let from = Subscription {
        enabled: false,
        retain_dead_tuples: true,
        max_retention_duration: 60000,
        ..sub("s")
    };
    let to = Subscription {
        enabled: false,
        max_retention_duration: 5000,
        ..sub("s")
    };
    assert_eq!(
        statements(&from.get_alter_script(&to, true)),
        vec![
            "ALTER SUBSCRIPTION s SET (retain_dead_tuples = false, max_retention_duration = 5000);"
        ]
    );

    // Disabled -> enabled while turning it on: SET first, ENABLE last.
    let from = Subscription {
        enabled: false,
        ..sub("s")
    };
    let to = Subscription {
        retain_dead_tuples: true,
        ..sub("s")
    };
    assert_eq!(
        statements(&from.get_alter_script(&to, true)),
        vec![
            "ALTER SUBSCRIPTION s SET (retain_dead_tuples = true);",
            "ALTER SUBSCRIPTION s ENABLE;",
        ]
    );
}

#[test]
fn subscription_alter_retain_dead_tuples_off_runs_before_the_source_change() {
    // While retain_dead_tuples is on, a CONNECTION / SERVER change makes the
    // server contact the (new) publisher, so it is switched off first.
    let from = Subscription {
        enabled: false,
        retain_dead_tuples: true,
        connection: "host=reachable".into(),
        ..sub("s")
    };
    let to = Subscription {
        enabled: false,
        connection: "host=unreachable".into(),
        ..sub("s")
    };
    assert_eq!(
        statements(&from.get_alter_script(&to, true)),
        vec![
            "ALTER SUBSCRIPTION s SET (retain_dead_tuples = false);",
            "ALTER SUBSCRIPTION s CONNECTION 'host=unreachable';",
        ]
    );
    let to_server = Subscription {
        enabled: false,
        ..sub_on_server("s", "srv")
    };
    assert_eq!(
        statements(&from.get_alter_script(&to_server, true)),
        vec![
            "ALTER SUBSCRIPTION s SET (retain_dead_tuples = false);",
            "ALTER SUBSCRIPTION s SERVER srv;",
        ]
    );
}

#[test]
fn subscription_alter_retain_dead_tuples_on_runs_after_the_source_change() {
    // Turning it on always contacts the publisher: check the new one.
    let from = Subscription {
        enabled: false,
        connection: "host=unreachable".into(),
        ..sub("s")
    };
    let to = Subscription {
        enabled: false,
        retain_dead_tuples: true,
        connection: "host=reachable".into(),
        publications: vec!["p2".into()],
        ..sub("s")
    };
    assert_eq!(
        statements(&from.get_alter_script(&to, true)),
        vec![
            "ALTER SUBSCRIPTION s CONNECTION 'host=reachable';",
            // Disabled throughout: a refreshing SET PUBLICATION is refused.
            "ALTER SUBSCRIPTION s SET PUBLICATION p2 WITH (refresh = false);",
            "ALTER SUBSCRIPTION s SET (retain_dead_tuples = true);",
        ]
    );
}

#[test]
fn subscription_alter_bounce_turning_off_stays_disabled_through_the_source_change() {
    // A CONNECTION change on an enabled subscription restarts its worker;
    // made while it is disabled, it restarts nothing, and the ENABLE comes
    // last.
    let from = Subscription {
        retain_dead_tuples: true,
        ..sub("s")
    };
    let to = Subscription {
        connection: "host=elsewhere".into(),
        ..sub("s")
    };
    let script = from.get_alter_script(&to, true);
    assert_eq!(
        blocks(&script)[1..],
        [
            "ALTER SUBSCRIPTION s DISABLE;",
            WAIT_S,
            "ALTER SUBSCRIPTION s SET (retain_dead_tuples = false);",
            "ALTER SUBSCRIPTION s CONNECTION 'host=elsewhere';",
            "ALTER SUBSCRIPTION s ENABLE;",
        ],
        "got: {script}"
    );
}

#[test]
fn subscription_alter_without_pg19_changes_keeps_the_pre_pg19_order() {
    let from = sub("s");
    let to = Subscription {
        connection: "host=new".into(),
        publications: vec!["p1".into(), "p2".into()],
        enabled: false,
        owner: "alice".into(),
        comment: Some("c".into()),
        ..sub("s")
    };
    assert_eq!(
        statements(&from.get_alter_script(&to, true)),
        vec![
            "ALTER SUBSCRIPTION s CONNECTION 'host=new';",
            "ALTER SUBSCRIPTION s SET PUBLICATION p1, p2;",
            "ALTER SUBSCRIPTION s DISABLE;",
            "ALTER SUBSCRIPTION s OWNER TO alice;",
            "COMMENT ON SUBSCRIPTION s IS 'c';",
        ]
    );
}

#[test]
fn subscription_serde_skips_default_pg19_fields() {
    let json = serde_json::to_value(sub("s")).unwrap();
    for key in [
        "server",
        "retain_dead_tuples",
        "max_retention_duration",
        "wal_receiver_timeout",
    ] {
        assert!(
            json.get(key).is_none(),
            "{key} must not be serialized at its default"
        );
    }
    assert_eq!(json["connection"], "host=primary dbname=mydb");

    let full = Subscription {
        retain_dead_tuples: true,
        max_retention_duration: 5,
        wal_receiver_timeout: Some("1min".into()),
        ..sub_on_server("s", "srv")
    };
    let back: Subscription = serde_json::from_value(serde_json::to_value(&full).unwrap()).unwrap();
    assert_eq!(back.server.as_deref(), Some("srv"));
    assert!(back.retain_dead_tuples);
    assert_eq!(back.max_retention_duration, 5);
    assert_eq!(back.wal_receiver_timeout.as_deref(), Some("1min"));
    assert_eq!(back.connection, "", "older pgc versions still find the key");
}

#[test]
fn subscription_alter_bounce_to_a_disabled_target_still_refreshes_the_new_publications() {
    // PostgreSQL refuses SET PUBLICATION's default refresh on a disabled
    // subscription ("ALTER SUBSCRIPTION with refresh is not allowed for
    // disabled subscriptions"). A bounced subscription is disabled for the
    // SET PUBLICATION, so it is written WITH (refresh = false), and the
    // subscription is enabled for the REFRESH PUBLICATION that SET would have
    // made on the enabled FROM subscription, then disabled again.
    let from = Subscription {
        retain_dead_tuples: true,
        ..sub("s")
    };
    let to = Subscription {
        enabled: false,
        publications: vec!["my_pub".into(), "p2".into()],
        ..sub("s")
    };
    let script = from.get_alter_script(&to, true);
    assert_eq!(
        blocks(&script)[1..],
        [
            "ALTER SUBSCRIPTION s DISABLE;",
            WAIT_S,
            "ALTER SUBSCRIPTION s SET (retain_dead_tuples = false);",
            "ALTER SUBSCRIPTION s SET PUBLICATION my_pub, p2 WITH (refresh = false);",
            "ALTER SUBSCRIPTION s ENABLE;",
            "ALTER SUBSCRIPTION s REFRESH PUBLICATION;",
            "ALTER SUBSCRIPTION s DISABLE;",
        ],
        "got: {script}"
    );

    // Without a publication change the bounce's DISABLE stays the final state.
    let to_same_publications = Subscription {
        enabled: false,
        ..sub("s")
    };
    let script = from.get_alter_script(&to_same_publications, true);
    assert_eq!(
        blocks(&script)[1..],
        [
            "ALTER SUBSCRIPTION s DISABLE;",
            WAIT_S,
            "ALTER SUBSCRIPTION s SET (retain_dead_tuples = false);",
        ],
        "got: {script}"
    );

    // Turning it on: the SET follows SET PUBLICATION, and nothing between
    // the DISABLE and the SET starts a worker.
    let from_off = sub("s");
    let to_on = Subscription {
        enabled: false,
        retain_dead_tuples: true,
        publications: vec!["my_pub".into(), "p2".into()],
        ..sub("s")
    };
    let script = from_off.get_alter_script(&to_on, true);
    assert_eq!(
        blocks(&script)[1..],
        [
            "ALTER SUBSCRIPTION s DISABLE;",
            WAIT_S,
            "ALTER SUBSCRIPTION s SET PUBLICATION my_pub, p2 WITH (refresh = false);",
            "ALTER SUBSCRIPTION s SET (retain_dead_tuples = true);",
            "ALTER SUBSCRIPTION s ENABLE;",
            "ALTER SUBSCRIPTION s REFRESH PUBLICATION;",
            "ALTER SUBSCRIPTION s DISABLE;",
        ],
        "got: {script}"
    );
}

#[test]
fn subscription_alter_owner_change_precedes_a_server_switch() {
    // ALTER SUBSCRIPTION ... SERVER checks the current owner's user mapping,
    // USAGE and (for a non-superuser) password, so it runs as the owner the
    // subscription will connect as.
    let from = sub_on_server("s", "srv_a");
    let to = Subscription {
        owner: "alice".into(),
        ..sub_on_server("s", "srv_b")
    };
    assert_eq!(
        statements(&from.get_alter_script(&to, true)),
        vec![
            "ALTER SUBSCRIPTION s OWNER TO alice;",
            "ALTER SUBSCRIPTION s SERVER srv_b;",
        ]
    );
    assert_eq!(
        statements(&sub("s").get_alter_script(&to, true)),
        vec![
            "ALTER SUBSCRIPTION s OWNER TO alice;",
            "ALTER SUBSCRIPTION s SERVER srv_b;",
        ],
        "CONNECTION -> SERVER too"
    );

    // A CONNECTION target keeps switching before the owner changes, as in
    // pgc 1.0.x: its password check favours the old (superuser) owner.
    let to_conn = Subscription {
        owner: "alice".into(),
        ..sub("s")
    };
    assert_eq!(
        statements(&from.get_alter_script(&to_conn, true)),
        vec![
            "ALTER SUBSCRIPTION s CONNECTION 'host=primary dbname=mydb';",
            "ALTER SUBSCRIPTION s OWNER TO alice;",
        ]
    );

    // No server switch: OWNER TO stays in its usual place.
    let same_server = Subscription {
        owner: "alice".into(),
        comment: Some("c".into()),
        wal_receiver_timeout: Some("1min".into()),
        ..sub_on_server("s", "srv_a")
    };
    assert_eq!(
        statements(&from.get_alter_script(&same_server, true)),
        vec![
            "ALTER SUBSCRIPTION s SET (wal_receiver_timeout = '1min');",
            "ALTER SUBSCRIPTION s OWNER TO alice;",
            "COMMENT ON SUBSCRIPTION s IS 'c';",
        ]
    );

    // Turning retain_dead_tuples off still comes before the source change.
    let from_rdt = Subscription {
        enabled: false,
        retain_dead_tuples: true,
        ..sub_on_server("s", "srv_a")
    };
    let to_rdt = Subscription {
        enabled: false,
        owner: "alice".into(),
        ..sub_on_server("s", "srv_b")
    };
    assert_eq!(
        statements(&from_rdt.get_alter_script(&to_rdt, true)),
        vec![
            "ALTER SUBSCRIPTION s SET (retain_dead_tuples = false);",
            "ALTER SUBSCRIPTION s OWNER TO alice;",
            "ALTER SUBSCRIPTION s SERVER srv_b;",
        ]
    );
}

#[test]
fn subscription_restarts_apply_worker_only_for_retain_dead_tuples_while_enabled() {
    let on = Subscription {
        retain_dead_tuples: true,
        ..sub("s")
    };
    assert!(sub("s").restarts_apply_worker(&on));
    assert!(on.restarts_apply_worker(&sub("s")));

    let disabled = Subscription {
        enabled: false,
        ..sub("s")
    };
    assert!(
        !disabled.restarts_apply_worker(&on),
        "a disabled subscription has no worker to wait for"
    );
    let other_options = Subscription {
        max_retention_duration: 5000,
        wal_receiver_timeout: Some("10s".into()),
        connection: "host=elsewhere".into(),
        ..sub("s")
    };
    assert!(!sub("s").restarts_apply_worker(&other_options));
    assert!(!on.restarts_apply_worker(&on.clone()));
}

// ==== fix:pre-grants tests ====

fn spec_of(columns: &[&str], row_filter: Option<&str>) -> PublicationTableSpec {
    PublicationTableSpec {
        columns: columns.iter().map(|c| c.to_string()).collect(),
        row_filter: row_filter.map(str::to_string),
    }
}

fn names(items: &[&str]) -> HashSet<String> {
    items.iter().map(|c| c.to_string()).collect()
}

#[test]
fn subscription_create_quotes_publication_names() {
    // subpublications holds raw names: a mixed-case name folded to lower case
    // and a spaced or keyword name did not parse.
    let s = Subscription {
        publications: vec![
            "MyPub".into(),
            "pub two".into(),
            "select".into(),
            "plain".into(),
        ],
        enabled: false,
        ..sub("s")
    };
    assert_eq!(
        s.get_script()
            .lines()
            .find(|l| l.starts_with("CREATE"))
            .unwrap(),
        "CREATE SUBSCRIPTION s CONNECTION 'host=primary dbname=mydb' \
         PUBLICATION \"MyPub\", \"pub two\", \"select\", plain WITH (connect = false, enabled = false);"
    );
}

#[test]
fn subscription_publication_names_stay_bare_when_quote_ident_would() {
    // Output for ordinary names is what pgc 1.0.x emitted.
    let s = Subscription {
        publications: vec!["p1".into(), "orders_pub".into()],
        ..sub("s")
    };
    assert_eq!(s.publication_list(), "p1, orders_pub");
    assert!(s.get_script().contains(" PUBLICATION p1, orders_pub;"));
}

#[test]
fn subscription_hash_keeps_raw_publication_names() {
    // The dump keeps subpublications raw, so a subscription on a name that
    // needs quotes hashes as pgc 1.0.x hashed it.
    let raw = Subscription {
        publications: vec!["MyPub".into()],
        ..sub("s")
    };
    let mut legacy = raw.clone();
    legacy.hash = None;
    assert_eq!(hash_of(raw), hash_of(legacy));
    let quoted = Subscription {
        publications: vec!["\"MyPub\"".into()],
        ..sub("s")
    };
    assert_ne!(
        hash_of(quoted),
        hash_of(Subscription {
            publications: vec!["MyPub".into()],
            ..sub("s")
        })
    );
}

#[test]
fn subscription_set_publication_quotes_names() {
    let from = sub("s");
    let to = Subscription {
        publications: vec!["MyPub".into(), "pub two".into()],
        ..sub("s")
    };
    assert_eq!(
        statements(&from.get_alter_script(&to, true)),
        vec!["ALTER SUBSCRIPTION s SET PUBLICATION \"MyPub\", \"pub two\";"]
    );
}

#[test]
fn subscription_set_publication_on_an_enabled_subscription_keeps_its_refresh() {
    let from = sub("s");
    let to = Subscription {
        publications: vec!["my_pub".into(), "p2".into()],
        ..sub("s")
    };
    assert_eq!(
        statements(&from.get_alter_script(&to, true)),
        vec!["ALTER SUBSCRIPTION s SET PUBLICATION my_pub, p2;"]
    );
}

#[test]
fn subscription_set_publication_on_a_disabled_subscription_skips_the_refresh() {
    // PostgreSQL refuses a refreshing SET PUBLICATION on a disabled
    // subscription, and there is nothing to refresh until it is enabled.
    let from = Subscription {
        enabled: false,
        ..sub("s")
    };
    let to = Subscription {
        enabled: false,
        publications: vec!["my_pub".into(), "p2".into()],
        ..sub("s")
    };
    assert_eq!(
        statements(&from.get_alter_script(&to, true)),
        vec!["ALTER SUBSCRIPTION s SET PUBLICATION my_pub, p2 WITH (refresh = false);"]
    );
}

#[test]
fn subscription_enabled_by_the_migration_refreshes_after_the_enable() {
    let from = Subscription {
        enabled: false,
        ..sub("s")
    };
    let to = Subscription {
        publications: vec!["my_pub".into(), "p2".into()],
        ..sub("s")
    };
    assert_eq!(
        statements(&from.get_alter_script(&to, true)),
        vec![
            "ALTER SUBSCRIPTION s SET PUBLICATION my_pub, p2 WITH (refresh = false);",
            "ALTER SUBSCRIPTION s ENABLE;",
            "ALTER SUBSCRIPTION s REFRESH PUBLICATION;",
        ]
    );
}

#[test]
fn subscription_disabled_by_the_migration_refreshes_before_the_disable() {
    // Unchanged: the refresh runs while it is still enabled.
    let from = sub("s");
    let to = Subscription {
        enabled: false,
        publications: vec!["p2".into()],
        ..sub("s")
    };
    assert_eq!(
        statements(&from.get_alter_script(&to, true)),
        vec![
            "ALTER SUBSCRIPTION s SET PUBLICATION p2;",
            "ALTER SUBSCRIPTION s DISABLE;",
        ]
    );
}

#[test]
fn subscription_set_publication_in_a_single_transaction_refreshes_after_commit() {
    // Refreshing is refused inside a transaction block, even when enabled.
    let from = sub("s");
    let to = Subscription {
        publications: vec!["p2".into()],
        ..sub("s")
    };
    let alter = from.get_alter_script_with(&to, true, true);
    assert_eq!(
        statements(&alter.script),
        vec!["ALTER SUBSCRIPTION s SET PUBLICATION p2 WITH (refresh = false);"]
    );
    assert_eq!(
        statements(&alter.post_commit),
        vec!["ALTER SUBSCRIPTION s REFRESH PUBLICATION;"]
    );
    // Without a single transaction nothing waits for the commit.
    let plain = from.get_alter_script_with(&to, true, false);
    assert!(plain.post_commit.is_empty());
    assert_eq!(plain.script, from.get_alter_script(&to, true));
}

#[test]
fn subscription_left_disabled_in_a_single_transaction_is_not_refreshed() {
    let from = Subscription {
        enabled: false,
        ..sub("s")
    };
    let to = Subscription {
        enabled: false,
        publications: vec!["p2".into()],
        ..sub("s")
    };
    let alter = from.get_alter_script_with(&to, true, true);
    assert_eq!(
        statements(&alter.script),
        vec!["ALTER SUBSCRIPTION s SET PUBLICATION p2 WITH (refresh = false);"]
    );
    assert!(alter.post_commit.is_empty());
}

#[test]
fn subscription_bounce_in_a_single_transaction_does_not_reenable_for_the_set_publication() {
    // retain_dead_tuples turns off on an enabled subscription that ends
    // disabled: outside a transaction it is enabled once more for the
    // refresh; inside one nothing can refresh before the commit, so it stays
    // off, and no wait is emitted either (the DISABLE is not committed yet,
    // so waiting could not help).
    let from = Subscription {
        retain_dead_tuples: true,
        ..sub("s")
    };
    let to = Subscription {
        enabled: false,
        publications: vec!["p2".into()],
        ..sub("s")
    };
    let script = from.get_alter_script(&to, true);
    assert_eq!(
        blocks(&script)[1..],
        [
            "ALTER SUBSCRIPTION s DISABLE;",
            WAIT_S,
            "ALTER SUBSCRIPTION s SET (retain_dead_tuples = false);",
            "ALTER SUBSCRIPTION s SET PUBLICATION p2 WITH (refresh = false);",
            "ALTER SUBSCRIPTION s ENABLE;",
            "ALTER SUBSCRIPTION s REFRESH PUBLICATION;",
            "ALTER SUBSCRIPTION s DISABLE;",
        ]
    );
    let inside = from.get_alter_script_with(&to, true, true);
    assert_eq!(
        blocks(&inside.script)[1..],
        [
            "ALTER SUBSCRIPTION s DISABLE;",
            "ALTER SUBSCRIPTION s SET (retain_dead_tuples = false);",
            "ALTER SUBSCRIPTION s SET PUBLICATION p2 WITH (refresh = false);",
        ]
    );
    assert!(inside.post_commit.is_empty());
}

#[test]
fn table_spec_uses_a_column_of_its_column_list() {
    let spec = spec_of(&["code", "id", "qty"], None);
    assert!(spec.uses_any_column(&names(&["qty"])));
    assert!(!spec.uses_any_column(&names(&["other"])));
    assert!(!PublicationTableSpec::default().uses_any_column(&names(&["qty"])));
}

#[test]
fn table_spec_uses_a_column_of_its_row_filter() {
    let spec = spec_of(&[], Some("((qty > 0) AND (\"Code\" <> 'qty'::text))"));
    assert!(spec.uses_any_column(&names(&["qty"])));
    assert!(spec.uses_any_column(&names(&["\"Code\""])));
    // A column spelled differently is another column; a literal is no column.
    assert!(!spec.uses_any_column(&names(&["code"])));
    assert!(!spec_of(&[], Some("(note = 'qty'::text)")).uses_any_column(&names(&["qty"])));
    // A doubled quote stays inside the literal or identifier.
    assert!(
        spec_of(&[], Some("((note <> 'it''s'::text) AND (\"a\"\"b\" > 1))"))
            .uses_any_column(&names(&["\"a\"\"b\""]))
    );
    // Numbers are not identifiers.
    assert!(!spec_of(&[], Some("(qty > 1e5)")).uses_any_column(&names(&["e5"])));
}

#[test]
fn release_script_drops_members_with_only() {
    let p = listing("p", &["s.a", "s.b", "s.c"]);
    assert_eq!(
        p.get_release_script(&["s.a".into(), "s.c".into()], &[]),
        "ALTER PUBLICATION p DROP TABLE ONLY s.a, ONLY s.c;\n\n"
    );
    assert_eq!(p.get_release_script(&[], &[]), "");
}

#[test]
fn release_script_keeps_the_other_except_entries() {
    let p = Publication {
        except_tables: vec!["s.a".into(), "s.b".into()],
        all_sequences: true,
        ..all_tables("p")
    };
    assert_eq!(
        p.get_release_script(&[], &["s.a".into()]),
        "ALTER PUBLICATION p SET ALL TABLES EXCEPT (TABLE ONLY s.b), ALL SEQUENCES;\n\n"
    );
    let single = Publication {
        except_tables: vec!["s.a".into()],
        ..all_tables("p")
    };
    assert_eq!(
        single.get_release_script(&[], &["s.a".into()]),
        "ALTER PUBLICATION p SET ALL TABLES;\n\n"
    );
}

fn released(publication: &str, tables: &[&str]) -> PublicationAlterContext {
    let mut ctx = PublicationAlterContext::default();
    ctx.released_tables
        .insert(publication.to_string(), names(tables));
    ctx
}

#[test]
fn released_member_is_restored_with_the_target_spec() {
    // The publication itself does not change, but the member was taken out
    // ahead of the table change it blocked.
    let mut from = listing("p", &["s.p", "s.q"]);
    from.table_specs
        .insert("s.p".into(), spec_of(&["code", "id"], Some("(qty > 0)")));
    let from = hashed(from);
    let to = from.clone();
    let ctx = released("p", &["s.p"]);
    assert!(ctx.touches(&from));
    assert!(!PublicationAlterContext::default().touches(&from));
    assert_eq!(
        from.get_alter_script_with(&to, true, &ctx),
        "ALTER PUBLICATION p SET TABLE s.p (code, id) WHERE ((qty > 0)), s.q;\n\n"
    );
}

#[test]
fn released_member_counts_only_for_its_own_publication() {
    let from = listing("other", &["s.p"]);
    let ctx = released("p", &["s.p"]);
    assert!(!ctx.touches(&from));
    // Emptying the other publication still drops the member.
    assert_eq!(
        from.get_alter_script_with(&base("other"), true, &ctx),
        "ALTER PUBLICATION other DROP TABLE ONLY s.p;\n\n"
    );
}

#[test]
fn released_member_the_target_drops_is_not_dropped_again() {
    let from = listing("p", &["s.p", "s.q"]);
    let to = listing("p", &["s.q"]);
    assert_eq!(
        from.get_alter_script_with(&to, true, &released("p", &["s.p"])),
        "ALTER PUBLICATION p SET TABLE s.q;\n\n"
    );
    // A publication the target empties drops only what is still there.
    assert_eq!(
        from.get_alter_script_with(&base("p"), true, &released("p", &["s.p"])),
        "ALTER PUBLICATION p DROP TABLE ONLY s.q;\n\n"
    );
}

#[test]
fn released_except_entry_is_restored_to_the_target_list() {
    let from = hashed(Publication {
        except_tables: vec!["s.a".into(), "s.b".into()],
        ..all_tables("p")
    });
    let to = hashed(Publication {
        except_tables: vec!["s.b".into()],
        ..all_tables("p")
    });
    let mut ctx = released("p", &["s.a"]);
    ctx.set_all_supported = true;
    assert_eq!(
        from.get_alter_script_with(&to, true, &ctx),
        "ALTER PUBLICATION p SET ALL TABLES EXCEPT (TABLE ONLY s.b);\n\n"
    );
}

// ---- fix:pre-grants review: restoring released rows of a kept publication ----

#[test]
fn restore_script_sets_the_publications_own_list_again() {
    // A SET, so a replayed migration does not fail on a member already back.
    let mut p = listing("p", &["s.a", "s.b", "s.c"]);
    p.table_specs
        .insert("s.a".into(), spec_of(&["id", "qty"], Some("(qty > 0)")));
    p.schemas = vec!["s2".into()];
    let ctx = PublicationAlterContext {
        inheritance_parents: names(&["s.c"]),
        ..Default::default()
    };
    assert_eq!(
        p.get_restore_script(&names(&["s.a", "s.c"]), &HashSet::new(), &ctx),
        "ALTER PUBLICATION p SET TABLE s.a (id, qty) WHERE ((qty > 0)), s.b, ONLY s.c, TABLES IN SCHEMA s2;\n\n"
    );
    assert_eq!(
        p.get_restore_script(&HashSet::new(), &HashSet::new(), &ctx),
        ""
    );
}

#[test]
fn restore_script_notes_what_cannot_come_back() {
    let p = listing("p", &["s.a", "s.b"]);
    assert_eq!(
        p.get_restore_script(
            &names(&["s.a", "s.b"]),
            &names(&["s.b"]),
            &PublicationAlterContext::default()
        ),
        "ALTER PUBLICATION p SET TABLE s.a;\n\n\
         -- publication p: member s.b was dropped because the migration makes it UNLOGGED, which a publication cannot hold\n"
    );
    let except = Publication {
        except_tables: vec!["s.x".into(), "s.y".into()],
        ..all_tables("p")
    };
    assert_eq!(
        except.get_restore_script(
            &names(&["s.y"]),
            &HashSet::new(),
            &PublicationAlterContext::default()
        ),
        "-- publication p: s.y was taken out of EXCEPT because the migration makes it UNLOGGED or a partition, which EXCEPT cannot hold\n"
    );
    // A name holding a newline cannot end the note early.
    // Nothing to set when every released member is lost.
    let only = listing("p", &["s.b"]);
    assert_eq!(
        only.get_restore_script(
            &names(&["s.b"]),
            &names(&["s.b"]),
            &PublicationAlterContext::default()
        )
        .lines()
        .count(),
        1
    );
    let odd = listing("p", &["s.\"a\nb\""]);
    let script = odd.get_restore_script(
        &names(&["s.\"a\nb\""]),
        &names(&["s.\"a\nb\""]),
        &PublicationAlterContext::default(),
    );
    assert_eq!(script.lines().count(), 1, "{script}");
}

#[test]
fn alter_recreates_follows_the_state_machine() {
    let list = hashed(listing("p", &["s.t"]));
    let all = hashed(all_tables("p"));
    let ctx = PublicationAlterContext::default();
    // Into or out of the all-objects kind before PostgreSQL 19.
    assert!(list.alter_recreates(&all, &ctx));
    assert!(all.alter_recreates(&list, &ctx));
    // Into it on 19: SET ALL, in place.
    assert!(!list.alter_recreates(&all, &pg19()));
    // Out of it is a recreate on 19 as well.
    assert!(all.alter_recreates(&list, &pg19()));
    // Between lists, and with nothing to change.
    assert!(!list.alter_recreates(&hashed(listing("p", &["s.u"])), &ctx));
    assert!(!list.alter_recreates(&list, &ctx));
    assert!(!all.alter_recreates(&all, &ctx));
}

// ==== r3-compat tests ====

// PostgreSQL 19 refuses `SET (retain_dead_tuples = ...)` while any worker of
// the subscription runs. A `SET PUBLICATION`, source change or owner switch
// made while the subscription is enabled restarts its worker, and the launcher
// starts the new one at once: a `DISABLE` + `SET` right after it met that
// worker in almost every live run. So a bounced subscription is disabled
// before anything else, and a `DO` block waits for its workers to exit.

#[test]
fn subscription_bounce_disables_before_a_set_publication() {
    // The reported case: SET PUBLICATION used to run first, on the enabled
    // subscription, and restart its worker right before the SET.
    let from = sub("s3");
    let to = Subscription {
        publications: vec!["pp".into(), "pq".into()],
        retain_dead_tuples: true,
        max_retention_duration: 1000,
        ..sub("s3")
    };
    let script = from.get_alter_script(&to, true);
    assert_eq!(
        blocks(&script)[1..],
        [
            "ALTER SUBSCRIPTION s3 DISABLE;",
            from.worker_exit_wait_script().as_str(),
            "ALTER SUBSCRIPTION s3 SET PUBLICATION pp, pq WITH (refresh = false);",
            "ALTER SUBSCRIPTION s3 SET (retain_dead_tuples = true, max_retention_duration = 1000);",
            "ALTER SUBSCRIPTION s3 ENABLE;",
            "ALTER SUBSCRIPTION s3 REFRESH PUBLICATION;",
        ],
        "got: {script}"
    );
}

#[test]
fn subscription_bounce_disables_before_a_connection_change() {
    // Turning it on still runs after the source change (the check contacts
    // the new publisher), but the change is made while it is disabled.
    let from = sub("s");
    let to = Subscription {
        connection: "host=elsewhere".into(),
        retain_dead_tuples: true,
        ..sub("s")
    };
    let script = from.get_alter_script(&to, true);
    assert_eq!(
        blocks(&script)[1..],
        [
            "ALTER SUBSCRIPTION s DISABLE;",
            WAIT_S,
            "ALTER SUBSCRIPTION s CONNECTION 'host=elsewhere';",
            "ALTER SUBSCRIPTION s SET (retain_dead_tuples = true);",
            "ALTER SUBSCRIPTION s ENABLE;",
        ],
        "got: {script}"
    );
}

#[test]
fn subscription_bounce_disables_before_an_owner_and_server_switch() {
    // OWNER TO restarts the worker as well; it still precedes the SERVER
    // switch, which checks the new owner's mapping.
    let from = sub_on_server("s", "srv_a");
    let to = Subscription {
        owner: "alice".into(),
        retain_dead_tuples: true,
        comment: Some("c".into()),
        ..sub_on_server("s", "srv_b")
    };
    let script = from.get_alter_script(&to, true);
    assert_eq!(
        blocks(&script)[1..],
        [
            "ALTER SUBSCRIPTION s DISABLE;",
            WAIT_S,
            "ALTER SUBSCRIPTION s OWNER TO alice;",
            "ALTER SUBSCRIPTION s SERVER srv_b;",
            "ALTER SUBSCRIPTION s SET (retain_dead_tuples = true);",
            "ALTER SUBSCRIPTION s ENABLE;",
            "COMMENT ON SUBSCRIPTION s IS 'c';",
        ],
        "got: {script}"
    );
}

#[test]
fn subscription_bounce_in_a_single_transaction_refreshes_after_commit_without_waiting() {
    // Inside begin/commit the DISABLE reaches no worker before the commit, so
    // a wait could only time out; the comparer warns on stderr instead.
    let from = sub("s");
    let to = Subscription {
        publications: vec!["p2".into()],
        retain_dead_tuples: true,
        ..sub("s")
    };
    let alter = from.get_alter_script_with(&to, true, true);
    let steps = blocks(&alter.script);
    assert!(
        steps[0].starts_with("-- retain_dead_tuples can only change")
            && steps[0].contains("outside a single transaction"),
        "got: {}",
        alter.script
    );
    assert_eq!(
        steps[1..],
        [
            "ALTER SUBSCRIPTION s DISABLE;",
            "ALTER SUBSCRIPTION s SET PUBLICATION p2 WITH (refresh = false);",
            "ALTER SUBSCRIPTION s SET (retain_dead_tuples = true);",
            "ALTER SUBSCRIPTION s ENABLE;",
        ]
    );
    assert!(!alter.script.contains("DO $"), "{}", alter.script);
    assert_eq!(
        blocks(&alter.post_commit),
        ["ALTER SUBSCRIPTION s REFRESH PUBLICATION;"]
    );
}

#[test]
fn subscription_worker_exit_wait_matches_the_raw_name_in_the_current_database() {
    assert_eq!(sub("s").worker_exit_wait_script(), WAIT_S);

    // The catalog holds the raw name; the dump holds it quote_ident'ed.
    let quoted = sub("\"My Sub\"").worker_exit_wait_script();
    assert!(quoted.contains("WHERE s.subname = 'My Sub'\n"), "{quoted}");
    let apostrophe = sub("\"it's \"\"x\"\"\"").worker_exit_wait_script();
    assert!(
        apostrophe.contains("WHERE s.subname = 'it''s \"x\"'\n"),
        "{apostrophe}"
    );

    // A name holding the default tag cannot end the dollar quote early.
    let tagged = sub("\"a$pgc$b\"").worker_exit_wait_script();
    assert!(tagged.starts_with("DO $pgc1$\n"), "{tagged}");
    assert!(tagged.ends_with("\n$pgc1$;"), "{tagged}");
    assert!(tagged.contains("WHERE s.subname = 'a$pgc$b'\n"), "{tagged}");

    // Bounded, and scoped to this database: pg_subscription is shared, and
    // another database may hold an enabled subscription of the same name.
    assert!(WAIT_S.contains(&format!(
        "interval '{} seconds'",
        Subscription::WORKER_EXIT_WAIT_SECONDS
    )));
    assert!(WAIT_S.contains(
        "s.subdbid = (SELECT oid FROM pg_catalog.pg_database WHERE datname = current_database())"
    ));
}

/// Every bounce, whatever else changes: the `DISABLE` comes first, then the
/// wait, and nothing between them and the `SET` can start a worker — no
/// `ENABLE`, no refreshing `SET PUBLICATION`. The subscription ends in the
/// target's state, and a changed publication list is refreshed whenever the
/// subscription was enabled at any point.
#[test]
fn subscription_bounce_never_starts_a_worker_before_the_set() {
    let sources = [
        sub("s"),
        Subscription {
            connection: "host=new".into(),
            ..sub("s")
        },
        sub_on_server("s", "srv_b"),
    ];
    let mut checked = 0;
    for from_rdt in [false, true] {
        for to_enabled in [false, true] {
            for publications in [false, true] {
                for source in &sources {
                    for owner in [false, true] {
                        for mrd in [false, true] {
                            let from = Subscription {
                                retain_dead_tuples: from_rdt,
                                ..sub("s")
                            };
                            let mut to = Subscription {
                                enabled: to_enabled,
                                retain_dead_tuples: !from_rdt,
                                ..source.clone()
                            };
                            if publications {
                                to.publications = vec!["p2".into()];
                            }
                            if owner {
                                to.owner = "alice".into();
                            }
                            if mrd {
                                to.max_retention_duration = 5000;
                            }
                            assert!(from.restarts_apply_worker(&to));
                            let script = from.get_alter_script(&to, true);
                            let steps = blocks(&script);
                            assert_eq!(steps[1], "ALTER SUBSCRIPTION s DISABLE;", "{script}");
                            assert_eq!(steps[2], WAIT_S, "{script}");
                            let set = steps
                                .iter()
                                .position(|b| b.contains("SET (retain_dead_tuples"))
                                .unwrap_or_else(|| panic!("no SET: {script}"));
                            assert!(
                                !steps[..set].iter().any(|b| b.ends_with(" ENABLE;")),
                                "{script}"
                            );
                            for b in steps.iter().filter(|b| b.contains("SET PUBLICATION")) {
                                assert!(b.ends_with("WITH (refresh = false);"), "{script}");
                            }
                            let last_state = steps
                                .iter()
                                .rev()
                                .find(|b| b.ends_with(" ENABLE;") || b.ends_with(" DISABLE;"))
                                .unwrap();
                            assert_eq!(last_state.ends_with(" ENABLE;"), to_enabled, "{script}");
                            assert_eq!(
                                script.contains("REFRESH PUBLICATION;"),
                                publications,
                                "{script}"
                            );
                            checked += 1;
                        }
                    }
                }
            }
        }
    }
    assert_eq!(checked, 2 * 2 * 2 * 3 * 2 * 2);
}

// ==== r4-sub tests ====

// ---- replication slots (subslotname), every version ----

fn with_slot(enabled: bool, slot_name: SubscriptionSlot) -> Subscription {
    Subscription {
        enabled,
        slot_name,
        ..sub("s")
    }
}

fn named(slot: &str) -> SubscriptionSlot {
    SubscriptionSlot::Named(slot.into())
}

#[test]
fn subscription_slot_from_catalog() {
    // subslotname IS NULL, NULLIF(subslotname, subname)
    assert_eq!(
        SubscriptionSlot::from_catalog(true, None),
        SubscriptionSlot::NoSlot
    );
    assert_eq!(
        SubscriptionSlot::from_catalog(false, None),
        SubscriptionSlot::Own
    );
    assert_eq!(
        SubscriptionSlot::from_catalog(false, Some("other".into())),
        named("other")
    );
    assert!(SubscriptionSlot::default().is_own());
    assert!(!SubscriptionSlot::NoSlot.is_own());
    assert!(!named("s").is_own());
}

#[test]
fn subscription_slot_resolves_its_raw_name() {
    assert_eq!(sub("s").slot().as_deref(), Some("s"));
    assert_eq!(
        sub("\"My Sub\"").slot().as_deref(),
        Some("My Sub"),
        "a slot named after the subscription carries its raw name"
    );
    assert_eq!(with_slot(false, SubscriptionSlot::NoSlot).slot(), None);
    assert_eq!(
        with_slot(true, named("other")).slot().as_deref(),
        Some("other")
    );
}

#[test]
fn subscription_hash_folds_the_slot_only_when_not_its_own() {
    // A slot named after the subscription is the default and every pgc
    // 1.0.x dump's: the pinned 1.0.27 digests hold.
    assert_eq!(
        hash_of(with_slot(true, SubscriptionSlot::Own)),
        hash_of(sub("s"))
    );
    assert_eq!(
        hash_of(Subscription {
            slot_name: SubscriptionSlot::Own,
            ..sub("\"my_sub\"")
        }),
        "6d68a5e01b54c8245549a205c784c3dcbb7b5de538b81e798856f7714f5d1038"
    );
    // Pinned digests of the tagged forms: dump files store hashes, so the
    // encoding must not drift.
    assert_eq!(
        hash_of(with_slot(false, SubscriptionSlot::NoSlot)),
        "2001d11944753584acf867a0185ffeaffb777aa9dbc2c4d2f34e250f3e7e5aaf"
    );
    assert_eq!(
        hash_of(with_slot(true, named("other"))),
        "b6709034df4238d4ee451350c1bc9b27726d840c46fcfc0ed277f7b839e83d18"
    );

    let base = hash_of(sub("s"));
    let none = hash_of(Subscription {
        slot_name: SubscriptionSlot::NoSlot,
        ..sub("s")
    });
    let a = hash_of(Subscription {
        slot_name: named("a"),
        ..sub("s")
    });
    let b = hash_of(Subscription {
        slot_name: named("b"),
        ..sub("s")
    });
    let literal_none = hash_of(Subscription {
        slot_name: named("none"),
        ..sub("s")
    });
    let all = [&base, &none, &a, &b, &literal_none];
    for (i, x) in all.iter().enumerate() {
        for y in &all[i + 1..] {
            assert_ne!(x, y, "every slot state must hash differently");
        }
    }
}

#[test]
fn subscription_create_without_a_slot_never_contacts_the_publisher() {
    let s = with_slot(false, SubscriptionSlot::NoSlot);
    assert_eq!(
        statements(&s.get_script()),
        vec![
            "CREATE SUBSCRIPTION s CONNECTION 'host=primary dbname=mydb' PUBLICATION my_pub \
             WITH (connect = false, slot_name = NONE, enabled = false);",
            "ALTER SUBSCRIPTION s OWNER TO postgres;",
        ],
        "the target's own state: no note"
    );
    assert!(!s.connects_on_create(SlotNameRules::default()));

    // An enabled subscription cannot lack a slot in the catalog; if a dump
    // said so anyway, it is still created without contacting the publisher.
    let odd = with_slot(true, SubscriptionSlot::NoSlot);
    assert!(!odd.connects_on_create(SlotNameRules::default()));
    assert!(
        odd.get_script()
            .contains(" WITH (connect = false, slot_name = NONE, enabled = false);")
    );
}

#[test]
fn subscription_create_disabled_with_a_slot_is_not_connected_and_says_so() {
    let own = with_slot(false, SubscriptionSlot::Own).get_script();
    let steps = blocks(&own);
    assert_eq!(
        steps[0],
        "-- subscription s is created disabled without contacting its publisher \
         (connect = false): replication slot s is not created there and no table is \
         subscribed yet. The slot has to exist on the publisher before ALTER SUBSCRIPTION \
         ... ENABLE, and ALTER SUBSCRIPTION ... REFRESH PUBLICATION then subscribes the tables"
    );
    assert_eq!(
        steps[1],
        "CREATE SUBSCRIPTION s CONNECTION 'host=primary dbname=mydb' PUBLICATION my_pub \
         WITH (connect = false, enabled = false);"
    );

    let other = with_slot(false, named("pre_made")).get_script();
    let steps = blocks(&other);
    assert!(
        steps[0].contains("replication slot pre_made is not created there"),
        "{steps:?}"
    );
    assert_eq!(
        steps[1],
        "CREATE SUBSCRIPTION s CONNECTION 'host=primary dbname=mydb' PUBLICATION my_pub \
         WITH (connect = false, slot_name = 'pre_made', enabled = false);"
    );
}

#[test]
fn subscription_create_enabled_keeps_the_connected_form() {
    // Own slot: byte-identical to pgc 1.0.x, no note.
    assert_eq!(
        with_slot(true, SubscriptionSlot::Own).get_script(),
        "CREATE SUBSCRIPTION s CONNECTION 'host=primary dbname=mydb' PUBLICATION my_pub;\n\n\
         ALTER SUBSCRIPTION s OWNER TO postgres;\n\n"
    );
    assert!(with_slot(true, SubscriptionSlot::Own).connects_on_create(SlotNameRules::default()));

    // Another slot name: named, with a note on an existing slot.
    let steps = blocks(&with_slot(true, named("pre_made")).get_script()).join("\n");
    assert_eq!(
        steps,
        "-- subscription s connects to its publisher and creates replication slot pre_made \
         there: if that slot exists already, add create_slot = false to use it\n\
         CREATE SUBSCRIPTION s CONNECTION 'host=primary dbname=mydb' PUBLICATION my_pub \
         WITH (slot_name = 'pre_made');\n\
         ALTER SUBSCRIPTION s OWNER TO postgres;"
    );
}

#[test]
fn subscription_create_orders_the_slot_options_before_the_pg19_ones() {
    let s = Subscription {
        enabled: false,
        slot_name: SubscriptionSlot::NoSlot,
        retain_dead_tuples: true,
        max_retention_duration: 60000,
        wal_receiver_timeout: Some("30s".into()),
        ..sub_on_server("s", "srv")
    };
    assert_eq!(
        statements(&s.get_script())[0],
        "CREATE SUBSCRIPTION s SERVER srv PUBLICATION my_pub WITH (connect = false, \
         slot_name = NONE, enabled = false, retain_dead_tuples = true, \
         max_retention_duration = 60000, wal_receiver_timeout = '30s');"
    );
}

#[test]
fn subscription_create_note_is_comment_safe() {
    let s = Subscription {
        enabled: false,
        ..sub("\"evil\nDROP TABLE t; --\"")
    };
    let script = s.get_script();
    let note = blocks(&script)[0];
    assert!(
        note.starts_with("-- subscription \"evil DROP TABLE t; --\" is created"),
        "{script}"
    );
    assert!(
        !note.contains('\n'),
        "the note must stay one line: {script}"
    );
}

#[test]
fn subscription_alter_slot_to_none_on_a_disabled_subscription() {
    let from = with_slot(false, SubscriptionSlot::Own);
    let to = with_slot(false, SubscriptionSlot::NoSlot);
    assert_eq!(
        blocks(&from.get_alter_script(&to, true)),
        [
            "-- replication slot s stays on the publisher of subscription s: ALTER SUBSCRIPTION \
             does not drop it. Drop it there once nothing uses it, or it keeps retaining WAL",
            "ALTER SUBSCRIPTION s SET (slot_name = NONE);",
        ]
    );
}

#[test]
fn subscription_alter_slot_to_none_disables_an_enabled_subscription_first() {
    let from = with_slot(true, named("old"));
    let to = with_slot(false, SubscriptionSlot::NoSlot);
    assert_eq!(
        statements(&from.get_alter_script(&to, true))
            .into_iter()
            .filter(|l| !l.starts_with("--"))
            .collect::<Vec<_>>(),
        vec![
            "ALTER SUBSCRIPTION s DISABLE;",
            "ALTER SUBSCRIPTION s SET (slot_name = NONE);",
        ]
    );
    // With a publication change: no refreshing SET PUBLICATION on the
    // disabled subscription, and no refresh later either.
    let to_pubs = Subscription {
        publications: vec!["my_pub".into(), "p2".into()],
        ..to.clone()
    };
    let alter = from.get_alter_script_with(&to_pubs, true, false);
    assert_eq!(
        statements(&alter.script)
            .into_iter()
            .filter(|l| !l.starts_with("--"))
            .collect::<Vec<_>>(),
        vec![
            "ALTER SUBSCRIPTION s DISABLE;",
            "ALTER SUBSCRIPTION s SET PUBLICATION my_pub, p2 WITH (refresh = false);",
            "ALTER SUBSCRIPTION s SET (slot_name = NONE);",
        ]
    );
    assert!(alter.post_commit.is_empty());
}

#[test]
fn subscription_alter_named_slot_precedes_the_enable() {
    let from = with_slot(false, SubscriptionSlot::NoSlot);
    let to = with_slot(true, named("new_slot"));
    assert_eq!(
        blocks(&from.get_alter_script(&to, true)),
        [
            "-- ALTER SUBSCRIPTION does not create replication slot new_slot: it has to exist on \
             the publisher before subscription s is enabled",
            "ALTER SUBSCRIPTION s SET (slot_name = 'new_slot');",
            "ALTER SUBSCRIPTION s ENABLE;",
        ]
    );
}

#[test]
fn subscription_alter_slot_rename_on_an_enabled_subscription_needs_no_disable() {
    let from = with_slot(true, named("old"));
    let to = with_slot(true, SubscriptionSlot::Own);
    let script = from.get_alter_script(&to, true);
    assert_eq!(
        blocks(&script),
        [
            "-- replication slot old stays on the publisher of subscription s: ALTER SUBSCRIPTION \
             does not drop it. Drop it there once nothing uses it, or it keeps retaining WAL\n\
             -- ALTER SUBSCRIPTION does not create replication slot s: it has to exist on the \
             publisher before subscription s is enabled",
            "ALTER SUBSCRIPTION s SET (slot_name = 's');",
        ]
    );
    // Rides in the same SET as the other options.
    let to_opts = Subscription {
        max_retention_duration: 5000,
        ..with_slot(true, named("slot_2"))
    };
    assert!(from.get_alter_script(&to_opts, true).contains(
        "ALTER SUBSCRIPTION s SET (max_retention_duration = 5000, slot_name = 'slot_2');"
    ));
}

#[test]
fn subscription_alter_without_a_slot_change_is_unchanged() {
    for slot in [SubscriptionSlot::Own, SubscriptionSlot::NoSlot, named("x")] {
        let from = with_slot(false, slot.clone());
        let to = Subscription {
            wal_receiver_timeout: Some("1min".into()),
            ..from.clone()
        };
        assert_eq!(
            statements(&from.get_alter_script(&to, true)),
            vec!["ALTER SUBSCRIPTION s SET (wal_receiver_timeout = '1min');"],
            "{slot:?}"
        );
    }
}

#[test]
fn subscription_serde_writes_the_slot_only_when_not_its_own() {
    let own = serde_json::to_value(sub("s")).unwrap();
    assert!(own.get("slot_name").is_none(), "{own}");

    let none = serde_json::to_value(with_slot(false, SubscriptionSlot::NoSlot)).unwrap();
    assert_eq!(none["slot_name"], "no_slot");
    let other = serde_json::to_value(with_slot(true, named("x"))).unwrap();
    assert_eq!(other["slot_name"], serde_json::json!({ "named": "x" }));

    for value in [none, other] {
        let back: Subscription = serde_json::from_value(value.clone()).unwrap();
        assert_eq!(serde_json::to_value(&back).unwrap(), value);
    }
}

/// Every FROM -> TO slot / enabled / retain_dead_tuples / publication
/// combination PostgreSQL can hold (no enabled subscription without a
/// slot), in and out of a single transaction, including slots whose names
/// PostgreSQL refuses as a `slot_name` (`Bad`, and `none`, which it reads as
/// `NONE`). Replaying the statements:
///
/// - never sets `slot_name = NONE` on an enabled subscription;
/// - never enables a subscription without a slot, or refreshes one while it
///   is disabled;
/// - never writes a slot name PostgreSQL refuses;
/// - ends on TO's slot, or on FROM's when PostgreSQL refuses TO's name;
/// - ends in TO's enabled state, unless that leaves it without a slot.
#[test]
fn subscription_alter_slot_changes_replay_to_the_target_state() {
    let rules = SlotNameRules::default();
    let slots = [
        SubscriptionSlot::Own,
        SubscriptionSlot::NoSlot,
        named("x"),
        named("Bad"),
        named("none"),
    ];
    let mut checked = 0;
    for from_enabled in [false, true] {
        for from_slot in &slots {
            for to_enabled in [false, true] {
                for to_slot in &slots {
                    if (from_enabled && *from_slot == SubscriptionSlot::NoSlot)
                        || (to_enabled && *to_slot == SubscriptionSlot::NoSlot)
                    {
                        continue;
                    }
                    for (from_rdt, to_rdt) in [(false, false), (true, false), (false, true)] {
                        for publications in [false, true] {
                            for single_transaction in [false, true] {
                                let from = Subscription {
                                    retain_dead_tuples: from_rdt,
                                    ..with_slot(from_enabled, from_slot.clone())
                                };
                                let to = Subscription {
                                    retain_dead_tuples: to_rdt,
                                    publications: if publications {
                                        vec!["my_pub".into(), "p2".into()]
                                    } else {
                                        vec!["my_pub".into()]
                                    },
                                    ..with_slot(to_enabled, to_slot.clone())
                                };
                                let alter =
                                    from.get_alter_script_with(&to, true, single_transaction);
                                let case = format!(
                                    "{from_enabled} {from_slot:?} -> {to_enabled} {to_slot:?}, \
                                     rdt {from_rdt}->{to_rdt}, publications {publications}, \
                                     single transaction {single_transaction}:\n{}",
                                    alter.script
                                );
                                let reachable = to.slot_settable(rules);
                                let end_slot = if reachable { to.slot() } else { from.slot() };
                                let mut enabled = from_enabled;
                                let mut slot = from.slot();
                                let mut slot_sets = 0;
                                for step in blocks(&alter.script)
                                    .into_iter()
                                    .chain(blocks(&alter.post_commit))
                                    .filter(|b| !b.starts_with("--") && !b.starts_with("DO "))
                                {
                                    if step.contains("slot_name = NONE") {
                                        assert!(!enabled, "NONE while enabled: {case}");
                                        slot = None;
                                        slot_sets += 1;
                                    } else if let Some(at) = step.find("slot_name = '") {
                                        let value = &step[at + "slot_name = '".len()..];
                                        let value = &value[..value.find('\'').unwrap()];
                                        assert!(rules.accepts(value), "refused name: {case}");
                                        slot = Some(value.to_string());
                                        slot_sets += 1;
                                    }
                                    if step.ends_with(" ENABLE;") {
                                        assert!(slot.is_some(), "ENABLE without a slot: {case}");
                                        enabled = true;
                                    } else if step.ends_with(" DISABLE;") {
                                        enabled = false;
                                    } else if step.ends_with(" REFRESH PUBLICATION;") {
                                        assert!(enabled, "REFRESH while disabled: {case}");
                                    }
                                }
                                assert_eq!(slot, end_slot, "{case}");
                                assert_eq!(enabled, to_enabled && end_slot.is_some(), "{case}");
                                assert_eq!(
                                    slot_sets,
                                    usize::from(from_slot != to_slot && reachable),
                                    "{case}"
                                );
                                assert_eq!(
                                    alter
                                        .script
                                        .contains("is left out: PostgreSQL does not accept"),
                                    from_slot != to_slot && !reachable,
                                    "{case}"
                                );
                                checked += 1;
                            }
                        }
                    }
                }
            }
        }
    }
    // Five slots disabled, four enabled (none without a slot), on each side.
    assert_eq!(checked, 9 * 9 * 3 * 2 * 2);
}

// ---- slot names PostgreSQL refuses as slot_name (r4-sub review) ----

#[test]
fn slot_name_rules_accept_what_postgresql_accepts() {
    // Verified live on PostgreSQL 14, 18 and 19 against
    // ALTER SUBSCRIPTION ... SET (slot_name = '...').
    let pg18 = SlotNameRules::default();
    let pg19 = SlotNameRules {
        conflict_slot_reserved: true,
    };
    let max = "a".repeat(63);
    for accepted in ["s", "rv_r4s_named_slot", "pg_x", "_", "0abc", max.as_str()] {
        assert!(pg18.accepts(accepted), "{accepted}");
        assert!(pg19.accepts(accepted), "{accepted}");
    }
    let too_long = "a".repeat(64);
    for refused in [
        "",
        too_long.as_str(),
        "My Sub",
        "x\"y",
        "it's",
        "Abc",
        "NONE",
        "a-b",
        "caf\u{e9}",
        // PostgreSQL reads 'none' as slot_name = NONE.
        "none",
    ] {
        assert!(!pg18.accepts(refused), "{refused:?}");
        assert!(!pg19.accepts(refused), "{refused:?}");
    }
    // A valid slot name, which a publisher creates, but slot_name = 'none'
    // means NONE.
    assert!(pg18.is_valid("none") && pg19.is_valid("none"));
    for invalid in ["", too_long.as_str(), "My Sub", "x\"y", "NONE"] {
        assert!(!pg18.is_valid(invalid), "{invalid:?}");
    }
    assert!(pg18.is_valid("pg_conflict_detection"));
    assert!(!pg19.is_valid("pg_conflict_detection"));
    // Reserved from PostgreSQL 19 on only.
    assert!(pg18.accepts("pg_conflict_detection"));
    assert!(!pg19.accepts("pg_conflict_detection"));
    assert!(pg19.accepts("pg_conflict_detection_2"));
    assert_eq!(
        SlotNameRules::CONFLICT_DETECTION_SLOT,
        "pg_conflict_detection"
    );
    assert_eq!(SlotNameRules::CONFLICT_SLOT_RESERVED_MIN_VERSION, 190000);
}

#[test]
fn subscription_slot_settable_and_out_of_reach() {
    let rules = SlotNameRules::default();
    assert!(with_slot(false, SubscriptionSlot::NoSlot).slot_settable(rules));
    assert!(with_slot(false, SubscriptionSlot::Own).slot_settable(rules));
    assert!(with_slot(false, named("x")).slot_settable(rules));
    assert!(!with_slot(false, named("Bad")).slot_settable(rules));
    assert!(!with_slot(false, named("none")).slot_settable(rules));
    let own_refused = Subscription {
        slot_name: SubscriptionSlot::Own,
        ..sub("\"My Sub\"")
    };
    assert!(!own_refused.slot_settable(rules));

    // Created: only a slot not named after the subscription is out of reach;
    // CREATE SUBSCRIPTION records its own name unchecked.
    assert!(!own_refused.slot_out_of_reach(None, rules));
    assert!(with_slot(false, named("Bad")).slot_out_of_reach(None, rules));
    assert!(!with_slot(false, named("x")).slot_out_of_reach(None, rules));
    // Altered: only when the slot changes.
    let from = Subscription {
        slot_name: named("abc"),
        ..sub("\"My Sub\"")
    };
    assert!(own_refused.slot_out_of_reach(Some(&from), rules));
    assert!(!own_refused.slot_out_of_reach(Some(&own_refused), rules));
    assert!(!from.slot_out_of_reach(Some(&own_refused), rules));
}

#[test]
fn subscription_create_on_its_own_refused_slot_is_not_connected() {
    // CREATE SUBSCRIPTION "My Sub" connected asks the publisher for slot
    // "My Sub", which it refuses ("contains invalid character"): even an
    // enabled one is created connect = false, on that slot, with a note.
    for enabled in [false, true] {
        let s = Subscription {
            enabled,
            ..sub("\"My Sub\"")
        };
        assert!(!s.connects_on_create(SlotNameRules::default()));
        assert_eq!(
            blocks(&s.get_script()),
            [
                "-- subscription \"My Sub\" is created disabled without contacting its publisher \
                 (connect = false): PostgreSQL does not accept its replication slot My Sub as a \
                 slot name (a slot name has 1 to 63 lower-case letters, digits and underscores, \
                 and is neither none nor, on PostgreSQL 19+, pg_conflict_detection), so it needs \
                 another slot_name before ALTER SUBSCRIPTION ... ENABLE",
                "CREATE SUBSCRIPTION \"My Sub\" CONNECTION 'host=primary dbname=mydb' \
                 PUBLICATION my_pub WITH (connect = false, enabled = false);",
                "ALTER SUBSCRIPTION \"My Sub\" OWNER TO postgres;",
            ],
            "enabled {enabled}"
        );
    }
}

#[test]
fn subscription_create_on_its_own_slot_named_none_is_unchanged() {
    // A subscription named none records slot none. CREATE SUBSCRIPTION
    // writes no slot_name for it, and a publisher creates a slot of that
    // name, so it is created as any other subscription on its own slot.
    let enabled = Subscription {
        enabled: true,
        ..sub("\"none\"")
    };
    assert!(enabled.connects_on_create(SlotNameRules::default()));
    assert_eq!(
        enabled.get_script(),
        "CREATE SUBSCRIPTION \"none\" CONNECTION 'host=primary dbname=mydb' PUBLICATION \
         my_pub;\n\nALTER SUBSCRIPTION \"none\" OWNER TO postgres;\n\n"
    );
    let disabled = Subscription {
        enabled: false,
        ..sub("\"none\"")
    };
    let script = disabled.get_script();
    assert!(
        script.starts_with(
            "-- subscription \"none\" is created disabled without contacting its publisher \
             (connect = false): replication slot none is not created there"
        ),
        "{script}"
    );
    assert!(!disabled.slot_out_of_reach(None, SlotNameRules::default()));
}

#[test]
fn subscription_create_leaves_out_a_named_slot_postgresql_refuses() {
    for (enabled, slot) in [
        (false, "Bad"),
        (true, "Bad"),
        (false, "none"),
        (true, "x\"y"),
    ] {
        let s = with_slot(enabled, named(slot));
        assert!(!s.connects_on_create(SlotNameRules::default()), "{slot}");
        let script = s.get_script();
        let steps = blocks(&script);
        assert_eq!(
            steps[0],
            format!(
                "-- replication slot {slot} of subscription s is left out: PostgreSQL does not \
                 accept it as a slot_name (a slot name has 1 to 63 lower-case letters, digits \
                 and underscores, and is neither none nor, on PostgreSQL 19+, \
                 pg_conflict_detection). The subscription is created disabled without \
                 contacting its publisher (connect = false), on the slot named after it, and \
                 needs a slot PostgreSQL accepts before ALTER SUBSCRIPTION ... ENABLE"
            ),
            "{script}"
        );
        assert_eq!(
            steps[1],
            "CREATE SUBSCRIPTION s CONNECTION 'host=primary dbname=mydb' PUBLICATION my_pub \
             WITH (connect = false, enabled = false);",
            "{script}"
        );
        assert!(!script.contains("slot_name = '"), "{script}");
    }
}

#[test]
fn subscription_create_reserves_the_conflict_detection_slot_on_pg19_only() {
    let s = with_slot(false, named("pg_conflict_detection"));
    assert!(s.get_script().contains(
        " WITH (connect = false, slot_name = 'pg_conflict_detection', enabled = false);"
    ));
    let pg19 = s.get_script_for(SlotNameRules {
        conflict_slot_reserved: true,
    });
    assert!(
        pg19.contains(" PUBLICATION my_pub WITH (connect = false, enabled = false);"),
        "{pg19}"
    );
    assert!(
        pg19.starts_with("-- replication slot pg_conflict_detection of subscription s is left out"),
        "{pg19}"
    );
    // Enabled on PostgreSQL 18: created connected on that slot, as before.
    let enabled = with_slot(true, named("pg_conflict_detection"));
    assert!(enabled.connects_on_create(SlotNameRules::default()));
    assert!(!enabled.connects_on_create(SlotNameRules {
        conflict_slot_reserved: true,
    }));
}

#[test]
fn subscription_alter_leaves_out_a_slot_postgresql_refuses() {
    // The review's reproduction: FROM "x""y" on slot abc, TO on its own slot
    // x"y, which PostgreSQL refuses ("contains invalid character").
    let from = Subscription {
        enabled: false,
        slot_name: named("abc"),
        ..sub("\"x\"\"y\"")
    };
    let to = Subscription {
        enabled: false,
        slot_name: SubscriptionSlot::Own,
        ..sub("\"x\"\"y\"")
    };
    assert_eq!(
        blocks(&from.get_alter_script(&to, true)),
        [
            "-- replication slot x\"y of subscription \"x\"\"y\" is left out: PostgreSQL does not \
          accept it as a slot_name (a slot name has 1 to 63 lower-case letters, digits and \
          underscores, and is neither none nor, on PostgreSQL 19+, pg_conflict_detection). The \
          subscription stays on replication slot abc"
        ]
    );
    // Other options still change, without the slot.
    let to_timeout = Subscription {
        wal_receiver_timeout: Some("1min".into()),
        ..to.clone()
    };
    assert_eq!(
        statements(&from.get_alter_script(&to_timeout, true))
            .into_iter()
            .filter(|l| !l.starts_with("--"))
            .collect::<Vec<_>>(),
        vec!["ALTER SUBSCRIPTION \"x\"\"y\" SET (wal_receiver_timeout = '1min');"]
    );

    // 'none' would set slot_name = NONE.
    let from = with_slot(false, SubscriptionSlot::NoSlot);
    let to = with_slot(false, named("none"));
    let script = from.get_alter_script(&to, true);
    assert!(!script.contains("SET ("), "{script}");
    assert!(
        script.ends_with("The subscription stays on no replication slot (slot_name = NONE)\n\n"),
        "{script}"
    );
}

#[test]
fn subscription_alter_to_a_refused_slot_keeps_a_slotless_subscription_disabled() {
    let from = with_slot(false, SubscriptionSlot::NoSlot);
    let to = Subscription {
        publications: vec!["my_pub".into(), "p2".into()],
        ..with_slot(true, named("Bad"))
    };
    for single_transaction in [false, true] {
        let alter = from.get_alter_script_with(&to, true, single_transaction);
        let steps = blocks(&alter.script);
        assert!(
            steps[0].ends_with(
                "The subscription stays on no replication slot (slot_name = NONE), and stays \
                 disabled: PostgreSQL cannot enable a subscription without a slot"
            ),
            "{steps:?}"
        );
        assert_eq!(
            steps[1..],
            ["ALTER SUBSCRIPTION s SET PUBLICATION my_pub, p2 WITH (refresh = false);"],
            "no ENABLE and no REFRESH without a slot"
        );
        assert!(alter.post_commit.is_empty());
    }
    // With a slot to keep, the subscription is enabled on it.
    let from = with_slot(false, named("abc"));
    let to = with_slot(true, named("Bad"));
    assert_eq!(
        statements(&from.get_alter_script(&to, true))
            .into_iter()
            .filter(|l| !l.starts_with("--"))
            .collect::<Vec<_>>(),
        vec!["ALTER SUBSCRIPTION s ENABLE;"]
    );
}

#[test]
fn subscription_alter_reserves_the_conflict_detection_slot_on_pg19_only() {
    let from = with_slot(false, named("abc"));
    let to = with_slot(false, named("pg_conflict_detection"));
    assert!(
        from.get_alter_script(&to, true)
            .contains("ALTER SUBSCRIPTION s SET (slot_name = 'pg_conflict_detection');")
    );
    let pg19 = from
        .get_alter_script_for(
            &to,
            true,
            false,
            SlotNameRules {
                conflict_slot_reserved: true,
            },
        )
        .script;
    assert!(!pg19.contains("SET ("), "{pg19}");
    assert!(
        pg19.starts_with("-- replication slot pg_conflict_detection of subscription s is left out"),
        "{pg19}"
    );
}

#[test]
fn subscription_refused_slot_notes_are_comment_safe() {
    let from = Subscription {
        enabled: false,
        slot_name: named("abc"),
        ..sub("\"evil\nDROP TABLE t; --\"")
    };
    let to = Subscription {
        slot_name: SubscriptionSlot::Own,
        ..from.clone()
    };
    for script in [from.get_alter_script(&to, true), to.get_script()] {
        let note = blocks(&script)[0];
        assert!(note.starts_with("-- "), "{script}");
        assert!(
            !note.contains('\n'),
            "the note must stay one line: {script}"
        );
    }
}
