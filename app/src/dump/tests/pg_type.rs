use super::*;

fn base_pg_type(typtype: char) -> PgType {
    PgType {
        oid: Oid(1),
        schema: "public".to_string(),
        typname: "my_type".to_string(),
        typnamespace: Oid(2200),
        typowner: Oid(10),
        owner: String::new(),
        typlen: -1,
        typbyval: false,
        typtype: typtype as i8,
        typcategory: 'U' as i8,
        typispreferred: false,
        typisdefined: true,
        typdelim: ',' as i8,
        typrelid: None,
        typsubscript: None,
        typelem: None,
        typarray: None,
        typinput: "record_in".to_string(),
        typoutput: "record_out".to_string(),
        typreceive: None,
        typsend: None,
        typmodin: None,
        typmodout: None,
        typanalyze: None,
        typalign: 'd' as i8,
        typstorage: 'p' as i8,
        typnotnull: false,
        typbasetype: None,
        typtypmod: None,
        typndims: 0,
        typcollation: None,
        typdefault: None,
        formatted_basetype: None,
        enum_labels: Vec::new(),
        domain_constraints: Vec::new(),
        composite_attributes: Vec::new(),
        range_subtype: None,
        range_collation: None,
        range_opclass: None,
        range_canonical: None,
        range_subdiff: None,
        multirange_name: None,
        domain_collation_name: None,
        comment: None,
        acl: Vec::new(),
        hash: None,
    }
}

#[test]
fn hash_populates_hash_field() {
    let mut pg_type = base_pg_type('e');
    pg_type.enum_labels = vec!["alpha".to_string(), "beta".to_string()];

    pg_type.hash();
    let first = pg_type.hash.clone();

    let value = first.as_ref().expect("hash should be present");
    assert_eq!(value.len(), 64);
    assert!(value.chars().all(|c| c.is_ascii_hexdigit()));

    pg_type.hash();
    assert_eq!(pg_type.hash, first);
}

#[test]
fn hash_differs_when_fields_change() {
    let mut left = base_pg_type('e');
    left.enum_labels = vec!["alpha".to_string()];
    left.hash();
    let left_hash = left.hash.clone().unwrap();

    let mut right = base_pg_type('e');
    right.enum_labels = vec!["alpha".to_string()];
    right.typname = "different".to_string();
    right.hash();
    let right_hash = right.hash.clone().unwrap();

    assert_ne!(left_hash, right_hash);
}

#[test]
fn enum_get_script_generates_create_statement() {
    let mut pg_type = base_pg_type('e');
    pg_type.typname = "status".to_string();
    pg_type.enum_labels = vec!["simple".to_string(), "O'Reilly".to_string()];

    let script = pg_type.get_script();

    assert_eq!(
        script,
        "create type public.status as enum ('simple', 'O''Reilly');\n\n"
    );
}

#[test]
fn enum_get_script_handles_missing_labels() {
    let pg_type = base_pg_type('e');

    let script = pg_type.get_script();

    assert_eq!(
        script,
        "-- Enum public.my_type has no labels available in dump\n"
    );
}

#[test]
fn domain_get_script_includes_constraints() {
    let mut pg_type = base_pg_type('d');
    pg_type.typname = "amount".to_string();
    pg_type.formatted_basetype = Some("integer".to_string());
    pg_type.typdefault = Some("42".to_string());
    pg_type.typnotnull = true;
    pg_type.domain_constraints = vec![DomainConstraint {
        name: "ValueCheck".to_string(),
        definition: "check (value > 0)".to_string(),
    }];

    let script = pg_type.get_script();

    let expected = "create domain public.amount as integer default 42 not null;\n\n\
alter domain public.amount add constraint \"ValueCheck\" check (value > 0);\n\n";
    assert_eq!(script, expected);
}

#[test]
fn get_alter_script_enum_adds_missing_labels() {
    let mut current = base_pg_type('e');
    current.typname = "status".to_string();
    current.enum_labels = vec!["pending".to_string(), "completed".to_string()];

    let mut target = base_pg_type('e');
    target.typname = "status".to_string();
    target.enum_labels = vec![
        "pending".to_string(),
        "in_progress".to_string(),
        "completed".to_string(),
    ];

    let script = current.get_alter_script(&target, true);

    assert_eq!(
        script,
        "alter type public.status add value if not exists 'in_progress' before 'completed';\n\n"
    );
}

#[test]
fn get_alter_script_enum_requires_no_changes() {
    let mut current = base_pg_type('e');
    current.enum_labels = vec!["pending".to_string(), "completed".to_string()];
    let target = current.clone();

    let script = current.get_alter_script(&target, true);

    assert_eq!(script, "-- Enum public.my_type requires no changes.\n");
}

#[test]
fn get_alter_script_domain_handles_changes() {
    let mut current = base_pg_type('d');
    current.typname = "amount".to_string();
    current.formatted_basetype = Some("integer".to_string());
    current.typdefault = Some("42".to_string());
    current.typnotnull = true;
    current.domain_constraints = vec![DomainConstraint {
        name: "ValueCheck".to_string(),
        definition: "check (value > 0)".to_string(),
    }];

    let mut target = current.clone();
    target.typdefault = Some("84".to_string());
    target.typnotnull = false;
    target.domain_constraints = vec![
        DomainConstraint {
            name: "ValueCheck".to_string(),
            definition: "check (value >= 0)".to_string(),
        },
        DomainConstraint {
            name: "FreshConstraint".to_string(),
            definition: "check (value <> 0)".to_string(),
        },
    ];

    let script = current.get_alter_script(&target, true);

    let expected = "alter domain public.amount set default 84;\n\n\
alter domain public.amount drop not null;\n\n\
alter domain public.amount drop constraint \"ValueCheck\";\n\n\
alter domain public.amount add constraint \"ValueCheck\" check (value >= 0);\n\n\
alter domain public.amount add constraint \"FreshConstraint\" check (value <> 0);\n\n";

    assert_eq!(script, expected);
}

#[test]
fn composite_get_script_generates_create_statement() {
    let mut pg_type = base_pg_type('c');
    pg_type.typname = "address_type".to_string();
    pg_type.composite_attributes = vec![
        CompositeAttribute {
            name: "street".to_string(),
            data_type: "varchar(255)".to_string(),
        },
        CompositeAttribute {
            name: "city".to_string(),
            data_type: "varchar(100)".to_string(),
        },
    ];

    let script = pg_type.get_script();

    let expected = "create type public.address_type as (\n    \"street\" varchar(255),\n    \"city\" varchar(100)\n);\n\n";
    assert_eq!(script, expected);
}

#[test]
fn get_drop_script_returns_drop_statement() {
    let pg_type = base_pg_type('e');

    assert_eq!(
        pg_type.get_drop_script(),
        "drop type if exists public.my_type cascade;\n\n"
    );
}

#[test]
fn get_alter_script_includes_owner_change() {
    let mut current = base_pg_type('e');
    current.typname = "status".to_string();
    current.enum_labels = vec!["pending".to_string()];
    current.owner = "old_owner".to_string();

    let mut target = current.clone();
    target.owner = "new_owner".to_string();

    let script = current.get_alter_script(&target, true);

    assert!(script.contains("alter type public.status owner to new_owner;"));
}

#[test]
fn get_alter_script_domain_drop_default_use_drop_false() {
    let mut current = base_pg_type('d');
    current.typname = "amount".to_string();
    current.formatted_basetype = Some("integer".to_string());
    current.typdefault = Some("42".to_string());

    let mut target = current.clone();
    target.typdefault = None;

    let script = current.get_alter_script(&target, false);

    assert!(script.contains("drop default"));
    // The drop default line should be commented out
    for line in script.lines() {
        if line.contains("drop default") {
            assert!(
                line.starts_with("--"),
                "drop default should be commented: {}",
                line
            );
        }
    }
}

#[test]
fn get_alter_script_domain_drop_default_use_drop_true() {
    let mut current = base_pg_type('d');
    current.typname = "amount".to_string();
    current.formatted_basetype = Some("integer".to_string());
    current.typdefault = Some("42".to_string());

    let mut target = current.clone();
    target.typdefault = None;

    let script = current.get_alter_script(&target, true);

    assert!(script.contains("alter domain public.amount drop default;"));
    // Should NOT be commented out
    for line in script.lines() {
        if line.contains("drop default") {
            assert!(
                !line.starts_with("--"),
                "drop default should be active: {}",
                line
            );
        }
    }
}

#[test]
fn get_alter_script_domain_drop_not_null_use_drop_false() {
    let mut current = base_pg_type('d');
    current.typname = "amount".to_string();
    current.formatted_basetype = Some("integer".to_string());
    current.typnotnull = true;

    let mut target = current.clone();
    target.typnotnull = false;

    let script = current.get_alter_script(&target, false);

    assert!(script.contains("drop not null"));
    for line in script.lines() {
        if line.contains("drop not null") {
            assert!(
                line.starts_with("--"),
                "drop not null should be commented: {}",
                line
            );
        }
    }
}

#[test]
fn get_alter_script_domain_drop_not_null_use_drop_true() {
    let mut current = base_pg_type('d');
    current.typname = "amount".to_string();
    current.formatted_basetype = Some("integer".to_string());
    current.typnotnull = true;

    let mut target = current.clone();
    target.typnotnull = false;

    let script = current.get_alter_script(&target, true);

    assert!(script.contains("alter domain public.amount drop not null;"));
    for line in script.lines() {
        if line.contains("drop not null") {
            assert!(
                !line.starts_with("--"),
                "drop not null should be active: {}",
                line
            );
        }
    }
}

#[test]
fn get_alter_script_domain_drop_constraint_use_drop_false() {
    let mut current = base_pg_type('d');
    current.typname = "amount".to_string();
    current.formatted_basetype = Some("integer".to_string());
    current.domain_constraints = vec![DomainConstraint {
        name: "ValueCheck".to_string(),
        definition: "check (value > 0)".to_string(),
    }];

    let mut target = current.clone();
    target.domain_constraints = vec![DomainConstraint {
        name: "ValueCheck".to_string(),
        definition: "check (value >= 0)".to_string(),
    }];

    let script = current.get_alter_script(&target, false);

    // Should contain a warning about manual intervention
    assert!(
        script.contains("use_drop=false") && script.contains("manual intervention needed"),
        "should contain a warning comment, script:\n{}",
        script
    );

    // Both drop and add constraint should be commented out
    for line in script.lines() {
        if line.contains("drop constraint") || line.contains("add constraint") {
            assert!(line.starts_with("--"), "should be commented: {}", line);
        }
    }
}

#[test]
fn get_alter_script_domain_remove_constraint_use_drop_false() {
    let mut current = base_pg_type('d');
    current.typname = "amount".to_string();
    current.formatted_basetype = Some("integer".to_string());
    current.domain_constraints = vec![DomainConstraint {
        name: "OldCheck".to_string(),
        definition: "check (value > 0)".to_string(),
    }];

    let mut target = current.clone();
    target.domain_constraints = vec![];

    let script = current.get_alter_script(&target, false);

    assert!(script.contains("drop constraint"));
    for line in script.lines() {
        if line.contains("drop constraint") {
            assert!(
                line.starts_with("--"),
                "drop constraint should be commented: {}",
                line
            );
        }
    }
}

#[test]
fn get_alter_script_domain_all_drops_use_drop_false() {
    let mut current = base_pg_type('d');
    current.typname = "amount".to_string();
    current.formatted_basetype = Some("integer".to_string());
    current.typdefault = Some("42".to_string());
    current.typnotnull = true;
    current.domain_constraints = vec![DomainConstraint {
        name: "ValueCheck".to_string(),
        definition: "check (value > 0)".to_string(),
    }];

    let mut target = current.clone();
    target.typdefault = Some("84".to_string());
    target.typnotnull = false;
    target.domain_constraints = vec![
        DomainConstraint {
            name: "ValueCheck".to_string(),
            definition: "check (value >= 0)".to_string(),
        },
        DomainConstraint {
            name: "FreshConstraint".to_string(),
            definition: "check (value <> 0)".to_string(),
        },
    ];

    let script = current.get_alter_script(&target, false);

    // set default should still be active (not a drop)
    assert!(script.contains("set default 84"));
    // drop not null should be commented
    for line in script.lines() {
        if line.contains("drop not null") {
            assert!(
                line.starts_with("--"),
                "drop not null should be commented: {}",
                line
            );
        }
    }
    // drop constraint should be commented
    for line in script.lines() {
        if line.contains("drop constraint") {
            assert!(
                line.starts_with("--"),
                "drop constraint should be commented: {}",
                line
            );
        }
    }
    // add constraint for changed constraint should also be commented (depends on drop)
    // but add constraint for new constraint should be active
    assert!(
        script.contains("-- use_drop=false: constraint \"ValueCheck\""),
        "should warn about ValueCheck requiring manual intervention"
    );
    // FreshConstraint is brand new, its add should be active
    for line in script.lines() {
        if line.contains("add constraint") && line.contains("FreshConstraint") {
            assert!(
                !line.starts_with("--"),
                "new constraint add should be active: {}",
                line
            );
        }
    }
}
