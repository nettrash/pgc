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
        multirange_schema: None,
        domain_collation_name: None,
        comment: None,
        acl: Vec::new(),
        acl_explicitly_empty: false,
        typed_tables: None,
        composite_stored_in: None,
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
            collation: None,
        },
        CompositeAttribute {
            name: "city".to_string(),
            data_type: "varchar(100)".to_string(),
            collation: None,
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

// ---- Domain constraints: hash coverage and ALTER DOMAIN emission ----

fn domain_with(constraints: &[(&str, &str)]) -> PgType {
    let mut pg_type = base_pg_type('d');
    pg_type.typname = "amount".to_string();
    pg_type.formatted_basetype = Some("integer".to_string());
    pg_type.domain_constraints = constraints
        .iter()
        .map(|(name, definition)| DomainConstraint {
            name: name.to_string(),
            definition: definition.to_string(),
        })
        .collect();
    pg_type
}

fn hashed(mut pg_type: PgType) -> String {
    pg_type.hash();
    pg_type.hash.expect("hash is set")
}

#[test]
fn hash_covers_domain_constraint_body_addition_and_removal() {
    let base = hashed(domain_with(&[("amount_check", "CHECK ((VALUE > 0))")]));

    let body_changed = hashed(domain_with(&[("amount_check", "CHECK ((VALUE > 5))")]));
    let renamed = hashed(domain_with(&[("amount_chk", "CHECK ((VALUE > 0))")]));
    let added = hashed(domain_with(&[
        ("amount_check", "CHECK ((VALUE > 0))"),
        ("amount_max", "CHECK ((VALUE < 100))"),
    ]));
    let removed = hashed(domain_with(&[]));
    let not_valid = hashed(domain_with(&[(
        "amount_check",
        "CHECK ((VALUE > 0)) NOT VALID",
    )]));

    for (what, other) in [
        ("body change", &body_changed),
        ("rename", &renamed),
        ("added constraint", &added),
        ("removed constraint", &removed),
        ("NOT VALID", &not_valid),
    ] {
        assert_ne!(&base, other, "{what} must change the hash");
    }
}

#[test]
fn hash_ignores_domain_constraint_storage_order() {
    let sorted = hashed(domain_with(&[
        ("Z_chk", "CHECK ((VALUE > 1))"),
        ("a_chk", "CHECK ((VALUE < 100))"),
    ]));
    let reversed = hashed(domain_with(&[
        ("a_chk", "CHECK ((VALUE < 100))"),
        ("Z_chk", "CHECK ((VALUE > 1))"),
    ]));

    assert_eq!(sorted, reversed);
}

/// Pins the hash of a constrained domain stored in name order — the order
/// every pgc dump has stored them in — so ordering the hash input by name
/// provably changes no hash any dump already carries.
#[test]
fn hash_of_name_ordered_domain_constraints_is_pinned() {
    let pg_type = domain_with(&[
        ("Z_chk", "CHECK ((VALUE > 1))"),
        ("a_chk", "CHECK ((VALUE < 100))"),
    ]);

    assert_eq!(
        hashed(pg_type),
        "d3c08e96f4a22244a0993a0d3155a410921562ef837a593e070d9b99164c74f8"
    );
}

#[test]
fn hash_keeps_enum_label_order_significant() {
    let mut forward = base_pg_type('e');
    forward.enum_labels = vec!["low".to_string(), "high".to_string()];
    let mut backward = base_pg_type('e');
    backward.enum_labels = vec!["high".to_string(), "low".to_string()];
    let mut extended = base_pg_type('e');
    extended.enum_labels = vec!["low".to_string(), "high".to_string(), "max".to_string()];

    let forward = hashed(forward);
    assert_ne!(forward, hashed(backward));
    assert_ne!(forward, hashed(extended));
}

#[test]
fn hash_of_enum_labels_is_pinned() {
    let mut pg_type = base_pg_type('e');
    pg_type.enum_labels = vec!["low".to_string(), "high".to_string()];

    assert_eq!(
        hashed(pg_type),
        "56465ee4854d3a678c3eef2d1502331f320f6b80ae8cb473e54ad5eb4af29737"
    );
}

#[test]
fn get_alter_script_domain_constraint_only_change_is_drop_and_add() {
    let current = domain_with(&[("amount_check", "CHECK ((VALUE > 0))")]);
    let target = domain_with(&[("amount_check", "CHECK ((VALUE > 5))")]);

    assert_eq!(
        current.get_alter_script(&target, true),
        "alter domain public.amount drop constraint \"amount_check\";\n\n\
alter domain public.amount add constraint \"amount_check\" CHECK ((VALUE > 5));\n\n"
    );
}

#[test]
fn get_alter_script_domain_constraint_added_and_removed() {
    let current = domain_with(&[("old_chk", "CHECK ((VALUE <> 7))")]);
    let target = domain_with(&[("D add \"q\"", "CHECK ((VALUE > 0))")]);

    assert_eq!(
        current.get_alter_script(&target, true),
        "alter domain public.amount drop constraint \"old_chk\";\n\n\
alter domain public.amount add constraint \"D add \"\"q\"\"\" CHECK ((VALUE > 0));\n\n"
    );
    // The add needs no drop, so it stays live with use_drop off.
    assert_eq!(
        current.get_alter_script(&target, false),
        "-- alter domain public.amount drop constraint \"old_chk\";\n\n\
alter domain public.amount add constraint \"D add \"\"q\"\"\" CHECK ((VALUE > 0));\n\n"
    );
}

#[test]
fn get_alter_script_domain_unchanged_constraints_emit_nothing_for_them() {
    let current = domain_with(&[("amount_check", "CHECK ((VALUE > 0))")]);
    let mut target = current.clone();
    target.comment = Some("amounts".to_string());

    let script = current.get_alter_script(&target, true);

    assert!(!script.contains("constraint"), "{script}");
    assert!(script.contains("comment on type public.amount is 'amounts';"));
}

#[test]
fn get_alter_script_domain_not_valid_to_valid_validates_in_place() {
    let current = domain_with(&[("amount_check", "CHECK ((VALUE >= 0)) NOT VALID")]);
    let target = domain_with(&[("amount_check", "CHECK ((VALUE >= 0))")]);

    // Nothing is dropped, so use_drop does not matter.
    for use_drop in [true, false] {
        assert_eq!(
            current.get_alter_script(&target, use_drop),
            "alter domain public.amount validate constraint \"amount_check\";\n\n"
        );
    }
}

#[test]
fn get_alter_script_domain_valid_to_not_valid_is_drop_and_add_not_valid() {
    let current = domain_with(&[("amount_check", "CHECK ((VALUE >= 0))")]);
    let target = domain_with(&[("amount_check", "CHECK ((VALUE >= 0)) NOT VALID")]);

    assert_eq!(
        current.get_alter_script(&target, true),
        "alter domain public.amount drop constraint \"amount_check\";\n\n\
alter domain public.amount add constraint \"amount_check\" CHECK ((VALUE >= 0)) NOT VALID;\n\n"
    );
}

#[test]
fn get_alter_script_domain_not_valid_with_new_body_is_drop_and_add() {
    let current = domain_with(&[("amount_check", "CHECK ((VALUE >= 0)) NOT VALID")]);
    let target = domain_with(&[("amount_check", "CHECK ((VALUE > 0))")]);

    let script = current.get_alter_script(&target, true);

    assert!(!script.contains("validate"), "{script}");
    assert!(script.contains("drop constraint \"amount_check\";"));
    assert!(script.contains("add constraint \"amount_check\" CHECK ((VALUE > 0));"));
}

#[test]
fn get_alter_script_domain_use_drop_false_comments_out_every_line() {
    // A string literal can carry a newline into pg_get_constraintdef output.
    let current = domain_with(&[("amount_check", "CHECK ((VALUE <> 'a'::text))")]);
    let target = domain_with(&[("amount_check", "CHECK ((VALUE <> 'a\nb'::text))")]);

    let script = current.get_alter_script(&target, false);

    assert_eq!(
        script,
        "-- use_drop=false: constraint \"amount_check\" requires drop+add; statements commented out (manual intervention needed)\n\n\
-- alter domain public.amount drop constraint \"amount_check\";\n\n\
-- alter domain public.amount add constraint \"amount_check\" CHECK ((VALUE <> 'a\n\
-- b'::text));\n\n"
    );
    for line in script.lines().filter(|line| !line.is_empty()) {
        assert!(line.starts_with("-- "), "live line: {line}");
    }
}

// ---- get_alter_script_parts: when each statement has to run ----

#[test]
fn alter_parts_defer_the_adding_half_of_a_changed_domain_check() {
    let current = domain_with(&[("amount_check", "CHECK ((VALUE > 0))")]);
    let target = domain_with(&[("amount_check", "CHECK ((VALUE > 5))")]);

    assert_eq!(
        current.get_alter_script_parts(&target, true),
        TypeAlterScript {
            add_values: String::new(),
            in_place: "alter domain public.amount drop constraint \"amount_check\";\n\n"
                .to_string(),
            deferred: "alter domain public.amount add constraint \"amount_check\" CHECK ((VALUE > 5));\n\n"
                .to_string(),
            warnings: Vec::new(),
        }
    );
}

#[test]
fn alter_parts_defer_validate_and_new_checks_without_a_no_changes_note() {
    let current = domain_with(&[
        ("a_chk", "CHECK ((VALUE >= 0)) NOT VALID"),
        ("c_old", "CHECK ((VALUE <> 7))"),
    ]);
    let mut target = domain_with(&[
        ("a_chk", "CHECK ((VALUE >= 0))"),
        ("b_new", "CHECK ((VALUE < 100))"),
    ]);
    target.comment = Some("amounts".to_string());

    let parts = current.get_alter_script_parts(&target, true);

    assert_eq!(parts.add_values, "");
    assert_eq!(
        parts.in_place,
        "alter domain public.amount drop constraint \"c_old\";\n\n\
comment on type public.amount is 'amounts';\n\n"
    );
    assert_eq!(
        parts.deferred,
        "alter domain public.amount validate constraint \"a_chk\";\n\n\
alter domain public.amount add constraint \"b_new\" CHECK ((VALUE < 100));\n\n"
    );
}

#[test]
fn alter_parts_leave_in_place_empty_when_every_change_is_deferred() {
    let current = domain_with(&[]);
    let target = domain_with(&[("amount_check", "CHECK ((VALUE > 0))")]);

    let parts = current.get_alter_script_parts(&target, true);

    assert_eq!(parts.in_place, "");
    assert_eq!(
        parts.deferred,
        "alter domain public.amount add constraint \"amount_check\" CHECK ((VALUE > 0));\n\n"
    );
    assert!(
        !current
            .get_alter_script(&target, true)
            .contains("requires no")
    );
}

#[test]
fn alter_parts_keep_a_commented_out_drop_and_add_together_in_place() {
    let current = domain_with(&[("amount_check", "CHECK ((VALUE > 0))")]);
    let target = domain_with(&[("amount_check", "CHECK ((VALUE > 5))")]);

    let parts = current.get_alter_script_parts(&target, false);

    // Nothing runs, so there is nothing to defer.
    assert_eq!(parts.deferred, "");
    assert!(
        parts
            .in_place
            .contains("-- alter domain public.amount drop constraint")
    );
    assert!(
        parts
            .in_place
            .contains("-- alter domain public.amount add constraint")
    );
}

#[test]
fn alter_parts_of_an_unchanged_domain_carry_the_no_changes_note_in_place() {
    let current = domain_with(&[("amount_check", "CHECK ((VALUE > 0))")]);

    let parts = current.get_alter_script_parts(&current.clone(), true);

    assert_eq!(
        parts,
        TypeAlterScript {
            in_place: "-- Domain public.amount requires no supported changes.\n".to_string(),
            ..TypeAlterScript::default()
        }
    );
}

#[test]
fn alter_parts_put_enum_add_value_apart_and_concatenate_unchanged() {
    let mut current = base_pg_type('e');
    current.typname = "status".to_string();
    current.enum_labels = vec![
        "pending".to_string(),
        "gone".to_string(),
        "completed".to_string(),
    ];
    let mut target = current.clone();
    target.enum_labels = vec![
        "pending".to_string(),
        "in_progress".to_string(),
        "completed".to_string(),
    ];
    target.owner = "app".to_string();

    let parts = current.get_alter_script_parts(&target, true);

    assert_eq!(
        parts.add_values,
        "alter type public.status add value if not exists 'in_progress' before 'completed';\n\n"
    );
    assert_eq!(
        parts.in_place,
        "-- Enum public.status cannot automatically remove value 'gone'.\n\
alter type public.status owner to app;\n\n"
    );
    assert_eq!(parts.deferred, "");
    // The one-string form is what pgc has always emitted for an enum.
    assert_eq!(
        current.get_alter_script(&target, true),
        format!("{}{}", parts.add_values, parts.in_place)
    );
}

// ---- Enum label order ----

fn status_enum(labels: &[&str]) -> PgType {
    let mut pg_type = base_pg_type('e');
    pg_type.typname = "status".to_string();
    pg_type.enum_labels = labels.iter().map(|label| label.to_string()).collect();
    pg_type
}

#[test]
fn get_alter_script_enum_reorder_is_noted_not_called_unchanged() {
    let current = status_enum(&["a", "b", "c"]);
    let target = status_enum(&["c", "b", "a"]);

    assert_eq!(
        current.get_alter_script(&target, true),
        "-- Enum public.status has its values in a different order ('a', 'b', 'c') than the target ('c', 'b', 'a'); PostgreSQL cannot reorder enum values (manual intervention needed).\n"
    );
}

#[test]
fn get_alter_script_enum_reorder_note_compares_only_shared_labels() {
    // 'x' is removed and 'n' added, but the shared labels keep their order.
    let current = status_enum(&["a", "x", "b"]);
    let target = status_enum(&["a", "b", "n"]);

    let script = current.get_alter_script(&target, true);

    assert!(!script.contains("different order"), "{script}");
    assert!(script.contains("add value if not exists 'n' after 'b';"));

    // Shared labels swapped while one is added: both are reported.
    let current = status_enum(&["a", "b"]);
    let target = status_enum(&["b", "n", "a"]);

    let parts = current.get_alter_script_parts(&target, true);

    assert_eq!(
        parts.add_values,
        "alter type public.status add value if not exists 'n' before 'a';\n\n"
    );
    assert!(
        parts
            .in_place
            .contains("different order ('a', 'b') than the target ('b', 'a')"),
        "{}",
        parts.in_place
    );
}

#[test]
fn get_alter_script_enum_reorder_note_comments_out_every_line() {
    let current = status_enum(&["a\nb", "c'd"]);
    let target = status_enum(&["c'd", "a\nb"]);

    let script = current.get_alter_script(&target, true);

    assert_eq!(
        script,
        "-- Enum public.status has its values in a different order ('a\n\
-- b', 'c''d') than the target ('c''d', 'a\n\
-- b'); PostgreSQL cannot reorder enum values (manual intervention needed).\n"
    );
}

// ---- reg-cascade: putting back what a routine's `DROP … CASCADE` takes ----

fn cascade_domain(default: Option<&str>, checks: &[(&str, &str)]) -> PgType {
    let mut domain = base_pg_type('d');
    domain.schema = "s1".to_string();
    domain.typname = "code_d".to_string();
    domain.formatted_basetype = Some("text".to_string());
    domain.typdefault = default.map(str::to_string);
    domain.domain_constraints = checks
        .iter()
        .map(|(name, definition)| DomainConstraint {
            name: name.to_string(),
            definition: definition.to_string(),
        })
        .collect();
    domain
}

#[test]
fn domain_constraint_recreate_script_guards_the_drop_and_quotes_the_name() {
    let domain = cascade_domain(None, &[]);
    let check = DomainConstraint {
        name: "Code check".to_string(),
        definition: "CHECK (s1.is_valid(VALUE))".to_string(),
    };
    assert_eq!(
        domain.get_domain_constraint_recreate_script(&check),
        "alter domain s1.code_d drop constraint if exists \"Code check\";\n\n\
         alter domain s1.code_d add constraint \"Code check\" CHECK (s1.is_valid(VALUE));\n\n"
    );
}

#[test]
fn domain_constraint_recreate_script_keeps_not_valid() {
    let domain = cascade_domain(None, &[]);
    let check = DomainConstraint {
        name: "c".to_string(),
        definition: "CHECK (s1.is_valid(VALUE)) NOT VALID".to_string(),
    };
    assert!(
        domain
            .get_domain_constraint_recreate_script(&check)
            .contains("add constraint \"c\" CHECK (s1.is_valid(VALUE)) NOT VALID;")
    );
}

#[test]
fn domain_default_scripts() {
    let domain = cascade_domain(Some("s1.first_code()"), &[]);
    assert_eq!(
        domain.get_drop_default_script(),
        "alter domain s1.code_d drop default;\n\n"
    );
    assert_eq!(
        domain.get_set_default_script().as_deref(),
        Some("alter domain s1.code_d set default s1.first_code();\n\n")
    );
    assert_eq!(cascade_domain(None, &[]).get_set_default_script(), None);
    assert_eq!(
        cascade_domain(Some("  "), &[]).get_set_default_script(),
        None
    );
}

#[test]
fn keeps_domain_constraint_only_for_what_the_alter_leaves_in_place() {
    let from = cascade_domain(
        None,
        &[
            ("same", "CHECK (s1.f(VALUE))"),
            ("unvalidated", "CHECK (s1.f(VALUE)) NOT VALID"),
            ("changed", "CHECK (s1.f(VALUE))"),
            ("from_only", "CHECK (s1.f(VALUE))"),
            ("made_not_valid", "CHECK (s1.f(VALUE))"),
        ],
    );
    let to = cascade_domain(
        None,
        &[
            ("same", "CHECK (s1.f(VALUE))"),
            ("unvalidated", "CHECK (s1.f(VALUE))"),
            ("changed", "CHECK (s1.g(VALUE))"),
            ("to_only", "CHECK (s1.f(VALUE))"),
            ("made_not_valid", "CHECK (s1.f(VALUE)) NOT VALID"),
        ],
    );
    assert!(from.keeps_domain_constraint(&to, "same"));
    // Validated in place (`validate constraint`), never re-created.
    assert!(from.keeps_domain_constraint(&to, "unvalidated"));
    // Dropped early and added back late by the alter itself.
    assert!(!from.keeps_domain_constraint(&to, "changed"));
    assert!(!from.keeps_domain_constraint(&to, "made_not_valid"));
    assert!(!from.keeps_domain_constraint(&to, "from_only"));
    assert!(!from.keeps_domain_constraint(&to, "to_only"));
}

#[test]
fn alter_recreates_only_range_types() {
    // `get_alter_script_parts` drops and recreates a changed range type, and
    // alters every other kind in place (or not at all).
    let range = base_pg_type('r');
    let mut changed = base_pg_type('r');
    changed.range_subdiff = Some("int4range_subdiff".to_string());
    assert!(range.alter_recreates(&changed));
    let script = range.get_alter_script(&changed, true);
    assert!(script.contains("drop type if exists"), "{script}");
    for kind in ['d', 'c', 'e', 'b', 'm'] {
        assert!(
            !base_pg_type(kind).alter_recreates(&base_pg_type(kind)),
            "{kind}"
        );
    }
    // A kind change is only reported, never a recreate.
    assert!(!range.alter_recreates(&base_pg_type('d')));
    let mut other = base_pg_type('r');
    other.typname = "other".to_string();
    assert!(!range.alter_recreates(&other));
}

#[test]
fn differs_beyond_acl_ignores_the_acl_only() {
    let mut a = base_pg_type('r');
    a.hash();
    let mut b = a.clone();
    b.acl = vec!["=U/postgres".to_string(), "postgres=U/postgres".to_string()];
    b.hash();
    // The stored hash still covers the ACL, as pgc 1.0.x wrote it.
    assert_ne!(a.hash, b.hash);
    assert!(!a.differs_beyond_acl(&b));
    let mut c = b.clone();
    c.range_subdiff = Some("int4range_subdiff".to_string());
    c.hash();
    assert!(a.differs_beyond_acl(&c));
    assert!(b.differs_beyond_acl(&c));
}

// ---- r3:types — composite attributes, multirange schema, warnings ----

/// Composite type `app.addr` with `attributes` as (name, type).
fn composite(attributes: &[(&str, &str)]) -> PgType {
    let mut pg_type = base_pg_type('c');
    pg_type.schema = "app".to_string();
    pg_type.typname = "addr".to_string();
    pg_type.composite_attributes = attributes
        .iter()
        .map(|(name, data_type)| CompositeAttribute {
            name: name.to_string(),
            data_type: data_type.to_string(),
            collation: None,
        })
        .collect();
    pg_type.hash();
    pg_type
}

/// Nothing in the migrated database holds a value of the type.
fn unused() -> CompositeUsage {
    CompositeUsage {
        typed_tables: Vec::new(),
        stored_in: Some(Vec::new()),
    }
}

#[test]
fn composite_attribute_add_drop_and_retype_are_migrated() {
    let current = composite(&[("street", "text"), ("city", "text"), ("zip", "integer")]);
    let target = composite(&[("street", "text"), ("zip", "bigint"), ("region", "text")]);

    let parts = current.get_alter_script_parts_with(&target, true, &unused());

    assert_eq!(
        parts.in_place,
        "alter type app.addr drop attribute if exists \"city\";\n\n\
alter type app.addr alter attribute \"zip\" set data type bigint;\n\n\
alter type app.addr add attribute \"region\" text;\n\n"
    );
    assert!(parts.warnings.is_empty(), "{:?}", parts.warnings);
    assert_eq!(parts.add_values, "");
    assert_eq!(parts.deferred, "");
    // No "not implemented" catch-all any more.
    assert!(
        !current
            .get_alter_script(&target, true)
            .contains("not implemented")
    );
}

#[test]
fn composite_attribute_changes_cascade_to_typed_tables() {
    let current = composite(&[("x", "integer"), ("y", "integer")]);
    let target = composite(&[("x", "bigint"), ("z", "text")]);
    let usage = CompositeUsage {
        typed_tables: vec!["app.addresses".to_string()],
        stored_in: Some(Vec::new()),
    };

    let parts = current.get_alter_script_parts_with(&target, true, &usage);

    assert_eq!(
        parts.in_place,
        "-- Composite type app.addr drops attribute(s) \"y\" with CASCADE, which PostgreSQL needs to drop them from its typed table(s) app.addresses; CASCADE also drops every object that depends on those columns there, including views, rules, policies, foreign keys and routines outside the compared schemas.\n\n\
alter type app.addr drop attribute if exists \"y\" cascade;\n\n\
alter type app.addr alter attribute \"x\" set data type bigint cascade;\n\n\
alter type app.addr add attribute \"z\" text cascade;\n\n"
    );
    // Review finding: the CASCADE drop of dependents is said on stderr too.
    assert_eq!(parts.warnings.len(), 1, "{:?}", parts.warnings);
    assert!(parts.warnings[0].contains("drops attribute(s) \"y\" with CASCADE"));
    assert!(parts.warnings[0].contains("outside the compared schemas"));

    // Only a live drop warns: no drop, or use_drop=false, says nothing.
    let added_only = composite(&[("x", "integer"), ("y", "integer"), ("z", "text")]);
    let parts = current.get_alter_script_parts_with(&added_only, true, &usage);
    assert!(parts.warnings.is_empty(), "{:?}", parts.warnings);
    let parts = current.get_alter_script_parts_with(&target, false, &usage);
    assert!(parts.warnings.is_empty(), "{:?}", parts.warnings);
    assert!(
        parts
            .in_place
            .contains("-- alter type app.addr drop attribute if exists \"y\" cascade;"),
        "{}",
        parts.in_place
    );
    // Without typed tables there is no CASCADE, and no note.
    let parts = current.get_alter_script_parts_with(&target, true, &unused());
    assert!(!parts.in_place.contains("CASCADE"), "{}", parts.in_place);
    assert!(parts.warnings.is_empty(), "{:?}", parts.warnings);
}

#[test]
fn composite_attribute_retype_is_left_out_while_a_column_stores_the_type() {
    let current = composite(&[("zip", "integer")]);
    let target = composite(&[("zip", "bigint")]);
    let usage = CompositeUsage {
        typed_tables: Vec::new(),
        stored_in: Some(vec!["app.orders.ship_to".to_string()]),
    };

    let parts = current.get_alter_script_parts_with(&target, true, &usage);

    assert_eq!(
        parts.in_place,
        "-- Attribute \"zip\" of composite type app.addr cannot change its type (integer -> bigint) in place: column(s) app.orders.ship_to store values of the type, and PostgreSQL refuses to retype an attribute then (manual intervention needed).\n\n\
-- alter type app.addr alter attribute \"zip\" set data type bigint;\n\n"
    );
    assert_eq!(parts.warnings.len(), 1);
    assert!(parts.warnings[0].contains("app.orders.ship_to"));
    assert!(parts.warnings[0].contains("manual intervention needed"));
}

#[test]
fn composite_attribute_retype_is_emitted_when_stored_columns_are_unknown() {
    // A pgc 1.0.x FROM dump: PostgreSQL decides.
    let current = composite(&[("zip", "integer")]);
    let target = composite(&[("zip", "bigint")]);
    let parts = current.get_alter_script_parts(&target, true);
    assert_eq!(
        parts.in_place,
        "alter type app.addr alter attribute \"zip\" set data type bigint;\n\n"
    );
    assert!(parts.warnings.is_empty());
}

#[test]
fn composite_attribute_rename_is_a_drop_and_an_add() {
    // Nothing tells a rename apart; the last attribute renamed stays last.
    let current = composite(&[("a", "integer"), ("b", "integer")]);
    let target = composite(&[("a", "integer"), ("bb", "integer")]);
    let parts = current.get_alter_script_parts_with(&target, true, &unused());
    assert_eq!(
        parts.in_place,
        "alter type app.addr drop attribute if exists \"b\";\n\n\
alter type app.addr add attribute \"bb\" integer;\n\n"
    );
}

#[test]
fn composite_attributes_out_of_place_are_dropped_and_re_added_when_nothing_holds_values() {
    // `b` renamed in the middle: dropping `b` and adding `bb` would leave
    // (a, c, bb), so `c` is dropped and re-added after `bb`.
    let current = composite(&[("a", "integer"), ("b", "integer"), ("c", "text")]);
    let target = composite(&[("a", "integer"), ("bb", "integer"), ("c", "varchar(5)")]);

    let parts = current.get_alter_script_parts_with(&target, true, &unused());

    assert_eq!(
        parts.in_place,
        "alter type app.addr drop attribute if exists \"b\";\n\n\
alter type app.addr drop attribute if exists \"c\";\n\n\
alter type app.addr add attribute \"bb\" integer;\n\n\
alter type app.addr add attribute \"c\" varchar(5);\n\n"
    );
    assert!(parts.warnings.is_empty());
}

#[test]
fn composite_attribute_inserted_in_the_middle_is_re_added_in_order() {
    let current = composite(&[("a", "integer"), ("c", "integer")]);
    let target = composite(&[("a", "integer"), ("b", "integer"), ("c", "integer")]);
    let parts = current.get_alter_script_parts_with(&target, true, &unused());
    assert_eq!(
        parts.in_place,
        "alter type app.addr drop attribute if exists \"c\";\n\n\
alter type app.addr add attribute \"b\" integer;\n\n\
alter type app.addr add attribute \"c\" integer;\n\n"
    );
}

#[test]
fn composite_attribute_order_is_kept_with_a_note_when_values_may_be_lost() {
    let current = composite(&[("a", "integer"), ("c", "integer")]);
    let target = composite(&[("a", "integer"), ("b", "integer"), ("c", "integer")]);
    let cases = [
        (
            CompositeUsage {
                typed_tables: Vec::new(),
                stored_in: Some(vec!["app.t.v".to_string()]),
            },
            true,
            "column(s) app.t.v store values of it",
        ),
        (
            CompositeUsage {
                typed_tables: vec!["app.typed".to_string()],
                stored_in: Some(Vec::new()),
            },
            true,
            "typed table(s) app.typed hold its rows",
        ),
        (
            CompositeUsage::default(),
            true,
            "the FROM dump (written by pgc 1.0.x) does not record which columns store values of it",
        ),
        (unused(), false, "use_drop=false"),
    ];
    for (usage, use_drop, reason) in cases {
        let parts = current.get_alter_script_parts_with(&target, use_drop, &usage);
        let cascade = if usage.typed_tables.is_empty() {
            ""
        } else {
            " cascade"
        };
        assert_eq!(
            parts.in_place,
            format!(
                "-- Composite type app.addr will have its attributes in a different order (\"a\", \"c\", \"b\") than the target (\"a\", \"b\", \"c\"); pgc reorders attributes only by dropping and re-adding them, which would lose stored values ({reason}) (manual intervention needed).\n\n\
alter type app.addr add attribute \"b\" integer{cascade};\n\n"
            ),
            "{reason}"
        );
        assert_eq!(parts.warnings.len(), 1, "{reason}");
        assert!(parts.warnings[0].contains(reason), "{reason}");
    }
}

#[test]
fn composite_attribute_drop_is_commented_out_without_use_drop() {
    let current = composite(&[("a", "integer"), ("gone", "text")]);
    let target = composite(&[("a", "integer"), ("new", "text")]);
    let parts = current.get_alter_script_parts_with(&target, false, &unused());
    assert_eq!(
        parts.in_place,
        "-- alter type app.addr drop attribute if exists \"gone\";\n\n\
alter type app.addr add attribute \"new\" text;\n\n"
    );
}

#[test]
fn composite_attribute_names_are_quoted_and_comment_safe() {
    let current = composite(&[("x\"y", "integer"), ("line\nbreak", "integer")]);
    let target = composite(&[("x\"y", "bigint"), ("line\nbreak", "integer")]);
    let usage = CompositeUsage {
        typed_tables: Vec::new(),
        stored_in: Some(vec!["app.t.v".to_string()]),
    };
    let parts = current.get_alter_script_parts_with(&target, true, &usage);
    // Every line of the note and the left-out statement is a comment.
    for line in parts.in_place.lines().filter(|l| !l.is_empty()) {
        assert!(line.starts_with("-- "), "{line:?} in {:?}", parts.in_place);
    }
    assert!(parts.in_place.contains("\"x\"\"y\""), "{}", parts.in_place);

    let target = composite(&[
        ("x\"y", "integer"),
        ("line\nbreak", "integer"),
        ("n", "text"),
    ]);
    let parts = current.get_alter_script_parts_with(&target, true, &unused());
    assert_eq!(
        parts.in_place,
        "alter type app.addr add attribute \"n\" text;\n\n"
    );
}

#[test]
fn composite_comment_or_owner_change_alone_emits_no_attribute_statement() {
    let current = composite(&[("a", "integer")]);
    let mut target = current.clone();
    target.comment = Some("postal address".to_string());
    target.owner = "app".to_string();
    let parts = current.get_alter_script_parts_with(&target, true, &unused());
    assert_eq!(
        parts.in_place,
        "comment on type app.addr is 'postal address';\n\n\
alter type app.addr owner to app;\n\n"
    );
    assert!(parts.warnings.is_empty());
}

#[test]
fn composite_usage_recorded_reads_the_dump_fields() {
    let mut pg_type = composite(&[("a", "integer")]);
    assert_eq!(
        CompositeUsage::recorded(&pg_type),
        CompositeUsage::default()
    );
    pg_type.typed_tables = Some(vec!["app.t".to_string()]);
    pg_type.composite_stored_in = Some(vec!["app.u.v".to_string()]);
    assert_eq!(
        CompositeUsage::recorded(&pg_type),
        CompositeUsage {
            typed_tables: vec!["app.t".to_string()],
            stored_in: Some(vec!["app.u.v".to_string()]),
        }
    );
}

#[test]
fn hash_ignores_composite_usage_and_multirange_schema() {
    // None of the fields this release added is hashed, so every type keeps
    // the hash pgc 1.0.x gave it.
    let plain = composite(&[("street", "text"), ("zip", "integer")]);
    let mut recorded = plain.clone();
    recorded.typed_tables = Some(vec!["app.t".to_string()]);
    recorded.composite_stored_in = Some(vec!["app.u.v".to_string()]);
    recorded.hash();
    assert_eq!(plain.hash, recorded.hash);
    assert_eq!(
        plain.hash.as_deref(),
        Some("ce8ab0045a4b62fea5d17c105c628affa978ed8686025fb0b19a83d3d6e6635b")
    );

    let mut range = base_pg_type('r');
    range.multirange_name = Some("my_type_multirange".to_string());
    range.hash();
    let mut placed = range.clone();
    placed.multirange_schema = Some("public".to_string());
    placed.hash();
    assert_eq!(range.hash, placed.hash);
}

#[test]
fn range_get_script_qualifies_the_multirange() {
    let mut range = base_pg_type('r');
    range.schema = "app".to_string();
    range.typname = "span".to_string();
    range.range_subtype = Some("integer".to_string());
    range.multirange_name = Some("span_multirange".to_string());

    // A pgc 1.0.x dump did not record the schema: the range's own.
    assert!(
        range
            .get_script()
            .contains("multirange_type_name = app.span_multirange\n"),
        "{}",
        range.get_script()
    );
    // Otherwise where the TO multirange is, even outside the range's schema.
    range.multirange_schema = Some("\"Other\"".to_string());
    assert!(
        range
            .get_script()
            .contains("multirange_type_name = \"Other\".span_multirange\n"),
        "{}",
        range.get_script()
    );
    // The recreate path uses the same script.
    let mut changed = range.clone();
    changed.range_subtype = Some("bigint".to_string());
    assert!(
        range
            .get_alter_script(&changed, true)
            .contains("multirange_type_name = \"Other\".span_multirange")
    );
}

#[test]
fn multirange_set_schema_moves_a_misplaced_multirange_only() {
    let mut current = base_pg_type('r');
    current.schema = "app".to_string();
    current.typname = "span".to_string();
    current.multirange_name = Some("span_multirange".to_string());
    current.multirange_schema = Some("public".to_string());
    let mut target = current.clone();
    target.multirange_schema = Some("app".to_string());

    assert_eq!(
        current.get_multirange_set_schema_script(&target).as_deref(),
        Some("alter type public.span_multirange set schema app;\n\n")
    );
    // In place already.
    assert_eq!(target.get_multirange_set_schema_script(&target), None);
    // Unknown on either side (a pgc 1.0.x dump).
    let mut legacy = current.clone();
    legacy.multirange_schema = None;
    assert_eq!(legacy.get_multirange_set_schema_script(&target), None);
    assert_eq!(current.get_multirange_set_schema_script(&legacy), None);
    // A different multirange name is a definition change (hashed), which
    // recreates the range; not a move.
    let mut renamed = target.clone();
    renamed.multirange_name = Some("other_multirange".to_string());
    assert_eq!(current.get_multirange_set_schema_script(&renamed), None);
    // Not a range.
    assert_eq!(
        base_pg_type('c').get_multirange_set_schema_script(&base_pg_type('c')),
        None
    );
}

#[test]
fn is_named_by_matches_format_type_spellings() {
    let mut pg_type = base_pg_type('c');
    pg_type.schema = "app".to_string();
    pg_type.typname = "\"Addr\"".to_string();
    assert!(pg_type.is_named_by("app.\"Addr\""));
    assert!(pg_type.is_named_by("app.\"Addr\"[]"));
    assert!(pg_type.is_named_by("app.\"Addr\"[][]"));
    assert!(!pg_type.is_named_by("\"Addr\""));
    assert!(!pg_type.is_named_by("other.\"Addr\""));
    pg_type.schema = "public".to_string();
    assert!(pg_type.is_named_by("\"Addr\""));
    assert!(pg_type.is_named_by("public.\"Addr\""));
}

#[test]
fn every_difference_left_in_place_is_a_warning() {
    // Kind change.
    let parts = base_pg_type('d').get_alter_script_parts(&base_pg_type('c'), true);
    assert!(parts.in_place.starts_with("-- Cannot change type kind"));
    assert_eq!(parts.warnings.len(), 1);
    assert!(parts.warnings[0].contains("public.my_type changes its kind"));

    // Enum value removal and reorder.
    let mut current = base_pg_type('e');
    current.enum_labels = vec!["a".to_string(), "b".to_string(), "gone".to_string()];
    let mut target = current.clone();
    target.enum_labels = vec!["b".to_string(), "a".to_string()];
    let parts = current.get_alter_script_parts(&target, true);
    assert_eq!(parts.warnings.len(), 2, "{:?}", parts.warnings);
    assert!(parts.warnings[0].contains("keeps value 'gone'"));
    assert!(parts.warnings[1].contains("different order"));

    // Domain base type.
    let mut current = base_pg_type('d');
    current.formatted_basetype = Some("integer".to_string());
    let mut target = current.clone();
    target.formatted_basetype = Some("bigint".to_string());
    let parts = current.get_alter_script_parts(&target, true);
    assert_eq!(parts.warnings.len(), 1);
    assert!(parts.warnings[0].contains("(integer -> bigint)"));

    // A kind pgc cannot alter at all.
    let parts = base_pg_type('b').get_alter_script_parts(&base_pg_type('b'), true);
    assert!(parts.in_place.contains("Alter script not implemented"));
    assert_eq!(parts.warnings.len(), 1);

    // What pgc migrates warns about nothing.
    let current = domain_with(&[("amount_check", "CHECK ((VALUE > 0))")]);
    let target = domain_with(&[("amount_check", "CHECK ((VALUE > 5))")]);
    assert!(
        current
            .get_alter_script_parts(&target, true)
            .warnings
            .is_empty()
    );
}

#[test]
fn create_warning_names_every_type_get_script_leaves_out() {
    let mut labelless = base_pg_type('e');
    assert!(labelless.get_script().starts_with("-- "));
    assert!(
        labelless
            .create_warning()
            .is_some_and(|w| w.contains("public.my_type") && w.contains("no labels"))
    );
    labelless.enum_labels = vec!["a".to_string()];
    assert_eq!(labelless.create_warning(), None);

    let empty = base_pg_type('c');
    assert!(empty.get_script().starts_with("-- "));
    assert!(
        empty
            .create_warning()
            .is_some_and(|w| w.contains("no attributes"))
    );
    assert_eq!(composite(&[("a", "integer")]).create_warning(), None);

    let unsupported = base_pg_type('b');
    assert!(unsupported.get_script().starts_with("-- "));
    assert!(unsupported.create_warning().is_some());

    for kind in ['d', 'r', 'm'] {
        assert_eq!(base_pg_type(kind).create_warning(), None, "{kind}");
    }
}

/// Review finding: a domain whose collation changes reached only "requires
/// no supported changes", with no warning, so it never converged silently.
#[test]
fn domain_collation_change_is_noted_and_warned() {
    let mut current = base_pg_type('d');
    current.schema = "app".to_string();
    current.typname = "code".to_string();
    current.formatted_basetype = Some("text".to_string());
    current.domain_collation_name = Some("C".to_string());
    current.hash();
    let mut target = current.clone();
    target.domain_collation_name = Some("POSIX".to_string());
    target.hash();

    let parts = current.get_alter_script_parts(&target, true);

    assert_eq!(
        parts.in_place,
        "-- Changing the collation of domain app.code (C -> POSIX) is not supported automatically.\n\n\n"
    );
    assert!(!parts.in_place.contains("requires no supported changes"));
    assert_eq!(parts.warnings.len(), 1, "{:?}", parts.warnings);
    assert!(parts.warnings[0].contains("domain app.code changes its collation (C -> POSIX)"));

    // To or from the default collation.
    let mut default = current.clone();
    default.domain_collation_name = None;
    default.hash();
    let parts = default.get_alter_script_parts(&target, true);
    assert!(
        parts.warnings[0].contains("(default -> POSIX)"),
        "{:?}",
        parts.warnings
    );
}

#[test]
fn domain_difference_nothing_covers_is_warned() {
    let mut current = base_pg_type('d');
    current.schema = "app".to_string();
    current.typname = "code".to_string();
    current.formatted_basetype = Some("text".to_string());
    current.hash();
    // Something no domain statement covers (and PostgreSQL cannot alter).
    let mut target = current.clone();
    target.typdelim = b';' as i8;
    target.hash();
    let parts = current.get_alter_script_parts(&target, true);
    assert!(parts.in_place.contains("requires no supported changes"));
    assert_eq!(parts.warnings.len(), 1, "{:?}", parts.warnings);
    assert!(parts.warnings[0].contains("pgc cannot alter"));

    // An owner or comment change is emitted, and warns about nothing.
    let mut owner_and_comment = current.clone();
    owner_and_comment.owner = "app_owner".to_string();
    owner_and_comment.comment = Some("codes".to_string());
    owner_and_comment.hash();
    let parts = current.get_alter_script_parts(&owner_and_comment, true);
    assert!(parts.warnings.is_empty(), "{:?}", parts.warnings);
    assert!(
        parts
            .in_place
            .contains("comment on type app.code is 'codes';")
    );
    assert!(
        parts.in_place.contains("owner to app_owner"),
        "{}",
        parts.in_place
    );
}

// ---- r4:typed — composite attribute collations ----

/// `composite` with the attribute `name` given the collation `collation`.
fn with_collation(mut pg_type: PgType, name: &str, collation: &str) -> PgType {
    for attribute in &mut pg_type.composite_attributes {
        if attribute.name == name {
            attribute.collation = Some(collation.to_string());
        }
    }
    pg_type.hash();
    pg_type
}

/// A typed table holds values of the type (so `CASCADE`).
fn typed_usage() -> CompositeUsage {
    CompositeUsage {
        typed_tables: vec!["app.t".to_string()],
        stored_in: Some(Vec::new()),
    }
}

#[test]
fn composite_attribute_type_clause_appends_its_collation() {
    let plain = CompositeAttribute {
        name: "note".to_string(),
        data_type: "text".to_string(),
        collation: None,
    };
    assert_eq!(plain.type_clause(), "text");
    let collated = CompositeAttribute {
        collation: Some("pg_catalog.\"C\"".to_string()),
        ..plain
    };
    assert_eq!(collated.type_clause(), "text collate pg_catalog.\"C\"");
}

#[test]
fn composite_hash_without_collations_is_the_one_pgc_1_0_gave() {
    // Computed by pgc before attribute collations were recorded.
    let pg_type = composite(&[("street", "text"), ("zip", "integer")]);
    assert_eq!(
        pg_type.hash.as_deref(),
        Some("ce8ab0045a4b62fea5d17c105c628affa978ed8686025fb0b19a83d3d6e6635b")
    );
}

#[test]
fn composite_hash_covers_an_attribute_collation() {
    let plain = composite(&[("street", "text"), ("zip", "integer")]);
    let c = with_collation(plain.clone(), "street", "pg_catalog.\"C\"");
    let posix = with_collation(plain.clone(), "street", "pg_catalog.\"POSIX\"");
    assert_ne!(plain.hash, c.hash);
    assert_ne!(c.hash, posix.hash);
    assert_eq!(
        c.hash,
        with_collation(plain, "street", "pg_catalog.\"C\"").hash
    );
}

#[test]
fn composite_get_script_keeps_an_attribute_collation() {
    let pg_type = with_collation(
        composite(&[("street", "text"), ("zip", "integer")]),
        "street",
        "pg_catalog.\"C\"",
    );
    assert_eq!(
        pg_type.get_script(),
        "create type app.addr as (\n    \"street\" text collate pg_catalog.\"C\",\n    \"zip\" integer\n);\n\n"
    );
}

#[test]
fn composite_collation_change_is_a_retype() {
    let current = with_collation(
        composite(&[("a", "text"), ("b", "text")]),
        "b",
        "pg_catalog.\"C\"",
    );
    let target = with_collation(
        composite(&[("a", "text"), ("b", "text")]),
        "a",
        "public.\"my coll\"",
    );

    let parts = current.get_alter_script_parts_with(&target, true, &typed_usage());

    // To the data type's default collation, `SET DATA TYPE` without one.
    assert_eq!(
        parts.in_place,
        "alter type app.addr alter attribute \"a\" set data type text collate public.\"my coll\" cascade;\n\n\
alter type app.addr alter attribute \"b\" set data type text cascade;\n\n"
    );
    assert!(parts.warnings.is_empty(), "{:?}", parts.warnings);
    assert_eq!(
        current.composite_retyped_attributes(&target, true, &typed_usage()),
        vec!["a".to_string(), "b".to_string()]
    );
}

#[test]
fn composite_attribute_added_with_a_collation_keeps_it() {
    let current = composite(&[("a", "text")]);
    let target = with_collation(
        composite(&[("a", "text"), ("b", "text")]),
        "b",
        "pg_catalog.\"C\"",
    );
    let parts = current.get_alter_script_parts_with(&target, true, &unused());
    assert_eq!(
        parts.in_place,
        "alter type app.addr add attribute \"b\" text collate pg_catalog.\"C\";\n\n"
    );
}

#[test]
fn composite_collation_change_under_a_stored_column_is_a_note() {
    let current = composite(&[("a", "text")]);
    let target = with_collation(composite(&[("a", "text")]), "a", "pg_catalog.\"C\"");
    let usage = CompositeUsage {
        typed_tables: Vec::new(),
        stored_in: Some(vec!["app.holder.v".to_string()]),
    };
    let parts = current.get_alter_script_parts_with(&target, true, &usage);
    assert!(
        parts.in_place.contains(
            "-- Attribute \"a\" of composite type app.addr cannot change its type (text -> text collate pg_catalog.\"C\") in place"
        ),
        "{}",
        parts.in_place
    );
    assert!(
        parts
            .in_place
            .contains("-- alter type app.addr alter attribute \"a\" set data type text collate pg_catalog.\"C\";"),
        "{}",
        parts.in_place
    );
    // Nothing is retyped live, so nothing is in the way of a retype.
    assert!(
        current
            .composite_retyped_attributes(&target, true, &usage)
            .is_empty()
    );
}

#[test]
fn composite_retyped_attributes_lists_only_live_retypes() {
    let current = composite(&[("a", "integer"), ("b", "text"), ("gone", "text")]);
    let target = composite(&[("a", "bigint"), ("b", "text"), ("new", "text")]);
    assert_eq!(
        current.composite_retyped_attributes(&target, true, &typed_usage()),
        vec!["a".to_string()]
    );
    // Retyped without use_drop too: ALTER ATTRIBUTE drops nothing.
    assert_eq!(
        current.composite_retyped_attributes(&target, false, &typed_usage()),
        vec!["a".to_string()]
    );
    // An attribute dropped and re-added to reorder is not retyped.
    let reordered = composite(&[("b", "text"), ("a", "bigint"), ("gone", "text")]);
    assert!(
        current
            .composite_retyped_attributes(&reordered, true, &unused())
            .is_empty()
    );
    // Only composite types.
    assert!(
        base_pg_type('d')
            .composite_retyped_attributes(&base_pg_type('d'), true, &unused())
            .is_empty()
    );
}
