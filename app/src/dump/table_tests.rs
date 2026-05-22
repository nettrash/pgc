use super::*;
use sqlx::postgres::types::Oid;

fn base_column(name: &str, ordinal_position: i32) -> TableColumn {
    TableColumn {
        catalog: "postgres".to_string(),
        schema: "public".to_string(),
        table: "users".to_string(),
        name: name.to_string(),
        ordinal_position,
        column_default: None,
        is_nullable: true,
        data_type: "text".to_string(),
        character_maximum_length: None,
        character_octet_length: None,
        numeric_precision: None,
        numeric_precision_radix: None,
        numeric_scale: None,
        datetime_precision: None,
        interval_type: None,
        interval_precision: None,
        character_set_catalog: None,
        character_set_schema: None,
        character_set_name: None,
        collation_catalog: None,
        collation_schema: None,
        collation_name: None,
        domain_catalog: None,
        domain_schema: None,
        domain_name: None,
        udt_catalog: None,
        udt_schema: None,
        udt_name: None,
        scope_catalog: None,
        scope_schema: None,
        scope_name: None,
        maximum_cardinality: None,
        dtd_identifier: None,
        is_self_referencing: false,
        is_identity: false,
        identity_generation: None,
        identity_start: None,
        identity_increment: None,
        identity_maximum: None,
        identity_minimum: None,
        identity_cycle: false,
        is_generated: "NEVER".to_string(),
        generation_expression: None,
        generation_type: None,
        is_updatable: true,
        related_views: None,
        comment: None,
        storage: None,
        compression: None,
        statistics_target: None,
        acl: vec![],
        serial_type: None,
    }
}

fn identity_column(name: &str, ordinal_position: i32, data_type: &str) -> TableColumn {
    let mut column = base_column(name, ordinal_position);
    column.data_type = data_type.to_string();
    column.is_identity = true;
    column.is_nullable = false;
    column.identity_generation = Some("BY DEFAULT".to_string());
    column
}

fn name_column() -> TableColumn {
    let mut column = base_column("name", 2);
    column.is_nullable = false;
    column
}

fn name_column_with_default() -> TableColumn {
    let mut column = name_column();
    column.column_default = Some("'unknown'::text".to_string());
    column
}

fn legacy_column() -> TableColumn {
    let mut column = base_column("legacy", 3);
    column.is_nullable = true;
    column
}

fn email_column() -> TableColumn {
    let mut column = base_column("email", 3);
    column.is_nullable = true;
    column
}

fn primary_key_constraint() -> TableConstraint {
    TableConstraint {
        catalog: "postgres".to_string(),
        schema: "public".to_string(),
        name: "users_pkey".to_string(),
        table_name: "users".to_string(),
        constraint_type: "PRIMARY KEY".to_string(),
        is_deferrable: false,
        initially_deferred: false,
        definition: None,
        coninhcount: 0,
        is_enforced: true,
        no_inherit: false,
        nulls_not_distinct: false,
        comment: None,
    }
}

fn check_constraint(name: &str, definition: &str) -> TableConstraint {
    TableConstraint {
        catalog: "postgres".to_string(),
        schema: "public".to_string(),
        name: name.to_string(),
        table_name: "users".to_string(),
        constraint_type: "CHECK".to_string(),
        is_deferrable: false,
        initially_deferred: false,
        definition: Some(definition.to_string()),
        coninhcount: 0,
        is_enforced: true,
        no_inherit: false,
        nulls_not_distinct: false,
        comment: None,
    }
}

fn foreign_key_constraint(is_deferrable: bool, initially_deferred: bool) -> TableConstraint {
    TableConstraint {
        catalog: "postgres".to_string(),
        schema: "public".to_string(),
        name: "users_account_fk".to_string(),
        table_name: "users".to_string(),
        constraint_type: "FOREIGN KEY".to_string(),
        is_deferrable,
        initially_deferred,
        definition: Some("FOREIGN KEY (account_id) REFERENCES public.accounts(id)".to_string()),
        coninhcount: 0,
        is_enforced: true,
        no_inherit: false,
        nulls_not_distinct: false,
        comment: None,
    }
}

fn unique_constraint(name: &str, definition: &str) -> TableConstraint {
    TableConstraint {
        catalog: "postgres".to_string(),
        schema: "public".to_string(),
        name: name.to_string(),
        table_name: "users".to_string(),
        constraint_type: "UNIQUE".to_string(),
        is_deferrable: false,
        initially_deferred: false,
        definition: Some(definition.to_string()),
        coninhcount: 0,
        is_enforced: true,
        no_inherit: false,
        nulls_not_distinct: false,
        comment: None,
    }
}

fn primary_key_index() -> TableIndex {
    TableIndex {
        schema: "public".to_string(),
        table: "users".to_string(),
        name: "users_pkey".to_string(),
        catalog: None,
        indexdef:
            "create unique index users_pkey on public.users using btree (\"id\") primary key (\"id\")"
                .to_string(),
        is_partition_index: false,
        comment: None,
    }
}

fn name_index(definition: &str) -> TableIndex {
    TableIndex {
        schema: "public".to_string(),
        table: "users".to_string(),
        name: "idx_users_name".to_string(),
        catalog: None,
        indexdef: definition.to_string(),
        is_partition_index: false,
        comment: None,
    }
}

fn legacy_index() -> TableIndex {
    TableIndex {
        schema: "public".to_string(),
        table: "users".to_string(),
        name: "idx_users_old".to_string(),
        catalog: None,
        indexdef: "create index idx_users_old on public.users using btree (legacy)".to_string(),
        is_partition_index: false,
        comment: None,
    }
}

fn email_index() -> TableIndex {
    TableIndex {
        schema: "public".to_string(),
        table: "users".to_string(),
        name: "idx_users_email".to_string(),
        catalog: None,
        indexdef: "create index idx_users_email on public.users using btree (email)".to_string(),
        is_partition_index: false,
        comment: None,
    }
}

fn unique_email_index() -> TableIndex {
    TableIndex {
        schema: "public".to_string(),
        table: "users".to_string(),
        name: "idx_users_email".to_string(),
        catalog: None,
        indexdef: "create unique index idx_users_email on public.users using btree (email)"
            .to_string(),
        is_partition_index: false,
        comment: None,
    }
}

fn trigger(name: &str, definition: &str, oid: u32) -> TableTrigger {
    TableTrigger {
        oid: Oid(oid),
        name: name.to_string(),
        definition: definition.to_string(),
        enabled: "O".to_string(),
        comment: None,
    }
}

fn policy(
    name: &str,
    command: &str,
    using_clause: Option<&str>,
    check_clause: Option<&str>,
) -> TablePolicy {
    TablePolicy {
        schema: "public".to_string(),
        table: "users".to_string(),
        name: name.to_string(),
        command: command.to_string(),
        permissive: true,
        roles: vec!["public".to_string()],
        using_clause: using_clause.map(|c| c.to_string()),
        check_clause: check_clause.map(|c| c.to_string()),
    }
}

#[test]
fn test_escape_single_quotes() {
    let input = "O'Reilly";
    let escaped = super::escape_single_quotes(input);
    assert_eq!(escaped, "O''Reilly");
}

fn basic_table() -> Table {
    Table::new(
        "public".to_string(),
        "users".to_string(),
        "public".to_string(),
        "users".to_string(),
        "postgres".to_string(),
        Some("pg_default".to_string()),
        vec![identity_column("id", 1, "integer"), name_column()],
        vec![
            primary_key_constraint(),
            check_constraint("users_name_check", "CHECK (name <> '')"),
        ],
        vec![
            primary_key_index(),
            name_index("create index idx_users_name on public.users using btree (name)"),
        ],
        vec![trigger(
            "audit_user",
            "create trigger audit_user before insert on public.users for each row execute function log_user()",
            1,
        )],
        None,
    )
}

#[test]
fn test_table_new_initializes_flags_and_hash() {
    let table = basic_table();

    assert!(table.has_indexes);
    assert!(table.has_triggers);
    assert!(!table.has_rules);
    assert!(!table.has_rowsecurity);
    assert!(table.hash.is_some());

    let mut recomputed = table.clone();
    recomputed.hash();
    assert_eq!(table.hash, recomputed.hash);

    let mut modified = table.clone();
    if let Some(column) = modified.columns.iter_mut().find(|col| col.name == "name") {
        column.column_default = Some("'anonymous'::text".to_string());
    }
    modified.hash();
    assert_ne!(table.hash, modified.hash);
}

#[test]
fn test_table_hash_changes_with_policy() {
    let mut table = basic_table();
    table.hash();
    let original_hash = table.hash.clone();

    table.policies = vec![policy(
        "users_rls",
        "select",
        Some("tenant_id = current_setting('app.current_tenant')::int"),
        None,
    )];
    table.has_rowsecurity = true;
    table.hash();

    assert_ne!(original_hash, table.hash);
}

#[test]
fn test_get_script_generates_full_definition() {
    let table = basic_table();

    let script = table.get_script();

    let expected = concat!(
        "create table public.users (\n",
        "    id integer generated BY DEFAULT as identity not null,\n",
        "    name text not null,\n",
        "    constraint users_pkey primary key (\"id\")\n",
        ")\n",
        "tablespace \"pg_default\";\n\n",
        "alter table public.users add constraint users_name_check check (name <> '') ;\n\n",
        "create index idx_users_name on public.users using btree (name);\n\n",
        "create trigger audit_user before insert on public.users for each row execute function log_user();\n\n",
        "alter table public.users owner to postgres;\n\n",
    );

    assert_eq!(script, expected);
}

#[test]
fn test_get_script_includes_policies_and_row_security() {
    let mut table = basic_table();
    table.policies = vec![policy(
        "users_tenant_select",
        "select",
        Some("tenant_id = current_setting('app.current_tenant')::int"),
        None,
    )];
    table.has_rowsecurity = true;

    let script = table.get_script();

    assert!(script.contains("create policy users_tenant_select"));
    assert!(script.contains("for select"));
    assert!(script.contains("enable row level security"));
}

#[test]
fn test_get_script_includes_unique_indexes() {
    let table = Table::new(
        "public".to_string(),
        "users".to_string(),
        "public".to_string(),
        "users".to_string(),
        "postgres".to_string(),
        Some("pg_default".to_string()),
        vec![identity_column("id", 1, "integer")],
        vec![],
        vec![unique_email_index()],
        vec![],
        None,
    );

    let script = table.get_script();

    assert!(
        script.contains("create unique index idx_users_email on public.users using btree (email);")
    );
}

#[test]
fn test_get_script_identity_column_not_serial() {
    let table = Table::new(
        "public".to_string(),
        "users".to_string(),
        "public".to_string(),
        "users".to_string(),
        "postgres".to_string(),
        Some("pg_default".to_string()),
        vec![identity_column("id", 1, "integer")],
        vec![],
        vec![],
        vec![],
        None,
    );

    let script = table.get_script();
    assert!(script.contains("id integer generated BY DEFAULT as identity"));
    assert!(!script.contains("serial"));
}

#[test]
fn test_get_drop_script_returns_statement() {
    let table = basic_table();
    assert_eq!(
        table.get_drop_script(),
        "drop table if exists public.users;\n\n"
    );
}

#[test]
fn test_get_alter_script_handles_complex_differences() {
    let from_table = Table::new(
        "public".to_string(),
        "users".to_string(),
        "public".to_string(),
        "users".to_string(),
        "postgres".to_string(),
        Some("pg_default".to_string()),
        vec![
            identity_column("id", 1, "integer"),
            name_column(),
            legacy_column(),
        ],
        vec![
            primary_key_constraint(),
            check_constraint("users_name_check", "CHECK (name <> '')"),
            foreign_key_constraint(false, false),
            check_constraint("users_legacy_check", "CHECK (legacy IS NOT NULL)"),
        ],
        vec![
            primary_key_index(),
            name_index("create index idx_users_name on public.users using btree (name)"),
            legacy_index(),
        ],
        vec![
            trigger(
                "audit_user",
                "create trigger audit_user before insert on public.users for each row execute function log_user()",
                1,
            ),
            trigger(
                "cleanup_user",
                "create trigger cleanup_user after delete on public.users for each row execute function cleanup()",
                2,
            ),
        ],
        Some("create table public.users (...);".to_string()),
    );

    let to_table = Table::new(
        "public".to_string(),
        "users".to_string(),
        "public".to_string(),
        "users".to_string(),
        "postgres".to_string(),
        Some("pg_default".to_string()),
        vec![
            identity_column("id", 1, "integer"),
            name_column_with_default(),
            email_column(),
        ],
        vec![
            primary_key_constraint(),
            check_constraint("users_name_check", "CHECK (char_length(name) > 0)"),
            foreign_key_constraint(true, true),
            unique_constraint("users_email_unique", "UNIQUE (email)"),
        ],
        vec![
            primary_key_index(),
            name_index("create index idx_users_name on public.users using btree (lower(name))"),
            email_index(),
        ],
        vec![
            trigger(
                "audit_user",
                "create trigger audit_user after insert on public.users for each row execute function log_user_change()",
                3,
            ),
            trigger(
                "notify_user",
                "create trigger notify_user after insert on public.users for each row execute function notify()",
                4,
            ),
        ],
        Some("create table public.users (...);".to_string()),
    );

    let script = from_table.get_alter_script(&to_table, true);
    let fk_script = from_table.get_foreign_key_alter_script(&to_table);

    let expected_fragments = [
        "alter table public.users drop constraint users_name_check;\n",
        "alter table public.users drop constraint users_legacy_check;\n",
        "alter table public.users alter column name set default 'unknown'::text;\n",
        "alter table public.users add column email text;\n",
        "drop index if exists public.idx_users_name;\n",
        "drop index if exists public.idx_users_old;\n",
        "drop trigger if exists audit_user on public.users;\n",
        "drop trigger if exists cleanup_user on public.users;\n",
        "alter table public.users drop column legacy;\n",
        "alter table public.users add constraint users_name_check check (char_length(name) > 0) ;\n",
        "alter table public.users add constraint users_email_unique unique (email) ;\n",
        "create index idx_users_name on public.users using btree (lower(name));\n",
        "create index idx_users_email on public.users using btree (email);\n",
        "create trigger audit_user after insert on public.users for each row execute function log_user_change();\n",
        "create trigger notify_user after insert on public.users for each row execute function notify();\n",
    ];

    let mut last_position = 0usize;
    for fragment in expected_fragments {
        let position = script
            .find(fragment)
            .unwrap_or_else(|| panic!("fragment not found: {fragment}"));
        assert!(
            position >= last_position,
            "fragment `{fragment}` appears out of order"
        );
        last_position = position;
    }

    assert!(script.contains("'unknown'::text"));
    assert!(script.contains("lower(name)"));
    assert!(script.contains("notify_user"));

    assert!(fk_script.contains("alter table public.users alter constraint users_account_fk deferrable initially deferred;\n"));
}

#[test]
fn test_get_alter_script_handles_policy_changes() {
    let mut from_table = basic_table();
    let mut to_table = basic_table();

    to_table.policies = vec![policy(
        "users_tenant_insert",
        "insert",
        None,
        Some("tenant_id = current_setting('app.current_tenant')::int"),
    )];
    to_table.has_rowsecurity = true;

    let add_script = from_table.get_alter_script(&to_table, true);
    assert!(add_script.contains("create policy users_tenant_insert"));
    assert!(add_script.contains("enable row level security"));

    from_table = to_table.clone();
    let to_table_no_policy = basic_table();
    let drop_script = from_table.get_alter_script(&to_table_no_policy, true);
    assert!(drop_script.contains("drop policy if exists users_tenant_insert"));
    assert!(drop_script.contains("disable row level security"));
}

#[test]
fn test_get_foreign_key_script() {
    let table = Table::new(
        "public".to_string(),
        "users".to_string(),
        "public".to_string(),
        "users".to_string(),
        "postgres".to_string(),
        Some("pg_default".to_string()),
        vec![identity_column("id", 1, "integer")],
        vec![
            primary_key_constraint(),
            check_constraint("users_name_check", "CHECK (name <> '')"),
            foreign_key_constraint(false, false),
        ],
        vec![],
        vec![],
        None,
    );

    let script = table.get_foreign_key_script();

    assert!(script.contains("alter table public.users add constraint users_account_fk foreign key (account_id) references public.accounts(id)"));
    assert!(!script.contains("users_name_check"));
    assert!(!script.contains("users_pkey"));
}

fn custom_foreign_key_constraint(name: &str, definition: &str) -> TableConstraint {
    TableConstraint {
        catalog: "postgres".to_string(),
        schema: "public".to_string(),
        name: name.to_string(),
        table_name: "users".to_string(),
        constraint_type: "FOREIGN KEY".to_string(),
        is_deferrable: false,
        initially_deferred: false,
        definition: Some(definition.to_string()),
        coninhcount: 0,
        is_enforced: true,
        no_inherit: false,
        nulls_not_distinct: false,
        comment: None,
    }
}

#[test]
fn test_get_foreign_key_alter_script_add_new_fk() {
    let from_table = Table::new(
        "public".to_string(),
        "users".to_string(),
        "public".to_string(),
        "users".to_string(),
        "postgres".to_string(),
        None,
        vec![],
        vec![],
        vec![],
        vec![],
        None,
    );

    let to_table = Table::new(
        "public".to_string(),
        "users".to_string(),
        "public".to_string(),
        "users".to_string(),
        "postgres".to_string(),
        None,
        vec![],
        vec![custom_foreign_key_constraint(
            "fk_new",
            "FOREIGN KEY (col) REFERENCES other(id)",
        )],
        vec![],
        vec![],
        None,
    );

    let script = from_table.get_foreign_key_alter_script(&to_table);
    assert!(script.contains(
        "alter table public.users add constraint fk_new foreign key (col) references other(id)"
    ));
}

#[test]
fn test_get_foreign_key_alter_script_drop_fk() {
    let from_table = Table::new(
        "public".to_string(),
        "users".to_string(),
        "public".to_string(),
        "users".to_string(),
        "postgres".to_string(),
        None,
        vec![],
        vec![custom_foreign_key_constraint(
            "fk_old",
            "FOREIGN KEY (col) REFERENCES other(id)",
        )],
        vec![],
        vec![],
        None,
    );

    let to_table = Table::new(
        "public".to_string(),
        "users".to_string(),
        "public".to_string(),
        "users".to_string(),
        "postgres".to_string(),
        None,
        vec![],
        vec![],
        vec![],
        vec![],
        None,
    );

    let script = from_table.get_foreign_key_alter_script(&to_table);
    assert_eq!(script, ""); // Should be empty as drop is handled in get_alter_script
}

#[test]
fn test_get_foreign_key_alter_script_recreate_fk() {
    let from_table = Table::new(
        "public".to_string(),
        "users".to_string(),
        "public".to_string(),
        "users".to_string(),
        "postgres".to_string(),
        None,
        vec![],
        vec![custom_foreign_key_constraint(
            "fk_change",
            "FOREIGN KEY (col) REFERENCES table_a(id)",
        )],
        vec![],
        vec![],
        None,
    );

    let to_table = Table::new(
        "public".to_string(),
        "users".to_string(),
        "public".to_string(),
        "users".to_string(),
        "postgres".to_string(),
        None,
        vec![],
        vec![custom_foreign_key_constraint(
            "fk_change",
            "FOREIGN KEY (col) REFERENCES table_b(id)",
        )],
        vec![],
        vec![],
        None,
    );

    let script = from_table.get_foreign_key_alter_script(&to_table);
    // Should contain the add constraint part. Drop is elsewhere.
    assert!(script.contains(
        "alter table public.users add constraint fk_change foreign key (col) references table_b(id)"
    ));
}

#[test]
fn test_get_foreign_key_alter_script_no_change() {
    let fk = custom_foreign_key_constraint("fk_same", "FOREIGN KEY (col) REFERENCES other(id)");

    let from_table = Table::new(
        "public".to_string(),
        "users".to_string(),
        "public".to_string(),
        "users".to_string(),
        "postgres".to_string(),
        None,
        vec![],
        vec![fk.clone()],
        vec![],
        vec![],
        None,
    );

    let to_table = Table::new(
        "public".to_string(),
        "users".to_string(),
        "public".to_string(),
        "users".to_string(),
        "postgres".to_string(),
        None,
        vec![],
        vec![fk],
        vec![],
        vec![],
        None,
    );

    let script = from_table.get_foreign_key_alter_script(&to_table);
    assert_eq!(script, "");
}

#[test]
fn test_foreign_key_full_lifecycle_workflow() {
    // 1. Drop FK (exists in from, not in to)
    let fk_drop_from = Table::new(
        "public".to_string(),
        "users".to_string(),
        "public".to_string(),
        "users".to_string(),
        "postgres".to_string(),
        None,
        vec![],
        vec![custom_foreign_key_constraint(
            "fk_drop",
            "FOREIGN KEY (col) REFERENCES other(id)",
        )],
        vec![],
        vec![],
        None,
    );
    let fk_drop_to = Table::new(
        "public".to_string(),
        "users".to_string(),
        "public".to_string(),
        "users".to_string(),
        "postgres".to_string(),
        None,
        vec![],
        vec![],
        vec![],
        vec![],
        None,
    );

    let drop_main_script = fk_drop_from.get_alter_script(&fk_drop_to, true);
    let drop_fk_script = fk_drop_from.get_foreign_key_alter_script(&fk_drop_to);

    assert!(drop_main_script.contains("alter table public.users drop constraint fk_drop;"));
    assert_eq!(drop_fk_script, "");

    // 2. Add FK (not in from, exists in to)
    let fk_add_from = fk_drop_to.clone();
    let fk_add_to = fk_drop_from.clone(); // reusing table with FK

    let add_main_script = fk_add_from.get_alter_script(&fk_add_to, true);
    let add_fk_script = fk_add_from.get_foreign_key_alter_script(&fk_add_to);

    assert!(!add_main_script.contains("fk_drop")); // Main script shouldn't touch new FKs
    assert!(add_fk_script.contains(
        "alter table public.users add constraint fk_drop foreign key (col) references other(id)"
    ));

    // 3. Recreate FK (definition change)
    let fk_change_from = Table::new(
        "public".to_string(),
        "users".to_string(),
        "public".to_string(),
        "users".to_string(),
        "postgres".to_string(),
        None,
        vec![],
        vec![custom_foreign_key_constraint(
            "fk_change",
            "FOREIGN KEY (col) REFERENCES old_table(id)",
        )],
        vec![],
        vec![],
        None,
    );
    let fk_change_to = Table::new(
        "public".to_string(),
        "users".to_string(),
        "public".to_string(),
        "users".to_string(),
        "postgres".to_string(),
        None,
        vec![],
        vec![custom_foreign_key_constraint(
            "fk_change",
            "FOREIGN KEY (col) REFERENCES new_table(id)",
        )],
        vec![],
        vec![],
        None,
    );

    let change_main_script = fk_change_from.get_alter_script(&fk_change_to, true);
    let change_fk_script = fk_change_from.get_foreign_key_alter_script(&fk_change_to);

    assert!(change_main_script.contains("alter table public.users drop constraint fk_change;"));
    assert!(change_fk_script.contains("alter table public.users add constraint fk_change foreign key (col) references new_table(id)"));
}

fn create_dummy_column(name: &str, data_type: &str) -> TableColumn {
    TableColumn {
        name: name.to_string(),
        data_type: data_type.to_string(),
        is_nullable: true,
        ordinal_position: 1,
        catalog: "".to_string(),
        schema: "".to_string(),
        table: "".to_string(),
        column_default: None,
        character_maximum_length: None,
        character_octet_length: None,
        numeric_precision: None,
        numeric_precision_radix: None,
        numeric_scale: None,
        datetime_precision: None,
        interval_type: None,
        interval_precision: None,
        character_set_catalog: None,
        character_set_schema: None,
        character_set_name: None,
        collation_catalog: None,
        collation_schema: None,
        collation_name: None,
        domain_catalog: None,
        domain_schema: None,
        domain_name: None,
        udt_catalog: None,
        udt_schema: None,
        udt_name: None,
        scope_catalog: None,
        scope_schema: None,
        scope_name: None,
        maximum_cardinality: None,
        dtd_identifier: None,
        is_self_referencing: false,
        is_identity: false,
        identity_generation: None,
        identity_start: None,
        identity_increment: None,
        identity_maximum: None,
        identity_minimum: None,
        identity_cycle: false,
        is_generated: "".to_string(),
        generation_expression: None,
        generation_type: None,
        is_updatable: true,
        related_views: None,
        comment: None,
        storage: None,
        compression: None,
        statistics_target: None,
        acl: vec![],
        serial_type: None,
    }
}

#[test]
fn test_partitioned_table_script() {
    let mut table = Table::new(
        "data".to_string(),
        "test".to_string(),
        "data".to_string(),
        "test".to_string(),
        "owner".to_string(),
        None,
        vec![
            create_dummy_column("id", "bigint"),
            create_dummy_column("flow_id", "varchar"),
        ],
        vec![],
        vec![],
        vec![],
        None,
    );
    table.partition_key = Some("LIST (flow_id)".to_string());

    let script = table.get_script();
    assert!(script.contains("create table data.test"));
    assert!(script.contains("partition by LIST (flow_id)"));
}

#[test]
fn test_partition_child_script() {
    let mut table = Table::new(
        "data".to_string(),
        "test_default".to_string(),
        "data".to_string(),
        "test_default".to_string(),
        "owner".to_string(),
        None,
        vec![],
        vec![],
        vec![],
        vec![],
        None,
    );
    table.partition_of = Some("data.test".to_string());
    table.partition_bound = Some("DEFAULT".to_string());

    let script = table.get_script();
    assert!(script.contains("create table data.test_default partition of data.test"));
    assert!(script.contains("DEFAULT"));
}

#[test]
fn test_sub_partition_script() {
    // A sub-partition is both a child of a partitioned table AND itself partitioned.
    let mut table = Table::new(
        "data".to_string(),
        "test_2023".to_string(),
        "data".to_string(),
        "test_2023".to_string(),
        "owner".to_string(),
        None,
        vec![create_dummy_column("id", "bigint")],
        vec![],
        vec![],
        vec![],
        None,
    );
    table.partition_of = Some("data.test".to_string());
    table.partition_bound = Some("FOR VALUES FROM (2023) TO (2024)".to_string());
    table.partition_key = Some("LIST (id)".to_string());

    let script = table.get_script();
    assert!(
        script.contains("create table data.test_2023 partition of data.test"),
        "should reference parent table"
    );
    assert!(
        script.contains("FOR VALUES FROM (2023) TO (2024)"),
        "should contain partition bound"
    );
    assert!(
        script.contains("partition by LIST (id)"),
        "should contain sub-partition key"
    );
}

#[test]
fn test_partition_child_with_tablespace() {
    let mut table = Table::new(
        "data".to_string(),
        "test_default".to_string(),
        "data".to_string(),
        "test_default".to_string(),
        "owner".to_string(),
        Some("fast_ssd".to_string()),
        vec![],
        vec![],
        vec![],
        vec![],
        None,
    );
    table.partition_of = Some("data.test".to_string());
    table.partition_bound = Some("DEFAULT".to_string());

    let script = table.get_script();
    assert!(
        script.contains("tablespace \"fast_ssd\""),
        "partition create should include tablespace"
    );
}

#[test]
fn test_regular_table_with_tablespace() {
    let table = Table::new(
        "data".to_string(),
        "test".to_string(),
        "data".to_string(),
        "test".to_string(),
        "owner".to_string(),
        Some("fast_ssd".to_string()),
        vec![create_dummy_column("id", "bigint")],
        vec![],
        vec![],
        vec![],
        None,
    );

    let script = table.get_script();
    assert!(
        script.contains("tablespace \"fast_ssd\""),
        "regular table create should include tablespace"
    );
}

#[test]
fn test_get_alter_script_tablespace_change() {
    let from_table = Table::new(
        "data".to_string(),
        "test".to_string(),
        "data".to_string(),
        "test".to_string(),
        "owner".to_string(),
        Some("old_space".to_string()),
        vec![],
        vec![],
        vec![],
        vec![],
        None,
    );

    let to_table = Table::new(
        "data".to_string(),
        "test".to_string(),
        "data".to_string(),
        "test".to_string(),
        "owner".to_string(),
        Some("new_space".to_string()),
        vec![],
        vec![],
        vec![],
        vec![],
        None,
    );

    let script = from_table.get_alter_script(&to_table, true);
    assert!(
        script.contains("set tablespace \"new_space\""),
        "alter script should set new tablespace"
    );
}

#[test]
fn test_get_alter_script_partition_bound_change() {
    let mut from_table = Table::new(
        "data".to_string(),
        "test_default".to_string(),
        "data".to_string(),
        "test_default".to_string(),
        "owner".to_string(),
        None,
        vec![],
        vec![],
        vec![],
        vec![],
        None,
    );
    from_table.partition_of = Some("\"data\".\"test\"".to_string());
    from_table.partition_bound = Some("FOR VALUES IN (1)".to_string());

    let mut to_table = Table::new(
        "data".to_string(),
        "test_default".to_string(),
        "data".to_string(),
        "test_default".to_string(),
        "owner".to_string(),
        None,
        vec![],
        vec![],
        vec![],
        vec![],
        None,
    );
    to_table.partition_of = Some("\"data\".\"test\"".to_string());
    to_table.partition_bound = Some("FOR VALUES IN (2)".to_string());

    let script = from_table.get_alter_script(&to_table, true);

    assert!(script.contains("detach partition"));
    assert!(script.contains("attach partition"));
}

#[test]
fn test_get_alter_script_partition_key_change() {
    let mut from_table = Table::new(
        "data".to_string(),
        "test".to_string(),
        "data".to_string(),
        "test".to_string(),
        "owner".to_string(),
        None,
        vec![],
        vec![],
        vec![],
        vec![],
        None,
    );
    from_table.partition_key = Some("LIST (id)".to_string());

    let mut to_table = Table::new(
        "data".to_string(),
        "test".to_string(),
        "data".to_string(),
        "test".to_string(),
        "owner".to_string(),
        None,
        vec![],
        vec![],
        vec![],
        vec![],
        None,
    );
    to_table.partition_key = Some("LIST (flow_id)".to_string());

    let script = from_table.get_alter_script(&to_table, true);

    assert!(script.contains("Partition key changed"));
    assert!(script.contains("drop table"));
    assert!(script.contains("create table"));
}

#[test]
fn test_get_alter_script_partition_key_change_no_drop() {
    let mut from_table = Table::new(
        "data".to_string(),
        "test".to_string(),
        "data".to_string(),
        "test".to_string(),
        "owner".to_string(),
        None,
        vec![],
        vec![],
        vec![],
        vec![],
        None,
    );
    from_table.partition_key = Some("LIST (id)".to_string());

    let mut to_table = Table::new(
        "data".to_string(),
        "test".to_string(),
        "data".to_string(),
        "test".to_string(),
        "owner".to_string(),
        None,
        vec![],
        vec![],
        vec![],
        vec![],
        None,
    );
    to_table.partition_key = Some("LIST (flow_id)".to_string());

    let script = from_table.get_alter_script(&to_table, false);

    assert!(script.contains("Partition key changed"));
    assert!(script.contains("-- drop table"));
    assert!(script.contains("create table"));
}

#[test]
fn test_get_alter_script_detach_partition() {
    let mut from_table = Table::new(
        "data".to_string(),
        "test_default".to_string(),
        "data".to_string(),
        "test_default".to_string(),
        "owner".to_string(),
        None,
        vec![],
        vec![],
        vec![],
        vec![],
        None,
    );
    from_table.partition_of = Some("data.test".to_string());
    from_table.partition_bound = Some("DEFAULT".to_string());

    let to_table = Table::new(
        "data".to_string(),
        "test_default".to_string(),
        "data".to_string(),
        "test_default".to_string(),
        "owner".to_string(),
        None,
        vec![],
        vec![],
        vec![],
        vec![],
        None,
    );
    // to_table has no partition info, so it's a standalone table

    let script = from_table.get_alter_script(&to_table, true);

    assert!(script.contains("alter table data.test detach partition data.test_default;"));
    assert!(!script.contains("attach partition"));
}

#[test]
fn test_get_alter_script_attach_partition() {
    let from_table = Table::new(
        "data".to_string(),
        "test_default".to_string(),
        "data".to_string(),
        "test_default".to_string(),
        "owner".to_string(),
        None,
        vec![],
        vec![],
        vec![],
        vec![],
        None,
    );
    // from_table is standalone

    let mut to_table = Table::new(
        "data".to_string(),
        "test_default".to_string(),
        "data".to_string(),
        "test_default".to_string(),
        "owner".to_string(),
        None,
        vec![],
        vec![],
        vec![],
        vec![],
        None,
    );
    to_table.partition_of = Some("data.test".to_string());
    to_table.partition_bound = Some("DEFAULT".to_string());

    let script = from_table.get_alter_script(&to_table, true);

    assert!(!script.contains("detach partition"));
    assert!(script.contains("alter table data.test attach partition data.test_default DEFAULT;"));
}

// --- Helper for building a partition child table ---
fn partition_child_table(
    columns: Vec<TableColumn>,
    constraints: Vec<TableConstraint>,
    indexes: Vec<TableIndex>,
) -> Table {
    let mut table = Table::new(
        "public".to_string(),
        "users_p1".to_string(),
        "public".to_string(),
        "users_p1".to_string(),
        "postgres".to_string(),
        None,
        columns,
        constraints,
        indexes,
        vec![],
        None,
    );
    table.partition_of = Some("public.users".to_string());
    table.partition_bound = Some("FOR VALUES FROM (1) TO (100)".to_string());
    table.hash();
    table
}

fn partition_child_column(name: &str, ordinal_position: i32) -> TableColumn {
    let mut col = base_column(name, ordinal_position);
    col.table = "users_p1".to_string();
    col
}

fn partition_child_column_not_null(name: &str, ordinal_position: i32) -> TableColumn {
    let mut col = partition_child_column(name, ordinal_position);
    col.is_nullable = false;
    col
}

fn partition_child_identity_column(
    name: &str,
    ordinal_position: i32,
    data_type: &str,
) -> TableColumn {
    let mut col = partition_child_column(name, ordinal_position);
    col.data_type = data_type.to_string();
    col.is_identity = true;
    col.is_nullable = false;
    col.identity_generation = Some("BY DEFAULT".to_string());
    col
}

fn partition_child_constraint(
    name: &str,
    constraint_type: &str,
    definition: Option<&str>,
) -> TableConstraint {
    TableConstraint {
        catalog: "postgres".to_string(),
        schema: "public".to_string(),
        name: name.to_string(),
        table_name: "users_p1".to_string(),
        constraint_type: constraint_type.to_string(),
        is_deferrable: false,
        initially_deferred: false,
        definition: definition.map(|d| d.to_string()),
        coninhcount: 1,
        is_enforced: true,
        no_inherit: false,
        nulls_not_distinct: false,
        comment: None,
    }
}

// --- Partition child: column ADD is skipped ---
#[test]
fn test_partition_child_skips_add_column() {
    let from = partition_child_table(
        vec![partition_child_identity_column("id", 1, "integer")],
        vec![],
        vec![],
    );
    let mut to = partition_child_table(
        vec![
            partition_child_identity_column("id", 1, "integer"),
            partition_child_column("email", 2),
        ],
        vec![],
        vec![],
    );
    to.hash();

    let script = from.get_alter_script(&to, true);
    assert!(
        !script.contains("add column"),
        "ADD COLUMN must not appear for partition child, got: {script}"
    );
}

// --- Partition child: column DROP is skipped ---
#[test]
fn test_partition_child_skips_drop_column() {
    let from = partition_child_table(
        vec![
            partition_child_identity_column("id", 1, "integer"),
            partition_child_column("legacy", 2),
        ],
        vec![],
        vec![],
    );
    let to = partition_child_table(
        vec![partition_child_identity_column("id", 1, "integer")],
        vec![],
        vec![],
    );

    let script = from.get_alter_script(&to, true);
    assert!(
        !script.contains("drop column"),
        "DROP COLUMN must not appear for partition child, got: {script}"
    );
}

// --- Partition child: SET NOT NULL / DROP NOT NULL is skipped ---
#[test]
fn test_partition_child_skips_set_not_null() {
    let from = partition_child_table(
        vec![
            partition_child_identity_column("id", 1, "integer"),
            partition_child_column("name", 2), // nullable
        ],
        vec![],
        vec![],
    );
    let to = partition_child_table(
        vec![
            partition_child_identity_column("id", 1, "integer"),
            partition_child_column_not_null("name", 2), // not null
        ],
        vec![],
        vec![],
    );

    let script = from.get_alter_script(&to, true);
    assert!(
        !script.contains("set not null"),
        "SET NOT NULL must not appear for partition child, got: {script}"
    );
}

#[test]
fn test_partition_child_skips_drop_not_null() {
    let from = partition_child_table(
        vec![
            partition_child_identity_column("id", 1, "integer"),
            partition_child_column_not_null("name", 2), // not null
        ],
        vec![],
        vec![],
    );
    let to = partition_child_table(
        vec![
            partition_child_identity_column("id", 1, "integer"),
            partition_child_column("name", 2), // nullable
        ],
        vec![],
        vec![],
    );

    let script = from.get_alter_script(&to, true);
    assert!(
        !script.contains("drop not null"),
        "DROP NOT NULL must not appear for partition child, got: {script}"
    );
}

// --- Partition child: SET DEFAULT / DROP DEFAULT is skipped ---
#[test]
fn test_partition_child_skips_set_default() {
    let from = partition_child_table(
        vec![
            partition_child_identity_column("id", 1, "integer"),
            partition_child_column("name", 2),
        ],
        vec![],
        vec![],
    );

    let mut name_with_default = partition_child_column("name", 2);
    name_with_default.column_default = Some("'unknown'::text".to_string());
    let to = partition_child_table(
        vec![
            partition_child_identity_column("id", 1, "integer"),
            name_with_default,
        ],
        vec![],
        vec![],
    );

    let script = from.get_alter_script(&to, true);
    assert!(
        !script.contains("set default"),
        "SET DEFAULT must not appear for partition child, got: {script}"
    );
}

#[test]
fn test_partition_child_skips_drop_default() {
    let mut name_with_default = partition_child_column("name", 2);
    name_with_default.column_default = Some("'unknown'::text".to_string());
    let from = partition_child_table(
        vec![
            partition_child_identity_column("id", 1, "integer"),
            name_with_default,
        ],
        vec![],
        vec![],
    );
    let to = partition_child_table(
        vec![
            partition_child_identity_column("id", 1, "integer"),
            partition_child_column("name", 2),
        ],
        vec![],
        vec![],
    );

    let script = from.get_alter_script(&to, true);
    assert!(
        !script.contains("drop default"),
        "DROP DEFAULT must not appear for partition child, got: {script}"
    );
}

// --- Partition child: non-FK constraint add/drop/alter is skipped ---
#[test]
fn test_partition_child_skips_add_check_constraint() {
    let from = partition_child_table(
        vec![partition_child_identity_column("id", 1, "integer")],
        vec![],
        vec![],
    );
    let to = partition_child_table(
        vec![partition_child_identity_column("id", 1, "integer")],
        vec![partition_child_constraint(
            "users_p1_name_check",
            "CHECK",
            Some("CHECK (name <> '')"),
        )],
        vec![],
    );

    let script = from.get_alter_script(&to, true);
    assert!(
        !script.contains("add constraint"),
        "ADD CONSTRAINT (CHECK) must not appear for partition child, got: {script}"
    );
}

#[test]
fn test_partition_child_skips_drop_check_constraint() {
    let from = partition_child_table(
        vec![partition_child_identity_column("id", 1, "integer")],
        vec![partition_child_constraint(
            "users_p1_name_check",
            "CHECK",
            Some("CHECK (name <> '')"),
        )],
        vec![],
    );
    let to = partition_child_table(
        vec![partition_child_identity_column("id", 1, "integer")],
        vec![],
        vec![],
    );

    let script = from.get_alter_script(&to, true);
    assert!(
        !script.contains("drop constraint"),
        "DROP CONSTRAINT (CHECK) must not appear for partition child, got: {script}"
    );
}

#[test]
fn test_partition_child_skips_primary_key_constraint() {
    let from = partition_child_table(
        vec![partition_child_identity_column("id", 1, "integer")],
        vec![],
        vec![],
    );
    let to = partition_child_table(
        vec![partition_child_identity_column("id", 1, "integer")],
        vec![partition_child_constraint(
            "users_p1_pkey",
            "PRIMARY KEY",
            None,
        )],
        vec![],
    );

    let script = from.get_alter_script(&to, true);
    assert!(
        !script.contains("add constraint"),
        "ADD CONSTRAINT (PK) must not appear for partition child, got: {script}"
    );
}

// --- Partition child: FK constraints are still emitted ---
#[test]
fn test_partition_child_still_emits_fk_changes() {
    let from = partition_child_table(
        vec![partition_child_identity_column("id", 1, "integer")],
        vec![],
        vec![],
    );
    let fk = TableConstraint {
        catalog: "postgres".to_string(),
        schema: "public".to_string(),
        name: "users_p1_account_fk".to_string(),
        table_name: "users_p1".to_string(),
        constraint_type: "FOREIGN KEY".to_string(),
        is_deferrable: false,
        initially_deferred: false,
        definition: Some("FOREIGN KEY (account_id) REFERENCES public.accounts(id)".to_string()),
        coninhcount: 0,
        is_enforced: true,
        no_inherit: false,
        nulls_not_distinct: false,
        comment: None,
    };
    let to = partition_child_table(
        vec![partition_child_identity_column("id", 1, "integer")],
        vec![fk],
        vec![],
    );

    // FKs on partitions are handled by get_foreign_key_alter_script, not
    // build_alter_script, so they should NOT appear in the alter script
    // itself.  The key point is that the FK is not suppressed when the
    // caller asks for it via the foreign-key path.
    let fk_script = from.get_foreign_key_alter_script(&to);
    assert!(
        fk_script.contains("users_p1_account_fk"),
        "FK constraint must still be emitted for partition child"
    );
}

// --- Partition child: column comment changes ARE emitted ---
#[test]
fn test_partition_child_emits_column_comment_change() {
    let mut col_from = partition_child_column("name", 2);
    col_from.comment = Some("old comment".to_string());
    let from = partition_child_table(
        vec![
            partition_child_identity_column("id", 1, "integer"),
            col_from,
        ],
        vec![],
        vec![],
    );

    let mut col_to = partition_child_column("name", 2);
    col_to.comment = Some("new comment".to_string());
    let to = partition_child_table(
        vec![partition_child_identity_column("id", 1, "integer"), col_to],
        vec![],
        vec![],
    );

    let script = from.get_alter_script(&to, true);
    assert!(
        script.contains("comment on column"),
        "Column comment change must be emitted for partition child, got: {script}"
    );
    assert!(
        script.contains("new comment"),
        "New comment text must appear, got: {script}"
    );
}

// --- Partition child: index changes ARE still emitted ---
#[test]
fn test_partition_child_still_emits_non_inherited_index_changes() {
    let idx = TableIndex {
        schema: "public".to_string(),
        table: "users_p1".to_string(),
        name: "idx_users_p1_name".to_string(),
        catalog: None,
        indexdef: "create index idx_users_p1_name on public.users_p1 using btree (name)"
            .to_string(),
        is_partition_index: false,
        comment: None,
    };
    let from = partition_child_table(
        vec![partition_child_identity_column("id", 1, "integer")],
        vec![],
        vec![],
    );
    let to = partition_child_table(
        vec![partition_child_identity_column("id", 1, "integer")],
        vec![],
        vec![idx],
    );

    let script = from.get_alter_script(&to, true);
    assert!(
        script.contains("create index idx_users_p1_name"),
        "Non-inherited index on partition child must still be emitted, got: {script}"
    );
}

// --- Non-partitioned table: all changes are still emitted ---
#[test]
fn test_non_partition_table_emits_all_column_changes() {
    let from = basic_table();
    let mut to = basic_table();
    // Add a new column
    to.columns.push(email_column());
    to.hash();

    let script = from.get_alter_script(&to, true);
    assert!(
        script.contains("add column email"),
        "ADD COLUMN must appear for non-partitioned table, got: {script}"
    );
}

// --- Partition child: combined add + alter + drop all skipped ---
#[test]
fn test_partition_child_skips_combined_column_changes() {
    // From has: id, legacy (nullable)
    // To has: id, name (not null), email (new)
    // This means: legacy dropped, name added, no alter on id
    let from = partition_child_table(
        vec![
            partition_child_identity_column("id", 1, "integer"),
            partition_child_column("legacy", 2),
        ],
        vec![partition_child_constraint(
            "users_p1_check",
            "CHECK",
            Some("CHECK (legacy IS NOT NULL)"),
        )],
        vec![],
    );
    let to = partition_child_table(
        vec![
            partition_child_identity_column("id", 1, "integer"),
            partition_child_column_not_null("name", 2),
            partition_child_column("email", 3),
        ],
        vec![partition_child_constraint(
            "users_p1_name_check",
            "CHECK",
            Some("CHECK (name <> '')"),
        )],
        vec![],
    );

    let script = from.get_alter_script(&to, true);
    assert!(
        !script.contains("add column"),
        "ADD COLUMN must not appear, got: {script}"
    );
    assert!(
        !script.contains("drop column"),
        "DROP COLUMN must not appear, got: {script}"
    );
    assert!(
        !script.contains("add constraint"),
        "ADD CONSTRAINT must not appear, got: {script}"
    );
    assert!(
        !script.contains("drop constraint"),
        "DROP CONSTRAINT must not appear, got: {script}"
    );
}

// Helper: local partition constraint (coninhcount = 0)
fn partition_child_local_constraint(
    name: &str,
    constraint_type: &str,
    definition: Option<&str>,
) -> TableConstraint {
    TableConstraint {
        catalog: "postgres".to_string(),
        schema: "public".to_string(),
        name: name.to_string(),
        table_name: "users_p1".to_string(),
        constraint_type: constraint_type.to_string(),
        is_deferrable: false,
        initially_deferred: false,
        definition: definition.map(|d| d.to_string()),
        coninhcount: 0,
        is_enforced: true,
        no_inherit: false,
        nulls_not_distinct: false,
        comment: None,
    }
}

// --- Partition child: local (non-inherited) constraint add IS emitted ---
#[test]
fn test_partition_child_emits_add_local_check_constraint() {
    let from = partition_child_table(
        vec![partition_child_identity_column("id", 1, "integer")],
        vec![],
        vec![],
    );
    let to = partition_child_table(
        vec![partition_child_identity_column("id", 1, "integer")],
        vec![partition_child_local_constraint(
            "users_p1_local_chk",
            "CHECK",
            Some("CHECK (id > 0)"),
        )],
        vec![],
    );

    let script = from.get_alter_script(&to, true);
    assert!(
        script.contains("add constraint users_p1_local_chk"),
        "Local CHECK constraint must be emitted for partition child, got: {script}"
    );
}

// --- Partition child: local constraint drop IS emitted ---
#[test]
fn test_partition_child_emits_drop_local_check_constraint() {
    let from = partition_child_table(
        vec![partition_child_identity_column("id", 1, "integer")],
        vec![partition_child_local_constraint(
            "users_p1_local_chk",
            "CHECK",
            Some("CHECK (id > 0)"),
        )],
        vec![],
    );
    let to = partition_child_table(
        vec![partition_child_identity_column("id", 1, "integer")],
        vec![],
        vec![],
    );

    let script = from.get_alter_script(&to, true);
    assert!(
        script.contains("drop constraint users_p1_local_chk"),
        "Local CHECK constraint drop must be emitted for partition child, got: {script}"
    );
}

// --- Partition child: local constraint modification IS emitted ---
#[test]
fn test_partition_child_emits_alter_local_check_constraint() {
    let from = partition_child_table(
        vec![partition_child_identity_column("id", 1, "integer")],
        vec![partition_child_local_constraint(
            "users_p1_local_chk",
            "CHECK",
            Some("CHECK (id > 0)"),
        )],
        vec![],
    );
    let to = partition_child_table(
        vec![partition_child_identity_column("id", 1, "integer")],
        vec![partition_child_local_constraint(
            "users_p1_local_chk",
            "CHECK",
            Some("CHECK (id > 100)"),
        )],
        vec![],
    );

    let script = from.get_alter_script(&to, true);
    assert!(
        script.contains("drop constraint users_p1_local_chk"),
        "Local CHECK modification must emit drop, got: {script}"
    );
    assert!(
        script.contains("add constraint users_p1_local_chk"),
        "Local CHECK modification must emit add, got: {script}"
    );
}

// --- Partition child: inherited constraint skipped, local constraint emitted in same table ---
#[test]
fn test_partition_child_mixed_inherited_and_local_constraints() {
    let from = partition_child_table(
        vec![partition_child_identity_column("id", 1, "integer")],
        vec![partition_child_constraint(
            "users_p1_name_check",
            "CHECK",
            Some("CHECK (name <> '')"),
        )],
        vec![],
    );
    let to = partition_child_table(
        vec![partition_child_identity_column("id", 1, "integer")],
        vec![
            partition_child_constraint(
                "users_p1_name_check",
                "CHECK",
                Some("CHECK (char_length(name) > 0)"),
            ),
            partition_child_local_constraint("users_p1_local_chk", "CHECK", Some("CHECK (id > 0)")),
        ],
        vec![],
    );

    let script = from.get_alter_script(&to, true);
    // Inherited constraint change should be suppressed
    assert!(
        !script.contains("users_p1_name_check"),
        "Inherited constraint must be suppressed, got: {script}"
    );
    // Local constraint addition should be emitted
    assert!(
        script.contains("add constraint users_p1_local_chk"),
        "Local constraint must be emitted, got: {script}"
    );
}

// --- Partition key substring false-positive regression ---
#[test]
fn test_partitioned_parent_non_key_col_type_change_uses_alter() {
    // Parent table partitioned by expense_date.
    // Changing `amount` numeric(10,2) → numeric(15,4) must NOT trigger
    // DROP+CREATE because `amount` is not in the partition key.
    // This exercises the extract_partition_key_identifiers path
    // (is_target_partition == false, in_partition_key must be false).
    let schema = "pt_test";
    let table_name = "s6_issue2_expenses";

    let mut col_amount_old = create_dummy_column("amount", "numeric");
    col_amount_old.schema = schema.to_string();
    col_amount_old.table = table_name.to_string();
    col_amount_old.numeric_precision = Some(10);
    col_amount_old.numeric_scale = Some(2);
    col_amount_old.ordinal_position = 3;

    let mut col_amount_new = create_dummy_column("amount", "numeric");
    col_amount_new.schema = schema.to_string();
    col_amount_new.table = table_name.to_string();
    col_amount_new.numeric_precision = Some(15);
    col_amount_new.numeric_scale = Some(4);
    col_amount_new.ordinal_position = 3;

    let mut id_col = create_dummy_column("id", "bigint");
    id_col.schema = schema.to_string();
    id_col.table = table_name.to_string();
    id_col.ordinal_position = 1;

    let mut date_col_old = create_dummy_column("expense_date", "date");
    date_col_old.schema = schema.to_string();
    date_col_old.table = table_name.to_string();
    date_col_old.ordinal_position = 2;
    let date_col_new = date_col_old.clone();

    let mut from = Table::new(
        schema.to_string(),
        table_name.to_string(),
        schema.to_string(),
        table_name.to_string(),
        "postgres".to_string(),
        None,
        vec![id_col.clone(), date_col_old, col_amount_old],
        vec![],
        vec![],
        vec![],
        None,
    );
    from.partition_key = Some("range (expense_date)".to_string());
    from.hash();

    let mut to = Table::new(
        schema.to_string(),
        table_name.to_string(),
        schema.to_string(),
        table_name.to_string(),
        "postgres".to_string(),
        None,
        vec![id_col, date_col_new, col_amount_new],
        vec![],
        vec![],
        vec![],
        None,
    );
    to.partition_key = Some("range (expense_date)".to_string());
    to.hash();

    let script = from.get_alter_script(&to, true);
    assert!(
        !script.contains("drop table"),
        "Non-partition-key column type change must not trigger DROP TABLE, got: {script}"
    );
    assert!(
        !script.contains("Data loss"),
        "No data loss warning expected for non-partition-key column, got: {script}"
    );
    assert!(
        script.contains("alter"),
        "Should produce an ALTER statement for non-key column, got: {script}"
    );
}

#[test]
fn test_non_partition_key_column_type_change_uses_alter() {
    // Partition key references "category_id" but we change column "id".
    // The old substring check matched "id" inside "category_id" and
    // incorrectly triggered DROP + CREATE.
    let mut from_table = Table::new(
        "data".to_string(),
        "test".to_string(),
        "data".to_string(),
        "test".to_string(),
        "owner".to_string(),
        None,
        vec![
            create_dummy_column("id", "integer"),
            create_dummy_column("category_id", "integer"),
        ],
        vec![],
        vec![],
        vec![],
        None,
    );
    from_table.partition_key = Some("LIST (category_id)".to_string());

    let mut to_table = Table::new(
        "data".to_string(),
        "test".to_string(),
        "data".to_string(),
        "test".to_string(),
        "owner".to_string(),
        None,
        vec![
            create_dummy_column("id", "bigint"),
            create_dummy_column("category_id", "integer"),
        ],
        vec![],
        vec![],
        vec![],
        None,
    );
    to_table.partition_key = Some("LIST (category_id)".to_string());

    let script = from_table.get_alter_script(&to_table, true);
    assert!(
        !script.contains("drop table"),
        "Non-partition-key column type change must not trigger DROP TABLE, got: {script}"
    );
    assert!(
        !script.contains("Data loss"),
        "Non-partition-key column type change must not warn about data loss, got: {script}"
    );
    assert!(
        script.contains("alter"),
        "Should produce an ALTER statement, got: {script}"
    );
}

#[test]
fn test_partition_key_column_type_change_still_triggers_recreate() {
    // When the actual partition key column type changes, DROP + CREATE is correct.
    let mut from_table = Table::new(
        "data".to_string(),
        "test".to_string(),
        "data".to_string(),
        "test".to_string(),
        "owner".to_string(),
        None,
        vec![
            create_dummy_column("id", "integer"),
            create_dummy_column("category_id", "integer"),
        ],
        vec![],
        vec![],
        vec![],
        None,
    );
    from_table.partition_key = Some("LIST (category_id)".to_string());

    let mut to_table = Table::new(
        "data".to_string(),
        "test".to_string(),
        "data".to_string(),
        "test".to_string(),
        "owner".to_string(),
        None,
        vec![
            create_dummy_column("id", "integer"),
            create_dummy_column("category_id", "bigint"),
        ],
        vec![],
        vec![],
        vec![],
        None,
    );
    to_table.partition_key = Some("LIST (category_id)".to_string());

    let script = from_table.get_alter_script(&to_table, true);
    assert!(
        script.contains("drop table"),
        "Partition key column type change must trigger DROP TABLE, got: {script}"
    );
    assert!(
        script.contains("Data loss"),
        "Partition key column type change must warn about data loss, got: {script}"
    );
}

#[test]
fn test_extract_partition_key_single_unquoted() {
    let ids = extract_partition_key_identifiers("range (created_at)");
    assert_eq!(ids, vec!["created_at"]);
}

#[test]
fn test_extract_partition_key_multiple_unquoted() {
    let ids = extract_partition_key_identifiers("range (region, created_at)");
    assert_eq!(ids, vec!["region", "created_at"]);
}

#[test]
fn test_extract_partition_key_list_method() {
    let ids = extract_partition_key_identifiers("LIST (flow_id)");
    assert_eq!(ids, vec!["flow_id"]);
}

#[test]
fn test_extract_partition_key_hash_method() {
    let ids = extract_partition_key_identifiers("hash (id)");
    assert_eq!(ids, vec!["id"]);
}

#[test]
fn test_extract_partition_key_does_not_include_method() {
    // Column named "range" should NOT match the method keyword
    let ids = extract_partition_key_identifiers("range (created_at)");
    assert!(!ids.contains(&"range".to_string()));
}

#[test]
fn test_extract_partition_key_dollar_identifier() {
    let ids = extract_partition_key_identifiers("range (my$col)");
    assert_eq!(ids, vec!["my$col"]);
}

#[test]
fn test_extract_partition_key_quoted_identifier() {
    let ids = extract_partition_key_identifiers("list (\"My Column\")");
    assert_eq!(ids, vec!["my column"]);
}

#[test]
fn test_extract_partition_key_quoted_with_escaped_quote() {
    let ids = extract_partition_key_identifiers("list (\"a\"\"b\")");
    assert_eq!(ids, vec!["a\"b"]);
}

#[test]
fn test_extract_partition_key_mixed_quoted_and_unquoted() {
    let ids = extract_partition_key_identifiers("range (\"Region\", created_at)");
    assert_eq!(ids, vec!["region", "created_at"]);
}

#[test]
fn test_extract_partition_key_empty_parens() {
    let ids = extract_partition_key_identifiers("range ()");
    assert!(ids.is_empty());
}

#[test]
fn test_extract_partition_key_no_parens() {
    let ids = extract_partition_key_identifiers("range");
    assert!(ids.is_empty());
}

#[test]
fn test_extract_partition_key_column_named_list() {
    // A column actually named "list" inside the key should be extracted
    let ids = extract_partition_key_identifiers("range (list)");
    assert_eq!(ids, vec!["list"]);
}

// ---- PG18: WITHOUT OVERLAPS temporal constraint tests ----

#[test]
fn test_get_script_temporal_pk_without_overlaps() {
    let table = Table::new(
        "public".to_string(),
        "reservations".to_string(),
        "public".to_string(),
        "reservations".to_string(),
        "postgres".to_string(),
        None,
        vec![
            {
                let mut c = base_column("id", 1);
                c.data_type = "integer".to_string();
                c.is_nullable = false;
                c
            },
            {
                let mut c = base_column("valid_range", 2);
                c.table = "reservations".to_string();
                c.data_type = "tsrange".to_string();
                c.is_nullable = false;
                c
            },
        ],
        vec![TableConstraint {
            catalog: "postgres".to_string(),
            schema: "public".to_string(),
            name: "reservations_pkey".to_string(),
            table_name: "reservations".to_string(),
            constraint_type: "PRIMARY KEY".to_string(),
            is_deferrable: false,
            initially_deferred: false,
            definition: Some("PRIMARY KEY (id, valid_range WITHOUT OVERLAPS)".to_string()),
            coninhcount: 0,
            is_enforced: true,
            no_inherit: false,
            nulls_not_distinct: false,
            comment: None,
        }],
        vec![],
        vec![],
        None,
    );

    let script = table.get_script();
    assert!(
        script.contains("primary key (id, valid_range WITHOUT OVERLAPS)"),
        "expected WITHOUT OVERLAPS in PK definition: {script}"
    );
}

#[test]
fn test_get_script_temporal_pk_named_constraint_without_overlaps() {
    let table = Table::new(
        "public".to_string(),
        "bookings".to_string(),
        "public".to_string(),
        "bookings".to_string(),
        "postgres".to_string(),
        None,
        vec![
            {
                let mut c = base_column("room_id", 1);
                c.data_type = "integer".to_string();
                c.is_nullable = false;
                c
            },
            {
                let mut c = base_column("period", 2);
                c.table = "bookings".to_string();
                c.data_type = "tsrange".to_string();
                c.is_nullable = false;
                c
            },
        ],
        vec![TableConstraint {
            catalog: "postgres".to_string(),
            schema: "public".to_string(),
            name: "bookings_pk".to_string(),
            table_name: "bookings".to_string(),
            constraint_type: "PRIMARY KEY".to_string(),
            is_deferrable: false,
            initially_deferred: false,
            definition: Some("PRIMARY KEY (room_id, period WITHOUT OVERLAPS)".to_string()),
            coninhcount: 0,
            is_enforced: true,
            no_inherit: false,
            nulls_not_distinct: false,
            comment: None,
        }],
        vec![],
        vec![],
        None,
    );

    let script = table.get_script();
    assert!(
        script.contains("constraint bookings_pk primary key (room_id, period WITHOUT OVERLAPS)"),
        "expected named constraint with WITHOUT OVERLAPS: {script}"
    );
}

#[test]
fn test_get_script_not_enforced_constraint_in_table() {
    let table = Table::new(
        "public".to_string(),
        "orders".to_string(),
        "public".to_string(),
        "orders".to_string(),
        "postgres".to_string(),
        None,
        vec![
            {
                let mut c = base_column("id", 1);
                c.data_type = "integer".to_string();
                c.is_nullable = false;
                c
            },
            {
                let mut c = base_column("status", 2);
                c.data_type = "text".to_string();
                c
            },
        ],
        vec![TableConstraint {
            catalog: "postgres".to_string(),
            schema: "public".to_string(),
            name: "chk_status".to_string(),
            table_name: "orders".to_string(),
            constraint_type: "CHECK".to_string(),
            is_deferrable: false,
            initially_deferred: false,
            definition: Some("CHECK (status <> '')".to_string()),
            coninhcount: 0,
            is_enforced: false,
            no_inherit: false,
            nulls_not_distinct: false,
            comment: None,
        }],
        vec![],
        vec![],
        None,
    );

    let script = table.get_script();
    assert!(
        script.contains("not enforced"),
        "expected NOT ENFORCED check constraint in table script: {script}"
    );
}

#[test]
fn test_get_script_virtual_generated_column_in_table() {
    let table = Table::new(
        "public".to_string(),
        "products".to_string(),
        "public".to_string(),
        "products".to_string(),
        "postgres".to_string(),
        None,
        vec![
            {
                let mut c = base_column("price", 1);
                c.data_type = "numeric".to_string();
                c.is_nullable = false;
                c
            },
            {
                let mut c = base_column("qty", 2);
                c.data_type = "integer".to_string();
                c
            },
            {
                let mut c = base_column("total", 3);
                c.table = "products".to_string();
                c.data_type = "numeric".to_string();
                c.is_generated = "ALWAYS".to_string();
                c.generation_expression = Some("(price * qty)".to_string());
                c.generation_type = Some("v".to_string());
                c
            },
        ],
        vec![],
        vec![],
        vec![],
        None,
    );

    let script = table.get_script();
    assert!(
        script.contains("generated always as (price * qty) virtual"),
        "expected virtual generated column in table script: {script}"
    );
}

// --- Named NOT NULL constraint tests (PG18 contype='n') ---

fn not_null_constraint(name: &str, column: &str) -> TableConstraint {
    TableConstraint {
        catalog: "postgres".to_string(),
        schema: "public".to_string(),
        name: name.to_string(),
        table_name: "users".to_string(),
        constraint_type: "NOT NULL".to_string(),
        is_deferrable: false,
        initially_deferred: false,
        definition: Some(format!("NOT NULL {column}")),
        coninhcount: 0,
        is_enforced: true,
        no_inherit: false,
        nulls_not_distinct: false,
        comment: None,
    }
}

#[test]
fn test_named_not_null_constraint_emitted_in_column_def() {
    let table = Table::new(
        "public".to_string(),
        "users".to_string(),
        "public".to_string(),
        "users".to_string(),
        "postgres".to_string(),
        None,
        vec![identity_column("id", 1, "integer"), name_column()],
        vec![
            primary_key_constraint(),
            not_null_constraint("name_must_exist", "name"),
        ],
        vec![primary_key_index()],
        vec![],
        None,
    );

    let script = table.get_script();
    assert!(
        script.contains("constraint name_must_exist not null"),
        "expected named NOT NULL constraint in column definition: {script}"
    );
    // Must NOT appear as a separate ALTER TABLE statement
    assert!(
        !script.contains("alter table public.users add constraint name_must_exist"),
        "named NOT NULL should not be emitted as ALTER TABLE: {script}"
    );
}

#[test]
fn test_auto_generated_not_null_name_skips_constraint_keyword() {
    // Auto-generated name follows the pattern {table}_{col}_not_null
    let table = Table::new(
        "public".to_string(),
        "users".to_string(),
        "public".to_string(),
        "users".to_string(),
        "postgres".to_string(),
        None,
        vec![identity_column("id", 1, "integer"), name_column()],
        vec![
            primary_key_constraint(),
            not_null_constraint("users_name_not_null", "name"),
        ],
        vec![primary_key_index()],
        vec![],
        None,
    );

    let script = table.get_script();
    // Auto-generated name: plain "not null" without CONSTRAINT keyword
    assert!(
        script.contains("name text not null"),
        "expected plain NOT NULL for auto-generated name: {script}"
    );
    assert!(
        !script.contains("constraint users_name_not_null"),
        "auto-generated NOT NULL name should not use CONSTRAINT keyword: {script}"
    );
}

#[test]
fn test_named_not_null_with_not_enforced() {
    let mut nn = not_null_constraint("name_nn", "name");
    nn.is_enforced = false;

    let table = Table::new(
        "public".to_string(),
        "users".to_string(),
        "public".to_string(),
        "users".to_string(),
        "postgres".to_string(),
        None,
        vec![identity_column("id", 1, "integer"), name_column()],
        vec![primary_key_constraint(), nn],
        vec![primary_key_index()],
        vec![],
        None,
    );

    let script = table.get_script();
    assert!(
        script.contains("constraint name_nn not null not enforced"),
        "expected NOT ENFORCED on named NOT NULL constraint: {script}"
    );
}

#[test]
fn test_named_not_null_with_no_inherit() {
    let mut nn = not_null_constraint("name_nn", "name");
    nn.no_inherit = true;

    let table = Table::new(
        "public".to_string(),
        "users".to_string(),
        "public".to_string(),
        "users".to_string(),
        "postgres".to_string(),
        None,
        vec![identity_column("id", 1, "integer"), name_column()],
        vec![primary_key_constraint(), nn],
        vec![primary_key_index()],
        vec![],
        None,
    );

    let script = table.get_script();
    assert!(
        script.contains("constraint name_nn not null no inherit"),
        "expected NO INHERIT on named NOT NULL constraint: {script}"
    );
}

#[test]
fn fetch_columns_query_casts_attstattarget_to_int4() {
    let query = Table::build_columns_query("", "('public')");

    assert!(
        query.contains("a.attstattarget::int4"),
        "expected ::int4 cast for attstattarget"
    );
    assert!(
        query.contains("pd.classoid = 'pg_class'::regclass"),
        "expected pg_class classoid filter for table column comments"
    );
}

#[test]
fn build_indexes_bulk_query_filters_by_pg_class() {
    let query = Table::build_indexes_bulk_query("('public')");

    assert!(
        query.contains("d.classoid = 'pg_class'::regclass"),
        "expected pg_class classoid filter for table index comments"
    );
}

#[test]
fn test_auto_generated_not_null_with_numeric_suffix_skips_constraint_keyword() {
    // PG appends a numeric suffix to resolve cross-table auto-name collisions.
    // The result is still semantically anonymous and should not be emitted
    // with the CONSTRAINT keyword in the column definition.
    let table = Table::new(
        "public".to_string(),
        "users".to_string(),
        "public".to_string(),
        "users".to_string(),
        "postgres".to_string(),
        None,
        vec![identity_column("id", 1, "integer"), name_column()],
        vec![
            primary_key_constraint(),
            not_null_constraint("users_name_not_null1", "name"),
        ],
        vec![primary_key_index()],
        vec![],
        None,
    );

    let script = table.get_script();
    assert!(
        script.contains("name text not null"),
        "expected plain NOT NULL for auto-generated suffixed name: {script}"
    );
    assert!(
        !script.contains("constraint users_name_not_null1"),
        "auto-generated NOT NULL name with suffix should not use CONSTRAINT keyword: {script}"
    );
}

#[test]
fn test_auto_named_not_null_swap_produces_no_diff() {
    // Reproduces the cross-table auto-name swap scenario:
    // OLD has "users_name_not_null", NEW has "users_name_not_null1" on the
    // same column. Both are auto-generated names — diff must be empty.
    let from = Table::new(
        "public".to_string(),
        "users".to_string(),
        "public".to_string(),
        "users".to_string(),
        "postgres".to_string(),
        None,
        vec![identity_column("id", 1, "integer"), name_column()],
        vec![
            primary_key_constraint(),
            not_null_constraint("users_name_not_null", "name"),
        ],
        vec![primary_key_index()],
        vec![],
        None,
    );
    let to = Table::new(
        "public".to_string(),
        "users".to_string(),
        "public".to_string(),
        "users".to_string(),
        "postgres".to_string(),
        None,
        vec![identity_column("id", 1, "integer"), name_column()],
        vec![
            primary_key_constraint(),
            not_null_constraint("users_name_not_null1", "name"),
        ],
        vec![primary_key_index()],
        vec![],
        None,
    );

    let script = from.get_alter_script(&to, true);
    assert!(
        !script.contains("drop constraint users_name_not_null"),
        "auto-name suffix swap must not emit a drop: {script}"
    );
    assert!(
        !script.contains("add constraint users_name_not_null"),
        "auto-name suffix swap must not emit an add: {script}"
    );
}

#[test]
fn test_named_to_auto_named_not_null_still_diffs() {
    // A user-named NOT NULL ("name_must_exist") replaced with an auto-named
    // one is a real change — diff should drop+add.
    let from = Table::new(
        "public".to_string(),
        "users".to_string(),
        "public".to_string(),
        "users".to_string(),
        "postgres".to_string(),
        None,
        vec![identity_column("id", 1, "integer"), name_column()],
        vec![
            primary_key_constraint(),
            not_null_constraint("name_must_exist", "name"),
        ],
        vec![primary_key_index()],
        vec![],
        None,
    );
    let to = Table::new(
        "public".to_string(),
        "users".to_string(),
        "public".to_string(),
        "users".to_string(),
        "postgres".to_string(),
        None,
        vec![identity_column("id", 1, "integer"), name_column()],
        vec![
            primary_key_constraint(),
            not_null_constraint("users_name_not_null", "name"),
        ],
        vec![primary_key_index()],
        vec![],
        None,
    );

    let script = from.get_alter_script(&to, true);
    assert!(
        script.contains("drop constraint name_must_exist"),
        "renaming away from a user-chosen name must still emit a drop: {script}"
    );
}
