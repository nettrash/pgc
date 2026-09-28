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
        generation_blocks_retype: false,
        generation_routine_dropped: false,
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

/// `basic_table` with the definition `pg_get_constraintdef` gives its primary
/// key, which is what the inline `primary key (...)` line of `CREATE TABLE`
/// is built from. (The helper leaves it out so the hash pins built on it stay
/// as they are.)
fn basic_table_with_pk_definition() -> Table {
    let mut table = basic_table();
    table.constraints[0].definition = Some("PRIMARY KEY (id)".to_string());
    table
}

#[test]
fn test_get_script_generates_full_definition() {
    let table = basic_table_with_pk_definition();

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

// A quoted column name can contain spaces. The CREATE TABLE path cut the
// column script at its first space to drop the name, which turned `"Col X"`
// into `"Col X" X" integer` — an unterminated identifier that swallows the
// rest of the migration in psql.
#[test]
fn test_get_script_keeps_quoted_column_names_with_spaces_intact() {
    let mut table = basic_table_with_pk_definition();
    let mut spaced = base_column("\"Col X\"", 3);
    spaced.data_type = "integer".to_string();
    table.columns.push(spaced);

    let script = table.get_script();

    assert!(
        script.contains("    \"Col X\" integer,\n"),
        "column rendered wrongly: {script}"
    );
    assert!(
        !script.contains("X\" X\""),
        "name was cut at its space: {script}"
    );
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
        generation_blocks_retype: false,
        generation_routine_dropped: false,
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

// A partition recreated without its reloptions comes back with none at all, so the
// migration only reaches the target schema on a second pass (issue #216).
#[test]
fn test_partition_child_emits_storage_parameters() {
    let mut table = Table::new(
        "data".to_string(),
        "test_2025".to_string(),
        "data".to_string(),
        "test_2025".to_string(),
        "owner".to_string(),
        None,
        vec![create_dummy_column("id", "bigint")],
        vec![],
        vec![],
        vec![],
        None,
    );
    table.partition_of = Some("data.test".to_string());
    table.partition_bound = Some("FOR VALUES FROM (2025) TO (2026)".to_string());
    table.storage_parameters = Some(vec![
        "autovacuum_analyze_scale_factor=0.02".to_string(),
        "autovacuum_vacuum_scale_factor=0.05".to_string(),
    ]);

    let script = table.get_script();
    assert!(
        script.contains(
            "with (autovacuum_analyze_scale_factor=0.02, autovacuum_vacuum_scale_factor=0.05)"
        ),
        "partition must carry its reloptions: {script}"
    );
    // CREATE TABLE ... PARTITION OF takes WITH (...) after the bound, so the clause has
    // to sit inside the statement rather than trail the terminating semicolon.
    assert!(
        script.contains("FOR VALUES FROM (2025) TO (2026)\nwith ("),
        "with (...) must follow the partition bound inside the statement: {script}"
    );
}

// PostgreSQL's grammar fixes the order: PARTITION BY, then WITH (...), then TABLESPACE.
#[test]
fn test_partition_child_orders_storage_parameters_before_tablespace() {
    let mut table = Table::new(
        "data".to_string(),
        "test_2025".to_string(),
        "data".to_string(),
        "test_2025".to_string(),
        "owner".to_string(),
        None,
        vec![create_dummy_column("id", "bigint")],
        vec![],
        vec![],
        vec![],
        None,
    );
    table.partition_of = Some("data.test".to_string());
    table.partition_bound = Some("DEFAULT".to_string());
    table.partition_key = Some("LIST (id)".to_string());
    table.storage_parameters = Some(vec!["fillfactor=70".to_string()]);
    table.space = Some("fast_disk".to_string());

    let script = table.get_script();
    let key = script
        .find("partition by LIST (id)")
        .expect("partition key");
    let with = script
        .find("with (fillfactor=70)")
        .expect("storage parameters");
    let space = script.find("tablespace \"fast_disk\"").expect("tablespace");
    assert!(
        key < with && with < space,
        "expected partition by -> with (...) -> tablespace; got:\n{script}"
    );
}

#[test]
fn test_partition_child_without_storage_parameters_omits_with() {
    let mut table = Table::new(
        "data".to_string(),
        "test_default".to_string(),
        "data".to_string(),
        "test_default".to_string(),
        "owner".to_string(),
        None,
        vec![create_dummy_column("id", "bigint")],
        vec![],
        vec![],
        vec![],
        None,
    );
    table.partition_of = Some("data.test".to_string());
    table.partition_bound = Some("DEFAULT".to_string());
    table.storage_parameters = Some(Vec::new());

    assert!(
        !table.get_script().contains("with ("),
        "an empty reloptions list must not emit an empty with ()"
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
    let query = Table::build_columns_query("", "", "('public')");

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
fn fetch_columns_query_uses_pg_catalog_not_information_schema_columns() {
    // information_schema.columns is filtered by `pg_has_role(...) OR
    // has_column_privilege(...)`, so dumps taken by a role without
    // privileges silently lose columns of tables it can't read. The
    // query must read from pg_attribute directly.
    let query = Table::build_columns_query("", "", "('public')");

    assert!(
        !query.contains("information_schema.columns"),
        "build_columns_query must not read from information_schema.columns \
            (privilege-filtered); use pg_attribute directly. Query was:\n{query}"
    );
    assert!(
        query.contains("FROM pg_attribute a"),
        "expected pg_attribute as the column source. Query was:\n{query}"
    );
    assert!(
        !query.contains("information_schema.view_column_usage"),
        "related_views lookup must not use information_schema.view_column_usage \
            (privilege-filtered); use pg_depend/pg_rewrite. Query was:\n{query}"
    );

    // ---- column_default: identity & generated columns must be suppressed ----
    //
    // information_schema.columns returns NULL for generated columns, and
    // identity columns don't usually have a pg_attrdef row (the sequence
    // is linked via pg_depend with deptype='i'). The new query must not
    // accidentally surface a `nextval(...)` literal for identity columns
    // or a default expression alongside `GENERATED ALWAYS AS (...)` for
    // generated columns — both would produce invalid CREATE TABLE output
    // and false diffs against dumps that originated from the old query.
    assert!(
        !query.to_lowercase().contains("nextval("),
        "column_default must not hardcode nextval(); identity sequences \
            are linked through pg_depend(deptype='i'), not pg_attrdef. \
            Query was:\n{query}"
    );
    assert!(
        query.contains("a.attgenerated <> ''")
            && query.contains("a.attidentity <> ''")
            && query.contains("THEN NULL"),
        "column_default CASE must NULL out defaults for both generated \
            (attgenerated <> '') and identity (attidentity <> '') columns \
            so the dump never emits `GENERATED ... AS (...) DEFAULT ...` \
            or `IDENTITY ... DEFAULT nextval(...)`. Query was:\n{query}"
    );

    // ---- formatted_data_type: domain columns keep their domain name ----
    //
    // Domain-typed columns must render as the domain (e.g.
    // `myschema.my_domain`), not the underlying base type, so dumps round
    // trip without losing the domain reference. The CASE has a dedicated
    // `WHEN t.typtype = 'd'` branch that resolves through
    // format_type(a.atttypid, a.atttypmod), which expands to the domain's
    // own qualified name. Lock that branch and its inputs down so the
    // logic can't silently regress to the base type.
    assert!(
        query.contains("t.typtype = 'd'"),
        "formatted_data_type CASE must branch on t.typtype='d' to render \
            domain-typed columns. Query was:\n{query}"
    );
    assert!(
        query.contains("LEFT JOIN pg_type bt ON t.typtype = 'd' AND bt.oid = t.typbasetype"),
        "expected a LEFT JOIN to pg_type bt on t.typbasetype so the domain \
            branch can introspect the base type. Query was:\n{query}"
    );
    assert!(
        query.contains("format_type(a.atttypid, a.atttypmod)"),
        "formatted_data_type must reach format_type(a.atttypid, a.atttypmod) \
            so the domain's own qualified name (not the base type) is \
            emitted for domain-typed columns. Query was:\n{query}"
    );
}

/// Pin field-level semantics of `build_columns_query` so the rewrite from
/// `information_schema.columns` to `pg_catalog` cannot silently regress on
/// the columns that actually drive SQL generation in
/// `TableColumn::get_alter_script` (data_type, column_default, is_nullable,
/// is_identity, identity_*, is_generated, generation_expression).
///
/// This is a structural-but-load-bearing test: it does not run the SQL,
/// but it asserts that the exact pg_catalog expressions the owner walked
/// through field-by-field in the review remain in place. If a future change
/// rewires any of these derivations the test fires and forces a deliberate
/// update.
///
/// For a true behavioral check the project relies on the CI dump-and-compare
/// matrix against `data/test/schema_a.sql` / `schema_b.sql`, plus the
/// manual verification recorded in the CHANGELOG entry for this PR.
#[test]
fn fetch_columns_query_pins_load_bearing_field_semantics() {
    let query = Table::build_columns_query("", "", "('public')");

    // ---- identity columns: metadata sourced from pg_sequence ps ----
    //
    // Identity sequences are linked to the column via pg_depend with
    // deptype='i'. The new query joins pg_depend → pg_class (relkind='S')
    // → pg_sequence and reads seqstart / seqincrement / seqmax / seqmin /
    // seqcycle. All identity_* fields are gated on attidentity IN ('a','d')
    // so non-identity columns receive NULL (matching information_schema's
    // behavior).
    for (field, expr) in [
        ("identity_start", "ps.seqstart::text"),
        ("identity_increment", "ps.seqincrement::text"),
        ("identity_maximum", "ps.seqmax::text"),
        ("identity_minimum", "ps.seqmin::text"),
    ] {
        let snippet = format!("WHEN a.attidentity IN ('a', 'd') THEN {expr}");
        assert!(
            query.contains(&snippet),
            "{field} must derive from pg_sequence via attidentity gate. \
                Expected snippet:\n  {snippet}\nQuery was:\n{query}"
        );
    }
    assert!(
        query.contains(
            "CASE WHEN a.attidentity IN ('a', 'd') THEN 'YES' ELSE 'NO' END as is_identity"
        ),
        "is_identity must be derived from attidentity. Query was:\n{query}"
    );
    assert!(
        query.contains("WHEN 'a' THEN 'ALWAYS'") && query.contains("WHEN 'd' THEN 'BY DEFAULT'"),
        "identity_generation must map attidentity 'a'→ALWAYS, 'd'→BY DEFAULT. \
            Query was:\n{query}"
    );
    assert!(
        query.contains("LEFT JOIN pg_depend ident_dep")
            && query.contains("AND ident_dep.deptype = 'i'")
            && query.contains(
                "LEFT JOIN pg_class seq ON seq.oid = ident_dep.objid AND seq.relkind = 'S'"
            )
            && query.contains("LEFT JOIN pg_sequence ps ON ps.seqrelid = seq.oid"),
        "identity sequence lookup must be pg_depend(deptype='i') → \
            pg_class(relkind='S') → pg_sequence. Query was:\n{query}"
    );

    // ---- generated columns: expression via pg_get_expr, gated on attgenerated ----
    //
    // `c.generation_expression` from information_schema is replaced by
    // pg_get_expr(ad.adbin, ad.adrelid) gated on attgenerated <> ''. PG18
    // virtual generated columns (attgenerated='v') still match this gate
    // and pg_attrdef holds the expression for them too, so virtual/stored
    // remain differentiable via the separate `attgenerated::text` column.
    assert!(
        query.contains(
            "CASE WHEN a.attgenerated <> '' THEN 'ALWAYS' ELSE 'NEVER' END as is_generated"
        ),
        "is_generated must be derived from attgenerated <> ''. Query was:\n{query}"
    );
    assert!(
        query.contains(
            "CASE WHEN a.attgenerated <> '' THEN pg_get_expr(ad.adbin, ad.adrelid) ELSE NULL END as generation_expression"
        ),
        "generation_expression must use pg_get_expr gated on attgenerated. \
            Query was:\n{query}"
    );
    assert!(
        query.contains("a.attgenerated::text as attgenerated"),
        "attgenerated raw char must survive so stored ('s') vs virtual ('v') \
            stays differentiable for PG18 virtual-generated tracking \
            (see PR #185). Query was:\n{query}"
    );

    // ---- is_nullable: attnotnull OR domain typnotnull ----
    //
    // information_schema.columns.is_nullable returns 'NO' when either the
    // column is declared NOT NULL or the column's type is a domain whose
    // base is NOT NULL. The new query must reproduce both halves.
    assert!(
        query.contains("WHEN a.attnotnull OR (t.typtype = 'd' AND t.typnotnull) THEN 'NO'"),
        "is_nullable must consider both attnotnull and domain typnotnull \
            so domain-of-not-null preserves nullability semantics. \
            Query was:\n{query}"
    );

    // ---- typmod-derived metrics: _pg_* helpers, not privilege-filtered ----
    //
    // _pg_char_max_length / _pg_numeric_precision / _pg_numeric_scale /
    // _pg_datetime_precision / _pg_interval_type are plain SQL helpers used
    // internally by information_schema.columns. They are safe to call
    // because they are not gated by has_column_privilege; only the .columns
    // view itself wraps them in a privilege filter.
    for helper in [
        "_pg_char_max_length",
        "_pg_char_octet_length",
        "_pg_numeric_precision",
        "_pg_numeric_precision_radix",
        "_pg_numeric_scale",
        "_pg_datetime_precision",
        "_pg_interval_type",
    ] {
        let call = format!("information_schema.{helper}(");
        assert!(
            query.contains(&call),
            "typmod-derived metric must use the public helper \
                `information_schema.{helper}` (not privilege-filtered). \
                Query was:\n{query}"
        );
    }
    assert!(
        query.contains("information_schema._pg_truetypid(a, t)")
            && query.contains("information_schema._pg_truetypmod(a, t)"),
        "_pg_* helpers must receive truetypid / truetypmod (a, t) so domain \
            columns resolve through to their base type's typmod. \
            Query was:\n{query}"
    );

    // ---- formatted_data_type: 4-arm CASE preserving information_schema layout ----
    //
    // The CASE mirrors information_schema.columns.data_type's four-way logic
    // (domain vs non-domain × array vs non-array × pg_catalog vs user
    // namespace). The first arm covers arrays (incl. domain-of-array), the
    // second covers user-namespace types, the third is the domain catch-all
    // (returns the domain's own qualified name), and the else falls back to
    // format_type(atttypid, NULL) for built-in scalars.
    assert!(
        query.contains("(t.typtype = 'd' AND bt.typelem <> 0 AND bt.typlen = -1)")
            && query.contains("(t.typtype <> 'd' AND t.typelem <> 0 AND t.typlen = -1)"),
        "formatted_data_type CASE must have an array arm that covers both \
            domain-of-array (via bt.typelem) and direct array \
            (via t.typelem). Query was:\n{query}"
    );
    assert!(
        query.contains("nbt.nspname <> 'pg_catalog'")
            && query.contains("nt.nspname <> 'pg_catalog'"),
        "formatted_data_type CASE must distinguish pg_catalog vs user \
            namespace for both domain (nbt) and non-domain (nt) types. \
            Query was:\n{query}"
    );
    assert!(
        query.contains("ELSE pg_catalog.format_type(a.atttypid, NULL)"),
        "formatted_data_type fallback for built-in scalars must drop typmod \
            so e.g. 'integer' is emitted without precision qualifier. \
            Query was:\n{query}"
    );

    // ---- column_default: suppressed for generated AND identity columns ----
    //
    // Already covered by the previous test, re-asserted here so this test
    // forms a single point of truth for the load-bearing CASE branches.
    assert!(
        query.contains("WHEN a.attgenerated <> '' OR a.attidentity <> '' THEN NULL"),
        "column_default CASE must NULL out defaults for BOTH generated \
            and identity columns to avoid bogus `DEFAULT nextval(...)` or \
            `DEFAULT <expr>` alongside `GENERATED ALWAYS AS ...`. \
            Query was:\n{query}"
    );

    // ---- related_views: pg_depend(deptype='n') + pg_rewrite, no info_schema ----
    //
    // The unified pg_depend + pg_rewrite walk replaces the old UNION ALL
    // over information_schema.view_column_usage + pg_depend. The relkind
    // filter must include BOTH regular ('v') and materialized ('m') views
    // (the old code split them across two queries; the new code merges).
    assert!(
        query.contains("AND vc.relkind IN ('v', 'm')"),
        "related_views must include both regular views ('v') and \
            materialized views ('m'). Query was:\n{query}"
    );
    assert!(
        query.contains("AND dep.deptype = 'n'"),
        "related_views must filter pg_depend by deptype='n' (normal \
            dependency) — that is the link a view's parsed rule body \
            records against the source column. Query was:\n{query}"
    );

    // ---- relkind filter on the outer FROM ----
    //
    // Match information_schema.columns's implicit filter: regular ('r'),
    // view ('v'), matview ('m'), foreign ('f'), partitioned table ('p').
    // Composite types ('c') are dumped via a separate path in core.rs
    // (pg_type, not pg_class) and are intentionally excluded here so we do
    // not double-emit composite attributes.
    assert!(
        query.contains("c.relkind IN ('r','v','m','f','p')"),
        "outer relkind filter must mirror information_schema.columns \
            (regular/view/matview/foreign/partitioned). Composite types \
            are dumped via core.rs and excluded on purpose. \
            Query was:\n{query}"
    );

    // ---- hardcoded fields are not load-bearing ----
    //
    // These columns are kept as constants because they do not appear in
    // TableColumn::add_to_hasher (see table_column.rs ~L313–L353) and are
    // not read by TableColumn::get_alter_script. They participate in
    // PartialEq only — a mismatch produces zero SQL.
    for hardcoded in [
        "NULL::int4 as interval_precision",
        "NULL::text as character_set_catalog",
        "NULL::text as character_set_schema",
        "NULL::text as character_set_name",
        "NULL::text as scope_catalog",
        "NULL::text as scope_schema",
        "NULL::text as scope_name",
        "NULL::int4 as maximum_cardinality",
        "NULL::text as dtd_identifier",
        "'NO' as is_self_referencing",
        "'YES' as is_updatable",
    ] {
        assert!(
            query.contains(hardcoded),
            "expected hardcoded constant `{hardcoded}` (safe: not in \
                add_to_hasher or get_alter_script). Query was:\n{query}"
        );
    }
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
fn test_named_to_auto_named_not_null_keeps_the_constraint() {
    // A user-named NOT NULL ("name_must_exist") where TO has PostgreSQL's
    // own name for the column's NOT NULL is the same constraint: a column
    // has one. Dropping it cleared NOT NULL on a column TO declares NOT
    // NULL (and no ADD followed, the TO name being auto-generated); the
    // FROM name, TO's being anonymous, stays.
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
        !script.contains("drop constraint name_must_exist"),
        "a NOT NULL constraint TO keeps under an auto-generated name must not be dropped: {script}"
    );
    assert!(!script.contains("add constraint"), "{script}");
    assert!(!script.contains("rename constraint"), "{script}");
}

// Issue #218: a column becoming NOT NULL must not also emit ADD CONSTRAINT for the
// auto-named NOT NULL that PG18 surfaces in pg_constraint. The column diff already
// emits `set not null`; the ADD CONSTRAINT is redundant, PG18-only syntax, and leaves
// a named constraint the source never declared.
#[test]
fn test_column_gains_not_null_omits_add_constraint() {
    let mut nullable_name = name_column();
    nullable_name.is_nullable = true;
    let from = Table::new(
        "public".to_string(),
        "users".to_string(),
        "public".to_string(),
        "users".to_string(),
        "postgres".to_string(),
        None,
        vec![identity_column("id", 1, "integer"), nullable_name],
        vec![primary_key_constraint()],
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
        script.contains("alter column name set not null"),
        "the column diff must set NOT NULL: {script}"
    );
    assert!(
        !script.contains("add constraint users_name_not_null"),
        "an auto-named NOT NULL must not be emitted as ADD CONSTRAINT: {script}"
    );
}

// Issue #218: a column losing NOT NULL must not emit DROP CONSTRAINT for the auto-named
// NOT NULL. On PG14–17 that named constraint does not exist and the drop errors; on
// PG18 the column diff's `drop not null` already removes it.
#[test]
fn test_column_loses_not_null_omits_drop_constraint() {
    let mut nullable_name = name_column();
    nullable_name.is_nullable = true;
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
        vec![identity_column("id", 1, "integer"), nullable_name],
        vec![primary_key_constraint()],
        vec![primary_key_index()],
        vec![],
        None,
    );

    let script = from.get_alter_script(&to, true);
    assert!(
        script.contains("alter column name drop not null"),
        "the column diff must drop NOT NULL: {script}"
    );
    assert!(
        !script.contains("drop constraint users_name_not_null"),
        "an auto-named NOT NULL must not be emitted as DROP CONSTRAINT: {script}"
    );
}

// A column dropped entirely must not emit DROP CONSTRAINT for its auto-named NOT NULL:
// dropping the column removes the constraint, and the explicit drop errors on PG14–17.
#[test]
fn test_dropped_not_null_column_omits_drop_constraint() {
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
        vec![identity_column("id", 1, "integer")],
        vec![primary_key_constraint()],
        vec![primary_key_index()],
        vec![],
        None,
    );

    let script = from.get_alter_script(&to, true);
    assert!(
        script.contains("drop column name"),
        "the column must be dropped: {script}"
    );
    assert!(
        !script.contains("drop constraint users_name_not_null"),
        "dropping the column must not also drop its auto-named NOT NULL: {script}"
    );
}

// ---- REPLICA IDENTITY USING INDEX (fix:replident) ----

fn ri_index(name: &str, columns: &str) -> TableIndex {
    TableIndex {
        schema: "public".to_string(),
        table: "users".to_string(),
        name: name.to_string(),
        catalog: None,
        indexdef: format!("CREATE UNIQUE INDEX {name} ON public.users USING btree ({columns})"),
        is_partition_index: false,
        comment: None,
    }
}

fn ri_column(name: &str, ordinal_position: i32, not_null: bool) -> TableColumn {
    let mut column = base_column(name, ordinal_position);
    column.data_type = "integer".to_string();
    column.is_nullable = !not_null;
    column
}

/// `users (id integer, code integer [not null])` with the given replica
/// identity (`ri`, `ri_index`), constraints and indexes.
fn ri_table(
    code_not_null: bool,
    constraints: Vec<TableConstraint>,
    indexes: Vec<TableIndex>,
    ri: Option<&str>,
    ri_index_name: Option<&str>,
) -> Table {
    let mut table = Table::new(
        "public".to_string(),
        "users".to_string(),
        "public".to_string(),
        "users".to_string(),
        "postgres".to_string(),
        None,
        vec![
            ri_column("id", 1, true),
            ri_column("code", 2, code_not_null),
        ],
        constraints,
        indexes,
        vec![],
        None,
    );
    table.replica_identity = ri.map(str::to_string);
    table.replica_identity_index = ri_index_name.map(str::to_string);
    table.hash();
    table
}

fn code_uk() -> TableIndex {
    ri_index("users_code_uk", "code")
}

const USING_CODE_UK: &str = "alter table public.users replica identity using index users_code_uk;";

/// The `USING INDEX` an alter from `from` to `to` hands to the comparer.
fn ri_after(from: &Table, to: &Table, use_drop: bool, defer_indexes: bool) -> Option<String> {
    from.get_alter_script_in_family(
        to,
        use_drop,
        defer_indexes,
        TableAlterCaps::default(),
        &InheritedRecreates::default(),
    )
    .1
}

#[test]
fn replica_identity_hash_is_unchanged_without_a_using_index() {
    // Pinned from pgc 1.0.27: a table not on USING INDEX has no
    // replica_identity_index, so its hash must not move.
    let pins = [
        (
            None,
            "fd67e130e28ceb6bb23efa0a9144765bea42ed824b0394b4e3a284417ab465af",
        ),
        (
            Some("d"),
            "5a198916fb4a898020ecd69942b49ccf7df45f27d71be6bdbbbabbf2936a9c2e",
        ),
        (
            Some("n"),
            "00d23d634ccaf41885b9b2aafb87a951a222211e9eaaf98084f3352beaa11608",
        ),
        (
            Some("f"),
            "74b52e1fcda32a941f8717d6d01f46275efe92b734622a8d0fec62768dadb0c0",
        ),
        // A pre-1.1.0 dump's USING INDEX table, which has no index name.
        (
            Some("i"),
            "7e4a7c9abb3a83c2f7b639438bbb757859a8a119a379771424bdf558f1e36f3d",
        ),
    ];
    for (ri, pin) in pins {
        let mut table = basic_table();
        table.replica_identity = ri.map(str::to_string);
        table.hash();
        assert_eq!(table.hash.as_deref(), Some(pin), "replica identity {ri:?}");
    }
}

#[test]
fn replica_identity_hash_folds_in_the_index_name() {
    let hash_with = |index: Option<&str>| {
        let mut table = basic_table();
        table.replica_identity = Some("i".to_string());
        table.replica_identity_index = index.map(str::to_string);
        table.hash();
        table.hash.unwrap()
    };
    let unrecorded = hash_with(None);
    let a = hash_with(Some("uk_a"));
    let b = hash_with(Some("uk_b"));
    // A switch between two indexes is a change pgc can see.
    assert_ne!(a, b);
    assert_ne!(a, unrecorded);
    assert_eq!(a, hash_with(Some("uk_a")));
}

#[test]
fn replica_identity_using_index_is_created_after_indexes_and_constraints() {
    let table = ri_table(
        true,
        vec![
            primary_key_constraint(),
            unique_constraint("users_id_code_key", "UNIQUE (id, code)"),
        ],
        vec![code_uk()],
        Some("i"),
        Some("users_code_uk"),
    );
    let script = table.get_script();
    let using = position(&script, USING_CODE_UK);
    assert!(
        position(&script, "CREATE UNIQUE INDEX users_code_uk") < using,
        "{script}"
    );
    assert!(
        position(&script, "add constraint users_id_code_key unique") < using,
        "{script}"
    );
    assert_eq!(script.matches("replica identity").count(), 1, "{script}");
}

#[test]
fn replica_identity_using_an_unrecorded_index_creates_nothing() {
    // A pre-1.1.0 dump says USING INDEX without naming the index.
    let table = ri_table(true, vec![], vec![code_uk()], Some("i"), None);
    assert!(!table.get_script().contains("replica identity"));
    assert_eq!(table.replica_identity_using_index_script(), None);
    assert_eq!(table.replica_identity_create_script_after_indexes(), None);
}

#[test]
fn replica_identity_using_index_waits_for_production_index_builds_on_create() {
    // The production path builds a new table's own indexes after commit, so
    // it takes the statement over; a constraint's index exists in-txn.
    let table = ri_table(
        true,
        vec![],
        vec![code_uk()],
        Some("i"),
        Some("users_code_uk"),
    );
    assert!(
        !table
            .get_script_without_triggers_no_indexes()
            .contains("replica identity")
    );
    assert_eq!(
        table.replica_identity_create_script_after_indexes(),
        Some(format!("{USING_CODE_UK}\n\n"))
    );
    assert!(table.get_script_without_triggers().contains(USING_CODE_UK));

    let on_constraint = ri_table(
        true,
        vec![unique_constraint("users_code_key", "UNIQUE (code)")],
        vec![],
        Some("i"),
        Some("users_code_key"),
    );
    let script = on_constraint.get_script_without_triggers_no_indexes();
    assert!(
        position(&script, "add constraint users_code_key unique (code)")
            < position(
                &script,
                "alter table public.users replica identity using index users_code_key;"
            ),
        "{script}"
    );
    assert_eq!(
        on_constraint.replica_identity_create_script_after_indexes(),
        None
    );
}

#[test]
fn replica_identity_plain_changes_keep_their_1_0_output() {
    // DEFAULT / NOTHING / FULL transitions: same statement, same place.
    for (from_ri, to_ri, stmt) in [
        (Some("d"), Some("f"), "full"),
        (Some("f"), Some("n"), "nothing"),
        (Some("n"), Some("d"), "default"),
        (None, Some("d"), "default"),
    ] {
        let from = ri_table(true, vec![], vec![], from_ri, None);
        let to = ri_table(true, vec![], vec![], to_ri, None);
        assert_eq!(
            from.get_alter_script(&to, true),
            format!("alter table public.users replica identity {stmt};\n\n")
        );
    }
}

#[test]
fn replica_identity_enters_using_index_after_the_index_and_not_null() {
    // DEFAULT -> USING INDEX on a new index over a column that gains NOT NULL
    // (pgc 1.0.x emitted `replica identity default` here).
    let from = ri_table(false, vec![], vec![], Some("d"), None);
    let to = ri_table(
        true,
        vec![],
        vec![code_uk()],
        Some("i"),
        Some("users_code_uk"),
    );
    let script = from.get_alter_script(&to, true);
    let using = position(&script, USING_CODE_UK);
    assert!(
        position(
            &script,
            "alter table public.users alter column code set not null;"
        ) < using,
        "{script}"
    );
    assert!(
        position(&script, "CREATE UNIQUE INDEX users_code_uk") < using,
        "{script}"
    );
    assert!(!script.contains("replica identity default"), "{script}");
    assert!(
        script.ends_with(&format!("{USING_CODE_UK}\n\n")),
        "{script}"
    );
}

#[test]
fn replica_identity_leaves_using_index_before_drop_not_null() {
    // DROP NOT NULL on a replica-identity key column fails, so the table
    // leaves the index first.
    let from = ri_table(
        true,
        vec![],
        vec![code_uk()],
        Some("i"),
        Some("users_code_uk"),
    );
    let to = ri_table(false, vec![], vec![code_uk()], Some("d"), None);
    let script = from.get_alter_script(&to, true);
    assert!(
        script.starts_with("alter table public.users replica identity default;\n\n"),
        "{script}"
    );
    assert!(
        script.contains("alter table public.users alter column code drop not null;"),
        "{script}"
    );
    assert_eq!(script.matches("replica identity").count(), 1, "{script}");
}

#[test]
fn replica_identity_leaves_using_index_before_the_index_is_dropped() {
    let from = ri_table(
        true,
        vec![],
        vec![code_uk()],
        Some("i"),
        Some("users_code_uk"),
    );
    let to = ri_table(true, vec![], vec![], Some("f"), None);
    let script = from.get_alter_script(&to, true);
    assert_eq!(
        script,
        "alter table public.users replica identity full;\n\n\
         drop index if exists public.users_code_uk;\n\n"
    );
}

#[test]
fn replica_identity_switch_between_indexes_is_detected() {
    // Both indexes stay: the table moves in one step, with no DEFAULT in
    // between (a table without a primary key would have no replica identity
    // there).
    let indexes = vec![ri_index("users_a_uk", "id"), code_uk()];
    let from = ri_table(true, vec![], indexes.clone(), Some("i"), Some("users_a_uk"));
    let to = ri_table(true, vec![], indexes, Some("i"), Some("users_code_uk"));
    assert_ne!(from.hash, to.hash);
    assert_eq!(
        from.get_alter_script(&to, true),
        format!("{USING_CODE_UK}\n\n")
    );
}

#[test]
fn replica_identity_switch_leaves_an_index_that_is_dropped_first() {
    let from = ri_table(
        true,
        vec![],
        vec![ri_index("users_a_uk", "id"), code_uk()],
        Some("i"),
        Some("users_a_uk"),
    );
    let to = ri_table(
        true,
        vec![],
        vec![code_uk()],
        Some("i"),
        Some("users_code_uk"),
    );
    assert_eq!(
        from.get_alter_script(&to, true),
        format!(
            "alter table public.users replica identity default;\n\n\
             drop index if exists public.users_a_uk;\n\n\
             {USING_CODE_UK}\n\n"
        )
    );
}

#[test]
fn replica_identity_is_re_applied_after_its_index_is_rebuilt() {
    // Same name, new definition: the rebuilt index does not inherit
    // indisreplident, so the table leaves it first and returns last.
    let from = ri_table(
        true,
        vec![],
        vec![code_uk()],
        Some("i"),
        Some("users_code_uk"),
    );
    let to = ri_table(
        true,
        vec![],
        vec![ri_index("users_code_uk", "code, id")],
        Some("i"),
        Some("users_code_uk"),
    );
    let script = from.get_alter_script(&to, true);
    let reset = position(
        &script,
        "alter table public.users replica identity default;",
    );
    let drop = position(&script, "drop index if exists public.users_code_uk;");
    let create = position(&script, "CREATE UNIQUE INDEX users_code_uk");
    let using = position(&script, USING_CODE_UK);
    assert!(reset < drop && drop < create && create < using, "{script}");
}

#[test]
fn replica_identity_is_re_applied_after_its_constraint_is_re_added() {
    let from = ri_table(
        true,
        vec![unique_constraint("users_code_key", "UNIQUE (code)")],
        vec![],
        Some("i"),
        Some("users_code_key"),
    );
    let to = ri_table(
        true,
        vec![unique_constraint("users_code_key", "UNIQUE (code, id)")],
        vec![],
        Some("i"),
        Some("users_code_key"),
    );
    let script = from.get_alter_script(&to, true);
    let reset = position(
        &script,
        "alter table public.users replica identity default;",
    );
    let drop = position(
        &script,
        "alter table public.users drop constraint users_code_key;",
    );
    let add = position(&script, "add constraint users_code_key unique (code, id)");
    let using = position(
        &script,
        "alter table public.users replica identity using index users_code_key;",
    );
    assert!(reset < drop && drop < add && add < using, "{script}");
}

#[test]
fn replica_identity_leaves_and_returns_around_a_not_null_re_add() {
    // PG18 NOT VALID -> validated not-null is a drop + re-add; dropping the
    // not-null of a replica-identity key column fails, and PG19 / 18.7 only
    // accept USING INDEX once it is validated again.
    let mut nn_from = not_null_constraint("users_code_nn", "code");
    nn_from.definition = Some("NOT NULL code NOT VALID".to_string());
    let nn_to = not_null_constraint("users_code_nn", "code");
    let from = ri_table(
        true,
        vec![nn_from],
        vec![code_uk()],
        Some("i"),
        Some("users_code_uk"),
    );
    let to = ri_table(
        true,
        vec![nn_to],
        vec![code_uk()],
        Some("i"),
        Some("users_code_uk"),
    );
    let script = from.get_alter_script(&to, true);
    let reset = position(
        &script,
        "alter table public.users replica identity default;",
    );
    let drop = position(
        &script,
        "alter table public.users drop constraint users_code_nn;",
    );
    let add = position(&script, "add constraint users_code_nn not null code");
    let using = position(&script, USING_CODE_UK);
    assert!(reset < drop && drop < add && add < using, "{script}");
}

#[test]
fn replica_identity_untouched_by_an_unrelated_change() {
    let from = ri_table(
        true,
        vec![],
        vec![code_uk()],
        Some("i"),
        Some("users_code_uk"),
    );
    let mut to = from.clone();
    to.comment = Some("users".to_string());
    to.hash();
    let script = from.get_alter_script(&to, true);
    assert_eq!(script, "comment on table public.users is 'users';\n\n");
}

#[test]
fn replica_identity_left_alone_for_an_unrecorded_target_index() {
    // A pre-1.1.0 TO dump: USING INDEX, index unknown. pgc 1.0.x emitted
    // `replica identity default` here; now nothing is guessed.
    let from = ri_table(true, vec![], vec![code_uk()], Some("d"), None);
    let to = ri_table(true, vec![], vec![code_uk()], Some("i"), None);
    assert_eq!(from.get_alter_script(&to, true), "");
}

#[test]
fn replica_identity_from_an_unrecorded_index_is_set_again() {
    // A pre-1.1.0 FROM dump against a 1.1.0 TO dump.
    let from = ri_table(true, vec![], vec![code_uk()], Some("i"), None);
    let to = ri_table(
        true,
        vec![],
        vec![code_uk()],
        Some("i"),
        Some("users_code_uk"),
    );
    assert_eq!(
        from.get_alter_script(&to, true),
        format!("alter table public.users replica identity default;\n\n{USING_CODE_UK}\n\n")
    );
}

#[test]
fn replica_identity_waits_for_the_production_index_build_on_alter() {
    let from = ri_table(true, vec![], vec![], Some("d"), None);
    let to = ri_table(
        true,
        vec![],
        vec![code_uk()],
        Some("i"),
        Some("users_code_uk"),
    );

    // Production: the index is built after commit, so the comparer emits
    // USING INDEX after it.
    let script = from.get_alter_script_without_triggers_no_indexes(&to, true);
    assert!(!script.contains("replica identity"), "{script}");
    assert_eq!(
        ri_after(&from, &to, true, true),
        Some(format!("{USING_CODE_UK}\n\n"))
    );

    // Default mode: inline, and nothing is left to the comparer.
    assert!(from.get_alter_script(&to, true).contains(USING_CODE_UK));
    assert_eq!(ri_after(&from, &to, true, false), None);

    // Production, index already there (a switch): inline as well.
    let indexes = vec![ri_index("users_a_uk", "id"), code_uk()];
    let from = ri_table(true, vec![], indexes.clone(), Some("i"), Some("users_a_uk"));
    let to = ri_table(true, vec![], indexes, Some("i"), Some("users_code_uk"));
    let script = from.get_alter_script_without_triggers_no_indexes(&to, true);
    assert!(script.contains(USING_CODE_UK), "{script}");
    assert_eq!(ri_after(&from, &to, true, true), None);
}

#[test]
fn replica_identity_waits_for_dependents_of_a_re_created_column() {
    // code becomes GENERATED ... STORED: DROP COLUMN + ADD COLUMN, and the
    // drop takes users_code_uk with it until the comparer re-creates it.
    let from = ri_table(
        true,
        vec![],
        vec![code_uk()],
        Some("i"),
        Some("users_code_uk"),
    );
    let mut to = from.clone();
    to.columns[1].is_generated = "ALWAYS".to_string();
    to.columns[1].generation_expression = Some("(id * 2)".to_string());
    to.columns[1].generation_type = Some("s".to_string());
    to.hash();

    let script = from.get_alter_script(&to, true);
    assert!(
        position(
            &script,
            "alter table public.users replica identity default;"
        ) < position(&script, "alter table public.users drop column code;"),
        "{script}"
    );
    assert!(!script.contains("using index"), "{script}");
    assert_eq!(
        ri_after(&from, &to, true, false),
        Some(format!("{USING_CODE_UK}\n\n"))
    );

    // use_drop=false only comments the drop out: nothing is re-created.
    assert_eq!(ri_after(&from, &to, false, false), None);
}

// ---- fix:replident review: use_drop=false, unrelated columns, partitions ----

#[test]
fn replica_identity_untouched_by_drops_left_commented_out() {
    // use_drop=false only comments the drops out, so nothing takes the index
    // or a NOT NULL on its key column away: no leave + return, as in pgc
    // 1.0.x. The column list is unknown here, so every column would count.
    let mut from = ri_table(
        true,
        vec![not_null_constraint("users_code_nn", "code")],
        vec![code_uk(), ri_index("users_id_ix", "id")],
        Some("i"),
        Some("users_code_uk"),
    );
    from.columns.push(ri_column("junk", 3, true));
    from.hash();
    // junk, users_id_ix and the named NOT NULL are gone; users_code_uk is
    // rebuilt; id loses NOT NULL.
    let mut to = ri_table(
        true,
        vec![],
        vec![ri_index("users_code_uk", "code, id")],
        Some("i"),
        Some("users_code_uk"),
    );
    to.columns[0].is_nullable = true;
    to.hash();

    let script = from.get_alter_script(&to, false);
    assert!(!script.contains("replica identity"), "{script}");
    for commented in [
        "-- alter table public.users drop column junk;",
        "-- drop index if exists public.users_id_ix;",
        "-- drop index if exists public.users_code_uk;",
        "-- alter table public.users alter column id drop not null;",
    ] {
        assert!(script.contains(commented), "`{commented}` in:\n{script}");
    }
    // Nor is anything handed to the comparer, in either mode.
    assert_eq!(ri_after(&from, &to, false, false), None);
    assert_eq!(ri_after(&from, &to, false, true), None);
    assert!(
        !from
            .get_alter_script_without_triggers_no_indexes(&to, false)
            .contains("replica identity")
    );
}

/// `users (id, code, g)` on USING INDEX `users_code_uk (code)`, with its
/// column list recorded as a pgc 1.1.0 dump does.
fn ri_table_with_known_columns() -> Table {
    let mut table = ri_table(
        true,
        vec![],
        vec![code_uk()],
        Some("i"),
        Some("users_code_uk"),
    );
    table.columns.push(ri_column("g", 3, false));
    table.replica_identity_columns = vec!["code".to_string()];
    table.hash();
    table
}

#[test]
fn replica_identity_stays_on_its_index_when_an_unrelated_column_is_re_created() {
    // g becomes GENERATED ... STORED (DROP COLUMN + ADD COLUMN), but the
    // index is on code only, so the table stays on it throughout: leaving
    // in-txn and returning after COMMIT would leave a table without a
    // primary key with no replica identity in between.
    let from = ri_table_with_known_columns();
    let mut to = from.clone();
    to.columns[2].is_generated = "ALWAYS".to_string();
    to.columns[2].generation_expression = Some("(code * 2)".to_string());
    to.columns[2].generation_type = Some("s".to_string());
    to.hash();

    let script = from.get_alter_script(&to, true);
    assert!(
        script.contains("alter table public.users drop column g;"),
        "{script}"
    );
    assert!(!script.contains("replica identity"), "{script}");
    assert_eq!(ri_after(&from, &to, true, false), None);
    assert_eq!(ri_after(&from, &to, true, true), None);

    // The key column itself still leaves first and waits for the re-created
    // index.
    let mut to = from.clone();
    to.columns[1].is_generated = "ALWAYS".to_string();
    to.columns[1].generation_expression = Some("(id * 2)".to_string());
    to.columns[1].generation_type = Some("s".to_string());
    to.hash();
    let script = from.get_alter_script(&to, true);
    assert!(
        script.starts_with("alter table public.users replica identity default;\n\n"),
        "{script}"
    );
    assert_eq!(
        ri_after(&from, &to, true, false),
        Some(format!("{USING_CODE_UK}\n\n"))
    );
}

#[test]
fn replica_identity_stays_on_its_index_when_another_column_loses_not_null() {
    let from = ri_table_with_known_columns();
    let mut to = from.clone();
    to.columns[0].is_nullable = true;
    to.hash();
    assert_eq!(
        from.get_alter_script(&to, true),
        "alter table public.users alter column id drop not null;\n\n"
    );
}

#[test]
fn replica_identity_leaves_only_for_a_not_null_constraint_on_its_columns() {
    // PG18+ named NOT NULL constraints: dropping the one on id does not
    // touch the index; dropping the one on code does.
    let mut from = ri_table_with_known_columns();
    from.constraints = vec![
        not_null_constraint("users_id_nn", "id"),
        not_null_constraint("users_code_nn", "code"),
    ];
    from.hash();

    let mut to = from.clone();
    to.constraints = vec![not_null_constraint("users_code_nn", "code")];
    to.hash();
    let script = from.get_alter_script(&to, true);
    assert!(
        script.contains("alter table public.users drop constraint users_id_nn;"),
        "{script}"
    );
    assert!(!script.contains("replica identity"), "{script}");

    let mut to = from.clone();
    let mut not_valid = not_null_constraint("users_code_nn", "code");
    not_valid.definition = Some("NOT NULL code NOT VALID".to_string());
    to.constraints = vec![not_null_constraint("users_id_nn", "id"), not_valid];
    to.hash();
    let script = from.get_alter_script(&to, true);
    let reset = position(
        &script,
        "alter table public.users replica identity default;",
    );
    let drop = position(
        &script,
        "alter table public.users drop constraint users_code_nn;",
    );
    let using = position(&script, USING_CODE_UK);
    assert!(reset < drop && drop < using, "{script}");
}

#[test]
fn not_null_constraint_column_reads_the_quote_ident_form() {
    let column = |definition: &str| {
        let mut constraint = not_null_constraint("nn", "x");
        constraint.definition = Some(definition.to_string());
        not_null_constraint_column(&constraint).map(str::to_string)
    };
    assert_eq!(column("NOT NULL code"), Some("code".to_string()));
    assert_eq!(column("NOT NULL code NOT VALID"), Some("code".to_string()));
    assert_eq!(
        column("NOT NULL \"Co\"\"de x\" NO INHERIT"),
        Some("\"Co\"\"de x\"".to_string())
    );
    assert_eq!(column("NOT NULL \"unterminated"), None);
    assert_eq!(column("CHECK (code > 0)"), None);
}

/// `public.users_p1`, a partition of `public.users`, optionally carrying
/// `users_p1_code_idx`: the partition index PostgreSQL attaches to a
/// parent index (`is_partition_index`), or, with `local`, an index of the
/// partition's own.
fn ri_partition(index: Option<bool>, ri: Option<&str>, ri_index_name: Option<&str>) -> Table {
    let indexes = index
        .map(|local| TableIndex {
            schema: "public".to_string(),
            table: "users_p1".to_string(),
            name: "users_p1_code_idx".to_string(),
            catalog: None,
            indexdef: "CREATE UNIQUE INDEX users_p1_code_idx ON public.users_p1 USING btree (code)"
                .to_string(),
            is_partition_index: !local,
            comment: None,
        })
        .into_iter()
        .collect();
    let mut table = Table::new(
        "public".to_string(),
        "users_p1".to_string(),
        "public".to_string(),
        "users_p1".to_string(),
        "postgres".to_string(),
        None,
        vec![ri_column("id", 1, true), ri_column("code", 2, true)],
        vec![],
        indexes,
        vec![],
        None,
    );
    table.partition_of = Some("public.users".to_string());
    table.partition_bound = Some("FOR VALUES FROM (0) TO (100)".to_string());
    table.replica_identity = ri.map(str::to_string);
    table.replica_identity_index = ri_index_name.map(str::to_string);
    table.hash();
    table
}

const USING_P1_CODE_IDX_GUARDED: &str = "do $pgc$
begin
    if exists (
        select 1 from pg_catalog.pg_index
        where indexrelid = to_regclass('public.users_p1_code_idx')
          and indrelid = 'public.users_p1'::regclass
    ) then
        alter table public.users_p1 replica identity using index users_p1_code_idx;
    else
        raise warning '%', 'pgc: replica identity using index users_p1_code_idx not set on public.users_p1: the partition has no index of that name';
    end if;
end
$pgc$;

";

#[test]
fn replica_identity_on_a_new_partitions_inherited_index_waits_for_the_table_section() {
    // PostgreSQL creates users_p1_code_idx when the partition is created
    // under a parent that has the index, or when the parent gains it, and a
    // new partition is created before its parent is altered. So the
    // partition's own CREATE leaves the statement out, and the comparer runs
    // it, guarded, after the table section.
    let to = ri_partition(Some(false), Some("i"), Some("users_p1_code_idx"));
    for script in [
        to.get_script(),
        to.get_script_without_triggers(),
        to.get_script_without_triggers_no_indexes(),
    ] {
        assert!(!script.contains("replica identity"), "{script}");
    }
    assert_eq!(to.replica_identity_create_script_after_indexes(), None);
    assert_eq!(
        to.replica_identity_script_after_tables(None).as_deref(),
        Some(USING_P1_CODE_IDX_GUARDED)
    );

    // The same for the index of a PRIMARY KEY the partition inherits, which
    // the dump lists on the parent only.
    let to = ri_partition(None, Some("i"), Some("users_p1_pkey"));
    assert!(!to.get_script().contains("replica identity"));
    assert!(
        to.replica_identity_script_after_tables(None)
            .is_some_and(|stmt| stmt.contains(
                "alter table public.users_p1 replica identity using index users_p1_pkey;"
            ))
    );
}

#[test]
fn replica_identity_on_a_partitions_inherited_index_it_does_not_have_yet() {
    // The parent gains the index in this migration: the partition's alter
    // leaves USING INDEX to the comparer's pass after the table section.
    let from = ri_partition(None, Some("d"), None);
    let to = ri_partition(Some(false), Some("i"), Some("users_p1_code_idx"));
    assert!(to.replica_identity_waits_for_parent(Some(&from)));
    let script = from.get_alter_script(&to, true);
    assert!(!script.contains("replica identity"), "{script}");
    assert_eq!(ri_after(&from, &to, true, false), None);
    assert_eq!(ri_after(&from, &to, true, true), None);
    assert_eq!(
        to.replica_identity_script_after_tables(Some(&from))
            .as_deref(),
        Some(USING_P1_CODE_IDX_GUARDED)
    );
}

#[test]
fn replica_identity_on_a_partition_index_that_is_there_stays_inline() {
    let from = ri_partition(Some(false), Some("d"), None);
    let to = ri_partition(Some(false), Some("i"), Some("users_p1_code_idx"));
    assert!(!to.replica_identity_waits_for_parent(Some(&from)));
    assert_eq!(to.replica_identity_script_after_tables(Some(&from)), None);
    assert_eq!(
        from.get_alter_script(&to, true),
        "alter table public.users_p1 replica identity using index users_p1_code_idx;\n\n"
    );

    // An inherited constraint's index is known once the FROM dump names it.
    let from = ri_partition(None, Some("i"), Some("users_p1_pkey"));
    let to = ri_partition(None, Some("i"), Some("users_p1_pkey"));
    assert_eq!(to.replica_identity_script_after_tables(Some(&from)), None);
}

#[test]
fn replica_identity_on_a_partitions_own_index_is_not_deferred() {
    // A local (not attached) index is the partition's to create.
    let to = ri_partition(Some(true), Some("i"), Some("users_p1_code_idx"));
    assert!(
        to.get_script().contains(
            "alter table public.users_p1 replica identity using index users_p1_code_idx;"
        )
    );
    assert_eq!(to.replica_identity_script_after_tables(None), None);
    // Nor is a table that is not a partition.
    let table = ri_table(
        true,
        vec![],
        vec![code_uk()],
        Some("i"),
        Some("users_code_uk"),
    );
    assert_eq!(table.replica_identity_script_after_tables(None), None);
}

#[test]
fn replica_identity_guard_escapes_quotes_in_names() {
    let mut to = ri_partition(Some(false), Some("i"), Some("\"it's\""));
    to.indexes[0].name = "\"it's\"".to_string();
    to.name = "\"p'1\"".to_string();
    let stmt = to.replica_identity_script_after_tables(None).unwrap();
    assert!(stmt.contains("to_regclass('public.\"it''s\"')"), "{stmt}");
    assert!(stmt.contains("'public.\"p''1\"'::regclass"), "{stmt}");
    assert!(
        stmt.contains("alter table public.\"p'1\" replica identity using index \"it's\";"),
        "{stmt}"
    );
}

// The guard is a DO block, and its body carries the partition and index
// names. With the fixed `$pgc$` tag, a partition named with `$pgc$` ended the
// dollar quote and ran the rest of its name as SQL.
#[test]
fn replica_identity_guard_picks_a_dollar_tag_no_name_contains() {
    let hostile = "\"x$pgc$;drop table probe.victim;do $pgc$\"";
    let mut to = ri_partition(Some(false), Some("i"), Some("users_p1_code_idx"));
    to.name = hostile.to_string();
    let stmt = to.replica_identity_script_after_tables(None).unwrap();
    assert!(stmt.starts_with("do $pgc1$\nbegin\n"), "{stmt}");
    assert!(stmt.ends_with("\nend\n$pgc1$;\n\n"), "{stmt}");
    assert_eq!(stmt.matches("$pgc1$").count(), 2, "{stmt}");
    assert!(
        stmt.contains(&format!(
            "alter table public.{hostile} replica identity using index users_p1_code_idx;"
        )),
        "{stmt}"
    );

    // A name that holds the next tag as well moves it on again.
    to.name = "\"a$pgc$b$pgc1$\"".to_string();
    let stmt = to.replica_identity_script_after_tables(None).unwrap();
    assert!(stmt.starts_with("do $pgc2$\n"), "{stmt}");
    assert!(stmt.ends_with("$pgc2$;\n\n"), "{stmt}");

    // Ordinary names keep the `$pgc$` block byte for byte.
    let to = ri_partition(Some(false), Some("i"), Some("users_p1_code_idx"));
    assert_eq!(
        to.replica_identity_script_after_tables(None).as_deref(),
        Some(USING_P1_CODE_IDX_GUARDED)
    );
}

#[test]
fn replica_identity_columns_are_not_hashed() {
    // They follow from the index, whose definition is hashed already.
    let from = ri_table(
        true,
        vec![],
        vec![code_uk()],
        Some("i"),
        Some("users_code_uk"),
    );
    let mut with_columns = from.clone();
    with_columns.replica_identity_columns = vec!["code".to_string()];
    with_columns.hash();
    assert_eq!(from.hash, with_columns.hash);
}

// ---- PG17/PG19: TableAlterCaps threading ----

/// `public.users` with a CHECK constraint in the given state and a STORED
/// generated column `total` with the given expression.
fn caps_table(check_def: &str, check_enforced: bool, total_expr: &str) -> Table {
    let mut total = base_column("total", 3);
    total.data_type = "integer".to_string();
    total.is_generated = "ALWAYS".to_string();
    total.generation_expression = Some(total_expr.to_string());
    total.generation_type = Some("s".to_string());
    let mut check = check_constraint("users_qty_check", check_def);
    check.is_enforced = check_enforced;
    Table::new(
        "public".to_string(),
        "users".to_string(),
        "public".to_string(),
        "users".to_string(),
        "postgres".to_string(),
        None,
        vec![identity_column("id", 1, "integer"), total],
        vec![check],
        vec![],
        vec![],
        None,
    )
}

#[test]
fn table_alter_caps_default_matches_the_capability_less_methods() {
    let from = caps_table("CHECK (id > 0)", true, "(id * 2)");
    let to = caps_table("CHECK (id > 0) NOT ENFORCED", false, "(id * 3)");
    let default = TableAlterCaps::default();
    assert_eq!(
        from.get_alter_script(&to, true),
        from.get_alter_script_with_caps(&to, true, default)
    );
    assert_eq!(
        from.get_alter_script_without_triggers(&to, true),
        from.get_alter_script_without_triggers_with_caps(&to, true, default)
    );
    assert_eq!(
        from.get_alter_script_without_triggers_no_indexes(&to, true),
        from.get_alter_script_without_triggers_no_indexes_with_caps(&to, true, default)
    );
}

#[test]
fn table_alter_caps_default_drops_and_re_adds() {
    // PostgreSQL 14-16 / unknown version: CHECK flip = drop + re-add, STORED
    // expression change = drop column + add column.
    let from = caps_table("CHECK (id > 0)", true, "(id * 2)");
    let to = caps_table("CHECK (id > 0) NOT ENFORCED", false, "(id * 3)");
    let script = from.get_alter_script(&to, true);
    assert!(
        script.contains("alter table public.users drop constraint users_qty_check;"),
        "{script}"
    );
    assert!(
        script.contains(
            "alter table public.users add constraint users_qty_check check (id > 0) not enforced ;"
        ),
        "{script}"
    );
    assert!(
        script.contains("alter table public.users drop column total;"),
        "{script}"
    );
    assert!(!script.contains("alter constraint"), "{script}");
    assert!(!script.contains("set expression"), "{script}");
    assert!(!script.contains("add generated"), "{script}");
}

#[test]
fn table_alter_caps_pg19_alters_in_place() {
    let from = caps_table("CHECK (id > 0)", true, "(id * 2)");
    let to = caps_table("CHECK (id > 0) NOT ENFORCED", false, "(id * 3)");
    let caps = TableAlterCaps {
        constraint: ConstraintAlterCaps {
            check_enforceability: true,
        },
        generated: GeneratedAlterCaps {
            stored_set_expression: true,
            virtual_set_expression: true,
        },
    };
    let script = from.get_alter_script_with_caps(&to, true, caps);
    // The relaxing flip runs before the rewrite (see
    // `check_enforceability_flips_bracket_the_column_changes`).
    assert_eq!(
        script,
        "alter table public.users alter constraint users_qty_check not enforced;\n\n\
         alter table only public.users alter column total set expression as (id * 3);\n\n"
    );
    // The reverse direction re-validates in place, after the rewrite.
    let back = to.get_alter_script_with_caps(&from, true, caps);
    assert_eq!(
        back,
        "alter table only public.users alter column total set expression as (id * 2);\n\n\
         alter table public.users alter constraint users_qty_check enforced;\n\n"
    );
}

#[test]
fn table_alter_caps_are_independent() {
    // PostgreSQL 17/18: SET EXPRESSION for STORED, but CHECK flips still drop
    // and re-add.
    let from = caps_table("CHECK (id > 0)", true, "(id * 2)");
    let to = caps_table("CHECK (id > 0) NOT ENFORCED", false, "(id * 3)");
    let caps = TableAlterCaps {
        constraint: ConstraintAlterCaps::default(),
        generated: GeneratedAlterCaps {
            stored_set_expression: true,
            virtual_set_expression: false,
        },
    };
    let script = from.get_alter_script_with_caps(&to, true, caps);
    assert!(script.contains("set expression as (id * 3);"), "{script}");
    assert!(
        script.contains("drop constraint users_qty_check;"),
        "{script}"
    );
    assert!(!script.contains("alter constraint"), "{script}");
}

// ---- Review fixes: ordering of generated-column statements, CHECK
// fallback next to a re-created column, inheritance families ----

const PG19_TABLE_CAPS: TableAlterCaps = TableAlterCaps {
    constraint: ConstraintAlterCaps {
        check_enforceability: true,
    },
    generated: GeneratedAlterCaps {
        stored_set_expression: true,
        virtual_set_expression: true,
    },
};

fn int_col(table: &str, name: &str, ordinal_position: i32) -> TableColumn {
    let mut column = base_column(name, ordinal_position);
    column.table = table.to_string();
    column.data_type = "integer".to_string();
    column
}

fn generated_col(
    table: &str,
    name: &str,
    ordinal_position: i32,
    kind: &str,
    expr: &str,
) -> TableColumn {
    let mut column = int_col(table, name, ordinal_position);
    column.is_generated = "ALWAYS".to_string();
    column.generation_expression = Some(expr.to_string());
    column.generation_type = Some(kind.to_string());
    column
}

fn table_of(name: &str, columns: Vec<TableColumn>, constraints: Vec<TableConstraint>) -> Table {
    Table::new(
        "public".to_string(),
        name.to_string(),
        "public".to_string(),
        name.to_string(),
        "postgres".to_string(),
        None,
        columns,
        constraints,
        vec![],
        vec![],
        None,
    )
}

fn position(script: &str, needle: &str) -> usize {
    script
        .find(needle)
        .unwrap_or_else(|| panic!("`{needle}` missing from:\n{script}"))
}

#[test]
fn check_enforceability_is_not_altered_in_place_next_to_a_re_created_column() {
    // Review finding: on PG19 the in-place ALTER CONSTRAINT ran from
    // constraint_post_script, after the DROP COLUMN had CASCADE-dropped the
    // CHECK ("constraint ck_g ... does not exist"). A table that drops and
    // re-adds a column keeps the drop + re-add, which runs before it.
    let mut ck_from = check_constraint("ck_g", "CHECK (g > 0)");
    ck_from.table_name = "t".to_string();
    let mut ck_to = ck_from.clone();
    ck_to.definition = Some("CHECK (g > 0) NOT ENFORCED".to_string());
    ck_to.is_enforced = false;
    let from = table_of(
        "t",
        vec![int_col("t", "id", 1), int_col("t", "g", 2)],
        vec![ck_from],
    );
    let to = table_of(
        "t",
        vec![
            int_col("t", "id", 1),
            generated_col("t", "g", 2, "s", "(id * 2)"),
        ],
        vec![ck_to],
    );

    let script = from.get_alter_script_with_caps(&to, true, PG19_TABLE_CAPS);
    assert!(!script.contains("alter constraint"), "{script}");
    assert!(
        position(&script, "alter table public.t drop constraint ck_g;")
            < position(&script, "alter table public.t drop column g;"),
        "{script}"
    );
    assert!(
        script.contains("alter table public.t add constraint ck_g check (g > 0) not enforced ;"),
        "{script}"
    );

    // Nothing re-created: the flip is in place, as before.
    let to_same_column = table_of(
        "t",
        vec![int_col("t", "id", 1), int_col("t", "g", 2)],
        to.constraints.clone(),
    );
    let script = from.get_alter_script_with_caps(&to_same_column, true, PG19_TABLE_CAPS);
    assert_eq!(
        script,
        "alter table public.t alter constraint ck_g not enforced;\n\n"
    );

    // With use_drop=false the drop + add is only a comment, so the column
    // and its CHECK survive and the in-place form is still the right one.
    let script = from.get_alter_script_with_caps(&to, false, PG19_TABLE_CAPS);
    assert!(
        script.contains("alter table public.t alter constraint ck_g not enforced;"),
        "{script}"
    );
}

#[test]
fn set_expression_and_re_created_columns_follow_every_add_column() {
    // Review finding: the column loop walks TO attnum order, so a generated
    // column whose new expression reads a column the migration adds came
    // out before that ADD COLUMN ("column bonus does not exist").
    let from = table_of(
        "t",
        vec![
            int_col("t", "qty", 1),
            generated_col("t", "total", 2, "s", "(qty * 2)"),
            int_col("t", "legacy", 3),
        ],
        vec![],
    );
    let to = table_of(
        "t",
        vec![
            int_col("t", "qty", 1),
            generated_col("t", "total", 2, "s", "((qty * 2) + bonus)"),
            int_col("t", "bonus", 3),
        ],
        vec![],
    );
    // PG17+: SET EXPRESSION.
    let script = from.get_alter_script_with_caps(&to, true, PG19_TABLE_CAPS);
    let add = position(&script, "alter table public.t add column bonus integer");
    let set = position(
        &script,
        "alter table only public.t alter column total set expression as ((qty * 2) + bonus);",
    );
    let drop = position(&script, "alter table public.t drop column legacy;");
    assert!(add < set && set < drop, "{script}");

    // PG14-16: drop + add, likewise after the ADD COLUMN.
    let script = from.get_alter_script_with_caps(&to, true, TableAlterCaps::default());
    let add = position(&script, "alter table public.t add column bonus integer");
    let recreate = position(&script, "alter table public.t drop column total;");
    let re_add = position(
        &script,
        "alter table public.t add column total integer generated always as ((qty * 2) + bonus) stored;",
    );
    assert!(add < recreate && recreate < re_add, "{script}");
}

#[test]
fn set_expression_recurses_only_on_a_partitioned_table() {
    // An inheritance child is diffed on its own and may keep an expression
    // of its own, so a plain table uses ONLY. A partitioned table has to
    // recurse: its partitions' columns are never diffed.
    let from = table_of(
        "pt",
        vec![generated_col("pt", "v", 1, "v", "(1 * 2)")],
        vec![],
    );
    let to = table_of(
        "pt",
        vec![generated_col("pt", "v", 1, "v", "(1 * 3)")],
        vec![],
    );
    assert_eq!(
        from.get_alter_script_with_caps(&to, true, PG19_TABLE_CAPS),
        "alter table only public.pt alter column v set expression as (1 * 3);\n\n"
    );
    let mut from = from;
    let mut to = to;
    from.partition_key = Some("RANGE (v)".to_string());
    to.partition_key = Some("RANGE (v)".to_string());
    assert_eq!(
        from.get_alter_script_with_caps(&to, true, PG19_TABLE_CAPS),
        "alter table public.pt alter column v set expression as (1 * 3);\n\n"
    );
}

#[test]
fn inherited_recreates_drop_descendant_copies_between_drop_and_add() {
    // Review finding: a parent's DROP COLUMN recurses (a merged child copy
    // survives as a local column), and its ADD COLUMN merges into whatever a
    // child still has. Children that follow the parent lose their copy in
    // between; the children then skip their own drop + add, which failed
    // with "cannot drop inherited column" and left a merged copy stale.
    let from = table_of(
        "gp",
        vec![
            int_col("gp", "qty", 1),
            generated_col("gp", "total", 2, "s", "(qty * 2)"),
        ],
        vec![],
    );
    let to = table_of(
        "gp",
        vec![
            int_col("gp", "qty", 1),
            generated_col("gp", "total", 2, "s", "(qty * 3)"),
        ],
        vec![],
    );
    let mut inherited = InheritedRecreates::default();
    inherited.descendant_drops.insert(
        "total".to_string(),
        vec!["public.gc".to_string(), "public.gg".to_string()],
    );
    let script = from
        .get_alter_script_in_family(&to, true, false, TableAlterCaps::default(), &inherited)
        .0;
    assert_eq!(
        script,
        "alter table public.gp drop column total;\n\n\
         alter table public.gc drop column if exists total;\n\n\
         alter table public.gg drop column if exists total;\n\n\
         alter table public.gp add column total integer generated always as (qty * 3) stored;\n\n"
    );
    // Without the plan the table's own script is unchanged.
    assert_eq!(
        from.get_alter_script_in_family(
            &to,
            true,
            false,
            TableAlterCaps::default(),
            &InheritedRecreates::default()
        )
        .0,
        from.get_alter_script_without_triggers(&to, true)
    );

    // The follower: no drop, no add — but what ADD COLUMN does not copy
    // (the comment, a statistics change) is still its to set.
    let child_from = table_of(
        "gc",
        vec![
            int_col("gc", "qty", 1),
            generated_col("gc", "total", 2, "s", "(qty * 2)"),
        ],
        vec![],
    );
    let mut child_total = generated_col("gc", "total", 2, "s", "(qty * 3)");
    child_total.comment = Some("child copy".to_string());
    child_total.statistics_target = Some(300);
    let child_to = table_of("gc", vec![int_col("gc", "qty", 1), child_total], vec![]);
    let mut follower = InheritedRecreates::default();
    follower.recreated_by_ancestor.insert("total".to_string());
    let script = child_from
        .get_alter_script_in_family(&child_to, true, false, TableAlterCaps::default(), &follower)
        .0;
    assert_eq!(
        script,
        "alter table public.gc alter column total set statistics 300;\n\n\
         comment on column public.gc.total is 'child copy';\n\n"
    );
}

#[test]
fn constraint_drops_say_if_exists_where_a_parent_already_dropped_the_column() {
    // A partition's copy of a re-created column, and a follower's (see
    // `InheritedRecreates`), is dropped by the parent's recursing DROP COLUMN
    // before this table is altered, and the table's own CHECK on it goes with
    // it: a plain DROP CONSTRAINT then failed ("constraint ... does not
    // exist", verified live on PostgreSQL 16). The root's own drop runs
    // before its DROP COLUMN, so it keeps the plain form.
    let check = |table: &str, definition: &str| {
        let mut ck = check_constraint(&format!("{table}_total_check"), definition);
        ck.table_name = table.to_string();
        ck
    };
    let member = |name: &str, expr: &str, definition: &str| {
        table_of(
            name,
            vec![
                int_col(name, "qty", 1),
                generated_col(name, "total", 2, "s", expr),
            ],
            vec![check(name, definition)],
        )
    };

    // A partition, on a server without STORED SET EXPRESSION.
    let mut from = member("pt_1", "(qty * 2)", "CHECK (total >= 0)");
    let mut to = member("pt_1", "(qty * 3)", "CHECK (total > '-1'::integer)");
    for table in [&mut from, &mut to] {
        table.partition_of = Some("public.pt".to_string());
        table.partition_bound = Some("FOR VALUES FROM (0) TO (100)".to_string());
    }
    let script = from.get_alter_script_with_caps(&to, true, TableAlterCaps::default());
    assert_eq!(
        script,
        "alter table public.pt_1 drop constraint if exists pt_1_total_check;\n\n\
         alter table public.pt_1 add constraint pt_1_total_check check (total > '-1'::integer) ;\n\n"
    );
    // With SET EXPRESSION nothing is re-created, and the drop is plain.
    let script = from.get_alter_script_with_caps(&to, true, PG19_TABLE_CAPS);
    assert!(
        script.starts_with("alter table public.pt_1 drop constraint pt_1_total_check;"),
        "{script}"
    );

    // An INHERITS follower, and the same table as a root.
    let from = member("fc", "(qty * 2)", "CHECK (total >= 0)");
    let to = member("fc", "(qty * 3)", "CHECK (total > '-1'::integer)");
    let mut follower = InheritedRecreates::default();
    follower.recreated_by_ancestor.insert("total".to_string());
    let script = from
        .get_alter_script_in_family(&to, true, false, TableAlterCaps::default(), &follower)
        .0;
    assert_eq!(
        script,
        "alter table public.fc drop constraint if exists fc_total_check;\n\n\
         alter table public.fc add constraint fc_total_check check (total > '-1'::integer) ;\n\n"
    );
    let script = from.get_alter_script_with_caps(&to, true, TableAlterCaps::default());
    assert!(
        position(
            &script,
            "alter table public.fc drop constraint fc_total_check;"
        ) < position(&script, "alter table public.fc drop column total;"),
        "{script}"
    );
    assert!(!script.contains("if exists"), "{script}");
}

#[test]
fn check_enforceability_flips_bracket_the_column_changes() {
    // Review finding (PG19): the in-place flip always ran after the column
    // changes. An ALTER COLUMN ... TYPE or SET EXPRESSION rewrites the table
    // and checks every row against the CHECK constraints still enforced, so
    // a CHECK relaxed in the same migration failed the rewrite ("check
    // constraint ... is violated by some row"). NOT ENFORCED now goes
    // first; ENFORCED, which validates, still waits for the final values.
    let check = |enforced: bool| {
        let mut c = check_constraint(
            "ck_c",
            if enforced {
                "CHECK (c <> 1)"
            } else {
                "CHECK (c <> 1) NOT ENFORCED"
            },
        );
        c.table_name = "t".to_string();
        c.is_enforced = enforced;
        c
    };
    let typed = |data_type: &str, enforced: bool| {
        let mut c = int_col("t", "c", 2);
        c.data_type = data_type.to_string();
        table_of("t", vec![int_col("t", "id", 1), c], vec![check(enforced)])
    };
    let relax = "alter table public.t alter constraint ck_c not enforced;";
    let tighten = "alter table public.t alter constraint ck_c enforced;";
    let retype = "alter table public.t alter column c type bigint;";

    // Relaxed with a type change: the flip runs first.
    let script = typed("integer", true).get_alter_script_with_caps(
        &typed("bigint", false),
        true,
        PG19_TABLE_CAPS,
    );
    assert!(
        position(&script, relax) < position(&script, retype),
        "{script}"
    );
    // Tightened with a type change: the flip validates the retyped rows.
    let script = typed("integer", false).get_alter_script_with_caps(
        &typed("bigint", true),
        true,
        PG19_TABLE_CAPS,
    );
    assert!(
        position(&script, retype) < position(&script, tighten),
        "{script}"
    );

    // The same with a VIRTUAL expression change (SET EXPRESSION on PG19).
    let virtual_table = |expr: &str, enforced: bool| {
        table_of(
            "t",
            vec![int_col("t", "id", 1), generated_col("t", "c", 2, "v", expr)],
            vec![check(enforced)],
        )
    };
    let set = "alter table only public.t alter column c set expression as (id * 3);";
    let script = virtual_table("(id * 2)", true).get_alter_script_with_caps(
        &virtual_table("(id * 3)", false),
        true,
        PG19_TABLE_CAPS,
    );
    assert!(
        position(&script, relax) < position(&script, set),
        "{script}"
    );
    let script = virtual_table("(id * 2)", false).get_alter_script_with_caps(
        &virtual_table("(id * 3)", true),
        true,
        PG19_TABLE_CAPS,
    );
    assert!(
        position(&script, set) < position(&script, tighten),
        "{script}"
    );

    // A comment travels with the flip; a comment-only change stays last.
    let mut commented = typed("bigint", false);
    commented.constraints[0].comment = Some("relaxed".to_string());
    let script =
        typed("integer", true).get_alter_script_with_caps(&commented, true, PG19_TABLE_CAPS);
    let comment = "comment on constraint ck_c on public.t is 'relaxed';";
    assert!(
        position(&script, relax) < position(&script, comment)
            && position(&script, comment) < position(&script, retype),
        "{script}"
    );
    let mut commented = typed("bigint", true);
    commented.constraints[0].comment = Some("kept".to_string());
    let script =
        typed("integer", true).get_alter_script_with_caps(&commented, true, PG19_TABLE_CAPS);
    assert!(
        position(&script, retype)
            < position(&script, "comment on constraint ck_c on public.t is 'kept';"),
        "{script}"
    );

    // Without the capability (PG14-18) nothing changes: drop first, add last.
    let script = typed("integer", true).get_alter_script_with_caps(
        &typed("bigint", false),
        true,
        TableAlterCaps::default(),
    );
    let drop = position(&script, "alter table public.t drop constraint ck_c;");
    let add = position(
        &script,
        "alter table public.t add constraint ck_c check (c <> 1) not enforced ;",
    );
    assert!(
        drop < position(&script, retype) && position(&script, retype) < add,
        "{script}"
    );
}

#[test]
fn ancestor_rewrite_pre_script_takes_the_local_check_relaxes_and_drops() {
    // Review finding: a partition's local CHECK relaxed (PG19) or dropped
    // (drop + re-add, every version) in the partition's own alter ran after
    // the parent's recursing ALTER COLUMN ... TYPE, which checked the
    // rewritten rows against it. With `rewritten_by_ancestor` those
    // statements leave the alter for `ancestor_rewrite_pre_script`, which
    // the comparer runs ahead of every table alter.
    let check = |name: &str, definition: &str, enforced: bool, coninhcount: i32| {
        let mut c = check_constraint(name, definition);
        c.table_name = "p1".to_string();
        c.is_enforced = enforced;
        c.coninhcount = coninhcount;
        c
    };
    let partition = |data_type: &str, constraints: Vec<TableConstraint>| {
        let mut c = int_col("p1", "c", 2);
        c.data_type = data_type.to_string();
        let mut table = table_of("p1", vec![int_col("p1", "id", 1), c], constraints);
        table.partition_of = Some("public.p".to_string());
        table.partition_bound = Some("FOR VALUES FROM (0) TO (10)".to_string());
        table
    };
    let from = partition(
        "integer",
        vec![
            check("ck_relax", "CHECK (c <> 1)", true, 0),
            check("ck_tight", "CHECK (c <> 2) NOT ENFORCED", false, 0),
            check("ck_redef", "CHECK (c <> 3)", true, 0),
            check("ck_gone", "CHECK (c <> 4)", true, 0),
        ],
    );
    let to = partition(
        "bigint",
        vec![
            check("ck_relax", "CHECK (c <> 1) NOT ENFORCED", false, 0),
            check("ck_tight", "CHECK (c <> 2)", true, 0),
            check("ck_redef", "CHECK (c <> 30)", true, 0),
        ],
    );
    let rewritten = InheritedRecreates {
        rewritten_by_ancestor: true,
        ..InheritedRecreates::default()
    };
    let unflagged = InheritedRecreates::default();
    let family = |caps: TableAlterCaps, inherited: &InheritedRecreates| {
        from.get_alter_script_in_family(&to, true, false, caps, inherited)
            .0
    };

    // PG19: the relax and the drops go ahead; the tighten and the re-add stay.
    assert_eq!(
        from.ancestor_rewrite_pre_script(&to, true, PG19_TABLE_CAPS, &rewritten),
        "alter table public.p1 alter constraint ck_relax not enforced;\n\n\
         alter table public.p1 drop constraint ck_redef;\n\n\
         alter table public.p1 drop constraint ck_gone;\n\n"
    );
    assert_eq!(
        family(PG19_TABLE_CAPS, &rewritten),
        "alter table public.p1 alter constraint ck_tight enforced;\n\n\
         alter table public.p1 add constraint ck_redef check (c <> 30) ;\n\n"
    );
    // Before PG19 the flips are a drop + re-add too.
    assert_eq!(
        from.ancestor_rewrite_pre_script(&to, true, TableAlterCaps::default(), &rewritten),
        "alter table public.p1 drop constraint ck_relax;\n\n\
         alter table public.p1 drop constraint ck_tight;\n\n\
         alter table public.p1 drop constraint ck_redef;\n\n\
         alter table public.p1 drop constraint ck_gone;\n\n"
    );
    let script = family(TableAlterCaps::default(), &rewritten);
    assert!(!script.contains("drop constraint"), "{script}");
    assert!(
        script.contains(
            "alter table public.p1 add constraint ck_relax check (c <> 1) not enforced ;"
        ),
        "{script}"
    );
    // Without use_drop the drops are commented out, wherever they go.
    assert_eq!(
        from.ancestor_rewrite_pre_script(&to, false, PG19_TABLE_CAPS, &rewritten),
        "alter table public.p1 alter constraint ck_relax not enforced;\n\n\
         -- alter table public.p1 drop constraint ck_redef;\n-- \n\
         -- alter table public.p1 drop constraint ck_gone;\n-- \n"
    );

    // Unflagged, nothing moves: the table's own alter is what it was.
    assert_eq!(
        from.ancestor_rewrite_pre_script(&to, true, PG19_TABLE_CAPS, &unflagged),
        ""
    );
    for caps in [PG19_TABLE_CAPS, TableAlterCaps::default()] {
        assert_eq!(
            family(caps, &unflagged),
            from.get_alter_script_without_triggers_with_caps(&to, true, caps)
        );
    }

    // An inheritance child's merged copy (coninhcount > 0) follows its
    // parent's statement instead, so it stays in the child's alter.
    let child = |enforced: bool| {
        let mut table = table_of(
            "p1",
            vec![int_col("p1", "c", 1)],
            vec![check(
                "ck_merged",
                if enforced {
                    "CHECK (c <> 1)"
                } else {
                    "CHECK (c <> 1) NOT ENFORCED"
                },
                enforced,
                1,
            )],
        );
        table.inherits_from = vec!["public.p".to_string()];
        table
    };
    assert_eq!(
        child(true).ancestor_rewrite_pre_script(&child(false), true, PG19_TABLE_CAPS, &rewritten),
        ""
    );
    assert_eq!(
        child(true)
            .get_alter_script_in_family(&child(false), true, false, PG19_TABLE_CAPS, &rewritten)
            .0,
        "alter table public.p1 alter constraint ck_merged not enforced;\n\n"
    );
}

// ---- pre-columns: partition trigger clones -------------------------------

#[test]
fn pre_columns_triggers_query_reads_clones_by_tgparentid_from_pg13() {
    for version in [130000, 140000, 150000, 180000, 190000] {
        let query = Table::build_triggers_bulk_query("('s1')", version);
        assert!(query.contains("t.tgparentid <> 0 as is_clone"), "{query}");
        // Own triggers as before; clones whatever their tgisinternal (true
        // on 13-14, false from 15), each with whether its state is its own
        // and the table of the trigger it was cloned from, for
        // `recorded_trigger_clones` to choose from.
        assert!(
            query.contains("(t.tgparentid = 0 and t.tgisinternal = false)"),
            "{query}"
        );
        assert!(
            query.contains("left join pg_trigger u on u.oid = t.tgparentid"),
            "{query}"
        );
        assert!(
            query.contains(
                "coalesce(t.tgenabled <> u.tgenabled or d.description is not null, false)\n                    as clone_own_state"
            ),
            "{query}"
        );
        assert!(
            query.contains("left join pg_class uc on uc.oid = u.tgrelid"),
            "{query}"
        );
        assert!(query.contains("un.nspname as parent_raw_schema"), "{query}");
        assert!(
            query.contains("uc.relname as parent_raw_table_name"),
            "{query}"
        );
        // Every clone is read: the state filter is not in the WHERE clause.
        let filter = &query[query.find("where").unwrap()..];
        assert!(!filter.contains("u.tgenabled"), "{query}");
        // Never a foreign key's internal clones.
        assert!(
            query.contains("where k.oid = t.tgconstraint and k.contype <> 't'"),
            "{query}"
        );
        assert!(query.contains("n.nspname IN ('s1')"), "{query}");
    }
}

#[test]
fn pre_columns_triggers_query_before_pg13_has_no_tgparentid() {
    let query = Table::build_triggers_bulk_query("('s1')", 120000);
    assert!(!query.contains("tgparentid"), "{query}");
    assert!(query.contains("false as is_clone"), "{query}");
    assert!(query.contains("t.tgisinternal = false"), "{query}");
}

fn clone_of(name: &str, table: &str, enabled: &str, comment: Option<&str>) -> TableTrigger {
    TableTrigger {
        oid: Oid(77),
        name: name.to_string(),
        definition: format!(
            "CREATE TRIGGER {name} BEFORE INSERT ON public.{table} FOR EACH ROW EXECUTE FUNCTION log_user()"
        ),
        enabled: enabled.to_string(),
        comment: comment.map(str::to_string),
    }
}

#[test]
fn pre_columns_trigger_clones_are_hashed_only_when_recorded() {
    // Pinned: the hash pgc 1.1.0 gave this table before clones were
    // recorded apart. A table without a recorded clone keeps it.
    let table = basic_table();
    assert_eq!(
        table.hash.as_deref(),
        Some("fd67e130e28ceb6bb23efa0a9144765bea42ed824b0394b4e3a284417ab465af")
    );
    let mut with_clone = table.clone();
    with_clone.trigger_clones = vec![clone_of("audit_parent", "users", "D", None)];
    with_clone.hash();
    assert_ne!(table.hash, with_clone.hash);
    // A clone is not the same as a trigger of the table's own.
    let mut as_own = table.clone();
    as_own
        .triggers
        .push(clone_of("audit_parent", "users", "D", None));
    as_own.hash();
    assert_ne!(as_own.hash, with_clone.hash);
    // The enabled state and the comment count.
    let mut enabled = with_clone.clone();
    enabled.trigger_clones[0].enabled = "A".to_string();
    enabled.hash();
    assert_ne!(enabled.hash, with_clone.hash);
    let mut commented = with_clone.clone();
    commented.trigger_clones[0].comment = Some("mine".to_string());
    commented.hash();
    assert_ne!(commented.hash, with_clone.hash);
}

#[test]
fn pre_columns_trigger_clones_are_never_created_or_dropped_by_the_table() {
    let mut from = basic_table();
    from.partition_of = Some("public.parent".to_string());
    from.partition_bound = Some("FOR VALUES IN (1)".to_string());
    from.trigger_clones = vec![clone_of("audit_parent", "users", "O", Some("old"))];
    from.hash();
    let mut to = from.clone();
    to.trigger_clones = vec![clone_of("audit_parent", "users", "D", None)];
    to.hash();

    for script in [
        to.get_script(),
        to.get_trigger_script(),
        from.get_trigger_alter_script(&to, true),
        from.get_alter_script(&to, true),
        to.get_trigger_alter_script(&from, true),
    ] {
        assert!(!script.contains("audit_parent"), "{script}");
    }
    // The table's own trigger is still created.
    assert!(
        to.get_trigger_script()
            .contains("create trigger audit_user")
    );
}

// ---- pre-columns: a retyped column under a generated column --------------

fn retype_from() -> Table {
    table_of(
        "t",
        vec![
            int_col("t", "id", 1),
            int_col("t", "a", 2),
            generated_col("t", "g", 3, "s", "(a * 2)"),
            int_col("t", "flag", 4),
        ],
        vec![],
    )
}

fn retype_to(g: Option<TableColumn>) -> Table {
    let mut a = int_col("t", "a", 2);
    a.data_type = "bigint".to_string();
    let mut columns = vec![int_col("t", "id", 1), a];
    columns.extend(g);
    columns.push(int_col("t", "flag", 4));
    table_of("t", columns, vec![])
}

#[test]
fn pre_columns_generated_column_leaves_the_way_of_a_retype() {
    // PostgreSQL refuses ALTER COLUMN a TYPE while g reads a, whatever the
    // version and whether or not g changes: g is dropped first and re-added
    // from TO after the retype.
    let from = retype_from();
    let to = retype_to(Some(generated_col("t", "g", 3, "s", "(a * 2)")));
    for caps in [TableAlterCaps::default(), PG19_TABLE_CAPS] {
        assert_eq!(
            from.get_alter_script_with_caps(&to, true, caps),
            "alter table public.t drop column g;\n\n\
             alter table public.t alter column a type bigint;\n\n\
             alter table public.t add column g integer generated always as (a * 2) stored;\n\n"
        );
    }
    assert_eq!(
        from.generated_columns_in_the_way_of_retype(&to, true, GeneratedAlterCaps::default()),
        HashSet::from(["g"])
    );
    assert_eq!(
        from.columns_recreated_for_retype(&to, true, GeneratedAlterCaps::default()),
        HashSet::from(["g".to_string()])
    );

    // VIRTUAL alike.
    let from = table_of(
        "t",
        vec![
            int_col("t", "id", 1),
            int_col("t", "a", 2),
            generated_col("t", "g", 3, "v", "(a * 2)"),
        ],
        vec![],
    );
    let mut a = int_col("t", "a", 2);
    a.data_type = "bigint".to_string();
    let to = table_of(
        "t",
        vec![
            int_col("t", "id", 1),
            a,
            generated_col("t", "g", 3, "v", "(a * 2)"),
        ],
        vec![],
    );
    assert_eq!(
        from.get_alter_script_with_caps(&to, true, PG19_TABLE_CAPS),
        "alter table public.t drop column g;\n\n\
         alter table public.t alter column a type bigint;\n\n\
         alter table public.t add column g integer generated always as (a * 2) virtual;\n\n"
    );
}

#[test]
fn pre_columns_generated_column_change_next_to_a_retype_is_a_drop_and_add() {
    // An expression SET EXPRESSION could change on PG17+ is re-created all
    // the same: it has to leave before the retype.
    let from = retype_from();
    let to = retype_to(Some(generated_col("t", "g", 3, "s", "(a * 3)")));
    let script = from.get_alter_script_with_caps(&to, true, PG19_TABLE_CAPS);
    assert!(!script.contains("set expression"), "{script}");
    assert_eq!(
        script,
        "alter table public.t drop column g;\n\n\
         alter table public.t alter column a type bigint;\n\n\
         alter table public.t add column g integer generated always as (a * 3) stored;\n\n"
    );
}

#[test]
fn pre_columns_generated_column_turning_plain_or_gone_leaves_before_the_retype() {
    let from = retype_from();
    // STORED -> plain keeps its values: DROP EXPRESSION, ahead of the retype.
    let to = retype_to(Some(int_col("t", "g", 3)));
    assert_eq!(
        from.get_alter_script(&to, true),
        "alter table public.t alter column g drop expression;\n\n\
         alter table public.t alter column a type bigint;\n\n"
    );
    assert!(
        from.columns_recreated_for_retype(&to, true, GeneratedAlterCaps::default())
            .is_empty()
    );
    // Gone in TO: dropped ahead of the retype.
    let to = retype_to(None);
    assert_eq!(
        from.get_alter_script(&to, true),
        "alter table public.t drop column g;\n\n\
         alter table public.t alter column a type bigint;\n\n"
    );
}

#[test]
fn pre_columns_dropped_generated_column_goes_before_the_columns_it_reads() {
    // From PostgreSQL 15 dropping `a` first fails (g depends on it); on 14 it
    // takes g along and the drop of g fails.
    let from = retype_from();
    let to = table_of(
        "t",
        vec![int_col("t", "id", 1), int_col("t", "flag", 4)],
        vec![],
    );
    assert_eq!(
        from.get_alter_script(&to, true),
        "alter table public.t drop column g;\n\n\
         alter table public.t drop column a;\n\n"
    );
    // Without a generated column the column order is kept.
    let plain = table_of(
        "t",
        vec![
            int_col("t", "id", 1),
            int_col("t", "a", 2),
            int_col("t", "g", 3),
        ],
        vec![],
    );
    let to = table_of("t", vec![int_col("t", "id", 1)], vec![]);
    assert_eq!(
        plain.get_alter_script(&to, true),
        "alter table public.t drop column a;\n\n\
         alter table public.t drop column g;\n\n"
    );
}

#[test]
fn pre_columns_retype_leaves_unrelated_generated_columns_and_old_output_alone() {
    // g reads only id: the retype of `a` does not touch it.
    let from = table_of(
        "t",
        vec![
            int_col("t", "id", 1),
            int_col("t", "a", 2),
            generated_col("t", "g", 3, "s", "(id * 2)"),
        ],
        vec![],
    );
    let mut a = int_col("t", "a", 2);
    a.data_type = "bigint".to_string();
    let to = table_of(
        "t",
        vec![
            int_col("t", "id", 1),
            a,
            generated_col("t", "g", 3, "s", "(id * 2)"),
        ],
        vec![],
    );
    assert_eq!(
        from.get_alter_script(&to, true),
        "alter table public.t alter column a type bigint;\n\n"
    );

    // Without use_drop nothing is dropped, so nothing moves: the output is
    // what it always was.
    let from = retype_from();
    let to = retype_to(Some(generated_col("t", "g", 3, "s", "(a * 2)")));
    assert_eq!(
        from.get_alter_script(&to, false),
        "alter table public.t alter column a type bigint;\n\n"
    );
    assert!(
        from.generated_columns_in_the_way_of_retype(&to, false, GeneratedAlterCaps::default())
            .is_empty()
    );
}

#[test]
fn pre_columns_retype_on_a_partitioned_parent_and_its_partition() {
    // The parent's drop, retype and add recurse; the partition's own alter
    // leaves them to it but still counts the column as re-created.
    let mut from = retype_from();
    let mut to = retype_to(Some(generated_col("t", "g", 3, "s", "(a * 2)")));
    from.partition_key = Some("RANGE (id)".to_string());
    to.partition_key = Some("RANGE (id)".to_string());
    assert_eq!(
        from.get_alter_script(&to, true),
        "alter table public.t drop column g;\n\n\
         alter table public.t alter column a type bigint;\n\n\
         alter table public.t add column g integer generated always as (a * 2) stored;\n\n"
    );

    let mut from = retype_from();
    let mut to = retype_to(Some(generated_col("t", "g", 3, "s", "(a * 2)")));
    for table in [&mut from, &mut to] {
        table.partition_of = Some("public.parent".to_string());
        table.partition_bound = Some("FOR VALUES FROM (0) TO (10)".to_string());
    }
    assert_eq!(from.get_alter_script(&to, true), "");
    assert_eq!(
        from.columns_recreated_for_retype(&to, true, GeneratedAlterCaps::default()),
        HashSet::from(["g".to_string()])
    );
}

// ---- pre-columns: CHECKs an inheritance family re-creates together -------

fn ck(table: &str, definition: &str, enforced: bool, coninhcount: i32) -> TableConstraint {
    let mut constraint = check_constraint("ck_amount", definition);
    constraint.table_name = table.to_string();
    constraint.is_enforced = enforced;
    constraint.coninhcount = coninhcount;
    constraint
}

#[test]
fn pre_columns_dropped_checks_are_those_no_in_place_form_reaches() {
    let from = table_of(
        "ch",
        vec![int_col("ch", "amount", 1)],
        vec![ck("ch", "CHECK (amount > 0)", true, 1)],
    );
    let relaxed = table_of(
        "ch",
        vec![int_col("ch", "amount", 1)],
        vec![ck("ch", "CHECK (amount > 0) NOT ENFORCED", false, 1)],
    );
    let names = |caps: TableAlterCaps, to: &Table, use_drop: bool| -> Vec<String> {
        from.dropped_checks(to, use_drop, caps)
            .into_iter()
            .map(|c| c.name.clone())
            .collect()
    };
    // PG18 and older: a drop + re-add; PG19: in place.
    assert_eq!(
        names(TableAlterCaps::default(), &relaxed, true),
        ["ck_amount"]
    );
    assert!(names(PG19_TABLE_CAPS, &relaxed, true).is_empty());
    // An expression change, and a CHECK TO does not have locally, on any
    // version; a comment is altered in place.
    let changed = table_of(
        "ch",
        vec![int_col("ch", "amount", 1)],
        vec![ck("ch", "CHECK (amount > 1)", true, 1)],
    );
    assert_eq!(names(PG19_TABLE_CAPS, &changed, true), ["ck_amount"]);
    let gone = table_of("ch", vec![int_col("ch", "amount", 1)], vec![]);
    assert_eq!(names(PG19_TABLE_CAPS, &gone, true), ["ck_amount"]);
    let mut commented = from.clone();
    commented.constraints[0].comment = Some("c".to_string());
    assert!(names(TableAlterCaps::default(), &commented, true).is_empty());
    // Without use_drop nothing is dropped.
    assert!(names(TableAlterCaps::default(), &changed, false).is_empty());
}

#[test]
fn pre_columns_family_checks_are_dropped_ahead_and_re_added_in_the_alter() {
    let from = table_of(
        "ch",
        vec![int_col("ch", "amount", 1)],
        vec![ck("ch", "CHECK (amount > 0)", true, 1)],
    );
    let to = table_of(
        "ch",
        vec![int_col("ch", "amount", 1)],
        vec![ck("ch", "CHECK (amount > 0) NOT ENFORCED", false, 1)],
    );
    let mut family = InheritedRecreates::default();
    family.family_checks.insert("ck_amount".to_string());
    for caps in [TableAlterCaps::default(), PG19_TABLE_CAPS] {
        assert_eq!(
            from.ancestor_rewrite_pre_script(&to, true, caps, &family),
            "alter table public.ch drop constraint ck_amount;\n\n"
        );
        // Never in place inside the family, even where the server could.
        assert_eq!(
            from.get_alter_script_in_family(&to, true, false, caps, &family)
                .0,
            "alter table public.ch add constraint ck_amount check (amount > 0) not enforced ;\n\n"
        );
    }
    // An ancestor whose copy does not change drops and re-adds it as it is.
    assert_eq!(
        from.ancestor_rewrite_pre_script(&from, true, TableAlterCaps::default(), &family),
        "alter table public.ch drop constraint ck_amount;\n\n"
    );
    assert_eq!(
        from.get_alter_script_in_family(&from, true, false, TableAlterCaps::default(), &family)
            .0,
        "alter table public.ch add constraint ck_amount check (amount > 0) ;\n\n"
    );
    // Without the plan the table's own alter is what it was.
    assert_eq!(
        from.get_alter_script_in_family(
            &to,
            true,
            false,
            TableAlterCaps::default(),
            &InheritedRecreates::default()
        )
        .0,
        "alter table public.ch drop constraint ck_amount;\n\n\
         alter table public.ch add constraint ck_amount check (amount > 0) not enforced ;\n\n"
    );
}

// ---- pre-columns review fixes ----------------------------------------------

fn clone_row(name: &str, own_state: bool, parent: (&str, &str)) -> TriggerCloneRow {
    TriggerCloneRow {
        trigger: clone_of(name, "p1", "O", None),
        own_state,
        parent: (parent.0.to_string(), parent.1.to_string()),
    }
}

#[test]
fn pre_columns_recorded_trigger_clones_are_every_clone_of_a_parent_not_dumped() {
    // A clone in its parent's state is recorded only when that parent is not
    // dumped (it lives outside the dumped schemas): nothing else tells the
    // state then. One with a state of its own is recorded either way.
    let rows = vec![
        clone_row("own_state", true, ("s", "dumped")),
        clone_row("same_state", false, ("s", "dumped")),
        clone_row("orphan", false, ("outside", "parent")),
        clone_row("orphan_own", true, ("outside", "parent")),
    ];
    let dumped = |key: &(String, String)| key.0 == "s" && key.1 == "dumped";
    let names: Vec<String> = Table::recorded_trigger_clones(rows, dumped)
        .into_iter()
        .map(|t| t.name)
        .collect();
    assert_eq!(names, ["own_state", "orphan", "orphan_own"]);
}

#[test]
fn pre_columns_attached_table_drops_its_own_trigger_before_the_attach() {
    // Review finding: `ATTACH PARTITION` clones the parent's row triggers
    // and failed ("already exists") on the table's own trigger of the same
    // name, whose drop ran after it.
    let mut from = basic_table();
    from.hash();
    let mut to = basic_table();
    to.triggers.clear();
    to.partition_of = Some("public.parent".to_string());
    to.partition_bound = Some("FOR VALUES IN (2)".to_string());
    to.hash();
    assert!(from.attaches_to_another_parent(&to));
    let drop = "drop trigger if exists audit_user on public.users;";
    let attach = "alter table public.parent attach partition public.users FOR VALUES IN (2);";
    let before_attach = InheritedRecreates {
        triggers_dropped_before_attach: HashSet::from(["audit_user".to_string()]),
        ..Default::default()
    };
    let caps = TableAlterCaps::default();
    let (script, _) = from.get_alter_script_in_family(&to, true, false, caps, &before_attach);
    assert_eq!(script.matches(drop).count(), 1, "{script}");
    assert!(
        position(&script, drop) < position(&script, attach),
        "{script}"
    );
    // The comparer's trigger statements (after every table) leave it out.
    assert!(
        !from
            .get_trigger_alter_script_in_family(&to, true, &before_attach)
            .contains(drop)
    );
    // Commented out without use_drop, still ahead of the attach.
    let (script, _) = from.get_alter_script_in_family(&to, false, false, caps, &before_attach);
    assert!(
        position(&script, &format!("-- {drop}")) < position(&script, attach),
        "{script}"
    );
    // A trigger no row trigger of the new parent shares a name with is
    // dropped with the other trigger statements, as before.
    let none = InheritedRecreates::default();
    let (script, _) = from.get_alter_script_in_family(&to, true, false, caps, &none);
    assert!(!script.contains(drop), "{script}");
    assert!(
        from.get_trigger_alter_script_in_family(&to, true, &none)
            .contains(drop)
    );
    assert_eq!(
        from.get_trigger_alter_script(&to, true),
        from.get_trigger_alter_script_in_family(&to, true, &none)
    );

    // Another bound under the same parent, or a detach: as before.
    let mut from_partition = to.clone();
    from_partition.triggers = from.triggers.clone();
    from_partition.partition_bound = Some("FOR VALUES IN (3)".to_string());
    from_partition.hash();
    assert!(!from_partition.attaches_to_another_parent(&to));
    assert!(
        from_partition
            .get_trigger_alter_script_in_family(&to, true, &before_attach)
            .contains(drop)
    );
    let mut standalone = from.clone();
    standalone.triggers.clear();
    standalone.hash();
    assert!(!to.attaches_to_another_parent(&standalone));
    assert!(!from.attaches_to_another_parent(&from));
}

/// An inheritance child `ic` of a parent whose alter retypes `a`: in FROM
/// `a` integer and `g` reading it; in TO both bigint.
fn inherits_child(to: bool) -> Table {
    let mut a = int_col("ic", "a", 2);
    let mut g = generated_col("ic", "g", 3, "s", "(a * 2)");
    if to {
        a.data_type = "bigint".to_string();
        g.data_type = "bigint".to_string();
    }
    let mut table = table_of("ic", vec![int_col("ic", "id", 1), a, g], vec![]);
    table.inherits_from = vec!["public.ip".to_string()];
    table.hash();
    table
}

#[test]
fn pre_columns_column_retyped_by_an_inheritance_ancestor_is_left_to_it() {
    // Review finding: the parent's recursing ALTER COLUMN ... TYPE failed
    // under the child's generated column, and the child's own retype of the
    // inherited column failed too ("cannot alter inherited column").
    let from = inherits_child(false);
    let to = inherits_child(true);
    let family = InheritedRecreates {
        retyped_by_ancestor: HashSet::from(["a".to_string()]),
        rewritten_by_ancestor: true,
        ..Default::default()
    };
    let caps = TableAlterCaps::default();
    let (script, _) = from.get_alter_script_in_family(&to, true, false, caps, &family);
    assert_eq!(
        script,
        "alter table public.ic add column g bigint generated always as (a * 2) stored;\n\n"
    );
    assert_eq!(
        from.ancestor_retype_pre_script(&to, true, caps, &family),
        "alter table public.ic drop column g;\n\n"
    );
    // The CHECK pass is not where it goes.
    assert_eq!(
        from.ancestor_rewrite_pre_script(&to, true, caps, &family),
        ""
    );
    // Turning plain, it keeps its values: DROP EXPRESSION ahead of the parent.
    let mut to_plain = inherits_child(true);
    to_plain.columns[2].is_generated = "NEVER".to_string();
    to_plain.columns[2].generation_expression = None;
    to_plain.hash();
    assert_eq!(
        from.ancestor_retype_pre_script(&to_plain, true, caps, &family),
        "alter table public.ic alter column g type bigint;\n\n\
         alter table public.ic alter column g drop expression;\n\n"
    );
    // Gone in TO: dropped ahead of the parent.
    let mut to_gone = inherits_child(true);
    to_gone.columns.pop();
    to_gone.hash();
    assert_eq!(
        from.ancestor_retype_pre_script(&to_gone, true, caps, &family),
        "alter table public.ic drop column g;\n\n"
    );
    assert!(
        !from
            .get_alter_script_in_family(&to_gone, true, false, caps, &family)
            .0
            .contains("drop column"),
    );
    // Without the plan the child retypes itself, as before.
    let (script, _) =
        from.get_alter_script_in_family(&to, true, false, caps, &InheritedRecreates::default());
    assert!(script.contains("alter table public.ic alter column a type bigint;"));
    assert_eq!(
        from.ancestor_retype_pre_script(&to, true, caps, &InheritedRecreates::default()),
        ""
    );
}

#[test]
fn pre_columns_rewrite_column_drops_are_the_re_created_and_the_dropped_ahead() {
    let from = retype_from();
    // Re-created around the retype.
    let to = retype_to(Some(generated_col("t", "g", 3, "s", "(a * 2)")));
    assert_eq!(
        from.rewrite_column_drops(&to, true, GeneratedAlterCaps::default()),
        ["g"]
    );
    // Gone in TO, dropped ahead of the retype.
    assert_eq!(
        from.rewrite_column_drops(&retype_to(None), true, GeneratedAlterCaps::default()),
        ["g"]
    );
    // Plain in TO: DROP EXPRESSION keeps the column.
    let mut plain = int_col("t", "g", 3);
    plain.data_type = "integer".to_string();
    assert!(
        from.rewrite_column_drops(&retype_to(Some(plain)), true, GeneratedAlterCaps::default())
            .is_empty()
    );
    // Nothing without use_drop.
    assert!(
        from.rewrite_column_drops(&to, false, GeneratedAlterCaps::default())
            .is_empty()
    );
}

#[test]
fn pre_columns_live_re_add_creates_a_named_not_null_under_its_name() {
    // Review finding (PostgreSQL 18+): the re-add's inline NOT NULL created
    // an auto-named constraint, and the restore's `add constraint g_nn not
    // null g` was then refused.
    let mut from = retype_from();
    from.columns[2].is_nullable = false;
    from.constraints = vec![not_null_constraint("g_nn", "g")];
    for c in &mut from.constraints {
        c.table_name = "t".to_string();
    }
    from.hash();
    let mut g = generated_col("t", "g", 3, "s", "(a * 2)");
    g.is_nullable = false;
    let mut to = retype_to(Some(g));
    to.constraints = from.constraints.clone();
    to.hash();
    let script = from.get_alter_script(&to, true);
    assert!(
        script.contains(
            "alter table public.t add column g integer generated always as (a * 2) stored constraint g_nn not null;"
        ),
        "{script}"
    );
    // An auto-named one stays inline, as before (PostgreSQL gives it the
    // same name again).
    let mut to_auto = to.clone();
    to_auto.constraints = vec![not_null_constraint("t_g_not_null", "g")];
    let mut from_auto = from.clone();
    from_auto.constraints = to_auto.constraints.clone();
    for t in [&mut to_auto, &mut from_auto] {
        for c in &mut t.constraints {
            c.table_name = "t".to_string();
        }
        t.hash();
    }
    let script = from_auto.get_alter_script(&to_auto, true);
    assert!(
        script.contains(
            "alter table public.t add column g integer generated always as (a * 2) stored not null;"
        ),
        "{script}"
    );
    // The CREATE TABLE form is unchanged by the shared helper.
    assert!(
        to.get_script()
            .contains("g integer generated always as (a * 2) stored constraint g_nn not null"),
        "{}",
        to.get_script()
    );
}

#[test]
fn pre_columns_foreign_key_dropped_ahead_is_dropped_again_with_if_exists() {
    let fk = |definition: &str| TableConstraint {
        catalog: "postgres".to_string(),
        schema: "public".to_string(),
        name: "ref_gx_fkey".to_string(),
        table_name: "ref".to_string(),
        constraint_type: "FOREIGN KEY".to_string(),
        is_deferrable: false,
        initially_deferred: false,
        definition: Some(definition.to_string()),
        coninhcount: 0,
        is_enforced: true,
        no_inherit: false,
        nulls_not_distinct: false,
        comment: None,
    };
    let mut from = table_of("ref", vec![int_col("ref", "gx", 1)], vec![]);
    from.constraints = vec![fk("FOREIGN KEY (gx) REFERENCES public.t(g)")];
    from.hash();
    let mut to = from.clone();
    to.constraints = vec![fk(
        "FOREIGN KEY (gx) REFERENCES public.t(g) ON DELETE CASCADE",
    )];
    to.hash();
    let ahead = InheritedRecreates {
        dropped_ahead: HashSet::from(["ref_gx_fkey".to_string()]),
        ..Default::default()
    };
    let caps = TableAlterCaps::default();
    assert!(
        from.get_alter_script_in_family(&to, true, false, caps, &ahead)
            .0
            .contains("alter table public.ref drop constraint if exists ref_gx_fkey;")
    );
    assert!(
        from.get_alter_script_in_family(&to, true, false, caps, &InheritedRecreates::default())
            .0
            .contains("alter table public.ref drop constraint ref_gx_fkey;")
    );
}

// ==== pre-clear tests ====

/// The capabilities `PgCatalogCaps::detect` finds on each supported major.
fn caps_for(pg_version: i32) -> PgCatalogCaps {
    PgCatalogCaps {
        has_attcompression: pg_version >= 140000,
        has_conenforced: pg_version >= 180000,
        generation_deps_on_column: pg_version < 150000,
    }
}

#[test]
fn constraints_query_never_reads_a_nulls_not_distinct_column() {
    // pg_constraint has no connullsnotdistinct on any version (the flag is
    // pg_index.indnullsnotdistinct, PG15+), and pg_get_constraintdef already
    // prints UNIQUE NULLS NOT DISTINCT, so the query reads neither.
    for version in [140000, 150000, 170000, 180000, 190000] {
        let query = Table::build_constraints_query("('app')", version, caps_for(version));
        assert!(!query.contains("nullsnotdistinct"), "{version}: {query}");
        assert!(!query.contains("nulls_not_distinct"), "{version}: {query}");
        assert!(
            query.contains("pg_get_constraintdef(c.oid, true) AS definition"),
            "{version}"
        );
    }
}

#[test]
fn constraints_query_gates_enforceability_and_named_not_null_on_pg18() {
    let pg17 = Table::build_constraints_query("('app')", 170000, caps_for(170000));
    assert!(!pg17.contains("conenforced"), "{pg17}");
    assert!(
        pg17.contains("c.contype IN ('p','u','f','c','x') AND"),
        "{pg17}"
    );
    for version in [180000, 190000] {
        let query = Table::build_constraints_query("('app')", version, caps_for(version));
        assert!(
            query.contains("c.conenforced AS is_enforced"),
            "{version}: {query}"
        );
        assert!(
            query.contains("c.contype IN ('p','u','f','c','x','n') AND"),
            "{version}: {query}"
        );
    }
    let query = Table::build_constraints_query("('app', 'b')", 190000, caps_for(190000));
    assert!(query.contains("n.nspname IN ('app', 'b') AND"), "{query}");
}

// ==== r3-gencol tests ====

#[test]
fn generation_blocks_retype_is_read_on_postgresql_14_only() {
    // PostgreSQL 14 records a generated column's expression dependencies on
    // the column itself; 15+ on its pg_attrdef row, where they never block
    // ALTER COLUMN ... TYPE.
    let pg14 = Table::generation_blocks_retype_column(caps_for(140000));
    for fragment in [
        "AS generation_blocks_retype",
        "gd.classid = 'pg_class'::regclass",
        "gd.objid = a.attrelid",
        "gd.objsubid = a.attnum",
        "gd.refclassid = 'pg_type'::regclass AND gd.refobjid = a.atttypid",
        "gd.refclassid = 'pg_collation'::regclass AND gd.refobjid = a.attcollation",
        "gd.refobjsubid <> 0",
        "CASE WHEN a.attgenerated <> ''",
    ] {
        assert!(pg14.contains(fragment), "{fragment}: {pg14}");
    }
    for version in [150000, 170000, 180000, 190000] {
        assert_eq!(
            Table::generation_blocks_retype_column(caps_for(version)),
            ""
        );
    }
    let query = Table::build_columns_query("", pg14, "('public')");
    assert!(
        query.contains("pd.description as column_comment,\n                CASE WHEN a.attgenerated <> '' THEN EXISTS"),
        "{query}"
    );
    let query = Table::build_columns_query("", "", "('public')");
    assert!(!query.contains("generation_blocks_retype"), "{query}");
}

// ==== r3-ddl: inline primary key ====

/// `basic_table` with its primary key as `pg_get_constraintdef` renders
/// `definition`, and the `primary key` line of its `CREATE TABLE`.
fn inline_pk_line(definition: &str) -> Option<String> {
    let mut table = basic_table();
    table.constraints[0].definition = Some(definition.to_string());
    let script = table.get_script();
    script
        .lines()
        .find(|l| l.contains(" primary key ("))
        .map(str::to_string)
}

/// Key columns are cut from the definition quote-aware: a `)` or `,` inside a
/// quoted column name is part of the name, not the end of the list or of the
/// column.
#[test]
fn inline_primary_key_keeps_quoted_names_with_parens_and_commas_whole() {
    assert_eq!(
        inline_pk_line("PRIMARY KEY (\"Code (x)\", \"a,b\")").as_deref(),
        Some("    constraint users_pkey primary key (\"Code (x)\", \"a,b\")")
    );
    assert_eq!(
        inline_pk_line("PRIMARY KEY (\"x,y\")").as_deref(),
        Some("    constraint users_pkey primary key (\"x,y\")")
    );
    assert_eq!(
        inline_pk_line("PRIMARY KEY (\"say \"\"hi\"\")\", id)").as_deref(),
        Some("    constraint users_pkey primary key (\"say \"\"hi\"\")\", \"id\")")
    );
}

/// The spelling of an ordinary key is what it has always been: every column
/// double-quoted, and a temporal key's list kept verbatim.
#[test]
fn inline_primary_key_output_is_unchanged_for_ordinary_keys() {
    assert_eq!(
        inline_pk_line("PRIMARY KEY (id)").as_deref(),
        Some("    constraint users_pkey primary key (\"id\")")
    );
    assert_eq!(
        inline_pk_line("PRIMARY KEY (tenant_id, \"Id\")").as_deref(),
        Some("    constraint users_pkey primary key (\"tenant_id\", \"Id\")")
    );
    assert_eq!(
        inline_pk_line("PRIMARY KEY (id, valid_range WITHOUT OVERLAPS)").as_deref(),
        Some("    constraint users_pkey primary key (id, valid_range WITHOUT OVERLAPS)")
    );
}

/// The key comes from the constraint alone: an index whose definition text
/// happens to hold `primary key (` does not stand in for it (pg_get_indexdef
/// never prints those words, so only a name could), and a key without a
/// definition gets no line.
#[test]
fn inline_primary_key_is_read_from_the_constraint_only() {
    let mut table = basic_table();
    table.constraints[0].definition = Some("PRIMARY KEY (id)".to_string());
    table.indexes.push(name_index(
        "CREATE INDEX \"x primary key (name\" ON public.users USING btree (name)",
    ));
    let script = table.get_script();
    assert!(
        script.contains("    constraint users_pkey primary key (\"id\")\n"),
        "{script}"
    );
    assert!(!script.contains("primary key (\"name"), "{script}");

    let mut undefined = basic_table();
    undefined.constraints[0].definition = None;
    assert!(
        !undefined.get_script().contains(" primary key ("),
        "{}",
        undefined.get_script()
    );
}

/// What follows the key list in the definition — `INCLUDE (...)`,
/// `DEFERRABLE INITIALLY DEFERRED` — follows it in `CREATE TABLE` too. It was
/// left out, so the table was created with a different key, and the next
/// compare dropped and re-added it.
#[test]
fn inline_primary_key_keeps_include_and_deferrable() {
    assert_eq!(
        inline_pk_line("PRIMARY KEY (tenant_id, \"Id\") INCLUDE (name)").as_deref(),
        Some("    constraint users_pkey primary key (\"tenant_id\", \"Id\") INCLUDE (name)")
    );
    assert_eq!(
        inline_pk_line("PRIMARY KEY (\"a,b\") INCLUDE (\"x)\") DEFERRABLE INITIALLY DEFERRED")
            .as_deref(),
        Some(
            "    constraint users_pkey primary key (\"a,b\") INCLUDE (\"x)\") DEFERRABLE INITIALLY DEFERRED"
        )
    );
}

#[test]
fn primary_key_column_list_and_split_are_quote_aware() {
    assert_eq!(
        primary_key_column_list("PRIMARY KEY (\"a (b)\", c) INCLUDE (d)"),
        Some(("\"a (b)\", c", "INCLUDE (d)"))
    );
    assert_eq!(
        primary_key_column_list("PRIMARY KEY (id)"),
        Some(("id", ""))
    );
    assert_eq!(primary_key_column_list("PRIMARY KEY (\"unterminated"), None);
    assert_eq!(primary_key_column_list("PRIMARY KEY"), None);
    assert_eq!(
        split_key_columns("\"a,b\", c , \"d\"\"e,\""),
        vec!["\"a,b\"", "c", "\"d\"\"e,\""]
    );
    assert!(is_single_identifier("\"a \"\"b\"\" c\""));
    assert!(is_single_identifier("tenant_id"));
    assert!(!is_single_identifier("valid_range WITHOUT OVERLAPS"));
    assert!(!is_single_identifier("\"a\" \"b\""));
    assert!(!is_single_identifier(""));
}

// ==== r3-ddl review: collation change on a partition-key column ====

/// `data.pl`, partitioned by `LIST (c)` on a `text` column `c` collated
/// `collation` (`pg_catalog`), `c` being `data_type`.
fn partition_key_table(data_type: &str, collation: &str) -> Table {
    let mut c = create_dummy_column("c", data_type);
    c.schema = "data".to_string();
    c.table = "pl".to_string();
    c.collation_schema = Some("pg_catalog".to_string());
    c.collation_name = Some(collation.to_string());
    let mut table = Table::new(
        "data".to_string(),
        "pl".to_string(),
        "data".to_string(),
        "pl".to_string(),
        "owner".to_string(),
        None,
        vec![create_dummy_column("id", "integer"), c],
        vec![],
        vec![],
        vec![],
        None,
    );
    table.partition_key = Some("LIST (c)".to_string());
    table.hash();
    table
}

/// PostgreSQL retypes no partition-key column, not even for a collation
/// alone, so such a change drops and recreates the table like a type change
/// there; its note says it is the collation. A type change keeps the note it
/// always had.
#[test]
fn a_collation_change_on_a_partition_key_column_says_so_in_its_recreate_note() {
    let from = partition_key_table("text", "C");
    let to = partition_key_table("text", "POSIX");
    assert!(from.will_be_dropped_and_recreated(&to));
    let script = from.get_alter_script(&to, true);
    assert!(
        script.starts_with(
            "/* Column c participates in partitioning; collation change requires table recreation. Data loss will occur! */\n\
             drop table if exists data.pl;"
        ),
        "{script}"
    );
    assert!(script.contains("c text collate \"POSIX\""), "{script}");

    let retyped = partition_key_table("varchar", "POSIX");
    let script = from.get_alter_script(&retyped, true);
    assert!(
        script.starts_with(
            "/* Column c participates in partitioning; type change requires table recreation. Data loss will occur! */\n"
        ),
        "{script}"
    );
}

// ---- r3:types — typed tables ----

/// `public.users OF public.user_t` with `columns`.
fn typed_users(columns: Vec<TableColumn>) -> Table {
    let mut table = Table::new(
        "public".to_string(),
        "users".to_string(),
        "public".to_string(),
        "users".to_string(),
        "postgres".to_string(),
        None,
        columns,
        vec![],
        vec![],
        vec![],
        None,
    );
    table.typed_table_type = Some("user_t".to_string());
    table.hash();
    table
}

fn integer_column(name: &str, ordinal_position: i32) -> TableColumn {
    TableColumn {
        data_type: "integer".to_string(),
        numeric_precision: Some(32),
        ..base_column(name, ordinal_position)
    }
}

/// Review finding: `ALTER TYPE … ADD ATTRIBUTE … CASCADE` leaves the new
/// column NULL in every row, so a NOT NULL set on top failed on every run.
/// The rows get the default first, as `ADD COLUMN … DEFAULT` gives them.
#[test]
fn typed_table_fills_a_new_column_with_its_default_before_not_null() {
    let from = typed_users(vec![integer_column("id", 1)]);
    let mut extra = integer_column("extra", 2);
    extra.column_default = Some("7".to_string());
    extra.is_nullable = false;
    let to = typed_users(vec![integer_column("id", 1), extra]);

    let script = from.get_alter_script(&to, true);

    let default = script
        .find("alter table public.users alter column extra set default 7;")
        .unwrap_or_else(|| panic!("{script}"));
    let fill = script
        .find("update public.users set extra = default where extra is null;")
        .unwrap_or_else(|| panic!("{script}"));
    let not_null = script
        .find("alter table public.users alter column extra set not null;")
        .unwrap_or_else(|| panic!("{script}"));
    assert!(default < fill && fill < not_null, "{script}");
    assert!(!script.contains("add column"), "{script}");
    assert_eq!(script.matches("set not null").count(), 1, "{script}");

    // A nullable column with a default is filled too, as ADD COLUMN would.
    let mut nullable = integer_column("extra", 2);
    nullable.column_default = Some("7".to_string());
    let to = typed_users(vec![integer_column("id", 1), nullable]);
    let script = from.get_alter_script(&to, true);
    assert!(
        script.contains("update public.users set extra = default where extra is null;"),
        "{script}"
    );
    assert!(!script.contains("set not null"), "{script}");

    // Without a default nothing is filled: NOT NULL alone, as before.
    let mut no_default = integer_column("extra", 2);
    no_default.is_nullable = false;
    let to = typed_users(vec![integer_column("id", 1), no_default]);
    let script = from.get_alter_script(&to, true);
    assert!(!script.contains("update "), "{script}");
    assert!(
        script.contains("alter table public.users alter column extra set not null;"),
        "{script}"
    );
}

#[test]
fn typed_column_changes_lists_what_only_the_type_can_change() {
    let from = typed_users(vec![
        integer_column("id", 1),
        integer_column("gone", 2),
        integer_column("keep", 3),
    ]);
    let mut id = integer_column("id", 1);
    id.data_type = "bigint".to_string();
    id.numeric_precision = Some(64);
    let to = typed_users(vec![
        id,
        integer_column("keep", 3),
        integer_column("new", 4),
    ]);

    assert_eq!(
        from.typed_column_changes(&to),
        vec![
            "retyped id (integer -> bigint)".to_string(),
            "added new".to_string(),
            "dropped gone".to_string(),
        ]
    );
    // Nothing for an equal column list, or for a table that is not typed by
    // the same type on both sides.
    assert!(from.typed_column_changes(&from).is_empty());
    let mut plain = to.clone();
    plain.typed_table_type = None;
    assert!(from.typed_column_changes(&plain).is_empty());
    assert!(plain.typed_column_changes(&from).is_empty());
}

// ==== r3-constraints tests ====

/// `public.users` with an integer `id` and the NOT NULL `b`, and the given
/// NOT NULL constraints.
fn nn_table(name: &str, constraints: Vec<TableConstraint>) -> Table {
    let mut b = int_col(name, "b", 2);
    b.is_nullable = false;
    let mut constraints = constraints;
    for constraint in &mut constraints {
        constraint.table_name = name.to_string();
    }
    table_of(name, vec![int_col(name, "id", 1), b], constraints)
}

#[test]
fn r3c_drifted_not_null_name_is_left_when_to_s_name_is_generated() {
    // Finding: a NOT NULL constraint whose name no longer follows
    // {table}_{column}_not_null (a column rename kept `t_a_not_null` on `b`)
    // was dropped, clearing NOT NULL on a column TO declares NOT NULL.
    let from = nn_table("t", vec![not_null_constraint("t_a_not_null", "b")]);
    let to = nn_table("t", vec![not_null_constraint("t_b_not_null", "b")]);
    let script = from.get_alter_script(&to, true);
    assert_eq!(script, "", "{script}");
    // A table rename: `old_b_not_null` on `t`.
    let from = nn_table("t", vec![not_null_constraint("old_b_not_null", "b")]);
    assert_eq!(from.get_alter_script(&to, true), "");
}

/// The guarded `RENAME CONSTRAINT` a table's alter emits for one of its
/// NOT NULL constraints (see `Table::guarded_not_null_rename`).
fn guarded_rename(table: &str, from: &str, to: &str, raw_from: &str, column: &str) -> String {
    format!(
        "do $pgc$\n\
         begin\n    \
             if exists (\n        \
                 select 1 from pg_catalog.pg_constraint c\n        \
                 join pg_catalog.pg_attribute a\n          \
                   on a.attrelid = c.conrelid and a.attnum = c.conkey[1]\n        \
                 where c.conrelid = 'public.{table}'::regclass\n          \
                   and c.contype = 'n'\n          \
                   and c.conname = '{raw_from}'\n          \
                   and a.attname = '{column}'\n    \
             ) then\n        \
                 alter table public.{table} rename constraint {from} to {to};\n    \
             end if;\n\
         end\n\
         $pgc$;\n\n"
    )
}

#[test]
fn r3c_not_null_name_chosen_by_to_is_renamed_to() {
    // Finding (mirror): TO names the column's NOT NULL itself; the ADD
    // CONSTRAINT emitted before failed every run, as the column had one.
    let from = nn_table("m", vec![not_null_constraint("m_b_not_null", "b")]);
    let to = nn_table("m", vec![not_null_constraint("my_nn", "b")]);
    let script = from.get_alter_script(&to, true);
    assert_eq!(
        script,
        guarded_rename("m", "m_b_not_null", "my_nn", "m_b_not_null", "b")
    );
    // Both names chosen by the user.
    let from = nn_table("m", vec![not_null_constraint("old_nn", "b")]);
    assert_eq!(
        from.get_alter_script(&to, true),
        guarded_rename("m", "old_nn", "my_nn", "old_nn", "b")
    );
    // The rename needs no use_drop: it drops nothing.
    assert_eq!(
        from.get_alter_script(&to, false),
        guarded_rename("m", "old_nn", "my_nn", "old_nn", "b")
    );
    // A quoted name is compared as the catalog stores it, and a quote in a
    // name is doubled in the literal.
    let from = nn_table("m", vec![not_null_constraint("\"Old'nn\"", "b")]);
    assert_eq!(
        from.get_alter_script(&to, true),
        guarded_rename("m", "\"Old'nn\"", "my_nn", "Old''nn", "b")
    );
}

#[test]
fn r3c_not_null_name_moving_to_another_column_is_renamed_away_first() {
    // Review finding: `x_nn` moving from column `a` to `b` was paired by
    // name, so it was dropped (clearing `a`'s NOT NULL) and re-added on `b`,
    // which failed while `b` still had its own.
    let mut a = int_col("t1", "a", 1);
    a.is_nullable = false;
    let mut b = int_col("t1", "b", 2);
    b.is_nullable = false;
    let mut from_constraints = vec![
        not_null_constraint("x_nn", "a"),
        not_null_constraint("t1_b_not_null", "b"),
    ];
    let mut to_constraints = vec![
        not_null_constraint("t1_a_not_null", "a"),
        not_null_constraint("x_nn", "b"),
    ];
    for c in from_constraints.iter_mut().chain(to_constraints.iter_mut()) {
        c.table_name = "t1".to_string();
    }
    let from = table_of("t1", vec![a.clone(), b.clone()], from_constraints);
    let to = table_of("t1", vec![a, b], to_constraints);
    let script = from.get_alter_script(&to, true);
    assert_eq!(
        script,
        format!(
            "{}{}",
            guarded_rename("t1", "x_nn", "t1_a_not_null", "x_nn", "a"),
            guarded_rename("t1", "t1_b_not_null", "x_nn", "t1_b_not_null", "b"),
        )
    );
    assert!(!script.contains("drop constraint"), "{script}");
}

#[test]
fn r3c_not_null_names_swapped_between_columns_go_through_a_temporary_name() {
    let mut a = int_col("t2", "a", 1);
    a.is_nullable = false;
    let mut b = int_col("t2", "b", 2);
    b.is_nullable = false;
    let mut from_constraints = vec![
        not_null_constraint("p_nn", "a"),
        not_null_constraint("q_nn", "b"),
    ];
    let mut to_constraints = vec![
        not_null_constraint("q_nn", "a"),
        not_null_constraint("p_nn", "b"),
    ];
    for c in from_constraints.iter_mut().chain(to_constraints.iter_mut()) {
        c.table_name = "t2".to_string();
    }
    let from = table_of("t2", vec![a.clone(), b.clone()], from_constraints);
    let to = table_of("t2", vec![a, b], to_constraints);
    let script = from.get_alter_script(&to, true);
    let temporary = from.temporary_not_null_name("p_nn");
    assert!(temporary.starts_with("pgc_nn_") && temporary.len() == "pgc_nn_".len() + 16);
    assert_eq!(temporary, from.temporary_not_null_name("p_nn"));
    assert_eq!(
        script,
        format!(
            "{}{}{}",
            guarded_rename("t2", "p_nn", &temporary, "p_nn", "a"),
            guarded_rename("t2", "q_nn", "p_nn", "q_nn", "b"),
            guarded_rename("t2", &temporary, "q_nn", &temporary, "a"),
        )
    );
}

#[test]
fn r3c_not_null_paired_by_column_carries_its_comment_to_the_final_name() {
    let from = nn_table("m", vec![not_null_constraint("old_nn", "b")]);
    let mut commented = not_null_constraint("my_nn", "b");
    commented.comment = Some("kept".to_string());
    let to = nn_table("m", vec![commented.clone()]);
    let script = from.get_alter_script(&to, true);
    let rename = position(&script, "rename constraint old_nn to my_nn;");
    let comment = position(
        &script,
        "comment on constraint my_nn on public.m is 'kept';",
    );
    assert!(rename < comment, "{script}");
    // TO's name generated: the comment goes on the FROM name, which stays.
    let mut auto = not_null_constraint("m_b_not_null", "b");
    auto.comment = Some("kept".to_string());
    let to = nn_table("m", vec![auto]);
    let script = from.get_alter_script(&to, true);
    assert_eq!(
        script, "comment on constraint old_nn on public.m is 'kept';\n\n",
        "{script}"
    );
}

#[test]
fn r3c_not_null_paired_by_column_with_other_properties_is_re_added() {
    // A NO INHERIT change is no rename: drop the FROM one, add TO's.
    let from = nn_table("m", vec![not_null_constraint("old_nn", "b")]);
    let mut no_inherit = not_null_constraint("my_nn", "b");
    no_inherit.no_inherit = true;
    no_inherit.definition = Some("NOT NULL b NO INHERIT".to_string());
    let to = nn_table("m", vec![no_inherit]);
    let script = from.get_alter_script(&to, true);
    let drop = position(&script, "alter table public.m drop constraint old_nn;");
    let add = position(
        &script,
        "alter table public.m add constraint my_nn not null b no inherit",
    );
    assert!(drop < add, "{script}");
    assert!(!script.contains("rename constraint"), "{script}");
}

#[test]
fn r3c_both_generated_not_null_names_still_compare_equal() {
    // As before: two generated names for the same column are no change.
    let from = nn_table("t", vec![not_null_constraint("t_b_not_null", "b")]);
    let to = nn_table("t", vec![not_null_constraint("t_b_not_null1", "b")]);
    assert_eq!(from.get_alter_script(&to, true), "");
}

#[test]
fn r3c_not_null_of_a_column_to_inherits_is_not_dropped() {
    // Finding 2 (NOT NULL facet): `LIKE parent INCLUDING CONSTRAINTS` gives
    // the table the parent-named `pt_id_not_null`; once attached, TO's NOT
    // NULL on the column is inherited and never dumped. The attach merges
    // the FROM one; a DROP after it failed ("cannot drop inherited
    // constraint").
    let from = nn_table("x", vec![not_null_constraint("pt_b_not_null", "b")]);
    let mut to = nn_table("x", vec![]);
    to.partition_of = Some("public.pt".to_string());
    to.partition_bound = Some("FOR VALUES FROM (10) TO (20)".to_string());
    to.hash();
    let script = from.get_alter_script(&to, true);
    assert_eq!(
        script,
        "alter table public.pt attach partition public.x FOR VALUES FROM (10) TO (20);\n\n"
    );
    // A table with no parent still drops it: its column is NOT NULL only
    // through that constraint (or a NOT NULL domain).
    let plain = nn_table("x", vec![]);
    assert!(
        from.get_alter_script(&plain, true)
            .contains("alter table public.x drop constraint pt_b_not_null;")
    );
}

#[test]
fn r3c_not_null_of_a_re_created_column_is_left_to_the_re_add() {
    // A column the alter drops and re-adds loses its NOT NULL constraint
    // with the drop; its re-add names TO's. No rename runs on it.
    let mut g_from = generated_col("m", "b", 2, "v", "(id * 2)");
    g_from.is_nullable = false;
    let mut g_to = generated_col("m", "b", 2, "s", "(id * 2)");
    g_to.is_nullable = false;
    let mut nn_from = not_null_constraint("old_nn", "b");
    nn_from.table_name = "m".to_string();
    let mut nn_to = not_null_constraint("my_nn", "b");
    nn_to.table_name = "m".to_string();
    let from = table_of("m", vec![int_col("m", "id", 1), g_from], vec![nn_from]);
    let to = table_of("m", vec![int_col("m", "id", 1), g_to], vec![nn_to]);
    let script = from.get_alter_script(&to, true);
    assert!(!script.contains("rename constraint"), "{script}");
    assert!(!script.contains("drop constraint"), "{script}");
    assert!(script.contains("constraint my_nn not null"), "{script}");
}

/// `public.<name>`, a partition of `public.pt` in TO: columns `id`, `a`.
fn attach_to(name: &str) -> Table {
    let mut table = table_of(
        name,
        vec![int_col(name, "id", 1), int_col(name, "a", 2)],
        vec![],
    );
    table.partition_of = Some("public.pt".to_string());
    table.partition_bound = Some("FOR VALUES FROM (10) TO (20)".to_string());
    table.hash();
    table
}

/// `public.<name>` on its own, with the given CHECKs.
fn attach_from(name: &str, checks: Vec<TableConstraint>) -> Table {
    let checks = checks
        .into_iter()
        .map(|mut c| {
            c.table_name = name.to_string();
            c
        })
        .collect();
    table_of(
        name,
        vec![int_col(name, "id", 1), int_col(name, "a", 2)],
        checks,
    )
}

/// `ck` as the copy `public.<table>` inherits from `pt`.
fn attach_copy(table: &str, definition: &str, enforced: bool) -> TableConstraint {
    let mut parent = check_constraint("ck", definition);
    parent.table_name = "pt".to_string();
    parent.is_enforced = !definition.ends_with("NOT ENFORCED");
    parent.inherited_copy("public", table, enforced)
}

const ATTACH_X: &str =
    "alter table public.pt attach partition public.x FOR VALUES FROM (10) TO (20);";

#[test]
fn r3c_attach_merges_a_matching_check_and_never_drops_it() {
    // Finding 2: the DROP CONSTRAINT after the attach failed.
    let from = attach_from("x", vec![check_constraint("ck", "CHECK ((a > 0))")]);
    let to = attach_to("x");
    let plan = InheritedRecreates {
        attach_checks: vec![attach_copy("x", "CHECK ((a > 0))", true)],
        ..Default::default()
    };
    let (script, _) =
        from.get_alter_script_in_family(&to, true, false, TableAlterCaps::default(), &plan);
    assert_eq!(script, format!("{ATTACH_X}\n\n"));
    // A comment of its own does not stand in the way.
    let mut commented = check_constraint("ck", "CHECK ((a > 0))");
    commented.comment = Some("mine".to_string());
    let from = attach_from("x", vec![commented]);
    let (script, _) =
        from.get_alter_script_in_family(&to, true, false, TableAlterCaps::default(), &plan);
    assert_eq!(script, format!("{ATTACH_X}\n\n"));
}

#[test]
fn r3c_attach_adds_a_missing_check_and_re_adds_a_different_one_first() {
    let to = attach_to("x");
    let plan = InheritedRecreates {
        attach_checks: vec![attach_copy("x", "CHECK ((a > 0))", true)],
        ..Default::default()
    };
    let caps = TableAlterCaps::default();
    // Missing: "child table is missing constraint".
    let from = attach_from("x", vec![]);
    let (script, _) = from.get_alter_script_in_family(&to, true, false, caps, &plan);
    let add = position(
        &script,
        "alter table public.x add constraint ck check ((a > 0)) ;",
    );
    assert!(add < position(&script, ATTACH_X), "{script}");
    // Another definition: "has different definition for check constraint".
    let from = attach_from("x", vec![check_constraint("ck", "CHECK ((a > 1))")]);
    let (script, _) = from.get_alter_script_in_family(&to, true, false, caps, &plan);
    let drop = position(&script, "alter table public.x drop constraint ck;");
    let add = position(
        &script,
        "alter table public.x add constraint ck check ((a > 0)) ;",
    );
    assert!(drop < add && add < position(&script, ATTACH_X), "{script}");
    assert_eq!(script.matches("drop constraint").count(), 1, "{script}");
    // Without use_drop the drop is commented out, still ahead.
    let (script, _) = from.get_alter_script_in_family(&to, false, false, caps, &plan);
    assert!(
        position(&script, "-- alter table public.x drop constraint ck;")
            < position(&script, ATTACH_X),
        "{script}"
    );
    // A CHECK the parent does not have is dropped with the table's other
    // constraint drops, which now run before the attach too.
    let from = attach_from(
        "x",
        vec![
            check_constraint("ck", "CHECK ((a > 0))"),
            check_constraint("other", "CHECK ((a < 9))"),
        ],
    );
    let (script, _) = from.get_alter_script_in_family(&to, true, false, caps, &plan);
    assert!(
        position(&script, "alter table public.x drop constraint other;")
            < position(&script, ATTACH_X),
        "{script}"
    );
}

#[test]
fn r3c_attach_gives_the_copy_to_s_enforceability_first() {
    // A NOT ENFORCED parent CHECK whose copy TO has NOT ENFORCED: the
    // table's enforced one would merge and stay enforced.
    let to = attach_to("x");
    let plan = InheritedRecreates {
        attach_checks: vec![attach_copy("x", "CHECK ((a > 0)) NOT ENFORCED", false)],
        ..Default::default()
    };
    let from = attach_from("x", vec![check_constraint("ck", "CHECK ((a > 0))")]);
    // PostgreSQL 19: in place.
    let pg19 = TableAlterCaps {
        constraint: ConstraintAlterCaps {
            check_enforceability: true,
        },
        ..Default::default()
    };
    let (script, _) = from.get_alter_script_in_family(&to, true, false, pg19, &plan);
    assert_eq!(
        script,
        format!("alter table public.x alter constraint ck not enforced;\n\n{ATTACH_X}\n\n")
    );
    // PostgreSQL 18: a drop and re-add.
    let (script, _) =
        from.get_alter_script_in_family(&to, true, false, TableAlterCaps::default(), &plan);
    let drop = position(&script, "alter table public.x drop constraint ck;");
    let add = position(
        &script,
        "alter table public.x add constraint ck check ((a > 0)) not enforced ;",
    );
    assert!(drop < add && add < position(&script, ATTACH_X), "{script}");
    // A copy TO keeps enforced under the NOT ENFORCED parent merges as it is.
    let plan = InheritedRecreates {
        attach_checks: vec![attach_copy("x", "CHECK ((a > 0)) NOT ENFORCED", true)],
        ..Default::default()
    };
    let (script, _) = from.get_alter_script_in_family(&to, true, false, pg19, &plan);
    assert_eq!(script, format!("{ATTACH_X}\n\n"));
}

#[test]
fn r3c_detach_diffs_the_copies_it_leaves_behind() {
    // DETACH PARTITION leaves the inherited copies as local constraints the
    // FROM dump never showed: TO's standalone table drops what it lacks
    // right after the detach, keeps what it has.
    let mut from = attach_to("x");
    from.partition_bound = Some("FOR VALUES FROM (0) TO (10)".to_string());
    from.hash();
    let mut to = attach_from("x", vec![check_constraint("kept", "CHECK ((a > 0))")]);
    to.hash();
    let detached = |name: &str, definition: &str| {
        let mut copy = attach_copy("x", definition, true);
        copy.name = name.to_string();
        copy.coninhcount = 0;
        copy
    };
    let plan = InheritedRecreates {
        detached_checks: vec![
            detached("kept", "CHECK ((a > 0))"),
            detached("gone", "CHECK ((a < 9))"),
        ],
        ..Default::default()
    };
    let (script, _) =
        from.get_alter_script_in_family(&to, true, false, TableAlterCaps::default(), &plan);
    assert_eq!(
        script,
        "alter table public.pt detach partition public.x;\n\n\
         alter table public.x drop constraint gone;\n\n"
    );
    // Without use_drop the detach is only commented out, and so nothing is
    // left behind.
    let (script, _) =
        from.get_alter_script_in_family(&to, false, false, TableAlterCaps::default(), &plan);
    assert!(!script.contains("drop constraint gone"), "{script}");
    assert!(script.contains("add constraint kept"), "{script}");

    // A new bound under the same parent: detached and attached again, the
    // parent's copies merging back.
    let to = attach_to("x");
    let plan = InheritedRecreates {
        detached_checks: vec![detached("ck", "CHECK ((a > 0))")],
        attach_checks: vec![attach_copy("x", "CHECK ((a > 0))", true)],
        ..Default::default()
    };
    let (script, _) =
        from.get_alter_script_in_family(&to, true, false, TableAlterCaps::default(), &plan);
    assert_eq!(
        script,
        format!("alter table public.pt detach partition public.x;\n\n{ATTACH_X}\n\n")
    );
}

#[test]
fn r3c_partition_moves_tell_detach_and_attach() {
    let standalone = attach_from("x", vec![]);
    let partition = attach_to("x");
    let mut rebound = partition.clone();
    rebound.partition_bound = Some("FOR VALUES FROM (0) TO (10)".to_string());
    let mut moved = partition.clone();
    moved.partition_of = Some("public.other".to_string());
    assert_eq!(standalone.partition_moves(&partition), (false, true));
    assert_eq!(partition.partition_moves(&standalone), (true, false));
    assert_eq!(rebound.partition_moves(&partition), (true, true));
    assert_eq!(moved.partition_moves(&partition), (true, true));
    assert_eq!(partition.partition_moves(&partition), (false, false));
    assert_eq!(standalone.partition_moves(&standalone), (false, false));
}

#[test]
fn r3c_enforced_inherited_checks_are_hashed_only_when_recorded() {
    // Every table without such a copy keeps the hash pgc 1.0.x gave it.
    let table = basic_table();
    assert_eq!(
        table.hash.as_deref(),
        Some("fd67e130e28ceb6bb23efa0a9144765bea42ed824b0394b4e3a284417ab465af")
    );
    let mut recorded = table.clone();
    recorded.enforced_inherited_checks = vec!["ck".to_string()];
    recorded.hash();
    assert_ne!(recorded.hash, table.hash);
    let mut other = table.clone();
    other.enforced_inherited_checks = vec!["ck2".to_string()];
    other.hash();
    assert_ne!(other.hash, recorded.hash);
    // A change to them is the comparer's to make, not the table alter's.
    assert_eq!(table.get_alter_script(&recorded, true), "");
}

#[test]
fn r3c_enforced_inherited_checks_query_reads_only_copies_no_parent_enforces() {
    let query = Table::build_enforced_inherited_checks_query("('app', 'b')");
    for part in [
        "n.nspname IN ('app', 'b') AND",
        "c.contype = 'c' AND",
        "NOT c.conislocal AND",
        "c.conenforced AND",
        "FROM pg_inherits i",
        "JOIN pg_constraint pc ON pc.conrelid = i.inhparent",
        "pc.conname = c.conname AND",
        "pc.conenforced",
        "quote_ident(c.conname) AS constraint_name",
    ] {
        assert!(query.contains(part), "{part}: {query}");
    }
}

// ==== r3-constraints review fixes ====

/// `public.<name>` with columns `id` and `a`, the given
/// CHECKs and `INHERITS` parents.
fn inherits_table(name: &str, parents: &[&str], checks: Vec<TableConstraint>) -> Table {
    let checks = checks
        .into_iter()
        .map(|mut c| {
            c.table_name = name.to_string();
            c
        })
        .collect();
    let mut table = table_of(
        name,
        vec![int_col(name, "id", 1), int_col(name, "a", 2)],
        checks,
    );
    table.inherits_from = parents.iter().map(|p| format!("public.{p}")).collect();
    table.hash();
    table
}

#[test]
fn r3c_inheritance_moves_keep_the_common_leading_parents() {
    let from = inherits_table("c", &["p1", "p2"], vec![]);
    let same = inherits_table("c", &["p1", "p2"], vec![]);
    assert_eq!(
        from.inheritance_moves(&same),
        (&[] as &[String], &[] as &[String])
    );
    let appended = inherits_table("c", &["p1", "p2", "p3"], vec![]);
    let (left, joined) = from.inheritance_moves(&appended);
    assert!(left.is_empty());
    assert_eq!(joined, ["public.p3".to_string()]);
    // Only reordered: both are left and joined again, in TO order.
    let reordered = inherits_table("c", &["p2", "p1"], vec![]);
    let (left, joined) = from.inheritance_moves(&reordered);
    assert_eq!(left, ["public.p1".to_string(), "public.p2".to_string()]);
    assert_eq!(joined, ["public.p2".to_string(), "public.p1".to_string()]);
    let standalone = inherits_table("c", &[], vec![]);
    assert_eq!(from.inheritance_moves(&standalone).0.len(), 2);
}

#[test]
fn r3c_inherit_and_no_inherit_are_emitted() {
    // Review finding: an INHERITS change was never migrated — the table's
    // copy of the parent's CHECK was dropped and the table never joined.
    let from = inherits_table("c", &["old"], vec![]);
    let to = inherits_table("c", &["new"], vec![]);
    let script = from.get_alter_script(&to, true);
    let leave = position(&script, "alter table public.c no inherit public.old;");
    let join = position(&script, "alter table public.c inherit public.new;");
    assert!(leave < join, "{script}");
    // Leaving a parent is commented out without use_drop, like a detach;
    // joining one is not.
    let script = from.get_alter_script(&to, false);
    assert!(
        script.contains("-- alter table public.c no inherit public.old;"),
        "{script}"
    );
    assert!(
        script.contains("\nalter table public.c inherit public.new;")
            || script.starts_with("alter table public.c inherit public.new;"),
        "{script}"
    );
}

#[test]
fn r3c_inherit_waits_for_the_column_changes_and_keeps_a_merged_check() {
    // INHERIT wants the parent's columns (NOT NULL where the parent's are)
    // and its CHECKs; a copy TO lists on the table itself is merged, and
    // takes TO's comment afterwards.
    let from = inherits_table("c", &[], vec![check_constraint("ck", "CHECK ((a > 0))")]);
    let mut to_ck = check_constraint("ck", "CHECK ((a > 0))");
    to_ck.coninhcount = 1;
    to_ck.comment = Some("merged".to_string());
    let mut to = inherits_table("c", &["p"], vec![to_ck]);
    to.columns[0].is_nullable = false;
    to.columns.push(int_col("c", "b", 3));
    to.hash();
    let mut copy = check_constraint("ck", "CHECK ((a > 0))");
    copy.table_name = "p".to_string();
    let plan = InheritedRecreates {
        attach_checks: vec![copy.inherited_copy("public", "c", true)],
        ..Default::default()
    };
    let (script, _) =
        from.get_alter_script_in_family(&to, true, false, TableAlterCaps::default(), &plan);
    let not_null = position(
        &script,
        "alter table public.c alter column id set not null;",
    );
    let add_column = position(&script, "alter table public.c add column b integer");
    let inherit = position(&script, "alter table public.c inherit public.p;");
    let comment = position(&script, "comment on constraint ck on public.c is 'merged';");
    assert!(
        not_null < inherit && add_column < inherit && inherit < comment,
        "{script}"
    );
    assert!(!script.contains("drop constraint"), "{script}");
    assert!(!script.contains("add constraint"), "{script}");
}

#[test]
fn r3c_inherit_re_adds_the_holders_checks_around_it() {
    // TO has the joining table only inherit `g_ck` (held by `g` and, merged,
    // by `p`): the holders drop theirs before the INHERIT, parents first,
    // the table drops its own, and they add them back after it, children
    // first, which leaves the table's copy inherited only.
    let from = inherits_table("c", &[], vec![check_constraint("g_ck", "CHECK ((a > 0))")]);
    let to = inherits_table("c", &["p"], vec![]);
    let mut on_g = check_constraint("g_ck", "CHECK ((a > 0))");
    on_g.table_name = "g".to_string();
    let mut on_p = on_g.clone();
    on_p.table_name = "p".to_string();
    on_p.coninhcount = 1;
    let plan = InheritedRecreates {
        inherit_readds: vec![on_g, on_p],
        ..Default::default()
    };
    let (script, _) =
        from.get_alter_script_in_family(&to, true, false, TableAlterCaps::default(), &plan);
    let own = position(&script, "alter table public.c drop constraint g_ck;");
    let drop_g = position(&script, "alter table public.g drop constraint g_ck;");
    let drop_p = position(&script, "alter table public.p drop constraint g_ck;");
    let inherit = position(&script, "alter table public.c inherit public.p;");
    let add_p = position(
        &script,
        "alter table public.p add constraint g_ck check ((a > 0)) ;",
    );
    let add_g = position(
        &script,
        "alter table public.g add constraint g_ck check ((a > 0)) ;",
    );
    assert!(
        own < inherit && drop_g < drop_p && drop_p < inherit && inherit < add_p && add_p < add_g,
        "{script}"
    );
    // Nothing of the kind without a change of parents.
    let same = inherits_table("c", &[], vec![check_constraint("g_ck", "CHECK ((a > 0))")]);
    let (script, _) =
        from.get_alter_script_in_family(&same, true, false, TableAlterCaps::default(), &plan);
    assert_eq!(script, "", "{script}");
}

#[test]
fn r3c_no_inherit_leaves_copies_that_are_diffed_after_it() {
    // NO INHERIT leaves the table's inherited copies local; TO drops one.
    let from = inherits_table("c", &["p"], vec![]);
    let to = inherits_table("c", &[], vec![check_constraint("kept", "CHECK ((a > 0))")]);
    let mut kept = check_constraint("kept", "CHECK ((a > 0))");
    kept.table_name = "p".to_string();
    let mut gone = check_constraint("gone", "CHECK ((a < 9))");
    gone.table_name = "p".to_string();
    let detached = |c: &TableConstraint| {
        let mut copy = c.inherited_copy("public", "c", true);
        copy.coninhcount = 0;
        copy
    };
    let plan = InheritedRecreates {
        detached_checks: vec![detached(&kept), detached(&gone)],
        ..Default::default()
    };
    let (script, _) =
        from.get_alter_script_in_family(&to, true, false, TableAlterCaps::default(), &plan);
    let leave = position(&script, "alter table public.c no inherit public.p;");
    let drop = position(&script, "alter table public.c drop constraint gone;");
    assert!(leave < drop, "{script}");
    assert!(!script.contains("constraint kept"), "{script}");
}

#[test]
fn r3c_attach_follows_the_column_changes_it_needs() {
    // Review finding: ATTACH PARTITION ran before `SET NOT NULL`, `ADD
    // COLUMN` and `DROP COLUMN`, and failed ("must be marked NOT NULL",
    // "missing column", "contains column not found in parent").
    let mut from = attach_from("x", vec![]);
    from.columns.push(int_col("x", "extra", 3));
    let mut to = attach_to("x");
    to.columns[0].is_nullable = false;
    to.columns.push(int_col("x", "c", 3));
    to.hash();
    let (script, _) = from.get_alter_script_in_family(
        &to,
        true,
        false,
        TableAlterCaps::default(),
        &InheritedRecreates::default(),
    );
    let attach = position(&script, ATTACH_X);
    for statement in [
        "alter table public.x alter column id set not null;",
        "alter table public.x add column c integer",
        "alter table public.x drop column extra;",
    ] {
        assert!(position(&script, statement) < attach, "{script}");
    }
}

// ---- r4:typed — CREATE TABLE ... OF <type> ----

/// `public.users OF public.user_t` with `columns` (all of them the type's)
/// and `constraints`.
fn typed_users_with(columns: Vec<TableColumn>, constraints: Vec<TableConstraint>) -> Table {
    let mut table = Table::new(
        "public".to_string(),
        "users".to_string(),
        "public".to_string(),
        "users".to_string(),
        "postgres".to_string(),
        None,
        columns,
        constraints,
        vec![],
        vec![],
        None,
    );
    table.typed_table_type = Some("public.user_t".to_string());
    table.hash();
    table
}

#[test]
fn typed_table_without_options_is_created_with_no_column_list() {
    // PostgreSQL takes the columns from the type and refuses an empty list.
    let table = typed_users_with(vec![base_column("id", 1), base_column("name", 2)], vec![]);
    assert_eq!(
        table.get_script(),
        "create table public.users of public.user_t;\n\nalter table public.users owner to postgres;\n\n"
    );
}

#[test]
fn typed_table_lists_only_what_it_adds_with_options() {
    let mut id = base_column("id", 1);
    id.data_type = "integer".to_string();
    id.is_nullable = false;
    let mut pk = primary_key_constraint();
    pk.definition = Some("PRIMARY KEY (id)".to_string());
    let table = typed_users_with(
        vec![id, name_column_with_default(), email_column()],
        vec![
            pk,
            check_constraint("users_name_check", "CHECK (name <> '')"),
        ],
    );

    let script = table.get_script();

    // No data type: a typed table's column list refuses one ("syntax error
    // at or near integer"); the column without a default or NOT NULL is left
    // out; the CHECK is added after, as for any table.
    assert_eq!(
        script,
        concat!(
            "create table public.users of public.user_t (\n",
            "    id with options not null,\n",
            "    name with options default 'unknown'::text not null,\n",
            "    constraint users_pkey primary key (\"id\")\n",
            ");\n\n",
            "alter table public.users add constraint users_name_check check (name <> '') ;\n\n",
            "alter table public.users owner to postgres;\n\n",
        )
    );
}

#[test]
fn typed_table_keeps_a_named_not_null_constraint() {
    let table = typed_users_with(
        vec![base_column("id", 1), name_column()],
        vec![not_null_constraint("name_must_exist", "name")],
    );
    let script = table.get_script();
    assert!(
        script.starts_with(
            "create table public.users of public.user_t (\n    name with options constraint name_must_exist not null\n);\n\n"
        ),
        "{script}"
    );
    assert!(
        !script.contains("add constraint name_must_exist"),
        "{script}"
    );
}

#[test]
fn typed_partitioned_unlogged_table_keeps_its_clauses() {
    let mut id = base_column("id", 1);
    id.is_nullable = false;
    let mut table = typed_users_with(vec![id, base_column("name", 2)], vec![]);
    table.is_unlogged = true;
    table.partition_key = Some("RANGE (id)".to_string());
    table.storage_parameters = Some(vec!["fillfactor=50".to_string()]);
    table.hash();

    assert_eq!(
        table.get_script(),
        concat!(
            "create unlogged table public.users of public.user_t (\n",
            "    id with options not null\n",
            ")\n",
            "partition by RANGE (id)\n",
            "with (fillfactor=50);\n\n",
            "alter table public.users owner to postgres;\n\n",
        )
    );

    // And with nothing of its own, no list at all before its clauses.
    let mut bare = typed_users_with(vec![base_column("id", 1)], vec![]);
    bare.partition_key = Some("RANGE (id)".to_string());
    assert!(
        bare.get_script().starts_with(
            "create table public.users of public.user_t\npartition by RANGE (id);\n\n"
        ),
        "{}",
        bare.get_script()
    );
}

#[test]
fn typed_table_recreated_by_its_alter_uses_the_typed_form() {
    // A partition-key change recreates the table from TO.
    let mut from = typed_users_with(vec![base_column("id", 1)], vec![]);
    from.partition_key = Some("LIST (id)".to_string());
    from.hash();
    let mut to = typed_users_with(vec![base_column("id", 1)], vec![]);
    to.partition_key = Some("RANGE (id)".to_string());
    to.hash();

    let script = from.get_alter_script(&to, true);

    assert!(
        script.contains(
            "drop table if exists public.users;\n\ncreate table public.users of public.user_t\npartition by RANGE (id);"
        ),
        "{script}"
    );
    assert!(!script.contains("id text"), "{script}");
}
