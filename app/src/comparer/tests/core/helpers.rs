//! Fixture builders shared by more than one `comparer::core` test module.
//!
//! Helpers used by a single module live in that module instead.

use crate::comparer::core::*;
use crate::dump::pg_type::PgType;
use crate::dump::routine::Routine;
use crate::dump::table::Table;
use crate::dump::table_column::TableColumn;
use crate::dump::table_constraint::TableConstraint;
use crate::dump::table_index::TableIndex;
use crate::dump::table_policy::TablePolicy;
use crate::dump::view::View;
use sqlx::postgres::types::Oid;

pub(super) fn make_domain_type(schema: &str, name: &str, oid: u32) -> PgType {
    PgType::new(
        Oid(oid),
        schema.to_string(),
        name.to_string(),
        Oid(2200),
        Oid(10),
        "postgres".to_string(),
        -1,
        false,
        'd' as i8,
        'U' as i8,
        false,
        true,
        ',' as i8,
        None,
        None,
        None,
        None,
        "domain_in".to_string(),
        "domain_out".to_string(),
        None,
        None,
        None,
        None,
        None,
        'i' as i8,
        'x' as i8,
        false,
        Some(Oid(25)),
        None,
        0,
        None,
        None,
        Some("text".to_string()),
        Vec::new(),
        Vec::new(),
        None,
    )
}

pub(super) fn make_enum_type(schema: &str, name: &str, oid: u32, labels: Vec<&str>) -> PgType {
    let mut enum_type = make_domain_type(schema, name, oid);
    enum_type.typtype = 'e' as i8;
    enum_type.typcategory = 'E' as i8;
    enum_type.typinput = "enum_in".to_string();
    enum_type.typoutput = "enum_out".to_string();
    enum_type.typbasetype = None;
    enum_type.formatted_basetype = None;
    enum_type.enum_labels = labels.into_iter().map(|label| label.to_string()).collect();
    enum_type.domain_constraints.clear();
    enum_type.hash();
    enum_type
}

pub(super) fn int_column(schema: &str, table: &str, name: &str, ordinal: i32) -> TableColumn {
    TableColumn {
        catalog: "postgres".to_string(),
        schema: schema.to_string(),
        table: table.to_string(),
        name: name.to_string(),
        ordinal_position: ordinal,
        column_default: None,
        is_nullable: true,
        data_type: "integer".to_string(),
        character_maximum_length: None,
        character_octet_length: None,
        numeric_precision: Some(32),
        numeric_precision_radix: Some(2),
        numeric_scale: Some(0),
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

/// Build a `Routine` mirroring `test_deps.compute(x integer)` from the
/// issue report, parameterised by return type so a single helper covers
/// both the FROM (integer) and TO (bigint) sides.
pub(super) fn issue179_compute_routine(return_type: &str, body: &str) -> Routine {
    let mut routine = Routine::new(
        "test_deps".to_string(),
        Oid(900),
        "compute".to_string(),
        "sql".to_string(),
        "FUNCTION".to_string(),
        return_type.to_string(),
        "x integer".to_string(),
        None,
        None,
        body.to_string(),
    );
    routine.hash();
    routine
}

/// Construct an `items` table that mirrors the issue's example: each
/// dependent (functional index, CHECK constraint, generated column,
/// column DEFAULT, RLS policy) references `test_deps.compute`.
pub(super) fn issue179_items_table(value_type: &str, def_default: &str, gen_type: &str) -> Table {
    let mut def_col = int_column("test_deps", "items", "def_col", 2);
    def_col.data_type = value_type.to_string();
    def_col.column_default = Some(def_default.to_string());

    let mut gen_col = int_column("test_deps", "items", "gen_col", 3);
    gen_col.data_type = gen_type.to_string();
    gen_col.is_generated = "ALWAYS".to_string();
    gen_col.generation_expression = Some("test_deps.compute(value)".to_string());
    gen_col.generation_type = Some("s".to_string());

    let mut value_col = int_column("test_deps", "items", "value", 1);
    value_col.is_nullable = false;

    let chk_constraint = TableConstraint {
        catalog: "postgres".to_string(),
        schema: "test_deps".to_string(),
        name: "chk_compute".to_string(),
        table_name: "items".to_string(),
        constraint_type: "CHECK".to_string(),
        is_deferrable: false,
        initially_deferred: false,
        definition: Some("CHECK (test_deps.compute(value) > 0)".to_string()),
        coninhcount: 0,
        is_enforced: true,
        no_inherit: false,
        nulls_not_distinct: false,
        comment: None,
    };

    let idx = TableIndex {
        schema: "test_deps".to_string(),
        table: "items".to_string(),
        name: "idx_compute".to_string(),
        catalog: Some("postgres".to_string()),
        indexdef:
            "CREATE INDEX idx_compute ON test_deps.items USING btree (test_deps.compute(value))"
                .to_string(),
        is_partition_index: false,
        comment: None,
    };

    let policy = TablePolicy {
        schema: "test_deps".to_string(),
        table: "items".to_string(),
        name: "p_items".to_string(),
        command: "all".to_string(),
        permissive: true,
        roles: vec![],
        using_clause: Some("(test_deps.compute(value) > 0)".to_string()),
        check_clause: None,
    };

    let mut table = Table::new(
        "test_deps".to_string(),
        "items".to_string(),
        "test_deps".to_string(),
        "items".to_string(),
        "postgres".to_string(),
        None,
        vec![value_col, def_col, gen_col],
        vec![chk_constraint],
        vec![idx],
        vec![],
        None,
    );
    table.policies = vec![policy];
    table.has_rowsecurity = true;
    table.hash();
    table
}

/// Build a view whose definition textually references `test_deps.compute`.
/// Returns a regular or materialized view depending on `is_materialized`.
pub(super) fn issue189_view(name: &str, is_materialized: bool) -> View {
    let mut view = View::new(
        name.to_string(),
        " SELECT test_deps.compute(value) AS c\n   FROM test_deps.items;".to_string(),
        "test_deps".to_string(),
        vec!["test_deps.items".to_string()],
    );
    view.is_materialized = is_materialized;
    view.hash();
    view
}
