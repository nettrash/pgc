//! Names interpolated into the comments of the migration (fix `comments`).
//!
//! PostgreSQL lets an identifier hold any character, and pgc names the objects
//! it touches in comments — `/* Table: s.t*/` headers, `-- use_drop=false: …`
//! notes, statements commented out for review. Written verbatim, a line break
//! (a line feed, or a bare carriage return: psql ends a `--` comment at either)
//! ends a line comment, `*/` ends a block comment and `/*` opens a nested one,
//! so the rest of the name runs as SQL or swallows the rest of the migration.
//!
//! Every scenario builds objects whose names carry such a payload, runs them
//! through the comparer (or the object's own script builder, for the notes the
//! comparer never reaches), and lexes the result the way PostgreSQL does
//! ([`executable_sql`]): no payload may be left outside a comment, quoted
//! identifier or literal, and no comment may be left open.

use super::helpers::*;
use crate::comparer::core::*;
use crate::config::dump_config::DumpConfig;
use crate::config::grants_mode::GrantsMode;
use crate::dump::cast::Cast;
use crate::dump::collation::Collation;
use crate::dump::event_trigger::EventTrigger;
use crate::dump::extension::{AvailableExtension, Extension};
use crate::dump::fdw::ForeignServer;
use crate::dump::foreign_table::{ForeignTable, ForeignTableColumn};
use crate::dump::operator::Operator;
use crate::dump::pg_type::DomainConstraint;
use crate::dump::publication::{Publication, Subscription};
use crate::dump::routine::Routine;
use crate::dump::schema::Schema;
use crate::dump::sequence::Sequence;
use crate::dump::statistic::Statistic;
use crate::dump::table::Table;
use crate::dump::table_index::TableIndex;
use crate::dump::table_policy::TablePolicy;
use crate::dump::table_trigger::TableTrigger;
use crate::dump::text_search::{TextSearchConfig, TextSearchDict};
use crate::dump::view::{View, ViewColumn};
use sqlx::postgres::types::Oid;

/// The relation every payload tries to reach.
const VICTIM: &str = "pgc_victim";

/// Raw catalog names, each built to escape one kind of comment.
const HOSTILE: &[&str] = &[
    // A line feed ends a `--` comment.
    "lf\ndrop table pgc_victim; --",
    // So does a bare carriage return — which `str::lines` does not split on.
    "cr\rdrop table pgc_victim; --",
    "crlf\r\ndrop table pgc_victim; --",
    // `*/` ends a block comment early …
    "blk*/ drop table pgc_victim; /*",
    // … and `/*` opens a nested one that swallows the rest of the script.
    "nest/*ed pgc_victim",
];

/// `quote_ident` for a name that needs quoting, as a dump stores it.
fn q(raw: &str) -> String {
    format!("\"{}\"", raw.replace('"', "\"\""))
}

fn is_ident_char(c: char) -> bool {
    c.is_alphanumeric() || c == '_' || c == '$'
}

/// What PostgreSQL would execute of `sql`: everything outside comments, quoted
/// identifiers, string literals and dollar-quoted bodies. Lexed as PostgreSQL
/// and psql do — a `--` comment ends at `\n` or `\r`, block comments nest — and
/// independently of the crate's own scanner, which is under test too.
///
/// `Err` when a block comment, quoted token or dollar quote is still open at
/// the end: whatever followed its opener was swallowed.
fn executable_sql(sql: &str) -> Result<String, String> {
    let chars: Vec<char> = sql.chars().collect();
    let len = chars.len();
    let mut out = String::with_capacity(sql.len());
    let mut i = 0;
    while i < len {
        let c = chars[i];
        let next = chars.get(i + 1).copied();
        if c == '-' && next == Some('-') {
            while i < len && chars[i] != '\n' && chars[i] != '\r' {
                i += 1;
            }
            continue;
        }
        if c == '/' && next == Some('*') {
            let start = i;
            let mut depth = 0usize;
            loop {
                if i + 1 >= len {
                    let opened: String = chars[start..len.min(start + 60)].iter().collect();
                    return Err(format!("unterminated block comment opened at {opened:?}"));
                }
                match (chars[i], chars[i + 1]) {
                    ('/', '*') => {
                        depth += 1;
                        i += 2;
                    }
                    ('*', '/') => {
                        depth -= 1;
                        i += 2;
                        if depth == 0 {
                            break;
                        }
                    }
                    _ => i += 1,
                }
            }
            out.push(' ');
            continue;
        }
        if c == '\'' || c == '"' {
            let backslash_escapes = c == '\''
                && i > 0
                && matches!(chars[i - 1], 'E' | 'e')
                && (i < 2 || !is_ident_char(chars[i - 2]));
            let start = i;
            i += 1;
            loop {
                let Some(&ch) = chars.get(i) else {
                    let opened: String = chars[start..len.min(start + 60)].iter().collect();
                    return Err(format!("unterminated {c} token opened at {opened:?}"));
                };
                i += 1;
                if backslash_escapes && ch == '\\' {
                    i += 1;
                } else if ch == c {
                    if chars.get(i) == Some(&c) {
                        i += 1;
                    } else {
                        break;
                    }
                }
            }
            out.push(' ');
            continue;
        }
        if c == '$' && (i == 0 || !is_ident_char(chars[i - 1])) {
            let mut j = i + 1;
            while j < len && (chars[j].is_alphanumeric() || chars[j] == '_') {
                j += 1;
            }
            if j < len && chars[j] == '$' && !chars.get(i + 1).is_some_and(char::is_ascii_digit) {
                let tag: String = chars[i..=j].iter().collect();
                let rest: String = chars[j + 1..].iter().collect();
                let Some(close) = rest.find(&tag) else {
                    return Err(format!("unterminated dollar quote {tag}"));
                };
                i = j + 1 + rest[..close].chars().count() + tag.chars().count();
                out.push(' ');
                continue;
            }
        }
        out.push(c);
        i += 1;
    }
    Ok(out)
}

#[track_caller]
fn assert_inert(context: &str, sql: &str) {
    match executable_sql(sql) {
        Ok(code) => assert!(
            !code.contains(VICTIM),
            "{context}: the payload reached executable SQL:\n{code}\n---- script ----\n{sql}"
        ),
        Err(error) => panic!("{context}: {error}\n---- script ----\n{sql}"),
    }
}

#[test]
fn the_oracle_catches_every_payload_left_in_a_comment() {
    // Guard against a lexer that would let every scenario pass: each hostile
    // name, pasted raw into the comment kind it targets, must be caught.
    for raw in &HOSTILE[..3] {
        let sql = format!("-- view s.{}\nselect 1;\n", q(raw));
        assert!(
            executable_sql(&sql).unwrap().contains(VICTIM),
            "{raw:?} in a line comment"
        );
    }
    let sql = format!("/* Table: s.{}*/\nselect 1;\n", q(HOSTILE[3]));
    assert!(executable_sql(&sql).unwrap().contains(VICTIM));
    let sql = format!("/* Table: s.{}*/\nselect 1;\n", q(HOSTILE[4]));
    assert!(executable_sql(&sql).is_err());
    // And a quoted name or literal holding the payload is not executable.
    let sql = format!(
        "drop view s.{};\ncomment on table t is 'x\ndrop table {VICTIM}; --';\n",
        q(HOSTILE[0])
    );
    assert!(!executable_sql(&sql).unwrap().contains(VICTIM));
}

// ---- fixture builders ------------------------------------------------------

fn table(schema: &str, name: &str, columns: Vec<TableColumn>) -> Table {
    let mut table = Table::new(
        schema.to_string(),
        name.to_string(),
        schema.trim_matches('"').to_string(),
        name.trim_matches('"').to_string(),
        "postgres".to_string(),
        None,
        columns,
        vec![],
        vec![],
        vec![],
        None,
    );
    table.hash();
    table
}

fn trigger(name: &str, definition: &str) -> TableTrigger {
    TableTrigger {
        oid: Oid(4242),
        name: name.to_string(),
        definition: definition.to_string(),
        enabled: "O".to_string(),
        comment: None,
    }
}

fn policy(schema: &str, table: &str, name: &str) -> TablePolicy {
    TablePolicy {
        schema: schema.to_string(),
        table: table.to_string(),
        name: name.to_string(),
        command: "select".to_string(),
        permissive: true,
        roles: vec!["public".to_string()],
        using_clause: Some("true".to_string()),
        check_clause: None,
    }
}

fn index(schema: &str, table: &str, name: &str) -> TableIndex {
    TableIndex {
        schema: schema.to_string(),
        table: table.to_string(),
        name: name.to_string(),
        catalog: None,
        indexdef: format!("CREATE INDEX {name} ON {schema}.{table} USING btree (id)"),
        is_partition_index: false,
        comment: None,
    }
}

fn view(schema: &str, name: &str, columns: &[&str]) -> View {
    let select = columns
        .iter()
        .enumerate()
        .map(|(i, c)| format!("{} AS {c}", i + 1))
        .collect::<Vec<_>>()
        .join(", ");
    let mut view = View::new(
        name.to_string(),
        format!(" SELECT {select};"),
        schema.to_string(),
        vec![],
    );
    view.columns = columns
        .iter()
        .map(|c| ViewColumn {
            name: c.to_string(),
            data_type: "integer".to_string(),
            collation: None,
        })
        .collect();
    view.hash();
    view
}

fn routine(schema: &str, name: &str, arguments: &str, return_type: &str) -> Routine {
    Routine::new(
        schema.to_string(),
        Oid(4343),
        name.to_string(),
        "sql".to_string(),
        "FUNCTION".to_string(),
        return_type.to_string(),
        arguments.to_string(),
        None,
        None,
        "SELECT 1".to_string(),
    )
}

fn sequence(schema: &str, name: &str, owned_by: Option<(&str, &str, &str)>) -> Sequence {
    Sequence::new(
        schema.to_string(),
        name.to_string(),
        "postgres".to_string(),
        "bigint".to_string(),
        Some(1),
        Some(1),
        Some(1000),
        Some(1),
        false,
        Some(1),
        None,
        owned_by.map(|(s, _, _)| s.to_string()),
        owned_by.map(|(_, t, _)| t.to_string()),
        owned_by.map(|(_, _, c)| c.to_string()),
    )
}

fn statistic(schema: &str, name: &str, columns: &[&str]) -> Statistic {
    Statistic::new(
        schema.to_string(),
        name.to_string(),
        "postgres".to_string(),
        schema.to_string(),
        "t".to_string(),
        vec!["ndistinct".to_string()],
        columns.iter().map(|c| c.to_string()).collect(),
        format!(
            "CREATE STATISTICS {schema}.{name} (ndistinct) ON {} FROM {schema}.t",
            columns.join(", ")
        ),
    )
}

fn foreign_table(schema: &str, name: &str, server: &str) -> ForeignTable {
    ForeignTable::new(
        schema.to_string(),
        name.to_string(),
        server.to_string(),
        "postgres".to_string(),
        Vec::new(),
        vec![ForeignTableColumn {
            name: "id".to_string(),
            data_type: "integer".to_string(),
            is_nullable: true,
            column_default: None,
            options: Vec::new(),
            acl: Vec::new(),
        }],
    )
}

fn subscription(name: &str) -> Subscription {
    let mut subscription = Subscription {
        name: name.to_string(),
        owner: "postgres".to_string(),
        connection: "host=primary dbname=db".to_string(),
        server: None,
        publications: vec![name.to_string()],
        enabled: false,
        slot_name: Default::default(),
        retain_dead_tuples: false,
        max_retention_duration: 0,
        wal_receiver_timeout: None,
        comment: None,
        hash: None,
    };
    subscription.hash();
    subscription
}

fn publication(name: &str, all_tables: bool) -> Publication {
    let mut publication = Publication {
        name: name.to_string(),
        owner: "postgres".to_string(),
        publish: "insert,update,delete,truncate".to_string(),
        all_tables,
        ..Default::default()
    };
    publication.hash();
    publication
}

// ---- scenarios -------------------------------------------------------------
//
// One per object kind the migration names in a comment. Each adds a FROM and
// a TO object whose names carry `raw` (in `schema`, quoted as a dump stores
// it), set up so the path with the comment is the one taken: changed so it is
// recreated (commented out when `use_drop` is off), removed, or new.

type Scenario = fn(raw: &str, schema: &str, from: &mut Dump, to: &mut Dump);

/// A table altered in place: a column, an index, a trigger and a policy
/// dropped, NOT NULL / a default / an identity dropped, a column turned
/// generated, a trigger redefined; grants to a hostile role.
fn scenario_table_altered(raw: &str, schema: &str, from: &mut Dump, to: &mut Dump) {
    let name = q(raw);
    let col = |suffix: &str| q(&format!("{raw} {suffix}"));
    let definition = |when: &str| {
        format!("{when} INSERT ON {schema}.{name} FOR EACH ROW EXECUTE FUNCTION {schema}.f()")
    };
    let mut notnull = int_column(schema, &name, &col("nn"), 3);
    notnull.is_nullable = false;
    let mut defaulted = int_column(schema, &name, &col("def"), 4);
    defaulted.column_default = Some("1".to_string());
    let mut identity = int_column(schema, &name, &col("ident"), 5);
    identity.is_identity = true;
    identity.identity_generation = Some("BY DEFAULT".to_string());
    let plain = int_column(schema, &name, &col("gen"), 6);
    let mut from_table = table(
        schema,
        &name,
        vec![
            int_column(schema, &name, "id", 1),
            int_column(schema, &name, &name, 2),
            notnull.clone(),
            defaulted.clone(),
            identity.clone(),
            plain.clone(),
        ],
    );
    from_table.indexes.push(index(schema, &name, &name));
    from_table
        .triggers
        .push(trigger(&name, &definition("BEFORE")));
    from_table
        .triggers
        .push(trigger(&col("trg"), &definition("BEFORE")));
    from_table.policies.push(policy(schema, &name, &name));
    from_table.hash();

    let mut generated = plain;
    generated.is_generated = "ALWAYS".to_string();
    generated.generation_expression = Some("(id + 1)".to_string());
    generated.generation_type = Some("s".to_string());
    notnull.is_nullable = true;
    defaulted.column_default = None;
    identity.is_identity = false;
    identity.identity_generation = None;
    let mut to_table = table(
        schema,
        &name,
        vec![
            int_column(schema, &name, "id", 1),
            notnull,
            defaulted,
            identity,
            generated,
        ],
    );
    to_table
        .triggers
        .push(trigger(&col("trg"), &definition("AFTER")));
    let role = col("role");
    to_table.acl = vec![
        "postgres=arwdDxtm/postgres".to_string(),
        format!("{role}=r/postgres"),
    ];
    to_table.columns[0].acl = vec![format!("{role}=w/postgres")];
    to_table.hash();
    from.tables.push(from_table);
    to.tables.push(to_table);
}

/// A table dropped (with a trigger and an owned sequence), a table created
/// (with an identity sequence), a table without a hash.
fn scenario_tables_dropped_and_created(raw: &str, schema: &str, from: &mut Dump, to: &mut Dump) {
    let dropped = q(&format!("{raw} dropped"));
    let mut dropped_table = table(
        schema,
        &dropped,
        vec![int_column(schema, &dropped, "id", 1)],
    );
    dropped_table.triggers.push(trigger(
        &q(raw),
        &format!("BEFORE INSERT ON {schema}.{dropped} FOR EACH ROW EXECUTE FUNCTION {schema}.f()"),
    ));
    dropped_table.hash();
    from.tables.push(dropped_table);
    from.sequences.push(sequence(
        schema,
        &q(&format!("{raw} owned")),
        Some((schema, &dropped, "id")),
    ));

    let created = q(&format!("{raw} created"));
    let ident = q(&format!("{raw} ident"));
    let mut identity = int_column(schema, &created, &ident, 1);
    identity.is_identity = true;
    identity.identity_generation = Some("ALWAYS".to_string());
    to.tables.push(table(schema, &created, vec![identity]));
    let mut identity_sequence = sequence(
        schema,
        &q(&format!("{raw} ident seq")),
        Some((schema, &created, &ident)),
    );
    identity_sequence.is_identity = true;
    identity_sequence.hash();
    to.sequences.push(identity_sequence);

    let mut unhashed = table(schema, &q(&format!("{raw} nohash")), vec![]);
    unhashed.hash = None;
    to.tables.push(unhashed);
}

/// Views: a column list change forces drop + recreate; one view dropped; a
/// materialized view recreated. Both kinds are named through `quote_ident`,
/// as the dump reads them (materialized views too, since
/// `build_materialized_views_query` quotes them).
fn scenario_views(raw: &str, schema: &str, from: &mut Dump, to: &mut Dump) {
    let name = q(raw);
    from.views.push(view(schema, &name, &["a", "b"]));
    to.views.push(view(schema, &name, &["a"]));
    from.views
        .push(view(schema, &q(&format!("{raw} view dropped")), &["a"]));
    let mat = q(&format!("{raw} mat"));
    let mut from_matview = view(schema, &mat, &["a"]);
    from_matview.is_materialized = true;
    from_matview.hash();
    let mut to_matview = view(schema, &mat, &["a", "b"]);
    to_matview.is_materialized = true;
    to_matview.hash();
    from.views.push(from_matview);
    to.views.push(to_matview);
}

/// Routines: a return-type change forces drop + recreate, and the argument
/// list names the payload too; one dropped, one created.
fn scenario_routines(raw: &str, schema: &str, from: &mut Dump, to: &mut Dump) {
    let name = q(raw);
    let arguments = format!("{name} integer");
    from.routines
        .push(routine(schema, &name, &arguments, "integer"));
    to.routines
        .push(routine(schema, &name, &arguments, "bigint"));
    from.routines.push(routine(
        schema,
        &q(&format!("{raw} fn dropped")),
        "",
        "integer",
    ));
    to.routines.push(routine(
        schema,
        &q(&format!("{raw} fn created")),
        "",
        "integer",
    ));
}

/// Sequences: one dropped, one created.
fn scenario_sequences(raw: &str, schema: &str, from: &mut Dump, to: &mut Dump) {
    from.sequences.push(sequence(schema, &q(raw), None));
    to.sequences
        .push(sequence(schema, &q(&format!("{raw} seq")), None));
    let mut unhashed = sequence(schema, &q(&format!("{raw} seq nohash")), None);
    unhashed.hash = None;
    to.sequences.push(unhashed);
}

/// Schemas: one changed, one created.
fn scenario_schemas(raw: &str, schema: &str, from: &mut Dump, to: &mut Dump) {
    let raw_schema = schema.trim_matches('"');
    from.schemas.push(Schema::new(
        schema.to_string(),
        raw_schema.to_string(),
        None,
    ));
    to.schemas.push(Schema::new(
        schema.to_string(),
        raw_schema.to_string(),
        Some("changed".to_string()),
    ));
    let created = format!("{raw} schema created");
    to.schemas.push(Schema::new(q(&created), created, None));
}

/// Types: a domain whose base type, default, NOT NULL and CHECK change; an
/// enum losing a hostile label and reordered; a new domain; a dropped enum.
fn scenario_types(raw: &str, schema: &str, from: &mut Dump, to: &mut Dump) {
    let name = q(raw);
    let mut from_domain = make_domain_type(schema, &name, 90001);
    from_domain.typdefault = Some("'x'::text".to_string());
    from_domain.typnotnull = true;
    from_domain.domain_constraints.push(DomainConstraint {
        name: raw.to_string(),
        definition: "CHECK (VALUE <> ''::text)".to_string(),
    });
    from_domain.hash();
    let mut to_domain = make_domain_type(schema, &name, 90001);
    to_domain.formatted_basetype = Some(q(&format!("{raw} base")));
    to_domain.hash();
    from.types.push(from_domain);
    to.types.push(to_domain);
    to.types
        .push(make_domain_type(schema, &q(&format!("{raw} new")), 90002));

    let enum_name = q(&format!("{raw} enum"));
    from.types.push(make_enum_type(
        schema,
        &enum_name,
        90003,
        vec!["a", raw, "b"],
    ));
    to.types
        .push(make_enum_type(schema, &enum_name, 90003, vec!["b", "a"]));
    from.types.push(make_enum_type(
        schema,
        &q(&format!("{raw} enum dropped")),
        90004,
        vec!["a"],
    ));
}

/// Extensions: an owner change to a hostile role, a version the migrated
/// server cannot update to, one it cannot install, a dropped extension.
fn scenario_extensions(raw: &str, schema: &str, from: &mut Dump, to: &mut Dump) {
    let name = q(raw);
    let mut from_ext = Extension::new(name.clone(), "1.0".to_string(), schema.to_string());
    from_ext.owner = "postgres".to_string();
    let mut to_ext = Extension::new(name.clone(), raw.to_string(), schema.to_string());
    to_ext.owner = q(&format!("{raw} role"));
    from.extensions.push(from_ext);
    to.extensions.push(to_ext);
    from.available_extensions.push(AvailableExtension {
        name: name.clone(),
        default_version: raw.to_string(),
        versions: vec!["1.0".to_string(), format!("{raw} other")],
    });
    let new_ext = q(&format!("{raw} ext new"));
    to.extensions.push(Extension::new(
        new_ext.clone(),
        raw.to_string(),
        schema.to_string(),
    ));
    from.available_extensions.push(AvailableExtension {
        name: new_ext,
        default_version: format!("{raw} default"),
        versions: vec!["1.0".to_string(), format!("{raw} default")],
    });
    from.extensions.push(Extension::new(
        q(&format!("{raw} ext dropped")),
        "1.0".to_string(),
        schema.to_string(),
    ));
}

/// Publications (recreated, dropped) and subscriptions (dropped).
fn scenario_replication(raw: &str, _schema: &str, from: &mut Dump, to: &mut Dump) {
    let name = q(raw);
    from.publications.push(publication(&name, false));
    to.publications.push(publication(&name, true));
    from.publications
        .push(publication(&q(&format!("{raw} pub dropped")), false));
    from.subscriptions.push(subscription(&name));
}

/// Extended statistics, foreign tables and foreign servers: recreated.
fn scenario_statistics_and_foreign(raw: &str, schema: &str, from: &mut Dump, to: &mut Dump) {
    let name = q(raw);
    from.statistics.push(statistic(schema, &name, &["a", "b"]));
    to.statistics.push(statistic(schema, &name, &["a", "c"]));
    from.foreign_tables
        .push(foreign_table(schema, &name, "srv_a"));
    to.foreign_tables
        .push(foreign_table(schema, &name, "srv_b"));
    let dropped = q(&format!("{raw} dropped"));
    from.statistics.push(statistic(schema, &dropped, &["a"]));
    from.foreign_tables
        .push(foreign_table(schema, &dropped, "srv_a"));
    let server = |fdw: &str| {
        let mut server = ForeignServer {
            name: name.clone(),
            owner: "postgres".to_string(),
            fdw_name: fdw.to_string(),
            server_type: None,
            server_version: None,
            options: Vec::new(),
            comment: None,
            hash: None,
            acl: Vec::new(),
        };
        server.hash();
        server
    };
    from.foreign_servers.push(server("fdw_a"));
    to.foreign_servers.push(server("fdw_b"));
}

/// Text search, casts, collations, operators and event triggers: recreated.
fn scenario_catalog_objects(raw: &str, schema: &str, from: &mut Dump, to: &mut Dump) {
    let name = q(raw);
    let ts_config = |parser: &str| {
        let mut config = TextSearchConfig {
            schema: schema.to_string(),
            name: name.clone(),
            owner: "postgres".to_string(),
            parser: parser.to_string(),
            mappings: Vec::new(),
            comment: None,
            hash: None,
        };
        config.hash();
        config
    };
    from.ts_configs.push(ts_config("pg_catalog.default"));
    to.ts_configs.push(ts_config(&format!("{schema}.{name}")));
    let ts_dict = |template: &str| {
        let mut dict = TextSearchDict {
            schema: schema.to_string(),
            name: name.clone(),
            owner: "postgres".to_string(),
            template: template.to_string(),
            options: Vec::new(),
            comment: None,
            hash: None,
        };
        dict.hash();
        dict
    };
    from.ts_dicts.push(ts_dict("pg_catalog.simple"));
    to.ts_dicts.push(ts_dict("pg_catalog.snowball"));
    let cast = |context: &str| {
        let mut cast = Cast {
            source_type: format!("{schema}.{name}"),
            target_type: "integer".to_string(),
            cast_method: "f".to_string(),
            function_name: Some(format!("{schema}.{name}({schema}.{name})")),
            cast_context: context.to_string(),
            comment: None,
            hash: None,
        };
        cast.hash();
        cast
    };
    from.casts.push(cast("e"));
    to.casts.push(cast("i"));
    let collation = |deterministic: bool| {
        let mut collation = Collation {
            schema: schema.to_string(),
            name: name.clone(),
            owner: "postgres".to_string(),
            provider: "i".to_string(),
            locale: None,
            lc_collate: None,
            lc_ctype: None,
            icu_locale: Some("und".to_string()),
            icu_rules: None,
            deterministic,
            comment: None,
            hash: None,
        };
        collation.hash();
        collation
    };
    from.collations.push(collation(true));
    to.collations.push(collation(false));
    let operator = |procedure: &str| {
        let mut operator = Operator {
            schema: schema.to_string(),
            name: "===".to_string(),
            owner: "postgres".to_string(),
            left_type: Some(format!("{schema}.{name}")),
            right_type: Some(format!("{schema}.{name}")),
            result_type: "boolean".to_string(),
            procedure: procedure.to_string(),
            commutator: None,
            negator: None,
            restrict: None,
            join: None,
            is_hashes: false,
            is_merges: false,
            comment: None,
            hash: None,
        };
        operator.hash();
        operator
    };
    from.operators.push(operator(&format!("{schema}.{name}")));
    to.operators.push(operator(&format!("{schema}.other")));
    let event_trigger = |function: &str| {
        EventTrigger::new(
            raw.to_string(),
            "ddl_command_start".to_string(),
            function.to_string(),
            Vec::new(),
            "O".to_string(),
            "postgres".to_string(),
            None,
        )
    };
    from.event_triggers.push(event_trigger("s.f"));
    to.event_triggers.push(event_trigger("s.g"));
    from.event_triggers.push(EventTrigger::new(
        format!("{raw} dropped"),
        "ddl_command_start".to_string(),
        "s.f".to_string(),
        Vec::new(),
        "O".to_string(),
        "postgres".to_string(),
        None,
    ));
}

const SCENARIOS: &[(&str, Scenario)] = &[
    ("table altered", scenario_table_altered),
    (
        "tables dropped and created",
        scenario_tables_dropped_and_created,
    ),
    ("views", scenario_views),
    ("routines", scenario_routines),
    ("sequences", scenario_sequences),
    ("schemas", scenario_schemas),
    ("types", scenario_types),
    ("extensions", scenario_extensions),
    ("publications and subscriptions", scenario_replication),
    (
        "statistics and foreign objects",
        scenario_statistics_and_foreign,
    ),
    (
        "text search, casts, collations, operators, event triggers",
        scenario_catalog_objects,
    ),
];

/// FROM and TO dumps holding the given scenarios.
fn dumps(raw: &str, schema: &str, scenarios: &[(&str, Scenario)]) -> (Dump, Dump) {
    let mut from = Dump::new(DumpConfig::default());
    let mut to = Dump::new(DumpConfig::default());
    for (_, scenario) in scenarios {
        scenario(raw, schema, &mut from, &mut to);
    }
    (from, to)
}

async fn compare(
    (from, to): (Dump, Dump),
    use_drop: bool,
    use_comments: bool,
    production: bool,
) -> String {
    let mut comparer = Comparer::new(from, to, use_drop, true, use_comments, GrantsMode::Full);
    comparer.set_output_for_production(production);
    comparer.compare().await.unwrap();
    comparer.get_script()
}

#[tokio::test]
async fn hostile_names_never_escape_a_comment_of_the_migration() {
    for raw in HOSTILE {
        // The schema carries the payload too: every header names it.
        let schema = q(&format!("{raw} schema"));
        // Each kind on its own — some comparer paths are only taken when no
        // other kind is in play — and then all of them together.
        let mut runs: Vec<(&str, Vec<(&str, Scenario)>)> = SCENARIOS
            .iter()
            .map(|&(label, scenario)| (label, vec![(label, scenario)]))
            .collect();
        runs.push(("all kinds", SCENARIOS.to_vec()));
        for (label, scenarios) in &runs {
            for use_drop in [false, true] {
                for use_comments in [true, false] {
                    for production in [false, true] {
                        let script = compare(
                            dumps(raw, &schema, scenarios),
                            use_drop,
                            use_comments,
                            production,
                        )
                        .await;
                        assert_inert(
                            &format!(
                                "{raw:?}, {label} (use_drop={use_drop}, use_comments={use_comments}, production={production})"
                            ),
                            &script,
                        );
                    }
                }
            }
        }
    }
}

#[tokio::test]
async fn hostile_scenarios_reach_the_commented_paths() {
    // The test above proves nothing if the objects never reach the comments:
    // with an ordinary name, pin that each note and header is emitted.
    let mut script = String::new();
    for scenario in SCENARIOS {
        let run = compare(dumps("plain", "s", &[*scenario]), false, true, false).await;
        script.push_str(&run);
    }
    script.push_str(&compare(dumps("plain", "s", SCENARIOS), false, true, false).await);
    for needle in [
        "/* Table: s.\"plain\"*/",
        "/* Table: s.\"plain created\"*/",
        "/* Table: s.\"plain dropped\"*/",
        "/* Skipping table s.\"plain nohash\" due to missing hash. */",
        "/* Skipping drop of sequence s.\"plain owned\" as it is owned by table s.\"plain dropped\" which will be dropped. */",
        "/* Skipping sequence s.\"plain ident seq\" as it will be created by column s.\"plain created\".\"plain ident\" (identity/serial) */",
        "-- use_drop=false: would drop NOT NULL",
        "-- alter table s.\"plain\" alter column \"plain nn\" drop not null;",
        "-- alter table s.\"plain\" alter column \"plain def\" drop default;",
        "-- alter table s.\"plain\" alter column \"plain ident\" drop identity if exists;",
        "-- use_drop=false: converting column to generated requires drop/add",
        "-- alter table s.\"plain\" drop column \"plain\"",
        "-- drop index if exists s.\"plain\"",
        "-- drop trigger if exists \"plain\" on s.\"plain\"",
        "-- drop trigger if exists \"plain trg\" on s.\"plain\"",
        "-- drop trigger if exists \"plain\" on s.\"plain dropped\"",
        "-- drop policy if exists \"plain\" on s.\"plain\"",
        "-- drop table if exists s.\"plain dropped\"",
        "/* Grants for table: s.\"plain\" */",
        "/* Column grants for s.\"plain\".id */",
        "/* View: s.\"plain\"*/",
        "-- use_drop=false: view s.\"plain\" requires drop+recreate",
        "-- use_drop=false: materialized view s.\"plain mat\"",
        "-- drop view if exists s.\"plain view dropped\"",
        "/* Routine: s.\"plain\"(\"plain\" integer)*/",
        "/* Routine: s.\"plain fn dropped\"()*/",
        "/* Routine: s.\"plain fn created\"()*/",
        "/* Sequence: s.\"plain seq\"*/",
        "/* Skipping sequence s.\"plain seq nohash\" due to missing hash. */",
        "/* Schema: s*/",
        "/* Schema: \"plain schema created\"*/",
        "-- drop statistics if exists s.\"plain dropped\"",
        "-- drop foreign table if exists s.\"plain dropped\"",
        "-- DROP EVENT TRIGGER IF EXISTS \"plain dropped\"",
        "/* Sequence: s.\"plain\"*/",
        "/* Type: s.\"plain\" */",
        "/* Type: s.\"plain new\" */",
        "-- Changing base type of domain s.\"plain\" (text -> \"plain base\")",
        "-- alter domain s.\"plain\" drop default;",
        "-- alter domain s.\"plain\" drop not null;",
        "-- alter domain s.\"plain\" drop constraint \"plain\";",
        "/* Enum: s.\"plain enum\" */",
        "/* Enum: s.\"plain enum dropped\" */",
        "-- Enum s.\"plain enum\" cannot automatically remove value 'plain'.",
        "-- Enum s.\"plain enum\" has its values in a different order",
        "/* Extension: s.\"plain\"*/",
        "-- Extension owner change is not supported by PostgreSQL ALTER EXTENSION syntax (postgres -> \"plain role\")",
        "-- Extension \"plain\" version 'plain' is not available on the migrated server",
        "-- Extension \"plain ext new\" version 'plain' is not installable",
        "-- drop extension if exists \"plain ext dropped\"",
        "-- use_drop=false: publication \"plain\" requires drop+recreate",
        "-- DROP PUBLICATION IF EXISTS \"plain pub dropped\"",
        "-- DROP SUBSCRIPTION IF EXISTS \"plain\"",
        "-- use_drop=false: statistics s.\"plain\" requires drop+recreate",
        "-- use_drop=false: foreign table s.\"plain\" requires drop+recreate",
        "-- DROP SERVER IF EXISTS \"plain\"",
        "-- DROP TEXT SEARCH CONFIGURATION IF EXISTS s.\"plain\"",
        "-- DROP TEXT SEARCH DICTIONARY IF EXISTS s.\"plain\"",
        "-- DROP CAST IF EXISTS (s.\"plain\" AS integer)",
        "-- DROP COLLATION IF EXISTS s.\"plain\"",
        "-- DROP OPERATOR IF EXISTS s.===",
        "-- Event trigger \"plain\" requires drop+recreate",
    ] {
        assert!(
            script.to_lowercase().contains(&needle.to_lowercase()),
            "missing {needle:?} in:\n{script}"
        );
    }
}

/// Notes the comparer never produces, because it matches objects by name or
/// kind first — reached through the object's own script builder.
#[test]
fn hostile_names_never_escape_an_object_level_note() {
    for raw in HOSTILE {
        let schema = q(&format!("{raw} schema"));
        let schema = schema.as_str();
        let name = q(raw);
        let other = q(&format!("{raw} other"));
        let mut scripts: Vec<(&str, String)> = Vec::new();

        let v = view(schema, &name, &["a", "b"]);
        scripts.push((
            "view renamed",
            v.get_alter_script(&view(schema, &other, &["a", "b"]), false),
        ));
        scripts.push(("view unchanged", v.get_alter_script(&v, false)));
        scripts.push((
            "view recreated",
            v.get_alter_script(&view(schema, &name, &["a"]), false),
        ));

        let domain = make_domain_type(schema, &name, 1);
        let other_domain = make_domain_type(schema, &other, 1);
        scripts.push((
            "type renamed",
            domain.get_alter_script(&other_domain, false),
        ));
        let enum_type = make_enum_type(schema, &name, 1, vec!["a"]);
        scripts.push((
            "type kind changed",
            domain.get_alter_script(&enum_type, false),
        ));
        scripts.push((
            "enum unchanged",
            enum_type.get_alter_script(&enum_type, false),
        ));
        scripts.push(("domain unchanged", domain.get_alter_script(&domain, false)));
        let mut constrained = make_domain_type(schema, &name, 1);
        constrained.domain_constraints.push(DomainConstraint {
            name: raw.to_string(),
            definition: "CHECK (VALUE <> ''::text)".to_string(),
        });
        let mut reconstrained = constrained.clone();
        reconstrained.domain_constraints[0].definition = "CHECK (VALUE <> 'x'::text)".to_string();
        scripts.push((
            "domain constraint redefined",
            constrained.get_alter_script(&reconstrained, false),
        ));
        let mut labelless = make_enum_type(schema, &name, 1, vec![]);
        labelless.enum_labels.clear();
        scripts.push(("enum without labels", labelless.get_script()));
        let mut range = make_domain_type(schema, &name, 1);
        range.typtype = 'r' as i8;
        let mut range_to = range.clone();
        range_to.typcategory = 'X' as i8;
        range.hash();
        range_to.hash();
        scripts.push(("range recreated", range.get_alter_script(&range_to, false)));
        let mut multirange = make_domain_type(schema, &name, 1);
        multirange.typtype = 'm' as i8;
        scripts.push(("multirange created", multirange.get_script()));
        scripts.push((
            "multirange altered",
            multirange.get_alter_script(&multirange, false),
        ));
        let mut composite = make_domain_type(schema, &name, 1);
        composite.typtype = 'c' as i8;
        scripts.push(("composite without attributes", composite.get_script()));
        let mut unsupported = make_domain_type(schema, &name, 1);
        unsupported.typtype = 'z' as i8;
        scripts.push(("unsupported type kind", unsupported.get_script()));
        scripts.push((
            "alter not implemented",
            unsupported.get_alter_script(&unsupported, false),
        ));

        let definition = format!("BEFORE INSERT ON {schema}.t FOR EACH ROW EXECUTE FUNCTION f()");
        let from_trigger = trigger(&name, &definition);
        let to_trigger = trigger(&name, &definition.replace("BEFORE", "AFTER"));
        scripts.push((
            "trigger redefined",
            from_trigger.get_alter_script(&to_trigger, schema, &name, false),
        ));

        let mut aggregate = routine(schema, &name, &format!("{name} integer"), "integer");
        aggregate.kind = "AGGREGATE".to_string();
        scripts.push(("aggregate without details", aggregate.get_script()));

        for (context, sql) in scripts {
            assert_inert(&format!("{raw:?}: {context}"), &sql);
        }
    }
}
