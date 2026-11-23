use super::*;

fn build_function_routine() -> Routine {
    Routine::new(
        "public".to_string(),
        Oid(42),
        "add".to_string(),
        "plpgsql".to_string(),
        "FUNCTION".to_string(),
        "integer".to_string(),
        "a integer".to_string(),
        Some("DEFAULT 1".to_string()),
        None,
        "BEGIN RETURN a + 1; END".to_string(),
    )
}

fn build_procedure_routine() -> Routine {
    Routine::new(
        "public".to_string(),
        Oid(7),
        "do_something".to_string(),
        "sql".to_string(),
        "Procedure".to_string(),
        "void".to_string(),
        "a integer".to_string(),
        None,
        None,
        "SELECT a;".to_string(),
    )
}

#[test]
fn new_initializes_fields_and_hash() {
    let schema = "public";
    let name = "add";
    let lang = "plpgsql";
    let kind = "FUNCTION";
    let return_type = "integer";
    let arguments = "a integer";
    let defaults = Some("DEFAULT 1".to_string());
    let source_code = "BEGIN RETURN a + 1; END";

    let routine = Routine::new(
        schema.to_string(),
        Oid(42),
        name.to_string(),
        lang.to_string(),
        kind.to_string(),
        return_type.to_string(),
        arguments.to_string(),
        defaults.clone(),
        None,
        source_code.to_string(),
    );

    assert_eq!(routine.schema, schema);
    assert_eq!(routine.oid, Oid(42));
    assert_eq!(routine.name, name);
    assert_eq!(routine.lang, lang);
    assert_eq!(routine.kind, kind);
    assert_eq!(routine.return_type, return_type);
    assert_eq!(routine.arguments, arguments);
    assert_eq!(routine.arguments_defaults, defaults);
    assert_eq!(routine.source_code, source_code);
    assert_eq!(routine.volatility, "volatile");
    assert!(!routine.is_strict);
    assert!(!routine.is_leakproof);
    assert_eq!(routine.parallel, "unsafe");
    assert!(!routine.security_definer);
    assert!(routine.aggregate_info.is_none());

    let expected_src = format!(
        "{}.{}.{}.{}.{}.{}.{}.{}.{}.{}.{}.{}.{}.{}.{}.{}.{}.{}.{}.{}.{}",
        schema,
        name,
        lang,
        kind,
        return_type,
        arguments,
        defaults.as_deref().unwrap_or(""),
        "",
        "",
        source_code,
        "volatile",
        false,
        false,
        "unsafe",
        false,
        "",
        "",
        "",
        "",
        "",
        "",
    );
    let expected_hash = format!("{:x}", md5::compute(expected_src));
    assert_eq!(routine.hash.as_ref(), Some(&expected_hash));
}

#[test]
fn hash_reflects_source_code_changes() {
    let mut routine = build_function_routine();
    let original_hash = routine.hash.clone().expect("hash should be initialized");

    routine.source_code = "BEGIN RETURN a + 2; END".to_string();
    routine.hash();

    let updated_hash = routine.hash.clone().expect("hash should be recomputed");
    assert_ne!(updated_hash, original_hash);
}

#[test]
fn hash_is_identical_for_crlf_and_lf_source_code() {
    let lf_routine = Routine::new(
        "public".to_string(),
        Oid(42),
        "add".to_string(),
        "plpgsql".to_string(),
        "FUNCTION".to_string(),
        "integer".to_string(),
        "a integer".to_string(),
        None,
        None,
        "BEGIN\n  RETURN a + 1;\nEND".to_string(),
    );
    let crlf_routine = Routine::new(
        "public".to_string(),
        Oid(42),
        "add".to_string(),
        "plpgsql".to_string(),
        "FUNCTION".to_string(),
        "integer".to_string(),
        "a integer".to_string(),
        None,
        None,
        "BEGIN\r\n  RETURN a + 1;\r\nEND".to_string(),
    );
    assert_eq!(lf_routine.hash, crlf_routine.hash);
    assert_eq!(lf_routine.source_code, crlf_routine.source_code);
}

#[test]
fn get_script_uses_custom_delimiter_when_body_contains_dollar_dollar() {
    let routine = Routine::new(
        "public".to_string(),
        Oid(99),
        "echo".to_string(),
        "plpgsql".to_string(),
        "FUNCTION".to_string(),
        "void".to_string(),
        "".to_string(),
        None,
        None,
        "BEGIN PERFORM $$nested$$; END".to_string(),
    );

    let script = routine.get_script();

    assert!(script.contains("create or replace function"));
    assert!(script.contains("$pgc_echo_body$BEGIN PERFORM $$nested$$; END$pgc_echo_body$"));
    assert!(!routine.source_code.contains("$pgc_echo_body$"));
}

// ---- SQL-standard bodies (BEGIN ATOMIC … END / RETURN expr, PG14+) ----

/// A LANGUAGE sql routine whose `source_code` is `body` as
/// `pg_get_function_sqlbody` deparses it.
fn sql_body_routine(kind: &str, name: &str, return_type: &str, body: &str) -> Routine {
    let mut routine = Routine::new(
        "test_schema".to_string(),
        Oid(500),
        name.to_string(),
        "sql".to_string(),
        kind.to_string(),
        return_type.to_string(),
        "p integer".to_string(),
        None,
        None,
        String::new(),
    );
    routine.source_code = body.to_string();
    routine.is_sql_body = true;
    routine.hash();
    routine
}

const ATOMIC_UPSERT: &str = "BEGIN ATOMIC\n INSERT INTO test_schema.w (id, a, b)\n   VALUES (f_atomic.p, 1, 1) ON CONFLICT(id) DO SELECT FOR UPDATE\n   RETURNING w.id,\n     w.a,\n     w.b;\nEND";

#[test]
fn sql_body_function_script_puts_the_body_after_every_option_without_as() {
    // The shape pg_dump emits and PostgreSQL accepts: options first, then the
    // body as part of the statement. `as $$…$$` would make it a string body.
    let mut routine =
        sql_body_routine("function", "f_atomic", "SETOF test_schema.w", ATOMIC_UPSERT);
    routine.rows = Some(1000.0);
    routine.hash();

    assert_eq!(
        routine.get_script(),
        format!(
            "create or replace function test_schema.f_atomic(p integer) returns SETOF test_schema.w \
             language sql VOLATILE PARALLEL UNSAFE ROWS 1000 {ATOMIC_UPSERT};\n\n"
        )
    );
}

#[test]
fn sql_body_return_form_follows_flags_and_set_clauses() {
    let mut routine = sql_body_routine("function", "f_ret", "integer", "RETURN (p + 1)");
    routine.volatility = "immutable".to_string();
    routine.is_strict = true;
    routine.parallel = "safe".to_string();
    routine.cost = Some(5.0);
    routine.config = vec!["search_path=pg_catalog".to_string()];
    routine.arguments_defaults = Some("1".to_string());
    routine.hash();

    let script = routine.get_script();
    assert!(
        script.starts_with(
            "create or replace function test_schema.f_ret(p integer DEFAULT 1) returns integer \
             language sql IMMUTABLE STRICT PARALLEL SAFE COST 5 SET search_path = pg_catalog \
             RETURN (p + 1);\n\n"
        ),
        "{script}"
    );
    assert!(!script.contains("$$"), "{script}");
    // Only an empty list GUC reads the server version; this body does not.
    assert_eq!(routine.get_script_for_server(true), script);
}

#[test]
fn sql_body_procedure_script_has_no_returns_and_keeps_comment_and_owner() {
    let body = "BEGIN ATOMIC\n INSERT INTO test_schema.w (id, a, b)\n   VALUES (p_atomic.p, 1, 1) ON CONFLICT(id) DO NOTHING;\n SELECT 1;\nEND";
    let mut routine = sql_body_routine("procedure", "p_atomic", "void", body);
    routine.security_definer = true;
    routine.comment = Some("it's atomic".to_string());
    routine.owner = "app_owner".to_string();
    routine.hash();

    let script = routine.get_script();
    assert!(
        script.starts_with(&format!(
            "create or replace procedure test_schema.p_atomic(p integer) language sql \
             SECURITY DEFINER {body};\n\n"
        )),
        "{script}"
    );
    assert!(!script.contains(" returns "), "{script}");
    assert!(
        script.contains("comment on procedure test_schema.p_atomic(p integer) is 'it''s atomic';"),
        "{script}"
    );
    assert!(
        script.contains("alter procedure test_schema.p_atomic(p integer) owner to app_owner;"),
        "{script}"
    );
}

#[test]
fn sql_body_is_never_dollar_quoted() {
    // `$$` inside a literal of a SQL-standard body is just text: no delimiter
    // is chosen, and nothing wraps the body.
    let routine = sql_body_routine(
        "function",
        "f_dollar",
        "text",
        "RETURN '$$ not a delimiter $$'::text",
    );
    let script = routine.get_script();
    assert!(
        script.ends_with(
            "language sql VOLATILE PARALLEL UNSAFE RETURN '$$ not a delimiter $$'::text;\n\n"
        ),
        "{script}"
    );
    assert!(!script.contains("$pgc_"), "{script}");
    assert!(!script.contains(" as "), "{script}");
}

#[test]
fn empty_atomic_block_is_emitted_as_an_atomic_block() {
    // Before the fix every SQL-standard body was dumped as '' and replayed as
    // `as $$$$`: a failing function, or a procedure turned into a silent no-op.
    let routine = sql_body_routine("procedure", "p_noop", "void", "BEGIN ATOMIC\nEND");
    let script = routine.get_script();
    assert_eq!(
        script,
        "create or replace procedure test_schema.p_noop(p integer) language sql BEGIN ATOMIC\nEND;\n\n"
    );
    assert!(!script.contains("$$$$"), "{script}");
}

#[test]
fn string_body_routine_hash_is_the_one_pgc_1_0_x_computed() {
    // The flag is folded into the hash only when set, so the hash of every
    // string-body routine, stored in every 1.0.x dump, is unchanged.
    let routine = build_function_routine();
    assert!(!routine.is_sql_body);
    assert_eq!(
        routine.hash.as_deref(),
        Some("5d1522486bcb4f809c24696c11899a00")
    );
}

#[test]
fn sql_body_hash_folds_the_body_and_the_flag() {
    let mut routine = sql_body_routine("function", "f_ret", "integer", "RETURN (p + 1)");
    routine.volatility = "immutable".to_string();
    routine.is_strict = true;
    routine.parallel = "safe".to_string();
    routine.config = vec!["search_path=pg_catalog".to_string()];
    routine.hash();
    // md5 of the 21 fields pgc 1.0.x hashed, then `.sqlbody`.
    assert_eq!(
        routine.hash.as_deref(),
        Some("93ffdc66993932510d5df13d12113eab")
    );

    // The same text as a string body is a different routine: CREATE OR
    // REPLACE converts one form into the other, so the switch must be seen.
    let mut as_string = routine.clone();
    as_string.is_sql_body = false;
    as_string.hash();
    assert_eq!(
        as_string.hash.as_deref(),
        Some("d8bdb26df7c00341e39d7b1a2876d2be")
    );

    // And a body change is a change — pgc 1.0.x hashed '' for every such body.
    let mut changed = routine.clone();
    changed.source_code = "RETURN (p + 2)".to_string();
    changed.hash();
    assert_ne!(changed.hash, routine.hash);
}

#[test]
fn sql_body_dependencies_never_enter_the_hash() {
    // They only decide what a detected change drags along (and a dump written
    // before the field existed has none), so they must not make a routine
    // compare as changed.
    let routine = sql_body_routine("function", "f_ret", "integer", "RETURN (p + 1)");
    let mut with_dependencies = routine.clone();
    with_dependencies.sql_body_dependencies = SqlBodyDependencies {
        relations: vec!["test_schema.t".to_string()],
        columns: vec!["test_schema.t.v".to_string()],
        routines: vec!["test_schema.g(integer)".to_string()],
        operators: vec!["test_schema.===(integer, integer)".to_string()],
    };
    with_dependencies.hash();
    assert_eq!(with_dependencies.hash, routine.hash);
}

#[test]
fn sql_body_dependencies_are_empty_only_when_nothing_is_recorded() {
    assert!(SqlBodyDependencies::default().is_empty());
    for dependencies in [
        SqlBodyDependencies {
            relations: vec!["s.t".to_string()],
            ..SqlBodyDependencies::default()
        },
        SqlBodyDependencies {
            columns: vec!["s.t.c".to_string()],
            ..SqlBodyDependencies::default()
        },
        SqlBodyDependencies {
            routines: vec!["s.f()".to_string()],
            ..SqlBodyDependencies::default()
        },
        SqlBodyDependencies {
            operators: vec!["s.===(integer, integer)".to_string()],
            ..SqlBodyDependencies::default()
        },
    ] {
        assert!(!dependencies.is_empty(), "{dependencies:?}");
    }
}

#[test]
fn dependency_key_is_the_form_pg_depend_rows_and_casts_name_a_routine_in() {
    // schema.name(identity arguments), quote_ident'ed as the routine query
    // renders them — what `SqlBodyDependencies::routines` and a cast's
    // `function_name` hold.
    let mut routine = sql_body_routine("function", "\"Mixed\"", "integer", "RETURN (p + 1)");
    assert_eq!(routine.dependency_key(), "test_schema.\"Mixed\"(p integer)");
    routine.arguments = String::new();
    assert_eq!(routine.dependency_key(), "test_schema.\"Mixed\"()");
}

#[test]
fn hash_includes_argument_defaults() {
    // PostgreSQL has no `ALTER FUNCTION` for default values — a
    // defaults-only change requires DROP+CREATE — so the hash must
    // reflect `arguments_defaults` or the comparer's `hashes_differ`
    // gate would silently swallow the diff.
    let mut routine = build_function_routine();
    let original_hash = routine.hash.clone();

    routine.arguments_defaults = Some("DEFAULT 99".to_string());
    routine.hash();

    assert_ne!(routine.hash, original_hash);
}

#[test]
fn get_script_for_function_includes_defaults() {
    let routine = build_function_routine();
    let script = routine.get_script();

    let expected = "create or replace function public.add(a integer DEFAULT 1) returns integer language plpgsql VOLATILE PARALLEL UNSAFE as $$BEGIN RETURN a + 1; END$$;\n\n";
    assert_eq!(script, expected);
}

#[test]
fn get_script_includes_owner_when_present() {
    let mut routine = build_function_routine();
    routine.owner = "pgc_owner".to_string();
    routine.hash();

    let expected = "create or replace function public.add(a integer DEFAULT 1) returns integer language plpgsql VOLATILE PARALLEL UNSAFE as $$BEGIN RETURN a + 1; END$$;\n\nalter function public.add(a integer) owner to pgc_owner;\n\n";
    assert_eq!(routine.get_script(), expected);
}

#[test]
fn get_script_for_procedure_omits_returns() {
    let routine = build_procedure_routine();
    let script = routine.get_script();

    let expected = "create or replace procedure public.do_something(a integer) language sql as $$SELECT a;$$;\n\n";
    assert_eq!(script, expected);
}

#[test]
fn get_drop_script_matches_kind() {
    let routine = build_function_routine();
    let drop_script = routine.get_drop_script();

    let expected = "drop function if exists public.add (a integer) cascade;\n\n";
    assert_eq!(drop_script, expected);
}

#[test]
fn get_script_handles_returns_table() {
    let routine = Routine::new(
        "data".to_string(),
        Oid(100),
        "test".to_string(),
        "plpgsql".to_string(),
        "FUNCTION".to_string(),
        "TABLE(row_to_json json)".to_string(),
        "fetching_id bigint, fetching_event_id character varying".to_string(),
        None,
        None,
        "BEGIN RETURN QUERY SELECT row_to_json(t) FROM t; END".to_string(),
    );
    let script = routine.get_script();

    let expected = "create or replace function data.test(fetching_id bigint, fetching_event_id character varying) returns TABLE(row_to_json json) language plpgsql VOLATILE PARALLEL UNSAFE as $$BEGIN RETURN QUERY SELECT row_to_json(t) FROM t; END$$;\n\n";
    assert_eq!(script, expected);
}

#[test]
fn get_script_for_immutable_strict_function() {
    let mut routine = build_function_routine();
    routine.volatility = "immutable".to_string();
    routine.is_strict = true;
    routine.parallel = "safe".to_string();
    routine.hash();

    let script = routine.get_script();
    assert!(script.contains("IMMUTABLE STRICT PARALLEL SAFE"));
    assert!(!script.contains("VOLATILE"));
}

#[test]
fn get_script_for_stable_leakproof_security_definer() {
    let mut routine = build_function_routine();
    routine.volatility = "stable".to_string();
    routine.is_leakproof = true;
    routine.security_definer = true;
    routine.hash();

    let script = routine.get_script();
    assert!(script.contains("STABLE LEAKPROOF PARALLEL UNSAFE SECURITY DEFINER"));
}

#[test]
fn get_script_procedure_only_emits_security_definer() {
    let mut routine = build_procedure_routine();
    routine.security_definer = true;
    routine.hash();

    let script = routine.get_script();
    assert!(script.contains("SECURITY DEFINER"));
    assert!(!script.contains("VOLATILE"));
    assert!(!script.contains("PARALLEL"));
    assert!(!script.contains("LEAKPROOF"));
}

#[test]
fn get_script_for_aggregate() {
    let mut routine = Routine::new(
        "public".to_string(),
        Oid(200),
        "my_sum".to_string(),
        "internal".to_string(),
        "aggregate".to_string(),
        "bigint".to_string(),
        "integer".to_string(),
        None,
        None,
        "-".to_string(),
    );
    routine.aggregate_info = Some(AggregateInfo {
        sfunc: "int4_sum".to_string(),
        stype: "bigint".to_string(),
        sspace: None,
        finalfunc: None,
        finalfunc_extra: false,
        finalfunc_modify: None,
        combinefunc: None,
        serialfunc: None,
        deserialfunc: None,
        initcond: Some("0".to_string()),
        msfunc: None,
        minvfunc: None,
        mstype: None,
        msspace: None,
        mfinalfunc: None,
        mfinalfunc_extra: false,
        mfinalfunc_modify: None,
        minitcond: None,
        sortop: None,
        kind: 'n',
        num_direct_args: 0,
    });
    routine.hash();

    let script = routine.get_script();
    assert!(script.contains("create or replace aggregate public.my_sum(integer)"));
    assert!(script.contains("SFUNC = int4_sum"));
    assert!(script.contains("STYPE = bigint"));
    assert!(script.contains("INITCOND = '0'"));
}

#[test]
fn get_drop_script_for_window_uses_function() {
    let routine = Routine::new(
        "public".to_string(),
        Oid(300),
        "my_window".to_string(),
        "plpgsql".to_string(),
        "window".to_string(),
        "integer".to_string(),
        "integer".to_string(),
        None,
        None,
        "BEGIN END".to_string(),
    );
    let script = routine.get_drop_script();
    assert!(script.starts_with("drop function"));
}

#[test]
fn hash_changes_when_volatility_changes() {
    let mut routine = build_function_routine();
    let h1 = routine.hash.clone();

    routine.volatility = "immutable".to_string();
    routine.hash();
    let h2 = routine.hash.clone();

    assert_ne!(h1, h2);
}

#[test]
fn get_aggregate_script_ordered_set() {
    let mut routine = Routine::new(
        "public".to_string(),
        Oid(500),
        "my_percentile".to_string(),
        "internal".to_string(),
        "aggregate".to_string(),
        "double precision".to_string(),
        "double precision, double precision".to_string(),
        None,
        None,
        "-".to_string(),
    );
    routine.aggregate_info = Some(AggregateInfo {
        sfunc: "ordered_set_transition".to_string(),
        stype: "internal".to_string(),
        sspace: None,
        finalfunc: Some("percentile_disc_final".to_string()),
        finalfunc_extra: true,
        finalfunc_modify: None,
        combinefunc: None,
        serialfunc: None,
        deserialfunc: None,
        initcond: None,
        msfunc: None,
        minvfunc: None,
        mstype: None,
        msspace: None,
        mfinalfunc: None,
        mfinalfunc_extra: false,
        mfinalfunc_modify: None,
        minitcond: None,
        sortop: None,
        kind: 'o',
        num_direct_args: 1,
    });
    routine.hash();

    let script = routine.get_script();
    assert!(
        script.contains(
            "create or replace aggregate public.my_percentile(double precision ORDER BY double precision)"
        ),
        "Expected ordered-set syntax with ORDER BY, got: {}",
        script
    );
    assert!(script.contains("SFUNC = ordered_set_transition"));
    assert!(script.contains("FINALFUNC = percentile_disc_final"));
    assert!(script.contains("FINALFUNC_EXTRA"));
}

#[test]
fn get_aggregate_script_hypothetical_set() {
    let mut routine = Routine::new(
        "public".to_string(),
        Oid(501),
        "my_rank".to_string(),
        "internal".to_string(),
        "aggregate".to_string(),
        "bigint".to_string(),
        "\"any\", \"any\"".to_string(),
        None,
        None,
        "-".to_string(),
    );
    routine.aggregate_info = Some(AggregateInfo {
        sfunc: "hypothetical_rank_sfunc".to_string(),
        stype: "internal".to_string(),
        sspace: None,
        finalfunc: Some("hypothetical_rank_final".to_string()),
        finalfunc_extra: true,
        finalfunc_modify: None,
        combinefunc: None,
        serialfunc: None,
        deserialfunc: None,
        initcond: None,
        msfunc: None,
        minvfunc: None,
        mstype: None,
        msspace: None,
        mfinalfunc: None,
        mfinalfunc_extra: false,
        mfinalfunc_modify: None,
        minitcond: None,
        sortop: None,
        kind: 'h',
        num_direct_args: 1,
    });
    routine.hash();

    let script = routine.get_script();
    assert!(
        script.contains("create or replace aggregate public.my_rank(\"any\" ORDER BY \"any\")"),
        "Expected hypothetical-set syntax with ORDER BY, got: {}",
        script
    );
    assert!(script.contains("HYPOTHETICAL"));
}

#[test]
fn get_aggregate_script_ordered_set_no_direct_args() {
    let mut routine = Routine::new(
        "public".to_string(),
        Oid(502),
        "my_mode".to_string(),
        "internal".to_string(),
        "aggregate".to_string(),
        "anyelement".to_string(),
        "anyelement".to_string(),
        None,
        None,
        "-".to_string(),
    );
    routine.aggregate_info = Some(AggregateInfo {
        sfunc: "ordered_set_transition".to_string(),
        stype: "internal".to_string(),
        sspace: None,
        finalfunc: Some("mode_final".to_string()),
        finalfunc_extra: true,
        finalfunc_modify: None,
        combinefunc: None,
        serialfunc: None,
        deserialfunc: None,
        initcond: None,
        msfunc: None,
        minvfunc: None,
        mstype: None,
        msspace: None,
        mfinalfunc: None,
        mfinalfunc_extra: false,
        mfinalfunc_modify: None,
        minitcond: None,
        sortop: None,
        kind: 'o',
        num_direct_args: 0,
    });
    routine.hash();

    let script = routine.get_script();
    assert!(
        script.contains("create or replace aggregate public.my_mode(ORDER BY anyelement)"),
        "Expected ordered-set syntax with no direct args, got: {}",
        script
    );
}

// -----------------------------------------------------------------
// split_arguments: single-quote handling (Issue #154)
// -----------------------------------------------------------------

#[test]
fn split_arguments_respects_single_quoted_comma() {
    // A single default value whose expression contains a comma inside quotes.
    let input = "','::character varying";
    let result = Routine::split_arguments(input);
    assert_eq!(result, vec!["','::character varying"]);
}

#[test]
fn split_arguments_multiple_defaults_with_quoted_comma() {
    // Two defaults: the first contains a quoted comma, the second is a plain number.
    let input = "','::character varying, 0";
    let result = Routine::split_arguments(input);
    assert_eq!(result, vec!["','::character varying", " 0"]);
}

#[test]
fn split_arguments_escaped_quotes() {
    // Two defaults: the first uses escaped single-quotes ('' inside a string).
    let input = "'''hello'''::text, 42";
    let result = Routine::split_arguments(input);
    assert_eq!(result, vec!["'''hello'''::text", " 42"]);
}

#[test]
fn split_arguments_parenthesized_still_works() {
    // Existing behaviour: parenthesised type expressions must not be split.
    let input = "a numeric(10,2), b text";
    let result = Routine::split_arguments(input);
    assert_eq!(result, vec!["a numeric(10,2)", " b text"]);
}

// -----------------------------------------------------------------
// split_arguments: JSONB default with commas (Issue #154)
// -----------------------------------------------------------------

#[test]
fn split_arguments_jsonb_default_with_commas() {
    // A JSONB literal default containing multiple comma-separated key-value
    // pairs must not be split into multiple arguments.
    let input = r#"p jsonb DEFAULT '{"key1": "value1", "key2": "value2"}'::jsonb"#;
    let result = Routine::split_arguments(input);
    assert_eq!(
        result,
        vec![r#"p jsonb DEFAULT '{"key1": "value1", "key2": "value2"}'::jsonb"#]
    );
}

#[test]
fn split_arguments_jsonb_default_multiple_args() {
    // JSONB default alongside other arguments: only the top-level comma
    // (outside the single-quoted literal) must be treated as a delimiter.
    let input = r#"id integer, opts jsonb DEFAULT '{"a": 1, "b": 2}'::jsonb"#;
    let result = Routine::split_arguments(input);
    assert_eq!(
        result,
        vec![
            "id integer",
            r#" opts jsonb DEFAULT '{"a": 1, "b": 2}'::jsonb"#
        ]
    );
}

#[test]
fn split_arguments_jsonb_default_multiline() {
    // A multiline JSONB literal (containing newlines inside the quoted string)
    // must be treated as a single argument — newlines and commas inside the
    // single-quoted literal must not trigger a split.
    let input =
        "p jsonb DEFAULT '{\n    \"key1\": \"value1\",\n    \"key2\": \"value2\"\n}'::jsonb";
    let result = Routine::split_arguments(input);
    assert_eq!(result, vec![input]);
}

#[test]
fn arguments_with_defaults_jsonb_default() {
    // Full round-trip for Issue #154: a function with a multi-key JSONB
    // default must reconstruct the full default without splitting on the
    // commas inside the quoted literal.
    let routine = Routine::new(
        "test_schema".to_string(),
        Oid(400),
        "foo".to_string(),
        "plpgsql".to_string(),
        "FUNCTION".to_string(),
        "void".to_string(),
        "p jsonb".to_string(),
        Some(r#"'{"key1": "value1", "key2": "value2"}'::jsonb"#.to_string()),
        None,
        "BEGIN END".to_string(),
    );

    let result = routine.arguments_with_defaults();
    assert_eq!(
        result,
        r#"p jsonb DEFAULT '{"key1": "value1", "key2": "value2"}'::jsonb"#
    );
}

#[test]
fn arguments_with_defaults_comma_default() {
    // Full round-trip: a procedure with two varchar params where only the
    // second has a default of ','::character varying.
    let routine = Routine::new(
        "test_schema".to_string(),
        Oid(300),
        "format_csv_line".to_string(),
        "plpgsql".to_string(),
        "PROCEDURE".to_string(),
        "void".to_string(),
        "p_value character varying, p_delimiter character varying".to_string(),
        Some("','::character varying".to_string()),
        None,
        "BEGIN RAISE NOTICE '%', p_value || p_delimiter; END".to_string(),
    );

    let result = routine.arguments_with_defaults();
    assert_eq!(
        result,
        "p_value character varying, p_delimiter character varying DEFAULT ','::character varying"
    );
}

// -----------------------------------------------------------------
// config (proconfig / SET parameters)
// -----------------------------------------------------------------

#[test]
fn new_has_empty_config_by_default() {
    let routine = build_function_routine();
    assert!(routine.config.is_empty());
}

#[test]
fn get_config_clause_empty_when_no_config() {
    let routine = build_function_routine();
    assert_eq!(routine.get_config_clause(false), "");
}

#[test]
fn get_config_clause_single_param() {
    // search_path is a GUC_LIST_QUOTE parameter, so its value is emitted verbatim.
    let mut routine = build_function_routine();
    routine.config = vec!["search_path=public".to_string()];
    assert_eq!(
        routine.get_config_clause(false),
        " SET search_path = public"
    );
}

#[test]
fn get_config_clause_multiple_params() {
    let mut routine = build_function_routine();
    routine.config = vec![
        "search_path=\"public, pg_temp\"".to_string(),
        "lock_timeout=5s".to_string(),
    ];
    assert_eq!(
        routine.get_config_clause(false),
        " SET search_path = \"public, pg_temp\" SET lock_timeout = '5s'"
    );
}

#[test]
fn get_config_clause_scalar_value_single_quote_is_escaped() {
    // A scalar (non-list) GUC is quoted as a literal, and single quotes inside the
    // value are doubled. search_path could not exercise this: it is list-quoted and
    // an apostrophe there lives inside a double-quoted element, not a literal.
    let mut routine = build_function_routine();
    routine.config = vec!["application_name=it's".to_string()];
    assert_eq!(
        routine.get_config_clause(false),
        " SET application_name = 'it''s'"
    );
}

#[test]
fn get_config_clause_skips_malformed_entry() {
    let mut routine = build_function_routine();
    routine.config = vec![
        "no_equals_sign".to_string(),
        "search_path=public".to_string(),
    ];
    // The malformed entry (no '=') is skipped; only the valid one is emitted.
    assert_eq!(
        routine.get_config_clause(false),
        " SET search_path = public"
    );
}

#[test]
fn get_config_clause_value_with_equals_sign() {
    // Values can contain '=' (e.g. an expression); only split on the first '='.
    let mut routine = build_function_routine();
    routine.config = vec!["extra_float_digits=3=yes".to_string()];
    assert_eq!(
        routine.get_config_clause(false),
        " SET extra_float_digits = '3=yes'"
    );
}

#[test]
fn get_config_clause_empty_value() {
    // An empty value on a scalar GUC still emits an empty string literal.
    let mut routine = build_function_routine();
    routine.config = vec!["application_name=".to_string()];
    assert_eq!(
        routine.get_config_clause(false),
        " SET application_name = ''"
    );
}

// An empty value on a list-quote GUC must not be emitted verbatim: `SET search_path =`
// with nothing after it is a syntax error. A bare empty is the empty list, which only
// PostgreSQL 19's `TO NULL` can write (see the PG19 block below); for a server not
// known to accept that it falls back to an empty string literal, valid everywhere.
#[test]
fn get_config_clause_empty_value_list_guc() {
    let mut routine = build_function_routine();
    routine.config = vec!["search_path=".to_string()];
    assert_eq!(routine.get_config_clause(false), " SET search_path = ''");
}

// Issue #217: a search_path list whose elements need no quoting (`t, pg_temp`) was
// wrapped in a single-quoted literal, so PostgreSQL re-stored it as one schema named
// "t, pg_temp" and every subsequent compare re-emitted the routine. GUC_LIST_QUOTE
// values must be emitted verbatim, exactly as pg_get_functiondef does.
#[test]
fn get_config_clause_list_guc_comma_list_is_verbatim() {
    let mut routine = build_function_routine();
    routine.config = vec!["search_path=t, pg_temp".to_string()];
    assert_eq!(
        routine.get_config_clause(false),
        " SET search_path = t, pg_temp"
    );
}

#[test]
fn get_config_clause_list_guc_quoted_element_is_verbatim() {
    let mut routine = build_function_routine();
    routine.config = vec!["search_path=\"My Schema\", public".to_string()];
    assert_eq!(
        routine.get_config_clause(false),
        " SET search_path = \"My Schema\", public"
    );
}

#[test]
fn get_config_clause_temp_tablespaces_is_list_guc() {
    let mut routine = build_function_routine();
    routine.config = vec!["temp_tablespaces=ts1, ts2".to_string()];
    assert_eq!(
        routine.get_config_clause(false),
        " SET temp_tablespaces = ts1, ts2"
    );
}

#[test]
fn get_config_clause_guc_name_match_is_case_insensitive() {
    // GUC names are case-insensitive, so Search_Path is still the list-quote parameter.
    let mut routine = build_function_routine();
    routine.config = vec!["Search_Path=t, pg_temp".to_string()];
    assert_eq!(
        routine.get_config_clause(false),
        " SET Search_Path = t, pg_temp"
    );
}

// A scalar GUC whose value happens to contain a double quote must still be quoted as
// a literal; the pre-fix heuristic keyed on the '"' character and emitted it verbatim,
// producing invalid SQL.
#[test]
fn get_config_clause_scalar_value_with_double_quote_is_quoted() {
    let mut routine = build_function_routine();
    routine.config = vec!["application_name=a\"b".to_string()];
    assert_eq!(
        routine.get_config_clause(false),
        " SET application_name = 'a\"b'"
    );
}

#[test]
fn hash_changes_when_config_changes() {
    let mut r1 = build_function_routine();
    let h1 = r1.hash.clone();

    r1.config = vec!["search_path=public".to_string()];
    r1.hash();
    let h2 = r1.hash.clone();

    assert_ne!(h1, h2, "adding config must change the hash");
}

#[test]
fn hash_differs_for_different_config_values() {
    let mut r1 = build_function_routine();
    r1.config = vec!["search_path=public".to_string()];
    r1.hash();

    let mut r2 = build_function_routine();
    r2.config = vec!["search_path=pg_catalog".to_string()];
    r2.hash();

    assert_ne!(r1.hash, r2.hash);
}

#[test]
fn hash_differs_for_different_config_order() {
    let mut r1 = build_function_routine();
    r1.config = vec![
        "search_path=public".to_string(),
        "lock_timeout=5s".to_string(),
    ];
    r1.hash();

    let mut r2 = build_function_routine();
    r2.config = vec![
        "lock_timeout=5s".to_string(),
        "search_path=public".to_string(),
    ];
    r2.hash();

    assert_ne!(r1.hash, r2.hash, "config order matters for hash");
}

#[test]
fn hash_identical_for_same_config() {
    let mut r1 = build_function_routine();
    r1.config = vec!["search_path=public".to_string()];
    r1.hash();

    let mut r2 = build_function_routine();
    r2.config = vec!["search_path=public".to_string()];
    r2.hash();

    assert_eq!(r1.hash, r2.hash);
}

#[test]
fn get_script_function_with_config() {
    let mut routine = build_function_routine();
    routine.config = vec!["work_mem=256MB".to_string()];
    routine.hash();

    let script = routine.get_script();
    assert!(
        script.contains("PARALLEL UNSAFE SET work_mem = '256MB' as $$"),
        "SET clause must appear between flags and AS, got:\n{}",
        script
    );
}

#[test]
fn get_script_function_with_multiple_config() {
    let mut routine = build_function_routine();
    routine.config = vec![
        "search_path=public, pg_temp".to_string(),
        "statement_timeout=30s".to_string(),
    ];
    routine.hash();

    let script = routine.get_script();
    assert!(
        script.contains("SET search_path = public, pg_temp SET statement_timeout = '30s'"),
        "all SET clauses must appear in order, got:\n{}",
        script
    );
}

#[test]
fn get_script_procedure_with_config() {
    let mut routine = build_procedure_routine();
    routine.security_definer = true;
    routine.config = vec![
        "search_path=public, pg_temp".to_string(),
        "lock_timeout=5s".to_string(),
    ];
    routine.hash();

    let script = routine.get_script();
    assert!(
        script.contains(
            "SECURITY DEFINER SET search_path = public, pg_temp SET lock_timeout = '5s' as $$"
        ),
        "procedure SET clauses must appear after SECURITY DEFINER and before AS, got:\n{}",
        script
    );
    // Must not contain function-only flags
    assert!(!script.contains("VOLATILE"));
    assert!(!script.contains("PARALLEL"));
}

#[test]
fn get_script_procedure_config_only_no_security_definer() {
    let mut routine = build_procedure_routine();
    routine.config = vec!["search_path=public".to_string()];
    routine.hash();

    let script = routine.get_script();
    assert!(
        script.contains("language sql SET search_path = public as $$"),
        "config must appear even without SECURITY DEFINER, got:\n{}",
        script
    );
}

#[test]
fn get_script_function_no_config_unchanged() {
    // Ensure functions without config still generate the same output as before.
    let routine = build_function_routine();
    let script = routine.get_script();
    assert!(!script.contains("SET "));
    assert!(script.contains("VOLATILE PARALLEL UNSAFE as $$"));
}

// ---- PG19: SET <list GUC> TO NULL, and element-wise literals for list GUCs ----

fn guc_list(value: &str) -> Vec<(String, bool)> {
    split_guc_list(value)
        .expect("value must parse as a GUC list")
        .into_iter()
        .map(|e| (e.value, e.quoted))
        .collect()
}

fn owned(elements: &[(&str, bool)]) -> Vec<(String, bool)> {
    elements.iter().map(|&(v, q)| (v.to_string(), q)).collect()
}

fn config_clause(entry: &str, set_list_to_null: bool) -> String {
    let mut routine = build_function_routine();
    routine.config = vec![entry.to_string()];
    routine.get_config_clause(set_list_to_null)
}

#[test]
fn split_guc_list_empty_and_blank_values_are_the_empty_list() {
    assert_eq!(guc_list(""), Vec::<(String, bool)>::new());
    assert_eq!(
        guc_list(" \t\n\r\u{0b}\u{0c}"),
        Vec::<(String, bool)>::new()
    );
}

#[test]
fn split_guc_list_unquoted_elements_keep_case_and_length() {
    // SplitGUCList never downcases or truncates, unlike the SQL lexer.
    assert_eq!(
        guc_list("MySchema,public"),
        owned(&[("MySchema", false), ("public", false)])
    );
    let long = "x".repeat(70);
    assert_eq!(guc_list(&long), owned(&[(&long, false)]));
}

#[test]
fn split_guc_list_skips_whitespace_around_elements_and_commas() {
    assert_eq!(
        guc_list("  t ,\tpg_temp\n"),
        owned(&[("t", false), ("pg_temp", false)])
    );
    assert_eq!(
        guc_list("\"$user\", public"),
        owned(&[("$user", true), ("public", false)])
    );
}

#[test]
fn split_guc_list_quoted_elements() {
    // `""` inside quotes is one literal quote; a quoted comma does not split.
    assert_eq!(
        guc_list("\"a\"\"b\", \"it's\", \"x, y\""),
        owned(&[("a\"b", true), ("it's", true), ("x, y", true)])
    );
    // The zero-length element `SET search_path = ''` stores.
    assert_eq!(guc_list("\"\""), owned(&[("", true)]));
    assert_eq!(
        guc_list("\"\", public"),
        owned(&[("", true), ("public", false)])
    );
}

#[test]
fn split_guc_list_rejects_what_split_guc_list_rejects() {
    for bad in ["\"unterminated", "a,,b", "a,", ",a", "a b", "\"a\"b"] {
        assert!(split_guc_list(bad).is_none(), "{bad:?} must not parse");
    }
}

#[test]
fn guc_list_element_survives_verbatim_only_when_the_lexer_keeps_it() {
    let el = |value: &str, quoted: bool| GucListElement {
        value: value.to_string(),
        quoted,
    };
    assert!(el("public", false).survives_verbatim());
    assert!(el("My Schema", true).survives_verbatim());
    assert!(el("$user", true).survives_verbatim());
    // Mixed case unquoted is downcased by the lexer, as search_path itself would.
    assert!(el("MySchema", false).survives_verbatim());
    assert!(el(&"x".repeat(63), false).survives_verbatim());
    assert!(el(&"x".repeat(63), true).survives_verbatim());

    // Zero-length delimited identifier.
    assert!(!el("", true).survives_verbatim());
    // Truncated to NAMEDATALEN - 1 bytes, quoted or not.
    assert!(!el(&"x".repeat(64), false).survives_verbatim());
    assert!(!el(&"x".repeat(64), true).survives_verbatim());
    // 32 characters, but 64 bytes: the limit is in bytes.
    assert!(!el(&"é".repeat(32), true).survives_verbatim());
    // Neither an identifier nor a number when unquoted (only `SET … FROM CURRENT`
    // stores these).
    assert!(!el("$user", false).survives_verbatim());
    assert!(!el("my-schema", false).survives_verbatim());
    assert!(!el("1st", false).survives_verbatim());
}

#[test]
fn guc_list_element_numbers_survive_verbatim() {
    // `SET search_path = public, 42` stores `public, 42`: flatten_set_variable_args
    // writes a numeric element as its bare text, and the lexer reads it back as the
    // same number. A number is not an identifier, so it is never truncated either
    // (a 70-digit one is stored and re-stored whole, verified on 18 and 19).
    let el = |value: &str| GucListElement {
        value: value.to_string(),
        quoted: false,
    };
    for number in ["1", "42", "-7", "-1.5e3", ".5", "1.", "99999999999"] {
        assert!(el(number).survives_verbatim(), "{number:?}");
    }
    assert!(el(&"9".repeat(70)).survives_verbatim());
}

#[test]
fn is_numeric_constant_matches_one_lexer_number() {
    for number in [
        "0",
        "1",
        "42",
        "-7",
        "-2147483648",
        "99999999999",
        "1.",
        ".5",
        "-1.5e3",
        "1.5E+10",
        "2e-3",
        // PostgreSQL 16+ digit separators and 15+ non-decimal integers, whose text
        // is stored verbatim when the value does not fit int4.
        "1_000_000_000_000",
        "1_0.5_0",
        "0x7FFF_FFFF_FFFF",
        "0x_1f",
        "0o17",
        "0B1_0",
    ] {
        assert!(is_numeric_constant(number), "{number:?} is a number");
    }
    for not_number in [
        "", "-", "+1", "--1", ".", "e5", "1e", "1e+", "1.2.3", "1..", "_1", "1_", "1__0", "1_.5",
        "1._5", "1abc", "0x", "0xg", "0b2", "0o8", "public",
    ] {
        assert!(
            !is_numeric_constant(not_number),
            "{not_number:?} is not a number"
        );
    }
}

#[test]
fn get_config_clause_empty_list_is_to_null_where_the_server_accepts_it() {
    // PG19: `SET search_path TO NULL` stores the bare `search_path=`.
    assert_eq!(
        config_clause("search_path=", true),
        " SET search_path TO NULL"
    );
    assert_eq!(
        config_clause("temp_tablespaces=", true),
        " SET temp_tablespaces TO NULL"
    );
    // GUC names are case-insensitive.
    assert_eq!(
        config_clause("Temp_Tablespaces=", true),
        " SET Temp_Tablespaces TO NULL"
    );
    // Not known to be PG19+: the form every version accepts, as before.
    assert_eq!(
        config_clause("search_path=", false),
        " SET search_path = ''"
    );
}

#[test]
fn get_config_clause_empty_value_of_other_gucs_is_never_null() {
    // `TO NULL` is rejected for scalars ("NULL is an invalid value"), and for a list
    // GUC without GUC_LIST_QUOTE `''` stores the same bare `name=` on every version.
    assert_eq!(
        config_clause("application_name=", true),
        " SET application_name = ''"
    );
    assert_eq!(config_clause("DateStyle=", true), " SET DateStyle = ''");
}

#[test]
fn get_config_clause_non_quote_list_guc_is_one_literal() {
    // flatten_set_variable_args stores a non-GUC_LIST_QUOTE list unquoted, so one
    // literal re-stores it exactly.
    assert_eq!(
        config_clause("DateStyle=ISO, MDY", true),
        " SET DateStyle = 'ISO, MDY'"
    );
}

// `SET search_path = ''` — the common hardening pattern — stores `search_path=""`.
// Pasting that back verbatim is `SET search_path = ""`, which every version rejects
// with "zero-length delimited identifier", so the routine was never migrated.
#[test]
fn get_config_clause_empty_string_element_is_a_literal() {
    for set_list_to_null in [false, true] {
        assert_eq!(
            config_clause("search_path=\"\"", set_list_to_null),
            " SET search_path TO ''"
        );
        assert_eq!(
            config_clause("search_path=\"\", public", set_list_to_null),
            " SET search_path TO '', 'public'"
        );
        assert_eq!(
            config_clause("temp_tablespaces=\"\"", set_list_to_null),
            " SET temp_tablespaces TO ''"
        );
    }
}

#[test]
fn get_config_clause_over_long_element_is_a_literal() {
    // The lexer would truncate a 70-byte identifier to 63; a literal is not truncated.
    let long = "x".repeat(70);
    assert_eq!(
        config_clause(&format!("search_path={long}, public"), false),
        format!(" SET search_path TO '{long}', 'public'")
    );
    // One element forces the literal form for the whole list; quotes inside an
    // element are kept and single quotes doubled.
    assert_eq!(
        config_clause(&format!("search_path=\"it's\", \"{long}\""), false),
        format!(" SET search_path TO 'it''s', '{long}'")
    );
    // At exactly 63 bytes the verbatim form still re-stores identically.
    let max = "y".repeat(63);
    assert_eq!(
        config_clause(&format!("search_path={max}"), false),
        format!(" SET search_path = {max}")
    );
}

#[test]
fn get_config_clause_ordinary_lists_stay_verbatim() {
    // Byte-identical to pgc 1.0.x for every list the lexer re-reads unchanged.
    for value in [
        "public",
        "t, pg_temp",
        "\"$user\", public",
        "\"My Schema\", public",
        "\"a\"\"b\", \"it's\"",
        "\"select\", \"UPPER\"",
        "wait, lsn, repack",
    ] {
        assert_eq!(
            config_clause(&format!("search_path={value}"), true),
            format!(" SET search_path = {value}"),
            "{value:?}"
        );
    }
}

#[test]
fn get_config_clause_numeric_elements_stay_verbatim() {
    // As literals ('1', '42') they would be stored back as `"1"` / `"42"`, and the
    // routine re-emitted on every compare (pgc 1.0.x emitted these verbatim too).
    for value in ["1", "public, 42", "-1.5e3, .5, 1.", "99999999999, -7"] {
        assert_eq!(
            config_clause(&format!("search_path={value}"), false),
            format!(" SET search_path = {value}"),
            "{value:?}"
        );
    }
}

#[test]
fn get_config_clause_literal_form_keeps_numbers_bare() {
    // `SET search_path TO '', 42, -1.5, public, 'My S'` stores
    // `"", 42, -1.5, public, "My S"` (verified on 14, 18 and 19): every element but
    // the numbers is a literal, and each is stored back as it was.
    assert_eq!(
        config_clause("search_path=\"\", 42, -1.5, public, \"My S\"", false),
        " SET search_path TO '', 42, -1.5, 'public', 'My S'"
    );
    // A quoted number is a string, not a number.
    assert_eq!(
        config_clause("search_path=\"\", \"42\"", false),
        " SET search_path TO '', '42'"
    );
}

#[test]
fn get_config_clause_unlexable_unquoted_element_is_a_literal() {
    // `SET search_path FROM CURRENT` after set_config('search_path', '$user, public')
    // stores the raw text; `SET search_path = $user, public` is a syntax error.
    assert_eq!(
        config_clause("search_path=$user, public", false),
        " SET search_path TO '$user', 'public'"
    );
    // A value SplitGUCList cannot parse is pasted back as before.
    assert_eq!(
        config_clause("search_path=a,,b", false),
        " SET search_path = a,,b"
    );
}

#[test]
fn get_config_clause_output_plugin_libraries_is_a_list_quote_guc() {
    // Wrapping `pgoutput, test_decoding` in one literal re-stores it as the single
    // element "pgoutput, test_decoding".
    assert_eq!(
        config_clause("output_plugin_libraries=pgoutput, test_decoding", false),
        " SET output_plugin_libraries = pgoutput, test_decoding"
    );
    assert_eq!(
        config_clause("output_plugin_libraries=\"pgoutput, test_decoding\"", false),
        " SET output_plugin_libraries = \"pgoutput, test_decoding\""
    );
    assert_eq!(
        config_clause("oauth_validator_libraries=\"\"", false),
        " SET oauth_validator_libraries TO ''"
    );
    assert_eq!(LIST_QUOTE_GUCS.len(), 8);
    assert!(is_list_quote_guc("OUTPUT_PLUGIN_LIBRARIES"));
    assert!(is_list_quote_guc("oauth_validator_libraries"));
    assert!(!is_list_quote_guc("DateStyle"));
}

#[test]
fn get_config_clause_mixes_forms_in_order() {
    let mut routine = build_function_routine();
    routine.config = vec![
        "search_path=\"\"".to_string(),
        "work_mem=64MB".to_string(),
        "temp_tablespaces=".to_string(),
    ];
    assert_eq!(
        routine.get_config_clause(true),
        " SET search_path TO '' SET work_mem = '64MB' SET temp_tablespaces TO NULL"
    );
}

#[test]
fn get_script_for_server_only_differs_for_an_empty_list() {
    let mut routine = build_procedure_routine();
    routine.config = vec!["search_path=".to_string()];
    routine.hash();
    assert_eq!(
        routine.get_script_for_server(true),
        "create or replace procedure public.do_something(a integer) language sql SET search_path TO NULL as $$SELECT a;$$;\n\n"
    );
    assert_eq!(
        routine.get_script(),
        "create or replace procedure public.do_something(a integer) language sql SET search_path = '' as $$SELECT a;$$;\n\n"
    );
    assert_eq!(routine.get_script(), routine.get_script_for_server(false));

    let mut ordinary = build_function_routine();
    ordinary.config = vec![
        "search_path=\"\", public".to_string(),
        "lock_timeout=5s".to_string(),
    ];
    ordinary.hash();
    assert_eq!(ordinary.get_script(), ordinary.get_script_for_server(true));
}

#[test]
fn hash_of_list_guc_config_is_the_raw_proconfig_text() {
    // Emission changed, the hash did not: it folds proconfig verbatim, so these are
    // the values pgc 1.0.x computed and stored for the same routines.
    let hash_of = |entry: &str| {
        let mut routine = build_function_routine();
        routine.config = vec![entry.to_string()];
        routine.hash();
        routine.hash.unwrap()
    };
    assert_eq!(
        hash_of("search_path=\"\""),
        "aa76498f0d63cf4a5ae426f4eee4cc29"
    );
    assert_eq!(hash_of("search_path="), "d4d3277d05a91f398399e07ddda65c39");
    assert_eq!(
        hash_of("search_path=public, pg_temp"),
        "2237925521f624c2c9f94095581e52b4"
    );
}

// ---- PG19: aggregate SUPPORT / PARALLEL / negative SSPACE, CREATE OR REPLACE ----

/// A count(bigint)-shaped aggregate; `msspace: Some(_)` also gives it a moving
/// implementation so MSSPACE is meaningful.
fn build_count_aggregate(sspace: Option<i32>, msspace: Option<i32>) -> Routine {
    let mut routine = Routine::new(
        "test_schema".to_string(),
        Oid(900),
        "pg19_cnt".to_string(),
        "internal".to_string(),
        "aggregate".to_string(),
        "bigint".to_string(),
        "bigint".to_string(),
        None,
        None,
        "aggregate_dummy".to_string(),
    );
    routine.aggregate_info = Some(AggregateInfo {
        sfunc: "int8inc_any".to_string(),
        stype: "bigint".to_string(),
        sspace,
        finalfunc: None,
        finalfunc_extra: false,
        finalfunc_modify: None,
        combinefunc: None,
        serialfunc: None,
        deserialfunc: None,
        initcond: Some("0".to_string()),
        msfunc: msspace.map(|_| "int8inc_any".to_string()),
        minvfunc: msspace.map(|_| "int8dec_any".to_string()),
        mstype: msspace.map(|_| "bigint".to_string()),
        msspace,
        mfinalfunc: None,
        mfinalfunc_extra: false,
        mfinalfunc_modify: None,
        minitcond: None,
        sortop: None,
        kind: 'n',
        num_direct_args: 0,
    });
    routine.hash();
    routine
}

#[test]
fn aggregate_script_is_create_or_replace() {
    // A plain CREATE AGGREGATE failed with "already exists" on every in-place change.
    let routine = build_count_aggregate(None, None);
    assert_eq!(
        routine.get_script(),
        "create or replace aggregate test_schema.pg19_cnt(bigint) (\n    SFUNC = int8inc_any,\n    STYPE = bigint,\n    INITCOND = '0'\n);\n\n"
    );
}

#[test]
fn aggregate_script_emits_support() {
    let mut routine = build_count_aggregate(None, None);
    routine.support_function = Some("int8inc_support".to_string());
    routine.hash();
    assert_eq!(
        routine.get_script(),
        "create or replace aggregate test_schema.pg19_cnt(bigint) (\n    SFUNC = int8inc_any,\n    STYPE = bigint,\n    INITCOND = '0',\n    SUPPORT = int8inc_support\n);\n\n"
    );
    // SUPPORT is a pg_proc property, kept out of the hashed option body.
    assert!(
        !routine
            .aggregate_info
            .as_ref()
            .unwrap()
            .get_options_body()
            .contains("SUPPORT")
    );
}

#[test]
fn aggregate_script_spells_sortop_as_operator() {
    // `SORTOP = pg_catalog.>` is a syntax error: the grammar takes a bare operator
    // or OPERATOR(schema.op), and regoper output is schema-qualified whenever the
    // name alone is ambiguous. pg_dump writes OPERATOR(...) as well.
    let mut routine = build_count_aggregate(None, None);
    routine.aggregate_info.as_mut().unwrap().sortop = Some("pg_catalog.>".to_string());
    routine.hash();
    assert_eq!(
        routine.get_script(),
        "create or replace aggregate test_schema.pg19_cnt(bigint) (\n    SFUNC = int8inc_any,\n    STYPE = bigint,\n    INITCOND = '0',\n    SORTOP = OPERATOR(pg_catalog.>)\n);\n\n"
    );
    // A quoted schema, or an operator regoper prints unqualified, alike.
    routine.aggregate_info.as_mut().unwrap().sortop = Some("\"My Ops\".===".to_string());
    assert!(
        routine
            .get_script()
            .contains("    SORTOP = OPERATOR(\"My Ops\".===)\n"),
        "{}",
        routine.get_script()
    );
    routine.aggregate_info.as_mut().unwrap().sortop = Some("===".to_string());
    assert!(
        routine
            .get_script()
            .contains("    SORTOP = OPERATOR(===)\n")
    );
}

#[test]
fn aggregate_hash_is_unchanged_by_the_sortop_spelling() {
    // Only the script spells OPERATOR(...); the hashed option body keeps the plain
    // regoper text, so the hash is the one pgc 1.0.x computed.
    let mut routine = build_count_aggregate(None, None);
    routine.aggregate_info.as_mut().unwrap().sortop = Some("pg_catalog.>".to_string());
    routine.hash();
    assert!(
        routine
            .aggregate_info
            .as_ref()
            .unwrap()
            .get_options_body()
            .ends_with("    INITCOND = '0',\n    SORTOP = pg_catalog.>")
    );
    assert_eq!(
        routine.hash.as_deref(),
        Some("bc8f5674a4345bc35aa646caf48f6d61")
    );
}

#[test]
fn aggregate_script_without_support_has_no_support_option() {
    let routine = build_count_aggregate(Some(0), None);
    assert!(!routine.get_script().contains("SUPPORT"));
}

#[test]
fn aggregate_script_emits_parallel_safe_and_restricted() {
    let mut routine = build_count_aggregate(None, None);
    routine.parallel = "safe".to_string();
    routine.hash();
    assert!(
        routine
            .get_script()
            .contains("    INITCOND = '0',\n    PARALLEL = SAFE\n);"),
        "{}",
        routine.get_script()
    );

    routine.parallel = "restricted".to_string();
    routine.hash();
    assert!(
        routine
            .get_script()
            .contains("    INITCOND = '0',\n    PARALLEL = RESTRICTED\n);")
    );

    // UNSAFE is the default CREATE OR REPLACE AGGREGATE resets to.
    routine.parallel = "unsafe".to_string();
    routine.hash();
    assert!(!routine.get_script().contains("PARALLEL"));
}

#[test]
fn aggregate_script_appends_support_and_parallel_after_hypothetical() {
    let mut routine = Routine::new(
        "test_schema".to_string(),
        Oid(901),
        "pg19_rank".to_string(),
        "internal".to_string(),
        "aggregate".to_string(),
        "bigint".to_string(),
        "VARIADIC \"any\" ORDER BY VARIADIC \"any\"".to_string(),
        None,
        None,
        "aggregate_dummy".to_string(),
    );
    routine.aggregate_info = Some(AggregateInfo {
        sfunc: "ordered_set_transition_multi".to_string(),
        stype: "internal".to_string(),
        sspace: None,
        finalfunc: Some("rank_final".to_string()),
        finalfunc_extra: true,
        finalfunc_modify: None,
        combinefunc: None,
        serialfunc: None,
        deserialfunc: None,
        initcond: None,
        msfunc: None,
        minvfunc: None,
        mstype: None,
        msspace: None,
        mfinalfunc: None,
        mfinalfunc_extra: false,
        mfinalfunc_modify: None,
        minitcond: None,
        sortop: None,
        kind: 'h',
        num_direct_args: 1,
    });
    routine.support_function = Some("int8inc_support".to_string());
    routine.parallel = "safe".to_string();
    routine.hash();
    // Option order is irrelevant to PostgreSQL (verified on 19beta4).
    assert!(
        routine.get_script().contains(
            "    FINALFUNC_EXTRA,\n    HYPOTHETICAL,\n    SUPPORT = int8inc_support,\n    PARALLEL = SAFE\n);"
        ),
        "{}",
        routine.get_script()
    );
}

#[test]
fn aggregate_script_emits_negative_state_space() {
    // PG19: a negative SSPACE marks an unbounded state. It was dropped (> 0 test).
    let routine = build_count_aggregate(Some(-1), None);
    assert!(
        routine
            .get_script()
            .contains("    STYPE = bigint,\n    SSPACE = -1,\n"),
        "{}",
        routine.get_script()
    );
    let routine = build_count_aggregate(Some(0), Some(-1));
    assert!(
        routine
            .get_script()
            .contains("    MSTYPE = bigint,\n    MSSPACE = -1\n"),
        "{}",
        routine.get_script()
    );
    // 0 is the default and stays omitted.
    let routine = build_count_aggregate(Some(0), Some(0));
    assert!(!routine.get_script().contains("SSPACE"));
}

#[test]
fn aggregate_script_is_the_same_for_every_server() {
    let mut routine = build_count_aggregate(Some(-1), None);
    routine.support_function = Some("int8inc_support".to_string());
    routine.parallel = "safe".to_string();
    routine.hash();
    assert_eq!(routine.get_script(), routine.get_script_for_server(true));
}

#[test]
fn aggregate_hash_is_unchanged_for_non_negative_state_space() {
    // Pinned to what pgc 1.0.x computed for the same aggregates: emitting SUPPORT
    // and PARALLEL, and the switch to CREATE OR REPLACE, must not move any hash.
    let hash = |r: &Routine| r.hash.clone().unwrap();
    assert_eq!(
        hash(&build_count_aggregate(None, None)),
        "aefc8fc7809942aa3f3da9773d368abb"
    );
    assert_eq!(
        hash(&build_count_aggregate(Some(0), None)),
        "aefc8fc7809942aa3f3da9773d368abb"
    );
    assert_eq!(
        hash(&build_count_aggregate(Some(16), None)),
        "28ee5b7c63260cccb3987d914f1f8e4f"
    );
    assert_eq!(
        hash(&build_count_aggregate(Some(0), Some(0))),
        "998fda60d7d8cc2406a4b663c47ffb1c"
    );

    let mut support = build_count_aggregate(Some(0), None);
    support.support_function = Some("int8inc_support".to_string());
    support.hash();
    assert_eq!(hash(&support), "718d19333e012ae82336c89711c28cd1");

    let mut parallel = build_count_aggregate(Some(0), None);
    parallel.parallel = "safe".to_string();
    parallel.hash();
    assert_eq!(hash(&parallel), "19678d6aa496415886b2c925fdeba4b7");
}

#[test]
fn aggregate_hash_sees_negative_state_space() {
    // The one intended hash change: pgc 1.0.x hashed SSPACE = -1 exactly like 0
    // (aefc8fc7… / 998fda60…), so a 0 <-> -1 change produced an empty diff. A
    // 1.0.x dump of such an aggregate compares as changed once, and the resulting
    // CREATE OR REPLACE AGGREGATE restores the lost value.
    let neg = build_count_aggregate(Some(-1), None);
    assert_eq!(
        neg.hash.as_deref(),
        Some("5160ac2c66a93dfa7d4cb4e302607d92")
    );
    assert_ne!(neg.hash, build_count_aggregate(Some(0), None).hash);

    let mneg = build_count_aggregate(Some(0), Some(-1));
    assert_eq!(
        mneg.hash.as_deref(),
        Some("f4a703684ae82a06636f704db7569bc4")
    );
    assert_ne!(mneg.hash, build_count_aggregate(Some(0), Some(0)).hash);
}

#[test]
fn aggregate_hash_changes_when_only_support_changes() {
    let without = build_count_aggregate(None, None);
    let mut with = build_count_aggregate(None, None);
    with.support_function = Some("int8inc_support".to_string());
    with.hash();
    assert_ne!(without.hash, with.hash);
}

#[test]
fn get_drop_routine_script_is_kind_agnostic() {
    // A zero-argument aggregate is `()` here, not the `(*)` DROP AGGREGATE needs.
    let count_star = Routine::new(
        "test_schema".to_string(),
        Oid(902),
        "pg19_cnt_star".to_string(),
        "internal".to_string(),
        "aggregate".to_string(),
        "bigint".to_string(),
        "".to_string(),
        None,
        None,
        "aggregate_dummy".to_string(),
    );
    assert_eq!(
        count_star.get_drop_routine_script(),
        "drop routine if exists test_schema.pg19_cnt_star () cascade;\n\n"
    );
    assert_eq!(
        build_function_routine().get_drop_routine_script(),
        "drop routine if exists public.add (a integer) cascade;\n\n"
    );
}

// ==== r3-ddl: routine language ====

/// The dump stores `pg_language.lanname` raw; it is spelled as `quote_ident`
/// would, so every language PostgreSQL ships stays bare (the output is
/// unchanged) and a name that needs quoting cannot break the statement.
#[test]
fn the_routine_language_is_quoted_only_when_it_needs_it() {
    let mut function = build_function_routine();
    function.lang = "My Lang\"; drop table t; --".to_string();
    assert!(
        function
            .get_script()
            .contains(" returns integer language \"My Lang\"\"; drop table t; --\" VOLATILE"),
        "{}",
        function.get_script()
    );
    let mut procedure = build_procedure_routine();
    procedure.lang = "PlX".to_string();
    assert!(
        procedure.get_script().starts_with(
            "create or replace procedure public.do_something(a integer) language \"PlX\" as"
        ),
        "{}",
        procedure.get_script()
    );
    for lang in ["sql", "plpgsql", "c", "internal", "plpython3u"] {
        let mut routine = build_function_routine();
        routine.lang = lang.to_string();
        assert!(
            routine
                .get_script()
                .contains(&format!(" language {lang} VOLATILE")),
            "{lang}"
        );
    }
}

// ---- r3:types — an empty list GUC on a server older than PostgreSQL 19 ----

#[test]
fn with_empty_lists_as_empty_strings_rewrites_only_empty_list_gucs() {
    let mut routine = build_function_routine();
    routine.config = vec![
        "search_path=".to_string(),
        "work_mem=64kB".to_string(),
        "TEMP_TABLESPACES= ".to_string(),
        "DateStyle=".to_string(),
        "session_preload_libraries=a, b".to_string(),
    ];
    routine.hash();

    let portable = routine
        .with_empty_lists_as_empty_strings()
        .expect("an empty list");
    assert_eq!(
        portable.config,
        vec![
            "search_path=\"\"".to_string(),
            "work_mem=64kB".to_string(),
            "TEMP_TABLESPACES=\"\"".to_string(),
            // Not a GUC_LIST_QUOTE parameter: an empty string, kept as is.
            "DateStyle=".to_string(),
            "session_preload_libraries=a, b".to_string(),
        ]
    );
    assert_ne!(portable.hash, routine.hash);
    // Hashed like a routine that holds `""` itself — what a pre-19 server
    // stores for `SET search_path = ''`.
    let mut stored = routine.clone();
    stored.config = portable.config.clone();
    stored.hash();
    assert_eq!(portable.hash, stored.hash);
    // The fallback script stores exactly that, and is unchanged.
    assert_eq!(
        routine.get_script_for_server(false),
        portable
            .get_script_for_server(false)
            .replace("SET search_path TO ''", "SET search_path = ''")
            .replace("SET TEMP_TABLESPACES TO ''", "SET TEMP_TABLESPACES = ''")
    );

    assert_eq!(
        routine.empty_list_settings(),
        vec!["search_path", "TEMP_TABLESPACES"]
    );
}

#[test]
fn with_empty_lists_as_empty_strings_is_none_without_an_empty_list() {
    let mut routine = build_function_routine();
    assert!(routine.with_empty_lists_as_empty_strings().is_none());
    routine.config = vec![
        "search_path=\"\"".to_string(),
        "search_path=public".to_string(),
    ];
    assert!(routine.with_empty_lists_as_empty_strings().is_none());
    assert!(routine.empty_list_settings().is_empty());
}
