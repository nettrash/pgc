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
    assert!(script.contains("create aggregate public.my_sum(integer)"));
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
            "create aggregate public.my_percentile(double precision ORDER BY double precision)"
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
        script.contains("create aggregate public.my_rank(\"any\" ORDER BY \"any\")"),
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
        script.contains("create aggregate public.my_mode(ORDER BY anyelement)"),
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
    assert_eq!(routine.get_config_clause(), "");
}

#[test]
fn get_config_clause_single_param() {
    let mut routine = build_function_routine();
    routine.config = vec!["search_path=public".to_string()];
    assert_eq!(routine.get_config_clause(), " SET search_path = 'public'");
}

#[test]
fn get_config_clause_multiple_params() {
    let mut routine = build_function_routine();
    routine.config = vec![
        "search_path=\"public, pg_temp\"".to_string(),
        "lock_timeout=5s".to_string(),
    ];
    assert_eq!(
        routine.get_config_clause(),
        " SET search_path = \"public, pg_temp\" SET lock_timeout = '5s'"
    );
}

#[test]
fn get_config_clause_value_with_single_quote() {
    let mut routine = build_function_routine();
    routine.config = vec!["search_path=it's_schema".to_string()];
    assert_eq!(
        routine.get_config_clause(),
        " SET search_path = 'it''s_schema'"
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
    assert_eq!(routine.get_config_clause(), " SET search_path = 'public'");
}

#[test]
fn get_config_clause_value_with_equals_sign() {
    // Values can contain '=' (e.g. an expression); only split on the first '='.
    let mut routine = build_function_routine();
    routine.config = vec!["extra_float_digits=3=yes".to_string()];
    assert_eq!(
        routine.get_config_clause(),
        " SET extra_float_digits = '3=yes'"
    );
}

#[test]
fn get_config_clause_empty_value() {
    let mut routine = build_function_routine();
    routine.config = vec!["search_path=".to_string()];
    assert_eq!(routine.get_config_clause(), " SET search_path = ''");
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
        script.contains("SET search_path = 'public, pg_temp' SET statement_timeout = '30s'"),
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
            "SECURITY DEFINER SET search_path = 'public, pg_temp' SET lock_timeout = '5s' as $$"
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
        script.contains("language sql SET search_path = 'public' as $$"),
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
