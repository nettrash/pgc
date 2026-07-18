use super::canonicalize_definition;

// Issue #226: PostgreSQL's IN-list deparsing is not idempotent. The array-level cast
// (first render) and the element-level cast (re-parsed fixed point) must canonicalize
// to the same string so pgc stops re-emitting DROP+CREATE.
#[test]
fn in_list_array_and_element_cast_forms_are_equal() {
    let array_level = "((label)::text = ANY ((ARRAY['FOO'::character varying, 'BAR'::character varying])::text[]))";
    let element_level = "((label)::text = ANY (ARRAY[('FOO'::character varying)::text, ('BAR'::character varying)::text]))";
    assert_eq!(
        canonicalize_definition(array_level),
        canonicalize_definition(element_level)
    );
}

#[test]
fn element_level_form_is_a_fixed_point() {
    // Canonicalizing the fixed-point form again yields the same string.
    let element_level = "((label)::text = ANY (ARRAY[('FOO'::character varying)::text, ('BAR'::character varying)::text]))";
    let once = canonicalize_definition(element_level);
    assert_eq!(once, canonicalize_definition(&once));
}

#[test]
fn full_matview_where_clause_forms_are_equal() {
    let round1 = " SELECT id,\n    label\n   FROM t.item\n  WHERE ((label)::text = ANY ((ARRAY['FOO'::character varying, 'BAR'::character varying])::text[]));";
    let round2 = " SELECT id,\n    label\n   FROM t.item\n  WHERE ((label)::text = ANY (ARRAY[('FOO'::character varying)::text, ('BAR'::character varying)::text]));";
    assert_eq!(
        canonicalize_definition(round1),
        canonicalize_definition(round2)
    );
}

#[test]
fn partial_index_predicate_forms_are_equal() {
    let round1 = "CREATE INDEX ix ON t.log USING btree (id) WHERE ((action_type)::text = ANY ((ARRAY['FOO'::character varying, 'BAR'::character varying])::text[]))";
    let round2 = "CREATE INDEX ix ON t.log USING btree (id) WHERE ((action_type)::text = ANY (ARRAY[('FOO'::character varying)::text, ('BAR'::character varying)::text]))";
    assert_eq!(
        canonicalize_definition(round1),
        canonicalize_definition(round2)
    );
}

// The paren-free CHECK-constraint rendering (already handled by the prior
// normalize_definition) must keep collapsing to the same simplified form.
#[test]
fn check_constraint_paren_free_forms_are_equal() {
    let array_level = "CHECK (priority::text = ANY (ARRAY['P1'::character varying, 'P2'::character varying]::text[]))";
    let element_level = "CHECK (priority::text = ANY (ARRAY['P1'::character varying::text, 'P2'::character varying::text]))";
    let expected =
        "check (priority::text = any (array['P1'::character varying, 'P2'::character varying]))";
    assert_eq!(canonicalize_definition(array_level), expected);
    assert_eq!(canonicalize_definition(element_level), expected);
}

#[test]
fn string_literal_content_is_never_rewritten() {
    // A literal that spells out the cast pattern must survive untouched (case too).
    let def = "CHECK (note = '(ARRAY[X])::text[]')";
    assert_eq!(
        canonicalize_definition(def),
        "check (note = '(ARRAY[X])::text[]')"
    );
}

#[test]
fn doubled_single_quote_escape_is_respected() {
    let def = "CHECK (label = 'O''Brien')";
    assert_eq!(canonicalize_definition(def), "check (label = 'O''Brien')");
}

// A view/index may reference a case-sensitive quoted identifier; folding its case
// would make two different columns compare equal and hide a real diff.
#[test]
fn double_quoted_identifiers_preserve_case() {
    let a = "SELECT \"MyCol\" FROM t";
    let b = "SELECT \"mycol\" FROM t";
    assert_ne!(canonicalize_definition(a), canonicalize_definition(b));
    assert!(canonicalize_definition(a).contains("\"MyCol\""));
}

#[test]
fn expression_without_array_cast_only_lowercases() {
    assert_eq!(
        canonicalize_definition("CHECK (age > 0)"),
        "check (age > 0)"
    );
}

#[test]
fn integer_array_cast_is_distributed() {
    assert_eq!(
        canonicalize_definition("(ARRAY[1, 2, 3])::integer[]"),
        "array[(1)::integer, (2)::integer, (3)::integer]"
    );
}

#[test]
fn empty_input_is_empty() {
    assert_eq!(canonicalize_definition(""), "");
}

// Leading/trailing whitespace must not affect the canonical form, so a caller that
// trims (View::get_alter_script) and one that does not (View::hash) agree, and a
// catalog rendering that leaves surrounding whitespace never causes spurious churn.
#[test]
fn surrounding_whitespace_is_ignored() {
    assert_eq!(
        canonicalize_definition(" SELECT a FROM t "),
        canonicalize_definition("SELECT a FROM t")
    );
    assert_eq!(
        canonicalize_definition("select x\n\n"),
        canonicalize_definition("select x")
    );
    assert_eq!(canonicalize_definition("   "), "");
}

// The cast collapse only applies inside an `array[...]` literal. An array subscript or
// slice cast such as `col[1:2]::text[]` must keep its `::text[]`: dropping it changes
// the type and would make two non-equivalent definitions compare equal (missed diff).
#[test]
fn array_subscript_slice_cast_is_preserved() {
    assert_eq!(
        canonicalize_definition("col[1:2]::text[]"),
        "col[1:2]::text[]"
    );
    assert_eq!(canonicalize_definition("col[1]::text[]"), "col[1]::text[]");
    assert_eq!(
        canonicalize_definition("(arr[1])[1:2]::text[]"),
        "(arr[1])[1:2]::text[]"
    );
}

// A standalone `x::character varying::text` double cast (varchar then text) is not
// inside an array literal and must be preserved rather than collapsed to varchar.
#[test]
fn standalone_double_cast_is_preserved() {
    assert_eq!(
        canonicalize_definition("x::character varying::text"),
        "x::character varying::text"
    );
}

// A subscript cast that is genuinely different from an uncast subscript must still
// produce a diff — the collapse must not fuse them.
#[test]
fn subscript_cast_and_uncast_differ() {
    assert_ne!(
        canonicalize_definition("GENERATED ALWAYS AS (arr[1:2]::text[]) STORED"),
        canonicalize_definition("GENERATED ALWAYS AS (arr[1:2]) STORED")
    );
}

// An array literal that contains a genuine `::character varying::text` inside a string
// value must not have that literal content rewritten.
#[test]
fn array_literal_string_content_with_cast_text_is_preserved() {
    let def = "ARRAY['a::character varying::text'::character varying]::text[]";
    assert_eq!(
        canonicalize_definition(def),
        "array['a::character varying::text'::character varying]"
    );
}

// PostgreSQL's deparser emits only standard single-quoted literals, but if an E-string
// ever reaches the canonicalizer its body — including a backslash-escaped quote and
// upper-case content — must be preserved verbatim, not mis-scanned.
#[test]
fn e_string_body_is_preserved() {
    let def = "WHERE x = E'A\\'B' AND y > 0";
    let out = canonicalize_definition(def);
    assert!(
        out.contains("E'A\\'B'"),
        "E-string body must survive verbatim: {out}"
    );
    // text outside the E-string is still lowercased
    assert!(out.starts_with("where x = "));
    assert!(out.contains("and y > 0"));
}

#[test]
fn e_string_escaped_quote_does_not_end_literal() {
    // The `\'` is escaped, so the whole `E'...'` is one literal and the trailing
    // uppercase stays inside it (would be lowercased if the scan ended early).
    let def = "E'A\\'BAR'";
    assert_eq!(canonicalize_definition(def), "E'A\\'BAR'");
}

// A lowercase-e E-string with `''` doubling.
#[test]
fn lowercase_e_string_with_doubled_quote_is_preserved() {
    assert_eq!(canonicalize_definition("e'O''BRIEN'"), "e'O''BRIEN'");
}

// `some_e'x'` is an identifier ending in `e` followed by a literal, not an E-string;
// the identifier is still lowercased and the literal preserved.
#[test]
fn trailing_e_is_not_an_e_string_prefix() {
    assert_eq!(canonicalize_definition("SOMEE'X'"), "somee'X'");
}

// Dollar-quoted strings must be preserved verbatim, including their upper-case content.
#[test]
fn dollar_quoted_string_is_preserved() {
    assert_eq!(
        canonicalize_definition("x = $$RAW '\\ Content$$"),
        "x = $$RAW '\\ Content$$"
    );
}

#[test]
fn tagged_dollar_quoted_string_is_preserved() {
    assert_eq!(
        canonicalize_definition("$tag$Body ']::text[]' Here$tag$"),
        "$tag$Body ']::text[]' Here$tag$"
    );
}

// A stray `$` that is not a dollar-quote (no closing tag) must be treated as an
// ordinary character, not swallow the rest of the string.
#[test]
fn stray_dollar_is_not_a_dollar_quote() {
    assert_eq!(canonicalize_definition("COL$1 > 0"), "col$1 > 0");
}

// Only the redundant `::text[]` cast on a varchar array literal is dropped. A real
// array-type cast must be preserved, or distinct expressions would compare equal
// (missed diff). `integer[]` and `bigint[]` must not collapse to the same key.
#[test]
fn non_text_array_cast_is_preserved() {
    assert_eq!(
        canonicalize_definition("array[1]::integer[]"),
        "array[1]::integer[]"
    );
    assert_eq!(
        canonicalize_definition("array[1]::bigint[]"),
        "array[1]::bigint[]"
    );
    assert_ne!(
        canonicalize_definition("array[1]::integer[]"),
        canonicalize_definition("array[1]::bigint[]")
    );
}

// A `::text[]` cast on a non-varchar array literal is a real conversion, not the
// redundant IN-list form, so it is preserved and stays distinct from the uncast array.
#[test]
fn text_cast_on_non_varchar_array_is_preserved() {
    assert_eq!(
        canonicalize_definition("array[1]::text[]"),
        "array[1]::text[]"
    );
    assert_ne!(
        canonicalize_definition("array[1]::text[]"),
        canonicalize_definition("array[1]")
    );
}

// The redundant form (varchar elements + `::text[]`) still collapses — the fix must
// not disable the #226 canonicalization it exists for.
#[test]
fn redundant_varchar_text_array_cast_still_collapses() {
    assert_eq!(
        canonicalize_definition(
            "array['FOO'::character varying, 'BAR'::character varying]::text[]"
        ),
        "array['FOO'::character varying, 'BAR'::character varying]"
    );
}
