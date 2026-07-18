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
