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

// The paren-free CHECK-constraint renderings must converge — on the element-level
// fixed point, which keeps the `::text` markers so type information is never lost.
#[test]
fn check_constraint_paren_free_forms_are_equal() {
    let array_level = "CHECK (priority::text = ANY (ARRAY['P1'::character varying, 'P2'::character varying]::text[]))";
    let element_level = "CHECK (priority::text = ANY (ARRAY['P1'::character varying::text, 'P2'::character varying::text]))";
    let expected = "check (priority::text = any (array[('P1'::character varying)::text, ('P2'::character varying)::text]))";
    assert_eq!(canonicalize_definition(array_level), expected);
    assert_eq!(canonicalize_definition(element_level), expected);
}

// All four renderings of the same varchar IN-list — parenthesized or paren-free,
// array-level or element-level cast — converge on one canonical key, so even a
// FROM dump and a TO dump rendered by different server behaviors compare equal.
#[test]
fn all_four_in_list_renderings_converge() {
    let forms = [
        "(ARRAY['v'::character varying, 'w'::character varying])::text[]",
        "ARRAY[('v'::character varying)::text, ('w'::character varying)::text]",
        "ARRAY['v'::character varying, 'w'::character varying]::text[]",
        "ARRAY['v'::character varying::text, 'w'::character varying::text]",
    ];
    let keys: Vec<String> = forms.iter().map(|f| canonicalize_definition(f)).collect();
    assert!(
        keys.windows(2).all(|w| w[0] == w[1]),
        "all renderings must share one canonical key: {keys:?}"
    );
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
        "array[('a::character varying::text'::character varying)::text]"
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

// The redundant form (varchar elements + `::text[]`) still canonicalizes — to the
// element-level fixed point — so the #226 loop stays fixed.
#[test]
fn redundant_varchar_text_array_cast_still_collapses() {
    assert_eq!(
        canonicalize_definition(
            "array['FOO'::character varying, 'BAR'::character varying]::text[]"
        ),
        "array[('FOO'::character varying)::text, ('BAR'::character varying)::text]"
    );
}

// The reviewer's conflation cases: a `text[]` rendering (in either paren-free form)
// must never share a canonical key with the plain `character varying[]` literal —
// they are different types, and fusing them would hide a real change.
#[test]
fn text_array_renderings_stay_distinct_from_plain_varchar_array() {
    let plain_varchar = canonicalize_definition("array['a'::character varying]");
    let element_cast = canonicalize_definition("array['a'::character varying::text]");
    let array_cast = canonicalize_definition("array['a'::character varying]::text[]");
    assert_ne!(element_cast, plain_varchar);
    assert_ne!(array_cast, plain_varchar);
    // ...while the two text[] renderings agree with each other.
    assert_eq!(element_cast, array_cast);
}

// An array literal with varchar::text element casts on only SOME elements is not an
// IN-list rendering; it must be left byte-for-byte untouched.
#[test]
fn mixed_element_casts_are_left_untouched() {
    let def = "array['a'::character varying::text, 'b'::character varying]";
    assert_eq!(
        canonicalize_definition(def),
        "array['a'::character varying::text, 'b'::character varying]"
    );
}

// A user-written `IN ('A'::varchar(10), …)` keeps its typmod through both pretty
// pg_get_constraintdef renderings and flips between them just like the bare form
// (verified live on PostgreSQL 16), so the typmod'd renderings must converge too or
// the constraint loops with DROP+ADD forever. Exact strings from the live server.
#[test]
fn typmod_varchar_in_list_renderings_converge() {
    let round1 = "CHECK (code::text = ANY (ARRAY['A'::character varying(10), 'B'::character varying(10)]::text[]))";
    let round2 = "CHECK (code::text = ANY (ARRAY['A'::character varying(10)::text, 'B'::character varying(10)::text]))";
    assert_eq!(
        canonicalize_definition(round1),
        canonicalize_definition(round2)
    );
    assert_eq!(
        canonicalize_definition(round1),
        "check (code::text = any (array[('A'::character varying(10))::text, ('B'::character varying(10))::text]))"
    );
}

// The typmod'd fixed point stays distinct from the bare-varchar fixed point: the two
// spellings render consistently on both sides of a compare, so folding them is
// unnecessary and keeping them apart is the safe direction.
#[test]
fn typmod_and_bare_varchar_fixed_points_stay_distinct() {
    assert_ne!(
        canonicalize_definition("array['A'::character varying(10)]::text[]"),
        canonicalize_definition("array['A'::character varying]::text[]")
    );
}

// A domain-typed element spelling (e.g. `::order_status`) is intentionally NOT gated
// in: PostgreSQL types IN-list literals as plain varchar even for a domain-over-varchar
// column (verified live), so this spelling never participates in the #226 flip; an
// expression carrying it renders identically on both sides and passes through
// untouched.
#[test]
fn domain_typed_elements_are_left_untouched() {
    let def = "array['new'::order_status, 'done'::order_status]::text[]";
    assert_eq!(canonicalize_definition(def), def);
}

// PostgreSQL's deparser never escapes a quote as `\'` — quotes are always doubled —
// and under standard_conforming_strings = on a string ending in a backslash renders
// as `'x\'`, where the quote after the backslash IS the terminator (verified live on
// PostgreSQL 16). The scanner must close the literal there; treating `\'` as an
// escape would swallow everything after the string. These strings are exact
// deparser output.
#[test]
fn standard_string_ending_in_backslash_terminates_at_quote() {
    // literal preserved, expression around it still lowercased
    assert_eq!(
        canonicalize_definition("WHERE (s = 'x\\'::text)"),
        "where (s = 'x\\'::text)"
    );
    // the scan must NOT continue past the closing quote: the keyword after the
    // literal is outside it and gets lowercased
    assert_eq!(
        canonicalize_definition("'x\\' AND UPPER_KEyword"),
        "'x\\' and upper_keyword"
    );
}

// Deparser output for a string containing both an embedded quote and a backslash:
// scs=on renders `'a''\b'` (quote doubled, backslash bare); scs=off renders
// `'a''\\b'` (backslash doubled). Both must be preserved verbatim.
#[test]
fn deparser_quote_and_backslash_renderings_are_preserved() {
    assert_eq!(
        canonicalize_definition("WHERE (s = 'a''\\b'::text)"),
        "where (s = 'a''\\b'::text)"
    );
    assert_eq!(
        canonicalize_definition("WHERE (s = 'a''\\\\b'::text)"),
        "where (s = 'a''\\\\b'::text)"
    );
}

// The typmod recognizer must not mistake other parenthesized tails for a typmod.
#[test]
fn non_typmod_parenthesized_tails_are_not_varchar_casts() {
    // function call tail — not a varchar cast
    let def = "array[f(1), f(2)]::text[]";
    assert_eq!(canonicalize_definition(def), def);
    // typmod-looking tail on a non-varchar cast
    let def2 = "array['x'::numeric(10, 2)]::text[]";
    assert_eq!(canonicalize_definition(def2), def2);
}
