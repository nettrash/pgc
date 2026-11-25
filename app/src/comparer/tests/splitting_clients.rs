//! Tests for `wrap_for_splitting_clients`: which statements a client that
//! splits at every `;` needs wrapped, that statement boundaries follow psql,
//! and that the wrapped statement survives byte for byte.

use super::*;

/// `statement` as [`wrap_for_splitting_clients`] wraps it, with the default
/// tags, followed by the statement's `;`.
fn wrapped(statement: &str) -> String {
    format!(
        "DO $pgc_client$\nBEGIN\n    EXECUTE $pgc_sql$\n{statement}\n$pgc_sql$;\nEND\n$pgc_client$;"
    )
}

/// `statement`, which holds a carriage return, as [`wrapped`] writes it:
/// without the CR, cut at each one, for `EXECUTE` to join with `chr(13)`.
fn wrapped_with_cr(statement: &str) -> String {
    let pieces = statement.replace('\r', "$pgc_sql$, $pgc_sql$");
    format!(
        "DO $pgc_client$\nBEGIN\n    EXECUTE pg_catalog.array_to_string(ARRAY[$pgc_sql$\n{pieces}\n$pgc_sql$], pg_catalog.chr(13));\nEND\n$pgc_client$;"
    )
}

/// Undo the wrapping: every block with the default tags becomes its statement
/// and `;` again, the pieces of one with a carriage return joined with a CR.
/// Wrapping must be exactly reversible, so this is asserted to give back the
/// input in every test that wraps.
fn unwrapped(script: &str) -> String {
    let open = "DO $pgc_client$\nBEGIN\n    EXECUTE ";
    let close = ";\nEND\n$pgc_client$;";
    let mut out = String::new();
    let mut rest = script;
    while let Some(at) = rest.find(open) {
        out.push_str(&rest[..at]);
        let body = &rest[at + open.len()..];
        let end = body.find(close).expect("an open block is closed");
        let text = &body[..end];
        let statement = match text
            .strip_prefix("pg_catalog.array_to_string(ARRAY[")
            .and_then(|t| t.strip_suffix("], pg_catalog.chr(13))"))
        {
            Some(pieces) => pieces.replace("$pgc_sql$, $pgc_sql$", "\r"),
            None => text.to_string(),
        };
        let statement = statement
            .strip_prefix("$pgc_sql$\n")
            .and_then(|t| t.strip_suffix("\n$pgc_sql$"))
            .expect("the statement is one dollar-quoted string");
        out.push_str(statement);
        out.push(';');
        rest = &body[end + close.len()..];
    }
    out.push_str(rest);
    out
}

/// Wrap `script`, check the result undoes to `script`, and return it.
fn wrap(script: &str) -> String {
    let out = wrap_for_splitting_clients(script);
    assert_eq!(
        unwrapped(&out),
        script,
        "wrapping must be reversible:\n{out}"
    );
    out
}

const ATOMIC_FN: &str = "create or replace function s.f(p integer) returns integer language sql IMMUTABLE BEGIN ATOMIC\n SELECT (p + 1);\nEND";

#[test]
fn a_begin_atomic_function_is_wrapped_whole() {
    let script = format!("{ATOMIC_FN};\n");
    assert_eq!(wrap(&script), format!("{}\n", wrapped(ATOMIC_FN)));
}

#[test]
fn a_begin_atomic_procedure_with_several_statements_is_wrapped_whole() {
    let procedure = "create or replace procedure s.p(IN v integer) language sql BEGIN ATOMIC\n \
                     INSERT INTO s.t (id) VALUES (p.v);\n DELETE FROM s.t\n   WHERE (t.id < 0);\nEND";
    let script = format!("{procedure};\n\nalter procedure s.p(IN v integer) owner to app;\n");
    assert_eq!(
        wrap(&script),
        format!(
            "{}\n\nalter procedure s.p(IN v integer) owner to app;\n",
            wrapped(procedure)
        )
    );
}

#[test]
fn case_inside_begin_atomic_does_not_end_the_body() {
    // psql counts CASE ... END inside a BEGIN, so the END of the CASE does
    // not close the body and the `;` after it is not the statement's end.
    let function = "create function s.sign(x integer) returns text language sql BEGIN ATOMIC\n \
                    SELECT CASE WHEN (x < 0) THEN 'neg'::text ELSE 'pos'::text END AS \"case\";\nEND";
    let script = format!("{function};\nselect 1;\n");
    assert_eq!(wrap(&script), format!("{}\nselect 1;\n", wrapped(function)));
}

#[test]
fn begin_atomic_is_recognised_in_any_case_and_with_or_replace() {
    for head in [
        "CREATE FUNCTION",
        "create procedure",
        "Create Or Replace Function",
        "CREATE OR REPLACE PROCEDURE",
    ] {
        let statement = format!("{head} s.x() LANGUAGE sql BEGIN ATOMIC\n SELECT 1;\nEND");
        let script = format!("{statement};\n");
        assert_eq!(
            wrap(&script),
            format!("{}\n", wrapped(&statement)),
            "{head}"
        );
    }
}

#[test]
fn a_rule_with_several_actions_is_wrapped() {
    let rule = "CREATE RULE r AS\n    ON INSERT TO s.t DO ALSO ( INSERT INTO s.log (id)\n  VALUES (new.id);\n INSERT INTO s.log (id)\n  VALUES ((new.id + 1));\n)";
    let script = format!("{rule};\n");
    assert_eq!(wrap(&script), format!("{}\n", wrapped(rule)));
}

#[test]
fn a_positional_parameter_outside_quotes_is_wrapped() {
    // A SQL-standard body over unnamed arguments is deparsed with `$1`, which
    // such a client reads as a bind parameter and asks a value for.
    let function = "create function s.eq(integer, integer) returns boolean language sql IMMUTABLE RETURN ($1 = $2)";
    let script = format!("{function};\n");
    assert_eq!(wrap(&script), format!("{}\n", wrapped(function)));
}

#[test]
fn statements_a_splitting_client_sends_whole_are_left_alone() {
    let script = concat!(
        "create table s.t (id integer, note text default ';');\n\n",
        "comment on table s.t is 'a; b $1';\n\n",
        "comment on column s.t.note is E'it\\'s; $1';\n\n",
        "create table s.\"odd;name$1\" (id integer);\n\n",
        "create function s.g(integer) returns integer language plpgsql as $$\n",
        "BEGIN\n    RETURN $1;\nEND;\n$$;\n\n",
        "create function s.h() returns integer language sql as $body$ select 1; select 2 $body$;\n\n",
        "-- a comment; with $1\n",
        "/* a block; /* nested; */ comment $1 */\n",
        "select a$1 from s.t;\n",
        "begin;\n\nselect 1;\n\ncommit;\n",
    );
    assert_eq!(wrap_for_splitting_clients(script), script);
}

#[test]
fn a_transaction_begin_does_not_open_a_block() {
    // Only CREATE FUNCTION / PROCEDURE counts BEGIN, so a migration framed by
    // `begin;` / `commit;` keeps its statement boundaries.
    let script = format!("begin;\n\n{ATOMIC_FN};\n\ncommit;\n");
    assert_eq!(
        wrap(&script),
        format!("begin;\n\n{}\n\ncommit;\n", wrapped(ATOMIC_FN))
    );
}

#[test]
fn leading_comments_stay_outside_the_wrapper() {
    let script = format!("select 1;\n\n/* ---> Routines */\n-- a note\n{ATOMIC_FN};\n");
    assert_eq!(
        wrap(&script),
        format!(
            "select 1;\n\n/* ---> Routines */\n-- a note\n{}\n",
            wrapped(ATOMIC_FN)
        )
    );
}

#[test]
fn the_statement_survives_byte_for_byte() {
    // A line feed and `$$` inside a literal, non-ASCII text and a trailing space.
    let function = "create function s.lit() returns text language sql BEGIN ATOMIC\n \
                    SELECT ('line1\nline2 – ü'::text || '$$ not a delimiter $$'::text); \nEND";
    let script = format!("{function};\n");
    assert_eq!(wrap(&script), format!("{}\n", wrapped(function)));
}

#[test]
fn a_carriage_return_is_written_as_chr_13() {
    // An editor that gives the file one kind of line ending (VS Code does, on
    // opening it) would make this CR LF a LF, so the script carries no CR.
    let function = "create function s.lit() returns text language sql BEGIN ATOMIC\n \
                    SELECT ('line1\r\nline2'::text || '$$ not a delimiter $$'::text);\nEND";
    let script = format!("{function};\n");
    let out = wrap(&script);
    assert_eq!(
        out,
        "DO $pgc_client$\nBEGIN\n    EXECUTE pg_catalog.array_to_string(ARRAY[$pgc_sql$\n\
         create function s.lit() returns text language sql BEGIN ATOMIC\n \
         SELECT ('line1$pgc_sql$, $pgc_sql$\nline2'::text || '$$ not a delimiter $$'::text);\nEND\n\
         $pgc_sql$], pg_catalog.chr(13));\nEND\n$pgc_client$;\n"
    );
    assert!(!out.contains('\r'));
}

#[test]
fn a_statement_is_wrapped_for_a_carriage_return_alone() {
    // None of these needs wrapping for a client that splits at every `;`:
    // a plpgsql body written with CR LF line ends, a comment with a lone CR,
    // and a CR just before the statement's `;` (the CR LF that follows it is
    // between statements and stays).
    let function = "create function s.g() returns integer language plpgsql as $$\r\nBEGIN\r\n    RETURN 1;\r\nEND;\r\n$$";
    let comment = "comment on table s.t is 'a\rb'";
    let view = "create view s.v as select 1 as one\r";
    let script = format!("{function};\n\n{comment};\n{view};\r\nselect 1;\n");
    let out = wrap(&script);
    assert_eq!(
        out,
        format!(
            "{}\n\n{}\n{}\r\nselect 1;\n",
            wrapped_with_cr(function),
            wrapped_with_cr(comment),
            wrapped_with_cr(view)
        )
    );
    assert_eq!(out.matches('\r').count(), 1);
}

#[test]
fn a_tag_numbered_for_the_statement_also_cuts_it_at_a_carriage_return() {
    let function = "create function s.t() returns text language sql BEGIN ATOMIC\n \
                    SELECT '$pgc_sql$\r\n'::text;\nEND";
    let out = wrap_for_splitting_clients(&format!("{function};\n"));
    assert_eq!(
        out,
        "DO $pgc_client$\nBEGIN\n    EXECUTE pg_catalog.array_to_string(ARRAY[$pgc_sql1$\n\
         create function s.t() returns text language sql BEGIN ATOMIC\n \
         SELECT '$pgc_sql$$pgc_sql1$, $pgc_sql1$\n'::text;\nEND\n\
         $pgc_sql1$], pg_catalog.chr(13));\nEND\n$pgc_client$;\n"
    );
}

#[test]
fn a_tag_the_statement_contains_gets_a_number() {
    let function = "create function s.t() returns text language sql BEGIN ATOMIC\n \
                    SELECT '$pgc_sql$ and $pgc_client$ and $pgc_sql1$'::text;\nEND";
    let out = wrap_for_splitting_clients(&format!("{function};\n"));
    assert_eq!(
        out,
        format!(
            "DO $pgc_client1$\nBEGIN\n    EXECUTE $pgc_sql2$\n{function}\n$pgc_sql2$;\nEND\n$pgc_client1$;\n"
        )
    );
}

#[test]
fn an_e_string_quote_escape_does_not_end_the_literal() {
    // `\'` keeps the E-string open, so the `;` after it is inside the literal.
    let script = "comment on table s.t is E'a\\'; b';\nselect 1;\n";
    assert_eq!(wrap_for_splitting_clients(script), script);
    // Read as a standard string, the literal would end at `\'` and the next
    // quote would open a string swallowing the function after it.
    let script = format!("comment on table s.t is E'it\\'s';\n{ATOMIC_FN};\n");
    assert_eq!(
        wrap(&script),
        format!(
            "comment on table s.t is E'it\\'s';\n{}\n",
            wrapped(ATOMIC_FN)
        )
    );
    // ... while in a standard string the backslash is an ordinary character.
    let function =
        "create function s.b() returns text language sql BEGIN ATOMIC\n SELECT 'a\\'::text;\nEND";
    let script = format!("{function};\n");
    assert_eq!(wrap(&script), format!("{}\n", wrapped(function)));
}

#[test]
fn a_unicode_escape_string_is_skipped() {
    let script = "comment on table s.t is U&'d\\0061t; $1';\nselect 1;\n";
    assert_eq!(wrap_for_splitting_clients(script), script);
}

#[test]
fn several_statements_are_each_wrapped_on_their_own() {
    let rule =
        "CREATE RULE r AS ON UPDATE TO s.t DO INSTEAD ( UPDATE s.u SET v = new.v;\n NOTIFY t;\n)";
    let script = format!("{ATOMIC_FN};\n\ngrant select on s.t to app;\n\n{rule};\n");
    assert_eq!(
        wrap(&script),
        format!(
            "{}\n\ngrant select on s.t to app;\n\n{}\n",
            wrapped(ATOMIC_FN),
            wrapped(rule)
        )
    );
}

#[test]
fn an_unterminated_tail_is_copied_as_it_is() {
    let script = format!("select 1;\n{ATOMIC_FN}");
    assert_eq!(wrap_for_splitting_clients(&script), script);
}

#[test]
fn an_empty_script_stays_empty() {
    assert_eq!(wrap_for_splitting_clients(""), "");
    assert_eq!(
        wrap_for_splitting_clients("\n\n-- nothing\n"),
        "\n\n-- nothing\n"
    );
}
