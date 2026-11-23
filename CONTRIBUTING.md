# Contributing to pgc

Thank you for helping. pgc writes SQL that people run against their production databases, so a
small mistake in its output can cost someone data. These rules keep the output safe and the
repository readable for the next person, and **every pull request is checked against them** (the
checklist at the end). A pull request that breaks one is not rejected for it. It is asked to change,
so the earlier you read this, the shorter the review.

## 1. Start from an issue

- **Open or pick an issue first.** For anything bigger than a small fix, agree on the approach in the
  issue before writing code. That matters most for new dependencies, CI changes, new flags or config
  keys, and anything that changes what the generated SQL does.
- **One issue, one pull request.** A refactor, a reformat or a bug you found along the way gets its
  own issue.
- **Branch name:** `<issue number>-<short-slug>`, for example
  `244-passwords-containing-url-reserved-characters`.
- **Target the open release branch** (`v1.x`, currently `v1.1`), never `master`. `master` only
  receives a finished release, from the maintainer.

## 2. Keep the pull request to its issue

A pull request contains the code, tests, fixtures and documentation the issue needs. It does **not**
contain:

- **A version bump** in `app/Cargo.toml`. Versions are set at release time.
- **A `CHANGELOG` entry.** The release notes are written for the whole release, in the file's fixed
  format, when it is cut.
- **Unrelated changes:** reformatting files you did not otherwise touch, renames, or moving code.

`README.md` is the reference for every command, flag and config key. A new flag or key, or a changed
default, is documented there in the same pull request, and a new config key is also added to the
sample `data/pgc.conf`.

## 3. Repository layout

The layout is fixed:

```
app/        the Cargo project: src/, tests/, Cargo.toml, Cargo.lock
data/       the sample pgc.conf, and data/test/ with the round-trip fixtures
packaging/  nfpm.yaml for the release packages
.github/    workflows/ and the catalog checks in scripts/
CHANGELOG  CODEOWNERS  CONTRIBUTING.md  Dockerfile  LICENSE  README.md
```

- **No new top-level directories or files**, including output folders.
- **Every `cargo` command runs from `app/`.** There is no manifest at the root.
- **One module per object kind.** A new kind of database object gets its own file in `app/src/dump/`
  (like `table.rs`, `view.rs`, `routine.rs`), following the `hash` / `get_script` /
  `get_drop_script` / `get_alter_script` shape documented in `app/src/dump/mod.rs`.
- **`.gitignore` changes need a reason.** Local files such as `data/test.conf`, `data/database.sql`,
  dumps and `output.sql` are already ignored. Use them, or a temporary directory outside the
  repository.

## 4. The migration contract

This is what pgc promises its users. A change to dumping or comparing keeps every part of it.

- **Round trip.** For the fixtures in `data/test/`, CI loads `schema_a.sql` and `schema_b.sql`,
  compares them, applies the migration to `a` and compares again. **The second migration must be
  empty**, on every supported PostgreSQL major (14 to 19).
- **A new case comes with its fixture.** A new object kind or diff case is added to `schema_a.sql`
  and `schema_b.sql`, listed in `data/test/README.md`, and checked by a
  `.github/scripts/assert-*.sh` script. The round trip cannot see a property pgc loses on both sides,
  so the script compares `a`'s catalogs with `b`'s directly. CI runs every `assert-*.sh` it finds.
- **Old dumps keep working.** Dumps are read back by later versions, so a new field on `Dump` uses
  `#[serde(default, skip_serializing_if = ...)]`, like the fields added before it.
- **DDL follows the database it runs on.** The migration is applied to the FROM database, so syntax
  that only a newer server accepts is gated on the FROM dump's version
  (`Comparer::migrated_server_at_least`), never on TO's, with a fallback every supported version
  accepts.
- **Default output doesn't change by accident.** With `--output-for-production` off, the output must
  stay byte-for-byte what it was, except where the issue says it was wrong.
- **Say when the SQL gets more destructive.** If a change makes the migration drop, recreate or
  rewrite something it did not before (a `DROP ... CASCADE`, a table rebuild, a lost grant), the
  pull request description says so and why.

## 5. Safe SQL

pgc puts names read from a database into SQL and into comments. A name can contain a quote, a line
break or a comment delimiter, so:

- **Every identifier goes through `quote_identifier`** (or `quote_identifier_for_server`) in
  `app/src/utils/string_extensions.rs`. Never paste a catalog name straight into a `format!`.
- **Every value written into a comment goes through `line_comment_safe` or `block_comment_safe`.**
  Without that, a name can end the comment and run as SQL when the migration is applied.
- **SQL rewriting understands SQL.** Code that edits generated statements (see
  `comparer/production.rs`) skips string literals, quoted identifiers, comments and dollar-quoted
  bodies. A plain text search-and-replace isn't accepted.

## 6. Tests

| Kind | Where | Run (from `app/`) |
|---|---|---|
| Unit tests | `app/src/<module>/tests/<source-file>.rs` | `cargo test --lib` |
| Integration tests | `app/tests/*.rs`, public API only | `cargo test` |
| Live database tests | `app/tests/live_database.rs`, `#[ignore]` | `cargo test -- --ignored` with the `PG*` variables set |
| Round trip | `data/test/`, run by CI on PostgreSQL 14 to 19 | see `data/test/README.md` |

- **A test that needs a private item is a unit test.** Don't make an item `pub` to test it. Each
  source file has one test file, attached with a `#[path = "tests/<file>.rs"]` block like the
  existing ones.
- **Tests read only files tracked in git.** A test that reads `data/test.conf` or another local file
  passes on your machine and fails everywhere else. Build the fixture in a scratch directory instead.
- **A bug fix comes with a test that fails without the fix.**
- **Check a clean checkout before pushing.** `git archive HEAD | tar -x -C <tmp>` gives exactly
  what CI sees.

## 7. Documentation

- **Use `///` on anything public, and `//!` for module headers.** A plain `//` is invisible to
  rustdoc.
- **Link only to items rustdoc can resolve.** CI builds the docs with
  `cargo doc --no-deps --lib --document-private-items` and `RUSTDOCFLAGS="-D warnings"`, so a broken
  intra-doc link fails the build. Write a private item in plain backticks.
- **Document what a reader can't guess:** catalog jargon (`typtype`, `coninhcount`, ...) and anything
  carrying an invariant. There is deliberately no `missing_docs` lint.

## 8. No evidence files in git

Test output, generated migrations, dumps, logs and benchmark results that show a change working go
in the **pull request description** or a CI artifact. They never go in a commit, because everything
committed stays in the history and in every clone, permanently.

## 9. Dependencies and toolchain

- **Every new dependency is named and justified in the issue first.** That covers a crate or a
  GitHub Action.
- **`app/Cargo.lock` is tracked.** It changes only for a dependency the issue agreed on. No blanket
  `cargo update` in a feature pull request.
- **The toolchain is fixed:** edition 2024, and the code builds on **Rust 1.88.0** (the oldest
  supported version) as well as on 1.95.0 and 1.100.0. Don't use language or library features
  newer than 1.88.
- **The `Dockerfile`'s Rust version matches the top of the CI matrix.** Changing either is a CI
  change (section 10).

## 10. CI

- **CI changes are their own pull request**, agreed in an issue first. A feature adds its tests to
  the suites CI already runs (section 6) and its catalog checks to `.github/scripts/`, not a new job.
- **Edits to `.github/workflows/` follow the file's existing style:**
  - actions pinned by major tag (`actions/checkout@v4`);
  - a comment explains why each job and step exists.
- **CI runs on `push` and `pull_request`.** A first-time contributor's run waits for the maintainer
  to approve it.
- **Copilot reviews every pull request.** Answer each comment: fix it, or explain why it is wrong.

## 11. Privacy and security

- **Passwords and connection strings never appear** in output, logs or error messages. That includes
  passwords inside a `postgres://` URL.
- **No secrets, and nothing real, in the repository:** no real hosts, users or passwords in tests,
  fixtures, docs or sample configs, and no dump of a real database. A dump holds a database's whole
  schema and its routine source code.
- **Report a security problem privately**, through the repository's Security tab ("Report a
  vulnerability"), never in a public issue or pull request.

## 12. Quality bar

- **Zero warnings**, test targets included.
- `cargo fmt --all -- --check` and `cargo clippy --all-targets --all-features` are clean on
  1.88.0, 1.95.0 and 1.100.0, and `cargo doc` builds as in section 7.
- **Every existing test still passes.** A new test that is skipped or ignored by default is called out
  in the pull request.
- **Commits reference the issue** (`#244 passwords with reserved characters`) and contain no build
  output, no editor files and no absolute paths from your machine.

---

## The checklist every pull request is reviewed against

- [ ] It has an issue, its branch is `<issue>-<slug>`, and it targets the open `v1.x` branch (§1).
- [ ] It only contains that issue's change: no version bump, `CHANGELOG` entry or unrelated edits,
      and any new flag or config key is in `README.md` and `data/pgc.conf` (§2).
- [ ] It adds no new top-level files or directories, and `.gitignore` is unchanged or explained (§3).
- [ ] The round trip still ends with an empty migration, and a new case has its fixture, its
      `data/test/README.md` entry and its `assert-*.sh` check (§4).
- [ ] Old dumps still load, newer DDL is gated on the FROM version, and any new destructive
      behaviour is explained in the description (§4).
- [ ] Identifiers are quoted and comment text is made safe with the existing helpers (§5).
- [ ] Its tests are in the existing suites, include one that fails without the fix, and read only
      tracked files (§6).
- [ ] Public items are documented with `///`, and `cargo doc` builds with `-D warnings` (§7).
- [ ] It commits no dumps, generated SQL, logs or reports (§8).
- [ ] Every new dependency was agreed in the issue, `Cargo.lock` changes only for it, and the code
      builds on Rust 1.88.0 (§9).
- [ ] It doesn't touch `.github/workflows/`, or that change is its own agreed pull request (§10).
- [ ] It leaks no password or connection string and adds nothing real or secret (§11).
- [ ] It has zero warnings, and fmt, clippy and doc are clean (§12).
