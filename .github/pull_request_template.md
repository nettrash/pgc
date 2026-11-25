<!--
Thank you for contributing. Please read CONTRIBUTING.md first: this pull request is reviewed
against the checklist below. Tick each item, or say next to it why it does not apply.
-->

## Issue

Closes #

## What changes

<!-- What this pull request does, and how the generated SQL changes. Say so if it now drops, recreates
or rewrites anything it did not before (CONTRIBUTING.md §4). -->

## How it was tested

<!-- The suites you ran and how many tests executed, the PostgreSQL majors you tried, and whether the
second round-trip migration was empty. Link the CI run. Paste output here, not in a commit
(CONTRIBUTING.md §8). -->

## Checklist ([CONTRIBUTING.md](https://github.com/nettrash/pgc/blob/master/CONTRIBUTING.md))

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
