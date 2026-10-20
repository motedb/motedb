<!-- Thanks! Keep the PR focused: one behaviour change per PR. -->

## What

<!-- One or two sentences: what changes and why. Link the issue / discussion / design note. -->

## How

<!-- The approach, and anything a reviewer should look at first (a tricky invariant, a lock order, a format change). -->

## Checklist

- [ ] Tests: new behaviour has a test; a bug fix has a regression test that failed before.
- [ ] `cargo fmt --check` and `cargo test --lib --all-features` pass locally.
- [ ] Touched suites pass: `cargo test --test <suite>` (and `CI=1 cargo test --workspace` if the change is broad).
- [ ] Storage format / WAL / recovery touched → migration test added, `docs/design/2026-10-file-layout.md` rules respected, crash tests run.
- [ ] SQL surface changed → `README.md` *SQL Support* and `docs/` updated; fuzz template / differential test extended where applicable.
- [ ] Engine capability added → wired through FFI + Python + Node (+ Go) or the gap is called out below.
- [ ] New dependency → passes `cargo deny check`, feature-gated if not needed by edge builds.
- [ ] `CHANGELOG.md` entry under the upcoming version.
- [ ] No `println!`/`eprintln!` in library code (use the `log` facade macros).

## Performance

<!-- Delete if not relevant. Otherwise: `make compete-quick` JSON attached, or "perf gate unchanged". No ad-hoc numbers. -->

## Not in this PR

<!-- Follow-ups you deliberately left out, so reviewers do not ask for them. -->
