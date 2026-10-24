# Governance

MoteDB is an open-source project maintained by **one person**. This document
explains **how decisions get made**, so you know where your input goes and who
is accountable for the outcome.

Being a single-maintainer project is a constraint, not a secret. It means
turnaround is fast and nobody can quietly outvote you — and it also means the
project needs your scrutiny more than a large committee would. Everything below
is written to make that explicit.

---

## Governance model

**The maintainer decides; the community participates.** Concretely:

| Decision | Who decides | How the community participates |
|---|---|---|
| Roadmap priorities | Maintainer | Discussion votes + issue proposals |
| Bug fix ordering | Maintainer | The maintainer explains priority in the issue thread |
| API design | Maintainer | Open a Discussion first; breaking changes are announced at least 2 weeks ahead |
| Docs & examples | Anyone can PR directly | — |
| Day-to-day code review | Maintainer (+ committers, once appointed) | — |
| Project direction (annual) | Maintainer | Community newsletter, annual AMA |

---

## How decisions are made

### Default: consensus

Most decisions do not need a vote — we try to reach agreement in discussion.

**How to participate:** state your position in an issue or discussion → the
maintainer integrates it → if there is no substantial objection, it ships.

### Changes that require extra care

The following need the maintainer's explicit, written agreement **and** a
public comment window of at least 2 weeks:

- Removing or renaming a public API
- Introducing a breaking change (SQL semantics, file format, configuration keys)
- Changing the minimum supported version (Rust version, platform support matrix)
- Changing the license
- Changing the project's positioning or target audience

Because there is only one maintainer, there is no second approver to
formally countersign. Instead of pretending otherwise, the maintainer
**records the decision, the reasoning, and the fact that no second reviewer
existed** in a tracking issue that stays open and visible. If a suitable
maintainer or committer exists at the time, their agreement is sought and
recorded in the same place.

### Disputes

When consensus is not reachable, the maintainer makes the final call and
**states the reasoning**. Anyone who disagrees may ask for the decision to be
recorded in an issue so it stays visible long-term — **we do not delete
recorded decisions.**

---

## Roles

### Contributor

Anyone who files an issue, opens a pull request, or contributes docs or code.
No application, no ceremony.

### Committer

Someone with write access to the repository who can merge PRs and close
issues.

**How the role is granted** (any one of these is enough):

- Sustained contribution over at least 3 months, with ≥ 10 merged PRs
- ≥ 5 high-quality PRs in one critical subsystem (vector index, full-text
  search, the Python or Node binding), plus a willingness to maintain that
  subsystem long-term
- A clear recommendation from an existing committer, approved by the
  maintainer

**What it grants:** independent review and merge rights within your own
subsystem, a voice in technical discussion, and a vote in release decisions.

> The first committers will be chosen from the earliest contributors. The
> maintainer will reach out to people who look like good candidates.

### Champion

Not a maintainer — a core contributor on the community side. Someone who is
active in Discussions, writes Show & Tell posts, produces content in Chinese
or English, organises events, and helps newcomers.

**What it grants:** a public badge of recognition, early access to new
versions and features, and a role in shaping community activities.

---

## Public commitments

### The roadmap is public

Major feature directions are written into [`ROADMAP.md`](./ROADMAP.md) with a
status: `in progress` / `planned` / `done` / `not planned`.

**"Not planned" is public information too** — it comes with a reason, so the
community doesn't have to ask the same question twice.

### Breaking changes are announced in advance

Breaking changes are announced in Discussions **at least 2 weeks** before they
land, including:

- What changes and why
- How to migrate, with runnable code
- The tracking issue and the target version

> Example: v0.12 changed the default semantics of multi-word `MATCH` from OR to
> AND, to match SQLite FTS5. It was announced with a migration recipe — **this
> is what the announcement mechanism is supposed to look like.**

### Attribution

Every merged PR is credited in the CHANGELOG and in the contributor list on
the README. Every accepted issue proposal is publicly acknowledged.
**This is a fixed part of the process, not an optional courtesy.**

---

## Handling disputes

1. Discuss it rationally in the original issue or discussion
2. If the discussion stalls, @ the maintainer
3. If it concerns conduct, follow the reporting process in
   [`CODE_OF_CONDUCT.md`](./CODE_OF_CONDUCT.md)
4. If it is still unresolved: the maintainer makes the final call and records
   the reasoning

---

## Contact

Governance questions can go straight to a Discussion tagged `governance`, or
to the maintainer ([@meluo](https://github.com/meluo)).

---

*This document changes as the community matures. Anyone can propose a change
to it — open a pull request.*
