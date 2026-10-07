# MoteDB Technical Report

`motedb.tex` — the arXiv-style technical report
(*MoteDB: An Embedded Multimodal Storage Engine for Embodied AI*).

## Build

```bash
pdflatex motedb.tex && pdflatex motedb.tex   # two passes for refs
# or: tectonic motedb.tex
```

## Submitting to arXiv

1. Register at arxiv.org (approval for a new submitter takes a few days —
   do this first).
2. Category: **cs.DB** (primary); cross-list **cs.AR** and **cs.RO**.
3. arXiv wants the *source*: upload `motedb.tex` (+ the compiled PDF for
   review). No figures yet — the two numbered tables are text-only, which
   keeps the submission one-file simple. If you add figures later, upload
   them alongside the .tex in a zip.
4. License: pick arXiv's non-exclusive license; the repo stays MIT.
5. After it's live, the arXiv ID becomes a citable DOI — add it to the
   README badges and to every pitch/outreach email.

## Updating

Numbers in §7 come from the public suites; regenerate with `make compete`
and refresh the tables when you cut a release with performance changes.
The "where others win" paragraph is a standing requirement: a benchmark
section without published losses is marketing, not measurement.
