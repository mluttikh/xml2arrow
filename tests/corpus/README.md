# The frozen corpus

Each directory here is one case: a configuration, a document, and a snapshot of
exactly what this crate produces for them.

```
<case>/
  config.yaml        the mapping
  input.xml          the document
  expected/
    <table>.txt      one snapshot per output table — or
    error.txt        the error's Display, for cases that must fail
```

The harness is `tests/frozen_corpus.rs`; read its module docs for the mechanics.

## What this is for

A configuration that does not opt into a new capability must keep producing
byte-identical output for the whole 0.x line. That promise is only worth
something if it is checked mechanically, on every commit, rather than by review —
this corpus is the check. Every case here is a configuration in today's format,
recorded against the behavior of 0.19.

Every case additionally runs through all three entry points (buffered,
zero-copy, and streaming with a batch size small enough to force flushes) and
requires them to agree. That guards the upcoming unification of the event pumps:
a refactor that changes one path and not the others fails here.

## The corpus deliberately freezes behavior we intend to change

Several snapshots record semantics that planned opt-in features replace. That is
the point: until a configuration opts in, these cases must still show the *old*
values. A release that changes one of them on purpose re-records it and says so.

| Case | Frozen behavior | Changed by |
|---|---|---|
| `metadata_multi_child_rows` | two half-filled rows, one per configured child element | declared `row:` |
| `repeated_container_resets_levels` | `<block>` = `0, 0, 1, 0` — the counter resets when the container re-opens, so a single-column join misattributes the last row | `parent:` links (global `UInt64` keys) |
| `structural_table_excluded` | a `fields: []` table is absent from the output but still feeds an index column | `index_of:` links |
| `utf8_missing_yields_empty_string` | a missing non-nullable `Utf8` yields `""` where every other type errors | per-field `on_missing` |
| `whitespace_without_trim` | `trim_text` governs strings, while numerics trim regardless | per-field `trim` |
| `empty_document_yields_empty_tables`, `stop_at_paths` | a zero-row table is returned empty when collecting, but never appears in a stream | — (asymmetry pinned, not yet scheduled) |
| `self_closing_table_element` | a field's value is **dropped** when a different table's scope is on top of the stack when it arrives (`outer.n` is null although `<n>1</n>` is present) | declared field ownership |
| `truncated_input` | a document cut off mid-element parses to `Ok` with the rows that closed before the cut — here one row instead of two | rejecting truncated input, first as an option, later as the default |
| `error_duplicate_value`, `error_invalid_number`, `error_missing_required_field` | value errors carry no document coordinates | error locations (row index and byte offset) |

## Adding a case

Create `<case>/config.yaml` and `<case>/input.xml`, then record it:

```sh
XML2ARROW_FREEZE=1 cargo test --test frozen_corpus
```

Keep documents small — the snapshot is meant to be read in a diff. Name the case
after the behavior it pins, not the feature it uses.

**Line endings are part of the fixture.** Expected errors carry byte offsets
into `input.xml`, so CRLF would shift every one of them by the number of
preceding lines. `.gitattributes` pins everything under `tests/corpus/` to LF on
every platform, and the harness normalizes on read as a second line of defence —
but an editor that rewrites a fixture to CRLF is still changing the test's input,
so keep them LF.

## When a case fails

Output changed. Read the diff and decide which it is:

- **a bug** — fix the code;
- **an intentional change** — during the 0.x line it must be reachable only
  through an opt-in, or be a release-noted change of default.
  Document what changed and why, then re-record.

Re-recording is a deliberate act. A pull request that re-records a snapshot
should say which of the two it is, in the commit message.
