# Migrating to xml2arrow 0.20

Everything here falls into three buckets, and only the first is mandatory:

1. **[Required](#1-required-mechanical-source-changes)** — mechanical source
   changes, because five public types became `#[non_exhaustive]`.
2. **[Behavioral](#2-behavioral-changes)** — two changes that can turn a
   previously-succeeding parse into an error. Both replace *silent* wrongness,
   and both have an opt-out.
3. **[Optional](#3-optional-declare-your-row-boundaries)** — `row:`, the opt-in
   that fixes inferred row boundaries. Nothing changes until you add the line.
4. **[Optional](#4-optional-write-field-paths-relative-to-the-row)** — `path:`,
   which lets a field be written relative to the row. Pure ergonomics: both
   spellings compile to the same node.

If you construct configs with `TableConfig::new` / `FieldConfigBuilder` and
parse with `Parser`, the required work is **nothing** unless you *read*
`FieldConfig::xml_path` — skip to §2.

---

## 1. Required: mechanical source changes

`Config`, `TableConfig`, `FieldConfig`, `ParserOptions` and `DType` are now
`#[non_exhaustive]`. Struct-literal construction and exhaustive `match`es on
`DType` no longer compile from outside the crate. Reading and mutating the
public fields is unaffected.

This is the one compile break in the release, and it is what makes every later
config key — `row:` here, per-field policies later — a non-breaking addition
rather than another break.

### Config / TableConfig / FieldConfig

```rust
// before — no longer compiles
let config = Config { tables, parser_options: ParserOptions::default() };

// after — either form works
let config = Config::builder().tables(tables).build()?;   // build() also validates
```

```rust
// before
let table = TableConfig { name, xml_path, levels, fields };

// after — `new` is unchanged and still the shortest form
let table = TableConfig::new("items", "/data", vec![], fields);
let table = TableConfig::builder("items", "/data").fields(fields).build();
```

### ParserOptions

There is no builder; mutate the default instead.

```rust
// before
let options = ParserOptions { trim_text: true, ..Default::default() };

// after
let mut options = ParserOptions::default();
options.trim_text = true;
```

### FieldConfig::xml_path is now an Option

`FieldConfig` gained `path`, and the two are alternative spellings of one
location, so `xml_path` became optional:

```rust
pub xml_path: Option<String>,   // was String
pub path:     Option<String>,   // new
```

**Nothing changes for YAML configs**, and nothing changes for code that builds
fields with `FieldConfigBuilder`. Only code that *reads* the field is affected:

```rust
// before
println!("{}", field.xml_path);

// after — either handle both spellings...
println!("{}", field.path.as_deref().or(field.xml_path.as_deref()).unwrap_or(""));
// ...or, if you only ever wrote absolute paths, the one you set:
println!("{}", field.path.as_deref().unwrap_or_default());
```

Note that `FieldConfigBuilder::new(name, path, dtype)` now populates `path`
rather than `xml_path`. Its behavior is unchanged for absolute values — an
absolute path means the same under either key — but a `FieldConfig` built this
way has `xml_path == None`.

### DType

An exhaustive `match` on `DType` outside the crate needs a wildcard arm:

```rust
match dtype {
    DType::Int32 => ...,
    DType::Utf8  => ...,
    _ => ...,          // required: new dtypes are planned and now additive
}
```

---

## 2. Behavioral changes

### 2.1 Truncated documents are an error

A document that ended mid-element used to parse to `Ok`, returning the rows
that had closed before the cut — a well-formed `RecordBatch`, indistinguishable
from a complete parse. A killed writer, a short read or a partial download
produced quietly incomplete data.

```text
Truncated document: input ended with 2 element(s) still open; the rows parsed
so far are incomplete and were discarded (set parser_options.allow_truncated_input
to accept them)
```

**If you were relying on partial results** — recovery tooling, salvaging a
damaged file — restore the old behavior explicitly:

```rust
let mut options = ParserOptions::default();
options.allow_truncated_input = true;
```

**If you were not**, you may have been silently accepting truncated data. This
is the change most likely to surface a real problem in an existing pipeline.

### 2.2 Value-level errors gained a coordinate suffix

`ParseError`, `MissingRequiredField` and the new `ValueTooLarge` now append
document coordinates:

```text
... (row index 41207, near byte offset 2883104)
```

**Message prefixes are unchanged**, so substring matches against the existing
text still work. If you match on the *whole* message, switch to a prefix or
substring match — or read `Error`'s structured fields, which is what they are
there for. The row index points into the output you were building, which is
usually the more actionable of the two coordinates.

---

## 3. Optional: declare your row boundaries

### The problem

A row of a table is finalized when any **configured direct child** of its
`xml_path` closes. That rule is invisible in the config and depends on which
fields happen to be configured — so a metadata table with three fields yields
three one-third-filled rows, and **adding a field can change a table's row
count**.

```yaml
tables:
  - name: header
    xml_path: /report/header
    fields:
      - {name: title,   xml_path: /report/header/title,   data_type: Utf8, nullable: true}
      - {name: created, xml_path: /report/header/created, data_type: Utf8, nullable: true}
```

```xml
<report><header><title>Q3</title><created>2026-07-26</created></header></report>
```

Output — two rows, each half empty:

```text
rows: 2
  "Q3" | null
  null | "2026-07-26"
```

### The fix

```yaml
    xml_path: /report/header
    row: "."          # <- the only line added
```

```text
rows: 1
  "Q3" | "2026-07-26"
```

### Spellings

| `row:` value | Resolves to | Meaning |
|---|---|---|
| `"."` | the `xml_path` element itself | one row per occurrence — what metadata tables usually mean |
| `item` | `<xml_path>/item` | relative to `xml_path` |
| `items/item` | `<xml_path>/items/item` | relative, multi-segment |
| `/report/data/item` | as written | absolute |

**A leading slash is what makes a path absolute.** This is stricter than the
crate's other paths, where a leading slash is insignificant: `row: report/data/item`
is *relative* and resolves to `<xml_path>/report/data/item`. Get it wrong and
every field lands outside the row subtree, which `Config::lint()` reports as
`FieldOutsideRow`.

### Finding the tables that need it

The linter names both the problem and the fix, and needs no document:

```rust
let parser = Parser::new(&config)?;
for lint in parser.warnings() {
    eprintln!("{lint}");
}
```

```text
Table 'header' (xml_path /report/header) has 2 distinct configured child
elements (title, created); row boundaries are inferred, so this table produces
2 partially-filled rows per <header> rather than one. Declare `row:` to fix it:
`row: "."` for one row per <header>, or `row: <element>` to name the repeating
element
```

The lint goes quiet once you declare a row. A table with exactly one configured
child element is never flagged — its inferred boundary is already unambiguous,
and declaring it explicitly changes nothing.

### Seeing the delta before you commit to it

`config_diff` parses one document under two configs and reports what moved:

```bash
cargo run --example config_diff -- before.yaml after.yaml document.xml
```

```text
  ~ header:
      rows: 2 -> 1

Configs differ on this document.
```

It exits non-zero when anything differs, so a migration can be gated in CI.

### Rules and limits

- `row:` must resolve to the table's `xml_path` or a descendant of it.
- Another table may not sit **strictly between** a table and its row element.
  Rows finalize against the innermost open table, so the inner one would
  absorb them; this is rejected at validation rather than parsed wrongly.
- A row resolving to the **root table** (`xml_path: /`) is rejected: the
  implicit document root never closes, so no row could be finalized. Leave
  `row:` off a root table — inference already yields one row there.
- A field outside the row subtree is a **lint, not an error**. It keeps its
  pre-declaration behavior: the value attaches to whichever row finalizes next.
- Opting one table in never affects another. Tables that declare nothing keep
  inferred boundaries exactly.

---

## 4. Optional: write field paths relative to the row

Once a table declares `row:`, its fields can be written relative to that row
element instead of repeating the full path on every column. Purely ergonomic:
both spellings compile to the same node, so the output is identical.

```yaml
# before — every column repeats the full path
  - name: readings
    xml_path: /report/stations/station/readings
    row: reading
    fields:
      - {name: seq,   xml_path: /report/stations/station/readings/reading/@seq,        data_type: Int32}
      - {name: value, xml_path: /report/stations/station/readings/reading/value,       data_type: Int32}
      - {name: unit,  xml_path: /report/stations/station/readings/reading/sensor/@unit, data_type: Utf8}

# after
  - name: readings
    xml_path: /report/stations/station/readings
    row: reading
    fields:
      - {name: seq,   path: "@seq",       data_type: Int32}
      - {name: value, path: value,        data_type: Int32}
      - {name: unit,  path: sensor/@unit, data_type: Utf8}
```

`path` follows **the same rule as `row:`** — there is one path rule in the whole
configuration:

| `path:` value | Resolves to |
|---|---|
| `/report/data/item/v` | as written (**leading slash = absolute**) |
| `v` | `<row>/v` |
| `sensor/@unit` | `<row>/sensor/@unit` |
| `@seq` | `<row>/@seq` — an attribute of the row element |

Rules:

- Set **exactly one** of `path` and `xml_path` per field. Setting both is an
  error rather than a silent winner; setting neither names no location.
- A **relative** `path` requires the table to declare `row:` — it is relative to
  the row element, so without one there is nothing to resolve against. An
  absolute `path` needs no row.
- Error messages quote the **resolved** path (`/report/data/item/v`), not the
  abbreviation, since that is what you need to find the value in the document.

### Renaming `xml_path` to `path`

For an absolute path this is a pure key rename with no output change, because an
absolute value means the same under either key:

```yaml
- {name: v, xml_path: /report/data/item/v, data_type: Int32}
- {name: v, path:     /report/data/item/v, data_type: Int32}   # identical
```

`xml_path` keeps working until 1.0, which removes it. Shortening to the relative
form afterwards is a second, independent step.

---

## 5. Deprecated, still working

All three keep working until 1.0.

| Deprecated | Replacement |
|---|---|
| `parse_xml(reader, &config)` | `Parser::new(&config)?.parse(reader)` |
| `parse_xml_slice(xml, &config)` | `Parser::new(&config)?.parse_slice(xml)` |
| `parser.parse_streaming(reader, opts, sink)` | `for item in parser.parse_batches(reader, opts)` |
| `xml_path:` on a **field** | `path:` — a key rename for absolute values (§4) |

The free functions hide the one-time path-compilation cost and pay it on *every*
call. Constructing a `Parser` once and reusing it is the whole "compile once,
parse many" design — measurably faster for anything beyond a single document.

---

## 6. New, purely additive

Nothing below requires action.

- **Owned streams.** `parser.clone().into_batches(reader, opts)` returns a
  `'static` stream, and `into_single_table` does the same for the
  `RecordBatchReader` adapter. `Parser` is a cheap `Clone` handle over shared
  compiled state, and is `Send + Sync` — no producer thread or channel needed
  to hand a stream to FFI or an async runtime.
- **`parser_options.error_on_unmatched_fields`** (default off) reports every
  configured field that captured nothing, in one pass. A misspelled `xml_path`
  otherwise shows up as a silently all-null column.
- **`parser_options.max_value_bytes`** (default unlimited) bounds what a single
  field may accumulate across text, CDATA and entity events.
- **`Config::lint()` / `Parser::warnings()`** return advisory findings. The
  library never prints, and lints never change how a document parses.
