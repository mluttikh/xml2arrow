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
5. **[Optional](#5-optional-declare-links-instead-of-levels)** — `links:`,
   replacing `levels` with declared relationships and a real join key.
6. **[Optional](#6-optional-per-field-value-policies)** — `trim`, `on_missing`,
   `on_invalid`, `on_repeat`, `null_values`, for opting out of the
   type-dependent value quirks.

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

## 5. Optional: declare links instead of `levels`

`levels` names *labels*. Its values come positionally from whatever ancestor
tables happen to enclose the table, so a mismatch between the names you wrote
and the tables that actually enclose you produces a column of plausible wrong
numbers rather than an error.

`links:` names the relationship itself:

```yaml
# before
  - name: measurements
    xml_path: /report/group/station/ms
    row: m
    levels: [station]

# after
  - name: measurements
    xml_path: /report/group/station/ms
    row: m
    links:
      - parent: stations
```

A table uses `levels` or `links`, never both.

### The two kinds

| Link | Column | Value | Use |
|---|---|---|---|
| `parent: <table>` | `_<table>_id`, `UInt64` | the parent's **global** row ordinal, never reset | a real join key |
| `index_of: <path>` | `<element>_idx`, `UInt32` | per-scope ordinal | positional only — **identical to the legacy `<level>` value** |

`index_of` exists so adopting `links:` need not change a single number. Only
the column name moves (`<station>` → `station_idx`), and `name:` can keep even
that identical.

### Why `parent:` is worth the value change

The difference only shows up when a container repeats:

```xml
<report>
  <group><station><id>A</id><ms><m>…</m><m>…</m></ms></station></group>
  <group><station><id>B</id><ms><m>…</m></ms></station></group>
</report>
```

| column | values | meaning |
|---|---|---|
| `station_idx` (or legacy `<station>`) | `0, 0, 0` | "first station of my group" — true for both A and B |
| `_stations_id` | `0, 0, 1` | station A, station A, station B |

A join on the positional column silently attributes B's measurement to A. The
`parent:` key does not, because it is a global ordinal.

### `_id` on the parent

A table referenced by a `parent:` link automatically materializes `_id`
(`UInt64`, non-null), so both sides of the join exist. Tables nobody references
gain nothing. Override per table with `row_id: my_name` or `row_id: false`.

### Rules

- The parent's rows must **enclose** the child's, checked by name at load time —
  the misalignment `levels` could express is rejected rather than parsed.
- `index_of:` must name an enclosing **table's** row element, since it reads
  that table's existing counter.
- A link column that would collide with a field name is an error, not a silent
  shadow.
- **Streaming note:** a parent row finalizes *after* its children, so a child
  batch can reference a parent row that arrives in a later batch. The foreign
  key is still correct — that is what "global" buys — but consumers that join
  incrementally need to buffer or join at the end.

### `levels` may now be omitted

`levels:` is no longer a required key, so a table that declares `links:` (or
needs no parent columns) simply leaves it out. Existing configs that state it
are unaffected.

---

## 6. Optional: per-field value policies

Five keys, all optional, all defaulting to **exactly what the parser did
before** — including the type-dependent quirks. Setting one opts out of a
specific quirk; it does not switch engines.

| Key | Values | Default |
|---|---|---|
| `trim` | `true` / `false` | numeric and boolean trim, `Utf8` does not |
| `on_missing` | `error` / `null` / `empty` | nullable → `null`, non-nullable `Utf8` → `""`, otherwise `error` |
| `on_invalid` | `error` / `null` | `error` |
| `on_repeat` | `error` / `first` / `last` | `error` |
| `null_values` | list of strings | none |

A `defaults:` block at the top of the config applies them to every field that
sets none; a field's own key wins.

```yaml
defaults:
  trim: true
tables:
  - name: items
    xml_path: /report
    row: item
    fields:
      - {name: n, path: n, data_type: Int32, nullable: true, on_invalid: null, null_values: ["N/A"]}
      - {name: v, path: v, data_type: Utf8, nullable: true, on_repeat: last}
```

### The quirk worth knowing about

A missing non-nullable `Utf8` field yields `""`, while a missing non-nullable
number is an error. Whether an absent element ends your parse therefore depends
on which type the column happens to be. `on_missing: error` (or `null`) makes a
column behave the same either way — this is the single most useful policy here.

### Notes

- **`on_missing: null` needs no quotes.** In YAML a bare `null` is the null
  literal, which would normally read as "key absent" and silently leave the
  field on its default. That case is handled: written out, it selects the
  policy.
- A policy that cannot apply is **rejected**, not ignored — `null` on a
  non-nullable column, or `empty` on a type that has no empty value.
- `on_repeat: first` keeps the whole first value even when its text arrives in
  several parser events; `last` keeps the final occurrence.
- `null_values` is compared after trimming, case-sensitively, and the resulting
  missing value is then handled by `on_missing`.

---

## 7. Optional: assert you are done with `version: 2`

Sections 3 through 6 are each independently optional, which is the point — but
it also means there is no moment where you find out you have finished. A config
can be half migrated indefinitely and never say so.

`version: 2` is that moment. It is an **assertion, not a switch**:

```yaml
version: 2
```

The parser holds the config to it at load, and rejects anything left over:

| Left over | Message names |
|---|---|
| a table with no `row:` | the table |
| a table still using `levels:` | the table |
| a field still spelled `xml_path:` | the table and the field |
| a nested table with no `links:` | the table and the one enclosing it |

Nothing there is a *correctness* problem — every one of those configs parses
perfectly well without the `version:` line. What they cannot do is parse under
1.0 semantics, which is the single thing declaring `2` claims.

### What you get in exchange

The two value defaults 1.0 will make mandatory, a full release cycle early.
Both are places where the historical default was chosen by the column's Arrow
type rather than by anything you asked for:

| | v1 | v2 |
|---|---|---|
| `trim` | on for numbers and booleans, off for `Utf8` | on for every type |
| a missing non-nullable value | `""` for `Utf8`, an error for anything else | an error for every type |

Same document, same config but for the one line:

```xml
<item><s> hi </s><n> 42 </n></item>
```

| Column | v1 | v2 |
|---|---|---|
| `s` (`Utf8`) | `" hi "` | `"hi"` |
| `n` (`Int32`) | `42` | `42` |

And with `<s>` absent entirely, a non-nullable `s` is `""` under v1 and a
`MissingRequiredField` error under v2 — so "does an absent element stop the
parse?" stops depending on which type the column happens to be.

### Reverting

Delete the line. That is the whole procedure: `version: 2` sets *defaults*, so
any field that states what it wants still gets it —

```yaml
- {name: s, path: s, data_type: Utf8, trim: false}
- {name: note, path: note, data_type: Utf8, on_missing: empty}
```

— and per-field opt-outs survive the flag going away, because they were never
about the flag.

`version: 1` and omitting the key are the same thing: every release so far.
An unrecognised version is rejected rather than guessed at, since pinning
semantics is the one job this key has.

---

## 8. Deprecated, still working

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

## 9. New, purely additive

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
- **`Config::from_yaml_str`**, the counterpart to `Config::from_yaml_file` for
  callers that already hold the YAML — an embedded default, a config fetched
  over the network, or a test that would rather not touch the filesystem. It
  validates like the file version does.
- **`From<ConfigIssue> for Error`**, so a tool that builds or checks configs
  can turn an issue into the error the library would have raised, with `?` or
  `.into()`, instead of writing `Error::InvalidConfig { reason }` by hand.
