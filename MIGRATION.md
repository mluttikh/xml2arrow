# Migrating to xml2arrow 0.20

Everything here falls into five sections, and only the first is mandatory:

1. **[Required](#1-required-mechanical-source-changes)**: mechanical source
   changes, because five public types became `#[non_exhaustive]`.
2. **[Behavioral](#2-behavioral-changes)**: two changes that can turn a
   previously-succeeding parse into an error. Both replace *silent* wrongness,
   and both have an opt-out.
3. **[Configuration format version 2](#3-configuration-format-version-2)**: a
   new configuration format. Version 1, the format of every config so far, is
   deprecated and keeps working until 1.0.
4. **[Deprecated](#4-deprecated-still-working)**: what still works, and what
   replaces it.
5. **[Additive](#5-new-purely-additive)**: new API that needs no action.

If you construct configs with `TableConfig::new` / `FieldConfigBuilder` and
parse with `Parser`, the required work is **nothing** unless you *read*
`TableConfig::xml_path` or `FieldConfig::xml_path` — skip to §2.

---

## 1. Required: mechanical source changes

`Config`, `TableConfig`, `FieldConfig`, `ParserOptions` and `DType` are now
`#[non_exhaustive]`. Struct-literal construction and exhaustive `match`es on
`DType` no longer compile from outside the crate. Reading and mutating the
public fields is unaffected.

This is the one compile break in the release, and it is what makes every new
config key a non-breaking addition rather than another break.

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

### `xml_path` is now an Option on both config structs

`TableConfig` gained `scope` and `FieldConfig` gained `path`, each an
alternative spelling of the same element, so `xml_path` became optional on both:

```rust
// TableConfig
pub xml_path: Option<String>,   // was String
pub scope:    Option<String>,   // new, the version 2 key

// FieldConfig
pub xml_path: Option<String>,   // was String
pub path:     Option<String>,   // new, the version 2 key
```

**Nothing changes for YAML configs**, and nothing changes for code that builds
fields with `FieldConfigBuilder`. Only code that *reads* the field is affected:

```rust
// before
println!("{}", field.xml_path);

// after — a version 1 config sets `xml_path`, a version 2 config sets `path`
println!("{}", field.path.as_deref().or(field.xml_path.as_deref()).unwrap_or(""));
// the same for a table, whose version 2 key is `scope`
println!("{}", table.scope.as_deref().or(table.xml_path.as_deref()).unwrap_or(""));
```

`TableConfig::new`, `TableConfig::builder` and `FieldConfigBuilder::new` still
populate `xml_path`, so a version 1 config built in code is unchanged. When the
config declares `version: 2`, `ConfigBuilder::build` moves each element to the
version 2 key.

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

## 3. Configuration format version 2

0.20 adds configuration format version 2. A config opts in by declaring it:

```yaml
version: 2
tables:
  - name: stations
    xml_path: /report/stations
    row: station                    # declared, not inferred
    fields:
      - {name: id, path: "@id", data_type: Utf8}   # relative to the row

  - name: readings
    xml_path: /report/stations/station/readings
    row: reading
    links:
      - parent: stations            # a join key, instead of levels
    fields:
      - {name: value, path: value, data_type: Float64}
```

| Version 1 | Version 2 | Why |
|---|---|---|
| rows inferred from the configured fields | `row:` | adding a field could change a table's row count |
| `levels:` position columns | `links:`: `parent:` join keys, or `index_of:` positions | positions start again in every container, so a join on them goes wrong when a container repeats |
| `xml_path:` on a table | `scope:`, the same value | the key names what the element does: it scopes the rows, the positions and the values |
| `xml_path:` on a field, absolute | `path:`, relative to the row or absolute | the full path no longer has to be repeated on every field |
| `Utf8` whitespace kept; a missing non-nullable `Utf8` value is `""` | trimmed; an error | both depended on the column's type rather than on the config |
| no per-field control | `trim`, `on_missing`, `on_invalid`, `on_repeat`, `null_values`, and `defaults:` | |

**An existing config needs no change.** A config without `version: 2` is
version 1, and parses exactly as before; the one visible change is the
deprecation notice described in §4. A config is version 1 or version 2 as a
whole: `scope:`, `row:`, `path:`, `links:`, `row_id:`, `metadata:`,
`defaults:` and the value policies are version 2 keys, and a version 1 config that sets one is
rejected when it is loaded, with a message naming the key. `levels:` is no
longer required in version 1.

- [Configuration reference](docs/configuration.md): version 2.
- [Configuration format version 1](docs/configuration-v1.md): the deprecated
  format, as it has always behaved.
- [Migrating to configuration format version 2](docs/migrating-to-version-2.md):
  convert, then adopt what version 2 changes one edit at a time, each checked
  with the new `config_diff` example, which parses one document under two
  configs and reports what differs:

  ```bash
  cargo run --example config_diff -- before.yaml after.yaml document.xml
  ```

- `Config::to_version_2` converts a config without changing the output, and
  lists what it leaves for you to decide. From the command line:
  `cargo run --example convert_config -- config.yaml > config-v2.yaml`.

---

## 4. Deprecated, still working

These all keep working until 1.0.

| Deprecated | Replacement |
|---|---|
| configuration format version 1 — a config with no `version:`, or `version: 1` | `version: 2`; see [the migration guide](docs/migrating-to-version-2.md) |
| `parse_xml(reader, &config)` | `Parser::new(&config)?.parse(reader)` |
| `parse_xml_slice(xml, &config)` | `Parser::new(&config)?.parse_slice(xml)` |
| `parser.parse_streaming(reader, opts, sink)` | `for item in parser.parse_batches(reader, opts)` |
| `xml_path:` on a **table** | `scope:` in version 2 — a key rename ([converting by hand](docs/migrating-to-version-2.md#converting-by-hand)) |
| `xml_path:` on a **field** | `path:` in version 2 — a key rename for absolute values ([converting by hand](docs/migrating-to-version-2.md#converting-by-hand)) |

**Every version 1 config now carries a deprecation notice** in `Config::lint()`
/ `Parser::warnings()`. The config parses exactly as before; the notice is the
only change. It lists what `version: 2` would reject, grouped by kind, and is
structured data for tooling (`Lint::ConfigVersion1 { steps }`, each a
`MigrationStep`):

```text
This config uses configuration format version 1, which is deprecated. Before
it can declare `version: 2`: 3 tables must rename the `xml_path:` key to
`scope:` (header, stations, readings); 3 tables must declare `row:` rather than
infer their rows (header, stations, readings); 2 tables must replace `levels:`
with `links:` (stations, readings); 6 fields must rename the `xml_path:` key to
`path:` (header.title, header.created, stations.id, …). Declaring `version: 2`
also changes two defaults: `Utf8` values are trimmed, and a missing
non-nullable `Utf8` value is an error rather than ""
```

A host that treats any warning as a failure will now see one for every version
1 config. Filter out `Lint::ConfigVersion1` if you need the previous behavior
until you migrate.

The free functions hide the one-time path-compilation cost and pay it on *every*
call. Constructing a `Parser` once and reusing it is the whole "compile once,
parse many" design — measurably faster for anything beyond a single document.

---

## 5. New, purely additive

Nothing below requires action.

- **Owned streams.** `parser.clone().into_batches(reader, opts)` returns a
  `'static` stream, and `into_single_table` does the same for the
  `RecordBatchReader` adapter. `Parser` is a cheap `Clone` handle over shared
  compiled state, and is `Send + Sync` — no producer thread or channel needed
  to hand a stream to FFI or an async runtime.
- **`parser_options.error_on_unmatched_fields`** (default off) reports every
  configured field that captured nothing, in one pass. A misspelled path
  otherwise shows up as a silently all-null column.
- **`parser_options.max_value_bytes`** (default unlimited) bounds what a single
  field may accumulate across text, CDATA and entity events.
- **`Config::lint()` / `Parser::warnings()`** return advisory findings. The
  library never prints, and lints never change how a document parses.
- **`Config::from_yaml_str`**, the counterpart to `Config::from_yaml_file` for
  callers that already hold the YAML — an embedded default, a config fetched
  over the network, or a test that would rather not touch the filesystem. It
  validates like the file version does.
- **`Config::to_version_2`** converts a configuration to format version 2
  without changing what it produces, and returns what it could not convert as
  `Unconverted` entries. The converted config declares `version: 2`, and loads
  when there are none.
- **`metadata:` on tables and fields**, your own key-value pairs, copied into
  the Arrow schema and field metadata of every batch, and so into Parquet files
  and pyarrow tables. Configuration format version 2 only; keys beginning
  `ARROW:` are rejected, as Arrow reserves them.
- **`From<ConfigIssue> for Error`**, so a tool that builds or checks configs
  can turn an issue into the error the library would have raised, with `?` or
  `.into()`, instead of writing `Error::InvalidConfig { reason }` by hand.
