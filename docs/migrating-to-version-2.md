# Migrating to configuration format version 2

[Configuration format version 1](configuration-v1.md) is deprecated, and keeps
working until 1.0. This guide moves a version 1 config to
[version 2](configuration.md) in four steps. Each step is a small edit that you
can check before taking the next, and most of them leave the output unchanged.

- [What changes](#what-changes)
- [Before you start](#before-you-start)
- [Step 1: rename `xml_path:` to `path:`](#step-1-rename-xml_path-to-path)
- [Step 2: declare each table's `row:`](#step-2-declare-each-tables-row)
- [Step 3: replace `levels:` with `links:`](#step-3-replace-levels-with-links)
- [Step 4: declare `version: 2`](#step-4-declare-version-2)
- [The result](#the-result)
- [Configs built in Rust](#configs-built-in-rust)

## What changes

| Step | Version 1 | Version 2 | Output |
|---|---|---|---|
| 1 | `xml_path:` on a field | `path:` | unchanged |
| 2 | rows are inferred | `row:` | unchanged, except for tables whose rows were split |
| 3 | `levels:` | `links:` | unchanged with `index_of:`; key columns replace the position columns if you use `parent:` |
| 4 | `Utf8` whitespace kept; a missing non-nullable `Utf8` value is `""` | trimmed; an error | changes where it applies, unless you keep the old behavior per field |

## Before you start

You need three things.

**The config as it is now**, saved as `before.yaml` and left alone.

**Documents to test with.** Pick a few representative ones, including any with
optional or empty elements.

**A way to compare output.** `config_diff`, in a clone of this repository, parses
one document under two configs and reports the difference in row counts, columns
and values:

```bash
cargo run --example config_diff -- before.yaml after.yaml document.xml
```

It exits with 0 when the output is identical and 1 when it differs. Values are
compared in tables whose row count is unchanged, and each changed column is
reported with a count and its first example. From Python, compare the tables
directly:

```python
from xml2arrow import XmlToArrowParser

before = XmlToArrowParser("before.yaml").parse("document.xml")
after = XmlToArrowParser("after.yaml").parse("document.xml")
for name in sorted(before.keys() | after.keys()):
    if name not in before or name not in after or not before[name].equals(after[name]):
        print("differs:", name)
```

The deprecation notice from `parser.warnings()` doubles as a checklist: it lists
what is left, and shrinks after each step.

This guide migrates the [version 1 example](configuration-v1.md#example):

```yaml
tables:
  - name: header
    xml_path: /report/header
    levels: []
    fields:
      - {name: title,   xml_path: /report/header/title,   data_type: Utf8, nullable: true}
      - {name: created, xml_path: /report/header/created, data_type: Utf8, nullable: true}

  - name: stations
    xml_path: /report/stations
    levels: [station]
    fields:
      - {name: id,   xml_path: /report/stations/station/@id,  data_type: Utf8}
      - {name: name, xml_path: /report/stations/station/name, data_type: Utf8}

  - name: readings
    xml_path: /report/stations/station/readings
    levels: [station, reading]
    fields:
      - {name: time,  xml_path: /report/stations/station/readings/reading/@time, data_type: Utf8}
      - {name: value, xml_path: /report/stations/station/readings/reading/value, data_type: Float64}
```

Its deprecation notice:

```text
This config uses configuration format version 1, which is deprecated. Before
it can declare `version: 2`: 3 tables must declare `row:` rather than infer
their rows (header, stations, readings); 2 tables must replace `levels:` with
`links:` (stations, readings); 6 fields must rename the `xml_path:` key to
`path:` (header.title, header.created, stations.id, …). Declaring `version: 2`
also changes two defaults: `Utf8` values are trimmed, and a missing
non-nullable `Utf8` value is an error rather than ""
```

## Step 1: rename `xml_path:` to `path:`

On every field, rename the key and keep the value:

```yaml
      # before
      - {name: title, xml_path: /report/header/title, data_type: Utf8, nullable: true}
      # after
      - {name: title, path: /report/header/title, data_type: Utf8, nullable: true}
```

Tables keep their `xml_path:`; only fields change.

**The output does not change**: an absolute path means the same under either
key.

```text
  = header: unchanged (2 rows)
  = readings: unchanged (3 rows)
  = stations: unchanged (2 rows)

No differences.
```

## Step 2: declare each table's `row:`

Name the element that makes one row:

```yaml
  - name: header
    xml_path: /report/header
    row: "."        # one row per <header>

  - name: stations
    xml_path: /report/stations
    row: station    # one row per <station>

  - name: readings
    xml_path: /report/stations/station/readings
    row: reading    # one row per <reading>
```

Use the name of the repeating element, or `"."` when the `xml_path` element
itself is the row, as for a header. [`row`](configuration.md#row) lists every
spelling.

**For a table that `parser.warnings()` does not report, the output does not
change.** Its fields all sit under one child element, and a row already ended
there. Here that is `stations` and `readings`.

**A table reported as `InferredRowBoundary` changes, and that is the fix.** Its
rows were split, one for each configured child element. `header` had two rows
holding one value each, and now has one row holding both:

```text
  ~ header:
      rows: 2 -> 1
  = readings: unchanged (3 rows)
  = stations: unchanged (2 rows)

Configs differ on this document.
```

Update anything downstream that worked around the split rows.

With `row:` declared, fields can use paths relative to the row element, such as
`path: "@id"` instead of `path: /report/stations/station/@id`. That is optional
and does not change the output.

## Step 3: replace `levels:` with `links:`

Version 2 does not allow `levels:`. Each entry becomes an `index_of:` link that
names the row element of the table the entry counts. Reading the entries from
the outside in, each one counts the rows of one enclosing table; when there is
one entry more than there are enclosing tables, the last one counts the table's
own rows. (The full rule is under [`levels`](configuration-v1.md#levels).)

| `levels:` entry | Replace it with | Output |
|---|---|---|
| none: `levels: []`, or no `levels:` | nothing, or `links: []` if the table is nested | unchanged |
| counts an enclosing table's rows | `index_of:` that table's row element, with `name:` | unchanged |
| counts the table's own rows | `index_of:` the table's own row element, with `name:` | unchanged |

In the example, `<station>` on `readings` counts rows of the enclosing
`stations` table. `<station>` on `stations` and `<reading>` on `readings` count
their own table's rows.

A nested table always needs `links:` in version 2, even one that had no
`levels:`. The deprecation notice names each one.

### Keeping the position values

```yaml
  - name: stations
    xml_path: /report/stations
    row: station
    links:
      - index_of: /report/stations/station                  # its own rows
        name: "<station>"

  - name: readings
    xml_path: /report/stations/station/readings
    row: reading
    links:
      - index_of: /report/stations/station                  # the enclosing station
        name: "<station>"
      - index_of: /report/stations/station/readings/reading # its own rows
        name: "<reading>"
```

`name:` keeps each column's version 1 name, so the output is identical:

```text
  = header: unchanged (1 rows)
  = readings: unchanged (3 rows)
  = stations: unchanged (2 rows)

No differences.
```

The version 1 join on `<station>` still works, with the weakness it always had:
positions start again in every `<stations>` element, so once that element
repeats, the join pairs readings with the wrong station. For a join, use keys
instead.

### Using join keys (recommended)

```yaml
  - name: readings
    xml_path: /report/stations/station/readings
    row: reading
    links:
      - parent: stations
```

```text
  = header: unchanged (1 rows)
  ~ readings:
      columns added:   _stations_id
      columns removed: <reading>, <station>
  ~ stations:
      columns added:   _id
      columns removed: <station>

Configs differ on this document.
```

Join on `readings._stations_id = stations._id`. This join stays correct when
`<stations>` repeats, where positions would pair rows with the wrong parent; see
[`index_of:`](configuration.md#index_of). The keys are `UInt64`. To keep the
column names from version 1, set `row_id: "<station>"` on `stations` and
`name: "<station>"` on the link.

The rest of this guide uses join keys.

## Step 4: declare `version: 2`

Add the line at the top of the config:

```yaml
version: 2
```

Loading the config now checks that nothing from version 1 is left, and an error
names anything that is. Once it loads, the deprecation notice is gone.

Two defaults change with it:

| | Version 1 | Version 2 |
|---|---|---|
| Whitespace around a `Utf8` value | kept | trimmed |
| A missing value in a non-nullable `Utf8` column | `""` | an error |

In the example, one station name has whitespace around it:

```text
  = header: unchanged (1 rows)
  = readings: unchanged (3 rows)
  ~ stations:
      values changed:  name: 1 of 2 rows, e.g. row 0: " North " -> "North"

Configs differ on this document.
```

To keep the version 1 behavior, set it on the field:

```yaml
      - {name: name, path: name, data_type: Utf8, trim: false}         # keep the whitespace
      - {name: note, path: note, data_type: Utf8, on_missing: empty}   # missing -> ""
```

With `trim: false` on `name`, the example's output is identical to step 3.
Set `trim: false` on individual fields rather than in `defaults:`: it would also
stop numbers from being trimmed, and a value like `" 42 "` would fail to parse.

A missing value in a non-nullable `Utf8` column now fails the parse, which
`config_diff` reports as an error rather than a difference. Test with documents
where values can be absent. Where that is legitimate, set `nullable: true`, or
`on_missing: empty` to keep `""`.

To undo this step, delete the line.

## The result

```yaml
version: 2

tables:
  - name: header
    xml_path: /report/header
    row: "."
    fields:
      - {name: title,   path: title,   data_type: Utf8, nullable: true}
      - {name: created, path: created, data_type: Utf8, nullable: true}

  - name: stations
    xml_path: /report/stations
    row: station
    fields:
      - {name: id,   path: "@id", data_type: Utf8}
      - {name: name, path: name,  data_type: Utf8}

  - name: readings
    xml_path: /report/stations/station/readings
    row: reading
    links:
      - parent: stations
    fields:
      - {name: time,  path: "@time", data_type: Utf8}
      - {name: value, path: value,   data_type: Float64}
```

```text
header                            stations
+------------------+------------+ +-----+----+-------+
| title            | created    | | _id | id | name  |
+------------------+------------+ +-----+----+-------+
| Station readings | 2026-07-26 | | 0   | S1 | North |
+------------------+------------+ | 1   | S2 | South |
                                  +-----+----+-------+
readings
+--------------+-------+-------+
| _stations_id | time  | value |
+--------------+-------+-------+
| 0            | 08:00 | 1.5   |
| 0            | 09:00 | 2.5   |
| 1            | 08:00 | 3.0   |
+--------------+-------+-------+
```

## Configs built in Rust

The same steps apply to a config built in code:

| YAML | Rust |
|---|---|
| `version: 2` | `ConfigBuilder::version(2)` |
| `row:` | `TableConfigBuilder::row` |
| `links:` | `TableConfigBuilder::links`, with `Link` values |
| `path:` | `FieldConfigBuilder::new(name, path, data_type)`, which already sets `path` |
| value policies | `FieldConfigBuilder::policies` and `ConfigBuilder::defaults` |

`TableConfig::new` takes `levels`; build version 2 tables with
`TableConfig::builder` instead.

```rust
use xml2arrow::{Config, DType, FieldConfigBuilder, Link, TableConfig};

let mut to_station = Link::default();
to_station.parent = Some("stations".to_string());

let config = Config::builder()
    .version(2)
    .table(
        TableConfig::builder("stations", "/report/stations")
            .row("station")
            .field(FieldConfigBuilder::new("id", "@id", DType::Utf8).build()?)
            .build(),
    )
    .table(
        TableConfig::builder("readings", "/report/stations/station/readings")
            .row("reading")
            .links([to_station])
            .field(FieldConfigBuilder::new("value", "value", DType::Float64).build()?)
            .build(),
    )
    .build()?;
```

To drive a migration from code, `Lint::ConfigVersion1 { steps }` lists the
remaining changes as `MigrationStep` values.
