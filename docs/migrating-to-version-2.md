# Migrating to configuration format version 2

[Configuration format version 1](configuration-v1.md) is deprecated, and keeps
working until 1.0. A config is version 1 or version 2 as a whole: version 1
rejects every key that version 2 adds. So the move itself is one change, which
the converter makes without changing the output. After it, you adopt what
version 2 offers one small edit at a time, and check each before the next.

- [What changes](#what-changes)
- [Before you start](#before-you-start)
- [Step 1: convert](#step-1-convert)
- [Step 2: resolve what is left](#step-2-resolve-what-is-left)
- [Step 3: use join keys](#step-3-use-join-keys)
- [Step 4: adopt the version 2 value defaults](#step-4-adopt-the-version-2-value-defaults)
- [The result](#the-result)
- [Configs built in Rust](#configs-built-in-rust)

## What changes

| Version 1 | Version 2 | Output |
|---|---|---|
| `xml_path:` on a field | `path:` | unchanged |
| rows are inferred | `row:` | unchanged, except for tables whose rows were split |
| `levels:` | `links:` | unchanged with `index_of:`; key columns replace the position columns if you use `parent:` |
| `Utf8` whitespace kept; a missing non-nullable `Utf8` value is `""` | trimmed; an error | unchanged while a field states the version 1 behavior |

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

The deprecation notice from `parser.warnings()` lists everything version 2
needs.

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

## Step 1: convert

`Config::to_version_2` writes the config in version 2 without changing its
output: every document parses to the same tables, columns, values and errors
under the converted config as under the original. Where a part can only be
written by changing the output, it leaves that part as it was and says why.

From a clone of this repository:

```bash
cargo run --example convert_config -- before.yaml > after.yaml
```

For the example config, everything converts except the header, whose rows end
at two child elements:

```text
left for you: table 'header': its rows end at 2 child elements of /report/header (title, created), and no `row:` keeps that; `row: "."` gives one row per <header>, which changes the row count
1 part(s) left: the config declares version: 2, and does not load until they are resolved.
```

The converted config always declares `version: 2`. It loads once you resolve
what is left, in [step 2](#step-2-resolve-what-is-left).

What the converter writes:

- `version: 2` at the top.
- `path:` in place of each field's `xml_path:`, with the same absolute value.
- `row:` on each table whose rows end at one child element. A row already ended
  there, so the output does not change.
- An `index_of:` link with `name:` for each `levels` entry, keeping every
  column, and `links: []` on nested tables without `levels`.
- `trim: false` on each `Utf8` field, and `on_missing: empty` on each
  non-nullable one, so that version 2's defaults change no value. Remove them
  where you want the version 2 behavior, in
  [step 4](#step-4-adopt-the-version-2-value-defaults).

It never makes a change to the output for you: it does not choose a `row:` for
a table whose rows are split, and it does not replace position columns with
`parent:` join keys. It also leaves four things that version 2 rejects, where
the fix can change the output and the choice is yours:

- an unknown key, usually a misspelling. Version 1 ignores it, so removing it
  changes nothing, but correcting it can. The written file does not keep it.
- a path with a `.` or `..` segment. No element matches it, so writing it
  correctly changes the output.
- a field inside the `xml_path` of a table nested inside its own. That table
  captures every value there, so the column has always been empty.
- a field outside its table's row element. Its value attaches to whichever row
  ends next.

The written file is fresh. The original's comments and layout are not kept,
and keys left at their defaults are left out.

In Rust:

```rust
let conversion = Config::from_yaml_file("before.yaml")?.to_version_2()?;
for part in &conversion.unconverted {
    eprintln!("left for you: {part}");
}
conversion.config.to_yaml_file("after.yaml")?;
```

### Converting by hand

A config with only some of these changes does not load, so make them in one
edit, and check the output once they are all made:

1. Add `version: 2` at the top.
2. On every field, rename `xml_path:` to `path:` and keep the value. Tables keep
   their `xml_path:`.
3. On every table, declare `row:`: the name of the repeating element, or `"."`
   when the `xml_path` element itself is the row, as for a header.
   [`row`](configuration.md#row) lists every spelling. For a table that
   `parser.warnings()` does not report as `InferredRowBoundary`, name its one
   configured child element, and the output does not change.
4. Replace `levels:` with `links:`. Each entry becomes an `index_of:` link that
   names the row element of the table the entry counts, with `name:` keeping
   the column name. Reading the entries from the outside in, each one counts the
   rows of one enclosing table; when there is one entry more than there are
   enclosing tables, the last one counts the table's own rows. (The full rule is
   under [`levels`](configuration-v1.md#levels).) A nested table always needs
   `links:`, so give one that had no `levels:` `links: []`.
5. On every `Utf8` field, set `trim: false`, and on every non-nullable one,
   `on_missing: empty`.

| `levels:` entry | Replace it with | Output |
|---|---|---|
| none: `levels: []`, or no `levels:` | nothing, or `links: []` if the table is nested | unchanged |
| counts an enclosing table's rows | `index_of:` that table's row element, with `name:` | unchanged |
| counts the table's own rows | `index_of:` the table's own row element, with `name:` | unchanged |

For the example's `stations` and `readings`:

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

Loading the config checks that nothing from version 1 is left, and an error
names anything that is.

## Step 2: resolve what is left

Each part the converter left is a decision about the output. For the example,
declare `row: "."` on `header`:

```yaml
  - name: header
    xml_path: /report/header
    row: "."        # one row per <header>
```

The config now loads, and `config_diff` against `before.yaml` shows the one
change:

```text
  ~ header:
      rows: 2 -> 1
  = readings: unchanged (3 rows)
  = stations: unchanged (2 rows)

Configs differ on this document.
```

That is the fix. `header` had two rows holding one value each, and now has one
row holding both. Update anything downstream that worked around the split rows.

The other parts the converter can leave:

- **An unknown key.** Correct its spelling, or remove it.
- **A path with a `.` or `..` segment.** Write it without one.
- **A field inside a nested table.** Declare it on that table, or remove it.
- **A field outside its table's row element.** Give the value a table of its
  own, or point the field inside the row. A value on the `xml_path` element
  itself, such as an attribute of `<data>` around `<item>` rows, has no version
  2 spelling; see [`path`](configuration.md#path).
- **`levels:` it could not replace.** Replace them with `links:` as described
  under [Converting by hand](#converting-by-hand). The converter leaves the
  `levels:` of a table whose row it could not declare, and of a table that
  counts that table's rows, because the links name the row element you choose.

## Step 3: use join keys

The converted links keep the version 1 position columns, and their weakness:
positions start again in every `<stations>` element, so once that element
repeats, a join on `<station>` pairs readings with the wrong station. A
`parent:` link gives a key instead:

```yaml
  - name: stations
    xml_path: /report/stations
    row: station

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
`<stations>` repeats; see [`index_of:`](configuration.md#index_of). The keys
are `UInt64`. To keep the column names from version 1, set
`row_id: "<station>"` on `stations` and `name: "<station>"` on the link.

With `row:` declared, fields can also use paths relative to the row element,
such as `path: "@id"` instead of `path: /report/stations/station/@id`. That does
not change the output.

## Step 4: adopt the version 2 value defaults

The converter kept the version 1 value handling on every `Utf8` field. Version
2's defaults differ:

| | Version 1 | Version 2 |
|---|---|---|
| Whitespace around a `Utf8` value | kept | trimmed |
| A missing value in a non-nullable `Utf8` column | `""` | an error |

Remove `trim: false` from a field to trim its value, and `on_missing: empty` to
make a missing value an error. In the example, one station name has whitespace
around it:

```yaml
      # before
      - {name: name, path: name, data_type: Utf8, trim: false, on_missing: empty}
      # after
      - {name: name, path: name, data_type: Utf8, on_missing: empty}
```

```text
  = header: unchanged (1 rows)
  = readings: unchanged (3 rows)
  ~ stations:
      values changed:  name: 1 of 2 rows, e.g. row 0: " North " -> "North"

Configs differ on this document.
```

Keep `trim: false` on individual fields rather than moving it to `defaults:`:
there it would also stop numbers from being trimmed, and a value like `" 42 "`
would fail to parse.

A missing value in a non-nullable `Utf8` column without `on_missing: empty`
fails the parse, which `config_diff` reports as an error rather than a
difference. Test with documents where values can be absent. Where that is
legitimate, set `nullable: true`, or keep `on_missing: empty`.

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

A config built in code is version 1 or version 2 as a whole too:

| YAML | Rust |
|---|---|
| `version: 2` | `ConfigBuilder::version(2)` |
| `row:` | `TableConfigBuilder::row` |
| `links:` | `TableConfigBuilder::links`, with `Link` values |
| `path:` | `FieldConfigBuilder::new(name, path, data_type)`, which `ConfigBuilder::build` spells `path` in a version 2 config |
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

To drive a migration from code, `Config::to_version_2` converts a config, and
`Lint::ConfigVersion1 { steps }` lists what version 2 needs as `MigrationStep`
values.
