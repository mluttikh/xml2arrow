# Configuration format version 1

> **Deprecated.** Configuration format version 1 still works, unchanged, until
> 1.0. Every version 1 config now gets a deprecation notice listing what is left
> to change; see [Checking a version 1 config](#checking-a-version-1-config).
> Write new configs in [version 2](configuration.md), and move existing ones
> with [Migrating to configuration format version 2](migrating-to-version-2.md).

A config is version 1 when it has no `version:` line, or says `version: 1`.
Every config written for xml2arrow 0.19 or earlier is version 1.

- [Version 1 and version 2 compared](#version-1-and-version-2-compared)
- [Example](#example)
- [Tables](#tables)
- [How rows are found](#how-rows-are-found)
- [`levels`](#levels)
- [Fields](#fields)
- [Values](#values)
- [Keys only version 2 has](#keys-only-version-2-has)
- [Checking a version 1 config](#checking-a-version-1-config)
- [Complete schema](#complete-schema)

## Version 1 and version 2 compared

| | Version 1 | Version 2 |
|---|---|---|
| Where a row ends | inferred from the configured fields | declared with `row:` |
| Where a field's value is | `xml_path:`, absolute | `path:`, relative to the row or absolute |
| How nested tables relate | `levels:` position columns | `links:`, with join keys or positions |
| Whitespace around a `Utf8` value | kept | trimmed |
| A missing value in a non-nullable `Utf8` column | `""` | an error, as for every other type |

## Example

```xml
<report>
  <header>
    <title>Station readings</title>
    <created>2026-07-26</created>
  </header>
  <stations>
    <station id="S1">
      <name> North </name>
      <readings>
        <reading time="08:00"><value>1.5</value></reading>
        <reading time="09:00"><value>2.5</value></reading>
      </readings>
    </station>
    <station id="S2">
      <name>South</name>
      <readings>
        <reading time="08:00"><value>3.0</value></reading>
      </readings>
    </station>
  </stations>
</report>
```

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

```text
header                            stations
+------------------+------------+ +-----------+----+---------+
| title            | created    | | <station> | id | name    |
+------------------+------------+ +-----------+----+---------+
| Station readings |            | | 0         | S1 |  North  |
|                  | 2026-07-26 | | 1         | S2 | South   |
+------------------+------------+ +-----------+----+---------+

readings
+-----------+-----------+-------+-------+
| <station> | <reading> | time  | value |
+-----------+-----------+-------+-------+
| 0         | 0         | 08:00 | 1.5   |
| 0         | 1         | 09:00 | 2.5   |
| 1         | 0         | 08:00 | 3.0   |
+-----------+-----------+-------+-------+
```

Three things in this output are specific to version 1:

- `header` has two rows, each holding one value (the blank cells are nulls).
  See [How rows are found](#how-rows-are-found).
- `<station>` and `<reading>` are position columns added by `levels`. See
  [`levels`](#levels).
- `" North "` keeps its whitespace. See [Values](#values).

The [migration guide](migrating-to-version-2.md) takes this config to version 2
step by step.

## Tables

| Key | Required | Meaning |
|---|---|---|
| `name` | yes | The table's name in the output. |
| `xml_path` | yes | The absolute path of the element whose children are rows. `/` is the document itself. |
| `levels` | no | Position columns. See [`levels`](#levels). Required before 0.20. |
| `fields` | yes | The columns. See [Fields](#fields). |

A table with an empty `fields: []` list produces no output. Its rows are still
counted, so it can supply a position to the `levels` of tables inside it.

## How rows are found

A version 1 table does not say what makes a row. Instead, **a row ends whenever
a configured child element of `xml_path` closes.** A child element is configured
when a field's path, or another table's `xml_path`, passes through it. Elements
with nothing configured under them never end a row.

With one configured child element, that gives one row per element: `stations`
has fields only under `<station>`, so every `<station>` is a row.

With several, each of them ends a row. `header` has fields under `<title>` and
under `<created>`, so every `<header>` produces two rows, each holding one value.
Adding a field under another child element adds another row. Nested tables count
as well: a table at `/report` with a field under `<header>`, alongside a table at
`/report/stations`, produces one row when `<header>` closes and another when
`<stations>` does.

`Config::lint()` reports each table whose rows end at more than one child element
(`InferredRowBoundary`), and each table with no child element that can end a
row (`NeverFinalizesRows`): for example, one whose fields are all attributes of
the `xml_path` element itself, which produces no rows at all.

## `levels`

Each entry in `levels` adds a `UInt32` column named after the entry in angle
brackets: `levels: [station, reading]` adds `<station>` and `<reading>`.

The names are only labels. The values come from the tables that the row sits
inside:

- The first column holds the position of the enclosing row of the outermost
  table, the next column that of the next table inward, and so on. A table at
  `xml_path: /` is not counted.
- When there is one entry more than there are enclosing tables, the last column
  holds the row's own position.
- Positions start at 0, and start again in every occurrence of the counting
  table's `xml_path`.

In the example, `readings` sits inside `stations`. Its `<station>` is the
position of the station it belongs to, and its `<reading>` is the reading's own
position within that station. `stations` sits inside no table, so its one entry
is its own position. Joining `readings.<station>` to `stations.<station>` pairs
each reading with its station.

**That join is only correct while the counting table's `xml_path` occurs once.**
Because positions start again in every occurrence, a document with two
`<stations>` elements has a station 0 in each, and the join pairs readings with
the wrong station. [Version 2 join keys](configuration.md#linking-nested-tables)
do not have this problem.

- `levels: []` adds no columns.
- More entries than there are tables to count is reported by `Config::lint()`
  (`ExcessLevels`), and fails as soon as the table produces a row.

## Fields

| Key | Required | Meaning |
|---|---|---|
| `name` | yes | The column name. |
| `xml_path` | yes | The absolute path of the element or attribute that holds the value, such as `/report/stations/station/@id`. |
| `data_type` | yes | The Arrow type. |
| `nullable` | no | Whether the column may hold nulls. Default `false`. |
| `scale` | no | Multiply the value by this number. `Float32` and `Float64` only. |
| `offset` | no | Add this number after scaling: `value * scale + offset`. `Float32` and `Float64` only. |

The data types, the accepted `Boolean` values, number parsing and the decoding
of entities are the same as in [version 2](configuration.md#data-types).

## Values

| | Numbers and booleans | `Utf8` |
|---|---|---|
| Whitespace around the value | removed | kept |
| Text that is only whitespace | missing | kept |
| No text (`<v/>`), or no element | missing | missing |
| Missing, in a `nullable` column | null | null |
| Missing, in a non-nullable column | an error | `""` |

An invalid number and a repeated element are errors, as they are by default in
version 2.

`parser_options.trim_text: true` trims element text for every type as the
document is read, so whitespace-only text becomes no text. `Config::lint()`
reports the non-nullable `Utf8` fields that would produce `""` for a missing
value (`ImplicitEmptyString`).

## Keys only version 2 has

Version 1 is the format xml2arrow 0.19 read, and a config is version 1 or
version 2 as a whole. A version 1 config that sets a key only version 2 has
(`row`, `path`, `links`, `row_id`, `defaults`, `metadata` or a value policy) is
rejected when it is loaded:

```text
The key 'row:' in table 'header' is part of configuration format version 2; declare `version: 2` at the top of the config to use it
```

To use one, move the whole config to version 2. `Config::to_version_2()` does
that without changing the output; see the
[migration guide](migrating-to-version-2.md).

The [parser options](configuration.md#parser-options) are the same in both
versions.

## Checking a version 1 config

`Config::lint()` and `parser.warnings()` report configs that are valid but
likely to surprise. They never change how a document is parsed.

| Lint | When |
|---|---|
| `UnknownKey` | the config sets a key it does not define, usually a misspelling, so the key is ignored |
| `DotSegmentInPath` | a table's `xml_path`, a field's `xml_path` or a `stop_at_paths` entry has a `.` or `..` segment, so it matches nothing |
| `InferredRowBoundary` | a table's rows end at more than one child element |
| `NeverFinalizesRows` | no child element can end a row of a table, so it produces no rows |
| `ExcessLevels` | a table has more `levels` than tables to count |
| `ImplicitEmptyString` | a non-nullable `Utf8` field produces `""` when its value is missing |
| `FieldInsideNestedTable` | a field lies inside the `xml_path` of a table nested inside its own table, which captures every value there, so the field never receives one |
| `StructuralTable` | a table has no fields, so it is left out of the output |
| `ConfigVersion1` | every version 1 config: the deprecation notice |

The deprecation notice lists what `version: 2` would reject, grouped by kind.
For the example config:

```text
This config uses configuration format version 1, which is deprecated. Before
it can declare `version: 2`: 3 tables must declare `row:` rather than infer
their rows (header, stations, readings); 2 tables must replace `levels:` with
`links:` (stations, readings); 6 fields must rename the `xml_path:` key to
`path:` (header.title, header.created, stations.id, …). Declaring `version: 2`
also changes two defaults: `Utf8` values are trimmed, and a missing
non-nullable `Utf8` value is an error rather than ""
```

In Rust, the notice is `Lint::ConfigVersion1 { steps }`, with one
`MigrationStep` per remaining change. A host that fails on any warning sees the
notice for every version 1 config; filter out `Lint::ConfigVersion1` until you
migrate.

## Complete schema

```yaml
parser_options:                    # optional: see the version 2 reference
  trim_text: <bool>
  stop_at_paths: [<path>, ...]
tables:
  - name: <string>
    xml_path: <absolute path>      # the element whose children are rows
    levels: [<label>, ...]         # optional: UInt32 position columns
    fields:
      - name: <column>
        xml_path: <absolute path>  # the element or attribute that holds the value
        data_type: <type>
        nullable: <bool>           # default false
        scale: <number>            # Float32 and Float64 only
        offset: <number>           # Float32 and Float64 only
```
