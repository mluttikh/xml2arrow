# Configuration reference

This is the reference for **configuration format version 2**: a YAML file that
starts with `version: 2`. Write new configs in this format.

> A config without a `version:` line uses configuration format version 1. It is
> deprecated, but works until 1.0, and is documented in
> [Configuration format version 1](configuration-v1.md). To move a config from
> version 1 to version 2, follow
> [Migrating to configuration format version 2](migrating-to-version-2.md).

The same YAML configures the Rust crate and the
[Python package](https://github.com/mluttikh/xml2arrow-python).

- [A first config](#a-first-config)
- [Top-level keys](#top-level-keys)
- [Tables](#tables)
- [Fields](#fields)
- [Values: missing, invalid and repeated](#values-missing-invalid-and-repeated)
- [Linking nested tables](#linking-nested-tables)
- [Parser options](#parser-options)
- [Checking a config](#checking-a-config)
- [Complete schema](#complete-schema)

## A first config

A document with stations, each holding readings:

```xml
<report>
  <stations>
    <station id="S1">
      <name>North</name>
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
version: 2
tables:
  - name: stations
    xml_path: /report/stations      # the element that contains the rows
    row: station                    # one row per <station>
    fields:
      - {name: id,   path: "@id", data_type: Utf8}
      - {name: name, path: name,  data_type: Utf8}

  - name: readings
    xml_path: /report/stations/station/readings
    row: reading
    links:
      - parent: stations            # which station each reading belongs to
    fields:
      - {name: time,  path: "@time", data_type: Utf8}
      - {name: value, path: value,   data_type: Float64}
```

```text
stations                     readings
+-----+----+-------+         +--------------+-------+-------+
| _id | id | name  |         | _stations_id | time  | value |
+-----+----+-------+         +--------------+-------+-------+
| 0   | S1 | North |         | 0            | 08:00 | 1.5   |
| 1   | S2 | South |         | 0            | 09:00 | 2.5   |
+-----+----+-------+         | 1            | 08:00 | 3.0   |
                             +--------------+-------+-------+
```

Each table says where its rows are (`xml_path` and `row`), each field says where
its value is (`path`, relative to the row), and a table inside another table
says how the two relate (`links`). Here `readings._stations_id` joins to
`stations._id`.

## Top-level keys

| Key | Required | Meaning |
|---|---|---|
| `version` | yes | `2`. Without it, the config is read as [configuration format version 1](configuration-v1.md). |
| `tables` | yes | The tables to extract. See [Tables](#tables). |
| `defaults` | no | Value policies for every field that does not set its own. See [Values](#values-missing-invalid-and-repeated). |
| `parser_options` | no | How the document is read. See [Parser options](#parser-options). |

`version: 2` is checked, not just recorded. A config that declares it but still
uses a version 1 construct is rejected when it is loaded, with a message that
names the table or field. Any version other than `1` or `2` is rejected too.

## Tables

| Key | Required | Meaning |
|---|---|---|
| `name` | yes | The table's name in the output, and how links refer to it. |
| `xml_path` | yes | The absolute path of the element that contains the rows. `/` is the document itself. |
| `row` | yes | The element that makes one row. See [`row`](#row). |
| `links` | when nested | How the table relates to the table it sits inside. See [Linking nested tables](#linking-nested-tables). |
| `row_id` | no | This table's own key column. See [Column names](#column-names). |
| `fields` | yes | The columns. See [Fields](#fields). |

A table with an empty `fields: []` list produces no output.

### `row`

A row ends when its row element closes, and every field of the row takes its
value from inside that element.

| `row:` | Resolves to | Typical use |
|---|---|---|
| `station` | `<xml_path>/station` | one row per repeating child element |
| `group/station` | `<xml_path>/group/station` | a repeating element further down |
| `"."` | the `xml_path` element itself | one row per occurrence of `xml_path`, such as a header section |
| `/report/stations/station` | as written | absolute |

**A leading slash is what makes a path absolute.** `report/stations/station` is
relative. The same rule applies to a field's `path`.

These are checked when the config is loaded:

- `row` must resolve to `xml_path` or to an element below it.
- No other table may sit between a table's `xml_path` and its row element. The
  inner table would take the rows.
- A table with `xml_path: /` cannot use `row: "."`, because the document itself
  never closes. Name the document element instead, as in `row: report`.

## Fields

| Key | Required | Meaning |
|---|---|---|
| `name` | yes | The column name. |
| `path` | yes | Where the value is. See [`path`](#path). |
| `data_type` | yes | The Arrow type. See [Data types](#data-types). |
| `nullable` | no | Whether the column may hold nulls. Default `false`. |
| `scale` | no | Multiply the value by this number. `Float32` and `Float64` only. |
| `offset` | no | Add this number after scaling: `value * scale + offset`. `Float32` and `Float64` only. |
| `trim`, `on_missing`, `on_invalid`, `on_repeat`, `null_values` | no | Value policies. See [Values](#values-missing-invalid-and-repeated). |

### `path`

| `path:` | Resolves to |
|---|---|
| `value` | `<row>/value` |
| `location/latitude` | `<row>/location/latitude` |
| `"@id"` | the `id` attribute of the row element |
| `sensor/@unit` | the `unit` attribute of `<row>/sensor` |
| `/report/header/title` | as written: a leading slash makes it absolute |

Quote a path that starts with `@`; YAML reserves the character. Error messages
show the resolved, absolute path, which is what you need to find the value in
the document.

A field whose absolute path lies outside its row element is allowed, but rarely
what you want: its value attaches to whichever row ends next, and
[`Config::lint()`](#checking-a-config) reports it. Give values from elsewhere in
the document, such as a header, a table of their own.

By default namespace prefixes are ignored when matching, so `<ns:station>`
matches `station`. See `strip_namespaces` under [Parser options](#parser-options).

### Data types

`Boolean`, `Int8`, `UInt8`, `Int16`, `UInt16`, `Int32`, `UInt32`, `Int64`,
`UInt64`, `Float32`, `Float64`, `Utf8`.

- `Boolean` accepts `true`, `false`, `1`, `0`, `yes`, `no`, `on`, `off`, `t`,
  `f`, `y` and `n`, in any letter case.
- A number must be the whole value. `30 units`, or `1.5` in an integer column,
  is invalid rather than truncated, and so is a value outside the type's range.
  An invalid value is an error unless `on_invalid` says otherwise.
- Character and entity references (`&amp;`, `&#66;`) are decoded. In attribute
  values, tabs and line breaks also become spaces, as XML requires.

## Values: missing, invalid and repeated

Every value goes through the same steps:

1. **`trim`** removes the whitespace around it.
2. The value is **missing** when its element or attribute does not occur in the
   row, when the element has no text (`<v/>`), or when it equals one of
   **`null_values`**. A number or boolean that is empty after trimming is also
   missing, since it has no empty value; `Utf8` text that is empty after
   trimming is the empty string. **`on_missing`** decides what a missing value
   becomes.
3. Otherwise the value is parsed as the field's `data_type`. **`on_invalid`**
   decides what happens when that fails.
4. When a row holds the element more than once with a value, **`on_repeat`**
   decides which value is kept.

| Policy | Values | Default |
|---|---|---|
| `trim` | `true`, `false` | `true` |
| `on_missing` | `error`, `null`, `empty` | `null` if the field is `nullable`, otherwise `error` |
| `on_invalid` | `error`, `null` | `error` |
| `on_repeat` | `error`, `first`, `last` | `error` |
| `null_values` | a list of strings | none |

- `null` needs `nullable: true`, and `empty` (the empty string) applies only to
  `Utf8`. A policy that cannot apply to a field is rejected when the config is
  loaded, not ignored.
- `on_missing: null` can be written without quotes.
- `null_values` are compared after trimming, and are case-sensitive. For
  `Utf8`, `null_values: [""]` makes blank text missing.
- `on_repeat: first` keeps the whole first value, even when its text arrives in
  pieces; `last` keeps the final occurrence. An occurrence without a value, such
  as the `<v/>` in `<v/><v>2</v>`, does not count as a repeat.

A top-level `defaults:` block sets policies for every field that does not set
them itself:

```yaml
version: 2
defaults:
  on_repeat: first
  null_values: ["N/A"]
tables:
  - name: readings
    xml_path: /report/readings
    row: reading
    fields:
      - {name: value,   path: value,   data_type: Float64, nullable: true}
      - {name: quality, path: quality, data_type: Int32,   nullable: true, null_values: ["-1"]}
```

A field's own setting replaces the default; `quality` above treats only `-1` as
missing. Defaults are checked against every field they reach, so
`defaults: {on_invalid: null}` requires every field to be nullable.

## Linking nested tables

A table is **nested** when its rows sit inside another table's rows, like
`readings` inside `stations` in the [first config](#a-first-config). A nested
table must declare `links:`, which says how its rows relate to the rows around
them. A table that is not nested needs no `links:`.

| Link | Adds | Use it for |
|---|---|---|
| `parent: <table>` | a `UInt64` key column, and `_id` on the parent | joining rows to their parent rows |
| `index_of: <path>` | a `UInt32` position column | the position of the enclosing row within its scope |

A table may declare several links. `links: []` declares that a nested table
deliberately has none.

### `parent:`

```yaml
    links:
      - parent: stations
```

`parent:` adds a column `_stations_id` to this table, and a column `_id` to
`stations`. `readings._stations_id == stations._id` joins each reading to its
station.

The values are **global** row numbers: a parent row's 0-based position among all
of that table's rows in the document. The join is correct however often the
surrounding elements repeat, and however the output was split into batches.

- The parent table's rows must contain this table's rows. This is checked when
  the config is loaded.
- When streaming with `parse_batches`, a parent row is complete only after its
  children, so a batch of children can refer to a parent row that arrives in a
  later batch. The keys are still correct; join once both sides are collected.

### `index_of:`

```yaml
    links:
      - index_of: /report/group/station
```

`index_of:` adds a column named after the element, here `station_idx`, holding
the 0-based position of the enclosing `<station>` among the rows of its table.
The path must be the absolute path of an enclosing table's row element.

The position restarts at 0 in every occurrence of that table's `xml_path`, so
**it is not a join key** when that element repeats:

```xml
<report>
  <group><station><id>A</id><ms><m>1</m><m>2</m></ms></station></group>
  <group><station><id>B</id><ms><m>3</m></ms></station></group>
</report>
```

| `<m>` | `parent: stations` | `index_of: /report/group/station` |
|---|---|---|
| 1 | 0 | 0 |
| 2 | 0 | 0 |
| 3 | **1** | **0** |

Both stations are the first in their `<group>`, so a join on the `index_of:`
column assigns B's measurement to A. Use `index_of:` when the position itself is
what you need, and `parent:` for joins.

### Column names

| Column | Default name | Set it with |
|---|---|---|
| a `parent:` link | `_<table>_id` | `name:` on the link |
| an `index_of:` link | `<element>_idx` | `name:` on the link |
| the key of a table that a `parent:` link refers to | `_id` | `row_id:` on that table |

```yaml
  - name: stations
    row_id: station_id        # instead of _id
    ...
  - name: readings
    links:
      - parent: stations
        name: station_id      # instead of _stations_id
```

`row_id:` also accepts `true`, to add `_id` to a table that nothing refers to,
and `false`, to leave the key out even when something does.

The default names contain the table's `name` exactly as written, so a table
whose name is not a plain identifier produces a column name that is not one
either. Set both names when that matters downstream. Two columns with the same
name, from two links or a link and a field, are rejected when the config is
loaded.

## Parser options

| Option | Default | Effect |
|---|---|---|
| `trim_text` | `false` | Trim element text as the document is read, before value policies run; text that is only whitespace then counts as absent. Does not apply to attributes. |
| `stop_at_paths` | `[]` | Stop reading after the closing tag of any of these paths, for example to read only the header of a large file. |
| `strip_namespaces` | `true` | Ignore namespace prefixes when matching paths. Set `false` to match names as written, which is about 4–7% faster; paths must then include any prefixes. |
| `allow_truncated_input` | `false` | Accept a document that ends while elements are still open. By default that is an error, not a silently short result. Meant for recovery tools. |
| `error_on_unmatched_fields` | `false` | Fail when a field matched nothing in the document, which usually means a misspelled path. Fields below a `stop_at_paths` path never match, so the two options conflict. |
| `max_value_bytes` | no limit | The most bytes a single value may accumulate. A larger value is an error. |
| `validate_closing_tags` | `true` | Reject a closing tag that does not match its opening tag. Setting `false` is about 2–6% faster, but malformed input can then produce wrong rows. Only for trusted input. |
| `validate_attributes` | `true` | Reject an element that repeats an attribute name. Setting `false` is faster on attribute-heavy documents, but a repeated attribute's values are then concatenated. Only for trusted input. |

## Checking a config

Loading a config (`Config::from_yaml_file`, `Config::from_yaml_str`) and
creating a parser both validate it. A config that cannot work is rejected with
a message that names the table or field.

`Config::lint()` and `parser.warnings()` report the next tier: configs that are
valid but likely to surprise. The library never prints them, and they never
change how a document is parsed. For a version 2 config they report:

| Lint | When |
|---|---|
| `FieldOutsideRow` | a field's path lies outside its table's row element |
| `StructuralTable` | a table has no fields, so it is left out of the output |

Version 1 configs get more lints, described in
[Configuration format version 1](configuration-v1.md#checking-a-version-1-config).

## Complete schema

```yaml
version: 2
defaults:                           # optional: value policies for every field
  trim: <bool>
  on_missing: <error|null|empty>
  on_invalid: <error|null>
  on_repeat: <error|first|last>
  null_values: [<string>, ...]
parser_options:                     # optional
  trim_text: <bool>                 # default false
  stop_at_paths: [<path>, ...]      # default []
  strip_namespaces: <bool>          # default true
  allow_truncated_input: <bool>     # default false
  error_on_unmatched_fields: <bool> # default false
  max_value_bytes: <number>         # default: no limit
  validate_closing_tags: <bool>     # default true
  validate_attributes: <bool>       # default true
tables:
  - name: <string>
    xml_path: <absolute path>       # the element that contains the rows
    row: <path>                     # ".", relative to xml_path, or absolute
    links:                          # required when nested; [] for no link
      - parent: <table name>        # UInt64 join key
        name: <column>              # default _<table>_id
      - index_of: <absolute path>   # UInt32 position; an enclosing table's row element
        name: <column>              # default <element>_idx
    row_id: <column|true|false>     # default: _id when a parent: link refers to this table
    fields:
      - name: <column>
        path: <path>                # relative to the row element, or absolute
        data_type: <type>
        nullable: <bool>            # default false
        scale: <number>             # Float32 and Float64 only
        offset: <number>            # Float32 and Float64 only
        trim: <bool>                # policies: see "Values"
        on_missing: <error|null|empty>
        on_invalid: <error|null>
        on_repeat: <error|first|last>
        null_values: [<string>, ...]
```
