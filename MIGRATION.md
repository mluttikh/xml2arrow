# Migrating to xml2arrow 0.20

0.20 is the one release before 1.0 that asks for source changes. They are
mechanical, and they are what lets every later release add configuration keys,
parser options and data types without breaking your build.

1. [Required](#required-mechanical-source-changes): source changes, because
   five public types became `#[non_exhaustive]` and two error variants gained
   a field.
2. [Behavioral](#behavioral-changes): value errors end with document
   coordinates.
3. [Deprecated](#deprecated-still-working): what still works, and what
   replaces it.
4. [New](#new-no-action-needed): additions that need no action.

If you load configs from YAML, or build them with `TableConfig::new` and
`FieldConfigBuilder`, and you do not `match` on `DType` or destructure
`Error::ParseError` or `Error::MissingRequiredField` without `..`, the required
work is nothing.

## Required: mechanical source changes

`Config`, `TableConfig`, `FieldConfig`, `ParserOptions` and `DType` are now
`#[non_exhaustive]`. Struct-literal construction and exhaustive `match`es on
`DType` no longer compile from outside the crate. Reading and mutating the
public fields is unaffected.

### Config

```rust
// before
let config = Config { tables, parser_options: ParserOptions::default() };

// after
let config = Config::builder().tables(tables).build()?;
```

`ConfigBuilder::build` validates the configuration, as `Config::from_yaml_file`
does, so it returns a `Result`.

### TableConfig

```rust
// before
let table = TableConfig { name, xml_path, levels, fields };

// after: either form
let table = TableConfig::new("items", "/data", levels, fields);
let table = TableConfig::builder("items", "/data").fields(fields).build();
```

`TableConfig::new` is unchanged. The builder has no method for `levels`; use
`TableConfig::new` for a table that needs them, or set `table.levels` on the
built value.

### xml_path is an Option

`TableConfig::xml_path` and `FieldConfig::xml_path` changed from `String` to
`Option<String>`. Nothing about the YAML changes, and the value is always
`Some` for a configuration that passed validation. The type changes now, in
the one release that breaks builds, so that a later configuration format can
spell these locations with another key without breaking them again.

Code that reads the path uses the new accessors, which return `&str` and will
keep working whichever key a configuration uses:

```rust
// before
let table_path: &str = &table.xml_path;
let field_path: &str = &field.xml_path;

// after
let table_path: &str = table.scope();
let field_path: &str = field.path();
```

Code that assigns the field wraps the value: `table.xml_path = Some(path)`.

A YAML table or field that leaves `xml_path` out used to fail while
deserializing, with serde's "missing field" message. It now deserializes and
fails validation instead, with `ConfigIssue::TablePathMissing` or
`ConfigIssue::FieldPathMissing`. `Config::from_yaml_file`,
`Config::from_yaml_str` and `Parser::new` all validate, so such a config is
still rejected before any parsing.

### ConfigIssue variants renamed

Three variants lose the `Xml` in their name, for the same reason. Their
messages are unchanged.

| 0.19 | 0.20 |
|---|---|
| `ConfigIssue::DuplicateTableXmlPath { xml_path, .. }` | `ConfigIssue::DuplicateTablePath { path, .. }` |
| `ConfigIssue::EmptyTableXmlPath` | `ConfigIssue::EmptyTablePath` |
| `ConfigIssue::EmptyFieldXmlPath` | `ConfigIssue::EmptyFieldPath` |

### FieldConfig

Use `FieldConfigBuilder`, which is unchanged:

```rust
let field = FieldConfigBuilder::new("value", "/data/item/value", DType::Float64)
    .nullable(true)
    .build()?;
```

### ParserOptions

There is no builder. Start from the default and set what you need:

```rust
// before
let options = ParserOptions { trim_text: true, ..Default::default() };

// after
let mut options = ParserOptions::default();
options.trim_text = true;
```

### DType

An exhaustive `match` on `DType` outside the crate needs a wildcard arm:

```rust
match dtype {
    DType::Int32 => ...,
    DType::Utf8 => ...,
    _ => ...,
}
```

### Error::ParseError and Error::MissingRequiredField

Both variants gained a `location` field. A pattern that names every field needs
`..`, and code that constructs either variant, usually a test, supplies
`location: Box::default()`:

```rust
// before
Error::MissingRequiredField { field, path } => ...

// after
Error::MissingRequiredField { field, path, .. } => ...
```

## Behavioral changes

### Value errors end with document coordinates

`ParseError`, `MissingRequiredField` and the new `ValueTooLarge` append the row
index and byte offset of the failure:

```text
Missing value for non-nullable field 'v' at path /data/item/v (row index 1, near byte offset 46)
```

The text before the suffix is unchanged, so a prefix or substring match still
works. A test that compares the whole message needs loosening, or can read the
structured fields instead: `location.row` and `location.position` on the error.
The row index is the position the failing row would have had in its table.

### Truncated documents are rejected

A document that ends while elements are still open used to parse to `Ok` with
the rows that had closed before the cut, so a killed writer or a short read
produced a result that looked complete and was not. It now fails:

```text
Truncated document: input ended with 2 element(s) still open; the rows parsed so far are incomplete and were discarded (set parser_options.allow_truncated_input to accept them)
```

The error is `Error::TruncatedInput { open_elements }`. A well-formed document
is unaffected, and so is an early stop through `stop_at_paths`.

If you read partial documents on purpose, restore the previous result with:

```yaml
parser_options:
  allow_truncated_input: true
```

When streaming, the error arrives at the end of the input, after earlier
batches were already yielded. Treat an `Err` from a batch stream as
invalidating everything the stream produced.

## Deprecated, still working

These keep working, unchanged, until 1.0.

| Deprecated | Replacement |
|---|---|
| `parse_xml(reader, &config)` | `Parser::new(&config)?.parse(reader)` |
| `parse_xml_slice(xml, &config)` | `Parser::new(&config)?.parse_slice(xml)` |
| `parser.parse_streaming(reader, options, sink)` | `for item in parser.parse_batches(reader, options)` |

The free functions build a `Parser` on every call, so each one pays the
config validation and path compilation again. Build a `Parser` once and reuse
it for every document that shares the config.

## New, no action needed

- `parser_options.error_on_unmatched_fields` (default off) fails the parse
  with `Error::UnmatchedFields` when a configured field captured nothing
  anywhere in the document, listing every such field. A misspelled `xml_path`
  otherwise shows up as a silently empty column.
- `parser_options.max_value_bytes` (default unlimited) bounds what one field
  value may accumulate across text, CDATA and entity events, and raises
  `Error::ValueTooLarge` beyond it.
- `Config::builder()` and `TableConfig::builder()`.
- `Config::lint()` and `Parser::warnings()` return advisory findings about a
  configuration that is valid but cannot do what it looks like it does, such as
  a table that can never produce a row. The library never prints them, and
  they never change how a document parses.
