# Migrating to xml2arrow 0.20

0.20 is the one release before 1.0 that asks for source changes. They are
mechanical, and they are what lets every later release add configuration keys,
parser options and data types without breaking your build.

If you load configs from YAML, or build them with `TableConfig::new` and
`FieldConfigBuilder`, and you do not `match` on `DType`, there is nothing to do.

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
