# AI Agents Guide

This file helps AI assistants work effectively in this repository. Keep changes minimal, focus on correctness and performance, and verify behavior with tests and benchmarks when relevant.

## Project overview

- `xml2arrow` is a Rust crate that converts XML data into Apache Arrow tables.
- XML parsing uses `quick-xml`, and Arrow structures use the Rust Arrow crate (see `README.md`).
- Configuration is driven by YAML mappings that define tables, fields, XML paths, and Arrow data types.

## Repository map

- `src/`: crate implementation
- `tests/`: test suite
- `benches/`: Criterion benchmarks
- `README.md`: usage, YAML mapping, and examples
- `Cargo.toml`: crate metadata and dependencies
- `target/`: build output (do not edit)

## Common commands

```/dev/null/commands.sh#L1-6
# Build
cargo build

# Tests
cargo test

# Benchmarks (Criterion)
cargo bench
```

### Benchmark baselines (Criterion)

```/dev/null/commands.sh#L1-6
# Save a baseline
cargo bench --bench parse_benchmark -- --save-baseline <name>

# Compare to a baseline
cargo bench --bench parse_benchmark -- --baseline <name>
```

## Development guidance for AI agents

- Prefer small, targeted changes. Avoid refactors unless they are required.
- Preserve performance-sensitive behavior; benchmark changes that affect XML parsing or Arrow conversion.
- Update `README.md` if you change public behavior, configuration schema, or examples.
- Add or update tests when fixing bugs or adding features.
- Avoid touching generated artifacts under `target/`.

## YAML configuration notes

- `tables` define record batches and map XML paths to Arrow fields.
- Field definitions include `name`, `xml_path`, `data_type`, and optional `nullable`, `scale`, `offset`.
- `levels` can define nested index levels for hierarchical XML structures.

For details and full examples, see `README.md`.
