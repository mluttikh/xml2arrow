# Path Trie Integration Guide

## Overview

The `path_trie` module provides a high-performance trie (prefix tree) structure that replaces runtime `XmlPath` construction and hash-based lookups with O(1)-amortized state transitions.

## Implementation Summary

### Core Components

1. **PathTrie**: The immutable trie structure built from configuration
2. **PathTrieBuilder**: Builder for constructing the trie
3. **StateId**: Integer identifier for trie nodes (u32)
4. **FieldId/TableId**: Integer identifiers for field and table builders

### Key Features

- **Adaptive child storage**: Automatically chooses optimal container based on fanout:
  - Empty: No children
  - Single: One child (common in linear paths)
  - SmallVec: 2-4 children (stack-allocated)
  - SortedVec: 5-32 children (binary search)
  - Hash: >32 children (FxHashMap)

- **Attribute support**: Attributes stored separately from elements, no path mutation needed

- **Compact flags**: Node properties stored in single byte bitfield

- **Unmatched state handling**: Unknown XML elements handled gracefully with sentinel value

## Integration with xml_parser.rs

### Current Implementation (Hash-based)

```rust
// Current approach in xml_parser.rs
struct XmlToArrowConverter {
    table_builders: IndexMap<XmlPath, TableBuilder, FxBuildHasher>,
    builder_stack: VecDeque<XmlPath>,
}

// On each XML event:
// 1. Mutate XmlPath (append/remove nodes)
// 2. Hash the entire XmlPath
// 3. Look up in IndexMap
// 4. Downcast Box<dyn ArrayBuilder>
```

### Proposed Trie-based Implementation

```rust
// New approach with trie
struct XmlToArrowConverter {
    trie: PathTrie,
    field_builders: Vec<FieldBuilder>,  // indexed by FieldId
    table_builders: Vec<TableBuilder>,  // indexed by TableId
    state_stack: Vec<StateId>,          // current path as state IDs
    table_stack: Vec<TableContext>,     // active table contexts
}

struct TableContext {
    table_id: TableId,
    row_index: u32,
    parent_indices: Vec<u32>,
}

// On each XML event:
// 1. Push/pop StateId (single u32)
// 2. Direct array access by ID
// 3. No hashing, no path reconstruction
```

## Migration Steps

### Phase 1: Parallel Implementation (Feature Flag)

1. Add feature flag to `Cargo.toml`:
```toml
[features]
trie_parser = []
```

2. Create `xml_parser_trie.rs` with new implementation

3. Conditional compilation in `lib.rs`:
```rust
#[cfg(feature = "trie_parser")]
pub use xml_parser_trie::parse_xml;
#[cfg(not(feature = "trie_parser"))]
pub use xml_parser::parse_xml;
```

### Phase 2: Implementation Details

#### Event Handling

**Event::Start**
```rust
Event::Start(e) => {
    let name = Atom::from(std::str::from_utf8(e.local_name().into_inner())?);
    let current_state = *state_stack.last().unwrap_or(&trie.root_id());
    let next_state = trie.transition_element(current_state, &name);
    
    state_stack.push(next_state);
    
    // Check for table root
    if let Some(table_id) = trie.get_table_id(next_state) {
        table_stack.push(TableContext::new(table_id));
    }
    
    // Handle attributes
    if trie.has_attributes(next_state) {
        for attr in e.attributes() {
            let attr = attr?;
            let attr_name = Atom::from(std::str::from_utf8(attr.key.local_name().into_inner())?);
            let attr_state = trie.transition_attribute(next_state, &attr_name);
            
            if let Some(field_id) = trie.get_field_id(attr_state) {
                let value = std::str::from_utf8(attr.value.as_ref())?;
                field_builders[field_id as usize].set_current_value(value);
            }
        }
    }
}
```

**Event::Text / Event::GeneralRef**
```rust
Event::Text(e) => {
    let current_state = *state_stack.last().unwrap();
    if let Some(field_id) = trie.get_field_id(current_state) {
        let text = String::from_utf8_lossy(e.into_inner());
        field_builders[field_id as usize].set_current_value(&text);
    }
}
```

**Event::End**
```rust
Event::End(_) => {
    let ending_state = state_stack.pop().unwrap();
    
    // Check if ending a table
    if trie.is_table_root(ending_state) {
        if let Some(mut ctx) = table_stack.pop() {
            table_builders[ctx.table_id as usize].end_row(&ctx.parent_indices)?;
        }
    }
    
    // Update parent row index if we're still in a table
    if let Some(ctx) = table_stack.last() {
        let parent_state = *state_stack.last().unwrap();
        if trie.is_table_root(parent_state) {
            // Increment row counter for nested tables
        }
    }
}
```

### Phase 3: Builder Restructuring

#### Field Builders
```rust
// Initialize field builders in order from trie
let mut field_builders = Vec::with_capacity(trie.field_configs.len());
for field_config in &trie.field_configs {
    field_builders.push(FieldBuilder::new(field_config)?);
}
```

#### Table Builders
```rust
// Initialize table builders in order from trie
let mut table_builders = Vec::with_capacity(trie.table_configs.len());
for table_config in &trie.table_configs {
    table_builders.push(TableBuilder::new(table_config)?);
}
```

### Phase 4: Testing Strategy

1. **Compatibility Tests**: Ensure trie-based parser produces identical RecordBatches
```rust
#[test]
fn test_trie_parser_compatibility() {
    let xml = "<root>...</root>";
    let config = Config { ... };
    
    let result_old = parse_xml_old(xml.as_bytes(), &config).unwrap();
    let result_new = parse_xml_trie(xml.as_bytes(), &config).unwrap();
    
    assert_eq!(result_old, result_new);
}
```

2. **Property Tests**: Random XML generation with known schema
```rust
#[test]
fn test_trie_random_xml() {
    for _ in 0..1000 {
        let xml = generate_random_xml(&config);
        let result_old = parse_xml_old(&xml, &config).unwrap();
        let result_new = parse_xml_trie(&xml, &config).unwrap();
        assert_eq!(result_old, result_new);
    }
}
```

3. **Benchmark Comparison**
```rust
fn bench_parse_trie(c: &mut Criterion) {
    let xml = generate_realistic_xml(10_000, 5);
    let config = get_config();
    
    c.bench_function("parse_with_trie", |b| {
        b.iter(|| parse_xml_trie(xml.as_bytes(), &config))
    });
}
```

### Phase 5: Performance Validation

Expected improvements:
- **Path matching**: ~15-30% faster (eliminates hashing + XmlPath allocation)
- **Field lookup**: ~10-20% faster (direct array indexing vs HashMap)
- **Memory**: ~20-40% reduction (no runtime path objects)
- **Cache efficiency**: Better (sequential array access vs scattered HashMap nodes)

Measure with:
```bash
# Before
cargo bench --bench parse_benchmark -- --save-baseline before

# After (with trie)
cargo bench --bench parse_benchmark --features trie_parser -- --baseline before
```

### Phase 6: Optimization Opportunities Post-Trie

Once trie is integrated, these become easier:

1. **Typed Field Builders**: Replace `Box<dyn ArrayBuilder>` with enum
```rust
enum TypedFieldBuilder {
    Int32(Int32Builder, FieldId),
    Float64(Float64Builder, FieldId),
    Utf8(StringBuilder, FieldId),
    // ...
}
```

2. **Inline Scale/Offset**: Apply during parse rather than post-processing
```rust
if let Some(scale) = field_config.scale {
    let val: f64 = value.parse()?;
    builder.append_value(val * scale);
}
```

3. **Fast Numeric Parsing**: Use `lexical-core` for hot paths
```rust
use lexical_core::parse;
let val: f64 = parse(value.as_bytes())?;
```

4. **Streaming Batches**: Emit RecordBatches incrementally
```rust
if table_builder.row_count() >= BATCH_SIZE {
    yield table_builder.flush()?;
}
```

## Error Handling

### Invalid Paths
```rust
// Gracefully handled with UNMATCHED_STATE
let state = trie.transition_element(current, &name);
if state == UNMATCHED_STATE {
    // Ignore this branch, continue parsing
    continue;
}
```

### Debug Mode
```rust
#[cfg(debug_assertions)]
{
    if state == UNMATCHED_STATE {
        eprintln!("Warning: unmatched element '{}' at depth {}", 
                  name, state_stack.len());
    }
}
```

## Memory Layout

### Before (Hash-based)
```
XmlPath: Vec<Atom> (24 bytes + heap allocation)
IndexMap entry: (XmlPath, FieldBuilder) (~100 bytes per field)
Total per lookup: ~124 bytes + 2 heap allocations
```

### After (Trie-based)
```
StateId: u32 (4 bytes, stack-allocated)
TrieNode: ~64 bytes (shared, one-time allocation)
Field lookup: Direct array index (cache-friendly)
Total per lookup: 4 bytes stack + 0 allocations
```

## Benchmarking Results (Projected)

Based on similar optimizations in other parsers:

| Metric | Before | After (Trie) | Improvement |
|--------|--------|--------------|-------------|
| Parse 10MB XML | 86.9 ms | ~65-70 ms | 20-25% |
| Parse 200MB XML | 1.71 s | ~1.25-1.35 s | 20-27% |
| Memory (10MB) | ~45 MB | ~32-35 MB | 25-30% |
| Allocations | ~500k | ~150k | 70% reduction |

## Migration Checklist

- [x] Implement PathTrie and PathTrieBuilder
- [x] Add comprehensive tests for trie construction
- [x] Add tests for path parsing (elements and attributes)
- [x] Add tests for child container optimization
- [ ] Create xml_parser_trie.rs with new implementation
- [ ] Add feature flag for conditional compilation
- [ ] Port event handling to use StateId stack
- [ ] Restructure FieldBuilder/TableBuilder indexing
- [ ] Add compatibility tests (old vs new)
- [ ] Add benchmark comparison
- [ ] Validate memory improvements
- [ ] Document performance gains
- [ ] Switch default to trie-based parser
- [ ] Remove old implementation (or keep behind legacy flag)

## Notes

- The trie is built once at startup, so construction cost is amortized
- StateId stack is typically very shallow (< 20 levels), excellent cache locality
- Attribute handling is now O(1) per attribute, not O(path_length)
- Unmatched elements have zero cost (single comparison to UNMATCHED_STATE)
- Type safety: FieldId/TableId prevent accidental mixing of indices

## Future Enhancements

1. **Parallel Table Building**: Once tables are identified by ID, they can be built independently
2. **Streaming Parser**: Emit batches as tables complete (memory-bounded)
3. **Path Statistics**: Track unmatched elements for config debugging
4. **Compressed State IDs**: Use u16 if node count < 65536
5. **SIMD Path Matching**: Vectorize child lookup for high-fanout nodes