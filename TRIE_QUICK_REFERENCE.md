# Trie Parser Quick Reference

**For:** Quick lookups during maintenance  
**See Also:** TRIE_IMPLEMENTATION_GUIDE.md (detailed explanations)

---

## File Overview

| File | Purpose | Lines |
|------|---------|-------|
| `src/path_trie.rs` | Trie data structure | 562 |
| `src/xml_parser_trie.rs` | XML parser using trie | 868 |

---

## Key Data Structures

### PathTrie
```rust
pub struct PathTrie {
    nodes: Vec<TrieNode>,           // All states, indexed by StateId
    field_to_table: Vec<TableId>,   // Maps field → table (CRITICAL!)
    table_configs: Vec<TableConfig>,
}
```

### XmlToArrowConverter
```rust
struct XmlToArrowConverter {
    trie: PathTrie,
    table_builders: Vec<TableBuilder>,
    table_stack: Vec<TableContext>,  // Active tables
    indices_buffer: Vec<u32>,        // CRITICAL: reusable!
}
```

### TableBuilder
```rust
struct TableBuilder {
    index_builders: Vec<UInt32Builder>,  // Level columns
    field_builders: Vec<FieldBuilder>,   // Data columns
    row_index: usize,
}
```

---

## Common Operations

### Navigate Trie
```rust
let mut state = trie.root_id();
state = trie.transition_element(state, &Atom::from("document"));
state = trie.transition_element(state, &Atom::from("data"));
```

### Check State Properties
```rust
if trie.is_table_root(state) { /* ... */ }
if trie.is_level_element(state) { /* ... */ }
if trie.has_attributes(state) { /* ... */ }
let field_id = trie.get_field_id(state);
let table_id = trie.get_table_id(state);
```

### Route Field to Table
```rust
let table_id = trie.get_field_table(field_id).unwrap();
```

---

## Event Handling Pattern

### Start Element
```rust
Event::Start(e) => {
    let atom = Atom::from(element_name);
    let current_state = *state_stack.last().unwrap();
    let next_state = trie.transition_element(current_state, &atom);
    state_stack.push(next_state);  // Always push!
    
    if next_state != UNMATCHED_STATE {
        if let Some(table_id) = trie.get_table_id(next_state) {
            converter.start_table(table_id);
        }
    }
}
```

### Text Content
```rust
Event::Text(e) => {
    let state = *state_stack.last().unwrap();
    if state != UNMATCHED_STATE {
        if let Some(field_id) = trie.get_field_id(state) {
            converter.set_field_value(field_id, &text)?;
        }
    }
}
```

### End Element
```rust
Event::End(_) => {
    let ending_state = state_stack.pop().unwrap();
    
    // Close table if needed
    if ending_state != UNMATCHED_STATE {
        if trie.is_table_root(ending_state) {
            converter.end_table();
        }
    }
    
    // End row if parent is table root
    if let Some(&parent) = state_stack.last() {
        if parent != UNMATCHED_STATE && trie.is_table_root(parent) {
            let table_id = trie.get_table_id(parent).unwrap();
            let config = &trie.table_configs[table_id as usize];
            
            if config.levels.is_empty() && has_any_field_value()
               || trie.is_level_element(ending_state)
            {
                converter.end_row()?;
            }
        }
    }
}
```

---

## Row Ending Logic

### Table with `levels: ["item"]`
```xml
<data>          <!-- start_table -->
  <item>...</item>  <!-- is_level_element → end_row() -->
  <item>...</item>  <!-- is_level_element → end_row() -->
</data>         <!-- end_table -->
```
**Result:** N rows (one per item)

### Table with `levels: []` (root table)
```xml
<document>      <!-- start_table -->
  <header>...</header>  <!-- has_field_values → end_row() -->
  <data>...</data>      <!-- no_field_values → skip -->
</document>     <!-- end_table -->
```
**Result:** 1 row

### Table with `levels: []` (item table)
```xml
<data>          <!-- start_table -->
  <item>...</item>  <!-- has_field_values → end_row() -->
  <item>...</item>  <!-- has_field_values → end_row() -->
</data>         <!-- end_table -->
```
**Result:** N rows

---

## Critical Performance Rules

### ✅ DO
- Reuse buffers (indices_buffer, event buffer)
- Pre-allocate with known capacity
- Check has_attributes() before parsing attributes
- Use const generic for compile-time optimization
- Clear buffers, don't reallocate

### ❌ DON'T
- Allocate in hot path (end_row, set_field_value)
- Use HashMap when fanout < 5
- Skip pushing UNMATCHED_STATE
- Forget to clear field buffers between rows
- Allocate new Vec for indices

---

## Debugging Checklist

### Column Length Mismatch
```rust
// Add in end_row()
println!("Table {}: row {}, {} fields", 
         table_id, row_index, field_builders.len());
for (i, fb) in field_builders.iter().enumerate() {
    println!("  Field {}: has_value={}", i, fb.has_value);
}
```

### Wrong Row Count
```rust
// Add in Event::End
println!("End row? ending={}, parent={:?}, is_level={}, levels={:?}",
         ending_state, parent_state, 
         trie.is_level_element(ending_state),
         table_config.levels);
```

### Field Routing
```rust
// Add in set_field_value()
println!("Field {} → table {}, local_idx {}", 
         field_id, table_id, local_field_idx);
```

### State Transitions
```rust
// Add in Event::Start
println!("  {} → state {} → {}", 
         element_name, current_state, next_state);
```

---

## Common Patterns

### Adding a New Field Type
1. Add variant to `DType` enum
2. Add case in `create_array_builder()`
3. Add case in `append_current_value()`
4. Add case in `append_numeric()` if numeric

### Supporting New Table Feature
1. Add field to `TableConfig`
2. Update `PathTrieBuilder::from_config()` if needed
3. Update `TableBuilder::new()` if needed
4. Update row ending logic if affects boundaries
5. Add tests!

### Optimizing a Hot Path
1. Profile first (cargo flamegraph)
2. Check for allocations (DHAT)
3. Look for reusable buffers
4. Consider const generics for compile-time optimization
5. Benchmark before/after

---

## Test Commands

```bash
# Run all tests
cargo test --features trie_parser

# Run specific test
cargo test --features trie_parser test_trie_parse_simple

# Run benchmarks
cargo bench --bench parse_benchmark --features trie_parser

# Compare with baseline
cargo bench --no-default-features -- --save-baseline baseline
cargo bench --features trie_parser -- --baseline baseline

# Run with profiling
cargo build --release --features trie_parser
valgrind --tool=dhat ./target/release/...
```

---

## Performance Targets

| Workload | Target Throughput | Status |
|----------|-------------------|--------|
| Small (< 1 MB) | > 220 MiB/s | ✅ ~227 MiB/s |
| Medium (~10 MB) | > 240 MiB/s | ✅ ~249 MiB/s |
| Large (~200 MB) | > 230 MiB/s | ✅ ~239 MiB/s |

---

## Quick Fixes

### Issue: "Column lengths don't match"
**Fix:** Check that `end_row()` is called same number of times for all fields

### Issue: "Too many/few rows"
**Fix:** Check row ending logic - verify `is_level_element` flags set correctly

### Issue: "Fields in wrong table"
**Fix:** Verify `field_to_table` mapping in `PathTrieBuilder::insert_field_path()`

### Issue: "Null in non-nullable column"
**Fix:** Field not being set - check path in config matches XML structure

### Issue: "Memory usage high"
**Fix:** Check buffers are cleared (not reallocated) between uses

---

## Architecture Diagram

```
Config → PathTrieBuilder → PathTrie (immutable)
                                ↓
XML → quick_xml → Events → XmlToArrowConverter
                                ↓
                          State Stack (Vec<StateId>)
                          Table Stack (Vec<TableContext>)
                                ↓
                          TableBuilder → FieldBuilder
                                ↓
                          Arrow RecordBatch
```

---

## Key Files to Review

- **Design:** TRIE_IMPLEMENTATION_GUIDE.md
- **Parser:** TRIE_PARSER_GUIDE.md  
- **Benchmarks:** TRIE_BENCHMARK_REPORT.md
- **Status:** TRIE_STATUS.md

---

**Remember:** The trie is fast because it does O(1) state transitions with zero allocations. Keep it that way!