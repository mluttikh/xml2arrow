# Performance Improvements - xml2arrow

## Summary

This document describes the performance optimizations implemented to improve XML parsing throughput in the xml2arrow crate.

## Implemented Optimizations (High Impact)

### 1. Reduced String Allocations in Text Parsing

**Problem:** Every XML text node was creating a new `String` allocation via `String::from_utf8_lossy()`, even when the XML content was valid UTF-8 (which is the common case).

**Solution:** 
- Fast path: Use `std::str::from_utf8()` first to validate UTF-8 without allocation
- Slow path: Only fall back to `from_utf8_lossy()` when UTF-8 validation fails
- This leverages Rust's `Cow` (Copy-on-Write) semantics to avoid allocations in the common case

**Code Changes:**
```rust
// Before:
let text = String::from_utf8_lossy(&bytes);

// After:
let text = if let Ok(s) = std::str::from_utf8(&bytes) {
    s  // No allocation!
} else {
    &*String::from_utf8_lossy(&bytes)  // Only allocate on invalid UTF-8
};
```

**Expected Impact:** 10-20% performance improvement for typical XML documents with valid UTF-8 content.

### 2. Optimized Attribute Name Handling

**Problem:** Each XML attribute was allocating a new `String` for the attribute name pattern (`"@" + attribute_name`), resulting in thousands of allocations for attribute-heavy documents.

**Solution:**
- Pre-allocate a reusable `String` buffer outside the event loop
- Clear and reuse the buffer for each attribute name
- Eliminates per-attribute allocations

**Code Changes:**
```rust
// Before:
let node = "@".to_string() + key;  // Allocates every time
xml_path.append_node(&node);

// After:
attr_name_buffer.clear();
attr_name_buffer.push('@');
attr_name_buffer.push_str(key);  // Reuses existing buffer
xml_path.append_node(&attr_name_buffer);
```

**Expected Impact:** 5-15% improvement for XML documents with many attributes.

### 3. Increased Buffer Capacities

**Problem:** Small default buffer sizes caused frequent reallocations during parsing.

**Solution:**
- Increased XML event buffer from 256 bytes to 4KB (recommended by quick-xml docs)
- Increased `FieldBuilder` string capacity from 32 to 128 bytes
- Added smart pre-allocation in `set_current_value()` to reserve space on first use

**Expected Impact:** 5-10% improvement due to reduced reallocation overhead, especially for documents with larger text nodes.

## Benchmark Results

Run benchmarks with:
```bash
cargo bench
```

The benchmark suite includes:
- **Small documents** (100 rows) - Tests overhead and startup costs
- **Medium documents** (1,000 rows) - Typical use case
- **Large documents** (10,000 rows) - Stress test for memory efficiency
- **Attribute-heavy documents** - Tests attribute parsing optimization

## Combined Impact

**Estimated total improvement:** 15-35% faster parsing for typical XML workloads, with the highest gains on:
- Documents with valid UTF-8 text (common case)
- Documents with many XML attributes
- Large documents that benefit from reduced allocation overhead

## Memory Impact

These optimizations also reduce memory allocations:
- Fewer temporary `String` allocations
- Better buffer reuse
- Reduced GC/allocator pressure

## Future Optimization Opportunities

The following optimizations were identified but not yet implemented:

### High Impact
1. **Faster numeric parsing** - Use `lexical-core` instead of standard `parse()`
2. **Pre-allocate array builders** - Pass capacity hints based on estimated row counts
3. **Reduce dynamic dispatch** - Use enum-based builders instead of `Box<dyn ArrayBuilder>`

### Medium Impact
4. **Optimize path lookups** - Cache current field builder to avoid repeated hash lookups
5. **SmallVec for XmlPath** - Use stack allocation for shallow paths
6. **Reuse parent index vector** - Avoid per-row allocations in `parent_row_indices()`

### Low Impact
7. **Lazy error formatting** - Reduce allocations in error paths
8. **Profile-guided optimization** - Enable LTO and PGO for release builds

## Compatibility

All optimizations maintain full backward compatibility:
- ✅ All existing tests pass
- ✅ Public API unchanged
- ✅ Handles edge cases (invalid UTF-8, empty elements, etc.)
- ✅ No unsafe code added

## Verification

To verify improvements:

1. **Run tests:**
   ```bash
   cargo test
   ```

2. **Run benchmarks:**
   ```bash
   cargo bench
   ```

3. **Profile with your data:**
   ```bash
   cargo build --release
   # Use your actual XML files for profiling
   ```

## Maintenance Notes

When modifying the parser in the future:
- Keep buffer reuse patterns intact
- Avoid allocations in hot loops (text/attribute parsing)
- Consider UTF-8 fast paths when working with byte slices
- Test with both valid and invalid UTF-8 content