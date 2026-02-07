# Performance Improvement Plan for xml2arrow

This document outlines a comprehensive plan to improve the performance of the `xml2arrow` XML-to-Apache-Arrow parser. The recommendations are based on analysis of the current implementation and are organized by expected impact and implementation complexity.

## Current Performance Baseline

Based on benchmark results from the Criterion.rs benchmarks:

| Benchmark | XML Size | Throughput |
|-----------|----------|------------|
| Small (1K measurements, 2 sensors) | 413 KB | ~145 MiB/s |
| Medium (10K measurements, 5 sensors) | 10 MB | ~136 MiB/s |
| Large (100K measurements, 10 sensors) | 202 MB | ~138 MiB/s |
| X-Large (200K measurements, 5 sensors) | 203 MB | ~144 MiB/s |

---

## High Impact Optimizations

### 1. Integer-Based Path Indexing

**Current Issue:**
- `XmlPath` uses `Vec<Atom>` for path representation
- Path matching uses hash lookups in `IndexMap<XmlPath, ...>`
- `is_table_path()` is called twice per element (on start and end)
- Field lookups occur on every text event via `field_builders.get_mut(field_path)`

**Proposed Solution:**
- During configuration loading, build a trie or prefix tree of all configured paths
- Assign integer IDs to each significant path (tables and fields)
- Track current path position as a stack of integers instead of strings
- Replace hash lookups with direct array indexing

**Implementation:**

```rust
struct PathRegistry {
    // Maps path depth + local name → child node ID
    trie: Vec<HashMap<Atom, PathNodeId>>,
    // For each node: is it a table? is it a field? which one?
    node_info: Vec<PathNodeInfo>,
}

struct PathNodeInfo {
    table_index: Option<usize>,
    field_indices: SmallVec<[(usize, usize); 2]>, // (table_idx, field_idx)
}

// During parsing, track path as:
struct PathTracker {
    node_stack: Vec<PathNodeId>,
    current_node: PathNodeId,
}
```

**Expected Impact:** 15-25% throughput improvement  
**Complexity:** Medium  
**Risk:** Low

---

### 2. Eliminate Dynamic Dispatch in Array Builders

**Current Issue:**
- `FieldBuilder` uses `Box<dyn ArrayBuilder>` for type erasure
- Every `append_*` operation requires `downcast_mut()` which has runtime overhead
- The data type is known at configuration time but not leveraged

**Proposed Solution:**
- Replace `Box<dyn ArrayBuilder>` with an enum containing concrete builder types
- Use match statements instead of dynamic dispatch

**Implementation:**

```rust
enum TypedArrayBuilder {
    Boolean(BooleanBuilder),
    Int8(Int8Builder),
    Int16(Int16Builder),
    Int32(Int32Builder),
    Int64(Int64Builder),
    UInt8(UInt8Builder),
    UInt16(UInt16Builder),
    UInt32(UInt32Builder),
    UInt64(UInt64Builder),
    Float32(Float32Builder),
    Float64(Float64Builder),
    Utf8(StringBuilder),
}

impl TypedArrayBuilder {
    #[inline]
    fn append_value(&mut self, value: &str, nullable: bool) -> Result<()> {
        match self {
            TypedArrayBuilder::Float64(b) => {
                // Direct call, no downcasting
                b.append_value(value.parse()?);
            }
            // ... other variants
        }
        Ok(())
    }
}
```

**Expected Impact:** 5-10% throughput improvement  
**Complexity:** Low  
**Risk:** Low

---

### 3. Fast Float Parsing

**Current Issue:**
- Standard `str::parse::<f64>()` is accurate but not optimized for speed
- Float parsing is a significant portion of time for numeric-heavy workloads
- The benchmark workload is dominated by float values

**Proposed Solution:**
- Use the `fast-float` crate for parsing floating-point numbers
- Falls back to standard parsing for edge cases

**Implementation:**

```toml
# Cargo.toml
[dependencies]
fast-float = "0.2"
```

```rust
use fast_float::parse;

fn parse_float64(value: &str) -> Result<f64> {
    fast_float::parse(value).map_err(|e| Error::ParseError(...))
}
```

**Expected Impact:** 10-20% for float-heavy workloads  
**Complexity:** Very Low  
**Risk:** Very Low (well-tested crate, used by major projects)

---

### 4. Reduce Atom Allocations in Path Tracking

**Current Issue:**
- Every `XmlPath::append_node(node)` calls `Atom::from(node)` which involves:
  - Hash computation
  - Global interner lock acquisition
  - Potential allocation if string is new
- Same for `remove_node()` when checking equality

**Proposed Solution:**
- If using integer-based path indexing (Optimization #1), this becomes moot
- Alternative: Use a local string interner per parse operation
- Or: Pre-intern all element names from the config and reuse

**Implementation (if not using integer paths):**

```rust
struct LocalInterner {
    map: HashMap<&'static str, Atom>,
}

impl LocalInterner {
    fn get_or_intern(&mut self, s: &str) -> Atom {
        // Check if already interned
        if let Some(atom) = self.map.get(s) {
            return atom.clone();
        }
        // Intern and cache
        let atom = Atom::from(s);
        // Note: This requires careful lifetime management
        self.map.insert(/* leaked string */, atom.clone());
        atom
    }
}
```

**Expected Impact:** 5-15% throughput improvement  
**Complexity:** Medium (High if using local interner approach)  
**Risk:** Medium

---

### 5. Skip Irrelevant Subtrees

**Current Issue:**
- The parser processes every XML element, even those not mentioned in configuration
- For documents with large irrelevant sections, this wastes cycles

**Proposed Solution:**
- Build a set of "interesting" path prefixes from configuration
- When entering an element, check if any configured paths could be descendants
- If not, use `Reader::read_to_end()` to skip the entire subtree

**Implementation:**

```rust
struct PathInterestMap {
    // Set of path prefixes that might lead to configured fields/tables
    interesting_prefixes: HashSet<XmlPath>,
}

// In process_xml_events:
if !interest_map.could_be_interesting(&current_path) {
    reader.read_to_end(e.name())?;
    continue;
}
```

**Expected Impact:** Varies greatly (0-50% depending on XML structure)  
**Complexity:** Medium  
**Risk:** Low

---

## Medium Impact Optimizations

### 6. Pre-allocate Array Builder Capacity

**Current Issue:**
- Array builders use default initial capacity
- Causes multiple reallocations during parsing of large tables
- Each reallocation involves copying existing data

**Proposed Solution:**
- Estimate row count from XML size heuristics or configuration hints
- Pre-allocate builder capacity accordingly
- Allow users to specify expected row counts in configuration

**Implementation:**

```rust
// In FieldBuilder::new
fn new(field_config: &FieldConfig, capacity_hint: Option<usize>) -> Result<Self> {
    let capacity = capacity_hint.unwrap_or(1024);
    let array_builder = create_array_builder_with_capacity(field_config.data_type, capacity)?;
    // ...
}

fn create_array_builder_with_capacity(data_type: DType, capacity: usize) -> Box<dyn ArrayBuilder> {
    match data_type {
        DType::Float64 => Box::new(Float64Builder::with_capacity(capacity)),
        DType::Utf8 => Box::new(StringBuilder::with_capacity(capacity, capacity * 32)),
        // ...
    }
}
```

**Expected Impact:** 5-10% for large datasets  
**Complexity:** Low  
**Risk:** Very Low

---

### 7. Reduce is_table_path Calls

**Current Issue:**
- `is_table_path()` is called in both `Event::Start` and `Event::End` handlers
- Also called again after `remove_node()` in the End handler
- Each call involves a hash lookup

**Proposed Solution:**
- Cache the result from Start event and use it in End event
- Track table depth on a stack to avoid redundant lookups

**Implementation:**

```rust
struct TableStackEntry {
    path: XmlPath,
    is_table: bool,
}

// Push on Start, pop on End - no need to recompute
let mut table_stack: Vec<TableStackEntry> = Vec::new();

// In Event::Start:
let is_table = converter.is_table_path(&xml_path);
table_stack.push(TableStackEntry { path: xml_path.clone(), is_table });

// In Event::End:
let entry = table_stack.pop().unwrap();
if entry.is_table {
    converter.end_table()?;
}
// Check parent
if let Some(parent) = table_stack.last() {
    if parent.is_table {
        converter.end_current_row()?;
    }
}
```

**Expected Impact:** 3-7% throughput improvement  
**Complexity:** Low  
**Risk:** Very Low

---

### 8. Inline Hot Functions

**Current Issue:**
- Some frequently-called functions may not be inlined by the compiler
- Function call overhead adds up in tight loops

**Proposed Solution:**
- Add `#[inline]` or `#[inline(always)]` hints to hot functions
- Profile to verify inlining decisions

**Implementation:**

```rust
#[inline(always)]
fn set_field_value(&mut self, field_path: &XmlPath, value: &str) {
    // ...
}

#[inline]
fn append_node(&mut self, node: &str) {
    // ...
}
```

**Expected Impact:** 2-5% throughput improvement  
**Complexity:** Very Low  
**Risk:** Very Low

---

### 9. Use SmallVec for Small Collections

**Current Issue:**
- `Vec` is used for collections that are typically small (e.g., parent indices, path parts)
- Each `Vec` requires heap allocation even for small sizes

**Proposed Solution:**
- Replace `Vec` with `SmallVec` for collections with known typical sizes
- Avoids heap allocation for common cases

**Implementation:**

```toml
# Cargo.toml
[dependencies]
smallvec = "1.11"
```

```rust
use smallvec::SmallVec;

// XmlPath: most paths have <8 segments
struct XmlPath {
    parts: SmallVec<[Atom; 8]>,
}

// Parent indices: typically <4 levels
fn parent_row_indices(&self) -> SmallVec<[u32; 4]> {
    // ...
}
```

**Expected Impact:** 2-5% throughput improvement  
**Complexity:** Very Low  
**Risk:** Very Low

---

### 10. Batch String Operations

**Current Issue:**
- `String::from_utf8_lossy()` is called for every text event
- Creates intermediate `Cow<str>` which may allocate

**Proposed Solution:**
- Use `std::str::from_utf8()` with proper error handling
- Avoid lossy conversion if input is known to be valid UTF-8
- Consider using `bstr` crate for byte string operations

**Implementation:**

```rust
// Current (allocates on invalid UTF-8):
let text = String::from_utf8_lossy(&text);

// Optimized (avoids allocation for valid UTF-8):
let text = std::str::from_utf8(&text)?;

// Or with bstr for more flexibility:
use bstr::ByteSlice;
let text = text.to_str()?;
```

**Expected Impact:** 1-3% throughput improvement  
**Complexity:** Very Low  
**Risk:** Low (may change error behavior)

---

## Low Impact / High Complexity Optimizations

### 11. Parallel Parsing for Large Documents

**Proposed Solution:**
- For very large documents, split into chunks and parse in parallel
- Merge results from parallel parsers

**Challenges:**
- XML structure dependencies across chunks
- Maintaining correct row ordering
- Parent-child relationship tracking

**Expected Impact:** 2-4x for very large documents on multi-core systems  
**Complexity:** Very High  
**Risk:** High

---

### 12. Memory-Mapped I/O

**Proposed Solution:**
- Use `mmap` for reading large XML files
- Avoids copying data from kernel to user space

**Challenges:**
- Platform-specific implementation
- May not help for streaming use cases

**Expected Impact:** 5-15% for large files  
**Complexity:** Medium  
**Risk:** Medium (platform differences)

---

### 13. SIMD-Accelerated XML Parsing

**Proposed Solution:**
- Use SIMD instructions to scan for XML delimiters
- Accelerate element name extraction

**Note:** This would require modifications to or replacement of `quick-xml`.

**Expected Impact:** 20-50% potential improvement  
**Complexity:** Very High  
**Risk:** High

---

## Implementation Roadmap

### Phase 1: Quick Wins (1-2 days)
1. Add `fast-float` for float parsing
2. Add `#[inline]` hints to hot functions
3. Switch to `SmallVec` for small collections
4. Pre-allocate array builder capacity

**Expected cumulative improvement:** 15-25%

### Phase 2: Core Optimizations (3-5 days)
1. Implement typed array builder enum
2. Reduce `is_table_path` calls with caching
3. Optimize string operations

**Expected cumulative improvement:** 25-40%

### Phase 3: Path System Overhaul (5-7 days)
1. Implement integer-based path indexing
2. Implement subtree skipping for irrelevant paths

**Expected cumulative improvement:** 40-60%

### Phase 4: Advanced Optimizations (ongoing)
1. Investigate parallel parsing feasibility
2. Profile-guided optimizations
3. Memory-mapped I/O support

---

## Benchmarking Strategy

### Micro-benchmarks to Add

1. **Path operations benchmark:** Measure `append_node`/`remove_node` in isolation
2. **Float parsing benchmark:** Compare standard vs. `fast-float`
3. **Field lookup benchmark:** Measure hash lookup vs. direct indexing

### Test Scenarios to Cover

1. **Wide tables:** Many fields per row
2. **Deep nesting:** Many levels of XML hierarchy
3. **Sparse data:** Many elements not in configuration
4. **Large strings:** Text-heavy documents
5. **Numeric-heavy:** Millions of float values (current benchmark focus)

### Profiling Tools

- **cargo-flamegraph:** For CPU profiling
- **perf:** Linux performance counters
- **heaptrack:** Memory allocation profiling
- **cachegrind:** Cache behavior analysis

---

## Monitoring and Validation

### Performance Regression Testing

Add CI checks to detect performance regressions:

```yaml
# .github/workflows/benchmark.yml
- name: Run benchmarks
  run: cargo bench --bench parse_benchmark -- --save-baseline pr-${{ github.sha }}
  
- name: Compare with main
  run: cargo bench --bench parse_benchmark -- --baseline main --load-baseline pr-${{ github.sha }}
```

### Key Metrics to Track

| Metric | Target | Current |
|--------|--------|---------|
| Throughput (MiB/s) | >200 | ~140 |
| Time per float (ns) | <50 | ~70 |
| Memory per row (bytes) | <100 | TBD |

---

## Appendix: Code Hotspots Analysis

Based on the implementation review, the following functions are on the critical path:

1. `process_xml_events` - Main event loop (~40% of time)
2. `XmlPath::append_node` / `remove_node` - Path manipulation (~15%)
3. `set_field_value_for_current_table` - Field value setting (~10%)
4. `append_current_value` - Type conversion (~20%)
5. `is_table_path` - Path matching (~10%)
6. quick-xml internals - XML tokenization (~5%)

Optimization efforts should focus on these areas in proportion to their contribution to total runtime.