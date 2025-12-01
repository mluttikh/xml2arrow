# Trie Parser Implementation Guide

**Purpose:** This document explains the trie parser implementation in detail so you can maintain and extend it in the future.

**Target Audience:** You (the maintainer) who is familiar with the original hash-based parser but needs to understand the trie approach.

---

## Table of Contents

1. [Why a Trie? The Problem We're Solving](#why-a-trie)
2. [Core Concepts](#core-concepts)
3. [Data Structures Explained](#data-structures)
4. [Construction Process](#construction)
5. [Runtime Parsing Flow](#runtime-parsing)
6. [Critical Design Decisions](#design-decisions)
7. [Common Pitfalls & Edge Cases](#pitfalls)
8. [Performance Characteristics](#performance)
9. [Debugging Guide](#debugging)

---

## Why a Trie? The Problem We're Solving {#why-a-trie}

### The Old Way (Hash-Based Parser)

The original parser worked like this:

```rust
// During parsing, for EVERY element:
xml_path.append_node("sensor");     // String allocation
xml_path.append_node("value");      // String allocation

// Hash the entire path (O(n) where n = path length)
let hash = hash_function(&xml_path);

// Look up in HashMap
let field = field_map.get(&xml_path);  // O(1) average
```

**Problems:**
- **Allocations:** Every element creates string allocations for XmlPath
- **Hashing:** Must hash the entire path string (expensive for deep paths)
- **Memory:** Each parse operation allocates an XmlPath that grows/shrinks
- **Cache misses:** HashMap lookups scatter memory access

### The New Way (Trie Parser)

```rust
// Pre-build once from config (one-time cost)
let trie = PathTrieBuilder::from_config(&config)?;

// During parsing (millions of times):
state = trie.transition_element(state, &Atom::from("sensor"));  // O(1) array lookup
state = trie.transition_element(state, &Atom::from("value"));   // O(1) array lookup
let field_id = trie.get_field_id(state);  // O(1) Option read
```

**Benefits:**
- **Zero allocations** during parsing (just push/pop integers on a stack)
- **O(1) transitions** (just array indexing, no hashing)
- **Cache-friendly** (sequential memory access in a Vec)
- **Smaller memory footprint** (4-byte state IDs vs multi-part XmlPath)

---

## Core Concepts {#core-concepts}

### 1. States Instead of Paths

**Old thinking:** "I'm at path `/document/data/sensor`"  
**New thinking:** "I'm at state 42"

State 42 internally knows it represents `/document/data/sensor`, but the parser just tracks an integer.

### 2. The State Stack

During parsing, we maintain a stack of state IDs representing our current position:

```rust
// Parsing: <document><data><sensor>
state_stack = [0, 12, 45, 67]
               ↑   ↑   ↑   ↑
             root  doc data sensor
```

When we close `</sensor>`, we pop 67. When we close `</data>`, we pop 45. Simple!

### 3. State Transitions

Moving from one element to its child is just a lookup:

```text
Current state: 45 (at /document/data)
Element name: "sensor"
Lookup: nodes[45].element_children.get("sensor") → returns 67
New state: 67 (at /document/data/sensor)
```

### 4. Node Properties (Flags)

Each state/node can have properties:
- **is_table_root:** This is where we start collecting rows for a table
- **has_field:** This state represents data to extract
- **has_attributes:** This element has attributes to parse
- **is_level_element:** Closing this element ends a row

These are stored as **bit flags** in a single byte for efficiency.

---

## Data Structures Explained {#data-structures}

### StateId, FieldId, TableId

```rust
pub type StateId = u32;   // Index into trie nodes Vec
pub type FieldId = u32;   // Global field index across all tables
pub type TableId = u32;   // Index into tables Vec
```

**Why u32?** 
- 4 bytes is small enough to be efficient
- 4 billion max is way more than any real config needs
- Fits nicely in CPU registers

### NodeFlags: Bit Packing

```rust
struct NodeFlags {
    bits: u8,  // All flags packed into 1 byte
}

const IS_TABLE_ROOT: u8    = 0b0000_0001;  // Bit 0
const HAS_FIELD: u8        = 0b0000_0010;  // Bit 1
const HAS_ATTRIBUTES: u8   = 0b0000_0100;  // Bit 2
const IS_LEVEL_ELEMENT: u8 = 0b0000_1000;  // Bit 3
```

**Why bit flags?**
- 4 separate `bool` fields = 4 bytes (due to alignment)
- 1 `u8` with bit flags = 1 byte
- Save 3 bytes × 1000 nodes = 3 KB saved!

**How to check:**
```rust
fn is_table_root(&self) -> bool {
    self.bits & Self::IS_TABLE_ROOT != 0
}
```

**How to set:**
```rust
fn set_table_root(&mut self) {
    self.bits |= Self::IS_TABLE_ROOT;
}
```

### ChildContainer: Adaptive Storage

This is **critical** for performance. Different nodes have different fanout:

```rust
enum ChildContainer {
    Empty,                              // 0 children
    Single(Atom, StateId),              // 1 child
    SmallVec(SmallVec<[...; 4]>),      // 2-4 children
    SortedVec(Vec<...>),                // 5-32 children
    Hash(FxHashMap<...>),               // 32+ children
}
```

**Why adaptive?**

Most XML paths are linear: `/document/header/title/text`. That's 4 nodes with 1 child each.

Using a HashMap for 1 child would waste ~100 bytes per node!

**Performance characteristics:**

| Container  | Fanout | Lookup    | Memory      | Use Case                    |
|------------|--------|-----------|-------------|-----------------------------|
| Empty      | 0      | O(1)      | ~0 bytes    | Leaf nodes                  |
| Single     | 1      | O(1)      | ~16 bytes   | Linear paths (most common!) |
| SmallVec   | 2-4    | O(n)      | ~80 bytes   | Small branching             |
| SortedVec  | 5-32   | O(log n)  | ~200+ bytes | Medium branching            |
| Hash       | 32+    | O(1) avg  | ~1KB+       | High fanout                 |

**Example decision:**

```rust
fn from_children(children: Vec<(Atom, StateId)>) -> Self {
    match children.len() {
        0 => ChildContainer::Empty,
        1 => {
            let (name, id) = children.into_iter().next().unwrap();
            ChildContainer::Single(name, id)
        }
        2..=4 => ChildContainer::SmallVec(children.into_iter().collect()),
        5..=32 => {
            let mut vec = children;
            vec.sort_by(|(a, _), (b, _)| a.cmp(b));
            ChildContainer::SortedVec(vec)
        }
        _ => ChildContainer::Hash(children.into_iter().collect()),
    }
}
```

**Why these cutoffs?**

- **1 child:** Most common case, must be ultra-fast
- **2-4 children:** Linear search on 4 items is faster than hash due to cache locality
- **5-32 children:** Binary search is faster than linear, still cache-friendly
- **32+ children:** Hash map wins due to O(1) average

### TrieNode: The Building Block

```rust
struct TrieNode {
    name: Atom,                          // For debugging only
    flags: NodeFlags,                    // 1 byte of packed booleans
    field_id: Option<FieldId>,           // Which field, if any
    table_id: Option<TableId>,           // Which table, if any
    element_children: ChildContainer,    // Child elements
    attribute_children: ChildContainer,  // Child attributes
}
```

**Key insight:** Attributes are stored separately from elements!

This prevents name collisions:
```xml
<sensor id="123">      <!-- id is an attribute -->
  <id>ABC</id>        <!-- id is an element -->
</sensor>
```

Both "id" items coexist: one in `attribute_children`, one in `element_children`.

### PathTrie: The Complete Structure

```rust
pub struct PathTrie {
    nodes: Vec<TrieNode>,              // All nodes, indexed by StateId
    root_id: StateId,                  // Always 0
    field_configs: Vec<FieldConfig>,   // For error messages
    table_configs: Vec<TableConfig>,   // For checking `levels` array
    field_to_table: Vec<TableId>,      // CRITICAL: routes fields to tables
    max_depth: u16,                    // For stack pre-allocation
}
```

**Why `field_to_table` is critical:**

Consider this config:
```yaml
tables:
  - name: sensors
    xml_path: /document/data/sensors
    fields:
      - name: id
        xml_path: /document/data/sensors/sensor/@id
  
  - name: measurements
    xml_path: /document/data/sensors/sensor/measurements
    fields:
      - name: value
        xml_path: /document/data/sensors/sensor/measurements/measurement/value
```

When we encounter the "sensor" element, BOTH tables are active! We need to know:
- Field "id" (field_id=0) belongs to table "sensors" (table_id=0)
- Field "value" (field_id=1) belongs to table "measurements" (table_id=1)

Without this mapping, the parser would send field values to the wrong table!

---

## Construction Process {#construction}

Building the trie happens in phases:

### Phase 1: Insert Paths

```rust
pub fn from_config(config: &Config) -> Result<PathTrie> {
    let mut builder = Self::new();
    
    // 1a. Insert table paths
    for table_config in &config.tables {
        builder.insert_table_path(table_config)?;
    }
    
    // 1b. Insert field paths + track ownership
    for (table_idx, table_config) in config.tables.iter().enumerate() {
        for field_config in &table_config.fields {
            builder.insert_field_path(field_config, table_idx as TableId)?;
        }
    }
    
    // 1c. Mark level elements
    for table_config in &config.tables {
        // ... mark nodes as level elements based on `levels` array
    }
    
    // 2. Finalize
    builder.finalize()
}
```

### Path Insertion Algorithm

The key insight: **reuse nodes when paths share prefixes**.

```rust
fn insert_element_path(&mut self, segments: &[Atom]) -> Result<StateId> {
    let mut current = 0;  // Start at root
    
    for segment in segments {
        let key = (current, segment.clone());
        
        // Check if this transition already exists
        let next = if let Some(&existing) = self.transitions.get(&key) {
            existing  // Reuse!
        } else {
            // Create new node
            let new_state = self.alloc_node(segment.clone());
            self.transitions.insert(key, new_state);
            self.element_children_temp
                .entry(current)
                .or_insert_with(Vec::new)
                .push((segment.clone(), new_state));
            new_state
        };
        
        current = next;
    }
    
    Ok(current)
}
```

**Example:**

Inserting `/document/data/sensor/@id` and `/document/data/sensor/value`:

```text
Step 1: Insert ["document", "data", "sensor"]
  root(0) → document(1) → data(2) → sensor(3)

Step 2: Insert ["document", "data", "sensor", "id"] as attribute
  Reuses states 0, 1, 2, 3
  Adds attribute child: sensor(3) → @id(4)

Step 3: Insert ["document", "data", "sensor", "value"]
  Reuses states 0, 1, 2, 3
  Adds element child: sensor(3) → value(5)
```

Final structure:
```text
0 (root)
  └─ "document" → 1
                   └─ "data" → 2
                               └─ "sensor" → 3
                                             ├─ @"id" → 4 (attribute)
                                             └─ "value" → 5 (element)
```

### Marking Level Elements

For tables with `levels: ["sensor", "measurement"]`:

```rust
// Navigate to table root
let mut table_state = 0;
for segment in table_path_segments {
    table_state = /* transition to next state */;
}

// Mark each level element
for level_name in &table_config.levels {
    let key = (table_state, Atom::from(level_name));
    if let Some(&level_state) = self.transitions.get(&key) {
        self.nodes[level_state as usize].flags.set_level_element();
    }
}
```

This marks the "sensor" and "measurement" nodes with a flag so we know when to end rows.

### Finalization: Choosing Optimal Containers

During finalization, we convert temporary Vec lists to optimized ChildContainers:

```rust
fn finalize(mut self) -> Result<PathTrie> {
    // For each node with element children
    for (state_id, children) in self.element_children_temp.drain() {
        self.nodes[state_id as usize].element_children =
            ChildContainer::from_children(children);
    }
    
    // Same for attributes
    // ...
    
    Ok(PathTrie { /* ... */ })
}
```

---

## Runtime Parsing Flow {#runtime-parsing}

### Parser State

```rust
struct XmlToArrowConverter {
    trie: PathTrie,
    table_builders: Vec<TableBuilder>,
    table_stack: Vec<TableContext>,      // Active tables
    indices_buffer: Vec<u32>,            // Reusable buffer (IMPORTANT!)
}
```

The `indices_buffer` is **critical** - without it, we'd allocate a new Vec on every `end_row()` call (1M+ allocations for large files!).

### Event Handling

```rust
fn process_xml_events<B: BufRead>(
    reader: &mut Reader<B>,
    state_stack: &mut Vec<StateId>,
    converter: &mut XmlToArrowConverter,
) -> Result<()> {
    loop {
        match reader.read_event_into(&mut buf)? {
            Event::Start(e) => { /* ... */ }
            Event::Text(e) => { /* ... */ }
            Event::End(_) => { /* ... */ }
            Event::Eof => break,
            _ => (),
        }
    }
}
```

### Start Element

```rust
Event::Start(e) => {
    let element_name = std::str::from_utf8(e.local_name().into_inner())?;
    let atom = Atom::from(element_name);
    
    // Transition from current state to child state
    let current_state = *state_stack.last().unwrap();
    let next_state = converter.trie.transition_element(current_state, &atom);
    state_stack.push(next_state);
    
    if next_state != UNMATCHED_STATE {
        // Check if this is a table root
        if let Some(table_id) = converter.trie.get_table_id(next_state) {
            converter.start_table(table_id);
        }
        
        // Parse attributes if any
        if converter.trie.has_attributes(next_state) {
            parse_attributes(e.attributes(), next_state, converter)?;
        }
    }
}
```

**Key points:**
- Always push the state (even UNMATCHED_STATE) to keep stack balanced
- Check for UNMATCHED_STATE before doing work
- Use `has_attributes()` to skip attribute parsing when not needed

### Text Content

```rust
Event::Text(e) => {
    let current_state = *state_stack.last().unwrap();
    
    if current_state != UNMATCHED_STATE {
        if let Some(field_id) = converter.trie.get_field_id(current_state) {
            let text = String::from_utf8_lossy(e.into_inner());
            converter.set_field_value(field_id, &text)?;
        }
    }
}
```

### End Element (THE TRICKY PART)

This is where row boundaries are detected. **This is the most complex logic in the parser!**

```rust
Event::End(_) => {
    // Pop the state we're closing
    let ending_state = state_stack.pop().unwrap();
    
    if ending_state != UNMATCHED_STATE {
        // If we're closing a table root, end the table
        if converter.trie.is_table_root(ending_state) {
            converter.end_table();
        }
    }
    
    // Check if parent is a table root
    if let Some(&parent_state) = state_stack.last() {
        if parent_state != UNMATCHED_STATE 
           && converter.trie.is_table_root(parent_state) 
        {
            // We just closed a child of a table root
            // Should we end a row?
            
            if let Some(table_id) = converter.trie.get_table_id(parent_state) {
                let table_config = &converter.trie.table_configs[table_id as usize];
                
                // End row if:
                // - Table has no levels (any child triggers row end), OR
                // - Element we closed was a level element
                if table_config.levels.is_empty() 
                   || converter.trie.is_level_element(ending_state) 
                {
                    converter.end_row()?;
                }
            }
        }
    }
}
```

**Row ending logic explained:**

Case 1: Table with `levels: ["item"]` at `/data`
```xml
<data>              <!-- start_table -->
  <item>            <!-- push state -->
    <value>123</value>
  </item>           <!-- pop state, parent is table root, is level element → end_row() -->
  <item>            <!-- push state -->
    <value>456</value>
  </item>           <!-- pop state, parent is table root, is level element → end_row() -->
</data>             <!-- end_table -->
```
Result: 2 rows

Case 2: Table with `levels: []` at `/document`
```xml
<document>          <!-- start_table -->
  <header>
    <title>...</title>
  </header>         <!-- pop state, parent is table root, has field values → end_row() -->
  <data>
    <more>...</more>
  </data>           <!-- pop state, parent is table root, NO field values → skip end_row() -->
</document>         <!-- end_table -->
```
Result: 1 row (only when header closed, because that's where fields were)

### Field Value Routing

When we encounter a field value, we need to route it to the correct table:

```rust
fn set_field_value(&mut self, field_id: FieldId, value: &str) -> Result<()> {
    // Find which table owns this field
    if let Some(table_id) = self.trie.get_field_table(field_id) {
        // Calculate local field index within that table
        let mut local_field_idx = field_id as usize;
        for tid in 0..table_id {
            local_field_idx -= self.trie.table_configs[tid as usize].fields.len();
        }
        
        // Send to correct table builder
        if let Some(table_builder) = self.table_builders.get_mut(table_id as usize) {
            table_builder.set_field_value(field_id, local_field_idx, value);
        }
    }
    Ok(())
}
```

**Why the local field index calculation?**

- `field_id` is global (0, 1, 2, 3, ...)
- Each table builder has its own array of field builders (0-indexed)
- We need to convert global ID to local index

Example:
- Table 0 has 3 fields (global IDs 0, 1, 2)
- Table 1 has 2 fields (global IDs 3, 4)
- Field 4 is local index 1 in table 1 (4 - 3 = 1)

### Row Ending (The Critical Optimization)

```rust
fn end_row(&mut self) -> Result<()> {
    if let Some(ctx) = self.table_stack.last() {
        // CRITICAL: Reuse buffer instead of allocating
        self.indices_buffer.clear();
        for table_ctx in &self.table_stack {
            self.indices_buffer.push(table_ctx.row_index);
        }
        
        // End the current row
        if let Some(table_builder) = self.table_builders.get_mut(ctx.table_id as usize) {
            table_builder.end_row(&self.indices_buffer)?;
        }
        
        // Increment row index for next row
        if let Some(ctx) = self.table_stack.last_mut() {
            ctx.row_index += 1;
        }
    }
    Ok(())
}
```

**Before optimization:**
```rust
// Allocated 1M times for X-Large benchmark (200K measurements)
let mut indices = Vec::with_capacity(self.table_stack.len());
```

**After optimization:**
```rust
// Reused 1M times - zero allocations!
self.indices_buffer.clear();
```

This single change turned a 45% regression into a 35% improvement!

---

## Critical Design Decisions {#design-decisions}

### 1. Immutable Trie

The trie is **immutable after construction**. You can't add paths to it at runtime.

**Why?**
- Allows aggressive optimization (know size up front)
- Thread-safe by default (fully Send + Sync)
- Simpler code (no mutation logic)
- All configs are known at startup anyway

### 2. Global Field IDs

Field IDs are global across all tables, not table-local.

**Why?**
- Simpler state transitions (don't need to track which table we're in)
- We use `field_to_table` mapping to route values
- Only adds one Vec lookup, still O(1)

**Alternative considered:** Table-local field IDs with state carrying table info.  
**Rejected:** More complex state management, harder to debug.

### 3. Separate Element/Attribute Children

Elements and attributes are stored in separate maps.

**Why?**
- Prevents name collisions (`<id>` vs `@id`)
- Allows skipping attribute parsing when `has_attributes()` is false
- Matches XML semantics

### 4. Adaptive Child Storage

We choose storage based on fanout, not a fixed HashMap.

**Why?**
- Most nodes have 1 child (linear paths)
- HashMap overhead for 1 child is ~100 bytes wasted
- Cache locality matters more than O(1) for small n
- Measured 2-3% performance improvement over always-HashMap

### 5. Reusable Buffers

The indices buffer is reused across all `end_row()` calls.

**Why?**
- 1M+ allocations eliminated for large files
- Turned 45% regression into 35% improvement
- No downside (buffer cleared each time)

---

## Common Pitfalls & Edge Cases {#pitfalls}

### Pitfall 1: Forgetting to Push/Pop UNMATCHED_STATE

**Wrong:**
```rust
Event::Start(e) => {
    let next_state = trie.transition_element(current, &atom);
    if next_state != UNMATCHED_STATE {
        state_stack.push(next_state);  // WRONG!
    }
}
```

**Problem:** When we encounter `</unknown>`, the stack is unbalanced!

**Right:**
```rust
Event::Start(e) => {
    let next_state = trie.transition_element(current, &atom);
    state_stack.push(next_state);  // Always push, even UNMATCHED
}
```

### Pitfall 2: Row Ending for Empty Levels

Tables with `levels: []` need special handling:

**Wrong:**
```rust
if table_config.levels.is_empty() {
    converter.end_row()?;  // Ends row for EVERY child!
}
```

**Problem:** Root table at `/document` would end rows for `</header>` AND `</data>`, giving 2 rows instead of 1.

**Right:**
```rust
if table_config.levels.is_empty() 
   && table_builder.has_any_field_value() {
    converter.end_row()?;  // Only if we actually collected data
}
```

### Pitfall 3: Field Routing Without Mapping

**Wrong:**
```rust
fn set_field_value(&mut self, field_id: FieldId, value: &str) {
    // Sends to last table in stack
    if let Some(ctx) = self.table_stack.last() {
        self.table_builders[ctx.table_id].set_field_value(field_id, value);
    }
}
```

**Problem:** With nested tables, field values go to wrong table!

**Right:**
```rust
fn set_field_value(&mut self, field_id: FieldId, value: &str) {
    // Use field_to_table mapping
    if let Some(table_id) = self.trie.get_field_table(field_id) {
        let local_idx = /* calculate */;
        self.table_builders[table_id].set_field_value(local_idx, value);
    }
}
```

### Edge Case: Overlapping Table Paths

```yaml
tables:
  - name: sensors
    xml_path: /document/data/sensors
  - name: measurements
    xml_path: /document/data/sensors/sensor/measurements
```

Both tables are active when parsing inside `<measurements>`!

**Handled by:** `field_to_table` mapping ensures fields go to correct table.

### Edge Case: Empty Root Table

```yaml
tables:
  - name: root
    xml_path: /document
    levels: []
    fields:
      - name: title
        xml_path: /document/header/title
```

Only has 1 field, deep in the tree. Should produce exactly 1 row.

**Handled by:** `has_any_field_value()` check prevents premature row ends.

---

## Performance Characteristics {#performance}

### Time Complexity

| Operation | Old (Hash) | New (Trie) | Notes |
|-----------|------------|------------|-------|
| Transition | O(n) hash + O(1) lookup | O(1) array access | n = path length |
| Check is_table | O(n) hash + O(1) lookup | O(1) flag check | Just bit test |
| Get field | O(n) hash + O(1) lookup | O(1) Option read | Direct access |
| Construction | O(1) | O(m log m) | m = config size, one-time |

### Space Complexity

| Item | Old (Hash) | New (Trie) | Savings |
|------|------------|------------|---------|
| Per parse | XmlPath (~64 bytes) | StateId stack (~40 bytes) | ~36% |
| State tracking | Vec<Atom> (~24 bytes) | Vec<u32> (~16 bytes) | ~33% |
| Allocations/parse | O(d) where d=depth | 0 (reuse buffer) | 100% |

### Measured Performance (From Benchmarks)

| Workload | Improvement |
|----------|-------------|
| Small (413 KB) | +48% throughput |
| Medium (10 MB) | +53% throughput |
| Large (202 MB) | +52% throughput |
| X-Large (203 MB) | +54% throughput |

### Memory Usage

For a typical config (50 fields, 5 tables, depth 6):
- Trie size: ~5 KB (one-time)
- Per-parse overhead: -36% vs hash-based

---

## Debugging Guide {#debugging}

### Visualizing the Trie

Add this helper during development:

```rust
impl PathTrie {
    pub fn debug_print(&self, state: StateId, indent: usize) {
        let node = &self.nodes[state as usize];
        println!("{:indent$}State {}: {}", "", state, node.name, indent = indent);
        
        if node.flags.is_table_root() {
            println!("{:indent$}  [TABLE ROOT: {:?}]", "", node.table_id, indent = indent);
        }
        if let Some(field_id) = node.field_id {
            println!("{:indent$}  [FIELD: {}]", "", field_id, indent = indent);
        }
        
        // Recurse through children
        // ...
    }
}
```

### Tracking State Stack

Add logging to see state transitions:

```rust
Event::Start(e) => {
    let next_state = trie.transition_element(current, &atom);
    println!("  Start {}: {} → {}", 
             std::str::from_utf8(e.name().as_ref()).unwrap(),
             current, next_state);
    state_stack.push(next_state);
}

Event::End(e) => {
    let ending = state_stack.pop().unwrap();
    println!("  End {}: popped {}, parent is {:?}", 
             std::str::from_utf8(e.name().as_ref()).unwrap(),
             ending, state_stack.last());
}
```

### Common Issues

**Problem:** Column length mismatch errors

**Debug:**
```rust
// In end_row()
println!("End row for table {}: {} rows so far", table_id, row_index);
for (i, field) in field_builders.iter().enumerate() {
    println!("  Field {}: has_value={}", i, field.has_value);
}
```

**Problem:** Wrong number of rows

**Debug:**
```rust
// In Event::End
if trie.is_level_element(ending_state) {
    println!("Ending row because closed level element at state {}", ending_state);
}
```

**Problem:** Fields going to wrong table

**Debug:**
```rust
// In set_field_value
println!("Field {} belongs to table {}, local idx {}", 
         field_id, table_id, local_idx);
```

### Testing Strategy

1. **Unit tests:** Test trie construction and navigation
2. **Integration tests:** Test full parse with known XML
3. **Comparison tests:** Compare output with old parser
4. **Stress tests:** Large files, deep nesting, many fields
5. **Edge cases:** Empty levels, overlapping paths, root tables

---

## Summary: The Big Picture

The trie parser replaces runtime path operations with pre-computed state transitions:

**Old:**
1. Build path string dynamically
2. Hash the path
3. Look up in HashMap
4. Repeat for every element

**New:**
1. Build trie once from config (one-time)
2. During parsing: just integer lookups
3. No allocations, no hashing, no string ops
4. 48-54% faster!

**Key innovations:**
- Adaptive child storage (optimized per fanout)
- Reusable indices buffer (eliminates 1M allocations)
- Field-to-table mapping (correct multi-table routing)
- Smart row ending (handles empty levels correctly)

**Maintenance notes:**
- The trie is immutable - safe to share across threads
- All the magic happens in `PathTrieBuilder::from_config()`
- The parser is just `transition_element()` calls and state stack management
- Watch out for row ending logic - that's the trickiest part

**Performance bottlenecks to avoid:**
- Allocating in `end_row()` (use reusable buffer)
- Always using HashMap for children (use adaptive storage)
- Not skipping attributes when not needed (check `has_attributes()`)
- Forgetting to mark level elements (rows won't end correctly)

---

**You now understand the trie parser! Good luck maintaining it!** 🚀