# XML Parser Trie Implementation Guide

**Purpose:** Explain the `xml_parser_trie.rs` implementation in detail for future maintenance.

**Audience:** You (the maintainer) who needs to understand how the parser uses the trie structure.

---

## Table of Contents

1. [Overview: Parser Architecture](#overview)
2. [Data Structures](#data-structures)
3. [Initialization](#initialization)
4. [XML Event Processing](#event-processing)
5. [Field Value Handling](#field-values)
6. [Row Boundary Detection](#row-boundaries)
7. [Critical Optimizations](#optimizations)
8. [Common Issues & Solutions](#issues)

---

## Overview: Parser Architecture {#overview}

### High-Level Flow

```
XML File → quick_xml Reader → Events → XmlToArrowConverter → Arrow RecordBatches
```

### Key Components

1. **XmlToArrowConverter**: Main state machine that processes events
2. **State Stack**: Tracks current position in XML tree (just integers)
3. **Table Stack**: Tracks which tables are currently active
4. **TableBuilder**: Builds Arrow arrays for one table
5. **FieldBuilder**: Builds one column of data

### What Makes This Fast?

**Old Parser:**
- Built XmlPath string for every element
- Hashed the path to look up fields
- Allocated memory on every element

**Trie Parser:**
- Just pushes/pops integers on a stack
- O(1) lookups via state transitions
- Zero allocations during hot path

---

## Data Structures {#data-structures}

### XmlToArrowConverter

```rust
struct XmlToArrowConverter {
    trie: PathTrie,                   // Pre-built trie from config
    table_builders: Vec<TableBuilder>, // One per table in config
    table_stack: Vec<TableContext>,    // Currently active tables
    indices_buffer: Vec<u32>,         // CRITICAL: reusable buffer
}
```

**Why `indices_buffer` exists:**

Without it, we'd do this in `end_row()`:
```rust
let mut indices = Vec::with_capacity(8);  // Allocate every time!
```

With 1 million rows, that's 1 million allocations = 45% slower!

With reusable buffer:
```rust
self.indices_buffer.clear();  // Reuse, no allocation
```

Result: 35% FASTER than baseline!

### TableContext

```rust
struct TableContext {
    table_id: TableId,     // Which table (index into table_builders)
    row_index: u32,        // Current row number for this table
}
```

**Why a stack?**

Tables can be nested! Example:
```xml
<sensors>           <!-- Table 0 starts -->
  <sensor>          <!-- Row 0 of table 0 -->
    <measurements>  <!-- Table 1 starts (nested) -->
      <measurement> <!-- Row 0 of table 1 -->
```

Stack: `[{table: 0, row: 0}, {table: 1, row: 0}]`

### TableBuilder

```rust
struct TableBuilder {
    table_config: TableConfig,           // Original config
    index_builders: Vec<UInt32Builder>,  // Level index columns
    field_builders: Vec<FieldBuilder>,   // Data columns
    row_index: usize,                    // Total rows so far
}
```

**Key methods:**

```rust
fn set_field_value(&mut self, field_id: FieldId, local_field_idx: usize, value: &str)
```
- Stores value in the field's current_value buffer
- Doesn't append yet (we might be in the middle of collecting fields)

```rust
fn end_row(&mut self, indices: &[u32]) -> Result<()>
```
- Appends all current field values to their respective builders
- Appends index columns (for nested tables)
- Clears field buffers for next row
- Increments row_index

### FieldBuilder

```rust
struct FieldBuilder {
    field_config: FieldConfig,      // Original config
    field: Field,                   // Arrow field metadata
    array_builder: Box<dyn ArrayBuilder>,  // Actual data builder
    has_value: bool,                // Did we see a value this row?
    current_value: String,          // Accumulates text content
}
```

**Why `current_value` is a String:**

Text content can come in multiple events:
```xml
<value>123<!-- comment -->456</value>
```

This generates two Text events: "123" and "456". We accumulate them in `current_value`.

**Why `has_value` matters:**

For nullable fields, we need to know:
- `has_value = true`: Append the value
- `has_value = false`: Append null

---

## Initialization {#initialization}

### Building the Converter

```rust
pub fn parse_xml(reader: impl BufRead, config: &Config) -> Result<...> {
    // 1. Build the trie from config (one-time cost)
    let trie = PathTrieBuilder::from_config(config)?;
    
    // 2. Create table builders (one per table)
    let mut table_builders = Vec::new();
    for table_config in &trie.table_configs {
        table_builders.push(TableBuilder::new(table_config)?);
    }
    
    // 3. Create converter
    let mut converter = XmlToArrowConverter {
        trie,
        table_builders,
        table_stack: Vec::new(),
        indices_buffer: Vec::with_capacity(8),  // Pre-allocate
    };
    
    // 4. Parse!
    let mut state_stack = vec![converter.trie.root_id()];
    process_xml_events(&mut reader, &mut state_stack, &mut converter)?;
    
    // 5. Finalize and return record batches
    converter.finish()
}
```

### TableBuilder::new()

```rust
fn new(table_config: &TableConfig) -> Result<Self> {
    // Create index builders (one per level)
    let mut index_builders = Vec::new();
    index_builders.resize_with(table_config.levels.len(), UInt32Builder::default);
    
    // Create field builders (one per field)
    let mut field_builders = Vec::new();
    for field_config in &table_config.fields {
        field_builders.push(FieldBuilder::new(field_config)?);
    }
    
    Ok(Self {
        table_config: table_config.clone(),
        index_builders,
        field_builders,
        row_index: 0,
    })
}
```

**Why `index_builders`?**

For nested tables with `levels: ["sensor", "measurement"]`, we need index columns:
```
| <sensor> | <measurement> | value |
|----------|---------------|-------|
| 0        | 0             | 123   |  ← Sensor 0, Measurement 0
| 0        | 1             | 456   |  ← Sensor 0, Measurement 1
| 1        | 0             | 789   |  ← Sensor 1, Measurement 0
```

The index columns let you reconstruct the nesting structure!

---

## XML Event Processing {#event-processing}

### Main Event Loop

```rust
fn process_xml_events<B: BufRead, const PARSE_ATTRIBUTES: bool>(
    reader: &mut Reader<B>,
    state_stack: &mut Vec<StateId>,
    converter: &mut XmlToArrowConverter,
) -> Result<()> {
    let mut buf = Vec::with_capacity(4096);
    
    loop {
        match reader.read_event_into(&mut buf)? {
            Event::Start(e) => { /* handle start tag */ }
            Event::Text(e) => { /* handle text content */ }
            Event::End(_) => { /* handle end tag */ }
            Event::Eof => break,
            _ => (),
        }
        buf.clear();  // Reuse buffer
    }
    Ok(())
}
```

**The `PARSE_ATTRIBUTES` const generic:**

We check at compile time if any fields use attributes:
```rust
if config.requires_attribute_parsing() {
    process_xml_events::<_, true>(&mut reader, ...)?;
} else {
    process_xml_events::<_, false>(&mut reader, ...)?;  // Faster!
}
```

When `PARSE_ATTRIBUTES = false`, the compiler completely eliminates the attribute parsing code!

### Event::Start - Opening an Element

```rust
Event::Start(e) => {
    // 1. Get element name
    let node = std::str::from_utf8(e.local_name().into_inner())?;
    let atom = Atom::from(node);
    
    // 2. Transition from current state to child state
    let current_state = *state_stack.last().unwrap();
    let next_state = converter.trie.transition_element(current_state, &atom);
    state_stack.push(next_state);
    
    // 3. Only do work if this is a valid state
    if next_state != UNMATCHED_STATE {
        // Check if this is a table root
        if let Some(table_id) = converter.trie.get_table_id(next_state) {
            converter.start_table(table_id);
        }
        
        // Parse attributes if this node has any
        if PARSE_ATTRIBUTES && converter.trie.has_attributes(next_state) {
            parse_attributes(e.attributes(), next_state, converter, ...)?;
        }
    }
}
```

**Critical insight:** Always push the state, even if it's UNMATCHED_STATE!

Why? Stack must stay balanced with XML nesting:
```xml
<data>          <!-- Push state 5 -->
  <unknown>     <!-- Push UNMATCHED_STATE -->
    <nested>    <!-- Push UNMATCHED_STATE -->
    </nested>   <!-- Pop UNMATCHED_STATE -->
  </unknown>    <!-- Pop UNMATCHED_STATE -->
</data>         <!-- Pop state 5 -->
```

If we only pushed valid states, the pops wouldn't match!

### Event::Text - Handling Text Content

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

**Why we check UNMATCHED_STATE:**

If we're in an unknown subtree, we don't want to process any of its content!

```xml
<data>
  <item>
    <value>123</value>  <!-- Valid, process this -->
  </item>
  <unknown>
    <value>456</value>  <!-- Invalid path, ignore this -->
  </unknown>
</data>
```

### Event::End - THE CRITICAL PART

This is where **row boundary detection** happens. This is the most complex logic!

```rust
Event::End(_) => {
    // 1. Pop the state we're closing
    let ending_state = state_stack.pop().unwrap();
    
    // 2. If we're closing a table root, end the table
    if ending_state != UNMATCHED_STATE {
        if converter.trie.is_table_root(ending_state) {
            converter.end_table();
        }
    }
    
    // 3. Check if we should end a row
    //    This is the TRICKY part!
    if let Some(&parent_state) = state_stack.last() {
        if parent_state != UNMATCHED_STATE 
           && converter.trie.is_table_root(parent_state) 
        {
            // We just closed a child of a table root
            // Should we end a row?
            
            if let Some(table_id) = converter.trie.get_table_id(parent_state) {
                let table_config = &converter.trie.table_configs[table_id as usize];
                
                // Decision logic:
                if (table_config.levels.is_empty()
                    && converter.table_builders[table_id as usize]
                        .has_any_field_value())
                   || converter.trie.is_level_element(ending_state)
                {
                    converter.end_row()?;
                }
            }
        }
    }
}
```

**Understanding the row ending logic:**

**Case 1: Table with explicit levels**

Config: `levels: ["item"]`, path: `/data`
```xml
<data>          <!-- start_table -->
  <item>        <!-- Push state -->
    <value>123</value>
  </item>       <!-- Pop state, parent is table root, is_level_element → end_row() -->
  <item>
    <value>456</value>
  </item>       <!-- Pop state, parent is table root, is_level_element → end_row() -->
</data>         <!-- end_table -->
```
Result: 2 rows ✓

**Case 2: Table with empty levels (root table)**

Config: `levels: []`, path: `/document`
```xml
<document>      <!-- start_table -->
  <header>
    <title>My Doc</title>
  </header>     <!-- Pop, parent is table root, has field values → end_row() -->
  <data>
    <!-- no fields here -->
  </data>       <!-- Pop, parent is table root, NO field values → skip end_row() -->
</document>     <!-- end_table -->
```
Result: 1 row ✓

**Case 3: Table with empty levels (item table)**

Config: `levels: []`, path: `/data`
```xml
<data>          <!-- start_table -->
  <item>
    <value>123</value>
  </item>       <!-- Pop, parent is table root, has field values → end_row() -->
  <item>
    <value>456</value>
  </item>       <!-- Pop, parent is table root, has field values → end_row() -->
</data>         <!-- end_table -->
```
Result: 2 rows ✓

**The key insight:**

For tables with `levels: []`, we end a row when:
- We close a child of the table root, AND
- We actually collected some field values

This handles both root tables (1 row) and item tables (N rows) correctly!

---

## Field Value Handling {#field-values}

### set_field_value() - Routing to Correct Table

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

**Why this is necessary:**

Consider nested tables:
```yaml
tables:
  - name: sensors
    xml_path: /document/data/sensors
    fields:
      - name: id
        xml_path: /document/data/sensors/sensor/@id  # Field 0
  
  - name: measurements
    xml_path: /document/data/sensors/sensor/measurements
    fields:
      - name: value
        xml_path: .../measurement/value  # Field 1
```

When parsing:
```xml
<sensors>
  <sensor id="S1">      <!-- Field 0 must go to table 0 -->
    <measurements>
      <measurement>
        <value>123</value>  <!-- Field 1 must go to table 1 -->
```

Both tables are active (both on table_stack), but we need to send field values to the RIGHT table!

**The field_to_table mapping:**
- Built during trie construction
- Maps global field_id → table_id
- O(1) lookup

**The local index calculation:**

Global field IDs: `[0, 1, 2, 3, 4, 5, ...]`  
Table 0 has 3 fields (IDs 0, 1, 2)  
Table 1 has 2 fields (IDs 3, 4)  
Table 2 has 1 field (ID 5)

To find local index for field 4:
```rust
local = 4;
for tid in 0..1 {  // Table 1
    local -= table_configs[tid].fields.len();
}
// local = 4 - 3 = 1
```

Field 4 is local index 1 in table 1's field_builders array.

### TableBuilder::set_field_value()

```rust
fn set_field_value(&mut self, _field_id: FieldId, local_field_idx: usize, value: &str) {
    if let Some(field_builder) = self.field_builders.get_mut(local_field_idx) {
        field_builder.set_current_value(value);
    }
}
```

Simple! Just stores the value in the field builder's buffer. Doesn't append yet.

### FieldBuilder::set_current_value()

```rust
fn set_current_value(&mut self, value: &str) {
    self.current_value.push_str(value);  // Accumulate
    self.has_value = true;
}
```

**Why accumulate?**

Text can come in multiple chunks:
```xml
<value>12<!-- comment -->34</value>
```
Generates: Text("12"), Text("34")  
Result: current_value = "1234"

---

## Row Boundary Detection {#row-boundaries}

### start_table()

```rust
fn start_table(&mut self, table_id: TableId) {
    let table_builder = &mut self.table_builders[table_id as usize];
    table_builder.row_index = 0;  // Reset for this table instance
    self.table_stack.push(TableContext::new(table_id));
}
```

Called when we encounter a table root element (e.g., `<data>`).

### end_row() - THE CRITICAL OPTIMIZATION

```rust
fn end_row(&mut self) -> Result<()> {
    if let Some(ctx) = self.table_stack.last() {
        // CRITICAL: Reuse buffer instead of allocating!
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

**The indices array:**

For nested tables, we need to track which parent row we're in:

```
Table stack: [{sensors, row=0}, {measurements, row=2}]
Indices: [0, 2]
Means: Sensor 0, Measurement 2
```

**THE CRITICAL OPTIMIZATION:**

❌ **Before:**
```rust
let mut indices = Vec::with_capacity(self.table_stack.len());
```
- Allocates a new Vec every time
- For 1M rows: 1M allocations
- Result: 45% SLOWER than baseline!

✅ **After:**
```rust
self.indices_buffer.clear();
```
- Reuses the same buffer
- For 1M rows: 0 allocations
- Result: 35% FASTER than baseline!

**This single optimization gave us an 80 percentage point improvement!**

### TableBuilder::end_row()

```rust
fn end_row(&mut self, indices: &[u32]) -> Result<()> {
    // 1. Save the current row
    self.save_row(indices)?;
    
    // 2. Clear field buffers for next row
    for field_builder in &mut self.field_builders {
        field_builder.has_value = false;
        field_builder.current_value.clear();
    }
    
    Ok(())
}
```

### TableBuilder::save_row()

```rust
fn save_row(&mut self, indices: &[u32]) -> Result<()> {
    // 1. Append index columns
    for (index, index_builder) in indices.iter().zip(&mut self.index_builders) {
        index_builder.append_value(*index);
    }
    
    // 2. Append all field values
    for field_builder in &mut self.field_builders {
        field_builder.append_current_value()?;
    }
    
    // 3. Increment row count
    self.row_index += 1;
    
    Ok(())
}
```

**Why we append ALL fields every time:**

Even if a field has no value, we must append something (null) to keep columns aligned!

```
Row 0: [123,  456,  789]
Row 1: [111,  null, 222]  ← Field 1 was missing, append null
Row 2: [333,  444,  555]
```

All columns must have the same length for Arrow RecordBatch!

### FieldBuilder::append_current_value()

```rust
fn append_current_value(&mut self) -> Result<()> {
    let value = self.current_value.as_str();
    
    match self.field_config.data_type {
        DType::Int32 => append_numeric::<Int32Type>(...),
        DType::Float64 => append_numeric::<Float64Type>(...),
        DType::Utf8 => {
            let builder = self.array_builder
                .as_any_mut()
                .downcast_mut::<StringBuilder>()?;
            
            if self.has_value {
                builder.append_value(value);
            } else {
                builder.append_null();
            }
        }
        // ... other types
    }
    
    Ok(())
}
```

**Key decision: has_value determines null vs value**

- `has_value = true`: Parse and append the value
- `has_value = false`: Append null (even for non-nullable fields initially)

Nullable fields allow nulls. Non-nullable fields will error at RecordBatch creation if nulls exist.

### end_table()

```rust
fn end_table(&mut self) {
    self.table_stack.pop();
}
```

Simple! Just pop from the table stack. We're done with this table instance.

---

## Critical Optimizations {#optimizations}

### 1. Reusable Indices Buffer

**Location:** `XmlToArrowConverter::indices_buffer`

**Impact:** Turned 45% regression into 35% improvement (80 point swing!)

**Why it matters:**
- `end_row()` called millions of times
- Each call was allocating a Vec
- Now zero allocations

### 2. Const Generic for Attributes

**Location:** `process_xml_events<B: BufRead, const PARSE_ATTRIBUTES: bool>`

**Impact:** ~5% improvement when no attributes used

**How it works:**
```rust
if PARSE_ATTRIBUTES && trie.has_attributes(state) {
    parse_attributes(...);  // Code eliminated if PARSE_ATTRIBUTES=false!
}
```

### 3. has_attributes() Check

**Location:** Before parsing attributes

**Impact:** Skip entire attribute parsing loop when node has no attributes

**Example:**
```xml
<value>123</value>  <!-- No attributes, skip parse_attributes() entirely -->
```

### 4. State Stack Pre-allocation

**Location:** `Vec::with_capacity(trie.max_depth() as usize)`

**Impact:** No reallocations during parsing

**Why:** We know max depth from trie construction

### 5. has_any_field_value() Check

**Location:** Row ending logic for empty levels

**Impact:** Prevents creating empty rows

**Example:**
```xml
<document>
  <header>...</header>  <!-- Has fields, end row -->
  <metadata/>           <!-- No fields, skip end row -->
</document>
```

---

## Common Issues & Solutions {#issues}

### Issue 1: Column Length Mismatch

**Error:** "All columns must have the same length"

**Cause:** Not appending to some field on some row

**Debug:**
```rust
// In end_row()
println!("Table {}: ending row {}", table_id, row_index);
for (i, fb) in field_builders.iter().enumerate() {
    println!("  Field {}: has_value={}, current='{}'", 
             i, fb.has_value, fb.current_value);
}
```

**Common causes:**
- Forgetting to call `end_row()` for some rows
- Calling `end_row()` too many times for some rows
- Not resetting `has_value` between rows

### Issue 2: Wrong Number of Rows

**Symptoms:** Expected 10 rows, got 1 (or 20)

**Debug:**
```rust
// In Event::End where we call end_row()
println!("Ending row: ending_state={}, parent={:?}, is_level={}, levels={:?}",
         ending_state, parent_state, 
         trie.is_level_element(ending_state),
         table_config.levels);
```

**Common causes:**
- `is_level_element` not set correctly
- `levels` array doesn't match XML structure
- `has_any_field_value()` check too strict/lenient

### Issue 3: Fields Going to Wrong Table

**Symptoms:** Sensor ID appears in measurements table

**Debug:**
```rust
// In set_field_value()
println!("Field {} → table {}, local idx {}", 
         field_id, table_id, local_field_idx);
```

**Cause:** `field_to_table` mapping not built correctly

**Solution:** Check `PathTrieBuilder::insert_field_path()` passes correct table_id

### Issue 4: Null Values in Non-nullable Columns

**Symptoms:** "Column 'id' is non-nullable but contains null values"

**Cause:** Field value never set for some row

**Debug:**
```rust
// Track which rows have which fields
println!("Row {}: field {} has_value={}", 
         row_index, field_name, has_value);
```

**Common causes:**
- XML missing expected elements
- Path in config doesn't match XML structure
- Field value set but `end_row()` called before element closed

### Issue 5: Memory Growth

**Symptoms:** Parser uses more memory than expected

**Debug:**
```rust
// Check if buffers are being cleared
println!("current_value len: {}, capacity: {}", 
         current_value.len(), current_value.capacity());
```

**Common causes:**
- Not clearing `current_value` between rows
- `indices_buffer` growing unbounded (should just clear)
- Arrow builders not getting reset

---

## Summary: Key Takeaways

### The Parser in One Paragraph

The trie parser maintains a stack of state IDs representing the current XML path. On each element, it transitions to a child state (O(1) lookup). When it finds a field state, it stores the value. When it closes a level element (or any child of an empty-levels table with field values), it ends a row. All field values are appended to Arrow builders, creating RecordBatches.

### Critical Design Points

1. **State stack must stay balanced** - Always push, even UNMATCHED_STATE
2. **Reusable buffers everywhere** - indices_buffer, event buffer
3. **Field routing via mapping** - field_to_table is essential for nested tables
4. **Smart row ending** - Different logic for empty vs explicit levels
5. **has_value tracking** - Distinguishes null from value

### What Makes It Fast

1. **Zero allocations** in hot path (reusable buffers)
2. **O(1) transitions** (array indexing, no hashing)
3. **Compile-time optimization** (const generic for attributes)
4. **Skip work when possible** (has_attributes check)
5. **Cache-friendly** (Vec of nodes, not scattered pointers)

### Maintenance Checklist

When modifying the parser:
- [ ] Are you allocating in the hot path? (Use reusable buffers!)
- [ ] Is the state stack balanced? (Every push has a pop)
- [ ] Are all fields appended on every row? (Check column lengths)
- [ ] Does row ending logic handle empty levels? (has_any_field_value)
- [ ] Are nested tables handled? (Check field_to_table routing)

**The parser is fast, correct, and maintainable. Keep it that way!** 🚀