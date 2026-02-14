# AI Agents Guide: xml2arrow

This file helps AI assistants work effectively in this repository.

**Core Philosophy:**
- **Zero-Copy Hot Paths:** The parsing loop must avoid allocation.
- **O(1) Lookups:** Use integer-based `PathNodeId`, never string matching during parsing.
- **Correctness:** XML hierarchy and Arrow schema alignment are paramount.

## Project Overview

- **Crate:** `xml2arrow` converts XML data to Apache Arrow tables using a streaming, single-pass approach.
- **Stack:** Rust, `quick-xml` (parsing), `arrow` (data structures), `pyo3` (Python bindings).
- **Configuration:** driven by `Config` (YAML) which maps XML paths to Arrow fields.

## Public API Surface

Treat `src/lib.rs` exports as the public API. Avoid breaking changes here.
- `parse_xml`: Main entry point.
- `Config`, `TableConfig`, `FieldConfig`: Configuration structs.
- `DType`: Supported Arrow types.

## Architecture & Data Flow (Read Carefully)

The converter operates in **phases** to ensure performance:

1.  **Setup Phase (Allocations Allowed):**
    -   `Config` is loaded and validated (`config.rs`).
    -   `PathRegistry` compiles all XML strings into a trie, assigning a unique `PathNodeId` (u32) to every relevant path (`path_registry.rs`).

2.  **Streaming Phase (Hot Path - NO Allocations):**
    -   `xml_parser.rs` iterates over `quick-xml` events.
    -   **Path Tracking:** `PathTracker` maintains a stack of `PathNodeId`. It uses `PathRegistry` to resolve children via integer indexing—**never string comparison**.
    -   **Value Extraction:** When a `Text` event occurs, if the current `PathNodeId` maps to a field, the text is appended to the active `FieldBuilder`.

3.  **Finalization Phase:**
    -   `TableBuilder` drains field builders into Arrow `RecordBatch`es.

## Critical Implementation Rules

When writing code, you **MUST** adhere to these rules:

1.  **Performance Constraints (Hot Path):**
    -   Inside `process_xml_events` and `PathTracker::enter`:
        -   **NEVER** clone `String` or `Vec`.
        -   **NEVER** use `HashMap` lookups with string keys. Use `PathNodeId`.
        -   **NEVER** perform heap allocation unless pushing to an Arrow builder.
    -   Reuse buffers (e.g., `attr_name_buffer` in `process_xml_events`).

2.  **Error Handling:**
    -   Use the custom `Error` enum in `errors.rs`.
    -   If modifying `Error`, you **MUST** update the `From<Error> for PyErr` impl in `errors.rs` (guarded by `#[cfg(feature = "python")]`).

3.  **Testing:**
    -   Use `rstest` for unit tests (`#[rstest]`).
    -   If fixing a bug, add a reproduction case in `tests/integration_tests.rs`.
    -   If modifying the parser, verify performance isn't degraded.
    
## Common Workflows

### 1. Adding a New Data Type
If asked to support a new Arrow type (e.g., `Date32`):
1.  Add variant to `DType` enum in `config.rs`.
2.  Update `DType::as_arrow_type` match arm.
3.  Update `create_array_builder` in `xml_parser.rs` to initialize the correct Arrow builder.
4.  Update `FieldBuilder::append_current_value` in `xml_parser.rs` to parse string input into the new type.
5.  Add a test case in `xml_parser.rs` `mod tests`.

### 2. Modifying Configuration Logic
1.  Modify the struct in `config.rs`.
2.  **Crucial:** Update `Config::validate()` to enforce constraints (e.g., scale/offset rules).
3.  Run `cargo test` to ensure YAML deserialization still works. 

## Repository Map

- `src/config.rs`: Structs for YAML config. **Validation logic lives here.**
- `src/path_registry.rs`: The integer-based path trie. **Performance critical.**
- `src/xml_parser.rs`: The main event loop and Arrow building logic. **Performance critical.**
- `src/errors.rs`: Error types and Python/PyO3 bindings.
- `tests/`: End-to-end integration tests.

## Development Commands

```bash
# Check code including Python feature
cargo check --features python

# Run all tests
cargo test --features python

# Run benchmarks
cargo bench

# Compare benchmarks (if modifying parser)
cargo bench --bench parse_benchmark -- --save-baseline before_changes
# ... make changes ...
cargo bench --bench parse_benchmark -- --baseline before_changes
```

## Feature flags
- `python`: Enables PyO3 bindings.
  - Code guarded by `#[cfg(feature = "python")]` is strictly for Python interop.
  - Ensure `From<Error>` conversions in `errors.rs` are exhaustive.

## 📝 Literate Programming & Readability

This project follows a "Literate Programming" approach. You must treat code as a document meant for human logic verification. Follow these rules for all contributions:

### 1. Document the "Why," Not the "How"
- **Constraint:** Do not comment on obvious Rust syntax (e.g., `i += 1; // increment i` is forbidden).
- **Requirement:** Explain the *intent* and *edge cases*. If a logic block handles a specific XML quirk or Arrow memory constraint, document that reasoning explicitly.

### 2. Top-Down Narrative Structure
- **Requirement:** Organize functions and modules as a logical story. 
- **Structure:** Start with high-level intent (Public API) → move to middle-ware logic (Coordination) → end with low-level details (Utilities/Helpers).
- **Visuals:** Use clear section headers (e.g., `// --- PHASE 1: SETUP ---`) to separate distinct conceptual steps in long functions like `process_xml_events`.

### 3. Inline Context over External Documentation
- **Requirement:** Keep the reasoning immediately adjacent to the implementation. A developer should not have to leave the file to understand why a specific parsing branch was taken.

### 4. Strategic Abstraction
- **Rule:** Favor **readable inline logic** over "clever" abstractions. 
- **Guideline:** Only move code into a separate function if:
    1. It is reused in multiple places.
    2. It represents a distinct, independent unit of work (e.g., string-to-float parsing).
    3. It simplifies the "main narrative" of the caller.
- **Note:** Avoid "function sprawl" where the logic is fragmented into 20 tiny functions that are only called once.

### 5. Self-Contained Logic
- **Constraint:** Avoid creating generic `utils.rs` files for simple logic. If a helper is only relevant to `xml_parser.rs`, keep it as a private helper in that file. This reduces "mental context switching."

**Expected Outcome:** Every file should be readable as a short technical essay: 
`[Section Intent]` → `[Architectural Reasoning]` → `[Implementation]`.
