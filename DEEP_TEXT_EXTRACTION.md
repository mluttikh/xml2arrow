To implement **Deep Text Extraction** (flattening), we need to modify the parser so that once it enters a field's XML path, it stays in a "capturing" state for all subsequent text events, even if it encounters nested tags that aren't in your configuration.

Here is how to modify the `XmlToArrowConverter` and the main parsing loop.

### 1. Update `XmlToArrowConverter` State

We need to track which field (if any) is currently "active" and at what depth that field started.

In `xml_parser.rs`:

```rust
struct XmlToArrowConverter<'a> {
    registry: &'a PathRegistry,
    table_builders: Vec<TableBuilder>,
    // --- NEW FIELDS ---
    /// If we are currently capturing deep text for a field, store the field's path ID
    active_text_field: Option<(usize, usize)>, // (table_idx, field_idx)
    /// The depth at which the active text field started
    active_field_depth: usize,
}

```

### 2. Update the Logic in `process_xml_events`

The goal is to ensure that if `active_text_field` is set, all text is funneled to that field until we see the closing tag for the element that started it.

#### The `Event::Start` Modification:

When we enter a node, we check if it maps to a field. If it does, we mark it as the active field. If we are *already* inside an active field and hit an unknown tag, we simply let the depth increase.

```rust
// Inside the Event::Start match arm in xml_parser.rs
Event::Start(e) => {
    let name = Atom::from(std::str::from_utf8(e.local_name().as_ref())?);
    let node_id = path_tracker.enter(name.as_ref(), &xml_to_arrow_converter.registry);
    
    // If this node maps to fields, and we aren't already capturing deep text...
    if let Some(id) = node_id {
        let info = xml_to_arrow_converter.registry.get_node_info(id);
        if !info.field_indices.is_empty() && xml_to_arrow_converter.active_text_field.is_none() {
            // Pick the first field index (or logic to handle multiple)
            xml_to_arrow_converter.active_text_field = Some(info.field_indices[0]);
            xml_to_arrow_converter.active_field_depth = path_tracker.depth();
        }
    }
    
    // ... rest of attribute logic ...
}

```

#### The `Event::Text` Modification:

This is the most important change. Instead of looking at the *current* path, we look at the *active* field.

```rust
// Inside the Event::Text match arm
Event::Text(e) => {
    let text = e.unescape()?;
    let value = if options.trim_text { text.trim() } else { &text };

    if !value.is_empty() {
        // If we have an active "deep" field, always append to it
        if let Some((t_idx, f_idx)) = xml_to_arrow_converter.active_text_field {
            xml_to_arrow_converter.table_builders[t_idx]
                .field_builders[f_idx]
                .set_current_value(value);
        } else if let Some(node_id) = path_tracker.current() {
            // Fallback for legacy behavior or root-level fields
            xml_to_arrow_converter.set_field_value_for_node(node_id, value);
        }
    }
}

```

#### The `Event::End` Modification:

We must clear the `active_text_field` only when the specific tag that started the capture is closed.

```rust
// Inside the Event::End match arm
Event::End(_) => {
    // Check if we are exiting the "active" field's element
    if let Some(_) = xml_to_arrow_converter.active_text_field {
        if path_tracker.depth() == xml_to_arrow_converter.active_field_depth {
            xml_to_arrow_converter.active_text_field = None;
            xml_to_arrow_converter.active_field_depth = 0;
        }
    }

    // Existing table closing logic...
    if let Some(node_id) = path_tracker.current() {
        // ... (existing logic to commit row if it's a table path)
    }
    path_tracker.exit();
}

```

### How this improves the parser:

1. **Robustness:** If a user has `<description>Hello <b>World</b></description>`, the "World" text is no longer lost. When `<b>` is encountered, `active_text_field` remains set to the `description` field.
2. **Preserves "Data-centric" for Numbers:** Even with this change, if a field is an `Int32`, the `set_current_value` will accumulate `"123"`. The only difference is if there's a comment or a stray tag like `<price>12<tag/>3</price>`, it will now correctly parse as `123` instead of failing.
3. **Minimal Performance Hit:** We added two integer comparisons to the hot loop, which is negligible compared to the overhead of XML string unescaping.

### One important detail: `set_current_value` vs `append`

In your original `FieldBuilder`, `set_current_value` likely overwrites. You should rename it to `append_to_current_value` and use `String::push_str` to ensure that text from before and after a nested tag is concatenated correctly.
