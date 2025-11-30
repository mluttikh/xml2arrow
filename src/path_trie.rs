//! Path trie for efficient XML path matching and field/table resolution.
//!
//! This module implements a trie (prefix tree) structure that replaces the runtime
//! `XmlPath` construction and hash-based lookups. The trie is built once from the
//! configuration and provides O(1)-amortized lookups during XML parsing.
//!
//! # Key concepts
//!
//! - **StateId**: Integer index into the trie node array
//! - **FieldId**: Index into the field builders array
//! - **TableId**: Index into the table builders array
//! - **TrieNode**: Represents one XML element or attribute in the path
//! - **ChildContainer**: Adaptive storage for child nodes (optimized by fanout)
//!
//! # Example
//!
//! ```rust
//! use xml2arrow::{PathTrieBuilder, Config};
//! use xml2arrow::config::{TableConfig, FieldConfigBuilder, DType};
//! use string_cache::DefaultAtom as Atom;
//!
//! // Create a configuration
//! let config = Config {
//!     parser_options: Default::default(),
//!     tables: vec![
//!         TableConfig::new(
//!             "sensors",
//!             "/document/data/sensors",
//!             vec![],
//!             vec![
//!                 FieldConfigBuilder::new("id", "/document/data/sensors/sensor/@id", DType::Utf8).build(),
//!                 FieldConfigBuilder::new("value", "/document/data/sensors/sensor/value", DType::Float64).build(),
//!             ]
//!         )
//!     ],
//! };
//!
//! // Build the trie
//! let trie = PathTrieBuilder::from_config(&config).unwrap();
//!
//! // Simulate parsing: navigate through XML elements
//! let mut state = trie.root_id();
//! state = trie.transition_element(state, &Atom::from("document"));
//! state = trie.transition_element(state, &Atom::from("data"));
//! state = trie.transition_element(state, &Atom::from("sensors"));
//!
//! // Check if this is a table root
//! assert!(trie.is_table_root(state));
//! assert_eq!(trie.get_table_id(state), Some(0));
//!
//! // Navigate into a sensor element
//! state = trie.transition_element(state, &Atom::from("sensor"));
//!
//! // Check for attributes
//! if trie.has_attributes(state) {
//!     let attr_state = trie.transition_attribute(state, &Atom::from("id"));
//!     if let Some(field_id) = trie.get_field_id(attr_state) {
//!         println!("Found attribute field with ID: {}", field_id);
//!     }
//! }
//!
//! // Navigate to value element
//! let value_state = trie.transition_element(state, &Atom::from("value"));
//! assert_eq!(trie.get_field_id(value_state), Some(1));
//! ```

use crate::config::{Config, FieldConfig, TableConfig};
use crate::errors::{Error, Result};
use fxhash::FxHashMap;
use smallvec::SmallVec;
use std::collections::HashMap;
use string_cache::DefaultAtom as Atom;

/// Integer identifier for a trie node state.
pub type StateId = u32;

/// Integer identifier for a field builder.
pub type FieldId = u32;

/// Integer identifier for a table builder.
pub type TableId = u32;

/// Sentinel value indicating no valid state (used for unmatched paths).
pub const UNMATCHED_STATE: StateId = StateId::MAX;

/// Compact flags stored in each trie node.
#[derive(Debug, Clone, Copy, Default)]
struct NodeFlags {
    bits: u8,
}

impl NodeFlags {
    const IS_TABLE_ROOT: u8 = 0b0000_0001;
    const HAS_FIELD: u8 = 0b0000_0010;
    const HAS_ATTRIBUTES: u8 = 0b0000_0100;

    #[inline]
    fn is_table_root(&self) -> bool {
        self.bits & Self::IS_TABLE_ROOT != 0
    }

    #[inline]
    #[allow(dead_code)]
    fn has_field(&self) -> bool {
        self.bits & Self::HAS_FIELD != 0
    }

    #[inline]
    fn has_attributes(&self) -> bool {
        self.bits & Self::HAS_ATTRIBUTES != 0
    }

    #[inline]
    fn set_table_root(&mut self) {
        self.bits |= Self::IS_TABLE_ROOT;
    }

    #[inline]
    fn set_field(&mut self) {
        self.bits |= Self::HAS_FIELD;
    }

    #[inline]
    fn set_has_attributes(&mut self) {
        self.bits |= Self::HAS_ATTRIBUTES;
    }
}

/// Adaptive container for child nodes, optimized based on fanout.
#[derive(Debug, Clone)]
enum ChildContainer {
    /// No children.
    Empty,
    /// Single child (common case for linear paths).
    Single(Atom, StateId),
    /// Small number of children (2-4), stored inline.
    SmallVec(SmallVec<[(Atom, StateId); 4]>),
    /// Medium number of children (5-32), sorted for binary search.
    SortedVec(Vec<(Atom, StateId)>),
    /// Many children (>32), hash map for O(1) lookup.
    Hash(FxHashMap<Atom, StateId>),
}

impl ChildContainer {
    /// Look up a child state by name.
    #[inline]
    fn get(&self, key: &Atom) -> Option<StateId> {
        match self {
            ChildContainer::Empty => None,
            ChildContainer::Single(a, id) => {
                if a == key {
                    Some(*id)
                } else {
                    None
                }
            }
            ChildContainer::SmallVec(v) => v.iter().find(|(a, _)| a == key).map(|(_, id)| *id),
            ChildContainer::SortedVec(v) => v
                .binary_search_by(|(a, _)| a.cmp(key))
                .ok()
                .map(|idx| v[idx].1),
            ChildContainer::Hash(h) => h.get(key).copied(),
        }
    }

    /// Create an optimal container from a list of children.
    fn from_children(mut children: Vec<(Atom, StateId)>) -> Self {
        match children.len() {
            0 => ChildContainer::Empty,
            1 => {
                let (name, id) = children.pop().unwrap();
                ChildContainer::Single(name, id)
            }
            2..=4 => {
                let small: SmallVec<[(Atom, StateId); 4]> = children.into_iter().collect();
                ChildContainer::SmallVec(small)
            }
            5..=32 => {
                children.sort_by(|(a, _), (b, _)| a.cmp(b));
                ChildContainer::SortedVec(children)
            }
            _ => {
                let map: FxHashMap<Atom, StateId> = children.into_iter().collect();
                ChildContainer::Hash(map)
            }
        }
    }
}

impl Default for ChildContainer {
    fn default() -> Self {
        ChildContainer::Empty
    }
}

/// A node in the path trie representing one XML element or attribute.
#[derive(Debug, Clone)]
struct TrieNode {
    /// The element or attribute name (without '@' prefix for attributes).
    #[allow(dead_code)]
    name: Atom,
    /// Compact flags indicating node properties.
    flags: NodeFlags,
    /// If this node represents a field, its builder index.
    field_id: Option<FieldId>,
    /// If this node represents a table root, its builder index.
    table_id: Option<TableId>,
    /// Child element nodes.
    element_children: ChildContainer,
    /// Child attribute nodes (keyed by attribute name without '@').
    attribute_children: ChildContainer,
}

impl TrieNode {
    fn new(name: Atom) -> Self {
        Self {
            name,
            flags: NodeFlags::default(),
            field_id: None,
            table_id: None,
            element_children: ChildContainer::Empty,
            attribute_children: ChildContainer::Empty,
        }
    }
}

/// The complete path trie structure.
#[derive(Debug, Clone)]
pub struct PathTrie {
    /// All trie nodes, indexed by StateId.
    nodes: Vec<TrieNode>,
    /// The root state ID (always 0).
    root_id: StateId,
    /// Mapping from FieldId to original field config (for error messages).
    pub field_configs: Vec<FieldConfig>,
    /// Mapping from TableId to original table config.
    pub table_configs: Vec<TableConfig>,
    /// Maximum depth of any path in the trie.
    max_depth: u16,
}

impl PathTrie {
    /// Get a reference to a node by StateId.
    #[inline]
    fn get_node(&self, state_id: StateId) -> Option<&TrieNode> {
        if state_id == UNMATCHED_STATE {
            None
        } else {
            self.nodes.get(state_id as usize)
        }
    }

    /// Get the root state ID.
    #[inline]
    pub fn root_id(&self) -> StateId {
        self.root_id
    }

    /// Check if a state represents a table root.
    #[inline]
    pub fn is_table_root(&self, state_id: StateId) -> bool {
        self.get_node(state_id)
            .map(|n| n.flags.is_table_root())
            .unwrap_or(false)
    }

    /// Get the table ID for a state, if it's a table root.
    #[inline]
    pub fn get_table_id(&self, state_id: StateId) -> Option<TableId> {
        self.get_node(state_id).and_then(|n| n.table_id)
    }

    /// Get the field ID for a state, if it's a field.
    #[inline]
    pub fn get_field_id(&self, state_id: StateId) -> Option<FieldId> {
        self.get_node(state_id).and_then(|n| n.field_id)
    }

    /// Transition to a child element state.
    #[inline]
    pub fn transition_element(&self, state_id: StateId, element_name: &Atom) -> StateId {
        self.get_node(state_id)
            .and_then(|n| n.element_children.get(element_name))
            .unwrap_or(UNMATCHED_STATE)
    }

    /// Transition to a child attribute state.
    #[inline]
    pub fn transition_attribute(&self, state_id: StateId, attr_name: &Atom) -> StateId {
        self.get_node(state_id)
            .and_then(|n| n.attribute_children.get(attr_name))
            .unwrap_or(UNMATCHED_STATE)
    }

    /// Check if a state has attributes.
    #[inline]
    pub fn has_attributes(&self, state_id: StateId) -> bool {
        self.get_node(state_id)
            .map(|n| n.flags.has_attributes())
            .unwrap_or(false)
    }

    /// Get the maximum depth.
    pub fn max_depth(&self) -> u16 {
        self.max_depth
    }
}

/// Builder for constructing a PathTrie from configuration.
pub struct PathTrieBuilder {
    /// Temporary map for deduplicating paths during construction.
    transitions: HashMap<(StateId, Atom), StateId>,
    /// Growing list of nodes.
    nodes: Vec<TrieNode>,
    /// Next available state ID.
    next_state: StateId,
    /// Field configurations in order.
    field_configs: Vec<FieldConfig>,
    /// Table configurations in order.
    table_configs: Vec<TableConfig>,
    /// Track maximum depth.
    max_depth: u16,
    /// Temporary storage for element children before finalizing.
    element_children_temp: HashMap<StateId, Vec<(Atom, StateId)>>,
    /// Temporary storage for attribute children before finalizing.
    attribute_children_temp: HashMap<StateId, Vec<(Atom, StateId)>>,
}

impl PathTrieBuilder {
    /// Create a new builder with a root node.
    pub fn new() -> Self {
        let mut nodes = Vec::new();
        let root = TrieNode::new(Atom::from(""));
        nodes.push(root);

        Self {
            transitions: HashMap::new(),
            nodes,
            next_state: 1, // 0 is reserved for root
            field_configs: Vec::new(),
            table_configs: Vec::new(),
            max_depth: 0,
            element_children_temp: HashMap::new(),
            attribute_children_temp: HashMap::new(),
        }
    }

    /// Build a PathTrie from a Config.
    pub fn from_config(config: &Config) -> Result<PathTrie> {
        let mut builder = Self::new();

        // Insert all tables first
        for table_config in &config.tables {
            builder.insert_table_path(table_config)?;
        }

        // Insert all fields
        for table_config in &config.tables {
            for field_config in &table_config.fields {
                builder.insert_field_path(field_config)?;
            }
        }

        builder.finalize()
    }

    /// Insert a table path.
    fn insert_table_path(&mut self, table_config: &TableConfig) -> Result<()> {
        let segments = parse_path(&table_config.xml_path);
        let table_id = self.table_configs.len() as TableId;
        self.table_configs.push(table_config.clone());

        let final_state = self.insert_element_path(&segments)?;
        let node = &mut self.nodes[final_state as usize];
        node.flags.set_table_root();
        node.table_id = Some(table_id);

        Ok(())
    }

    /// Insert a field path.
    fn insert_field_path(&mut self, field_config: &FieldConfig) -> Result<()> {
        let (segments, is_attribute) = parse_field_path(&field_config.xml_path);
        let field_id = self.field_configs.len() as FieldId;
        self.field_configs.push(field_config.clone());

        if is_attribute {
            // Last segment is an attribute
            if segments.is_empty() {
                return Err(Error::UnsupportedDataType(format!(
                    "Invalid attribute path: {}",
                    field_config.xml_path
                )));
            }
            let element_segments = &segments[..segments.len() - 1];
            let attr_name = &segments[segments.len() - 1];

            let parent_state = self.insert_element_path(element_segments)?;
            let final_state = self.insert_attribute(parent_state, attr_name.clone())?;

            let node = &mut self.nodes[final_state as usize];
            node.flags.set_field();
            node.field_id = Some(field_id);

            // Mark parent as having attributes
            self.nodes[parent_state as usize].flags.set_has_attributes();
        } else {
            // Regular element path
            let final_state = self.insert_element_path(&segments)?;
            let node = &mut self.nodes[final_state as usize];
            node.flags.set_field();
            node.field_id = Some(field_id);
        }

        Ok(())
    }

    /// Insert a path of element segments, returning the final state.
    fn insert_element_path(&mut self, segments: &[Atom]) -> Result<StateId> {
        let mut current = 0; // root
        let depth = segments.len() as u16;
        if depth > self.max_depth {
            self.max_depth = depth;
        }

        for segment in segments {
            let key = (current, segment.clone());
            let next = if let Some(&existing) = self.transitions.get(&key) {
                existing
            } else {
                // Allocate new node
                let new_id = self.alloc_node(segment.clone());
                self.transitions.insert(key, new_id);

                // Record as child of current
                self.element_children_temp
                    .entry(current)
                    .or_insert_with(Vec::new)
                    .push((segment.clone(), new_id));

                new_id
            };
            current = next;
        }

        Ok(current)
    }

    /// Insert an attribute as a child of a parent state.
    fn insert_attribute(&mut self, parent_state: StateId, attr_name: Atom) -> Result<StateId> {
        let key = (parent_state, attr_name.clone());

        if let Some(&existing) = self.transitions.get(&key) {
            Ok(existing)
        } else {
            let new_id = self.alloc_node(attr_name.clone());
            self.transitions.insert(key, new_id);

            self.attribute_children_temp
                .entry(parent_state)
                .or_insert_with(Vec::new)
                .push((attr_name, new_id));

            Ok(new_id)
        }
    }

    /// Allocate a new node with the given name.
    fn alloc_node(&mut self, name: Atom) -> StateId {
        let id = self.next_state;
        self.next_state += 1;
        self.nodes.push(TrieNode::new(name));
        id
    }

    /// Finalize the trie by converting temporary children into optimized containers.
    fn finalize(mut self) -> Result<PathTrie> {
        // Convert temporary children into optimized containers
        for (state_id, children) in self.element_children_temp {
            let node = &mut self.nodes[state_id as usize];
            node.element_children = ChildContainer::from_children(children);
        }

        for (state_id, children) in self.attribute_children_temp {
            let node = &mut self.nodes[state_id as usize];
            node.attribute_children = ChildContainer::from_children(children);
        }

        Ok(PathTrie {
            nodes: self.nodes,
            root_id: 0,
            field_configs: self.field_configs,
            table_configs: self.table_configs,
            max_depth: self.max_depth,
        })
    }
}

/// Parse an XML path into segments (Atoms).
fn parse_path(path: &str) -> Vec<Atom> {
    path.trim_start_matches('/')
        .split('/')
        .filter(|s| !s.is_empty())
        .map(Atom::from)
        .collect()
}

/// Parse a field path, separating attributes (segments starting with '@').
/// Returns (segments, is_attribute) where is_attribute is true if the last segment is an attribute.
fn parse_field_path(path: &str) -> (Vec<Atom>, bool) {
    let segments: Vec<Atom> = path
        .trim_start_matches('/')
        .split('/')
        .filter(|s| !s.is_empty())
        .map(|s| {
            if s.starts_with('@') {
                Atom::from(&s[1..]) // Strip '@' prefix
            } else {
                Atom::from(s)
            }
        })
        .collect();

    let is_attribute = path
        .split('/')
        .filter(|s| !s.is_empty())
        .last()
        .map(|s| s.starts_with('@'))
        .unwrap_or(false);

    (segments, is_attribute)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{DType, FieldConfigBuilder};

    #[test]
    fn test_parse_path() {
        let path = "/document/data/sensors";
        let segments = parse_path(path);
        assert_eq!(segments.len(), 3);
        assert_eq!(segments[0].as_ref(), "document");
        assert_eq!(segments[1].as_ref(), "data");
        assert_eq!(segments[2].as_ref(), "sensors");
    }

    #[test]
    fn test_parse_field_path_element() {
        let path = "/document/data/sensors/sensor/value";
        let (segments, is_attr) = parse_field_path(path);
        assert_eq!(segments.len(), 5);
        assert!(!is_attr);
    }

    #[test]
    fn test_parse_field_path_attribute() {
        let path = "/document/data/sensors/sensor/@id";
        let (segments, is_attr) = parse_field_path(path);
        assert_eq!(segments.len(), 5);
        assert_eq!(segments[4].as_ref(), "id");
        assert!(is_attr);
    }

    #[test]
    fn test_trie_simple_path() {
        let config = Config {
            parser_options: Default::default(),
            tables: vec![TableConfig::new(
                "test",
                "/root/items",
                vec![],
                vec![
                    FieldConfigBuilder::new("value", "/root/items/item/value", DType::Int32)
                        .build(),
                ],
            )],
        };

        let trie = PathTrieBuilder::from_config(&config).unwrap();

        assert_eq!(trie.nodes.len(), 5); // root, root, items, item, value
        assert_eq!(trie.table_configs.len(), 1);
        assert_eq!(trie.field_configs.len(), 1);
    }

    #[test]
    fn test_trie_with_attributes() {
        let config = Config {
            parser_options: Default::default(),
            tables: vec![TableConfig::new(
                "test",
                "/root/items",
                vec![],
                vec![
                    FieldConfigBuilder::new("id", "/root/items/item/@id", DType::Utf8).build(),
                    FieldConfigBuilder::new("value", "/root/items/item/value", DType::Int32)
                        .build(),
                ],
            )],
        };

        let trie = PathTrieBuilder::from_config(&config).unwrap();

        // Verify attribute transition works
        let root = trie.root_id();
        let root_state = trie.transition_element(root, &Atom::from("root"));
        let items_state = trie.transition_element(root_state, &Atom::from("items"));
        let item_state = trie.transition_element(items_state, &Atom::from("item"));

        assert!(trie.has_attributes(item_state));

        let id_state = trie.transition_attribute(item_state, &Atom::from("id"));
        assert_ne!(id_state, UNMATCHED_STATE);
        assert_eq!(trie.get_field_id(id_state), Some(0));

        let value_state = trie.transition_element(item_state, &Atom::from("value"));
        assert_ne!(value_state, UNMATCHED_STATE);
        assert_eq!(trie.get_field_id(value_state), Some(1));
    }

    #[test]
    fn test_child_container_optimization() {
        // Single child
        let single = ChildContainer::from_children(vec![(Atom::from("a"), 1)]);
        matches!(single, ChildContainer::Single(_, _));

        // SmallVec (2-4 children)
        let small = ChildContainer::from_children(vec![
            (Atom::from("a"), 1),
            (Atom::from("b"), 2),
            (Atom::from("c"), 3),
        ]);
        matches!(small, ChildContainer::SmallVec(_));

        // SortedVec (5-32 children)
        let mut children = Vec::new();
        for i in 0..10 {
            children.push((Atom::from(format!("child{}", i)), i));
        }
        let sorted = ChildContainer::from_children(children);
        matches!(sorted, ChildContainer::SortedVec(_));

        // Hash (>32 children)
        let mut many_children = Vec::new();
        for i in 0..50 {
            many_children.push((Atom::from(format!("child{}", i)), i));
        }
        let hash = ChildContainer::from_children(many_children);
        matches!(hash, ChildContainer::Hash(_));
    }

    #[test]
    fn test_unmatched_state() {
        let config = Config {
            parser_options: Default::default(),
            tables: vec![TableConfig::new(
                "test",
                "/root/items",
                vec![],
                vec![
                    FieldConfigBuilder::new("value", "/root/items/item/value", DType::Int32)
                        .build(),
                ],
            )],
        };

        let trie = PathTrieBuilder::from_config(&config).unwrap();

        let root = trie.root_id();
        let unknown = trie.transition_element(root, &Atom::from("unknown"));
        assert_eq!(unknown, UNMATCHED_STATE);

        // Further transitions from unmatched remain unmatched
        let still_unknown = trie.transition_element(unknown, &Atom::from("anything"));
        assert_eq!(still_unknown, UNMATCHED_STATE);
    }

    #[test]
    fn test_multiple_tables_shared_prefix() {
        let config = Config {
            parser_options: Default::default(),
            tables: vec![
                TableConfig::new(
                    "table1",
                    "/root/data/table1",
                    vec![],
                    vec![
                        FieldConfigBuilder::new("field1", "/root/data/table1/value", DType::Int32)
                            .build(),
                    ],
                ),
                TableConfig::new(
                    "table2",
                    "/root/data/table2",
                    vec![],
                    vec![
                        FieldConfigBuilder::new(
                            "field2",
                            "/root/data/table2/value",
                            DType::Float64,
                        )
                        .build(),
                    ],
                ),
            ],
        };

        let trie = PathTrieBuilder::from_config(&config).unwrap();

        // Navigate to shared prefix
        let mut state = trie.root_id();
        state = trie.transition_element(state, &Atom::from("root"));
        state = trie.transition_element(state, &Atom::from("data"));

        // Branch to table1
        let table1_state = trie.transition_element(state, &Atom::from("table1"));
        assert!(trie.is_table_root(table1_state));
        assert_eq!(trie.get_table_id(table1_state), Some(0));

        // Branch to table2
        let table2_state = trie.transition_element(state, &Atom::from("table2"));
        assert!(trie.is_table_root(table2_state));
        assert_eq!(trie.get_table_id(table2_state), Some(1));

        // Verify fields are distinct
        let value1_state = trie.transition_element(table1_state, &Atom::from("value"));
        assert_eq!(trie.get_field_id(value1_state), Some(0));

        let value2_state = trie.transition_element(table2_state, &Atom::from("value"));
        assert_eq!(trie.get_field_id(value2_state), Some(1));
    }

    #[test]
    fn test_nested_tables() {
        let config = Config {
            parser_options: Default::default(),
            tables: vec![
                TableConfig::new(
                    "parent",
                    "/root/parent",
                    vec![],
                    vec![
                        FieldConfigBuilder::new("parent_id", "/root/parent/@id", DType::Utf8)
                            .build(),
                    ],
                ),
                TableConfig::new(
                    "child",
                    "/root/parent/children",
                    vec!["parent".to_string(), "child".to_string()],
                    vec![
                        FieldConfigBuilder::new(
                            "child_value",
                            "/root/parent/children/child/value",
                            DType::Int32,
                        )
                        .build(),
                    ],
                ),
            ],
        };

        let trie = PathTrieBuilder::from_config(&config).unwrap();

        // Navigate to parent
        let mut state = trie.root_id();
        state = trie.transition_element(state, &Atom::from("root"));
        state = trie.transition_element(state, &Atom::from("parent"));
        assert!(trie.is_table_root(state));
        assert_eq!(trie.get_table_id(state), Some(0));

        // Check parent attribute
        let parent_id_state = trie.transition_attribute(state, &Atom::from("id"));
        assert_eq!(trie.get_field_id(parent_id_state), Some(0));

        // Navigate to nested table
        state = trie.transition_element(state, &Atom::from("children"));
        assert!(trie.is_table_root(state));
        assert_eq!(trie.get_table_id(state), Some(1));

        // Navigate to field in nested table
        state = trie.transition_element(state, &Atom::from("child"));
        state = trie.transition_element(state, &Atom::from("value"));
        assert_eq!(trie.get_field_id(state), Some(1));
    }

    #[test]
    fn test_deep_nesting() {
        // Create a deeply nested path (10 levels for table, 11 for field)
        let config = Config {
            parser_options: Default::default(),
            tables: vec![TableConfig::new(
                "deep",
                "/a/b/c/d/e/f/g/h/i/j",
                vec![],
                vec![
                    FieldConfigBuilder::new("value", "/a/b/c/d/e/f/g/h/i/j/value", DType::Int32)
                        .build(),
                ],
            )],
        };

        let trie = PathTrieBuilder::from_config(&config).unwrap();
        // Max depth is 11 because field path includes "value" after the table path
        assert_eq!(trie.max_depth(), 11);

        // Navigate through all levels
        let mut state = trie.root_id();
        for letter in &["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"] {
            state = trie.transition_element(state, &Atom::from(*letter));
            assert_ne!(state, UNMATCHED_STATE);
        }

        assert!(trie.is_table_root(state));
    }

    #[test]
    fn test_many_attributes() {
        // Test element with many attributes
        let mut fields = Vec::new();
        for i in 0..50 {
            fields.push(
                FieldConfigBuilder::new(
                    &format!("attr{}", i),
                    &format!("/root/element/@attr{}", i),
                    DType::Utf8,
                )
                .build(),
            );
        }

        let config = Config {
            parser_options: Default::default(),
            tables: vec![TableConfig::new("test", "/root", vec![], fields)],
        };

        let trie = PathTrieBuilder::from_config(&config).unwrap();

        let mut state = trie.root_id();
        state = trie.transition_element(state, &Atom::from("root"));
        state = trie.transition_element(state, &Atom::from("element"));

        assert!(trie.has_attributes(state));

        // Verify all attributes are accessible
        for i in 0..50 {
            let attr_state = trie.transition_attribute(state, &Atom::from(format!("attr{}", i)));
            assert_ne!(attr_state, UNMATCHED_STATE);
            assert_eq!(trie.get_field_id(attr_state), Some(i as FieldId));
        }
    }

    #[test]
    fn test_many_children() {
        // Test element with many child elements (should use Hash container)
        let mut fields = Vec::new();
        for i in 0..100 {
            fields.push(
                FieldConfigBuilder::new(
                    &format!("field{}", i),
                    &format!("/root/elements/elem{}", i),
                    DType::Int32,
                )
                .build(),
            );
        }

        let config = Config {
            parser_options: Default::default(),
            tables: vec![TableConfig::new("test", "/root", vec![], fields)],
        };

        let trie = PathTrieBuilder::from_config(&config).unwrap();

        let mut state = trie.root_id();
        state = trie.transition_element(state, &Atom::from("root"));
        state = trie.transition_element(state, &Atom::from("elements"));

        // Verify all children are accessible
        for i in 0..100 {
            let child_state = trie.transition_element(state, &Atom::from(format!("elem{}", i)));
            assert_ne!(child_state, UNMATCHED_STATE);
            assert_eq!(trie.get_field_id(child_state), Some(i as FieldId));
        }
    }

    #[test]
    fn test_all_data_types() {
        let config = Config {
            parser_options: Default::default(),
            tables: vec![TableConfig::new(
                "types",
                "/root",
                vec![],
                vec![
                    FieldConfigBuilder::new("bool", "/root/bool", DType::Boolean).build(),
                    FieldConfigBuilder::new("i8", "/root/i8", DType::Int8).build(),
                    FieldConfigBuilder::new("u8", "/root/u8", DType::UInt8).build(),
                    FieldConfigBuilder::new("i16", "/root/i16", DType::Int16).build(),
                    FieldConfigBuilder::new("u16", "/root/u16", DType::UInt16).build(),
                    FieldConfigBuilder::new("i32", "/root/i32", DType::Int32).build(),
                    FieldConfigBuilder::new("u32", "/root/u32", DType::UInt32).build(),
                    FieldConfigBuilder::new("i64", "/root/i64", DType::Int64).build(),
                    FieldConfigBuilder::new("u64", "/root/u64", DType::UInt64).build(),
                    FieldConfigBuilder::new("f32", "/root/f32", DType::Float32).build(),
                    FieldConfigBuilder::new("f64", "/root/f64", DType::Float64).build(),
                    FieldConfigBuilder::new("str", "/root/str", DType::Utf8).build(),
                ],
            )],
        };

        let trie = PathTrieBuilder::from_config(&config).unwrap();
        assert_eq!(trie.field_configs.len(), 12);

        // Verify all fields are accessible with correct data types
        let mut state = trie.root_id();
        state = trie.transition_element(state, &Atom::from("root"));

        let types = vec![
            ("bool", DType::Boolean),
            ("i8", DType::Int8),
            ("u8", DType::UInt8),
            ("i16", DType::Int16),
            ("u16", DType::UInt16),
            ("i32", DType::Int32),
            ("u32", DType::UInt32),
            ("i64", DType::Int64),
            ("u64", DType::UInt64),
            ("f32", DType::Float32),
            ("f64", DType::Float64),
            ("str", DType::Utf8),
        ];

        for (i, (name, dtype)) in types.iter().enumerate() {
            let field_state = trie.transition_element(state, &Atom::from(*name));
            assert_ne!(field_state, UNMATCHED_STATE);
            let field_id = trie.get_field_id(field_state).unwrap();
            assert_eq!(trie.field_configs[field_id as usize].data_type, *dtype);
        }
    }

    #[test]
    fn test_realistic_sensor_config() {
        // Simulate a realistic sensor data configuration
        let config = Config {
            parser_options: Default::default(),
            tables: vec![
                TableConfig::new(
                    "document",
                    "/document",
                    vec![],
                    vec![
                        FieldConfigBuilder::new(
                            "timestamp",
                            "/document/header/timestamp",
                            DType::Utf8,
                        )
                        .build(),
                        FieldConfigBuilder::new("version", "/document/header/version", DType::Utf8)
                            .build(),
                    ],
                ),
                TableConfig::new(
                    "sensors",
                    "/document/data/sensors",
                    vec!["sensor".to_string()],
                    vec![
                        FieldConfigBuilder::new(
                            "id",
                            "/document/data/sensors/sensor/@id",
                            DType::Utf8,
                        )
                        .build(),
                        FieldConfigBuilder::new(
                            "type",
                            "/document/data/sensors/sensor/@type",
                            DType::Utf8,
                        )
                        .build(),
                        FieldConfigBuilder::new(
                            "location",
                            "/document/data/sensors/sensor/metadata/location",
                            DType::Utf8,
                        )
                        .build(),
                    ],
                ),
                TableConfig::new(
                    "measurements",
                    "/document/data/sensors/sensor/measurements",
                    vec!["sensor".to_string(), "measurement".to_string()],
                    vec![
                        FieldConfigBuilder::new(
                            "timestamp",
                            "/document/data/sensors/sensor/measurements/measurement/@timestamp_ms",
                            DType::UInt64,
                        )
                        .build(),
                        FieldConfigBuilder::new(
                            "value",
                            "/document/data/sensors/sensor/measurements/measurement/value",
                            DType::Float64,
                        )
                        .build(),
                        FieldConfigBuilder::new(
                            "quality",
                            "/document/data/sensors/sensor/measurements/measurement/quality",
                            DType::Int8,
                        )
                        .build(),
                    ],
                ),
            ],
        };

        let trie = PathTrieBuilder::from_config(&config).unwrap();

        // Verify structure
        assert_eq!(trie.table_configs.len(), 3);
        assert_eq!(trie.field_configs.len(), 8);

        // Navigate through realistic path
        let mut state = trie.root_id();
        state = trie.transition_element(state, &Atom::from("document"));
        assert!(trie.is_table_root(state));

        // Check header fields
        let header_state = trie.transition_element(state, &Atom::from("header"));
        let ts_state = trie.transition_element(header_state, &Atom::from("timestamp"));
        assert!(trie.get_field_id(ts_state).is_some());

        // Navigate to sensors table
        state = trie.transition_element(state, &Atom::from("data"));
        state = trie.transition_element(state, &Atom::from("sensors"));
        assert!(trie.is_table_root(state));

        // Check sensor attributes and fields
        let sensor_state = trie.transition_element(state, &Atom::from("sensor"));
        assert!(trie.has_attributes(sensor_state));

        let id_state = trie.transition_attribute(sensor_state, &Atom::from("id"));
        assert!(trie.get_field_id(id_state).is_some());

        // Navigate to measurements table
        let measurements_state = trie.transition_element(sensor_state, &Atom::from("measurements"));
        assert!(trie.is_table_root(measurements_state));

        let measurement_state =
            trie.transition_element(measurements_state, &Atom::from("measurement"));
        assert!(trie.has_attributes(measurement_state));
    }

    #[test]
    fn test_empty_config() {
        let config = Config {
            parser_options: Default::default(),
            tables: vec![],
        };

        let trie = PathTrieBuilder::from_config(&config).unwrap();

        // Should have only root node
        assert_eq!(trie.nodes.len(), 1);
        assert_eq!(trie.table_configs.len(), 0);
        assert_eq!(trie.field_configs.len(), 0);
        assert_eq!(trie.max_depth(), 0);
    }

    #[test]
    fn test_single_root_table() {
        let config = Config {
            parser_options: Default::default(),
            tables: vec![TableConfig::new(
                "root",
                "/",
                vec![],
                vec![FieldConfigBuilder::new("value", "/value", DType::Int32).build()],
            )],
        };

        let trie = PathTrieBuilder::from_config(&config).unwrap();

        let root = trie.root_id();
        assert!(trie.is_table_root(root));
        assert_eq!(trie.get_table_id(root), Some(0));
    }

    #[test]
    fn test_state_stack_simulation() {
        let config = Config {
            parser_options: Default::default(),
            tables: vec![TableConfig::new(
                "test",
                "/root/items",
                vec![],
                vec![
                    FieldConfigBuilder::new("value", "/root/items/item/value", DType::Int32)
                        .build(),
                ],
            )],
        };

        let trie = PathTrieBuilder::from_config(&config).unwrap();

        // Simulate XML parsing with state stack
        let mut state_stack: Vec<StateId> = vec![trie.root_id()];

        // <root>
        let mut current = *state_stack.last().unwrap();
        current = trie.transition_element(current, &Atom::from("root"));
        state_stack.push(current);

        // <items>
        current = *state_stack.last().unwrap();
        current = trie.transition_element(current, &Atom::from("items"));
        state_stack.push(current);
        assert!(trie.is_table_root(current));

        // <item>
        current = *state_stack.last().unwrap();
        current = trie.transition_element(current, &Atom::from("item"));
        state_stack.push(current);

        // <value>
        current = *state_stack.last().unwrap();
        current = trie.transition_element(current, &Atom::from("value"));
        state_stack.push(current);
        assert!(trie.get_field_id(current).is_some());

        // Simulate text content
        assert_eq!(trie.get_field_id(current), Some(0));

        // </value>
        state_stack.pop();
        // </item>
        state_stack.pop();
        // </items>
        state_stack.pop();
        // </root>
        state_stack.pop();

        assert_eq!(state_stack.len(), 1); // Only root remains
        assert_eq!(state_stack[0], trie.root_id());
    }
}
