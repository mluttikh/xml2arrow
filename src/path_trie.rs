//! # Path Trie for High-Performance XML Path Matching
//!
//! This module implements a trie (prefix tree) data structure that replaces the runtime
//! `XmlPath` construction and hash-based field lookups used in the original parser.
//!
//! ## Why a Trie?
//!
//! The original hash-based parser had to:
//! 1. Build an XmlPath dynamically as it parsed (allocating strings)
//! 2. Hash the entire path to look up fields (O(n) where n = path length)
//! 3. Do this for EVERY element encountered during parsing
//!
//! The trie approach:
//! 1. Pre-builds the entire path structure once from the config
//! 2. Uses integer state IDs instead of strings (4 bytes vs many)
//! 3. Transitions between states in O(1) time (just an array lookup)
//! 4. Zero allocations during parsing (just push/pop state IDs on a stack)
//!
//! ## Core Concept
//!
//! Think of the trie as a finite state machine where:
//! - Each node is a state representing a position in the XML tree
//! - Transitions happen when we encounter an element or attribute
//! - Each state knows if it represents a table root, field, or just structure
//!
//! Example XML path: `/document/data/sensor/value`
//!
//! ```text
//! State 0 (root)
//!   ↓ "document"
//! State 1 (/document)
//!   ↓ "data"
//! State 2 (/document/data)
//!   ↓ "sensor"
//! State 3 (/document/data/sensor) ← might be a table root
//!   ↓ "value"
//! State 4 (/document/data/sensor/value) ← might be a field
//! ```
//!
//! During parsing, we just track the current state ID and transition as we
//! encounter elements. No string operations, no allocations, just integer lookups!
//!
//! ## Memory Layout
//!
//! The trie is stored as a flat Vec of nodes. Each node contains:
//! - Flags (1 byte): is_table_root, has_field, has_attributes, is_level_element
//! - Optional field_id (4 bytes)
//! - Optional table_id (4 bytes)
//! - Child element map (adaptive storage based on fanout)
//! - Child attribute map (adaptive storage)
//!
//! Total per node: ~80-100 bytes depending on number of children.
//! For a typical config (50 fields, 5 tables, depth 6): ~5KB total.

use crate::config::{Config, FieldConfig, TableConfig};
use crate::errors::{Error, Result};
use fxhash::FxHashMap;
use smallvec::SmallVec;
use std::collections::HashMap;
use string_cache::DefaultAtom as Atom;

// ============================================================================
// Type Aliases - Making the code more readable
// ============================================================================

/// **StateId**: Integer identifier for a trie node state.
///
/// Think of this as a "position in the XML tree". During parsing, we maintain
/// a stack of StateIds representing our current path. For example:
///
/// ```text
/// Stack: [0, 12, 45, 67]
/// Means: root → document → data → sensor
/// ```
///
/// Using u32 means each state is just 4 bytes, and we can have up to 4 billion
/// unique states (way more than any reasonable XML schema would need).
pub type StateId = u32;

/// **FieldId**: Integer identifier for a field builder.
///
/// This is a global index across ALL fields in ALL tables. Field 0 might be
/// in table 0, field 1 might be in table 0, field 2 might be in table 1, etc.
///
/// We use a separate `field_to_table` mapping to know which table owns each field.
pub type FieldId = u32;

/// **TableId**: Integer identifier for a table builder.
///
/// Simple index into the tables array. Table 0 is the first table in the config,
/// table 1 is the second, etc.
pub type TableId = u32;

/// **UNMATCHED_STATE**: Sentinel value indicating no valid state exists.
///
/// When we try to transition to a non-existent child (e.g., looking for element
/// "foo" but the current state has no child named "foo"), we return this special
/// value. The parser checks for this and knows to ignore the subtree.
///
/// We use u32::MAX because it's guaranteed to never be a valid state ID
/// (we'd run out of memory long before allocating 4 billion nodes).
pub const UNMATCHED_STATE: StateId = StateId::MAX;

// ============================================================================
// NodeFlags - Compact Boolean Storage
// ============================================================================

/// **NodeFlags**: Compact storage for boolean properties of a trie node.
///
/// Instead of using 4 separate bool fields (each would be 1 byte due to alignment),
/// we pack all flags into a single u8 (1 byte). This saves 3 bytes per node, which
/// adds up when you have hundreds or thousands of nodes.
///
/// Each flag is a bit position:
/// - Bit 0: IS_TABLE_ROOT
/// - Bit 1: HAS_FIELD
/// - Bit 2: HAS_ATTRIBUTES
/// - Bit 3: IS_LEVEL_ELEMENT
/// - Bits 4-7: unused (available for future flags)
#[derive(Debug, Clone, Copy, Default)]
struct NodeFlags {
    bits: u8,
}

impl NodeFlags {
    // Bit masks for each flag
    const IS_TABLE_ROOT: u8 = 0b0000_0001;
    const HAS_FIELD: u8 = 0b0000_0010;
    const HAS_ATTRIBUTES: u8 = 0b0000_0100;
    const IS_LEVEL_ELEMENT: u8 = 0b0000_1000;

    /// Check if this node represents a table root.
    ///
    /// A table root is the starting point for collecting rows of a table.
    /// Example: for table at `/document/data/sensors`, state at that path
    /// would have this flag set.
    #[inline]
    fn is_table_root(&self) -> bool {
        self.bits & Self::IS_TABLE_ROOT != 0
    }

    /// Check if this node represents a field (has data to extract).
    #[inline]
    #[allow(dead_code)]
    fn has_field(&self) -> bool {
        self.bits & Self::HAS_FIELD != 0
    }

    /// Check if this node has attribute children.
    ///
    /// This is an optimization: if a node has no attributes, we can skip
    /// the attribute parsing logic entirely for that element.
    #[inline]
    fn has_attributes(&self) -> bool {
        self.bits & Self::HAS_ATTRIBUTES != 0
    }

    /// Check if this node is a "level element" for row boundary detection.
    ///
    /// Level elements are specified in the table config's `levels` array.
    /// When we close a level element, we know we've finished a row.
    ///
    /// Example: table with `levels: ["sensor", "measurement"]`
    /// - The "sensor" state gets this flag set
    /// - The "measurement" state gets this flag set
    /// - When we close </measurement>, we end a row
    #[inline]
    fn is_level_element(&self) -> bool {
        self.bits & Self::IS_LEVEL_ELEMENT != 0
    }

    /// Mark this node as a table root.
    #[inline]
    fn set_table_root(&mut self) {
        self.bits |= Self::IS_TABLE_ROOT;
    }

    /// Mark this node as having a field.
    #[inline]
    fn set_field(&mut self) {
        self.bits |= Self::HAS_FIELD;
    }

    /// Mark this node as having attribute children.
    #[inline]
    fn set_has_attributes(&mut self) {
        self.bits |= Self::HAS_ATTRIBUTES;
    }

    /// Mark this node as a level element.
    #[inline]
    fn set_level_element(&mut self) {
        self.bits |= Self::IS_LEVEL_ELEMENT;
    }
}

// ============================================================================
// ChildContainer - Adaptive Storage Strategy
// ============================================================================

/// **ChildContainer**: Adaptive storage for child nodes based on fanout.
///
/// This is a key optimization! Different nodes have different numbers of children:
/// - Most nodes have 0-1 children (linear paths like `/document/header/title`)
/// - Some nodes have 2-4 children (small branching)
/// - Few nodes have many children (like a <record> with 50 different field elements)
///
/// We use different storage strategies based on the number of children:
///
/// 1. **Empty** (0 children): No storage at all, just a tag. ~0 bytes overhead.
///
/// 2. **Single** (1 child): Store the name and ID inline. ~16 bytes total.
///    Most common case! Paths like `/document/data/sensor` are just 3 Single nodes.
///
/// 3. **SmallVec** (2-4 children): Stack-allocated array, linear search.
///    Small enough that linear search is faster than hash lookup due to cache locality.
///    ~80 bytes total.
///
/// 4. **SortedVec** (5-32 children): Heap-allocated, binary search.
///    O(log n) lookup, still cache-friendly. ~200+ bytes.
///
/// 5. **Hash** (32+ children): FxHashMap for O(1) lookup.
///    Only used when we have many children. ~1KB+.
///
/// This adaptive approach means:
/// - Small configs use almost no memory
/// - Large configs only pay for what they need
/// - Hot paths (single children) are extremely fast
#[derive(Debug, Clone)]
enum ChildContainer {
    /// No children at all. Leaf nodes or nodes with only attributes.
    Empty,

    /// Exactly one child. Most common case for linear paths.
    /// Stores (child_name, child_state_id) inline.
    Single(Atom, StateId),

    /// 2-4 children. Stack-allocated, linear search.
    /// SmallVec<[T; 4]> means "store up to 4 items on the stack, spill to heap if more".
    SmallVec(SmallVec<[(Atom, StateId); 4]>),

    /// 5-32 children. Heap-allocated, kept sorted for binary search.
    SortedVec(Vec<(Atom, StateId)>),

    /// 32+ children. Hash map for O(1) average case lookup.
    /// Uses FxHashMap which is faster than std HashMap for small keys.
    Hash(FxHashMap<Atom, StateId>),
}

impl ChildContainer {
    /// Look up a child state by name.
    ///
    /// This is called during XML parsing when we encounter an element/attribute.
    /// Returns the StateId of the child, or None if no such child exists.
    ///
    /// Performance characteristics:
    /// - Empty: O(1) - just return None
    /// - Single: O(1) - one comparison
    /// - SmallVec: O(n) - linear search, but n ≤ 4 and cache-friendly
    /// - SortedVec: O(log n) - binary search
    /// - Hash: O(1) average - hash lookup
    #[inline]
    fn get(&self, key: &Atom) -> Option<StateId> {
        match self {
            ChildContainer::Empty => None,

            ChildContainer::Single(a, id) => {
                // Fast path: just one comparison
                if a == key { Some(*id) } else { None }
            }

            ChildContainer::SmallVec(vec) => {
                // Linear search through 2-4 items
                // This is faster than hashing for such small counts!
                vec.iter().find(|(a, _)| a == key).map(|(_, id)| *id)
            }

            ChildContainer::SortedVec(vec) => {
                // Binary search - O(log n)
                vec.binary_search_by(|(a, _)| a.cmp(key))
                    .ok()
                    .map(|idx| vec[idx].1)
            }

            ChildContainer::Hash(map) => {
                // Hash lookup - O(1) average
                map.get(key).copied()
            }
        }
    }

    /// Create the optimal container type from a list of children.
    ///
    /// Called during trie construction to choose the best storage strategy
    /// based on the number of children.
    fn from_children(children: Vec<(Atom, StateId)>) -> Self {
        match children.len() {
            0 => ChildContainer::Empty,

            1 => {
                // SAFETY: We just checked len() == 1
                let (name, id) = children.into_iter().next().unwrap();
                ChildContainer::Single(name, id)
            }

            2..=4 => {
                // SmallVec - stack allocated
                ChildContainer::SmallVec(children.into_iter().collect())
            }

            5..=32 => {
                // SortedVec - sort once for binary search later
                let mut vec = children;
                vec.sort_by(|(a, _), (b, _)| a.cmp(b));
                ChildContainer::SortedVec(vec)
            }

            _ => {
                // Hash map for many children
                ChildContainer::Hash(children.into_iter().collect())
            }
        }
    }
}

impl Default for ChildContainer {
    fn default() -> Self {
        ChildContainer::Empty
    }
}

// ============================================================================
// TrieNode - A Single Node in the Trie
// ============================================================================

/// **TrieNode**: A single node in the path trie representing one XML element or attribute.
///
/// Each node in our trie represents a specific position in the XML tree.
/// For example, if we have a path `/document/data/sensor`, we'd have three nodes:
///
/// ```text
/// Node 0 (root):
///   name: ""
///   element_children: {"document" → Node 1}
///
/// Node 1:
///   name: "document"
///   element_children: {"data" → Node 2}
///
/// Node 2:
///   name: "data"
///   element_children: {"sensor" → Node 3}
///
/// Node 3:
///   name: "sensor"
///   flags: IS_TABLE_ROOT
///   table_id: Some(0)
/// ```
///
/// The node stores:
/// - Its own name (for debugging, not used during parsing)
/// - Flags indicating special properties (table root, field, has attributes, level element)
/// - Optional field_id if this node represents a field to extract
/// - Optional table_id if this node represents a table root
/// - Maps of children (both element and attribute children)
#[derive(Debug, Clone)]
struct TrieNode {
    /// The element or attribute name (without '@' prefix for attributes).
    /// Only used for debugging - during parsing we use the StateId, not the name.
    #[allow(dead_code)]
    name: Atom,

    /// Compact flags indicating node properties (packed into 1 byte).
    flags: NodeFlags,

    /// If this node represents a field, its global field ID.
    /// Example: The node for `/document/data/sensor/@id` would have a field_id.
    field_id: Option<FieldId>,

    /// If this node represents a table root, its table ID.
    /// Example: The node for `/document/data/sensors` might have table_id: Some(1).
    table_id: Option<TableId>,

    /// Child element nodes. Maps element names to child state IDs.
    /// Example: {"sensor" → 42, "metadata" → 43}
    element_children: ChildContainer,

    /// Child attribute nodes. Maps attribute names to child state IDs.
    /// Example: {"id" → 50, "type" → 51}
    /// Note: Attribute names are stored WITHOUT the '@' prefix.
    attribute_children: ChildContainer,
}

impl TrieNode {
    /// Create a new empty trie node with the given name.
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

// ============================================================================
// PathTrie - The Complete Trie Structure
// ============================================================================

/// **PathTrie**: The complete path trie structure.
///
/// This is the main data structure used during XML parsing. It's built once
/// from the configuration and then used for millions of lookups.
///
/// **Key Design Decision**: The trie is immutable after construction.
/// This allows us to:
/// - Share it safely across threads (it's fully Send + Sync)
/// - Make aggressive optimizations (no need to handle modifications)
/// - Use simpler, faster data structures
///
/// **Memory Layout**:
/// All nodes are stored in a flat Vec. This means:
/// - Excellent cache locality (nodes are contiguous in memory)
/// - Fast allocation (single Vec allocation, not millions of small ones)
/// - Simple state IDs (just indices into the Vec)
///
/// Example structure:
/// ```text
/// nodes[0] = root node
/// nodes[1] = /document
/// nodes[2] = /document/data
/// nodes[3] = /document/data/sensor
/// ...
/// ```
#[derive(Debug, Clone)]
pub struct PathTrie {
    /// All trie nodes, indexed by StateId.
    /// nodes[0] is always the root node.
    nodes: Vec<TrieNode>,

    /// The root state ID (always 0, but we store it for clarity).
    root_id: StateId,

    /// Mapping from FieldId to original field config.
    /// Used for error messages and debugging. Not used during hot path parsing.
    pub field_configs: Vec<FieldConfig>,

    /// Mapping from TableId to original table config.
    /// Used to check table properties (like `levels` array) during parsing.
    pub table_configs: Vec<TableConfig>,

    /// Mapping from FieldId to TableId (which table owns which field).
    ///
    /// This is CRITICAL for multi-table support! Without this, we wouldn't know
    /// which table builder to send field values to.
    ///
    /// Example: field_to_table[5] = 2 means field 5 belongs to table 2.
    ///
    /// Why is this needed? Consider:
    /// - Table 0 at `/document/data/sensors` with fields at `/document/data/sensors/sensor/@id`
    /// - Table 1 at `/document/data/sensors/sensor/measurements` with fields at `.../measurement/value`
    ///
    /// When we encounter `/document/data/sensors/sensor/@id`, we need to know it goes to table 0,
    /// not table 1, even though table 1's path is a prefix of table 0's path!
    pub field_to_table: Vec<TableId>,

    /// Maximum depth of any path in the trie.
    /// Used to pre-allocate the state stack with the right capacity.
    max_depth: u16,
}

impl PathTrie {
    /// Get a reference to a node by StateId.
    ///
    /// Returns None for UNMATCHED_STATE or out-of-bounds IDs.
    /// This is an internal helper, not used in the hot path.
    #[inline]
    fn get_node(&self, state_id: StateId) -> Option<&TrieNode> {
        if state_id == UNMATCHED_STATE {
            None
        } else {
            self.nodes.get(state_id as usize)
        }
    }

    /// Get the root state ID.
    ///
    /// The parser starts here at the beginning of the XML document.
    /// Always returns 0, but using a method makes the code more readable.
    #[inline]
    pub fn root_id(&self) -> StateId {
        self.root_id
    }

    /// Check if a state represents a table root.
    ///
    /// Called when we encounter a start element to see if we should begin
    /// collecting rows for a table.
    #[inline]
    pub fn is_table_root(&self, state_id: StateId) -> bool {
        self.get_node(state_id)
            .map(|n| n.flags.is_table_root())
            .unwrap_or(false)
    }

    /// Get the table ID for a state, if it's a table root.
    ///
    /// Returns Some(table_id) if this state is a table root, None otherwise.
    #[inline]
    pub fn get_table_id(&self, state_id: StateId) -> Option<TableId> {
        self.get_node(state_id).and_then(|n| n.table_id)
    }

    /// Get the field ID for a state, if it's a field.
    ///
    /// Returns Some(field_id) if this state represents a field to extract,
    /// None otherwise.
    #[inline]
    pub fn get_field_id(&self, state_id: StateId) -> Option<FieldId> {
        self.get_node(state_id).and_then(|n| n.field_id)
    }

    /// Transition to a child element state.
    ///
    /// **This is called millions of times during parsing!** It's the hottest path
    /// in the entire trie implementation.
    ///
    /// Given the current state and an element name, returns the state ID of the
    /// child element, or UNMATCHED_STATE if no such child exists.
    ///
    /// Example:
    /// ```text
    /// current_state = 2 (at /document/data)
    /// element_name = "sensor"
    /// returns: 3 (the state for /document/data/sensor)
    /// ```
    #[inline]
    pub fn transition_element(&self, state_id: StateId, element_name: &Atom) -> StateId {
        self.get_node(state_id)
            .and_then(|n| n.element_children.get(element_name))
            .unwrap_or(UNMATCHED_STATE)
    }

    /// Transition to a child attribute state.
    ///
    /// Similar to transition_element, but for attributes.
    /// Called when we encounter an attribute like `id="123"`.
    ///
    /// Note: The attr_name should NOT include the '@' prefix.
    /// The parser strips that before calling this.
    #[inline]
    pub fn transition_attribute(&self, state_id: StateId, attr_name: &Atom) -> StateId {
        self.get_node(state_id)
            .and_then(|n| n.attribute_children.get(attr_name))
            .unwrap_or(UNMATCHED_STATE)
    }

    /// Check if a state has attributes.
    ///
    /// This is an optimization: if a node has no attributes, the parser can skip
    /// the entire attribute parsing logic for that element, saving time.
    #[inline]
    pub fn has_attributes(&self, state_id: StateId) -> bool {
        self.get_node(state_id)
            .map(|n| n.flags.has_attributes())
            .unwrap_or(false)
    }

    /// Check if a state is a level element (used for row end detection).
    ///
    /// Level elements are specified in the table config's `levels` array.
    /// When we close a level element, we know we've finished collecting fields
    /// for one row.
    ///
    /// Example: table with `levels: ["sensor", "measurement"]`
    /// - When we close `</measurement>`, is_level_element returns true
    /// - The parser calls end_row() to finalize the row
    #[inline]
    pub fn is_level_element(&self, state_id: StateId) -> bool {
        self.get_node(state_id)
            .map(|n| n.flags.is_level_element())
            .unwrap_or(false)
    }

    /// Get the table ID that owns a given field.
    ///
    /// This is used to route field values to the correct table builder when
    /// multiple tables have overlapping paths.
    ///
    /// Example: field 5 might belong to table 2.
    #[inline]
    pub fn get_field_table(&self, field_id: FieldId) -> Option<TableId> {
        self.field_to_table.get(field_id as usize).copied()
    }

    /// Get the maximum depth of any path in the trie.
    ///
    /// Used to pre-allocate the state stack with the right capacity,
    /// avoiding reallocations during parsing.
    pub fn max_depth(&self) -> u16 {
        self.max_depth
    }
}

// ============================================================================
// PathTrieBuilder - Constructs the Trie from Configuration
// ============================================================================

/// **PathTrieBuilder**: Builder for constructing a PathTrie from configuration.
///
/// Building a trie is a two-phase process:
///
/// **Phase 1: Insert all paths**
/// - Insert table paths (e.g., `/document/data/sensors`)
/// - Insert field paths (e.g., `/document/data/sensors/sensor/@id`)
/// - Track transitions in a HashMap for deduplication
/// - Build temporary child lists for each node
///
/// **Phase 2: Finalize**
/// - Convert temporary child lists to optimized ChildContainers
/// - Mark level elements for tables with explicit `levels` arrays
/// - Return the immutable PathTrie
///
/// Why two phases? Because we don't know how many children each node will have
/// until we've seen all the paths. Once we know, we can choose the optimal
/// storage strategy (Single, SmallVec, SortedVec, or Hash).
pub struct PathTrieBuilder {
    /// Temporary map for deduplicating paths during construction.
    /// Maps (parent_state, child_name) → child_state.
    /// This ensures we reuse states when paths share prefixes.
    ///
    /// Example: `/document/data/sensor/@id` and `/document/data/sensor/value`
    /// share the prefix `/document/data/sensor`, so they reuse the same states
    /// for "document", "data", and "sensor".
    transitions: HashMap<(StateId, Atom), StateId>,

    /// Growing list of nodes. Starts with just the root, grows as we insert paths.
    nodes: Vec<TrieNode>,

    /// Next available state ID. Increments each time we allocate a new node.
    next_state: StateId,

    /// Field configurations in order (global field ID is the index).
    field_configs: Vec<FieldConfig>,

    /// Table configurations in order (table ID is the index).
    table_configs: Vec<TableConfig>,

    /// Mapping from FieldId to TableId (which table owns which field).
    /// Built up as we insert field paths.
    field_to_table: Vec<TableId>,

    /// Track maximum depth of any path (for state stack pre-allocation).
    max_depth: u16,

    /// Temporary storage for element children before finalizing.
    /// Maps state_id → list of (child_name, child_state_id).
    /// During finalization, these are converted to ChildContainers.
    element_children_temp: HashMap<StateId, Vec<(Atom, StateId)>>,

    /// Temporary storage for attribute children before finalizing.
    /// Same structure as element_children_temp.
    attribute_children_temp: HashMap<StateId, Vec<(Atom, StateId)>>,
}

impl PathTrieBuilder {
    /// Create a new builder with a root node.
    pub fn new() -> Self {
        // Create the root node (state 0)
        let mut nodes = Vec::new();
        let root = TrieNode::new(Atom::from(""));
        nodes.push(root);

        Self {
            transitions: HashMap::new(),
            nodes,
            next_state: 1, // 0 is reserved for root, next node is 1
            field_configs: Vec::new(),
            table_configs: Vec::new(),
            field_to_table: Vec::new(),
            max_depth: 0,
            element_children_temp: HashMap::new(),
            attribute_children_temp: HashMap::new(),
        }
    }

    /// Build a PathTrie from a Config.
    ///
    /// This is the main entry point! It orchestrates the entire construction process:
    ///
    /// 1. Insert all table paths
    /// 2. Insert all field paths (and track which table owns each field)
    /// 3. Mark level elements for tables with explicit `levels` arrays
    /// 4. Finalize the trie (convert temporary structures to optimized ones)
    ///
    /// Returns an immutable PathTrie ready for parsing.
    pub fn from_config(config: &Config) -> Result<PathTrie> {
        let mut builder = Self::new();

        // Phase 1a: Insert all tables first
        // This ensures table nodes exist before we try to insert fields under them
        for table_config in &config.tables {
            builder.insert_table_path(table_config)?;
        }

        // Phase 1b: Insert all fields
        // We pass the table index so we can build the field_to_table mapping
        for (table_idx, table_config) in config.tables.iter().enumerate() {
            for field_config in &table_config.fields {
                builder.insert_field_path(field_config, table_idx as TableId)?;
            }
        }

        // Phase 1c: Mark level elements
        // For each table with explicit levels, mark those child elements as level elements
        // This enables proper row boundary detection during parsing
        for table_config in &config.tables {
            // Parse the table's XML path to get to the table root state
            let table_segments = parse_path(&table_config.xml_path);
            let mut table_state = 0; // Start at root

            for segment in &table_segments {
                let key = (table_state, segment.clone());
                table_state = *builder.transitions.get(&key).ok_or_else(|| {
                    Error::UnsupportedDataType(format!(
                        "Failed to find table path: {}",
                        table_config.xml_path
                    ))
                })?;
            }

            // Now mark level elements as children of the table root
            // Example: for `levels: ["sensor", "measurement"]`, we mark both
            // the "sensor" and "measurement" child states as level elements
            for level_name in &table_config.levels {
                let level_atom = Atom::from(level_name.as_str());
                let key = (table_state, level_atom);
                if let Some(&level_state) = builder.transitions.get(&key) {
                    builder.nodes[level_state as usize]
                        .flags
                        .set_level_element();
                }
            }
        }

        // Phase 2: Finalize (convert temp structures to optimized ones)
        builder.finalize()
    }

    /// Insert a table path into the trie.
    ///
    /// This creates nodes for the table's XML path and marks the final node
    /// as a table root.
    ///
    /// Example: for table at `/document/data/sensors`:
    /// - Creates/reuses nodes for "document", "data", "sensors"
    /// - Marks the "sensors" node as a table root
    /// - Stores the table ID in the node
    fn insert_table_path(&mut self, table_config: &TableConfig) -> Result<()> {
        let segments = parse_path(&table_config.xml_path);
        let table_id = self.table_configs.len() as TableId;
        self.table_configs.push(table_config.clone());

        // Walk the path, creating nodes as needed
        let final_state = self.insert_element_path(&segments)?;

        // Mark the final node as a table root
        let node = &mut self.nodes[final_state as usize];
        node.flags.set_table_root();
        node.table_id = Some(table_id);

        Ok(())
    }

    /// Insert a field path into the trie.
    ///
    /// This creates nodes for the field's XML path and marks the final node
    /// as a field. Also records which table owns this field.
    ///
    /// Handles both element fields (e.g., `/document/data/sensor/value`)
    /// and attribute fields (e.g., `/document/data/sensor/@id`).
    fn insert_field_path(&mut self, field_config: &FieldConfig, table_id: TableId) -> Result<()> {
        // Parse the path and check if it's an attribute
        let (segments, is_attribute) = parse_field_path(&field_config.xml_path);

        // Assign a global field ID (just the current count of fields)
        let field_id = self.field_configs.len() as FieldId;
        self.field_configs.push(field_config.clone());

        // Record which table owns this field
        self.field_to_table.push(table_id);

        if is_attribute {
            // Attribute field: last segment is the attribute name
            // Example: `/document/data/sensor/@id` → segments = ["document", "data", "sensor", "id"]
            // The "id" is an attribute, not an element

            if segments.is_empty() {
                return Err(Error::UnsupportedDataType(format!(
                    "Invalid attribute path: {}",
                    field_config.xml_path
                )));
            }

            // Split: all but last = element path, last = attribute name
            let element_segments = &segments[..segments.len() - 1];
            let attr_name = &segments[segments.len() - 1];

            // Walk to the parent element node
            let parent_state = self.insert_element_path(element_segments)?;

            // Create attribute child
            let final_state = self.insert_attribute(parent_state, attr_name.clone())?;

            // Mark as field and mark parent as having attributes
            let node = &mut self.nodes[final_state as usize];
            node.flags.set_field();
            node.field_id = Some(field_id);

            self.nodes[parent_state as usize].flags.set_has_attributes();
        } else {
            // Regular element field
            let final_state = self.insert_element_path(&segments)?;
            let node = &mut self.nodes[final_state as usize];
            node.flags.set_field();
            node.field_id = Some(field_id);
        }

        Ok(())
    }

    /// Insert a path of element segments, returning the final state.
    ///
    /// This is the core path insertion logic. It walks through each segment,
    /// creating nodes as needed or reusing existing ones.
    ///
    /// Example: for path ["document", "data", "sensor"]:
    /// - Start at root (state 0)
    /// - Transition to "document" (create state 1 if doesn't exist)
    /// - Transition to "data" (create state 2 if doesn't exist)
    /// - Transition to "sensor" (create state 3 if doesn't exist)
    /// - Return state 3
    ///
    /// The key insight: if we've seen `/document/data` before (from another field),
    /// we reuse those states! This is what makes the trie efficient.
    fn insert_element_path(&mut self, segments: &[Atom]) -> Result<StateId> {
        let mut current = 0; // Start at root
        let depth = segments.len() as u16;
        if depth > self.max_depth {
            self.max_depth = depth;
        }

        for segment in segments {
            // Check if this transition already exists
            let key = (current, segment.clone());
            let next = if let Some(&existing) = self.transitions.get(&key) {
                // Reuse existing state (path sharing!)
                existing
            } else {
                // Allocate new node
                let new_state = self.alloc_node(segment.clone());

                // Record the transition
                self.transitions.insert(key, new_state);

                // Add to parent's temporary child list
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

    /// Insert an attribute child for a given parent state.
    ///
    /// Similar to insert_element_path, but for attributes.
    /// Attributes are stored separately from elements to avoid name collisions.
    fn insert_attribute(&mut self, parent_state: StateId, attr_name: Atom) -> Result<StateId> {
        let key = (parent_state, attr_name.clone());
        let attr_state = if let Some(&existing) = self.transitions.get(&key) {
            existing
        } else {
            let new_state = self.alloc_node(attr_name.clone());
            self.transitions.insert(key, new_state);

            // Add to parent's temporary attribute child list
            self.attribute_children_temp
                .entry(parent_state)
                .or_insert_with(Vec::new)
                .push((attr_name, new_state));

            new_state
        };

        Ok(attr_state)
    }

    /// Allocate a new node and return its state ID.
    ///
    /// Simple helper that:
    /// 1. Creates a new TrieNode
    /// 2. Adds it to the nodes Vec
    /// 3. Returns its state ID (which is just its index)
    /// 4. Increments next_state for the next allocation
    fn alloc_node(&mut self, name: Atom) -> StateId {
        let state_id = self.next_state;
        self.nodes.push(TrieNode::new(name));
        self.next_state += 1;
        state_id
    }

    /// Finalize the trie by converting temporary structures to optimized ones.
    ///
    /// This is called after all paths have been inserted. It:
    /// 1. Converts temporary child lists to ChildContainers (choosing the optimal type)
    /// 2. Clears temporary structures to free memory
    /// 3. Returns the immutable PathTrie
    ///
    /// After finalization, the trie is read-only and optimized for fast lookups.
    fn finalize(mut self) -> Result<PathTrie> {
        // Convert temporary element children to optimized containers
        for (state_id, children) in self.element_children_temp.drain() {
            self.nodes[state_id as usize].element_children =
                ChildContainer::from_children(children);
        }

        // Convert temporary attribute children to optimized containers
        for (state_id, children) in self.attribute_children_temp.drain() {
            self.nodes[state_id as usize].attribute_children =
                ChildContainer::from_children(children);
        }

        // Return the immutable trie
        Ok(PathTrie {
            nodes: self.nodes,
            root_id: 0,
            field_configs: self.field_configs,
            table_configs: self.table_configs,
            field_to_table: self.field_to_table,
            max_depth: self.max_depth,
        })
    }
}

// ============================================================================
// Path Parsing Utilities
// ============================================================================

/// Parse an XML path into segments (atoms).
///
/// Example: `/document/data/sensor` → ["document", "data", "sensor"]
///
/// The leading '/' is stripped, and the path is split on '/'.
/// Each segment is interned as an Atom for fast comparison.
fn parse_path(path: &str) -> Vec<Atom> {
    path.trim_start_matches('/')
        .split('/')
        .filter(|s| !s.is_empty())
        .map(Atom::from)
        .collect()
}

/// Parse a field path, returning (segments, is_attribute).
///
/// Handles both element paths and attribute paths:
/// - `/document/data/sensor/value` → (["document", "data", "sensor", "value"], false)
/// - `/document/data/sensor/@id` → (["document", "data", "sensor", "id"], true)
///
/// Note: The '@' is stripped from attribute names in the returned segments.
fn parse_field_path(path: &str) -> (Vec<Atom>, bool) {
    let parts: Vec<&str> = path
        .trim_start_matches('/')
        .split('/')
        .filter(|s| !s.is_empty())
        .collect();

    let mut is_attribute = false;
    let segments: Vec<Atom> = parts
        .iter()
        .map(|s| {
            if s.starts_with('@') {
                is_attribute = true;
                Atom::from(&s[1..]) // Strip '@' prefix
            } else {
                Atom::from(*s)
            }
        })
        .collect();

    (segments, is_attribute)
}

// ============================================================================
// Tests - Comprehensive Test Coverage
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Config, DType, FieldConfigBuilder, TableConfig};

    #[test]
    fn test_parse_path() {
        assert_eq!(
            parse_path("/document/data/sensor"),
            vec![
                Atom::from("document"),
                Atom::from("data"),
                Atom::from("sensor")
            ]
        );
        assert_eq!(parse_path("/"), Vec::<Atom>::new());
        assert_eq!(parse_path(""), Vec::<Atom>::new());
    }

    #[test]
    fn test_parse_field_path_element() {
        let (segments, is_attr) = parse_field_path("/document/data/value");
        assert_eq!(
            segments,
            vec![
                Atom::from("document"),
                Atom::from("data"),
                Atom::from("value")
            ]
        );
        assert!(!is_attr);
    }

    #[test]
    fn test_parse_field_path_attribute() {
        let (segments, is_attr) = parse_field_path("/document/data/@id");
        assert_eq!(
            segments,
            vec![Atom::from("document"), Atom::from("data"), Atom::from("id")]
        );
        assert!(is_attr);
    }

    #[test]
    fn test_trie_simple_path() {
        let config = Config {
            parser_options: Default::default(),
            tables: vec![TableConfig::new(
                "items",
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

        // Navigate the path
        let mut state = trie.root_id();
        state = trie.transition_element(state, &Atom::from("root"));
        assert_ne!(state, UNMATCHED_STATE);

        state = trie.transition_element(state, &Atom::from("items"));
        assert_ne!(state, UNMATCHED_STATE);
        assert!(trie.is_table_root(state));

        state = trie.transition_element(state, &Atom::from("item"));
        state = trie.transition_element(state, &Atom::from("value"));
        assert_ne!(state, UNMATCHED_STATE);
        assert_eq!(trie.get_field_id(state), Some(0));
    }

    #[test]
    fn test_trie_with_attributes() {
        let config = Config {
            parser_options: Default::default(),
            tables: vec![TableConfig::new(
                "items",
                "/data",
                vec![],
                vec![
                    FieldConfigBuilder::new("id", "/data/item/@id", DType::Utf8).build(),
                    FieldConfigBuilder::new("value", "/data/item/value", DType::Int32).build(),
                ],
            )],
        };

        let trie = PathTrieBuilder::from_config(&config).unwrap();

        // Verify attribute transition works
        let mut state = trie.root_id();
        state = trie.transition_element(state, &Atom::from("data"));
        assert!(trie.is_table_root(state));

        state = trie.transition_element(state, &Atom::from("item"));
        assert!(trie.has_attributes(state));

        let attr_state = trie.transition_attribute(state, &Atom::from("id"));
        assert_ne!(attr_state, UNMATCHED_STATE);
        assert_eq!(trie.get_field_id(attr_state), Some(0));

        let value_state = trie.transition_element(state, &Atom::from("value"));
        assert_eq!(trie.get_field_id(value_state), Some(1));
    }

    #[test]
    fn test_child_container_optimization() {
        // Test that different container types are used based on fanout

        // Empty
        let container = ChildContainer::from_children(vec![]);
        assert!(matches!(container, ChildContainer::Empty));

        // Single
        let container = ChildContainer::from_children(vec![(Atom::from("a"), 1)]);
        assert!(matches!(container, ChildContainer::Single(_, _)));

        // SmallVec
        let container = ChildContainer::from_children(vec![
            (Atom::from("a"), 1),
            (Atom::from("b"), 2),
            (Atom::from("c"), 3),
        ]);
        assert!(matches!(container, ChildContainer::SmallVec(_)));

        // SortedVec
        let mut many: Vec<_> = (0..10)
            .map(|i| (Atom::from(format!("child{}", i)), i))
            .collect();
        let container = ChildContainer::from_children(many);
        assert!(matches!(container, ChildContainer::SortedVec(_)));

        // Hash
        let mut very_many: Vec<_> = (0..50)
            .map(|i| (Atom::from(format!("child{}", i)), i))
            .collect();
        let container = ChildContainer::from_children(very_many);
        assert!(matches!(container, ChildContainer::Hash(_)));
    }

    #[test]
    fn test_unmatched_state() {
        let config = Config {
            parser_options: Default::default(),
            tables: vec![TableConfig::new(
                "items",
                "/data",
                vec![],
                vec![FieldConfigBuilder::new("value", "/data/item/value", DType::Int32).build()],
            )],
        };

        let trie = PathTrieBuilder::from_config(&config).unwrap();

        let root = trie.root_id();

        // Transition to non-existent element
        let state = trie.transition_element(root, &Atom::from("nonexistent"));
        assert_eq!(state, UNMATCHED_STATE);

        // Verify we can't do anything with UNMATCHED_STATE
        assert!(!trie.is_table_root(state));
        assert_eq!(trie.get_field_id(state), None);
        assert_eq!(trie.get_table_id(state), None);
    }

    #[test]
    fn test_multiple_tables_shared_prefix() {
        let config = Config {
            parser_options: Default::default(),
            tables: vec![
                TableConfig::new(
                    "sensors",
                    "/document/data/sensors",
                    vec![],
                    vec![
                        FieldConfigBuilder::new(
                            "id",
                            "/document/data/sensors/sensor/@id",
                            DType::Utf8,
                        )
                        .build(),
                    ],
                ),
                TableConfig::new(
                    "measurements",
                    "/document/data/sensors/sensor/measurements",
                    vec![],
                    vec![
                        FieldConfigBuilder::new(
                            "value",
                            "/document/data/sensors/sensor/measurements/measurement/value",
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
        state = trie.transition_element(state, &Atom::from("document"));
        state = trie.transition_element(state, &Atom::from("data"));
        state = trie.transition_element(state, &Atom::from("sensors"));
        assert!(trie.is_table_root(state));
        assert_eq!(trie.get_table_id(state), Some(0));

        state = trie.transition_element(state, &Atom::from("sensor"));
        state = trie.transition_element(state, &Atom::from("measurements"));
        assert!(trie.is_table_root(state));
        assert_eq!(trie.get_table_id(state), Some(1));
    }

    #[test]
    fn test_nested_tables() {
        let config = Config {
            parser_options: Default::default(),
            tables: vec![
                TableConfig::new(
                    "parent",
                    "/data/parent",
                    vec!["item".to_string()],
                    vec![
                        FieldConfigBuilder::new("id", "/data/parent/item/@id", DType::Utf8).build(),
                    ],
                ),
                TableConfig::new(
                    "child",
                    "/data/parent/item/children",
                    vec!["item".to_string(), "child".to_string()],
                    vec![
                        FieldConfigBuilder::new(
                            "value",
                            "/data/parent/item/children/child/value",
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
        state = trie.transition_element(state, &Atom::from("data"));
        state = trie.transition_element(state, &Atom::from("parent"));
        assert!(trie.is_table_root(state));

        // Navigate into nested table
        state = trie.transition_element(state, &Atom::from("item"));
        assert!(trie.is_level_element(state)); // "item" is a level element for parent table

        state = trie.transition_element(state, &Atom::from("children"));
        assert!(trie.is_table_root(state));

        state = trie.transition_element(state, &Atom::from("child"));
        assert!(trie.is_level_element(state)); // "child" is a level element for child table
    }

    #[test]
    fn test_deep_nesting() {
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

        // Navigate all the way down
        let mut state = trie.root_id();
        for name in &["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"] {
            state = trie.transition_element(state, &Atom::from(*name));
            assert_ne!(state, UNMATCHED_STATE);
        }
        assert!(trie.is_table_root(state));
    }

    #[test]
    fn test_many_attributes() {
        let mut fields = vec![];
        for i in 0..50 {
            fields.push(
                FieldConfigBuilder::new(
                    &format!("attr{}", i),
                    &format!("/data/item/@attr{}", i),
                    DType::Utf8,
                )
                .build(),
            );
        }

        let config = Config {
            parser_options: Default::default(),
            tables: vec![TableConfig::new("items", "/data", vec![], fields)],
        };

        let trie = PathTrieBuilder::from_config(&config).unwrap();

        let mut state = trie.root_id();
        state = trie.transition_element(state, &Atom::from("data"));
        state = trie.transition_element(state, &Atom::from("item"));
        assert!(trie.has_attributes(state));

        // Verify all attributes are accessible
        for i in 0..50 {
            let attr_state = trie.transition_attribute(state, &Atom::from(format!("attr{}", i)));
            assert_ne!(attr_state, UNMATCHED_STATE);
            assert!(trie.get_field_id(attr_state).is_some());
        }
    }

    #[test]
    fn test_many_children() {
        let mut fields = vec![];
        for i in 0..100 {
            fields.push(
                FieldConfigBuilder::new(
                    &format!("field{}", i),
                    &format!("/data/item/field{}", i),
                    DType::Int32,
                )
                .build(),
            );
        }

        let config = Config {
            parser_options: Default::default(),
            tables: vec![TableConfig::new("items", "/data", vec![], fields)],
        };

        let trie = PathTrieBuilder::from_config(&config).unwrap();

        let mut state = trie.root_id();
        state = trie.transition_element(state, &Atom::from("data"));
        state = trie.transition_element(state, &Atom::from("item"));

        // Verify all fields are accessible
        for i in 0..100 {
            let field_state = trie.transition_element(state, &Atom::from(format!("field{}", i)));
            assert_ne!(field_state, UNMATCHED_STATE);
            assert!(trie.get_field_id(field_state).is_some());
        }
    }

    #[test]
    fn test_all_data_types() {
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

        let mut fields = vec![];
        for (_i, (name, dtype)) in types.iter().enumerate() {
            fields.push(
                FieldConfigBuilder::new(name, &format!("/data/item/{}", name), *dtype).build(),
            );
        }

        let config = Config {
            parser_options: Default::default(),
            tables: vec![TableConfig::new("items", "/data", vec![], fields)],
        };

        let trie = PathTrieBuilder::from_config(&config).unwrap();
        assert_eq!(trie.field_configs.len(), 12);

        let mut state = trie.root_id();
        state = trie.transition_element(state, &Atom::from("data"));
        state = trie.transition_element(state, &Atom::from("item"));

        for (_i, (name, dtype)) in types.iter().enumerate() {
            let field_state = trie.transition_element(state, &Atom::from(*name));
            assert_ne!(field_state, UNMATCHED_STATE);
            let field_id = trie.get_field_id(field_state).unwrap();
            assert_eq!(trie.field_configs[field_id as usize].data_type, *dtype);
        }
    }

    #[test]
    fn test_realistic_sensor_config() {
        let config = Config {
            parser_options: Default::default(),
            tables: vec![
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
                            "/document/data/sensors/sensor/measurements/measurement/@timestamp",
                            DType::UInt64,
                        )
                        .build(),
                        FieldConfigBuilder::new(
                            "value",
                            "/document/data/sensors/sensor/measurements/measurement/value",
                            DType::Float64,
                        )
                        .build(),
                    ],
                ),
            ],
        };

        let trie = PathTrieBuilder::from_config(&config).unwrap();

        // Verify structure
        assert_eq!(trie.table_configs.len(), 2);
        assert_eq!(trie.field_configs.len(), 5);

        // Navigate to sensors table
        let mut state = trie.root_id();
        state = trie.transition_element(state, &Atom::from("document"));
        state = trie.transition_element(state, &Atom::from("data"));
        state = trie.transition_element(state, &Atom::from("sensors"));
        assert!(trie.is_table_root(state));
        assert_eq!(trie.get_table_id(state), Some(0));

        // Navigate to sensor element (level element for sensors table)
        state = trie.transition_element(state, &Atom::from("sensor"));
        assert!(trie.is_level_element(state));
        assert!(trie.has_attributes(state));

        // Check attributes
        let id_state = trie.transition_attribute(state, &Atom::from("id"));
        assert_eq!(trie.get_field_id(id_state), Some(0));
        assert_eq!(trie.get_field_table(0), Some(0)); // Field 0 belongs to table 0

        // Navigate to measurements table
        state = trie.transition_element(state, &Atom::from("measurements"));
        assert!(trie.is_table_root(state));
        assert_eq!(trie.get_table_id(state), Some(1));

        // Navigate to measurement element (level element for measurements table)
        state = trie.transition_element(state, &Atom::from("measurement"));
        assert!(trie.is_level_element(state));

        // Check measurement fields
        state = trie.transition_element(state, &Atom::from("value"));
        assert_eq!(trie.get_field_id(state), Some(4));
        assert_eq!(trie.get_field_table(4), Some(1)); // Field 4 belongs to table 1
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
    }

    #[test]
    fn test_single_root_table() {
        let config = Config {
            parser_options: Default::default(),
            tables: vec![TableConfig::new("root", "/", vec![], vec![])],
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
