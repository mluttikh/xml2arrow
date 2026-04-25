//! Integer-based path indexing for efficient XML path lookups during parsing.
//!
//! This module keeps the parsing hot path free of string-heavy operations.
//! The registry compiles all configured XML paths into a trie, assigns each
//! node a compact integer ID, and allows the parser to operate on those IDs
//! with direct indexing.
//
// Design overview (top-down narrative):
//
// 1) Build-time: PathRegistry::from_config
//    - Convert every table path and field path into a trie of interned atoms.
//    - Store table/field metadata at the terminal node of each path.
//    - Use integer IDs so lookups are array indexing rather than hash maps.
//
// 2) Run-time: PathTracker
//    - Maintain a stack of node IDs corresponding to the current XML depth.
//    - On entering an element, attempt to resolve the child in the registry.
//      If the current path is not in the registry, mark the subtree as unknown
//      to skip further lookups until we pop back out.
//
// This design intentionally favors predictable O(1) operations during parsing
// over upfront construction work at startup.

use fxhash::FxHashMap;

use crate::config::Config;

/// Threshold at which a node's child list switches from a linear-scan `Vec`
/// to an `FxHashMap`. Chosen empirically: at small fan-out the linear scan
/// wins by skipping the hash + heap indirection (one or two short byte-slice
/// comparisons against cache-resident strings); above ~8 children the per-
/// lookup work outgrows the hash cost and the map starts paying off. Real
/// XML schemas almost always sit well below this threshold per node, so the
/// `Small` arm is the default hot path.
const SMALL_CHILDREN_THRESHOLD: usize = 8;

/// A node ID in the path registry trie.
///
/// Node 0 is always the root node (representing "/").
/// We keep this as a small integer to allow direct indexing into vectors.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct PathNodeId(u32);

impl PathNodeId {
    /// The root node ID, representing the "/" path.
    pub const ROOT: PathNodeId = PathNodeId(0);

    /// Returns the underlying index value.
    #[inline]
    pub fn index(self) -> usize {
        self.0 as usize
    }
}

/// Information about what a path node represents in the configuration.
///
/// This is the "semantic payload" for a trie node. It tells the parser whether
/// a path is a table boundary and which fields are mapped to it.
#[derive(Debug, Clone, Default)]
pub struct PathNodeInfo {
    /// If this path represents a table, store the table index.
    pub table_index: Option<usize>,
    /// Field indices: (`table_idx`, `field_idx`) pairs for fields at this path.
    pub field_indices: Vec<(usize, usize)>,
    /// Whether any child of this node has an attribute path (starts with "@").
    /// Used to skip attribute parsing for elements that have no attribute fields configured.
    pub has_attribute_children: bool,
}

impl PathNodeInfo {
    /// Returns true if this path is a table path.
    #[inline]
    pub fn is_table(&self) -> bool {
        self.table_index.is_some()
    }

    /// Returns true if this path has any associated fields.
    #[inline]
    #[allow(dead_code)]
    pub fn has_fields(&self) -> bool {
        !self.field_indices.is_empty()
    }
}

/// Registry for efficient path lookups during XML parsing.
///
/// The registry compiles all configured XML paths into a trie of interned strings (`Atom`).
/// Each node in the trie is assigned a compact integer ID (`PathNodeId`), allowing the parser
/// to operate entirely on IDs using direct array indexing, avoiding string hashing in the hot loop.
///
/// # Architecture Visualization
///
/// Given a configuration representing meteorological stations, with tables at `/report`
/// and `/report/monitoring_stations/monitoring_station`, and a field at `@id`,
/// the registry builds a logical tree structure like this:
///
/// ```text
/// [ID: 0] (ROOT)
///   │
///   └── "report" ─────────▶ [ID: 1] ── (Metadata: Table 0 boundary)
///                             │
///                             ├── "header" ──▶ [ID: 2]
///                             │                  │
///                             │                  └── "title" ──▶ [ID: 3] ── (Metadata: Field 0, Table 0)
///                             │
///                             └── "monitoring_stations" ──▶ [ID: 4]
///                                                             │
///                                                             └── "monitoring_station" ──▶ [ID: 5] ── (Metadata: Table 1 boundary)
///                                                                                            │
///                                                                                            └── "@id" ──▶ [ID: 6] ── (Metadata: Field 0, Table 1)
/// ```
///
/// Under the hood, this tree is flattened into parallel vectors to ensure cache-friendly lookups:
/// * `children`: Uses the node's `PathNodeId` as an index to find a `NodeChildren`
///   container. Small child sets (the common case) live in an inline `Vec` and are
///   resolved by linear scan; large child sets promote to an `FxHashMap`.
/// * `node_info`: Uses the node's `PathNodeId` as an index to retrieve `PathNodeInfo` (whether this node is a table boundary or contains fields).
pub struct PathRegistry {
    /// For each node, the container of its children.
    children: Vec<NodeChildren>,
    /// Information about each node (is it a table? which fields?).
    node_info: Vec<PathNodeInfo>,
}

/// Per-node child storage with a representation that adapts to fan-out.
///
/// `Small` is the hot path: real XML configs almost always have a handful of
/// children per node, where a contiguous `Vec` of `(name, id)` pairs beats a
/// hash map — no hash to compute, no bucket indirection, and the whole list
/// typically fits in a cache line or two. `Large` exists purely so worst-case
/// configurations (dozens of distinct sibling element names) don't regress
/// vs. the prior hashmap implementation.
enum NodeChildren {
    Small(Vec<(Box<[u8]>, PathNodeId)>),
    Large(FxHashMap<Box<[u8]>, PathNodeId>),
}

impl NodeChildren {
    fn new() -> Self {
        NodeChildren::Small(Vec::new())
    }

    #[inline]
    fn get(&self, name: &[u8]) -> Option<PathNodeId> {
        match self {
            NodeChildren::Small(entries) => {
                for (child_name, child_id) in entries {
                    if child_name.as_ref() == name {
                        return Some(*child_id);
                    }
                }
                None
            }
            NodeChildren::Large(map) => map.get(name).copied(),
        }
    }

    /// Inserts a child name → ID mapping. Caller must have verified the name
    /// is not already present (the registry's `get_or_create_child` does that
    /// via `get_child` first). Promotes from `Small` to `Large` once the
    /// linear scan would start to lose to hashing.
    fn insert(&mut self, name: Box<[u8]>, id: PathNodeId) {
        match self {
            NodeChildren::Small(entries) => {
                if entries.len() >= SMALL_CHILDREN_THRESHOLD {
                    let mut map =
                        FxHashMap::with_capacity_and_hasher(entries.len() + 1, Default::default());
                    for (n, i) in entries.drain(..) {
                        map.insert(n, i);
                    }
                    map.insert(name, id);
                    *self = NodeChildren::Large(map);
                } else {
                    entries.push((name, id));
                }
            }
            NodeChildren::Large(map) => {
                map.insert(name, id);
            }
        }
    }

    fn any_attribute_name(&self) -> bool {
        match self {
            NodeChildren::Small(entries) => entries.iter().any(|(n, _)| n.starts_with(b"@")),
            NodeChildren::Large(map) => map.keys().any(|n| n.starts_with(b"@")),
        }
    }
}

impl PathRegistry {
    /// Builds a path registry from the configuration.
    ///
    /// We do all string parsing here, so the runtime parser never touches raw
    /// strings for configured paths. This is the main performance lever.
    pub fn from_config(config: &Config) -> Self {
        let mut registry = Self {
            children: vec![NodeChildren::new()],      // Root node
            node_info: vec![PathNodeInfo::default()], // Root info
        };

        // Phase 1: register table paths
        // The table boundary must be known so the parser can push/pop row scopes.
        for (table_idx, table_config) in config.tables.iter().enumerate() {
            let node_id = registry.get_or_create_path(&table_config.xml_path);
            registry.node_info[node_id.index()].table_index = Some(table_idx);
        }

        // Phase 2: register field paths
        // We allow multiple fields to map to the same node (e.g., different
        // tables that share a path shape).
        for (table_idx, table_config) in config.tables.iter().enumerate() {
            for (field_idx, field_config) in table_config.fields.iter().enumerate() {
                let node_id = registry.get_or_create_path(&field_config.xml_path);
                registry.node_info[node_id.index()]
                    .field_indices
                    .push((table_idx, field_idx));
            }
        }

        // Phase 3: mark nodes that have attribute children so the parser can
        // skip attribute iteration for elements with no attribute fields.
        for node_id_idx in 0..registry.children.len() {
            registry.node_info[node_id_idx].has_attribute_children =
                registry.children[node_id_idx].any_attribute_name();
        }

        // Phase 4: register optional stop paths so the parser can resolve them
        // without string lookups in the hot loop.
        for stop_path in &config.parser_options.stop_at_paths {
            registry.get_or_create_path(stop_path);
        }

        registry
    }

    /// Gets or creates a path in the trie, returning its node ID.
    ///
    /// We intentionally parse paths by splitting on "/".
    /// - Leading "/" is ignored.
    /// - Empty segments are ignored (double slashes, trailing slash).
    fn get_or_create_path(&mut self, path_str: &str) -> PathNodeId {
        let mut current_node = PathNodeId::ROOT;

        for part in path_str
            .trim_start_matches('/')
            .split('/')
            .filter(|s| !s.is_empty())
        {
            current_node = self.get_or_create_child(current_node, part.as_bytes());
        }

        current_node
    }

    /// Resolves a path string to an existing node ID without creating new nodes.
    ///
    /// Returns `None` if the path doesn't exist in the registry.
    pub fn resolve_path(&self, path_str: &str) -> Option<PathNodeId> {
        let mut current_node = PathNodeId::ROOT;

        for part in path_str
            .trim_start_matches('/')
            .split('/')
            .filter(|s| !s.is_empty())
        {
            current_node = self.get_child(current_node, part.as_bytes())?;
        }

        Some(current_node)
    }

    /// Gets or creates a child node for the given parent and name.
    ///
    /// This is the only place we mutate the trie. By isolating that logic, we
    /// avoid duplicating bookkeeping for new nodes.
    fn get_or_create_child(&mut self, parent: PathNodeId, name: &[u8]) -> PathNodeId {
        if let Some(child_id) = self.get_child(parent, name) {
            return child_id;
        }

        // Create a new node and wire it into the trie.
        #[allow(clippy::cast_possible_truncation)] // Node count will never exceed u32::MAX
        let new_id = PathNodeId(self.children.len() as u32);
        self.children.push(NodeChildren::new());
        self.node_info.push(PathNodeInfo::default());
        self.children[parent.index()].insert(name.into(), new_id);

        new_id
    }

    /// Looks up a child node by name.
    ///
    /// Returns `None` if the child doesn't exist (path not in config).
    ///
    /// The underlying container adapts to fan-out: a linear scan when there
    /// are only a handful of children (the common case for XML element
    /// nodes), promoting to a hash map past the small-set threshold.
    #[inline]
    pub fn get_child(&self, parent: PathNodeId, name: &[u8]) -> Option<PathNodeId> {
        self.children.get(parent.index())?.get(name)
    }

    /// Gets information about a node.
    #[inline]
    pub fn get_node_info(&self, node_id: PathNodeId) -> &PathNodeInfo {
        &self.node_info[node_id.index()]
    }

    /// Returns true if the given node represents a table path.
    #[inline]
    pub fn is_table_path(&self, node_id: PathNodeId) -> bool {
        self.node_info[node_id.index()].is_table()
    }

    /// Returns the table index if this node represents a table.
    #[inline]
    pub fn get_table_index(&self, node_id: PathNodeId) -> Option<usize> {
        self.node_info[node_id.index()].table_index
    }

    /// Returns the root node info.
    #[inline]
    #[allow(dead_code)]
    pub fn root_info(&self) -> &PathNodeInfo {
        &self.node_info[0]
    }
}

/// One frame on the `PathTracker` stack — a single open XML element.
///
/// Carries the metadata the parser needs to react to the matching close
/// without a second registry lookup. `is_table` is materialized at `enter()`
/// time from `PathNodeInfo` so `Event::End` and `Event::Empty` can decide
/// whether to finalize a row using only the stack.
#[derive(Debug, Clone, Copy)]
struct StackEntry {
    /// Node ID for known paths; `ROOT` for unknown subtrees (placeholder).
    node_id: PathNodeId,
    /// Whether `node_id` corresponds to a configured path.
    is_known: bool,
    /// Whether this node is a table boundary. Only meaningful when `is_known`.
    is_table: bool,
}

/// Tracks the current position in the path trie during parsing.
///
/// The parser operates on streaming XML events. We maintain a stack that
/// mirrors XML nesting depth; the bottom is always the implicit document
/// root, so the stack is never empty.
///
/// Each frame carries the few bits the parser needs at `Event::End` /
/// `Event::Empty` time (is_table, is_known). The previous design kept those
/// in a parallel `element_stack` plus a separate `node_info[]` lookup at
/// close time; folding them onto this stack saves a Vec push/pop and a
/// cache-line read per element.
///
/// If a path is unknown, we keep pushing "unknown" placeholder frames until
/// we exit that subtree. This avoids repeated registry lookups for
/// irrelevant branches.
#[derive(Debug)]
pub struct PathTracker {
    node_stack: Vec<StackEntry>,
}

impl PathTracker {
    /// Creates a new path tracker starting at the root.
    ///
    /// `registry` is consulted exactly once to seed the root frame's
    /// `is_table` flag (so `top_is_table()` works at root depth without
    /// re-querying).
    pub fn new(registry: &PathRegistry) -> Self {
        let root_is_table = registry.is_table_path(PathNodeId::ROOT);
        Self {
            node_stack: vec![StackEntry {
                node_id: PathNodeId::ROOT,
                is_known: true,
                is_table: root_is_table,
            }],
        }
    }

    /// Enters a child element, updating the current path position.
    ///
    /// On a successful resolve, returns the new node ID together with a
    /// borrow of its `PathNodeInfo` so the caller can read every per-node
    /// flag (table_index, has_attribute_children, field_indices) without a
    /// second array lookup. Returns `None` if the path is not configured;
    /// a placeholder frame is still pushed to keep depth aligned with XML
    /// nesting.
    #[inline]
    pub fn enter<'r>(
        &mut self,
        name: &[u8],
        registry: &'r PathRegistry,
    ) -> Option<(PathNodeId, &'r PathNodeInfo)> {
        let top = *self.node_stack.last().unwrap();

        if !top.is_known {
            // Parent path is not in config, so children can't be either.
            // Push a placeholder so leave() pops at the right depth.
            self.node_stack.push(StackEntry {
                node_id: PathNodeId::ROOT,
                is_known: false,
                is_table: false,
            });
            return None;
        }

        if let Some(child_id) = registry.get_child(top.node_id, name) {
            let info = registry.get_node_info(child_id);
            self.node_stack.push(StackEntry {
                node_id: child_id,
                is_known: true,
                is_table: info.is_table(),
            });
            Some((child_id, info))
        } else {
            self.node_stack.push(StackEntry {
                node_id: PathNodeId::ROOT,
                is_known: false,
                is_table: false,
            });
            None
        }
    }

    /// Leaves the current element, returning to the parent path.
    ///
    /// Returns the node ID that was popped, or None if it wasn't a known path.
    #[inline]
    pub fn leave(&mut self) -> Option<PathNodeId> {
        if self.node_stack.len() > 1 {
            let entry = self.node_stack.pop().unwrap();
            if entry.is_known {
                return Some(entry.node_id);
            }
        }
        None
    }

    /// Returns the current node ID if it's a known path.
    #[inline]
    pub fn current(&self) -> Option<PathNodeId> {
        let top = *self.node_stack.last().unwrap();
        if top.is_known {
            Some(top.node_id)
        } else {
            None
        }
    }

    /// Returns the top frame's `(node_id_if_known, is_table)` when an element
    /// is open above root, or `None` at root depth (no element to close).
    ///
    /// Used by `Event::End` to read the closing element's attributes without
    /// popping (the pop happens later inside `close_element` via `leave()`).
    #[inline]
    pub fn peek_top(&self) -> Option<(Option<PathNodeId>, bool)> {
        if self.node_stack.len() > 1 {
            let top = self.node_stack.last().unwrap();
            let id = if top.is_known {
                Some(top.node_id)
            } else {
                None
            };
            Some((id, top.is_table))
        } else {
            None
        }
    }

    /// Returns true if the top of the stack is currently a table-boundary
    /// path. After `leave()` this answers "is the parent a table?", which is
    /// what `close_element` needs to decide whether to finalize a row.
    #[inline]
    pub fn top_is_table(&self) -> bool {
        self.node_stack.last().unwrap().is_table
    }

    /// Returns the current node ID, or ROOT if unknown.
    #[inline]
    #[allow(dead_code)]
    pub fn current_or_root(&self) -> PathNodeId {
        self.node_stack
            .last()
            .map_or(PathNodeId::ROOT, |e| e.node_id)
    }

    /// Returns true if the current path is known (exists in the registry).
    #[inline]
    #[allow(dead_code)]
    pub fn is_current_known(&self) -> bool {
        self.node_stack.last().is_some_and(|e| e.is_known)
    }

    /// Returns the depth of the current path (number of segments from root).
    #[inline]
    #[allow(dead_code)]
    pub fn depth(&self) -> usize {
        self.node_stack.len().saturating_sub(1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{DType, FieldConfigBuilder, TableConfig};
    use crate::config_from_yaml;

    fn create_test_config() -> Config {
        Config {
            tables: vec![
                TableConfig::new(
                    "items",
                    "/root/items",
                    vec!["item".to_string()],
                    vec![
                        FieldConfigBuilder::new("name", "/root/items/item/name", DType::Utf8)
                            .build()
                            .unwrap(),
                        FieldConfigBuilder::new("value", "/root/items/item/value", DType::Float64)
                            .build()
                            .unwrap(),
                    ],
                ),
                TableConfig::new(
                    "metadata",
                    "/root/metadata",
                    vec![],
                    vec![
                        FieldConfigBuilder::new("version", "/root/metadata/version", DType::Utf8)
                            .build()
                            .unwrap(),
                    ],
                ),
            ],
            parser_options: Default::default(),
        }
    }

    #[test]
    fn test_registry_built_from_config_correctly() {
        let config = create_test_config();
        let registry = PathRegistry::from_config(&config);

        // Check root node exists
        assert!(!registry.is_table_path(PathNodeId::ROOT));

        // Check that table paths are recognized
        let root_node = registry.get_child(PathNodeId::ROOT, b"root").unwrap();
        let items_node = registry.get_child(root_node, b"items").unwrap();
        let metadata_node = registry.get_child(root_node, b"metadata").unwrap();

        assert!(registry.is_table_path(items_node));
        assert!(registry.is_table_path(metadata_node));
        assert_eq!(registry.get_table_index(items_node), Some(0));
        assert_eq!(registry.get_table_index(metadata_node), Some(1));
    }

    #[test]
    fn test_registry_returns_correct_field_paths() {
        let config = create_test_config();
        let registry = PathRegistry::from_config(&config);

        // Navigate to /root/items/item/name
        let root_node = registry.get_child(PathNodeId::ROOT, b"root").unwrap();
        let items_node = registry.get_child(root_node, b"items").unwrap();
        let item_node = registry.get_child(items_node, b"item").unwrap();
        let name_node = registry.get_child(item_node, b"name").unwrap();

        let info = registry.get_node_info(name_node);
        assert!(!info.is_table());
        assert!(info.has_fields());
        assert_eq!(info.field_indices.len(), 1);
        assert_eq!(info.field_indices[0], (0, 0)); // table 0, field 0
    }

    #[test]
    fn test_path_tracker_tracks_known_paths() {
        let config = create_test_config();
        let registry = PathRegistry::from_config(&config);
        let mut tracker = PathTracker::new(&registry);

        // Enter /root
        let entered = tracker.enter(b"root", &registry);
        assert!(entered.is_some());
        let (node, info) = entered.unwrap();
        assert!(!info.is_table());
        assert!(!registry.is_table_path(node));

        // Enter /root/items
        let entered = tracker.enter(b"items", &registry);
        assert!(entered.is_some());
        let (node, info) = entered.unwrap();
        assert!(info.is_table());
        assert!(registry.is_table_path(node));

        // Leave /root/items
        let left = tracker.leave();
        assert!(left.is_some());

        // Leave /root
        let left = tracker.leave();
        assert!(left.is_some());
    }

    #[test]
    fn test_path_tracker_ignores_unknown_paths() {
        let config = create_test_config();
        let registry = PathRegistry::from_config(&config);
        let mut tracker = PathTracker::new(&registry);

        // Enter unknown path
        let entered = tracker.enter(b"unknown", &registry);
        assert!(entered.is_none());
        assert!(!tracker.is_current_known());

        // Children of unknown paths are also unknown
        let entered = tracker.enter(b"child", &registry);
        assert!(entered.is_none());

        // Leave unknown child
        tracker.leave();
        // Leave unknown parent
        tracker.leave();

        // Back to root
        assert!(tracker.is_current_known());
    }

    #[test]
    fn test_root_table_path_resolved_correctly() {
        let config = config_from_yaml!(
            r#"
            tables:
                - name: root
                  xml_path: /
                  levels: []
                  fields:
                    - name: value
                      xml_path: /data/value
                      data_type: Utf8
            "#
        );

        let registry = PathRegistry::from_config(&config);

        // Root path "/" should be a table
        assert!(registry.is_table_path(PathNodeId::ROOT));
        assert_eq!(registry.get_table_index(PathNodeId::ROOT), Some(0));
    }

    #[test]
    fn test_attribute_paths_registered_correctly() {
        let config = config_from_yaml!(
            r#"
            tables:
                - name: items
                  xml_path: /root/items
                  levels: [item]
                  fields:
                    - name: id
                      xml_path: /root/items/item/@id
                      data_type: Utf8
            "#
        );

        let registry = PathRegistry::from_config(&config);
        let mut tracker = PathTracker::new(&registry);

        // Navigate to /root/items/item
        tracker.enter(b"root", &registry);
        tracker.enter(b"items", &registry);
        tracker.enter(b"item", &registry);

        // Enter attribute path @id
        let entered = tracker.enter(b"@id", &registry);
        assert!(entered.is_some());
        let (node_id, _) = entered.unwrap();

        let info = registry.get_node_info(node_id);
        assert!(info.has_fields());
        assert_eq!(info.field_indices[0], (0, 0)); // table 0, field 0
    }
}
