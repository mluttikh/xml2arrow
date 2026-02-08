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
use string_cache::DefaultAtom as Atom;

use crate::config::Config;

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
    /// Field indices: (table_idx, field_idx) pairs for fields at this path.
    pub field_indices: Vec<(usize, usize)>,
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
/// The registry is a trie keyed by interned element names. Each node has:
/// - a map of child name -> child ID
/// - PathNodeInfo metadata for table/field mapping
pub struct PathRegistry {
    /// For each node, map child element name to child node ID.
    children: Vec<FxHashMap<Atom, PathNodeId>>,
    /// Information about each node (is it a table? which fields?).
    node_info: Vec<PathNodeInfo>,
}

impl PathRegistry {
    /// Builds a path registry from the configuration.
    ///
    /// We do all string parsing here, so the runtime parser never touches raw
    /// strings for configured paths. This is the main performance lever.
    pub fn from_config(config: &Config) -> Self {
        let mut registry = Self {
            children: vec![FxHashMap::default()],     // Root node
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

        registry
    }

    /// Gets or creates a path in the trie, returning its node ID.
    ///
    /// We intentionally parse paths by splitting on "/".
    /// - Leading "/" is ignored.
    /// - Empty segments are ignored (double slashes, trailing slash).
    fn get_or_create_path(&mut self, path_str: &str) -> PathNodeId {
        let parts: Vec<&str> = path_str
            .trim_start_matches('/')
            .split('/')
            .filter(|s| !s.is_empty())
            .collect();

        let mut current_node = PathNodeId::ROOT;

        for part in parts {
            let atom = Atom::from(part);
            current_node = self.get_or_create_child(current_node, atom);
        }

        current_node
    }

    /// Gets or creates a child node for the given parent and name.
    ///
    /// This is the only place we mutate the trie. By isolating that logic, we
    /// avoid duplicating bookkeeping for new nodes.
    fn get_or_create_child(&mut self, parent: PathNodeId, name: Atom) -> PathNodeId {
        if let Some(&child_id) = self.children[parent.index()].get(&name) {
            return child_id;
        }

        // Create a new node and wire it into the trie.
        let new_id = PathNodeId(self.children.len() as u32);
        self.children.push(FxHashMap::default());
        self.node_info.push(PathNodeInfo::default());
        self.children[parent.index()].insert(name, new_id);

        new_id
    }

    /// Looks up a child node by name.
    ///
    /// Returns `None` if the child doesn't exist (path not in config).
    #[inline]
    pub fn get_child(&self, parent: PathNodeId, name: &Atom) -> Option<PathNodeId> {
        self.children.get(parent.index())?.get(name).copied()
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

/// Tracks the current position in the path trie during parsing.
///
/// The parser operates on streaming XML events. We maintain a stack that mirrors
/// the XML nesting depth. Each entry records:
/// - the node ID (if known)
/// - whether this path exists in the registry
///
/// If a path is unknown, we keep pushing "unknown" entries until we exit that
/// subtree. This avoids repeated registry lookups for irrelevant branches.
#[derive(Debug)]
pub struct PathTracker {
    /// Stack of (node_id, is_known_path) pairs representing current XML nesting.
    /// is_known_path is true if the node exists in the registry (path is in config).
    node_stack: Vec<(PathNodeId, bool)>,
    /// Cached atom for reuse during lookups to avoid repeated interning.
    #[allow(dead_code)]
    cached_atom: Option<Atom>,
}

impl PathTracker {
    /// Creates a new path tracker starting at the root.
    pub fn new() -> Self {
        Self {
            node_stack: vec![(PathNodeId::ROOT, true)],
            cached_atom: None,
        }
    }

    /// Enters a child element, updating the current path position.
    ///
    /// Returns the new node ID if the path exists in the registry, or None if
    /// the path is not configured (and thus can be ignored).
    #[inline]
    #[allow(dead_code)]
    pub fn enter(&mut self, name: &str, registry: &PathRegistry) -> Option<PathNodeId> {
        let atom = Atom::from(name);
        self.enter_atom(atom, registry)
    }

    /// Enters a child element using a pre-interned atom.
    #[inline]
    pub fn enter_atom(&mut self, name: Atom, registry: &PathRegistry) -> Option<PathNodeId> {
        let (current_node, current_is_known) = self.node_stack.last().copied().unwrap();

        if !current_is_known {
            // Parent path is not in config, so children can't be either.
            // We still push to keep depth aligned with XML nesting.
            self.node_stack.push((PathNodeId::ROOT, false));
            return None;
        }

        if let Some(child_id) = registry.get_child(current_node, &name) {
            self.node_stack.push((child_id, true));
            Some(child_id)
        } else {
            // Path not in config; mark as unknown and keep depth aligned.
            self.node_stack.push((PathNodeId::ROOT, false));
            None
        }
    }

    /// Leaves the current element, returning to the parent path.
    ///
    /// Returns the node ID that was popped, or None if it wasn't a known path.
    #[inline]
    pub fn leave(&mut self) -> Option<PathNodeId> {
        if self.node_stack.len() > 1 {
            let (node_id, is_known) = self.node_stack.pop().unwrap();
            if is_known {
                return Some(node_id);
            }
        }
        None
    }

    /// Returns the current node ID if it's a known path.
    #[inline]
    pub fn current(&self) -> Option<PathNodeId> {
        let (node_id, is_known) = self.node_stack.last().copied().unwrap();
        if is_known { Some(node_id) } else { None }
    }

    /// Returns the current node ID, or ROOT if unknown.
    #[inline]
    #[allow(dead_code)]
    pub fn current_or_root(&self) -> PathNodeId {
        self.node_stack
            .last()
            .map(|(id, _)| *id)
            .unwrap_or(PathNodeId::ROOT)
    }

    /// Returns true if the current path is known (exists in the registry).
    #[inline]
    #[allow(dead_code)]
    pub fn is_current_known(&self) -> bool {
        self.node_stack
            .last()
            .map(|(_, known)| *known)
            .unwrap_or(false)
    }

    /// Gets or creates a cached atom for the given string.
    ///
    /// This is a micro-optimization for cases where the same element name is
    /// parsed repeatedly in a tight loop.
    #[inline]
    #[allow(dead_code)]
    pub fn intern(&mut self, s: &str) -> Atom {
        if let Some(ref atom) = self.cached_atom {
            if atom.as_ref() == s {
                return atom.clone();
            }
        }
        let atom = Atom::from(s);
        self.cached_atom = Some(atom.clone());
        atom
    }

    /// Returns the depth of the current path (number of segments from root).
    #[inline]
    #[allow(dead_code)]
    pub fn depth(&self) -> usize {
        self.node_stack.len().saturating_sub(1)
    }
}

impl Default for PathTracker {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Config, DType, FieldConfig, TableConfig};

    fn create_test_config() -> Config {
        Config {
            tables: vec![
                TableConfig {
                    name: "items".to_string(),
                    xml_path: "/root/items".to_string(),
                    levels: vec!["item".to_string()],
                    fields: vec![
                        FieldConfig {
                            name: "name".to_string(),
                            xml_path: "/root/items/item/name".to_string(),
                            data_type: DType::Utf8,
                            nullable: false,
                            scale: None,
                            offset: None,
                        },
                        FieldConfig {
                            name: "value".to_string(),
                            xml_path: "/root/items/item/value".to_string(),
                            data_type: DType::Float64,
                            nullable: false,
                            scale: None,
                            offset: None,
                        },
                    ],
                },
                TableConfig {
                    name: "metadata".to_string(),
                    xml_path: "/root/metadata".to_string(),
                    levels: vec![],
                    fields: vec![FieldConfig {
                        name: "version".to_string(),
                        xml_path: "/root/metadata/version".to_string(),
                        data_type: DType::Utf8,
                        nullable: false,
                        scale: None,
                        offset: None,
                    }],
                },
            ],
            parser_options: Default::default(),
        }
    }

    #[test]
    fn test_registry_from_config() {
        let config = create_test_config();
        let registry = PathRegistry::from_config(&config);

        // Check root node exists
        assert!(!registry.is_table_path(PathNodeId::ROOT));

        // Check that table paths are recognized
        let root_atom = Atom::from("root");
        let items_atom = Atom::from("items");
        let metadata_atom = Atom::from("metadata");

        let root_node = registry.get_child(PathNodeId::ROOT, &root_atom).unwrap();
        let items_node = registry.get_child(root_node, &items_atom).unwrap();
        let metadata_node = registry.get_child(root_node, &metadata_atom).unwrap();

        assert!(registry.is_table_path(items_node));
        assert!(registry.is_table_path(metadata_node));
        assert_eq!(registry.get_table_index(items_node), Some(0));
        assert_eq!(registry.get_table_index(metadata_node), Some(1));
    }

    #[test]
    fn test_registry_field_paths() {
        let config = create_test_config();
        let registry = PathRegistry::from_config(&config);

        // Navigate to /root/items/item/name
        let root_atom = Atom::from("root");
        let items_atom = Atom::from("items");
        let item_atom = Atom::from("item");
        let name_atom = Atom::from("name");

        let root_node = registry.get_child(PathNodeId::ROOT, &root_atom).unwrap();
        let items_node = registry.get_child(root_node, &items_atom).unwrap();
        let item_node = registry.get_child(items_node, &item_atom).unwrap();
        let name_node = registry.get_child(item_node, &name_atom).unwrap();

        let info = registry.get_node_info(name_node);
        assert!(!info.is_table());
        assert!(info.has_fields());
        assert_eq!(info.field_indices.len(), 1);
        assert_eq!(info.field_indices[0], (0, 0)); // table 0, field 0
    }

    #[test]
    fn test_path_tracker_basic() {
        let config = create_test_config();
        let registry = PathRegistry::from_config(&config);
        let mut tracker = PathTracker::new();

        // Enter /root
        let node = tracker.enter("root", &registry);
        assert!(node.is_some());
        assert!(!registry.is_table_path(node.unwrap()));

        // Enter /root/items
        let node = tracker.enter("items", &registry);
        assert!(node.is_some());
        assert!(registry.is_table_path(node.unwrap()));

        // Leave /root/items
        let left = tracker.leave();
        assert!(left.is_some());

        // Leave /root
        let left = tracker.leave();
        assert!(left.is_some());
    }

    #[test]
    fn test_path_tracker_unknown_path() {
        let config = create_test_config();
        let registry = PathRegistry::from_config(&config);
        let mut tracker = PathTracker::new();

        // Enter unknown path
        let node = tracker.enter("unknown", &registry);
        assert!(node.is_none());
        assert!(!tracker.is_current_known());

        // Children of unknown paths are also unknown
        let node = tracker.enter("child", &registry);
        assert!(node.is_none());

        // Leave unknown child
        tracker.leave();
        // Leave unknown parent
        tracker.leave();

        // Back to root
        assert!(tracker.is_current_known());
    }

    #[test]
    fn test_root_table_path() {
        let config = Config {
            tables: vec![TableConfig {
                name: "root".to_string(),
                xml_path: "/".to_string(),
                levels: vec![],
                fields: vec![FieldConfig {
                    name: "value".to_string(),
                    xml_path: "/data/value".to_string(),
                    data_type: DType::Utf8,
                    nullable: false,
                    scale: None,
                    offset: None,
                }],
            }],
            parser_options: Default::default(),
        };

        let registry = PathRegistry::from_config(&config);

        // Root path "/" should be a table
        assert!(registry.is_table_path(PathNodeId::ROOT));
        assert_eq!(registry.get_table_index(PathNodeId::ROOT), Some(0));
    }

    #[test]
    fn test_attribute_paths() {
        let config = Config {
            tables: vec![TableConfig {
                name: "items".to_string(),
                xml_path: "/root/items".to_string(),
                levels: vec!["item".to_string()],
                fields: vec![FieldConfig {
                    name: "id".to_string(),
                    xml_path: "/root/items/item/@id".to_string(),
                    data_type: DType::Utf8,
                    nullable: false,
                    scale: None,
                    offset: None,
                }],
            }],
            parser_options: Default::default(),
        };

        let registry = PathRegistry::from_config(&config);
        let mut tracker = PathTracker::new();

        // Navigate to /root/items/item
        tracker.enter("root", &registry);
        tracker.enter("items", &registry);
        tracker.enter("item", &registry);

        // Enter attribute path @id
        let node = tracker.enter("@id", &registry);
        assert!(node.is_some());

        let info = registry.get_node_info(node.unwrap());
        assert!(info.has_fields());
        assert_eq!(info.field_indices[0], (0, 0)); // table 0, field 0
    }
}
