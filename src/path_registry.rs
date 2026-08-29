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
//    - Convert every table path and field path into a trie whose edges are
//      path segments stored as raw bytes.
//    - Store table/field metadata at the terminal node of each path.
//    - Use integer IDs so lookups are array indexing rather than hash maps.
//
// 2) Run-time: PathTracker
//    - Maintain a stack of node IDs corresponding to the current XML depth.
//    - On entering an element, attempt to resolve the child in the registry.
//      If the current path is not in the registry, mark the subtree as unknown
//      to skip further lookups until we pop back out.
//
// This design intentionally accepts upfront construction work at startup in
// exchange for predictable, allocation-free lookups during parsing.

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
    /// Whether this element's closing tag finalizes a row of the innermost
    /// open table — true exactly when this node's **parent** is a table node.
    ///
    /// This is the row-boundary rule, precomputed. Evaluated at runtime it
    /// reads "the closing element is configured *and* its parent frame is a
    /// table", and both halves are static: a trie node has exactly one parent,
    /// and every configured node exists once the registry is built. Hoisting
    /// it here turns the close path into a single bit test on a frame the
    /// parser has already popped, and is what the declared-row element will
    /// extend rather than replace — a declared row marks *itself*, where the
    /// inferred rule marks each configured child of a table.
    ///
    /// Attribute pseudo-nodes are never marked: `parse_attributes` enters and
    /// leaves them directly, without going through the row-finalizing close
    /// path, so an attribute has never delimited a row.
    pub ends_row: bool,
    /// Whether closing this element finalizes a row of **its own** table,
    /// rather than of the enclosing one. Set only by `row: "."`, where the
    /// declared row element *is* the table element.
    ///
    /// It needs its own bit because of ordering: `close_element` pops the
    /// table scope before finalizing a row, so a single flag on a table node
    /// would hand the row to the parent table. `ends_own_row` fires before the
    /// pop, `ends_row` after it, and a node may legitimately carry both — a
    /// `row: "."` table that is itself a configured child of an outer table
    /// closes its own row *and* delimits the outer table's.
    pub ends_own_row: bool,
    /// Whether this node is a `stop_at_paths` target, so that closing it ends
    /// the parse. Replaces a linear scan of the configured stop paths on every
    /// element close.
    pub is_stop: bool,
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
/// The registry compiles all configured XML paths into a trie keyed by path-segment bytes.
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

    /// Iterates `(name, id)` pairs, whichever representation is in use.
    fn entries(&self) -> Box<dyn Iterator<Item = (&[u8], PathNodeId)> + '_> {
        match self {
            NodeChildren::Small(entries) => {
                Box::new(entries.iter().map(|(name, id)| (name.as_ref(), *id)))
            }
            NodeChildren::Large(map) => Box::new(map.iter().map(|(name, id)| (name.as_ref(), *id))),
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

        // Phase 4: register optional stop paths and mark them, so ending the
        // parse is a bit test rather than a scan of the configured paths.
        for stop_path in &config.parser_options.stop_at_paths {
            let node_id = registry.get_or_create_path(stop_path);
            registry.node_info[node_id.index()].is_stop = true;
        }

        // Phase 5a: mark declared row elements (`row:`). A declared row marks
        // *itself*, where the inferred rule below marks each configured child
        // of a table — the same bit, on a different node, which is what keeps
        // v1 and v2 row semantics on one code path rather than two engines.
        //
        // Runs before 5b because resolving a row path can create trie nodes,
        // and 5b's `children` iteration must see the finished trie.
        for table_config in &config.tables {
            let Some(row_path) = table_config.row_path() else {
                continue;
            };
            let table_node = registry.get_or_create_path(&table_config.xml_path);
            let row_node = registry.get_or_create_path(&row_path);
            if row_node == table_node {
                // `row: "."` — one row per table element. Finalized before the
                // table scope is popped; see `PathNodeInfo::ends_own_row`.
                registry.node_info[row_node.index()].ends_own_row = true;
            } else {
                registry.node_info[row_node.index()].ends_row = true;
            }
        }

        // Phase 5b: mark the nodes whose closing tag finalizes a row — every
        // element child of a table node. See `PathNodeInfo::ends_row` for why
        // this is exactly the rule the parser used to evaluate per event.
        //
        // Skipped for tables that declared a row: for those, phase 5a has
        // already named the element, and marking the children too would
        // finalize a row per child *as well*, which is the very behavior
        // `row:` exists to replace.
        //
        // Must run *after* phase 4: a stop path can introduce nodes, and a node
        // introduced under a table path delimits rows exactly like any other
        // configured child. Marking before phase 4 would miss those and quietly
        // change row counts for configs that combine the two.
        for node_idx in 0..registry.children.len() {
            let Some(table_idx) = registry.node_info[node_idx].table_index else {
                continue;
            };
            if config.tables[table_idx].row.is_some() {
                continue;
            }
            // Move the child list aside rather than collecting it: marking goes
            // through `registry`, which would otherwise stay borrowed for the
            // whole loop. `NodeChildren::new()` is an empty `Vec`, so the swap
            // costs no allocation — and this runs in `Parser::new`, whose fixed
            // cost is the whole parse for anyone handling small documents.
            let children = std::mem::replace(&mut registry.children[node_idx], NodeChildren::new());
            for (name, child_id) in children.entries() {
                if !name.starts_with(b"@") {
                    registry.node_info[child_id.index()].ends_row = true;
                }
            }
            registry.children[node_idx] = children;
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
/// Carries everything the parser needs when the matching close arrives, all
/// materialized once at `enter()` from `PathNodeInfo`. Closing an element is
/// therefore a pop plus four bit tests, with no registry lookup and no look
/// at the parent frame.
#[derive(Debug, Clone, Copy)]
struct StackEntry {
    /// Node ID for known paths; `ROOT` for unknown subtrees (placeholder).
    node_id: PathNodeId,
    /// Whether `node_id` corresponds to a configured path.
    is_known: bool,
    /// Whether this node is a table boundary. Only meaningful when `is_known`.
    is_table: bool,
    /// Whether closing this element finalizes a row. See
    /// [`PathNodeInfo::ends_row`].
    ends_row: bool,
    /// Whether closing this element finalizes a row of its *own* table. See
    /// [`PathNodeInfo::ends_own_row`].
    ends_own_row: bool,
    /// Whether closing this element ends the parse (`stop_at_paths`).
    is_stop: bool,
}

/// The bits a closing element carries, returned by [`PathTracker::leave`].
///
/// An unknown element (one outside every configured path) yields all-false:
/// it opens no table scope, delimits no row, and stops nothing — which is what
/// keeps a document's unconfigured subtrees from perturbing the output.
#[derive(Debug, Clone, Copy, Default)]
pub struct ClosedFrame {
    pub is_table: bool,
    pub ends_row: bool,
    pub ends_own_row: bool,
    pub is_stop: bool,
}

/// Tracks the current position in the path trie during parsing.
///
/// The parser operates on streaming XML events. We maintain a stack that
/// mirrors XML nesting depth; the bottom is always the implicit document
/// root, so the stack is never empty.
///
/// Each frame carries every bit the parser needs at `Event::End` /
/// `Event::Empty` time, captured once when `enter()` resolves the node.
/// Closing an element is therefore a single pop: the popped frame answers
/// "was this a table scope?", "does this finalize a row?" and "does this end
/// the parse?" on its own, without re-reading `node_info[]` and without
/// consulting the parent frame.
///
/// If a path is unknown, we keep pushing "unknown" placeholder frames until
/// we exit that subtree. This avoids repeated registry lookups for
/// irrelevant branches.
#[derive(Debug)]
pub struct PathTracker {
    node_stack: Vec<StackEntry>,
}

impl StackEntry {
    /// A frame for an element outside every configured path. Unknown subtrees
    /// push these to keep depth aligned with XML nesting without touching the
    /// registry again.
    const UNKNOWN: Self = Self {
        node_id: PathNodeId::ROOT,
        is_known: false,
        is_table: false,
        ends_row: false,
        ends_own_row: false,
        is_stop: false,
    };
}

impl PathTracker {
    /// Creates a new path tracker starting at the root.
    ///
    /// `registry` is consulted exactly once, to seed the root frame's
    /// `is_table` flag. The root frame is never popped, so its remaining bits
    /// are inert.
    pub fn new(registry: &PathRegistry) -> Self {
        let root_is_table = registry.is_table_path(PathNodeId::ROOT);
        Self {
            node_stack: vec![StackEntry {
                node_id: PathNodeId::ROOT,
                is_known: true,
                is_table: root_is_table,
                // The root frame is never popped, so these are inert.
                ends_row: false,
                ends_own_row: false,
                is_stop: false,
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
            self.node_stack.push(StackEntry::UNKNOWN);
            return None;
        }

        if let Some(child_id) = registry.get_child(top.node_id, name) {
            let info = registry.get_node_info(child_id);
            self.node_stack.push(StackEntry {
                node_id: child_id,
                is_known: true,
                is_table: info.is_table(),
                ends_row: info.ends_row,
                ends_own_row: info.ends_own_row,
                is_stop: info.is_stop,
            });
            Some((child_id, info))
        } else {
            self.node_stack.push(StackEntry::UNKNOWN);
            None
        }
    }

    /// Leaves the current element, returning to the parent path and handing
    /// back the closing element's precomputed bits.
    ///
    /// Returns `None` at root depth, where there is no open element to close —
    /// which is how a stray end tag stays a no-op.
    #[inline]
    pub fn leave(&mut self) -> Option<ClosedFrame> {
        if self.node_stack.len() > 1 {
            let entry = self.node_stack.pop().unwrap();
            return Some(ClosedFrame {
                is_table: entry.is_table,
                ends_row: entry.ends_row,
                ends_own_row: entry.ends_own_row,
                is_stop: entry.is_stop,
            });
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

    /// The row-boundary rule, checked at the level it is now decided: which
    /// nodes carry `ends_row`. Evaluated per event it read "the closing element
    /// is configured and its parent is a table"; these cases are the ones where
    /// a precomputed form could drift from that.
    mod ends_row {
        use super::*;

        fn registry(yaml: &str) -> PathRegistry {
            PathRegistry::from_config(&config_from_yaml!(yaml))
        }

        fn ends_row(registry: &PathRegistry, path: &[&[u8]]) -> bool {
            let mut node = PathNodeId::ROOT;
            for segment in path {
                node = registry.get_child(node, segment).expect("configured path");
            }
            registry.get_node_info(node).ends_row
        }

        const NESTED: &str = r#"
tables:
  - name: outer
    xml_path: /data
    levels: []
    fields:
      - {name: v, xml_path: /data/item/v, data_type: Int32}
  - name: inner
    xml_path: /data/item/sub
    levels: []
    fields:
      - {name: w, xml_path: /data/item/sub/row/w, data_type: Int32}
"#;

        #[test]
        fn children_of_a_table_delimit_rows() {
            let registry = registry(NESTED);
            assert!(ends_row(&registry, &[b"data", b"item"]));
            assert!(ends_row(&registry, &[b"data", b"item", b"sub", b"row"]));
        }

        #[test]
        fn grandchildren_of_a_table_do_not() {
            // `v` sits under `item`, which is not a table, so closing it must
            // not finalize anything.
            let registry = registry(NESTED);
            assert!(!ends_row(&registry, &[b"data", b"item", b"v"]));
            assert!(!ends_row(
                &registry,
                &[b"data", b"item", b"sub", b"row", b"w"]
            ));
        }

        #[test]
        fn what_delimits_is_the_parent_being_a_table_not_the_node_itself() {
            // `sub` opens a table scope, but its parent `item` is not a table,
            // so closing it finalizes nothing. Being a table says nothing about
            // whether *this* element ends a row — only the parent does.
            let nested = registry(NESTED);
            assert!(!ends_row(&nested, &[b"data", b"item", b"sub"]));

            // Whereas a table that *is* a direct child of another table both
            // opens a scope and ends a row of the enclosing table.
            let sibling = registry(
                r#"
tables:
  - name: outer
    xml_path: /data
    levels: []
    fields:
      - {name: n, xml_path: /data/n, data_type: Int32}
  - name: inner
    xml_path: /data/group
    levels: []
    fields:
      - {name: v, xml_path: /data/group/row/v, data_type: Int32}
"#,
            );
            assert!(ends_row(&sibling, &[b"data", b"group"]));
        }

        #[test]
        fn attributes_never_delimit_rows() {
            let registry = registry(
                r#"
tables:
  - name: items
    xml_path: /data
    levels: []
    fields:
      - {name: id, xml_path: /data/@id, data_type: Utf8}
      - {name: v, xml_path: /data/item/v, data_type: Int32}
"#,
            );
            assert!(!ends_row(&registry, &[b"data", b"@id"]));
            assert!(ends_row(&registry, &[b"data", b"item"]));
        }

        #[test]
        fn a_stop_path_under_a_table_delimits_a_row_too() {
            // Stop paths are registered before this is computed, so a node a
            // stop path introduces delimits rows like any other configured
            // child. Computing the flags first would silently drop that row.
            let registry = registry(
                r#"
parser_options:
  stop_at_paths: [/data/marker]
tables:
  - name: items
    xml_path: /data
    levels: []
    fields:
      - {name: v, xml_path: /data/item/v, data_type: Int32}
"#,
            );
            let marker = &[b"data".as_slice(), b"marker".as_slice()];
            assert!(ends_row(&registry, marker));
            let mut node = PathNodeId::ROOT;
            for segment in marker {
                node = registry.get_child(node, segment).unwrap();
            }
            assert!(registry.get_node_info(node).is_stop);
        }

        #[test]
        fn the_root_table_marks_the_document_element() {
            let registry = registry(
                r#"
tables:
  - name: doc
    xml_path: /
    levels: []
    fields:
      - {name: title, xml_path: /report/title, data_type: Utf8}
"#,
            );
            assert!(ends_row(&registry, &[b"report"]));
            assert!(!ends_row(&registry, &[b"report", b"title"]));
        }
    }
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
