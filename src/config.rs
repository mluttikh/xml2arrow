//! Configuration: the mapping from XML paths to Arrow tables and fields.
//!
//! A [`Config`] is normally written as YAML and loaded with
//! [`Config::from_yaml_file`] (or the `config_from_yaml!` macro in tests), but
//! it is an ordinary struct and [`Config::builder`] builds one in Rust.
//!
//! Two things are worth knowing before reading further:
//!
//! - **Everything is validated up front.** [`Config::validate`] runs when a
//!   config is built or loaded, and again in `Parser::new`. A configuration
//!   mistake is an error at load rather than a wrong column at parse — which
//!   is the trade this crate makes everywhere, since silently plausible data
//!   is the worst failure it could produce.
//! - **Newer keys are opt-in and additive.** [`TableConfig::row`],
//!   [`TableConfig::links`] and [`FieldConfig::path`] each replace an older
//!   mechanism, and a config that sets none of them behaves exactly as it did
//!   before they existed. [`Config::version`] is how a config asserts it has
//!   finished adopting them.
//!
//! See the crate-level documentation for a worked example, and `MIGRATION.md`
//! for moving an existing config forward.

use std::{
    borrow::Cow,
    collections::HashSet,
    fs::File,
    io::{BufReader, BufWriter},
    path::Path,
};

use crate::errors::{ConfigIssue, ConversionKind, Error, Result};
use arrow::datatypes::DataType;
use serde::{Deserialize, Serialize};

/// Configuration for the XML parser.
///
/// Marked `#[non_exhaustive]`: construct via [`ParserOptions::default`] and
/// mutate the fields you care about, so that adding an option in a future
/// release stays a non-breaking change.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[non_exhaustive]
pub struct ParserOptions {
    /// Whether to trim whitespace from text nodes. Defaults to false.
    #[serde(default)]
    pub trim_text: bool,
    /// Optional XML paths where parsing should stop after the closing tag.
    #[serde(default)]
    pub stop_at_paths: Vec<String>,
    /// Whether quick-xml should verify each closing tag's name matches the
    /// most recently opened tag. Defaults to `true` — malformed inputs
    /// surface as a parsing error.
    ///
    /// Setting `false` skips that per-end-tag check, a measurable ~2–6%
    /// throughput improvement on representative workloads. The trade-off
    /// is that an opening/closing-tag mismatch (e.g. `<a>...</b>`) is no
    /// longer rejected; the parser will silently emit an `Event::End` and
    /// our `PathTracker` will pop the top frame regardless, which can
    /// yield subtly wrong row counts. Only use when you trust the input
    /// to be well-formed (e.g. produced by your own pipeline).
    #[serde(default = "default_true")]
    pub validate_closing_tags: bool,
    /// Whether quick-xml should reject duplicate attribute names on a single
    /// element. Defaults to `true` — duplicates surface as a parsing error.
    ///
    /// Setting `false` disables that check. Beyond skipping an O(n²) scan
    /// over an element's attributes, this removes a heap allocation that
    /// quick-xml otherwise makes **per attribute-bearing element** (it
    /// records each key's byte range in a `Vec` to compare against). On
    /// attribute-heavy documents that allocation is the dominant cost of
    /// the check. The trade-off mirrors `validate_closing_tags`: a
    /// malformed element with a duplicated attribute is no longer rejected.
    /// Because field values accumulate by appending, a duplicated attribute's
    /// values are concatenated rather than reported as an error. Only disable
    /// when the input is trusted to be well-formed.
    #[serde(default = "default_true")]
    pub validate_attributes: bool,
    /// Whether to strip XML namespace prefixes from element and attribute
    /// names before matching them against configured paths. Defaults to
    /// `true` — `<ns:sensor>` matches a config path of `sensor`.
    ///
    /// Internally `true` resolves each name with quick-xml's `local_name()`,
    /// which scans the name for a `:` separator on **every** element and
    /// attribute. For documents that use no namespace prefixes (the common
    /// case for the data XML this crate targets) that scan finds nothing and
    /// is pure overhead — a measurable ~4–7% of total parse time. Setting
    /// `false` uses the raw qualified name (`name()`) and skips the scan.
    ///
    /// The trade-off: with `false`, configured paths must spell out any
    /// prefix exactly as it appears in the document (`ns:sensor`, not
    /// `sensor`). For namespace-free input the two modes produce identical
    /// results, so disabling is free; only disable when your input either
    /// uses no prefixes or your config already encodes them.
    #[serde(default = "default_true")]
    pub strip_namespaces: bool,
    /// Whether to accept input that ends while elements are still open.
    /// Defaults to `false`.
    ///
    /// A document cut short mid-element yields the rows parsed before the cut,
    /// which is indistinguishable from a complete parse — silent data loss. By
    /// default such input raises
    /// [`Error::TruncatedInput`](crate::errors::Error) and no batches are
    /// returned.
    ///
    /// Set to `true` only for recovery tooling that deliberately reads partial
    /// documents (salvaging a killed writer's output, tailing a log). To stop
    /// parsing early at a *known* point instead, use
    /// [`stop_at_paths`](Self::stop_at_paths), which is unaffected by this
    /// option.
    #[serde(default)]
    pub allow_truncated_input: bool,
    /// Whether to fail the parse when a configured field never captured a
    /// value anywhere in the document. Defaults to `false`.
    ///
    /// The usual cause is a misspelled `xml_path`, whose symptom is otherwise
    /// a silently all-null or all-empty column — the config looks fine and the
    /// data looks wrong. With this enabled, every offending field is reported
    /// at once as [`Error::UnmatchedFields`](crate::errors::Error).
    ///
    /// Off by default because a field that legitimately appears in only *some*
    /// documents is a normal configuration, and enabling it would change the
    /// outcome of parses that work today. A future release makes strictness
    /// the default and moves the opt-out to the field.
    ///
    /// **Interaction with [`stop_at_paths`](Self::stop_at_paths):** stopping
    /// early guarantees that every field below the stop path captures nothing,
    /// so combining the two reports those fields as unmatched. That is
    /// accurate but rarely what the caller means, and the error says so rather
    /// than blaming the spelling. Use one or the other, or split the config so
    /// that the header-only parse configures only header fields.
    #[serde(default)]
    pub error_on_unmatched_fields: bool,
    /// Maximum number of bytes a single field value may accumulate, or `None`
    /// (the default) for no limit.
    ///
    /// Element text arrives as a series of events — text, CDATA, resolved
    /// character references — that append into one value, so an adversarial or
    /// corrupt document can otherwise grow a single value without bound. Once
    /// the cap would be crossed the parser stops appending and the row raises
    /// [`Error::ValueTooLarge`](crate::errors::Error) when it finalizes.
    ///
    /// This bounds *our* accumulation, not the XML reader's: quick-xml still
    /// materializes each individual event before we see it, so one enormous
    /// text node is still buffered once by the reader. The guard is what keeps
    /// many such events from adding up.
    #[serde(default)]
    pub max_value_bytes: Option<usize>,
}

impl Default for ParserOptions {
    fn default() -> Self {
        Self {
            trim_text: false,
            stop_at_paths: Vec::new(),
            validate_closing_tags: true,
            validate_attributes: true,
            strip_namespaces: true,
            allow_truncated_input: false,
            error_on_unmatched_fields: false,
            max_value_bytes: None,
        }
    }
}

fn default_true() -> bool {
    true
}

/// Splits an XML path into its segments using the same rules as the path
/// registry: the leading `/` is ignored and empty segments (double or
/// trailing slashes) are skipped. Validation and runtime matching must agree
/// on what a "path" is, so this mirrors `PathRegistry::get_or_create_path`.
pub(crate) fn path_segments(path: &str) -> impl DoubleEndedIterator<Item = &str> {
    path.trim_start_matches('/')
        .split('/')
        .filter(|s| !s.is_empty())
}

/// Returns true when `descendant` is equal to or nested under `ancestor`,
/// compared segment-wise. A plain string prefix test is not enough:
/// `/root/items_other` starts with `/root/item` as a string but is not under
/// it as a path. The root path `/` has no segments, so everything is under it.
pub(crate) fn path_is_under(descendant: &str, ancestor: &str) -> bool {
    // Fast path for canonical spellings: a character-level prefix whose cut
    // lands on a segment boundary is exactly the segment-wise relation.
    // Validation runs once per `Parser::new`, but that fixed cost dominates
    // tiny-document parses, so the common case stays a single memcmp.
    if let Some(rest) = descendant.strip_prefix(ancestor)
        && (rest.is_empty() || rest.starts_with('/') || ancestor.ends_with('/'))
    {
        return true;
    }
    // Slow path normalizes non-canonical spellings (duplicate/trailing
    // slashes, missing leading slash).
    let mut descendant_segments = path_segments(descendant);
    path_segments(ancestor).all(|segment| descendant_segments.next() == Some(segment))
}

/// Returns true when `descendant` is nested *strictly* inside `ancestor` —
/// under it, and not the same path.
///
/// The distinction is load-bearing wherever one table's scope is compared with
/// another's. "Is this table inside that one?" and "is this table inside or
/// equal to that one?" are different questions with different right answers: a
/// table whose path equals another's is a sibling in scope, not a child, and
/// treating it as a child produces links to itself and rows attributed to the
/// wrong table. Spelling the pair out at each site invited getting one half
/// wrong, so it is named once here.
pub(crate) fn path_is_strictly_under(descendant: &str, ancestor: &str) -> bool {
    !paths_equal(descendant, ancestor) && path_is_under(descendant, ancestor)
}

/// Resolves a declared `row` against its table's `xml_path`.
///
/// `"."` names the table element itself, a leading `/` means the path is
/// already absolute, and anything else is relative to `xml_path`. All three
/// land on a single trie node, which is what makes `version: 2`'s eventual
/// switch to absolute-only a pure key rename rather than a semantic change.
pub(crate) fn resolve_row_path(xml_path: &str, row: &str) -> String {
    if row == "." {
        return xml_path.to_string();
    }
    if row.starts_with('/') {
        return row.to_string();
    }
    format!("{}/{}", xml_path.trim_end_matches('/'), row)
}

/// Resolves a field's declared location to an absolute path.
///
/// Borrows for the absolute spellings — which is every field written before
/// `path:` existed, and most written after — so the common case adds no
/// allocation to `Parser::new`. Only a genuinely relative path builds a
/// `String`.
///
/// Returns `None` when the field is relative but its table declares no `row:`,
/// which [`Config::validate`] rejects; callers past validation can expect
/// `Some`.
pub(crate) fn resolve_field_path<'a>(
    table: &TableConfig,
    field: &'a FieldConfig,
) -> Option<Cow<'a, str>> {
    if let Some(xml_path) = field.xml_path.as_deref() {
        return Some(Cow::Borrowed(xml_path));
    }
    let path = field.path.as_deref()?;
    if path.starts_with('/') {
        return Some(Cow::Borrowed(path));
    }
    // Relative: to the row element, not to `xml_path`. A field is part of a
    // row, so the row is the only base that makes `sensor/@id` mean the same
    // thing wherever the table sits in the document.
    let row_path = table.row_path()?;
    Some(Cow::Owned(format!(
        "{}/{}",
        row_path.trim_end_matches('/'),
        path
    )))
}

/// Returns true when two paths resolve to the same registry node, i.e. their
/// normalized segment sequences are identical regardless of spelling
/// ("/data", "data" and "/data/" are all the same path).
pub(crate) fn paths_equal(a: &str, b: &str) -> bool {
    path_segments(a).eq(path_segments(b))
}

/// Top-level configuration for XML to Arrow conversion.
///
/// This struct holds a collection of `TableConfig` structs, each defining how a specific
/// part of the XML document should be parsed into an Arrow table.
///
/// Marked `#[non_exhaustive]`: build one with [`Config::builder`] (or load it
/// from YAML), so that adding a configuration key in a future release stays a
/// non-breaking change. The fields remain public for reading and mutation.
///
/// ```rust
/// use xml2arrow::config::{Config, DType, FieldConfigBuilder, TableConfig};
///
/// let config = Config::builder()
///     .table(
///         TableConfig::builder("items", "/data")
///             .field(FieldConfigBuilder::new("value", "/data/item/value", DType::Int32).build()?)
///             .build(),
///     )
///     .build()?;
/// # Ok::<(), xml2arrow::Error>(())
/// ```
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[non_exhaustive]
pub struct Config {
    /// Which generation of configuration semantics this file is written
    /// against. Absent — or `1` — is every release before this one.
    ///
    /// `2` is an **assertion, not a switch**. It does not select a different
    /// engine; it says "this config is fully migrated", and validation holds it
    /// to that: every table declares [`TableConfig::row`], no table uses
    /// `levels`, and every field uses `path` rather than the deprecated
    /// `xml_path`. A config that has not finished migrating is rejected at load
    /// with a message naming what is left, rather than parsing under semantics
    /// its author did not intend.
    ///
    /// In exchange it opts into the value defaults 1.0 will make mandatory,
    /// a full release cycle early: `trim` on for every type, and a missing
    /// non-nullable value an error whatever the column's type — the two places
    /// where the historical defaults differ by type rather than by intent.
    /// Both are overridable per field, and deleting the line reverts
    /// everything.
    ///
    /// Declaring `2` therefore has one purpose: to find out, at load time and
    /// on your own schedule, whether you are ready for 1.0.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<u32>,
    /// A vector of `TableConfig` structs, each defining a table to be extracted from the XML.
    pub tables: Vec<TableConfig>,
    /// Parser options.
    #[serde(default)]
    pub parser_options: ParserOptions,
    /// Value-handling policies applied to every field that does not set its
    /// own. Absent leaves every field on current behavior.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub defaults: Option<ValuePolicies>,
}

impl Config {
    /// Starts building a configuration programmatically.
    ///
    /// See [`ConfigBuilder`] for the full example; [`ConfigBuilder::build`]
    /// runs [`Config::validate`], so a config obtained this way is always
    /// structurally valid.
    #[must_use]
    pub fn builder() -> ConfigBuilder {
        ConfigBuilder::default()
    }

    /// Validates the configuration for structural correctness and field constraints.
    ///
    /// Checks performed:
    /// - Table names must be non-empty and unique across the configuration.
    /// - Table `xml_path` values must be non-empty and unique (segment-wise):
    ///   the path registry stores one table per path node, so a duplicate
    ///   would silently starve the earlier table of rows.
    /// - Field names must be non-empty and unique within each table.
    /// - Field `xml_path` values must be non-empty.
    /// - Field `xml_path` must be a descendant of (or equal to) the parent table's
    ///   `xml_path`, compared per path segment. The root table `/` allows any field path.
    /// - Scale/offset may only be used with Float32 and Float64 fields.
    /// - When [`Config::version`] declares `2`, the config must be fully
    ///   migrated: every table declares `row:`, none uses `levels:`, every
    ///   field uses `path:` rather than `xml_path:`, and a table nested inside
    ///   another declares `links:`. These are checked last, so a config that is
    ///   both broken and unmigrated reports the breakage first.
    ///
    /// # Errors
    ///
    /// Returns an error if any of the above constraints are violated.
    pub fn validate(&self) -> Result<()> {
        // One pass per table rather than one pass per concern, so the first
        // error a broken config reports is the first problem in document
        // order rather than the first problem of whichever kind is checked
        // earliest. The four helpers below are in the order a reader meets
        // them in the YAML.
        let mut table_names = HashSet::with_capacity(self.tables.len());
        for (table_idx, table) in self.tables.iter().enumerate() {
            self.validate_table_identity(table, table_idx, &mut table_names)?;
            self.validate_declared_row(table)?;
            self.validate_declared_links(table)?;
            self.validate_table_fields(table)?;
        }

        // Last, so that a config which is broken *and* not yet migrated hears
        // about the breakage first: "this path is not under its table" is a
        // bug, while "this table still uses levels" is unfinished migration.
        self.validate_declared_version()?;
        Ok(())
    }

    /// A table must be nameable and addressable: a non-empty name unique
    /// across the config, and a non-empty `xml_path` no earlier table claims.
    ///
    /// `table_names` accumulates across the caller's loop, which is why it is
    /// threaded through rather than rebuilt here.
    fn validate_table_identity<'c>(
        &'c self,
        table: &'c TableConfig,
        table_idx: usize,
        table_names: &mut HashSet<&'c String>,
    ) -> Result<()> {
        if table.name.is_empty() {
            return Err(ConfigIssue::EmptyTableName.into());
        }
        if !table_names.insert(&table.name) {
            return Err(ConfigIssue::DuplicateTableName {
                name: table.name.clone(),
            }
            .into());
        }
        if table.xml_path.is_empty() {
            return Err(ConfigIssue::EmptyTableXmlPath {
                table: table.name.clone(),
            }
            .into());
        }
        // Duplicate-path detection compares normalized segments so that
        // "/data", "data" and "/data/" — which all resolve to the same
        // registry node — count as duplicates. A pairwise scan (rather
        // than a hash set of built keys) keeps `Parser::new` free of
        // per-table allocations; table counts are small.
        for earlier_table in &self.tables[..table_idx] {
            if paths_equal(&earlier_table.xml_path, &table.xml_path) {
                return Err(ConfigIssue::DuplicateTableXmlPath {
                    table_a: earlier_table.name.clone(),
                    table_b: table.name.clone(),
                    xml_path: table.xml_path.clone(),
                }
                .into());
            }
        }
        Ok(())
    }

    /// Checks a declared `row:` — that it names something, resolves inside its
    /// own table, and that no other table would intercept the rows it
    /// delimits. A table that leaves row boundaries inferred has nothing to
    /// check.
    fn validate_declared_row(&self, table: &TableConfig) -> Result<()> {
        if let Some(row) = &table.row {
            if row.trim().is_empty() {
                return Err(ConfigIssue::EmptyRowPath {
                    table: table.name.clone(),
                }
                .into());
            }
            let row_path = resolve_row_path(&table.xml_path, row);
            if !path_is_under(&row_path, &table.xml_path) {
                return Err(ConfigIssue::RowPathNotUnderTable {
                    table: table.name.clone(),
                    table_path: table.xml_path.clone(),
                    row: row.clone(),
                    row_path,
                }
                .into());
            }
            // The implicit document root is never closed by the parser —
            // `PathTracker`'s bottom frame is never popped — so a root
            // table whose row resolves to itself could never finalize one.
            // Rejected rather than silently returning an empty table, and
            // rejected loudly because *inference* handles this shape fine:
            // adding `row: "."` here would turn a working config into an
            // empty one.
            if paths_equal(&row_path, &table.xml_path)
                && path_segments(&table.xml_path).next().is_none()
            {
                return Err(ConfigIssue::RowIsRootTable {
                    table: table.name.clone(),
                }
                .into());
            }
            // A row is finalized against the *innermost open table*, so a
            // second table sitting strictly between this one and its row
            // element would receive the row instead — silently, with a
            // plausible-looking batch as the only evidence. Reject it.
            // A table whose path *equals* the row path is fine: its scope
            // is popped before the row is finalized, so the row still
            // lands here.
            for other in &self.tables {
                if path_is_strictly_under(&other.xml_path, &table.xml_path)
                    && path_is_strictly_under(&row_path, &other.xml_path)
                {
                    return Err(ConfigIssue::RowPathCrossesTable {
                        table: table.name.clone(),
                        row_path,
                        nested_table: other.name.clone(),
                        nested_table_path: other.xml_path.clone(),
                    }
                    .into());
                }
            }
        }
        Ok(())
    }

    /// Checks declared `links:` — that each names exactly one kind, refers to
    /// a table that genuinely encloses this one, and contributes a column name
    /// nothing else has claimed.
    fn validate_declared_links(&self, table: &TableConfig) -> Result<()> {
        let Some(links) = &table.links else {
            return Ok(());
        };
        if !table.levels.is_empty() {
            return Err(ConfigIssue::LinksAndLevels {
                table: table.name.clone(),
            }
            .into());
        }

        let scope = table.link_scope_path();
        // Seeded with the field names so a link column that shadowed a field
        // is caught by the same check as one that shadows another link.
        let mut column_names: HashSet<String> = table
            .fields
            .iter()
            .map(|field| field.name.clone())
            .collect();

        for link in links {
            match (link.parent.as_deref(), link.index_of.as_deref()) {
                (Some(parent), None) => self.validate_parent_link(table, &scope, parent)?,
                (None, Some(index_of)) => self.validate_index_of_link(table, &scope, index_of)?,
                // Neither or both. They produce different column types with
                // different guarantees, so picking one would be a guess.
                _ => {
                    return Err(ConfigIssue::LinkKindAmbiguous {
                        table: table.name.clone(),
                    }
                    .into());
                }
            }

            // A link column that shadowed a field would be a silently wrong
            // column, so collisions are rejected.
            if let Some(column) = link.column_name()
                && !column_names.insert(column.clone())
            {
                return Err(ConfigIssue::LinkColumnCollision {
                    table: table.name.clone(),
                    column,
                }
                .into());
            }
        }
        Ok(())
    }

    /// Checks a `parent:` link — that the named table exists and genuinely
    /// encloses this one.
    ///
    /// Checking it *by name* is the whole advantage over `levels`, which took
    /// its values from whatever happened to enclose the table and so could
    /// mis-align silently, producing a column of plausible wrong numbers.
    fn validate_parent_link(
        &self,
        table: &TableConfig,
        scope: &str,
        parent_name: &str,
    ) -> Result<()> {
        let Some(parent) = self.tables.iter().find(|t| t.name == parent_name) else {
            return Err(ConfigIssue::UnknownParentTable {
                table: table.name.clone(),
                parent: parent_name.to_string(),
            }
            .into());
        };
        let parent_scope = parent.link_scope_path();
        if !path_is_strictly_under(scope, &parent_scope) {
            return Err(ConfigIssue::ParentNotAncestor {
                table: table.name.clone(),
                table_path: scope.to_string(),
                parent: parent_name.to_string(),
                parent_path: parent_scope,
            }
            .into());
        }
        Ok(())
    }

    /// Checks an `index_of:` link — that the path names the row element of a
    /// table enclosing this one.
    ///
    /// Restricted to an enclosing *table*, deliberately: such a table already
    /// counts exactly this, so the ordinal costs nothing at parse time and is
    /// identical to the legacy `<level>` value for the same path. An arbitrary
    /// path would need a per-node occurrence counter maintained on every
    /// element open.
    fn validate_index_of_link(
        &self,
        table: &TableConfig,
        scope: &str,
        index_of: &str,
    ) -> Result<()> {
        let is_ancestor_table = self.tables.iter().any(|t| {
            let other = t.link_scope_path();
            paths_equal(&other, index_of) && path_is_strictly_under(scope, &other)
        });
        if !is_ancestor_table {
            return Err(ConfigIssue::IndexOfNotAncestorTable {
                table: table.name.clone(),
                index_of: index_of.to_string(),
            }
            .into());
        }
        Ok(())
    }

    /// Checks one table's fields: nameable, addressable by exactly one of
    /// `path`/`xml_path`, resolving inside the table, and carrying no policy
    /// that cannot apply to the column it is set on.
    fn validate_table_fields(&self, table: &TableConfig) -> Result<()> {
        let mut field_names = HashSet::with_capacity(table.fields.len());
        for field in &table.fields {
            if field.name.is_empty() {
                return Err(ConfigIssue::EmptyFieldName {
                    table: table.name.clone(),
                }
                .into());
            }
            if !field_names.insert(&field.name) {
                return Err(ConfigIssue::DuplicateFieldName {
                    table: table.name.clone(),
                    field: field.name.clone(),
                }
                .into());
            }
            // `path` and `xml_path` are two spellings of one thing, so
            // exactly one must be present. Accepting both would mean
            // silently picking a winner.
            match (field.path.as_deref(), field.xml_path.as_deref()) {
                (Some(_), Some(_)) => {
                    return Err(ConfigIssue::FieldPathConflict {
                        table: table.name.clone(),
                        field: field.name.clone(),
                    }
                    .into());
                }
                (None, None) => {
                    return Err(ConfigIssue::FieldPathMissing {
                        table: table.name.clone(),
                        field: field.name.clone(),
                    }
                    .into());
                }
                (Some(p), None) | (None, Some(p)) if p.trim().is_empty() => {
                    return Err(ConfigIssue::EmptyFieldXmlPath {
                        table: table.name.clone(),
                        field: field.name.clone(),
                    }
                    .into());
                }
                _ => {}
            }

            // A relative `path` is relative to the row element, so without
            // a declared row there is nothing to resolve against. Caught
            // here rather than resolving to something plausible-looking.
            let Some(field_path) = resolve_field_path(table, field) else {
                return Err(ConfigIssue::RelativeFieldPathWithoutRow {
                    table: table.name.clone(),
                    field: field.name.clone(),
                    path: field.path.clone().unwrap_or_default(),
                }
                .into());
            };

            // Field path must be under the table path, compared per
            // segment (the root table "/" has no segments and thus
            // accepts any field path).
            if !path_is_under(&field_path, &table.xml_path) {
                return Err(ConfigIssue::FieldPathNotUnderTable {
                    table: table.name.clone(),
                    table_path: table.xml_path.clone(),
                    field: field.name.clone(),
                    field_path: field_path.into_owned(),
                }
                .into());
            }

            self.validate_field_policies(table, field)?;
            field.validate()?;
        }
        Ok(())
    }

    /// Rejects a value policy that cannot apply to the column it is set on.
    ///
    /// Quietly ignoring one would leave the config saying a thing the data
    /// does not do, which is the failure mode this crate spends most of its
    /// validation budget avoiding. There are only three ways to be
    /// inapplicable, and all three are a mismatch with the column rather than
    /// with the policy: asking for null on a column that cannot hold one, or
    /// for an empty value on a type that has none.
    ///
    /// Checked against the *effective* policy — the field's own layered over
    /// the config-wide `defaults` — because a `defaults:` block can introduce
    /// the mismatch for a field that set nothing itself.
    fn validate_field_policies(&self, table: &TableConfig, field: &FieldConfig) -> Result<()> {
        let empty_policies = ValuePolicies::default();
        let effective = field
            .policies
            .over(self.defaults.as_ref().unwrap_or(&empty_policies));

        let inapplicable = if effective.on_missing == Some(OnMissing::Null) && !field.nullable {
            Some(("on_missing: null", "the column is not nullable"))
        } else if effective.on_missing == Some(OnMissing::Empty) && field.data_type != DType::Utf8 {
            Some((
                "on_missing: empty",
                "only Utf8 has an empty value; use null or error",
            ))
        } else if effective.on_invalid == Some(OnInvalid::Null) && !field.nullable {
            Some(("on_invalid: null", "the column is not nullable"))
        } else {
            None
        };

        if let Some((policy, reason)) = inapplicable {
            return Err(ConfigIssue::InapplicablePolicy {
                table: table.name.clone(),
                field: field.name.clone(),
                policy,
                reason,
            }
            .into());
        }
        Ok(())
    }

    /// Enforces what [`Config::version`] asserts.
    ///
    /// Every check here is a *migration* check, not a correctness one: each of
    /// these configs parses perfectly well without the `version:` line. What
    /// they cannot do is parse under 1.0 semantics, which is the single thing
    /// declaring `2` claims. Rejecting is therefore the whole feature — a
    /// `version: 2` that quietly tolerated `levels` would assert nothing.
    ///
    /// Reported one at a time rather than collected. The fixes are mechanical
    /// and usually repetitive, so the first one tells you what the rest of the
    /// pass looks like, and the migrator applies them in bulk anyway.
    fn validate_declared_version(&self) -> Result<()> {
        let Some(version) = self.version else {
            return Ok(());
        };
        match version {
            1 => return Ok(()),
            2 => {}
            other => {
                return Err(ConfigIssue::UnsupportedConfigVersion { version: other }.into());
            }
        }

        for table in &self.tables {
            if table.row.is_none() {
                return Err(ConfigIssue::InferredRowInVersion2 {
                    table: table.name.clone(),
                }
                .into());
            }
            if !table.levels.is_empty() {
                return Err(ConfigIssue::LevelsInVersion2 {
                    table: table.name.clone(),
                }
                .into());
            }
            for field in &table.fields {
                if field.xml_path.is_some() {
                    return Err(ConfigIssue::FieldXmlPathInVersion2 {
                        table: table.name.clone(),
                        field: field.name.clone(),
                    }
                    .into());
                }
            }
            // A table with no ancestor has nothing to relate to, so `links` is
            // rightly absent. One nested inside another and declaring none has
            // dropped the relationship `levels` used to carry positionally —
            // silently, and only for the tables where it matters.
            if table.links.as_ref().is_none_or(Vec::is_empty)
                && let Some(enclosing) = self.enclosing_table_of(table)
            {
                return Err(ConfigIssue::NestedTableWithoutLinksInVersion2 {
                    table: table.name.clone(),
                    enclosing_table: enclosing.name.clone(),
                }
                .into());
            }
        }
        Ok(())
    }

    /// The innermost other table whose scope contains `table`, if any.
    ///
    /// "Innermost" matters for the error message only: any enclosing table
    /// makes the missing link a problem, but naming the nearest one names the
    /// table the reader is most likely to link to.
    fn enclosing_table_of(&self, table: &TableConfig) -> Option<&TableConfig> {
        let scope = table.link_scope_path();
        self.tables
            .iter()
            .filter(|candidate| {
                if candidate.name == table.name {
                    return false;
                }
                let candidate_scope = candidate.link_scope_path();
                path_is_strictly_under(&scope, &candidate_scope)
            })
            .max_by_key(|candidate| path_segments(&candidate.link_scope_path()).count())
    }

    /// Creates a `Config` struct from a YAML configuration file.
    ///
    /// This function reads a YAML file at the given path, deserializes it into a
    /// `Config` struct, and runs [`Config::validate`] on the result — a config
    /// obtained here is always structurally valid.
    ///
    /// # Arguments
    ///
    /// *   `path`: The path to the YAML configuration file.
    ///
    /// # Returns
    ///
    /// A `Result` containing:
    ///
    /// *   `Ok(Config)`: The deserialized and validated `Config` struct.
    /// *   `Err(Error)`: An `Error` value if the file cannot be opened, read, or
    ///     parsed as YAML, or if the configuration fails validation.
    ///
    /// # Errors
    ///
    /// This function may return the following errors:
    ///
    /// *   `Error::Io`: If an I/O error occurs while opening or reading the file.
    /// *   `Error::Yaml`: If there is an error parsing the YAML data.
    /// *   `Error::InvalidConfig`: If the configuration fails [`Config::validate`]
    ///     (e.g. duplicate table names, a field path not under its table path).
    /// *   `Error::UnsupportedConversion`: If a scale/offset is configured on a
    ///     non-float field.
    pub fn from_yaml_file(path: impl AsRef<Path>) -> Result<Self> {
        let file = File::open(path)?;
        let reader = BufReader::new(file);
        let config: Config = yaml_serde::from_reader(reader).map_err(Error::Yaml)?;
        config.validate()?;
        Ok(config)
    }

    /// Parses a `Config` from a YAML string, validating it.
    ///
    /// The counterpart to [`Config::from_yaml_file`] for callers that already
    /// hold the YAML — an embedded default, a config fetched over the network,
    /// a string built by a tool, or a test that would rather not touch the
    /// filesystem. Validation runs here too, so a config obtained this way is
    /// always structurally valid.
    ///
    /// ```rust
    /// use xml2arrow::Config;
    ///
    /// let config = Config::from_yaml_str(r#"
    /// tables:
    ///   - name: items
    ///     xml_path: /data
    ///     row: item
    ///     fields:
    ///       - {name: value, path: value, data_type: Int32}
    /// "#)?;
    /// assert_eq!(config.tables.len(), 1);
    /// # Ok::<(), xml2arrow::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// *   [`Error::Yaml`]: the string is not valid YAML, or does not describe
    ///     a configuration.
    /// *   [`Error::InvalidConfig`]: the configuration parsed but failed
    ///     [`Config::validate`].
    /// *   [`Error::UnsupportedConversion`]: a scale or offset on a field whose
    ///     type does not support one.
    pub fn from_yaml_str(yaml: &str) -> Result<Self> {
        let config: Config = yaml_serde::from_str(yaml).map_err(Error::Yaml)?;
        config.validate()?;
        Ok(config)
    }

    /// Writes the `Config` struct to a YAML file.
    ///
    /// This function serializes the `Config` struct to YAML format and writes it to a file at the given path.
    ///
    /// # Arguments
    ///
    /// *   `path`: The path to the output YAML file.
    ///
    /// # Returns
    ///
    /// A `Result` containing:
    ///
    /// *   `Ok(())`: If the `Config` was successfully written to the file.
    /// *   `Err(Error)`: An `Error` value if the file cannot be created or the `Config` cannot be serialized to YAML.
    ///
    /// # Errors
    ///
    /// This function may return the following errors:
    ///
    /// *   `Error::Io`: If an I/O error occurs while creating or writing to the file.
    /// *   `Error::Yaml`: If there is an error serializing the `Config` to YAML.
    pub fn to_yaml_file(&self, path: impl AsRef<Path>) -> Result<()> {
        let file = File::create(path)?;
        let writer = BufWriter::new(file);
        yaml_serde::to_writer(writer, self).map_err(Error::Yaml)
    }

    /// Checks if the configuration contains any fields that require attribute parsing.
    ///
    /// This method iterates through all tables and their fields in the configuration and returns
    /// `true` if any field's XML path contains the "@" symbol, indicating that it targets an attribute.
    ///
    /// # Returns
    ///
    /// `true` if the configuration contains at least one attribute to parse, `false` otherwise.
    #[must_use]
    pub fn requires_attribute_parsing(&self) -> bool {
        for table in &self.tables {
            for field in &table.fields {
                if field
                    .path
                    .as_deref()
                    .or(field.xml_path.as_deref())
                    .is_some_and(|p| p.contains('@'))
                {
                    return true;
                }
            }
        }
        false
    }
}

/// Accepts YAML's bare `null` as the `Null` variant rather than as "key absent".
///
/// `on_missing: null` is what anyone would write, but in YAML a bare `null` is
/// the null *literal*, so `Option<OnMissing>` deserializes it to `None` — which
/// is indistinguishable from omitting the key, and would silently leave the
/// field on its default. A policy that is quietly ignored is worse than one
/// that is rejected, so the key is read through here: present-but-null means
/// the variant, and only an absent key means unset.
fn de_policy_allowing_null<'de, D, T>(deserializer: D) -> std::result::Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de> + NullPolicy,
{
    // Serde calls this only when the key is *present* — an absent key takes the
    // `#[serde(default)]` path and never reaches here. So a value that
    // deserializes to `None` is a written-out null, which is the variant.
    Ok(Some(
        Option::<T>::deserialize(deserializer)?.unwrap_or_else(T::null_variant),
    ))
}

/// Implemented by the policies that have a `null` outcome, so
/// [`de_policy_allowing_null`] knows what a bare `null` means for each.
trait NullPolicy {
    fn null_variant() -> Self;
}

impl NullPolicy for OnMissing {
    fn null_variant() -> Self {
        OnMissing::Null
    }
}

impl NullPolicy for OnInvalid {
    fn null_variant() -> Self {
        OnInvalid::Null
    }
}

/// What to do when a field captures no value in a row.
///
/// Absent from a config, the default depends on the field: a `nullable` field
/// yields null, a non-nullable `Utf8` field yields `""`, and any other
/// non-nullable field raises an error. That asymmetry is long-standing and
/// surprising, and naming a policy is how a config opts out of it.
#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum OnMissing {
    /// Raise `MissingRequiredField`.
    Error,
    /// Append null. Requires the column to be nullable.
    Null,
    /// Append the type's empty value — `""` for `Utf8`; not valid elsewhere.
    Empty,
}

/// What to do when a field's value cannot be parsed as its declared type.
#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum OnInvalid {
    /// Raise `ParseError`, naming the field, value and reason.
    Error,
    /// Append null and continue. Requires the column to be nullable.
    Null,
}

/// What to do when a field's element carries a value more than once in one row.
#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum OnRepeat {
    /// Raise `ParseKind::DuplicateValue`. Silently keeping one of several
    /// values is how a repeated element becomes a wrong column, so this is the
    /// default.
    Error,
    /// Keep the first occurrence and ignore later ones.
    First,
    /// Keep the last occurrence, overwriting earlier ones.
    Last,
}

/// Per-field value-handling policies.
///
/// Every key is optional and **absent means current behavior**, including the
/// type-dependent quirks — setting one is opting out of a specific quirk, not
/// switching to a different engine. A [`Config::defaults`] block sets them for
/// every field at once; a field's own setting wins.
#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
#[non_exhaustive]
pub struct ValuePolicies {
    /// Whether to strip surrounding whitespace before using the value.
    ///
    /// Absent keeps the existing split: numeric and boolean fields trim,
    /// `Utf8` does not. Setting it applies uniformly, whatever the type.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trim: Option<bool>,
    /// See [`OnMissing`].
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "de_policy_allowing_null"
    )]
    pub on_missing: Option<OnMissing>,
    /// See [`OnInvalid`].
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "de_policy_allowing_null"
    )]
    pub on_invalid: Option<OnInvalid>,
    /// See [`OnRepeat`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_repeat: Option<OnRepeat>,
    /// Literal values that count as *missing* rather than as data — `"N/A"`,
    /// `"-"`, `"null"`. Compared after trimming, case-sensitively.
    ///
    /// The resulting missing value is then handled by [`ValuePolicies::on_missing`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub null_values: Option<Vec<String>>,
}

impl ValuePolicies {
    /// Returns `self`'s settings, falling back to `defaults` key by key.
    #[must_use]
    pub(crate) fn over(&self, defaults: &ValuePolicies) -> ValuePolicies {
        ValuePolicies {
            trim: self.trim.or(defaults.trim),
            on_missing: self.on_missing.or(defaults.on_missing),
            on_invalid: self.on_invalid.or(defaults.on_invalid),
            on_repeat: self.on_repeat.or(defaults.on_repeat),
            null_values: self
                .null_values
                .clone()
                .or_else(|| defaults.null_values.clone()),
        }
    }
}

/// One declared relationship between a table and an ancestor of it.
///
/// Exactly one of [`Link::parent`] and [`Link::index_of`] must be set — they
/// are different kinds of column with different guarantees, and the difference
/// matters enough that picking a winner silently would be wrong:
///
/// - **`parent`** produces a `UInt64` **join key**. Its value is the parent's
///   *global* row ordinal, never reset, so `child._<parent>_id == parent._id`
///   is a correct equi-join no matter how often container elements repeat or
///   how the stream was batched.
/// - **`index_of`** produces a `UInt32` **positional ordinal** that resets with
///   its enclosing scope. It is value-identical to the legacy `levels` columns
///   for the same path, which is its purpose: adopting `links:` need not change
///   a single value. It is *not* a join key.
///
/// Marked `#[non_exhaustive]`: further link kinds arrive as new optional keys.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq, Default)]
#[non_exhaustive]
pub struct Link {
    /// Name of the ancestor table to link to, producing a `UInt64` foreign key.
    ///
    /// The referenced table's row element must be a proper ancestor of this
    /// table's row element, checked by name at compile time — so the
    /// misalignment that `levels` could express is unrepresentable here.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent: Option<String>,
    /// Path whose occurrences are counted, producing a `UInt32` ordinal.
    ///
    /// Must name an ancestor **table's** row element. That restriction is what
    /// keeps the counter free: the ancestor table already maintains exactly
    /// this counter to serve its `levels` columns, so no per-element
    /// bookkeeping is added to the parse.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub index_of: Option<String>,
    /// Column name. Defaults to `_<parent>_id` for a `parent` link and
    /// `<element>_idx` for an `index_of` link.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

impl Link {
    /// The column name this link produces: the override when `name` is set,
    /// otherwise `_<parent>_id` for a parent link and `<element>_idx` for an
    /// ordinal.
    #[must_use]
    pub fn column_name(&self) -> Option<String> {
        if let Some(name) = &self.name {
            return Some(name.clone());
        }
        if let Some(parent) = &self.parent {
            return Some(format!("_{parent}_id"));
        }
        let index_of = self.index_of.as_deref()?;
        let element = path_segments(index_of).next_back().unwrap_or("root");
        Some(format!("{element}_idx"))
    }
}

/// Whether and how a table materializes its own key column.
///
/// A table referenced by a `parent:` link materializes one automatically, so
/// both sides of every join exist; tables nobody references pay nothing.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(untagged)]
#[non_exhaustive]
pub enum RowId {
    /// `row_id: false` suppresses the column even when something references
    /// this table; `row_id: true` forces it on.
    Enabled(bool),
    /// `row_id: my_key` renames it.
    Named(String),
}

/// Configuration for an XML table to be parsed into an Arrow record batch.
///
/// This struct defines how an XML structure should be interpreted as a table:
/// the path to the element whose configured direct children delimit rows, the
/// index columns linking nested tables (`levels`), and the configuration of
/// the fields (columns) within the table.
///
/// Marked `#[non_exhaustive]`: build one with [`TableConfig::builder`] or
/// [`TableConfig::new`], so that adding a key in a future release stays a
/// non-breaking change.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[non_exhaustive]
pub struct TableConfig {
    /// The name of the table.
    pub name: String,
    /// The XML path to the table elements. For example `/data/dataset/table`.
    pub xml_path: String,
    /// The levels of nesting for this table. This is used to create the indices for nested tables.
    /// For example if the `xml_path` is `/data/dataset/table/item/properties` the levels should
    /// be `["table", "properties"]`.
    ///
    /// Optional since 0.20: a table that declares [`TableConfig::links`] — or
    /// that needs no parent columns at all — omits the key entirely rather than
    /// writing `levels: []`. Existing configs are unaffected.
    #[serde(default)]
    pub levels: Vec<String>,
    /// A vector of `FieldConfig` structs, each defining a field (column) in the table.
    pub fields: Vec<FieldConfig>,
    /// The element whose closing tag finalizes a row — **declared** instead of
    /// inferred.
    ///
    /// Absent (the default) keeps the historical rule: a row ends whenever any
    /// configured direct child of `xml_path` closes. That rule is invisible in
    /// the config and depends on which fields happen to be configured, so a
    /// table with two distinct configured children silently yields two
    /// half-filled rows per container, and adding a field can change a table's
    /// row count. [`Config::lint`] reports that shape.
    ///
    /// Three spellings, all resolving to one trie node:
    ///
    /// - `"."` — the `xml_path` element itself: **one row per occurrence**,
    ///   which is what metadata tables almost always mean.
    /// - a relative name (`"measurement"`, `"items/item"`) — resolved against
    ///   `xml_path`.
    /// - an absolute path (`"/report/…"`) — must still resolve to `xml_path`
    ///   or a descendant of it.
    ///
    /// Declaring a row changes nothing else about the table: `levels`,
    /// absolute field paths and scoping all behave as before.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub row: Option<String>,
    /// Declared relationships to ancestor tables, replacing [`TableConfig::levels`].
    ///
    /// A table uses one or the other, never both. `levels` names *labels* and
    /// takes its values positionally from whatever ancestor tables happen to
    /// enclose it; `links` names the relationship itself, so a misalignment is
    /// a compile-time error rather than a column of plausible wrong numbers.
    ///
    /// See [`Link`] for the two kinds and their guarantees.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub links: Option<Vec<Link>>,
    /// Whether this table materializes its own key column, and under what name.
    ///
    /// Defaults to materializing `_id` (`UInt64`, non-null) exactly when some
    /// other table declares a `parent:` link to this one, so both sides of a
    /// join always exist and unreferenced tables carry no extra column.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub row_id: Option<RowId>,
}

impl TableConfig {
    /// Builds a table from the four keys every config has always had.
    ///
    /// Leaves `row`, `links` and `row_id` unset, which is the pre-0.20
    /// behavior: row boundaries stay inferred and parent columns come from
    /// `levels`. Use [`TableConfig::builder`] to set any of them.
    #[must_use]
    pub fn new(name: &str, xml_path: &str, levels: Vec<String>, fields: Vec<FieldConfig>) -> Self {
        Self {
            name: name.to_string(),
            xml_path: xml_path.to_string(),
            levels,
            fields,
            row: None,
            links: None,
            row_id: None,
        }
    }

    /// Resolves [`TableConfig::row`] to an absolute path, or `None` when this
    /// table leaves its row boundaries inferred.
    ///
    /// Resolution is pure string work and happens twice per table per
    /// `Parser::new` (validation, then registry marking), never during parsing.
    #[must_use]
    pub(crate) fn row_path(&self) -> Option<String> {
        self.row
            .as_deref()
            .map(|row| resolve_row_path(&self.xml_path, row))
    }

    /// The path whose occurrences scope this table's rows: the declared row
    /// element, or `xml_path` when boundaries are inferred.
    ///
    /// This is what link ancestry is checked against, so that "is a proper
    /// ancestor of" means the same thing whether or not a table declares `row`.
    #[must_use]
    pub(crate) fn link_scope_path(&self) -> String {
        self.row_path().unwrap_or_else(|| self.xml_path.clone())
    }

    /// Starts building a table configuration, adding levels and fields one at
    /// a time.
    ///
    /// Prefer this over [`TableConfig::new`] when the levels or fields are
    /// assembled incrementally; both are forward-compatible with future keys.
    #[must_use]
    pub fn builder(name: &str, xml_path: &str) -> TableConfigBuilder {
        TableConfigBuilder {
            name: name.to_string(),
            xml_path: xml_path.to_string(),
            levels: Vec::new(),
            fields: Vec::new(),
            row: None,
            links: None,
            row_id: None,
        }
    }
}

/// A builder for [`Config`], created by [`Config::builder`].
///
/// [`ConfigBuilder::build`] validates the assembled configuration, so a
/// `Config` produced here is guaranteed to satisfy [`Config::validate`].
#[derive(Debug, Default)]
pub struct ConfigBuilder {
    version: Option<u32>,
    tables: Vec<TableConfig>,
    parser_options: ParserOptions,
    defaults: Option<ValuePolicies>,
}

impl ConfigBuilder {
    /// Appends one table.
    #[must_use]
    pub fn table(mut self, table: TableConfig) -> Self {
        self.tables.push(table);
        self
    }

    /// Appends several tables.
    #[must_use]
    pub fn tables(mut self, tables: impl IntoIterator<Item = TableConfig>) -> Self {
        self.tables.extend(tables);
        self
    }

    /// Replaces the parser options (defaults to [`ParserOptions::default`]).
    #[must_use]
    pub fn parser_options(mut self, parser_options: ParserOptions) -> Self {
        self.parser_options = parser_options;
        self
    }

    /// Sets the value-handling policies applied to fields that set none.
    #[must_use]
    pub fn defaults(mut self, defaults: ValuePolicies) -> Self {
        self.defaults = Some(defaults);
        self
    }

    /// Declares which generation of configuration semantics this config is
    /// written against. See [`Config::version`]; passing `2` makes
    /// [`ConfigBuilder::build`] reject a config that is not fully migrated.
    #[must_use]
    pub fn version(mut self, version: u32) -> Self {
        self.version = Some(version);
        self
    }

    /// Validates and returns the configuration.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidConfig`] (or [`Error::UnsupportedConversion`])
    /// for any violation listed on [`Config::validate`].
    pub fn build(self) -> Result<Config> {
        let config = Config {
            version: self.version,
            tables: self.tables,
            parser_options: self.parser_options,
            defaults: self.defaults,
        };
        config.validate()?;
        Ok(config)
    }
}

/// A builder for [`TableConfig`], created by [`TableConfig::builder`].
#[derive(Debug)]
pub struct TableConfigBuilder {
    name: String,
    xml_path: String,
    levels: Vec<String>,
    fields: Vec<FieldConfig>,
    row: Option<String>,
    links: Option<Vec<Link>>,
    row_id: Option<RowId>,
}

impl TableConfigBuilder {
    /// Appends one parent-link level. See [`TableConfig::levels`].
    #[must_use]
    pub fn level(mut self, level: impl Into<String>) -> Self {
        self.levels.push(level.into());
        self
    }

    /// Appends several parent-link levels.
    #[must_use]
    pub fn levels(mut self, levels: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.levels.extend(levels.into_iter().map(Into::into));
        self
    }

    /// Declares the element whose closing tag finalizes a row. See
    /// [`TableConfig::row`] for the accepted spellings.
    #[must_use]
    pub fn row(mut self, row: impl Into<String>) -> Self {
        self.row = Some(row.into());
        self
    }

    /// Declares the table's links to its ancestors, replacing `levels`.
    #[must_use]
    pub fn links(mut self, links: impl IntoIterator<Item = Link>) -> Self {
        self.links = Some(links.into_iter().collect());
        self
    }

    /// Sets whether this table materializes its own key column, and its name.
    #[must_use]
    pub fn row_id(mut self, row_id: RowId) -> Self {
        self.row_id = Some(row_id);
        self
    }

    /// Appends one field (column).
    #[must_use]
    pub fn field(mut self, field: FieldConfig) -> Self {
        self.fields.push(field);
        self
    }

    /// Appends several fields (columns).
    #[must_use]
    pub fn fields(mut self, fields: impl IntoIterator<Item = FieldConfig>) -> Self {
        self.fields.extend(fields);
        self
    }

    /// Returns the assembled table configuration.
    ///
    /// Infallible by design: the checks that could fail here (unique names,
    /// paths aligned across tables) need the *whole* configuration, and run in
    /// [`ConfigBuilder::build`] / [`Config::validate`].
    #[must_use]
    pub fn build(self) -> TableConfig {
        TableConfig {
            name: self.name,
            xml_path: self.xml_path,
            levels: self.levels,
            fields: self.fields,
            row: self.row,
            links: self.links,
            row_id: self.row_id,
        }
    }
}

/// Configuration for a single field within an XML table.
///
/// This struct defines how a specific XML element or attribute should be extracted and
/// converted into an Arrow column.
///
/// Marked `#[non_exhaustive]`: build one with [`FieldConfigBuilder`], so that
/// adding a per-field key in a future release stays a non-breaking change.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[non_exhaustive]
pub struct FieldConfig {
    /// The name of the field (and the name of the resulting Arrow column).
    pub name: String,
    /// Where the value lives, relative to the table's row element or absolute.
    ///
    /// Exactly one of `path` and [`FieldConfig::xml_path`] must be set; they
    /// are two spellings of the same thing, and `path` is the one that
    /// survives — `xml_path` is removed in 1.0.
    ///
    /// Resolution follows the same rule as [`TableConfig::row`], so there is
    /// one path rule in the whole configuration:
    ///
    /// - **a leading `/` makes it absolute** — `/report/data/item/v`;
    /// - anything else is **relative to the table's row element** —
    ///   `v`, `sensor/reading` — which requires the table to declare `row:`,
    ///   since without one there is no element to be relative to.
    ///
    /// Prefix the last segment with `@` for an attribute (`@id`, `sensor/@id`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    /// The absolute XML path to the element or attribute.
    ///
    /// The original spelling, kept working for every existing configuration.
    /// [`FieldConfig::path`] replaces it: renaming the key is a mechanical
    /// change, because an absolute value means the same under either name.
    ///
    /// Removed in 1.0.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub xml_path: Option<String>,
    /// The data type of the field. This determines the Arrow data type of the resulting column.
    pub data_type: DType,
    /// Whether the field is nullable (can contain null values). Defaults to false.
    ///
    /// A value is *missing* when the element/attribute is absent, empty, or
    /// (for numeric and boolean fields) whitespace-only. When `nullable` is
    /// `false`, a missing value raises `MissingRequiredField` — with one
    /// long-standing exception: **`Utf8` fields append an empty string
    /// instead of erroring**.
    #[serde(default)]
    pub nullable: bool,
    /// Multiplier applied to `Float32`/`Float64` values:
    /// `value = (value * scale) + offset`. Rejected on any other data type.
    pub scale: Option<f64>,
    /// Constant added to `Float32`/`Float64` values *after* scaling:
    /// `value = (value * scale) + offset`. Rejected on any other data type.
    pub offset: Option<f64>,
    /// Value-handling policies for this field. Flattened, so they are written
    /// as ordinary field keys (`trim:`, `on_missing:`, …).
    #[serde(flatten)]
    pub policies: ValuePolicies,
}

impl FieldConfig {
    /// Validates that scale/offset are only used with floating point data types.
    ///
    /// # Errors
    ///
    /// Returns an error if scale or offset is set on a non-float data type.
    pub fn validate(&self) -> Result<()> {
        match self.data_type {
            DType::Float32 | DType::Float64 => Ok(()),
            _ => {
                if self.scale.is_some() {
                    return Err(Error::UnsupportedConversion {
                        conversion: ConversionKind::Scaling,
                        data_type: format!("{:?}", self.data_type),
                    });
                }
                if self.offset.is_some() {
                    return Err(Error::UnsupportedConversion {
                        conversion: ConversionKind::Offset,
                        data_type: format!("{:?}", self.data_type),
                    });
                }
                Ok(())
            }
        }
    }
}
/// A builder for configuring a `FieldConfig` struct.
///
/// This builder allows you to set the various properties of a field
/// definition within a table configuration for parsing XML data.
#[derive(Default)]
pub struct FieldConfigBuilder {
    name: String,
    path: Option<String>,
    xml_path: Option<String>,
    data_type: DType,
    nullable: bool,
    scale: Option<f64>,
    offset: Option<f64>,
    policies: ValuePolicies,
}

impl FieldConfigBuilder {
    /// Creates a new `FieldConfigBuilder` with the provided name, XML path, and data type.
    ///
    /// This is the starting point for building a `FieldConfig`.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the field.
    /// * `path` - Where the value lives: absolute (`/report/data/item/v`) or,
    ///   when the table declares `row:`, relative to that row element (`v`).
    ///   See [`FieldConfig::path`].
    /// * `data_type` - The data type of the field.
    ///
    /// # Returns
    ///
    /// A new `FieldConfigBuilder` instance with the provided properties.
    ///
    /// This populates [`FieldConfig::path`] rather than the deprecated
    /// [`FieldConfig::xml_path`]. An absolute path means the same under either
    /// key, so existing callers are unaffected.
    #[must_use]
    pub fn new(name: &str, path: &str, data_type: DType) -> Self {
        Self {
            name: name.to_string(),
            path: Some(path.to_string()),
            data_type,
            ..Default::default()
        }
    }

    /// Sets this field's value-handling policies. See [`ValuePolicies`].
    #[must_use]
    pub fn policies(mut self, policies: ValuePolicies) -> Self {
        self.policies = policies;
        self
    }

    /// Sets the `nullable` flag for the field configuration being built.
    ///
    /// This method allows you to specify whether the field can be null (missing data) in the XML document.
    ///
    /// # Arguments
    ///
    /// * `nullable` - A boolean value indicating whether the field is nullable.
    ///
    /// # Returns
    ///
    /// The builder instance itself, allowing for method chaining.
    #[must_use]
    pub fn nullable(mut self, nullable: bool) -> Self {
        self.nullable = nullable;
        self
    }

    /// Sets the `scale` factor for the field configuration being built.
    ///
    /// This method is typically used with float data types to specify the scale factor.
    ///
    /// # Arguments
    ///
    /// * `scale` - The scale factor as an f64 value.
    ///
    /// # Returns
    ///
    /// The builder instance itself, allowing for method chaining.
    #[must_use]
    pub fn scale(mut self, scale: f64) -> Self {
        self.scale = Some(scale);
        self
    }

    /// Sets the `offset` value for the field configuration being built.
    ///
    /// This method can be used with float data types to specify an offset value.
    ///
    /// # Arguments
    ///
    /// * `offset` - The offset value as an f64 value.
    ///
    /// # Returns
    ///
    /// The builder instance itself, allowing for method chaining.
    #[must_use]
    pub fn offset(mut self, offset: f64) -> Self {
        self.offset = Some(offset);
        self
    }

    /// Consumes the builder and builds the final `FieldConfig` struct.
    ///
    /// This method takes the configuration set on the builder and returns a new `FieldConfig` instance.
    ///
    /// # Returns
    ///
    /// A `FieldConfig` struct with the configured properties.
    ///
    /// # Errors
    ///
    /// Returns an error if scale or offset is set on a non-float data type.
    pub fn build(self) -> Result<FieldConfig> {
        let cfg = FieldConfig {
            name: self.name,
            path: self.path,
            xml_path: self.xml_path,
            data_type: self.data_type,
            nullable: self.nullable,
            scale: self.scale,
            offset: self.offset,
            policies: self.policies,
        };
        cfg.validate()?;
        Ok(cfg)
    }
}

/// Represents the data type of a field.
///
/// Marked `#[non_exhaustive]`: downstream matches must include a wildcard arm,
/// so that adding a data type in a future release stays a non-breaking change.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
#[non_exhaustive]
pub enum DType {
    /// Arrow `Boolean`. Accepts `true`/`false`, `1`/`0`, `yes`/`no`,
    /// `on`/`off`, `t`/`f`, `y`/`n`, case-insensitively.
    Boolean,
    /// Arrow `Float32`. Accepts decimal and scientific notation; `scale` and
    /// `offset` apply.
    Float32,
    /// Arrow `Float64`. Accepts decimal and scientific notation; `scale` and
    /// `offset` apply.
    Float64,
    /// Arrow `Int8`. Out-of-range values are an error, never a wrap.
    Int8,
    /// Arrow `UInt8`. Out-of-range values are an error, never a wrap.
    UInt8,
    /// Arrow `Int16`. Out-of-range values are an error, never a wrap.
    Int16,
    /// Arrow `UInt16`. Out-of-range values are an error, never a wrap.
    UInt16,
    /// Arrow `Int32`. Out-of-range values are an error, never a wrap.
    Int32,
    /// Arrow `UInt32`. Out-of-range values are an error, never a wrap.
    UInt32,
    /// Arrow `Int64`. Out-of-range values are an error, never a wrap.
    Int64,
    /// Arrow `UInt64`. Out-of-range values are an error, never a wrap.
    UInt64,
    /// Arrow `Utf8`, and the default. The only type taken exactly as the
    /// document spells it: no surrounding whitespace is stripped unless the
    /// field asks for it with `trim`.
    #[default]
    Utf8,
}

impl DType {
    pub(crate) fn as_arrow_type(self) -> DataType {
        match self {
            DType::Boolean => DataType::Boolean,
            DType::Float32 => DataType::Float32,
            DType::Float64 => DataType::Float64,
            DType::Utf8 => DataType::Utf8,
            DType::Int8 => DataType::Int8,
            DType::UInt8 => DataType::UInt8,
            DType::Int16 => DataType::Int16,
            DType::UInt16 => DataType::UInt16,
            DType::Int32 => DataType::Int32,
            DType::UInt32 => DataType::UInt32,
            DType::Int64 => DataType::Int64,
            DType::UInt64 => DataType::UInt64,
        }
    }
}

/// Creates a `Config` struct from a YAML string literal.
///
/// This is a convenience wrapper around `yaml_serde::from_str` followed by
/// `Config::validate`. It is intended for tests and small examples where the
/// YAML is known to be valid at the call site — invalid YAML or a failing
/// validation will `panic!`. For production code that loads YAML from user
/// input or files, use [`Config::from_yaml_file`] or `yaml_serde::from_str`
/// directly and handle the error.
///
/// The macro is purely syntactic convenience: the YAML string is parsed at
/// runtime, when the expanded expression is evaluated. Rust does not support
/// compile-time YAML deserialization without a procedural macro.
#[macro_export]
macro_rules! config_from_yaml {
    ($yaml:expr) => {{
        match yaml_serde::from_str::<$crate::config::Config>($yaml) {
            Ok(config) => {
                if let Err(e) = config.validate() {
                    panic!("Invalid configuration: {:?}", e);
                }
                config
            }
            Err(e) => panic!("Invalid YAML configuration: {}", e),
        }
    }};
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;
    use rstest::rstest;

    #[rstest]
    fn test_config_yaml_roundtrip_preserves_values(
        #[values(
            Config {
                version: None,
                parser_options: Default::default(),
                defaults: None,
                tables: vec![
                    TableConfig::new("table1", "/path/to", vec![], vec![
                        FieldConfigBuilder::new("string_field", "/path/to/string_field", DType::Utf8)
                            .nullable(true)
                            .build()
                            .unwrap(),
                        FieldConfigBuilder::new("int32_field", "/path/to/int32_field", DType::Int32)
                            .build()
                            .unwrap(),
                        FieldConfigBuilder::new("float64_field", "/path/to/float64_field", DType::Float64)
                            .nullable(true)
                            .scale(1.0e-9)
                            .offset(1.0e-3)
                            .build()
                            .unwrap(),
                        ]
                    ),
                ],
            },
            Config {
                version: None,
                parser_options: Default::default(),
                defaults: None,
                tables: vec![]
            }
        )]
        config: Config,
    ) {
        // Write to a temporary file
        let temp_file = tempfile::NamedTempFile::new().unwrap();
        let path = temp_file.path().to_path_buf();
        config.to_yaml_file(&path).unwrap();

        // Read from the same file
        let read_config = Config::from_yaml_file(&path).unwrap();

        // Check if the read config is the same as the original
        assert_eq!(config, read_config);
    }

    #[test]
    fn test_invalid_yaml_file_returns_error() {
        let invalid_yaml = "tables:\n  - name: table1\n    row_element: /path\n    fields:\n      - name: field1\n        xml_path: path\n        type: InvalidType\n        nullable: true";
        let temp_file = tempfile::NamedTempFile::new().unwrap();
        let path = temp_file.path().to_path_buf();
        std::fs::write(&path, invalid_yaml).unwrap();
        let result = Config::from_yaml_file(&path);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::Yaml(_)));
    }

    #[test]
    fn test_missing_yaml_file_returns_error() {
        let result = Config::from_yaml_file(PathBuf::from("not_existing.yaml"));
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::Io(_)));
    }

    /// The string constructor must agree with the file one on all three
    /// outcomes, not just the happy path: same config from the same YAML, and
    /// the same error for YAML that is malformed or that describes an invalid
    /// configuration. A second parser that validated differently would be a
    /// second set of rules.
    #[test]
    fn from_yaml_str_matches_from_yaml_file() {
        let yaml = r"
tables:
  - name: items
    xml_path: /data
    row: item
    fields:
      - {name: value, path: value, data_type: Int32}
";
        let temp_file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(temp_file.path(), yaml).unwrap();

        let from_file = Config::from_yaml_file(temp_file.path()).unwrap();
        let from_str = Config::from_yaml_str(yaml).unwrap();
        assert_eq!(from_file, from_str);
    }

    #[test]
    fn from_yaml_str_rejects_malformed_yaml() {
        assert!(matches!(
            Config::from_yaml_str("tables: [oh no: {"),
            Err(Error::Yaml(_))
        ));
    }

    /// Validation runs here too, so a config obtained this way is as
    /// trustworthy as one loaded from a file.
    #[test]
    fn from_yaml_str_validates() {
        let err = Config::from_yaml_str(
            r"
tables:
  - name: items
    xml_path: /data
    fields:
      - {name: value, xml_path: /elsewhere/value, data_type: Int32}
",
        )
        .unwrap_err();
        assert!(matches!(
            err,
            Error::InvalidConfig {
                reason: ConfigIssue::FieldPathNotUnderTable { .. }
            }
        ));
    }

    #[test]
    fn test_yaml_write_invalid_path_returns_error() {
        let config = Config {
            version: None,
            tables: vec![],
            parser_options: Default::default(),
            defaults: None,
        };
        let result = config.to_yaml_file(PathBuf::from("/not/existing/path/config.yaml"));
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::Io(_)));
    }

    #[test]
    fn test_field_nullable_defaults_to_false() {
        let yaml_string = r#"
            name: test_field
            xml_path: /path/to/field
            data_type: Utf8
            "#;

        let field_config: FieldConfig = yaml_serde::from_str(yaml_string).unwrap();
        assert!(!field_config.nullable);
    }

    #[test]
    fn test_parser_options_trim_text_defaults_to_false() {
        let yaml_string = r#"
            tables:
              - name: test_table
                xml_path: /root
                levels: []
                fields:
                  - name: bool_field
                    xml_path: /root/value
                    data_type: Boolean
                    nullable: true
            "#;

        let config: Config = yaml_serde::from_str(yaml_string).unwrap();
        assert!(
            !config.parser_options.trim_text,
            "trim_text should default to false"
        );
    }

    #[test]
    fn test_parser_options_strip_namespaces_defaults_to_true() {
        let yaml_string = r#"
            parser_options: {}
            tables: []
            "#;

        let config: Config = yaml_serde::from_str(yaml_string).unwrap();
        assert!(
            config.parser_options.strip_namespaces,
            "strip_namespaces should default to true"
        );
    }

    #[test]
    fn test_parser_options_strip_namespaces_set_explicitly() {
        let yaml_string = r#"
            parser_options:
              strip_namespaces: false
            tables: []
            "#;

        let config: Config = yaml_serde::from_str(yaml_string).unwrap();
        assert!(
            !config.parser_options.strip_namespaces,
            "strip_namespaces should be false when explicitly set"
        );
    }

    #[test]
    fn test_parser_options_trim_text_set_explicitly() {
        let yaml_string = r#"
            parser_options:
              trim_text: true
            tables: []
            "#;

        let config: Config = yaml_serde::from_str(yaml_string).unwrap();
        assert!(
            config.parser_options.trim_text,
            "trim_text should be true when explicitly set"
        );
    }

    #[test]
    fn test_empty_parser_options_uses_defaults() {
        let yaml_string = r#"
            parser_options: {}
            tables: []
            "#;

        let config: Config = yaml_serde::from_str(yaml_string).unwrap();
        assert!(
            !config.parser_options.trim_text,
            "trim_text should default to false when parser_options is empty"
        );
    }

    /// Users embed extra annotations (`unit`, `description`, custom metadata)
    /// in their YAML for downstream tooling. The parser must ignore those
    /// keys at every level so the same file can serve both purposes. This
    /// is currently emergent from serde's default behavior — pin it down so
    /// a future `#[serde(deny_unknown_fields)]` cannot break user configs
    /// without a failing test.
    #[test]
    fn test_unknown_yaml_fields_are_ignored_at_every_level() {
        let yaml_string = r#"
            parser_options:
              trim_text: true
              custom_option: 42
            schema_version: "1.0"
            tables:
              - name: sensors
                xml_path: /root/sensors
                levels: [sensor]
                description: "Sensor readings table"
                owner: instrumentation-team
                fields:
                  - name: temperature
                    xml_path: /root/sensors/sensor/temperature
                    data_type: Float64
                    unit: celsius
                    description: "Air temperature"
                    meta:
                      source: probe-A
                      tags: [thermal, env]
            "#;

        let config: Config = yaml_serde::from_str(yaml_string)
            .expect("unknown YAML keys must not block deserialization");
        config
            .validate()
            .expect("unknown YAML keys must not cause validation to fail");

        // Known fields still come through correctly.
        assert!(config.parser_options.trim_text);
        assert_eq!(config.tables.len(), 1);
        assert_eq!(config.tables[0].name, "sensors");
        assert_eq!(config.tables[0].fields.len(), 1);
        assert_eq!(config.tables[0].fields[0].name, "temperature");
        assert_eq!(config.tables[0].fields[0].data_type, DType::Float64);
    }

    #[test]
    fn test_requires_attr_parsing_with_attribute_fields() {
        let config: Config = yaml_serde::from_str(
            r#"
            tables:
              - name: test
                xml_path: /root
                levels: []
                fields:
                  - name: id
                    xml_path: /root/item/@id
                    data_type: Int32
            "#,
        )
        .unwrap();
        assert!(config.requires_attribute_parsing());
    }

    #[test]
    fn test_requires_attr_parsing_without_attribute_fields() {
        let config: Config = yaml_serde::from_str(
            r#"
            tables:
              - name: test
                xml_path: /root
                levels: []
                fields:
                  - name: id
                    xml_path: /root/item/id
                    data_type: Int32
            "#,
        )
        .unwrap();
        assert!(!config.requires_attribute_parsing());
    }

    #[test]
    fn test_requires_attr_parsing_with_mixed_fields() {
        let config: Config = yaml_serde::from_str(
            r#"
            tables:
              - name: test
                xml_path: /root
                levels: []
                fields:
                  - name: id
                    xml_path: /root/item/id
                    data_type: Int32
                  - name: type
                    xml_path: /root/item/@type
                    data_type: Utf8
            "#,
        )
        .unwrap();
        assert!(config.requires_attribute_parsing());
    }

    #[test]
    fn test_all_dtype_variants_convert_to_arrow() {
        use arrow::datatypes::DataType as ArrowDataType;

        assert_eq!(DType::Boolean.as_arrow_type(), ArrowDataType::Boolean);
        assert_eq!(DType::Float32.as_arrow_type(), ArrowDataType::Float32);
        assert_eq!(DType::Float64.as_arrow_type(), ArrowDataType::Float64);
        assert_eq!(DType::Utf8.as_arrow_type(), ArrowDataType::Utf8);
        assert_eq!(DType::Int8.as_arrow_type(), ArrowDataType::Int8);
        assert_eq!(DType::UInt8.as_arrow_type(), ArrowDataType::UInt8);
        assert_eq!(DType::Int16.as_arrow_type(), ArrowDataType::Int16);
        assert_eq!(DType::UInt16.as_arrow_type(), ArrowDataType::UInt16);
        assert_eq!(DType::Int32.as_arrow_type(), ArrowDataType::Int32);
        assert_eq!(DType::UInt32.as_arrow_type(), ArrowDataType::UInt32);
        assert_eq!(DType::Int64.as_arrow_type(), ArrowDataType::Int64);
        assert_eq!(DType::UInt64.as_arrow_type(), ArrowDataType::UInt64);
    }

    #[test]
    fn test_field_config_builder_chaining_works() {
        let field = FieldConfigBuilder::new("test_field", "/path/to/field", DType::Float64)
            .nullable(true)
            .scale(0.001)
            .offset(100.0)
            .build()
            .unwrap();

        assert_eq!(field.name, "test_field");
        // The builder populates `path`, the spelling that survives to 1.0.
        // An absolute value means the same under either key.
        assert_eq!(field.path.as_deref(), Some("/path/to/field"));
        assert_eq!(field.xml_path, None);
        assert_eq!(field.data_type, DType::Float64);
        assert!(field.nullable);
        assert_eq!(field.scale, Some(0.001));
        assert_eq!(field.offset, Some(100.0));
    }

    #[test]
    fn test_field_config_builder_scale_only() {
        let field = FieldConfigBuilder::new("test", "/path", DType::Float32)
            .scale(0.5)
            .build()
            .unwrap();

        assert_eq!(field.scale, Some(0.5));
        assert_eq!(field.offset, None);
    }

    #[test]
    fn test_field_config_builder_offset_only() {
        let field = FieldConfigBuilder::new("test", "/path", DType::Float64)
            .offset(5.0)
            .build()
            .unwrap();

        assert_eq!(field.scale, None);
        assert_eq!(field.offset, Some(5.0));
    }

    // --- Config validation tests ---

    #[test]
    fn test_duplicate_table_names_rejected() {
        let config = Config {
            version: None,
            parser_options: Default::default(),
            defaults: None,
            tables: vec![
                TableConfig::new("items", "/root/a", vec![], vec![]),
                TableConfig::new("items", "/root/b", vec![], vec![]),
            ],
        };
        let err = config.validate().unwrap_err();
        assert!(matches!(err, Error::InvalidConfig { .. }));
        assert!(err.to_string().contains("Duplicate table name 'items'"));
    }

    #[test]
    fn test_empty_table_name_rejected() {
        let config = Config {
            version: None,
            parser_options: Default::default(),
            defaults: None,
            tables: vec![TableConfig::new("", "/root", vec![], vec![])],
        };
        let err = config.validate().unwrap_err();
        assert!(matches!(err, Error::InvalidConfig { .. }));
        assert!(err.to_string().contains("Table name must not be empty"));
    }

    #[test]
    fn test_empty_table_xml_path_rejected() {
        let config = Config {
            version: None,
            parser_options: Default::default(),
            defaults: None,
            tables: vec![TableConfig::new("items", "", vec![], vec![])],
        };
        let err = config.validate().unwrap_err();
        assert!(matches!(err, Error::InvalidConfig { .. }));
        assert!(err.to_string().contains("empty xml_path"));
    }

    #[test]
    fn test_duplicate_field_names_in_same_table_rejected() {
        let config = Config {
            version: None,
            parser_options: Default::default(),
            defaults: None,
            tables: vec![TableConfig::new(
                "items",
                "/root",
                vec![],
                vec![
                    FieldConfigBuilder::new("value", "/root/value", DType::Utf8)
                        .build()
                        .unwrap(),
                    FieldConfigBuilder::new("value", "/root/other", DType::Int32)
                        .build()
                        .unwrap(),
                ],
            )],
        };
        let err = config.validate().unwrap_err();
        assert!(matches!(err, Error::InvalidConfig { .. }));
        assert!(err.to_string().contains("Duplicate field name 'value'"));
    }

    #[test]
    fn test_same_field_name_in_different_tables_allowed() {
        let config = Config {
            version: None,
            parser_options: Default::default(),
            defaults: None,
            tables: vec![
                TableConfig::new(
                    "table_a",
                    "/root/a",
                    vec![],
                    vec![
                        FieldConfigBuilder::new("id", "/root/a/id", DType::Int32)
                            .build()
                            .unwrap(),
                    ],
                ),
                TableConfig::new(
                    "table_b",
                    "/root/b",
                    vec![],
                    vec![
                        FieldConfigBuilder::new("id", "/root/b/id", DType::Int32)
                            .build()
                            .unwrap(),
                    ],
                ),
            ],
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_empty_field_name_rejected() {
        let config = Config {
            version: None,
            parser_options: Default::default(),
            defaults: None,
            tables: vec![TableConfig::new(
                "items",
                "/root",
                vec![],
                vec![
                    FieldConfigBuilder::new("", "/root/value", DType::Utf8)
                        .build()
                        .unwrap(),
                ],
            )],
        };
        let err = config.validate().unwrap_err();
        assert!(matches!(err, Error::InvalidConfig { .. }));
        assert!(err.to_string().contains("Field name must not be empty"));
    }

    #[test]
    fn test_empty_field_xml_path_rejected() {
        let config = Config {
            version: None,
            parser_options: Default::default(),
            defaults: None,
            tables: vec![TableConfig::new(
                "items",
                "/root",
                vec![],
                vec![
                    FieldConfigBuilder::new("value", "", DType::Utf8)
                        .build()
                        .unwrap(),
                ],
            )],
        };
        let err = config.validate().unwrap_err();
        assert!(matches!(err, Error::InvalidConfig { .. }));
        assert!(err.to_string().contains("empty xml_path"));
    }

    #[test]
    fn test_field_path_not_under_table_path_rejected() {
        let config = Config {
            version: None,
            parser_options: Default::default(),
            defaults: None,
            tables: vec![TableConfig::new(
                "items",
                "/root/items",
                vec![],
                vec![
                    FieldConfigBuilder::new("value", "/root/other/value", DType::Utf8)
                        .build()
                        .unwrap(),
                ],
            )],
        };
        let err = config.validate().unwrap_err();
        assert!(matches!(err, Error::InvalidConfig { .. }));
        assert!(err.to_string().contains("not under table"));
    }

    /// The whole reason `path_is_strictly_under` exists as its own name: it
    /// answers "is this *inside* that", where `path_is_under` answers "inside
    /// or the same". Six call sites compare one table's scope with another's,
    /// and every one of them means the strict question — a table whose path
    /// equals another's is a sibling in scope, not a child.
    #[rstest]
    // Equal paths: under, but not strictly.
    #[case("/a/b", "/a/b", true, false)]
    // Non-canonical spellings of the same equality.
    #[case("a/b", "/a/b/", true, false)]
    // Genuinely nested.
    #[case("/a/b/c", "/a/b", true, true)]
    // Everything is under the root, and strictly so unless it *is* the root.
    #[case("/a", "/", true, true)]
    #[case("/", "/", true, false)]
    // A shared string prefix is not a shared path prefix.
    #[case("/root/items_other", "/root/item", false, false)]
    // The other direction is not under at all.
    #[case("/a", "/a/b", false, false)]
    fn strictly_under_differs_from_under_exactly_on_equality(
        #[case] descendant: &str,
        #[case] ancestor: &str,
        #[case] under: bool,
        #[case] strictly: bool,
    ) {
        assert_eq!(path_is_under(descendant, ancestor), under);
        assert_eq!(path_is_strictly_under(descendant, ancestor), strictly);
    }

    #[test]
    fn test_field_path_sharing_string_prefix_but_not_segments_rejected() {
        // "/root/items_other" starts with "/root/item" as a *string* but is
        // not under it as a *path* — the check must be segment-aware.
        let config = Config {
            version: None,
            parser_options: Default::default(),
            defaults: None,
            tables: vec![TableConfig::new(
                "items",
                "/root/item",
                vec![],
                vec![
                    FieldConfigBuilder::new("value", "/root/items_other/value", DType::Utf8)
                        .build()
                        .unwrap(),
                ],
            )],
        };
        let err = config.validate().unwrap_err();
        assert!(matches!(err, Error::InvalidConfig { .. }));
        assert!(err.to_string().contains("not under table"));
    }

    #[test]
    fn test_field_path_equal_to_table_path_accepted() {
        // A field can capture the table element's own text content.
        let config = Config {
            version: None,
            parser_options: Default::default(),
            defaults: None,
            tables: vec![TableConfig::new(
                "items",
                "/root/items",
                vec![],
                vec![
                    FieldConfigBuilder::new("content", "/root/items", DType::Utf8)
                        .build()
                        .unwrap(),
                ],
            )],
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_duplicate_table_xml_path_rejected() {
        // The path registry stores one table per node; a duplicate path
        // would silently starve the earlier table of rows.
        let config = Config {
            version: None,
            parser_options: Default::default(),
            defaults: None,
            tables: vec![
                TableConfig::new("a", "/data", vec![], vec![]),
                TableConfig::new("b", "/data", vec![], vec![]),
            ],
        };
        let err = config.validate().unwrap_err();
        assert!(matches!(err, Error::InvalidConfig { .. }));
        assert!(err.to_string().contains("share the same xml_path"));
        assert!(err.to_string().contains("'a'"));
        assert!(err.to_string().contains("'b'"));
    }

    #[test]
    fn test_duplicate_table_xml_path_detected_across_spellings() {
        // "/data", "data" and "/data/" all resolve to the same registry
        // node, so they must count as duplicates regardless of spelling.
        let config = Config {
            version: None,
            parser_options: Default::default(),
            defaults: None,
            tables: vec![
                TableConfig::new("a", "/data", vec![], vec![]),
                TableConfig::new("b", "data/", vec![], vec![]),
            ],
        };
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("share the same xml_path"));
    }

    #[test]
    fn test_field_path_under_table_path_accepted() {
        let config = Config {
            version: None,
            parser_options: Default::default(),
            defaults: None,
            tables: vec![TableConfig::new(
                "items",
                "/root/items",
                vec![],
                vec![
                    FieldConfigBuilder::new("value", "/root/items/item/value", DType::Utf8)
                        .build()
                        .unwrap(),
                ],
            )],
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_root_table_allows_any_field_path() {
        let config = Config {
            version: None,
            parser_options: Default::default(),
            defaults: None,
            tables: vec![TableConfig::new(
                "root",
                "/",
                vec![],
                vec![
                    FieldConfigBuilder::new("value", "/anywhere/deep/value", DType::Utf8)
                        .build()
                        .unwrap(),
                ],
            )],
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_valid_config_passes_all_checks() {
        let config = Config {
            version: None,
            parser_options: Default::default(),
            defaults: None,
            tables: vec![
                TableConfig::new(
                    "header",
                    "/doc/header",
                    vec![],
                    vec![
                        FieldConfigBuilder::new("title", "/doc/header/title", DType::Utf8)
                            .build()
                            .unwrap(),
                    ],
                ),
                TableConfig::new(
                    "items",
                    "/doc/items",
                    vec!["item".to_string()],
                    vec![
                        FieldConfigBuilder::new("id", "/doc/items/item/@id", DType::Int32)
                            .build()
                            .unwrap(),
                        FieldConfigBuilder::new("value", "/doc/items/item/value", DType::Float64)
                            .scale(0.001)
                            .build()
                            .unwrap(),
                    ],
                ),
            ],
        };
        assert!(config.validate().is_ok());
    }

    // --- Declared row boundaries (`row:`) -------------------------------------

    /// Resolution is pure string work, so pin it directly: these three
    /// spellings are the whole surface `version: 2` will later narrow to
    /// absolute-only.
    #[rstest]
    #[case::dot("/report/header", ".", "/report/header")]
    #[case::relative_name("/report/data", "item", "/report/data/item")]
    #[case::relative_path("/report/data", "items/item", "/report/data/items/item")]
    #[case::absolute("/report/data", "/report/data/item", "/report/data/item")]
    #[case::root_table("/", "item", "/item")]
    fn row_paths_resolve(#[case] xml_path: &str, #[case] row: &str, #[case] expected: &str) {
        assert_eq!(resolve_row_path(xml_path, row), expected);
    }

    fn table_with_row(xml_path: &str, row: &str) -> TableConfig {
        TableConfig::builder("t", xml_path)
            .row(row)
            .field(
                FieldConfigBuilder::new("v", &format!("{xml_path}/item/v"), DType::Int32)
                    .build()
                    .unwrap(),
            )
            .build()
    }

    #[test]
    fn empty_row_is_rejected() {
        let config = Config::builder().table(table_with_row("/a", "  ")).build();
        assert!(matches!(
            config,
            Err(Error::InvalidConfig {
                reason: ConfigIssue::EmptyRowPath { .. }
            })
        ));
    }

    #[test]
    fn row_outside_the_table_is_rejected() {
        let config = Config::builder()
            .table(table_with_row("/a", "/b/item"))
            .build();
        assert!(matches!(
            config,
            Err(Error::InvalidConfig {
                reason: ConfigIssue::RowPathNotUnderTable { .. }
            })
        ));
    }

    /// The silent-corruption case the check exists for: rows finalize against
    /// the innermost open table, so a table between `/a` and its row element
    /// would quietly collect `t`'s rows.
    #[test]
    fn a_table_between_a_table_and_its_row_is_rejected() {
        let config = Config::builder()
            .table(table_with_row("/a", "b/item"))
            .table(TableConfig::new("inner", "/a/b", vec![], vec![]))
            .build();
        let Err(Error::InvalidConfig {
            reason: ConfigIssue::RowPathCrossesTable { nested_table, .. },
        }) = config
        else {
            panic!("expected RowPathCrossesTable, got {config:?}");
        };
        assert_eq!(nested_table, "inner");
    }

    /// A table whose path *equals* the row path is fine: `close_element` pops
    /// that scope before finalizing, so the row still lands on the declarer.
    #[test]
    fn a_table_at_the_row_element_itself_is_accepted() {
        let config = Config::builder()
            .table(table_with_row("/a", "b"))
            .table(TableConfig::new("inner", "/a/b", vec![], vec![]))
            .build();
        assert!(config.is_ok(), "got {config:?}");
    }

    /// The root frame is never popped, so a row resolving to the root could
    /// never finalize. Silently returning an empty table would be worse than
    /// useless here, because inference handles this shape correctly — adding
    /// the line would *break* a working config.
    #[test]
    fn a_row_resolving_to_the_root_table_is_rejected() {
        for row in [".", "/"] {
            let config = Config::builder()
                .table(
                    TableConfig::builder("doc", "/")
                        .row(row)
                        .field(
                            FieldConfigBuilder::new("v", "/report/v", DType::Int32)
                                .build()
                                .unwrap(),
                        )
                        .build(),
                )
                .build();
            assert!(
                matches!(
                    config,
                    Err(Error::InvalidConfig {
                        reason: ConfigIssue::RowIsRootTable { .. }
                    })
                ),
                "row: {row:?} produced {config:?}"
            );
        }
    }

    /// The same declaration one level down is fine: `<report>` does close.
    #[test]
    fn row_dot_below_the_root_is_accepted() {
        let config = Config::builder()
            .table(
                TableConfig::builder("doc", "/report")
                    .row(".")
                    .field(
                        FieldConfigBuilder::new("v", "/report/v", DType::Int32)
                            .build()
                            .unwrap(),
                    )
                    .build(),
            )
            .build();
        assert!(config.is_ok(), "got {config:?}");
    }

    #[test]
    fn row_survives_a_yaml_round_trip() {
        let config = Config::builder()
            .table(table_with_row("/a", "."))
            .build()
            .unwrap();
        let yaml = yaml_serde::to_string(&config).unwrap();
        let restored: Config = yaml_serde::from_str(&yaml).unwrap();
        assert_eq!(restored.tables[0].row.as_deref(), Some("."));
        assert_eq!(restored, config);
    }

    /// A config that predates `row:` must still deserialize, and must not grow
    /// a `row: null` key when written back out.
    #[test]
    fn configs_without_row_are_unchanged_by_it() {
        let config: Config = yaml_serde::from_str(
            r#"
            tables:
              - name: t
                xml_path: /a
                levels: []
                fields:
                  - {name: v, xml_path: /a/item/v, data_type: Int32}
            "#,
        )
        .unwrap();
        assert_eq!(config.tables[0].row, None);
        assert!(!yaml_serde::to_string(&config).unwrap().contains("row"));
    }

    // --- Relative field paths (`path:`) ---------------------------------------

    #[rstest]
    // Absolute spellings borrow rather than allocate — the common case must not
    // add per-field allocation to `Parser::new`.
    #[case::legacy_xml_path(
        "/report/data",
        Some("item"),
        None,
        Some("/report/data/item/v"),
        "/report/data/item/v"
    )]
    #[case::absolute_path(
        "/report/data",
        Some("item"),
        Some("/report/data/item/v"),
        None,
        "/report/data/item/v"
    )]
    #[case::relative_to_row("/report/data", Some("item"), Some("v"), None, "/report/data/item/v")]
    #[case::relative_nested(
        "/report/data",
        Some("item"),
        Some("sensor/@id"),
        None,
        "/report/data/item/sensor/@id"
    )]
    #[case::relative_to_row_dot(
        "/report/header",
        Some("."),
        Some("title"),
        None,
        "/report/header/title"
    )]
    #[case::relative_multi_segment_row(
        "/report",
        Some("data/item"),
        Some("v"),
        None,
        "/report/data/item/v"
    )]
    fn field_paths_resolve(
        #[case] table_path: &str,
        #[case] row: Option<&str>,
        #[case] path: Option<&str>,
        #[case] xml_path: Option<&str>,
        #[case] expected: &str,
    ) {
        let mut table = TableConfig::new("t", table_path, vec![], vec![]);
        table.row = row.map(String::from);
        let mut field = FieldConfigBuilder::new("v", "unused", DType::Int32)
            .build()
            .unwrap();
        field.path = path.map(String::from);
        field.xml_path = xml_path.map(String::from);
        assert_eq!(resolve_field_path(&table, &field).unwrap(), expected);
    }

    /// The absolute spellings must borrow: a `String` per field per
    /// `Parser::new` is exactly the kind of setup cost `parse_tiny` measures.
    #[test]
    fn absolute_field_paths_resolve_without_allocating() {
        let table = TableConfig::new("t", "/a", vec![], vec![]);
        let field = FieldConfigBuilder::new("v", "/a/item/v", DType::Int32)
            .build()
            .unwrap();
        assert!(matches!(
            resolve_field_path(&table, &field),
            Some(Cow::Borrowed(_))
        ));
    }

    fn field_config(path: Option<&str>, xml_path: Option<&str>) -> FieldConfig {
        let mut field = FieldConfigBuilder::new("v", "unused", DType::Int32)
            .build()
            .unwrap();
        field.path = path.map(String::from);
        field.xml_path = xml_path.map(String::from);
        field
    }

    fn config_with_field(row: Option<&str>, field: FieldConfig) -> Result<Config> {
        let mut table = TableConfig::new("t", "/a", vec![], vec![field]);
        table.row = row.map(String::from);
        Config::builder().table(table).build()
    }

    #[test]
    fn setting_both_path_and_xml_path_is_rejected() {
        let config = config_with_field(Some("item"), field_config(Some("v"), Some("/a/item/v")));
        assert!(matches!(
            config,
            Err(Error::InvalidConfig {
                reason: ConfigIssue::FieldPathConflict { .. }
            })
        ));
    }

    #[test]
    fn setting_neither_path_nor_xml_path_is_rejected() {
        let config = config_with_field(Some("item"), field_config(None, None));
        assert!(matches!(
            config,
            Err(Error::InvalidConfig {
                reason: ConfigIssue::FieldPathMissing { .. }
            })
        ));
    }

    /// A relative field path is relative to the *row*, so a table without one
    /// has nothing to resolve against. Rejected rather than resolved to
    /// something plausible that silently captures nothing.
    #[test]
    fn a_relative_path_without_a_declared_row_is_rejected() {
        let config = config_with_field(None, field_config(Some("v"), None));
        let Err(Error::InvalidConfig {
            reason: ConfigIssue::RelativeFieldPathWithoutRow { field, path, .. },
        }) = config
        else {
            panic!("expected RelativeFieldPathWithoutRow, got {config:?}");
        };
        assert_eq!(field, "v");
        assert_eq!(path, "v");
    }

    /// An *absolute* path needs no row, so the same table is fine.
    #[test]
    fn an_absolute_path_without_a_declared_row_is_accepted() {
        let config = config_with_field(None, field_config(Some("/a/item/v"), None));
        assert!(config.is_ok(), "got {config:?}");
    }

    #[test]
    fn field_paths_survive_a_yaml_round_trip() {
        let config = config_with_field(Some("item"), field_config(Some("v"), None)).unwrap();
        let yaml = yaml_serde::to_string(&config).unwrap();
        let restored: Config = yaml_serde::from_str(&yaml).unwrap();
        assert_eq!(restored, config);
        // Neither key is emitted when unset, so a legacy config round-trips
        // without sprouting a `path: null`.
        assert!(!yaml.contains("xml_path: null"));
        assert!(!yaml.contains("path: null"));
    }

    // --- Declared links (`links:`) --------------------------------------------

    /// Two tables where `inner`'s rows sit inside `outer`'s.
    fn linked(links: Vec<Link>, levels: Vec<String>) -> Result<Config> {
        let outer = TableConfig::builder("outer", "/a")
            .row("station")
            .field(
                FieldConfigBuilder::new("id", "id", DType::Utf8)
                    .build()
                    .unwrap(),
            )
            .build();
        let mut inner = TableConfig::builder("inner", "/a/station/ms")
            .row("m")
            .field(
                FieldConfigBuilder::new("v", "v", DType::Int32)
                    .build()
                    .unwrap(),
            )
            .links(links)
            .build();
        inner.levels = levels;
        Config::builder().table(outer).table(inner).build()
    }

    fn parent_link(parent: &str) -> Link {
        Link {
            parent: Some(parent.to_string()),
            ..Default::default()
        }
    }

    #[test]
    fn a_valid_parent_link_is_accepted() {
        assert!(linked(vec![parent_link("outer")], vec![]).is_ok());
    }

    #[test]
    fn links_and_levels_together_are_rejected() {
        let config = linked(vec![parent_link("outer")], vec!["station".to_string()]);
        assert!(matches!(
            config,
            Err(Error::InvalidConfig {
                reason: ConfigIssue::LinksAndLevels { .. }
            })
        ));
    }

    #[test]
    fn an_unknown_parent_table_is_rejected() {
        let config = linked(vec![parent_link("nope")], vec![]);
        assert!(matches!(
            config,
            Err(Error::InvalidConfig {
                reason: ConfigIssue::UnknownParentTable { .. }
            })
        ));
    }

    /// A parent must *enclose* the child. This is the whole advantage over
    /// `levels`, which took values from whatever happened to enclose the table
    /// and so could silently mis-align.
    #[test]
    fn a_parent_that_does_not_enclose_the_child_is_rejected() {
        let sibling = TableConfig::builder("sibling", "/a/other").row("x").build();
        let child = TableConfig::builder("child", "/a/station/ms")
            .row("m")
            .field(
                FieldConfigBuilder::new("v", "v", DType::Int32)
                    .build()
                    .unwrap(),
            )
            .links(vec![parent_link("sibling")])
            .build();
        let config = Config::builder().table(sibling).table(child).build();
        assert!(matches!(
            config,
            Err(Error::InvalidConfig {
                reason: ConfigIssue::ParentNotAncestor { .. }
            })
        ));
    }

    /// A table cannot be its own parent, even though its path trivially
    /// "contains" itself.
    #[test]
    fn a_self_parent_link_is_rejected() {
        let table = TableConfig::builder("t", "/a")
            .row("station")
            .field(
                FieldConfigBuilder::new("v", "v", DType::Int32)
                    .build()
                    .unwrap(),
            )
            .links(vec![parent_link("t")])
            .build();
        assert!(matches!(
            Config::builder().table(table).build(),
            Err(Error::InvalidConfig {
                reason: ConfigIssue::ParentNotAncestor { .. }
            })
        ));
    }

    #[rstest]
    #[case::both(Some("outer"), Some("/a/station"))]
    #[case::neither(None, None)]
    fn a_link_must_name_exactly_one_kind(
        #[case] parent: Option<&str>,
        #[case] index_of: Option<&str>,
    ) {
        let link = Link {
            parent: parent.map(String::from),
            index_of: index_of.map(String::from),
            name: None,
        };
        assert!(matches!(
            linked(vec![link], vec![]),
            Err(Error::InvalidConfig {
                reason: ConfigIssue::LinkKindAmbiguous { .. }
            })
        ));
    }

    /// `index_of` reads an enclosing table's existing counter, so a path that
    /// names no such table is rejected rather than silently producing zeros.
    #[test]
    fn an_index_of_path_that_is_not_an_ancestor_table_is_rejected() {
        let link = Link {
            index_of: Some("/a/station/nothing".to_string()),
            ..Default::default()
        };
        assert!(matches!(
            linked(vec![link], vec![]),
            Err(Error::InvalidConfig {
                reason: ConfigIssue::IndexOfNotAncestorTable { .. }
            })
        ));
    }

    #[test]
    fn a_link_column_colliding_with_a_field_is_rejected() {
        let link = Link {
            parent: Some("outer".to_string()),
            name: Some("v".to_string()),
            ..Default::default()
        };
        let config = linked(vec![link], vec![]);
        let Err(Error::InvalidConfig {
            reason: ConfigIssue::LinkColumnCollision { column, .. },
        }) = config
        else {
            panic!("expected LinkColumnCollision, got {config:?}");
        };
        assert_eq!(column, "v");
    }

    #[rstest]
    #[case::parent_default(Some("stations"), None, None, "_stations_id")]
    #[case::parent_named(Some("stations"), None, Some("fk"), "fk")]
    #[case::index_of_default(None, Some("/report/stations/station"), None, "station_idx")]
    #[case::index_of_named(None, Some("/report/stations/station"), Some("idx"), "idx")]
    fn link_column_names_follow_the_documented_defaults(
        #[case] parent: Option<&str>,
        #[case] index_of: Option<&str>,
        #[case] name: Option<&str>,
        #[case] expected: &str,
    ) {
        let link = Link {
            parent: parent.map(String::from),
            index_of: index_of.map(String::from),
            name: name.map(String::from),
        };
        assert_eq!(link.column_name().as_deref(), Some(expected));
    }

    #[test]
    fn links_survive_a_yaml_round_trip() {
        let config = linked(vec![parent_link("outer")], vec![]).unwrap();
        let yaml = yaml_serde::to_string(&config).unwrap();
        let restored: Config = yaml_serde::from_str(&yaml).unwrap();
        assert_eq!(restored, config);
    }

    /// `levels` became optional so a `links:` table need not write `levels: []`.
    /// Existing configs that state it are unaffected.
    #[test]
    fn levels_may_be_omitted() {
        let config: Config = yaml_serde::from_str(
            r#"
            tables:
              - name: t
                xml_path: /a
                row: item
                fields:
                  - {name: v, path: v, data_type: Int32}
            "#,
        )
        .unwrap();
        assert!(config.tables[0].levels.is_empty());
        assert!(config.validate().is_ok());
    }

    // --- Value policies ------------------------------------------------------

    /// `on_missing: null` is what anyone would write, and in YAML a bare `null`
    /// is the null *literal* — so the obvious spelling deserializes to "key
    /// absent" unless it is handled. A silently ignored policy is worse than a
    /// rejected one, hence the custom deserializer this pins.
    #[rstest]
    #[case("on_missing: null")]
    #[case("on_missing: \"null\"")]
    fn a_bare_yaml_null_selects_the_null_policy(#[case] line: &str) {
        let config: Config = yaml_serde::from_str(&format!(
            r#"
            tables:
              - name: t
                xml_path: /a
                row: item
                fields:
                  - {{name: v, path: v, data_type: Int32, nullable: true, {line}}}
            "#
        ))
        .unwrap();
        assert_eq!(
            config.tables[0].fields[0].policies.on_missing,
            Some(OnMissing::Null)
        );
    }

    /// Omitting the key still means unset, which is what keeps every existing
    /// config on its current behavior.
    #[test]
    fn an_absent_policy_key_stays_unset() {
        let config: Config = yaml_serde::from_str(
            r#"
            tables:
              - name: t
                xml_path: /a
                row: item
                fields:
                  - {name: v, path: v, data_type: Int32}
            "#,
        )
        .unwrap();
        assert_eq!(
            config.tables[0].fields[0].policies,
            ValuePolicies::default()
        );
    }

    fn config_with_policy(nullable: bool, dtype: DType, policies: ValuePolicies) -> Result<Config> {
        let field = FieldConfigBuilder::new("v", "v", dtype)
            .nullable(nullable)
            .policies(policies)
            .build()
            .unwrap();
        Config::builder()
            .table(
                TableConfig::builder("t", "/a")
                    .row("item")
                    .field(field)
                    .build(),
            )
            .build()
    }

    /// A policy with no valid outcome is rejected rather than quietly ignored.
    #[rstest]
    #[case::null_missing_on_non_nullable(false, DType::Int32, ValuePolicies { on_missing: Some(OnMissing::Null), ..Default::default() })]
    #[case::null_invalid_on_non_nullable(false, DType::Int32, ValuePolicies { on_invalid: Some(OnInvalid::Null), ..Default::default() })]
    #[case::empty_on_a_number(false, DType::Int32, ValuePolicies { on_missing: Some(OnMissing::Empty), ..Default::default() })]
    fn inapplicable_policies_are_rejected(
        #[case] nullable: bool,
        #[case] dtype: DType,
        #[case] policies: ValuePolicies,
    ) {
        let config = config_with_policy(nullable, dtype, policies);
        assert!(
            matches!(
                config,
                Err(Error::InvalidConfig {
                    reason: ConfigIssue::InapplicablePolicy { .. }
                })
            ),
            "got {config:?}"
        );
    }

    /// The same policies are fine where they can apply.
    #[rstest]
    #[case::null_on_nullable(true, DType::Int32, ValuePolicies { on_missing: Some(OnMissing::Null), ..Default::default() })]
    #[case::empty_on_utf8(false, DType::Utf8, ValuePolicies { on_missing: Some(OnMissing::Empty), ..Default::default() })]
    #[case::error_anywhere(false, DType::Int32, ValuePolicies { on_missing: Some(OnMissing::Error), ..Default::default() })]
    fn applicable_policies_are_accepted(
        #[case] nullable: bool,
        #[case] dtype: DType,
        #[case] policies: ValuePolicies,
    ) {
        assert!(config_with_policy(nullable, dtype, policies).is_ok());
    }

    /// A field's own setting wins over the `defaults:` block, key by key.
    #[test]
    fn field_policies_layer_over_config_defaults() {
        let defaults = ValuePolicies {
            trim: Some(true),
            on_repeat: Some(OnRepeat::Last),
            ..Default::default()
        };
        let field = ValuePolicies {
            trim: Some(false),
            ..Default::default()
        };
        let merged = field.over(&defaults);
        assert_eq!(merged.trim, Some(false), "the field wins");
        assert_eq!(
            merged.on_repeat,
            Some(OnRepeat::Last),
            "the default fills the gap"
        );
    }

    #[test]
    fn policies_survive_a_yaml_round_trip() {
        let config = config_with_policy(
            true,
            DType::Int32,
            ValuePolicies {
                trim: Some(true),
                on_missing: Some(OnMissing::Null),
                on_repeat: Some(OnRepeat::First),
                null_values: Some(vec!["N/A".into()]),
                ..Default::default()
            },
        )
        .unwrap();
        let yaml = yaml_serde::to_string(&config).unwrap();
        let restored: Config = yaml_serde::from_str(&yaml).unwrap();
        assert_eq!(restored, config);
        // Unset keys must not appear, so a config that sets no policy stays as
        // it was written.
        assert!(!yaml.contains("on_invalid"));
    }

    /// `version: 2` is an assertion about the config, so every one of these
    /// cases is a config that parses perfectly well *without* the line. What
    /// is being tested is that declaring it and not meaning it fails loudly.
    mod version_2 {
        use super::*;

        /// The shape a fully migrated table has: a declared row, links instead
        /// of levels, and `path` instead of `xml_path`.
        fn migrated_root() -> TableConfig {
            TableConfig::builder("stations", "/report/stations")
                .row("station")
                .field(
                    FieldConfigBuilder::new("id", "@id", DType::Utf8)
                        .build()
                        .unwrap(),
                )
                .build()
        }

        #[test]
        fn a_fully_migrated_config_is_accepted() {
            let config = Config::builder().version(2).table(migrated_root()).build();
            assert!(config.is_ok(), "{config:?}");
        }

        /// Absent stays the default it has always been, and `1` is the same
        /// thing said out loud — neither imposes any of the checks below.
        #[rstest]
        #[case(None)]
        #[case(Some(1))]
        fn version_1_and_absent_impose_nothing(#[case] version: Option<u32>) {
            let legacy = TableConfig::new(
                "t",
                "/report/stations",
                vec!["station".into()],
                vec![
                    FieldConfigBuilder::new("id", "/report/stations/station/@id", DType::Utf8)
                        .build()
                        .unwrap(),
                ],
            );
            let mut config = Config::builder().table(legacy).build().unwrap();
            config.version = version;
            assert!(config.validate().is_ok());
        }

        /// Guessing which semantics an unknown version meant is exactly wrong
        /// for the one key whose job is to pin them.
        #[test]
        fn an_unknown_version_is_rejected() {
            let mut config = Config::builder().table(migrated_root()).build().unwrap();
            config.version = Some(3);
            assert!(matches!(
                config.validate(),
                Err(Error::InvalidConfig {
                    reason: ConfigIssue::UnsupportedConfigVersion { version: 3 }
                })
            ));
        }

        /// The field path is absolute here on purpose: dropping `row:` from a
        /// table whose fields are relative fails earlier, and for a different
        /// reason (`RelativeFieldPathWithoutRow`), which would leave this test
        /// asserting nothing about `version: 2`. `path:` stays dual-form under
        /// v2 — it is `xml_path:` that is out, not absolute values.
        #[test]
        fn an_inferred_row_is_rejected() {
            let table = TableConfig::builder("stations", "/report/stations")
                .field(
                    FieldConfigBuilder::new("id", "/report/stations/station/@id", DType::Utf8)
                        .build()
                        .unwrap(),
                )
                .build();
            let config = Config::builder().version(2).table(table).build();
            assert!(matches!(
                config,
                Err(Error::InvalidConfig {
                    reason: ConfigIssue::InferredRowInVersion2 { .. }
                })
            ));
        }

        #[test]
        fn levels_are_rejected() {
            let mut table = migrated_root();
            table.levels = vec!["station".into()];
            let config = Config::builder().version(2).table(table).build();
            assert!(matches!(
                config,
                Err(Error::InvalidConfig {
                    reason: ConfigIssue::LevelsInVersion2 { .. }
                })
            ));
        }

        #[test]
        fn a_field_spelled_xml_path_is_rejected() {
            let mut table = migrated_root();
            table.fields[0].path = None;
            table.fields[0].xml_path = Some("/report/stations/station/@id".into());
            let config = Config::builder().version(2).table(table).build();
            assert!(matches!(
                config,
                Err(Error::InvalidConfig {
                    reason: ConfigIssue::FieldXmlPathInVersion2 { .. }
                })
            ));
        }

        /// The one check that is about meaning rather than spelling: a nested
        /// table with no links has dropped the relationship `levels` carried,
        /// and nothing in the output would say so.
        #[test]
        fn a_nested_table_without_links_is_rejected() {
            let child = TableConfig::builder("measurements", "/report/stations/station/ms")
                .row("m")
                .field(
                    FieldConfigBuilder::new("v", "v", DType::Int32)
                        .build()
                        .unwrap(),
                )
                .build();
            let config = Config::builder()
                .version(2)
                .table(migrated_root())
                .table(child)
                .build();
            assert!(matches!(
                config,
                Err(Error::InvalidConfig {
                    reason: ConfigIssue::NestedTableWithoutLinksInVersion2 { .. }
                })
            ));
        }

        /// A table nobody encloses has nothing to link to, so absent `links`
        /// is right rather than missing.
        #[test]
        fn a_top_level_table_needs_no_links() {
            let sibling = TableConfig::builder("other", "/report/other")
                .row("x")
                .field(
                    FieldConfigBuilder::new("v", "v", DType::Int32)
                        .build()
                        .unwrap(),
                )
                .build();
            let config = Config::builder()
                .version(2)
                .table(migrated_root())
                .table(sibling)
                .build();
            assert!(config.is_ok(), "{config:?}");
        }

        /// The key survives a YAML round trip, and stays absent when unset —
        /// a config that never opted in must not acquire a version by being
        /// written back out.
        #[test]
        fn the_version_key_round_trips_and_stays_absent_when_unset() {
            let config = Config::builder()
                .version(2)
                .table(migrated_root())
                .build()
                .unwrap();
            let yaml = yaml_serde::to_string(&config).unwrap();
            assert!(yaml.contains("version: 2"));
            assert_eq!(yaml_serde::from_str::<Config>(&yaml).unwrap(), config);

            let unversioned = Config::builder().table(migrated_root()).build().unwrap();
            assert!(
                !yaml_serde::to_string(&unversioned)
                    .unwrap()
                    .contains("version")
            );
        }
    }
}
