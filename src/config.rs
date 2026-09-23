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
//! - **There are two configuration formats, and a config is one or the
//!   other.** A config whose [`Config::version`] is `2` uses
//!   [`TableConfig::row`], [`TableConfig::links`] and [`FieldConfig::path`]. A
//!   config without it is configuration format version 1, exactly the format
//!   0.19 read, which is deprecated: rows are inferred, nested tables use
//!   [`TableConfig::levels`], and a key only version 2 defines is rejected.
//!
//! See the crate-level documentation for a worked example, and the
//! [configuration reference](https://github.com/mluttikh/xml2arrow/blob/main/docs/configuration.md)
//! for every key.

use std::{
    borrow::Cow,
    collections::{BTreeMap, HashSet},
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
    #[serde(default, skip_serializing_if = "is_false")]
    pub trim_text: bool,
    /// Optional XML paths where parsing should stop after the closing tag.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
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
    #[serde(default = "default_true", skip_serializing_if = "is_true")]
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
    #[serde(default = "default_true", skip_serializing_if = "is_true")]
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
    #[serde(default = "default_true", skip_serializing_if = "is_true")]
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
    #[serde(default, skip_serializing_if = "is_false")]
    pub allow_truncated_input: bool,
    /// Whether to fail the parse when a configured field never captured a
    /// value anywhere in the document. Defaults to `false`.
    ///
    /// The usual cause is a misspelled path, whose symptom is otherwise
    /// a silently all-null or all-empty column — the config looks fine and the
    /// data looks wrong. With this enabled, every offending field is reported
    /// at once as [`Error::UnmatchedFields`](crate::errors::Error).
    ///
    /// Off by default, in both configuration versions, because it also fails
    /// documents that are correct: one with no rows, where no field matches,
    /// and one that lacks an optional element, which is what `nullable` often
    /// means. Version 2 already fails a non-nullable field at its first row
    /// without a value, so what this adds there is the nullable field whose
    /// misspelled path would otherwise be a column of nulls. Turn it on when
    /// every field has to appear in every document and no document is empty.
    ///
    /// With the streaming entry points, the error arrives after the last
    /// batch. The batches already returned stay valid: the error says the
    /// config did not match the document, not that the rows are wrong.
    ///
    /// **Interaction with [`stop_at_paths`](Self::stop_at_paths):** stopping
    /// early guarantees that every field below the stop path captures nothing,
    /// so combining the two reports those fields as unmatched. That is
    /// accurate but rarely what the caller means, and the error says so rather
    /// than blaming the spelling. Use one or the other, or split the config so
    /// that the header-only parse configures only header fields.
    #[serde(default, skip_serializing_if = "is_false")]
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_value_bytes: Option<usize>,
}

impl ParserOptions {
    /// Whether every option is at its default, so a written config can leave
    /// the whole block out.
    fn is_default(&self) -> bool {
        *self == Self::default()
    }
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

// A config written back out (`Config::to_yaml_file`, or a converted config)
// carries only the keys that differ from their defaults, so it reads like one a
// person would write. Each skipped key deserializes to exactly the value that
// was skipped, which keeps the round trip lossless.
fn is_true(value: &bool) -> bool {
    *value
}

fn is_false(value: &bool) -> bool {
    !*value
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

/// Whether `path` has a `.` or `..` segment.
///
/// No element can match one: an XML name cannot be `.` or `..`, so such a path
/// silently matches nothing, and a table built on it has no rows or a column
/// that is never filled. A `row:` or `path:` of exactly `.` is a different
/// spelling, meaning the table or row element itself, and callers let it
/// through before asking.
pub(crate) fn has_dot_segment(path: &str) -> bool {
    path_segments(path).any(|segment| segment == "." || segment == "..")
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

/// Resolves a declared `row` against its table's element.
///
/// `"."` names the table element itself, a leading `/` means the path is
/// already absolute, and anything else is relative to that element. All three
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
    // `.` is the row element itself, as it is for `row:` against `xml_path`:
    // how a field reads the row element's own text, as in `<m>1.5</m>`.
    // Appended as a segment it would name an element that never occurs.
    if path == "." {
        return Some(Cow::Owned(row_path));
    }
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
#[derive(Debug, Clone, Serialize, PartialEq)]
#[non_exhaustive]
pub struct Config {
    /// Which configuration format this file is written in. Absent — or `1` —
    /// is configuration format version 1, exactly the format 0.19 read, and it
    /// may not set a key only version 2 defines.
    ///
    /// `2` is configuration format version 2. Validation holds a config
    /// declaring it to that format: every table declares [`TableConfig::row`],
    /// no table uses `levels`, every table names its element with `scope` and
    /// every field with `path`, rather than with `xml_path`,
    /// every key is one the configuration defines, every table nested inside
    /// another declares [`TableConfig::links`] — `links: []` when it
    /// deliberately has none — every field lies inside its table's row
    /// element, and no field lies inside a table nested within its own, where
    /// the nested table would capture every value it could receive. A config
    /// that breaks one of these is rejected at load with a message naming it,
    /// rather than parsing under semantics its author did not intend.
    ///
    /// Its value defaults are the ones 1.0 makes mandatory: `trim` on for every
    /// type, and a missing non-nullable value an error whatever the column's
    /// type — the two places where the version 1 defaults differ by type rather
    /// than by intent. Both are overridable per field.
    ///
    /// [`Config::to_version_2`] writes a version 1 config in version 2 without
    /// changing its output, including those two defaults.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<u32>,
    /// A vector of `TableConfig` structs, each defining a table to be extracted from the XML.
    pub tables: Vec<TableConfig>,
    /// Parser options.
    #[serde(default, skip_serializing_if = "ParserOptions::is_default")]
    pub parser_options: ParserOptions,
    /// Value-handling policies applied to every field that does not set its
    /// own. Version 2 only; absent leaves every field on the version 2
    /// defaults.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub defaults: Option<ValuePolicies>,
    /// Keys the document set that the configuration does not define, in the
    /// order they appear, recorded while it was deserialized. Always empty for
    /// a config built in code.
    ///
    /// Kept here, on the one struct cloned once per `Parser::new`, rather than
    /// beside each table and field: `Parser::new` clones every `TableConfig`
    /// and `FieldConfig`, and its fixed cost is the whole parse for a small
    /// document. Not written back by `to_yaml_file`, because only where a key
    /// was is recorded, not its value.
    #[serde(skip)]
    unknown_keys: Vec<UnknownKey>,
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
    /// - Each table names its element with exactly one of `scope` and
    ///   `xml_path`, and those elements are non-empty and unique
    ///   (segment-wise): the path registry stores one table per path node, so
    ///   a duplicate would silently starve the earlier table of rows.
    /// - Field names must be non-empty and unique within each table.
    /// - Each field names its location with exactly one of `path` and
    ///   `xml_path`, and the location is non-empty.
    /// - A field's location must be a descendant of (or equal to) its table's
    ///   element, compared per path segment. The root table `/` allows any
    ///   field path.
    /// - Scale/offset may only be used with Float32 and Float64 fields.
    /// - A version 1 config, which declares no `version:` or `version: 1`, sets
    ///   no key only version 2 defines: `row`, `links`, `row_id`, `path`,
    ///   `metadata`, `defaults` or a value policy.
    /// - When [`Config::version`] declares `2`, the config must be fully
    ///   migrated: every key is one the configuration defines, every table
    ///   declares `row:`, none uses `levels:`, every field uses `path:` rather
    ///   than `xml_path:`, every table names its element with `scope:`, a
    ///   table nested inside
    ///   another declares `links:`, every field lies inside its table's row
    ///   element, and no field lies inside a table nested within its own.
    ///   These are checked last, so a config that is both
    ///   broken and unmigrated reports the breakage first.
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
        if self.is_version_1() && self.defaults.is_some() {
            return Err(ConfigIssue::KeyRequiresVersion2 {
                location: "the top level".to_string(),
                key: "defaults",
            }
            .into());
        }
        let mut table_names = HashSet::with_capacity(self.tables.len());
        for (table_idx, table) in self.tables.iter().enumerate() {
            Self::validate_table_name(table, &mut table_names)?;
            if self.is_version_1()
                && let Some(key) = table.version_2_key()
            {
                return Err(ConfigIssue::KeyRequiresVersion2 {
                    location: format!("table '{}'", table.name),
                    key,
                }
                .into());
            }
            self.validate_table_path(table, table_idx)?;
            self.validate_declared_row(table)?;
            Self::validate_key_column(table)?;
            self.validate_declared_links(table, table_idx)?;
            self.validate_table_fields(table)?;
        }

        // Last, so that a config which is broken *and* not yet migrated hears
        // about the breakage first: "this path is not under its table" is a
        // bug, while "this table still uses levels" is unfinished migration.
        self.validate_declared_version()?;
        Ok(())
    }

    /// Whether this config uses configuration format version 1: it declares
    /// no `version:`, or `version: 1`.
    ///
    /// Version 1 is exactly the format 0.19 read. A config is version 1 or
    /// version 2, never a mix, so the keys version 2 added are rejected here
    /// rather than half-honored.
    pub(crate) fn is_version_1(&self) -> bool {
        matches!(self.version, None | Some(1))
    }

    /// A table must be nameable: a non-empty name no earlier table claims.
    ///
    /// `table_names` accumulates across the caller's loop, which is why it is
    /// threaded through rather than rebuilt here.
    fn validate_table_name<'c>(
        table: &'c TableConfig,
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
        Ok(())
    }

    /// A table must be addressable: exactly one of `scope` and `xml_path`,
    /// naming a non-empty element no earlier table claims.
    ///
    /// Checked after the version 1 key check, so a version 1 config that sets
    /// `scope` hears which key it may not use rather than which one it lacks.
    fn validate_table_path(&self, table: &TableConfig, table_idx: usize) -> Result<()> {
        // `scope` and `xml_path` are two spellings of one element, so exactly
        // one must be present, as for a field's `path` and `xml_path`.
        let path = match (table.scope.as_deref(), table.xml_path.as_deref()) {
            (Some(_), Some(_)) => {
                return Err(ConfigIssue::TablePathConflict {
                    table: table.name.clone(),
                }
                .into());
            }
            (None, None) => {
                return Err(ConfigIssue::TablePathMissing {
                    table: table.name.clone(),
                }
                .into());
            }
            (Some(path), None) | (None, Some(path)) => path,
        };
        if path.is_empty() {
            return Err(ConfigIssue::EmptyTablePath {
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
            if paths_equal(earlier_table.path(), path) {
                return Err(ConfigIssue::DuplicateTablePath {
                    table_a: earlier_table.name.clone(),
                    table_b: table.name.clone(),
                    path: path.to_string(),
                }
                .into());
            }
        }
        Ok(())
    }

    /// The first metadata key in the namespace the Arrow format reserves for
    /// itself, keys beginning `ARROW:`, if `metadata` uses one.
    ///
    /// Some of those keys have meaning. `ARROW:extension:name` on a column
    /// declares an extension type, which readers such as pyarrow act on, so a
    /// config that set one would change how every consumer reads the output
    /// rather than annotate it. Rejected for both configuration versions: the
    /// key is new, so no existing config can depend on it.
    ///
    /// Called from the table and field checks that already run, rather than
    /// as a pass of its own: `Parser::new` validates, and its fixed cost is the
    /// whole parse for a small document.
    ///
    /// The emptiness test comes first because nearly every table and field has
    /// no metadata, and it is a length read, where walking even an empty map
    /// builds an iterator first.
    #[inline]
    fn reserved_metadata_key(metadata: &BTreeMap<String, String>) -> Option<&String> {
        if metadata.is_empty() {
            return None;
        }
        metadata.keys().find(|key| key.starts_with("ARROW:"))
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
            // Only version 2 has `row:`, and it rejects every such path.
            if row != "." && has_dot_segment(row) {
                return Err(ConfigIssue::DotSegmentInPath {
                    location: format!("the row of table '{}'", table.name),
                    path: row.clone(),
                }
                .into());
            }
            let row_path = resolve_row_path(table.path(), row);
            if !path_is_under(&row_path, table.path()) {
                return Err(ConfigIssue::RowPathNotUnderTable {
                    table: table.name.clone(),
                    table_path: table.path().to_string(),
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
            if paths_equal(&row_path, table.path()) && path_segments(table.path()).next().is_none()
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
                if path_is_strictly_under(other.path(), table.path())
                    && path_is_strictly_under(&row_path, other.path())
                {
                    return Err(ConfigIssue::RowPathCrossesTable {
                        table: table.name.clone(),
                        row_path,
                        nested_table: other.name.clone(),
                        nested_table_path: other.path().to_string(),
                    }
                    .into());
                }
            }
        }
        Ok(())
    }

    /// Checks a table's own key column, from `row_id:`: that it has a name, and
    /// that no field has the same one. Link columns are checked against it in
    /// `validate_declared_links`, which only runs for a table with links.
    fn validate_key_column(table: &TableConfig) -> Result<()> {
        let Some(key) = table.key_column_name() else {
            return Ok(());
        };
        if key.is_empty() {
            return Err(ConfigIssue::EmptyColumnName {
                location: format!("`row_id:` of table '{}'", table.name),
            }
            .into());
        }
        if table.fields.iter().any(|field| field.name == key) {
            return Err(ConfigIssue::ColumnNameCollision {
                table: table.name.clone(),
                column: key,
            }
            .into());
        }
        Ok(())
    }

    /// Checks declared `links:` — that each names exactly one kind, refers to
    /// a table that genuinely encloses this one, and contributes a column name
    /// nothing else has claimed.
    fn validate_declared_links(&self, table: &TableConfig, table_idx: usize) -> Result<()> {
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
        // Seeded with the field names and the key column, so a link column
        // that shadowed either is caught by the same check as one that shadows
        // another link.
        let mut column_names: HashSet<String> = table
            .fields
            .iter()
            .map(|field| field.name.clone())
            .chain(table.key_column_name())
            .collect();

        for (link_idx, link) in links.iter().enumerate() {
            // Only version 2 has `links:`, and it rejects every such path.
            if let Some(index_of) = link.index_of.as_deref()
                && has_dot_segment(index_of)
            {
                return Err(ConfigIssue::DotSegmentInPath {
                    location: format!("link {} of table '{}'", link_idx + 1, table.name),
                    path: index_of.to_string(),
                }
                .into());
            }
            match (link.parent.as_deref(), link.index_of.as_deref()) {
                (Some(parent), None) => self.validate_parent_link(table, &scope, parent)?,
                (None, Some(index_of)) => {
                    self.validate_index_of_link(table, table_idx, &scope, index_of)?;
                }
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
            // column, so collisions are rejected, as is a column without a name.
            if let Some(column) = link.column_name() {
                if column.is_empty() {
                    return Err(ConfigIssue::EmptyColumnName {
                        location: format!(
                            "`name:` of link {} of table '{}'",
                            link_idx + 1,
                            table.name
                        ),
                    }
                    .into());
                }
                if !column_names.insert(column.clone()) {
                    return Err(ConfigIssue::ColumnNameCollision {
                        table: table.name.clone(),
                        column,
                    }
                    .into());
                }
            }
        }
        Ok(())
    }

    /// Checks a `parent:` link — that the named table exists, genuinely
    /// encloses this one, and declares its key column with `row_id:`.
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
        // The parent gains a key column because of this link. Stated on the
        // parent, so a table's columns follow from its own config: adding or
        // removing a child table must not change another table's schema.
        if parent.row_id.is_none() {
            return Err(ConfigIssue::ParentWithoutRowId {
                table: table.name.clone(),
                parent: parent_name.to_string(),
            }
            .into());
        }
        Ok(())
    }

    /// Checks an `index_of:` link — that the path names the row element of a
    /// table enclosing this one.
    ///
    /// Restricted to a *table's* row element, deliberately: this table's own,
    /// or an enclosing table's. Such a table already counts exactly this, so
    /// the ordinal costs nothing at parse time and is identical to the legacy
    /// `<level>` value for that table. An arbitrary path would need a per-node
    /// occurrence counter maintained on every element open.
    ///
    /// The table it names must also be able to count past 0. A table whose
    /// row element is its own scope element, as with `row: "."`, holds one row
    /// per occurrence of that element, and its count restarts at every
    /// occurrence, so a link counting it is a column of zeros.
    fn validate_index_of_link(
        &self,
        table: &TableConfig,
        table_idx: usize,
        scope: &str,
        index_of: &str,
    ) -> Result<()> {
        let Some(counted_idx) = self.table_counted_by_index_of(table_idx, scope, index_of) else {
            return Err(ConfigIssue::IndexOfNotAncestorTable {
                table: table.name.clone(),
                index_of: index_of.to_string(),
            }
            .into());
        };
        let counted = &self.tables[counted_idx];
        // Only a declared row: a table without one is rejected for that, and
        // its row element is not yet known.
        if let Some(row_path) = counted.row_path()
            && paths_equal(&row_path, counted.path())
        {
            return Err(ConfigIssue::IndexOfAlwaysZero {
                table: table.name.clone(),
                index_of: index_of.to_string(),
                counted_table: counted.name.clone(),
                scope: counted.path().to_string(),
            }
            .into());
        }
        Ok(())
    }

    /// The table an `index_of:` link on the table at `table_idx` counts, or
    /// `None` when the path names no such table.
    ///
    /// The path names a table by its row element: the table's own, whose
    /// `scope` is passed in so it is resolved once per table, or the first
    /// table in the config whose row element encloses this table's. Shared by
    /// validation and by the parser's link plans, so the table a link is
    /// checked against is the table it reads.
    pub(crate) fn table_counted_by_index_of(
        &self,
        table_idx: usize,
        scope: &str,
        index_of: &str,
    ) -> Option<usize> {
        if paths_equal(scope, index_of) {
            return Some(table_idx);
        }
        self.tables.iter().position(|t| {
            let other = t.link_scope_path();
            paths_equal(&other, index_of) && path_is_strictly_under(scope, &other)
        })
    }

    /// Checks one table's fields: nameable, addressable by exactly one of
    /// `path`/`xml_path`, resolving inside the table, carrying no metadata key
    /// Arrow reserves, and carrying no policy that cannot apply to the column
    /// it is set on. The table's own metadata is checked here too, before its
    /// fields.
    fn validate_table_fields(&self, table: &TableConfig) -> Result<()> {
        if let Some(key) = Self::reserved_metadata_key(&table.metadata) {
            return Err(ConfigIssue::ReservedMetadataKey {
                table: table.name.clone(),
                field: None,
                key: key.clone(),
            }
            .into());
        }
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
            if self.is_version_1()
                && let Some(key) = field.version_2_key()
            {
                return Err(ConfigIssue::KeyRequiresVersion2 {
                    location: format!("field '{}' of table '{}'", field.name, table.name),
                    key,
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
                    return Err(ConfigIssue::EmptyFieldPath {
                        table: table.name.clone(),
                        field: field.name.clone(),
                    }
                    .into());
                }
                _ => {}
            }

            // Only version 2 has `path:`, and it rejects every such path. The
            // keys version 1 has are checked in `dot_segment_paths`.
            if let Some(path) = field.path.as_deref()
                && path != "."
                && has_dot_segment(path)
            {
                return Err(ConfigIssue::DotSegmentInPath {
                    location: format!("field '{}' of table '{}'", field.name, table.name),
                    path: path.to_string(),
                }
                .into());
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
            if !path_is_under(&field_path, table.path()) {
                return Err(ConfigIssue::FieldPathNotUnderTable {
                    table: table.name.clone(),
                    table_path: table.path().to_string(),
                    field: field.name.clone(),
                    field_path: field_path.into_owned(),
                }
                .into());
            }

            if let Some(key) = Self::reserved_metadata_key(&field.metadata) {
                return Err(ConfigIssue::ReservedMetadataKey {
                    table: table.name.clone(),
                    field: Some(field.name.clone()),
                    key: key.clone(),
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

    /// Enforces what [`Config::version`] declares.
    ///
    /// Each of these configs would parse without the `version:` line; what
    /// they cannot do is parse as version 2, which is what the line declares.
    /// Reported one at a time: the fixes are usually repetitive, so the first
    /// one shows what the rest look like, and `Config::to_version_2` applies
    /// them in bulk.
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

        // The same list the deprecation notice reports for a version 1 config,
        // so what the notice says is left and what this rejects cannot
        // disagree. The first entry is the error: the first problem in the
        // order a reader meets them in the YAML.
        if let Some(issue) = self.version_2_issues().into_iter().next() {
            return Err(issue.into());
        }
        self.validate_fields_inside_rows()
    }

    /// Rejects a field that lies outside its table's row element, whose value
    /// would attach to whichever row ends next.
    ///
    /// Not in `version_2_issues`, because only version 2 declares rows: a
    /// version 1 config has no row element for a field to lie outside. Checked
    /// after those, so a table without `row:` has already been rejected for that, and
    /// a field a nested table captures has been rejected for the capture. Such
    /// a field never receives a value at all, and moving it to that table is
    /// the fix; telling it to move into its row as well would send the author
    /// two ways at once.
    fn validate_fields_inside_rows(&self) -> Result<()> {
        for table in &self.tables {
            // Resolved only when a field could fall outside it: a relative
            // `path` resolves inside the row element by construction, and
            // resolving allocates on every `Parser::new`.
            if !table.fields.iter().any(FieldConfig::has_absolute_location) {
                continue;
            }
            let Some(row_path) = table.row_path() else {
                continue;
            };
            for field in &table.fields {
                if let Some(field_path) = field.location_outside(&row_path) {
                    return Err(ConfigIssue::FieldOutsideRow {
                        table: table.name.clone(),
                        field: field.name.clone(),
                        field_path: field_path.to_string(),
                        row_path,
                    }
                    .into());
                }
            }
        }
        Ok(())
    }

    /// Every error version 2 raises for this configuration, in the order a
    /// reader meets them in the YAML: what the config still has to change
    /// before it can declare `version: 2`.
    ///
    /// Empty for a config that satisfies version 2, whether or not it says so.
    /// Shared by `validate`, which rejects on the first entry, and
    /// `Config::lint`, whose deprecation notice lists them.
    pub(crate) fn version_2_issues(&self) -> Vec<ConfigIssue> {
        // Unknown keys first. A misspelled key is the likeliest explanation for
        // the issues after it: `rows:` for `row:` is why a table is told to
        // declare a row. Collecting an empty iterator allocates nothing.
        let mut issues: Vec<ConfigIssue> = self
            .unknown_keys()
            .map(|(location, key)| ConfigIssue::UnknownKey {
                location,
                key: key.to_string(),
            })
            .collect();
        // Then paths with a `.` or `..` segment in the keys version 1 has. Like
        // a misspelled key, such a path explains what follows: a table on one
        // never has a row to declare.
        issues.extend(self.dot_segment_paths().map(|(location, path)| {
            ConfigIssue::DotSegmentInPath {
                location,
                path: path.to_string(),
            }
        }));
        for table in &self.tables {
            if table.xml_path.is_some() {
                issues.push(ConfigIssue::ReplacedKey {
                    table: table.name.clone(),
                    field: None,
                    key: "xml_path",
                    replacement: "scope",
                });
            }
            if table.row.is_none() {
                issues.push(ConfigIssue::MissingRow {
                    table: table.name.clone(),
                });
            }
            if !table.levels.is_empty() {
                issues.push(ConfigIssue::ReplacedKey {
                    table: table.name.clone(),
                    field: None,
                    key: "levels",
                    replacement: "links",
                });
            }
            for field in &table.fields {
                if field.xml_path.is_some() {
                    issues.push(ConfigIssue::ReplacedKey {
                        table: table.name.clone(),
                        field: Some(field.name.clone()),
                        key: "xml_path",
                        replacement: "path",
                    });
                }
                // Version 1 cannot reject this: the config loads today, and the
                // column is merely empty. Version 2 has no such config to
                // protect.
                if let Some((field_path, nested)) = self.nested_table_capturing(table, field) {
                    issues.push(ConfigIssue::FieldInsideNestedTable {
                        table: table.name.clone(),
                        field: field.name.clone(),
                        field_path,
                        nested_table: nested.name.clone(),
                    });
                }
            }
            // A table with no ancestor has nothing to relate to, so `links` is
            // rightly absent. One nested inside another must say how it
            // relates — and "it doesn't" is a valid answer, written
            // `links: []`. What is rejected is *omitting* the key, which is
            // how a migration silently drops the relationship `levels` used to
            // carry positionally.
            //
            // So only an absent key fails, never an empty list. Treating `[]`
            // as absent left no way to say "deliberately unlinked": a v1
            // table with `levels: []` could not reach version 2 without its
            // output gaining a column.
            //
            // Not reported for a table still using `levels:`: replacing those
            // with `links:` is the fix, and listing "add links" beside
            // "replace levels" would be one change counted twice.
            if table.levels.is_empty()
                && table.links.is_none()
                && let Some(enclosing) = self.enclosing_table_of(table)
            {
                issues.push(ConfigIssue::NestedTableWithoutLinks {
                    table: table.name.clone(),
                    enclosing_table: enclosing.name.clone(),
                });
            }
        }
        issues
    }

    /// The table that captures `field`'s values instead of `table`, with the
    /// field's resolved path, or `None` when `table` receives them.
    ///
    /// A value is delivered to the fields of the innermost *open* table only,
    /// and a table is open for as long as its element is. So when another
    /// table's element lies inside `table`'s and encloses the
    /// field's location, that table is open whenever a value there arrives,
    /// and the field never receives one: its column is all null, `""`, or a
    /// `MissingRequiredField` error, however the document is written. The
    /// innermost such table is the one that captures the value, so it is the
    /// one named.
    ///
    /// Compared by the table's element rather than by its row element, because
    /// that element is what opens and closes a table's scope. An attribute of the nested
    /// table's own element is inside it too: the table opens before its
    /// element's attributes are read.
    ///
    /// Resolves the field's path only when `table` has a table nested inside
    /// it, so a config with no nesting pays for nothing but the table scan.
    pub(crate) fn nested_table_capturing(
        &self,
        table: &TableConfig,
        field: &FieldConfig,
    ) -> Option<(String, &TableConfig)> {
        let mut nested = self
            .tables
            .iter()
            .filter(|other| path_is_strictly_under(other.path(), table.path()))
            .peekable();
        nested.peek()?;
        let field_path = resolve_field_path(table, field)?;
        // As in `location_outside`: a path no element can match is reported
        // for that alone.
        if has_dot_segment(&field_path) {
            return None;
        }
        nested
            .filter(|other| path_is_under(&field_path, other.path()))
            .max_by_key(|other| path_segments(other.path()).count())
            .map(|other| (field_path.into_owned(), other))
    }

    /// The innermost other table whose scope contains `table`, if any.
    ///
    /// "Innermost" matters for the error message only: any enclosing table
    /// makes the missing link a problem, but naming the nearest one names the
    /// table the reader is most likely to link to.
    pub(crate) fn enclosing_table_of(&self, table: &TableConfig) -> Option<&TableConfig> {
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
    /// version: 2
    /// tables:
    ///   - name: items
    ///     scope: /data
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

// --- Deserialization ---------------------------------------------------------
//
// `Config` and `FieldConfig` are deserialized through explicit twins rather than
// straight from their own derives, so that every key the document sets but the
// configuration does not define is recorded instead of silently dropped.
//
// `serde_ignored` reports each key serde skips, with its path, without changing
// how anything else deserializes: values still reach their fields straight from
// the YAML deserializer, so metadata keeps its spelling and errors keep their line
// and column. It cannot see into a `#[serde(flatten)]` struct, whose leftover keys
// serde discards internally, which is why a field's policy keys are spelled out
// in `FieldConfigRepr` instead of flattened in as `FieldConfig` serializes them.
//
// Both twins convert with a struct literal naming every field, so a key added to
// `Config`, `FieldConfig` or `ValuePolicies` without its twin fails to compile.

/// A key the document set that the configuration does not define.
#[derive(Debug, Clone, PartialEq, Eq)]
struct UnknownKey {
    /// Mapping keys and sequence indices from the top of the document down to
    /// the unknown key, which is last.
    path: Vec<KeySegment>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum KeySegment {
    Key(String),
    Index(usize),
}

impl UnknownKey {
    fn from_path(path: &serde_ignored::Path<'_>) -> Self {
        fn collect(path: &serde_ignored::Path<'_>, segments: &mut Vec<KeySegment>) {
            use serde_ignored::Path;
            match path {
                Path::Root => {}
                Path::Seq { parent, index } => {
                    collect(parent, segments);
                    segments.push(KeySegment::Index(*index));
                }
                Path::Map { parent, key } => {
                    collect(parent, segments);
                    segments.push(KeySegment::Key(key.clone()));
                }
                // An `Option` or a newtype is a level in serde's path but not in
                // the document's.
                Path::Some { parent }
                | Path::NewtypeStruct { parent }
                | Path::NewtypeVariant { parent } => collect(parent, segments),
            }
        }
        let mut segments = Vec::new();
        collect(path, &mut segments);
        Self { path: segments }
    }
}

impl Config {
    /// Every key the document set that this configuration does not define, as
    /// `(where, key)`, in document order.
    ///
    /// `where` names what a reader would look for in the YAML: `table
    /// 'readings'`, `field 'value' of table 'readings'`, `link 1 of table
    /// 'readings'`, `parser_options`, `defaults`, or `the top level`.
    pub(crate) fn unknown_keys(&self) -> impl Iterator<Item = (String, &str)> + '_ {
        self.unknown_keys.iter().filter_map(|unknown| {
            let (KeySegment::Key(key), parents) = unknown.path.split_last()? else {
                return None;
            };
            Some((self.describe_key_location(parents), key.as_str()))
        })
    }

    /// Every path with a `.` or `..` segment in the keys version 1 has, as
    /// `(where, path)`, in document order: `stop_at_paths`, each table's
    /// `xml_path`, and each field's `xml_path`.
    ///
    /// Only these, because a version 1 config may use them today and must keep
    /// loading. The keys only version 2 has, `row:`, `path:` and `index_of:`,
    /// are checked where they are validated.
    pub(crate) fn dot_segment_paths(&self) -> impl Iterator<Item = (String, &str)> + '_ {
        let stop_paths = self
            .parser_options
            .stop_at_paths
            .iter()
            .filter(|path| has_dot_segment(path))
            .map(|path| ("`parser_options.stop_at_paths`".to_string(), path.as_str()));
        let tables = self.tables.iter().flat_map(|table| {
            let table_path = has_dot_segment(table.path()).then(|| {
                // Named by the key the table actually sets, so the message
                // points at the line the reader has to edit.
                let key = if table.scope.is_some() {
                    "scope"
                } else {
                    "xml_path"
                };
                (format!("the {key} of table '{}'", table.name), table.path())
            });
            let fields = table.fields.iter().filter_map(move |field| {
                let xml_path = field.xml_path.as_deref()?;
                has_dot_segment(xml_path).then(|| {
                    (
                        format!("field '{}' of table '{}'", field.name, table.name),
                        xml_path,
                    )
                })
            });
            table_path.into_iter().chain(fields)
        });
        stop_paths.chain(tables)
    }

    fn describe_key_location(&self, parents: &[KeySegment]) -> String {
        use KeySegment::{Index, Key};
        let table = |index: usize| match self.tables.get(index) {
            Some(table) => format!("table '{}'", table.name),
            None => format!("table {}", index + 1),
        };
        match parents {
            [] => "the top level".to_string(),
            [Key(section)] if section == "parser_options" || section == "defaults" => {
                format!("`{section}`")
            }
            [Key(tables), Index(t)] if tables == "tables" => table(*t),
            [Key(tables), Index(t), Key(links), Index(l)]
                if tables == "tables" && links == "links" =>
            {
                format!("link {} of {}", l + 1, table(*t))
            }
            [Key(tables), Index(t), Key(fields), Index(f)]
                if tables == "tables" && fields == "fields" =>
            {
                match self.tables.get(*t).and_then(|table| table.fields.get(*f)) {
                    Some(field) => format!("field '{}' of {}", field.name, table(*t)),
                    None => format!("field {} of {}", f + 1, table(*t)),
                }
            }
            // Nowhere else accepts a mapping with fixed keys today; a path from
            // somewhere new is shown as written rather than guessed at.
            other => other
                .iter()
                .map(|segment| match segment {
                    Key(key) => key.clone(),
                    Index(index) => index.to_string(),
                })
                .collect::<Vec<_>>()
                .join("."),
        }
    }
}

/// `Config` as the document spells it.
#[derive(Deserialize)]
#[serde(rename = "Config")]
struct ConfigRepr {
    #[serde(default)]
    version: Option<u32>,
    tables: Vec<TableConfig>,
    #[serde(default)]
    parser_options: ParserOptions,
    #[serde(default)]
    defaults: Option<ValuePolicies>,
}

impl<'de> Deserialize<'de> for Config {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let mut unknown_keys = Vec::new();
        let repr: ConfigRepr = serde_ignored::deserialize(deserializer, |path| {
            unknown_keys.push(UnknownKey::from_path(&path));
        })?;
        Ok(Config {
            version: repr.version,
            tables: repr.tables,
            parser_options: repr.parser_options,
            defaults: repr.defaults,
            unknown_keys,
        })
    }
}

/// `FieldConfig` as the document spells it: the policy keys side by side with
/// the others, where `serde_ignored` can see a misspelled one.
///
/// The attributes match `FieldConfig`'s and `ValuePolicies`', including the
/// bare-`null` handling of `on_missing` and `on_invalid`.
#[derive(Deserialize)]
#[serde(rename = "FieldConfig")]
struct FieldConfigRepr {
    name: String,
    #[serde(default)]
    path: Option<String>,
    #[serde(default)]
    xml_path: Option<String>,
    data_type: DType,
    #[serde(default)]
    nullable: bool,
    #[serde(default)]
    scale: Option<f64>,
    #[serde(default)]
    offset: Option<f64>,
    #[serde(default)]
    trim: Option<bool>,
    #[serde(default, deserialize_with = "de_policy_allowing_null")]
    on_missing: Option<OnMissing>,
    #[serde(default, deserialize_with = "de_policy_allowing_null")]
    on_invalid: Option<OnInvalid>,
    #[serde(default)]
    on_repeat: Option<OnRepeat>,
    #[serde(default)]
    null_values: Option<Vec<String>>,
    #[serde(default)]
    metadata: BTreeMap<String, String>,
}

impl From<FieldConfigRepr> for FieldConfig {
    fn from(repr: FieldConfigRepr) -> Self {
        FieldConfig {
            name: repr.name,
            path: repr.path,
            xml_path: repr.xml_path,
            data_type: repr.data_type,
            nullable: repr.nullable,
            scale: repr.scale,
            offset: repr.offset,
            policies: ValuePolicies {
                trim: repr.trim,
                on_missing: repr.on_missing,
                on_invalid: repr.on_invalid,
                on_repeat: repr.on_repeat,
                null_values: repr.null_values,
            },
            metadata: repr.metadata,
        }
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

/// Per-field value-handling policies, in configuration format version 2.
///
/// Every key is optional, and absent means the version 2 default: values are
/// trimmed, a missing non-nullable value is an error, as is an unparseable or
/// repeated one. A [`Config::defaults`] block sets them for every field at
/// once; a field's own setting wins.
#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
#[non_exhaustive]
pub struct ValuePolicies {
    /// Whether to strip surrounding whitespace before using the value, whatever
    /// the type. Absent trims.
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
    /// For `Utf8`, trimming alone never makes a value missing, because `""` is
    /// a string; add `""` here to treat blank text as missing.
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

/// One declared link column: how a table's rows relate to the rows that
/// contain them, or where each row sits among its siblings.
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
///   its enclosing scope. It holds the same values as a `levels` column that
///   counts the same table's rows. It is *not* a join key.
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
    /// Must name a **table's** row element: an enclosing table's, for the
    /// position of the row that contains this one, or this table's own, for
    /// each row's position among its siblings. That restriction is what keeps
    /// the counter free: the table already maintains exactly this counter to
    /// serve its `levels` columns, so no per-element bookkeeping is added to
    /// the parse.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub index_of: Option<String>,
    /// Column name. Defaults to `_<parent>_id` for a `parent` link and
    /// `<element>_idx` for an `index_of` link.
    ///
    /// Set it whenever the derived name is not what you want downstream. The
    /// `parent` default interpolates the referenced table's `name` exactly as
    /// written, so a table whose name is not a plain identifier produces a
    /// column that is not one either — legal as an Arrow field name, and
    /// handled by anything that quotes identifiers, but not by everything.
    /// [`TableConfig::row_id`] does the same for the key side.
    ///
    /// Collisions are rejected rather than resolved: if two links, or a link
    /// and a field, would produce the same column, `Config::validate` fails
    /// and names this key as the fix.
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
/// A table that another table names with a `parent:` link must declare one,
/// so that its columns follow from its own config. Any other table may.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(untagged)]
#[non_exhaustive]
pub enum RowId {
    /// `row_id: true` adds the column, named `_id`; `row_id: false` leaves it
    /// out, including on a table that a `parent:` link names, whose link
    /// column is still written.
    Enabled(bool),
    /// `row_id: my_key` adds the column under that name.
    Named(String),
}

/// Configuration for an XML table to be parsed into an Arrow record batch.
///
/// This struct defines how an XML structure should be interpreted as a table:
/// the path to the element whose configured direct children delimit rows, the
/// index columns linking nested tables (`levels`), and the configuration of
/// the fields (columns) within the table.
///
/// Marked `#[non_exhaustive]`: build one with [`TableConfig::builder`], so
/// that adding a key in a future release stays a non-breaking change.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[non_exhaustive]
pub struct TableConfig {
    /// The name of the table.
    pub name: String,
    /// The absolute path of the element that scopes this table, and the
    /// version 2 spelling of [`TableConfig::xml_path`].
    ///
    /// The element the path names does three things, which is why a table
    /// states it rather than deriving it from [`TableConfig::row`]:
    ///
    /// - **It bounds the rows.** [`TableConfig::row`] resolves against it, and
    ///   a row lies inside it.
    /// - **It resets the counters.** An [`Link::index_of`] position restarts at
    ///   every occurrence of this element, which is what makes a position
    ///   local to a container rather than to the document.
    /// - **It owns the values inside it.** A value is delivered to the
    ///   innermost open table, so two tables may not name the same element.
    ///
    /// A table one level up therefore counts across a wider span, and a table
    /// at its own row element counts nothing but itself.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope: Option<String>,
    /// The version 1 spelling of [`TableConfig::scope`]. For example
    /// `/data/dataset/table`.
    ///
    /// Version 2 replaces it with `scope`: renaming the key is a mechanical
    /// change, because the value means the same under either name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub xml_path: Option<String>,
    /// The element whose closing tag finalizes a row — **declared** instead of
    /// inferred. Version 2 only, where every table declares one.
    ///
    /// Version 1 has no `row:` and keeps the historical rule: a row ends
    /// whenever any configured direct child of `xml_path` closes. That rule is
    /// invisible in the config and depends on which fields happen to be
    /// configured, so a table with two distinct configured children silently
    /// yields two half-filled rows per container, and adding a field can change
    /// a table's row count. [`Config::lint`] reports that shape.
    ///
    /// Three spellings, all resolving to one trie node:
    ///
    /// - `"."` — the `xml_path` element itself: **one row per occurrence**,
    ///   which is what metadata tables almost always mean.
    /// - a relative name (`"measurement"`, `"items/item"`) — resolved against
    ///   `xml_path`.
    /// - an absolute path (`"/report/…"`) — must still resolve to `xml_path`
    ///   or a descendant of it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub row: Option<String>,
    /// Declared relationships to ancestor tables, replacing [`TableConfig::levels`].
    /// Version 2 only.
    ///
    /// A table uses one or the other, never both. `levels` names *labels* and
    /// takes its values positionally from whatever ancestor tables happen to
    /// enclose it; `links` names the relationship itself, so a misalignment is
    /// a compile-time error rather than a column of plausible wrong numbers.
    ///
    /// See [`Link`] for the two kinds and their guarantees.
    ///
    /// An empty list, `links: []`, is a declaration in its own right: the
    /// table deliberately has no link to any enclosing table, and produces no
    /// link column. It is distinct from omitting the key, which under
    /// [`Config::version`] `2` is an error for a nested table — the difference
    /// between "I decided there is no relationship" and "I forgot one".
    /// `links: []` is also the value-identical form of a v1 `levels: []`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub links: Option<Vec<Link>>,
    /// Whether this table materializes its own key column, and under what name.
    /// Version 2 only.
    ///
    /// The column is `UInt64` and non-null, and holds the row's ordinal across
    /// the whole parse, which is what a `parent:` link column in another table
    /// holds. Absent means no column. A table that another table names with a
    /// `parent:` link must declare the key, so that adding or removing a child
    /// table never changes this table's schema: `row_id: false` is how it says
    /// it wants the link without the column.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub row_id: Option<RowId>,
    /// The levels of nesting for this table. This is used to create the indices for nested tables.
    /// For example if the `xml_path` is `/data/dataset/table/item/properties` the levels should
    /// be `["table", "properties"]`.
    ///
    /// Version 1 only; version 2 replaces it with [`TableConfig::links`].
    /// Optional since 0.20: a table that needs no index columns may omit the key
    /// rather than write `levels: []`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub levels: Vec<String>,
    /// Key-value pairs copied into this table's Arrow schema metadata, and so
    /// into every batch it produces and, for example, the Parquet file written
    /// from them. The parser does nothing else with them. Version 2 only.
    ///
    /// Keys and values are strings, taken exactly as the YAML spells them:
    /// `version: 1.50` is `"1.50"`, not a number. Keys beginning `ARROW:` are
    /// rejected, because the Arrow format reserves them. See
    /// [`FieldConfig::metadata`] for a column's own.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, String>,
    /// A vector of `FieldConfig` structs, each defining a field (column) in the table.
    ///
    /// Last, so that a written config states where a table's rows are before
    /// the list of columns, however long that list is.
    pub fields: Vec<FieldConfig>,
}

impl TableConfig {
    /// The first key this table sets that only configuration format version 2
    /// defines, if any. A version 1 config is rejected for setting one.
    ///
    /// Checked in the validation pass that already visits every table, rather
    /// than in one of its own: `Parser::new` validates, and its fixed cost is
    /// the whole parse for a small document.
    pub(crate) fn version_2_key(&self) -> Option<&'static str> {
        if self.scope.is_some() {
            Some("scope")
        } else if self.row.is_some() {
            Some("row")
        } else if self.links.is_some() {
            Some("links")
        } else if self.row_id.is_some() {
            Some("row_id")
        } else if !self.metadata.is_empty() {
            Some("metadata")
        } else {
            None
        }
    }

    /// Builds a table from the four keys every config had before 0.20: a
    /// version 1 table, whose rows are inferred and whose parent columns come
    /// from `levels`.
    ///
    /// Deprecated with configuration format version 1, which 1.0 removes along
    /// with `levels`, so this signature cannot survive it. Use
    /// [`TableConfig::builder`], and relate nested tables with
    /// [`TableConfigBuilder::links`] in a version 2 config. Until then, a
    /// version 1 table built with the builder sets [`TableConfig::levels`]
    /// directly.
    #[deprecated(
        since = "0.20.0",
        note = "configuration format version 1 is deprecated; use `TableConfig::builder`, and `links` in a version 2 config instead of `levels`"
    )]
    #[must_use]
    pub fn new(name: &str, xml_path: &str, levels: Vec<String>, fields: Vec<FieldConfig>) -> Self {
        Self {
            name: name.to_string(),
            scope: None,
            xml_path: Some(xml_path.to_string()),
            levels,
            fields,
            row: None,
            links: None,
            row_id: None,
            metadata: BTreeMap::new(),
        }
    }

    /// The name of this table's own key column, or `None` when it has none.
    ///
    /// Decided by `row_id:` alone: `true` is `_id`, a string names it, and
    /// `false` or an absent key means no column.
    #[must_use]
    pub(crate) fn key_column_name(&self) -> Option<String> {
        match self.row_id.as_ref()? {
            RowId::Enabled(true) => Some("_id".to_string()),
            RowId::Enabled(false) => None,
            RowId::Named(name) => Some(name.clone()),
        }
    }

    /// The element that scopes this table, whichever key spells it.
    ///
    /// Empty only for a config that sets neither, which validation rejects.
    #[must_use]
    pub(crate) fn path(&self) -> &str {
        self.scope
            .as_deref()
            .or(self.xml_path.as_deref())
            .unwrap_or_default()
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
            .map(|row| resolve_row_path(self.path(), row))
    }

    /// The path whose occurrences scope this table's rows: the declared row
    /// element, or `xml_path` when boundaries are inferred.
    ///
    /// This is what link ancestry is checked against, so that "is a proper
    /// ancestor of" means the same thing whether or not a table declares `row`.
    #[must_use]
    pub(crate) fn link_scope_path(&self) -> String {
        self.row_path().unwrap_or_else(|| self.path().to_string())
    }

    /// Starts building a table configuration, adding keys and fields one at
    /// a time.
    ///
    /// The element is held as `xml_path`, the version 1 spelling;
    /// [`ConfigBuilder::build`] moves it to `scope` for a version 2 config.
    #[must_use]
    pub fn builder(name: &str, path: &str) -> TableConfigBuilder {
        TableConfigBuilder {
            name: name.to_string(),
            xml_path: path.to_string(),
            fields: Vec::new(),
            row: None,
            links: None,
            row_id: None,
            metadata: BTreeMap::new(),
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
    ///
    /// [`TableConfig::builder`] and [`FieldConfigBuilder::new`] hold an element
    /// as `xml_path`, the version 1 spelling. In a version 2 config, `build`
    /// moves a table's to `scope` and a field's to `path`, the spellings that
    /// format has, so one builder serves both and a config written back out
    /// spells its elements as its version does.
    pub fn build(mut self) -> Result<Config> {
        if self.version == Some(2) {
            for table in &mut self.tables {
                if table.scope.is_none() {
                    table.scope = table.xml_path.take();
                }
                for field in &mut table.fields {
                    if field.path.is_none() {
                        field.path = field.xml_path.take();
                    }
                }
            }
        }
        let config = Config {
            version: self.version,
            tables: self.tables,
            parser_options: self.parser_options,
            defaults: self.defaults,
            unknown_keys: Vec::new(),
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
    fields: Vec<FieldConfig>,
    row: Option<String>,
    links: Option<Vec<Link>>,
    row_id: Option<RowId>,
    metadata: BTreeMap<String, String>,
}

impl TableConfigBuilder {
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

    /// Adds entries to the table's schema metadata. See
    /// [`TableConfig::metadata`].
    #[must_use]
    pub fn metadata(
        mut self,
        entries: impl IntoIterator<Item = (impl Into<String>, impl Into<String>)>,
    ) -> Self {
        self.metadata
            .extend(entries.into_iter().map(|(k, v)| (k.into(), v.into())));
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
            scope: None,
            xml_path: Some(self.xml_path),
            levels: Vec::new(),
            fields: self.fields,
            row: self.row,
            links: self.links,
            row_id: self.row_id,
            metadata: self.metadata,
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
#[serde(from = "FieldConfigRepr")]
#[non_exhaustive]
pub struct FieldConfig {
    /// The name of the field (and the name of the resulting Arrow column).
    pub name: String,
    /// Where the value lives, relative to the table's row element or absolute.
    ///
    /// The version 2 spelling of [`FieldConfig::xml_path`]: a version 1 config
    /// sets `xml_path`, and a version 2 config sets `path`.
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
    /// The version 1 spelling. Version 2 replaces it with [`FieldConfig::path`]:
    /// renaming the key is a mechanical change, because an absolute value means
    /// the same under either name.
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
    #[serde(default, skip_serializing_if = "is_false")]
    pub nullable: bool,
    /// Multiplier applied to `Float32`/`Float64` values:
    /// `value = (value * scale) + offset`. Rejected on any other data type.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scale: Option<f64>,
    /// Constant added to `Float32`/`Float64` values *after* scaling:
    /// `value = (value * scale) + offset`. Rejected on any other data type.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub offset: Option<f64>,
    /// Value-handling policies for this field. Flattened, so they are written
    /// as ordinary field keys (`trim:`, `on_missing:`, …). Version 2 only.
    #[serde(flatten)]
    pub policies: ValuePolicies,
    /// Key-value pairs copied into this column's Arrow field metadata, such as
    /// a unit or a description. The parser does nothing else with them. Version
    /// 2 only.
    ///
    /// Strings, taken exactly as the YAML spells them, and keys beginning
    /// `ARROW:` are rejected, as for [`TableConfig::metadata`]. Arrow reserves
    /// that prefix, and `ARROW:extension:name` on a column would change how
    /// readers such as pyarrow interpret it.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, String>,
}

impl FieldConfig {
    /// The first key this field sets that only configuration format version 2
    /// defines, if any: `path`, a value policy, or `metadata`. A version 1
    /// config is rejected for setting one.
    pub(crate) fn version_2_key(&self) -> Option<&'static str> {
        let policies = &self.policies;
        if self.path.is_some() {
            Some("path")
        } else if policies.trim.is_some() {
            Some("trim")
        } else if policies.on_missing.is_some() {
            Some("on_missing")
        } else if policies.on_invalid.is_some() {
            Some("on_invalid")
        } else if policies.on_repeat.is_some() {
            Some("on_repeat")
        } else if policies.null_values.is_some() {
            Some("null_values")
        } else if !self.metadata.is_empty() {
            Some("metadata")
        } else {
            None
        }
    }

    /// The field's location when it lies outside `row_path`, the resolved row
    /// element of its table; `None` when it lies inside.
    ///
    /// Only an absolute location can fall outside: `xml_path` always is one,
    /// and so is a `path` with a leading slash, while a relative `path`
    /// resolves inside the row element by construction. Checking just those
    /// needs no resolution and so no allocation, which matters because every
    /// `Parser::new` of a version 2 config asks.
    pub(crate) fn location_outside(&self, row_path: &str) -> Option<&str> {
        let location = match (self.xml_path.as_deref(), self.path.as_deref()) {
            (Some(xml_path), _) => xml_path,
            (None, Some(path)) if path.starts_with('/') => path,
            _ => return None,
        };
        // A location no element can match is reported for that alone: where
        // it sits relative to the row does not matter until it is fixed.
        (!path_is_under(location, row_path) && !has_dot_segment(location)).then_some(location)
    }

    /// Whether the field names its location absolutely, and so could lie
    /// outside its table's row element. See [`FieldConfig::location_outside`].
    pub(crate) fn has_absolute_location(&self) -> bool {
        self.xml_path.is_some() || self.path.as_deref().is_some_and(|p| p.starts_with('/'))
    }

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
    metadata: BTreeMap<String, String>,
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
    ///   in a version 2 config, relative to the table's row element (`v`).
    /// * `data_type` - The data type of the field.
    ///
    /// # Returns
    ///
    /// A new `FieldConfigBuilder` instance with the provided properties.
    ///
    /// The field holds the location as [`FieldConfig::xml_path`], as it did in
    /// 0.19, which is the spelling a version 1 config uses. Building a version
    /// 2 config with [`ConfigBuilder::build`] moves it to
    /// [`FieldConfig::path`], the spelling version 2 uses, so the same builder
    /// serves both formats.
    #[must_use]
    pub fn new(name: &str, path: &str, data_type: DType) -> Self {
        Self {
            name: name.to_string(),
            xml_path: Some(path.to_string()),
            data_type,
            ..Default::default()
        }
    }

    /// Adds entries to the column's field metadata. See
    /// [`FieldConfig::metadata`].
    #[must_use]
    pub fn metadata(
        mut self,
        entries: impl IntoIterator<Item = (impl Into<String>, impl Into<String>)>,
    ) -> Self {
        self.metadata
            .extend(entries.into_iter().map(|(k, v)| (k.into(), v.into())));
        self
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
            metadata: self.metadata,
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
// Version 1 tables are built with `TableConfig::new`, deprecated with that
// format; the tests of version 1 behavior keep using it until both go.
#[allow(deprecated)]
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
                unknown_keys: Vec::new(),
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
                unknown_keys: Vec::new(),
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
version: 2
tables:
  - name: items
    scope: /data
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
            unknown_keys: Vec::new(),
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
        // The version 1 spelling, which `ConfigBuilder::build` moves to `path`
        // for a version 2 config.
        assert_eq!(field.xml_path.as_deref(), Some("/path/to/field"));
        assert_eq!(field.path, None);
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
            unknown_keys: Vec::new(),
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
            unknown_keys: Vec::new(),
            tables: vec![TableConfig::new("", "/root", vec![], vec![])],
        };
        let err = config.validate().unwrap_err();
        assert!(matches!(err, Error::InvalidConfig { .. }));
        assert!(err.to_string().contains("Table name must not be empty"));
    }

    #[test]
    fn test_empty_table_path_rejected() {
        let config = Config {
            version: None,
            parser_options: Default::default(),
            defaults: None,
            unknown_keys: Vec::new(),
            tables: vec![TableConfig::new("items", "", vec![], vec![])],
        };
        let err = config.validate().unwrap_err();
        assert!(matches!(err, Error::InvalidConfig { .. }));
        assert!(err.to_string().contains("names no element"));
    }

    #[test]
    fn test_duplicate_field_names_in_same_table_rejected() {
        let config = Config {
            version: None,
            parser_options: Default::default(),
            defaults: None,
            unknown_keys: Vec::new(),
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
            unknown_keys: Vec::new(),
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
            unknown_keys: Vec::new(),
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
    fn test_empty_field_path_rejected() {
        let config = Config {
            version: None,
            parser_options: Default::default(),
            defaults: None,
            unknown_keys: Vec::new(),
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
        assert!(err.to_string().contains("names no location"));
    }

    #[test]
    fn test_field_path_not_under_table_path_rejected() {
        let config = Config {
            version: None,
            parser_options: Default::default(),
            defaults: None,
            unknown_keys: Vec::new(),
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
        assert!(err.to_string().contains("not inside table"));
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
            unknown_keys: Vec::new(),
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
        assert!(err.to_string().contains("not inside table"));
    }

    #[test]
    fn test_field_path_equal_to_table_path_accepted() {
        // A field can capture the table element's own text content.
        let config = Config {
            version: None,
            parser_options: Default::default(),
            defaults: None,
            unknown_keys: Vec::new(),
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
    fn test_duplicate_table_path_rejected() {
        // The path registry stores one table per node; a duplicate path
        // would silently starve the earlier table of rows.
        let config = Config {
            version: None,
            parser_options: Default::default(),
            defaults: None,
            unknown_keys: Vec::new(),
            tables: vec![
                TableConfig::new("a", "/data", vec![], vec![]),
                TableConfig::new("b", "/data", vec![], vec![]),
            ],
        };
        let err = config.validate().unwrap_err();
        assert!(matches!(err, Error::InvalidConfig { .. }));
        assert!(err.to_string().contains("name the same element"));
        assert!(err.to_string().contains("'a'"));
        assert!(err.to_string().contains("'b'"));
    }

    #[test]
    fn test_duplicate_table_path_detected_across_spellings() {
        // "/data", "data" and "/data/" all resolve to the same registry
        // node, so they must count as duplicates regardless of spelling.
        let config = Config {
            version: None,
            parser_options: Default::default(),
            defaults: None,
            unknown_keys: Vec::new(),
            tables: vec![
                TableConfig::new("a", "/data", vec![], vec![]),
                TableConfig::new("b", "data/", vec![], vec![]),
            ],
        };
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("name the same element"));
    }

    #[test]
    fn test_field_path_under_table_path_accepted() {
        let config = Config {
            version: None,
            parser_options: Default::default(),
            defaults: None,
            unknown_keys: Vec::new(),
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
            unknown_keys: Vec::new(),
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
            unknown_keys: Vec::new(),
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

    /// Resolution is pure string work, so pin it directly.
    #[rstest]
    #[case::dot("/report/header", ".", "/report/header")]
    #[case::relative_name("/report/data", "item", "/report/data/item")]
    #[case::relative_path("/report/data", "items/item", "/report/data/items/item")]
    #[case::absolute("/report/data", "/report/data/item", "/report/data/item")]
    #[case::root_table("/", "item", "/item")]
    fn row_paths_resolve(#[case] xml_path: &str, #[case] row: &str, #[case] expected: &str) {
        assert_eq!(resolve_row_path(xml_path, row), expected);
    }

    /// A table with one field, relative to the row, so it lies inside the row
    /// whatever the row is.
    fn table_with_row(xml_path: &str, row: &str) -> TableConfig {
        TableConfig::builder("t", xml_path)
            .row(row)
            .field(
                FieldConfigBuilder::new("v", "v", DType::Int32)
                    .build()
                    .unwrap(),
            )
            .build()
    }

    #[test]
    fn empty_row_is_rejected() {
        let config = Config::builder()
            .version(2)
            .table(table_with_row("/a", "  "))
            .build();
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
            .version(2)
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
            .version(2)
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
    /// `t` has no fields, because `inner` would capture every one inside the
    /// row.
    #[test]
    fn a_table_at_the_row_element_itself_is_accepted() {
        let config = Config::builder()
            .version(2)
            .table(TableConfig::builder("t", "/a").row("b").build())
            .table(
                TableConfig::builder("inner", "/a/b")
                    .row(".")
                    .links(vec![])
                    .field(
                        FieldConfigBuilder::new("v", "v", DType::Int32)
                            .build()
                            .unwrap(),
                    )
                    .build(),
            )
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
                .version(2)
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
            .version(2)
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
            .version(2)
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

    /// A written config carries only what differs from the defaults, states a
    /// table's row before its fields, and reads back to exactly what was
    /// written.
    #[test]
    fn a_written_config_omits_default_keys_and_round_trips() {
        let config: Config = yaml_serde::from_str(
            r#"
            version: 2
            parser_options:
              trim_text: true
              validate_attributes: false
            tables:
              - name: t
                scope: /a
                row: item
                fields:
                  - {name: v, path: v, data_type: Float64, scale: 2.0}
                  - {name: s, path: s, data_type: Utf8, nullable: true}
            "#,
        )
        .unwrap();
        let yaml = yaml_serde::to_string(&config).unwrap();
        for left_at_default in [
            "levels",
            "offset",
            "nullable: false",
            "stop_at_paths",
            "validate_closing_tags",
            "strip_namespaces",
            "allow_truncated_input",
            "error_on_unmatched_fields",
            "max_value_bytes",
        ] {
            assert!(
                !yaml.contains(left_at_default),
                "{left_at_default} in:\n{yaml}"
            );
        }
        for set in [
            "trim_text: true",
            "validate_attributes: false",
            "scale: 2.0",
            "nullable: true",
        ] {
            assert!(yaml.contains(set), "{set} missing from:\n{yaml}");
        }
        assert!(yaml.find("row:").unwrap() < yaml.find("fields:").unwrap());
        assert_eq!(yaml_serde::from_str::<Config>(&yaml).unwrap(), config);

        let defaults_only = Config::builder()
            .version(2)
            .table(table_with_row("/a", "item"))
            .build()
            .unwrap();
        assert!(
            !yaml_serde::to_string(&defaults_only)
                .unwrap()
                .contains("parser_options")
        );
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
    #[case::dot_is_the_row_element("/report/ms", Some("m"), Some("."), None, "/report/ms/m")]
    #[case::dot_on_a_dot_row_is_the_table_element(
        "/report/header",
        Some("."),
        Some("."),
        None,
        "/report/header"
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
        Config::builder().version(2).table(table).build()
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

    /// An *absolute* path resolves without a row, so the same table is
    /// rejected for the missing row instead.
    #[test]
    fn an_absolute_path_without_a_declared_row_is_rejected_for_the_row() {
        let config = config_with_field(None, field_config(Some("/a/item/v"), None));
        assert!(
            matches!(
                config,
                Err(Error::InvalidConfig {
                    reason: ConfigIssue::MissingRow { .. }
                })
            ),
            "got {config:?}"
        );
    }

    #[test]
    fn field_paths_survive_a_yaml_round_trip() {
        let config = config_with_field(Some("item"), field_config(Some("v"), None)).unwrap();
        let yaml = yaml_serde::to_string(&config).unwrap();
        let restored: Config = yaml_serde::from_str(&yaml).unwrap();
        assert_eq!(restored, config);
        // Neither key is emitted when unset, so a written config never
        // sprouts a `xml_path: null` beside its `path`.
        assert!(!yaml.contains("xml_path: null"));
        assert!(!yaml.contains("path: null"));
    }

    // --- Declared links (`links:`) --------------------------------------------

    /// Two tables where `inner`'s rows sit inside `outer`'s.
    fn linked(links: Vec<Link>, levels: Vec<String>) -> Result<Config> {
        let outer = TableConfig::builder("outer", "/a")
            .row("station")
            .row_id(RowId::Enabled(true))
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
        Config::builder()
            .version(2)
            .table(outer)
            .table(inner)
            .build()
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

    /// The table a `parent:` link names gains a key column because of the
    /// link, so it has to say so itself: otherwise adding a child table would
    /// change the parent's schema.
    #[test]
    fn a_parent_without_row_id_is_rejected() {
        let mut config = linked(vec![parent_link("outer")], vec![]).unwrap();
        config.tables[0].row_id = None;
        let err = config.validate().unwrap_err();
        assert!(
            matches!(
                &err,
                Error::InvalidConfig {
                    reason: ConfigIssue::ParentWithoutRowId { table, parent }
                } if table == "inner" && parent == "outer"
            ),
            "{err:?}"
        );
        assert_eq!(
            err.to_string(),
            "Table 'inner' links to 'outer' with `parent:`, so 'outer' needs a key column; \
             declare it on 'outer' with `row_id: true` for a column named `_id`, \
             `row_id: <name>` to name it, or `row_id: false` to leave it out"
        );
    }

    /// Every spelling of the declaration satisfies the rule, including the one
    /// that asks for no key column at all.
    #[rstest]
    #[case::default_name(RowId::Enabled(true))]
    #[case::a_name(RowId::Named("station_key".into()))]
    #[case::no_column(RowId::Enabled(false))]
    fn any_row_id_on_the_parent_is_accepted(#[case] row_id: RowId) {
        let mut config = linked(vec![parent_link("outer")], vec![]).unwrap();
        config.tables[0].row_id = Some(row_id);
        assert!(config.validate().is_ok());
    }

    /// No two of a table's columns may share a name, whichever key produced
    /// them, and none may be nameless.
    #[rstest]
    #[case::key_and_field(Some(RowId::Named("v".into())), None, "two columns named 'v'")]
    #[case::key_and_link(Some(RowId::Enabled(true)), Some("_id"), "two columns named '_id'")]
    #[case::empty_key(
        Some(RowId::Named(String::new())),
        None,
        "The column name set by `row_id:` of table 'inner' is empty"
    )]
    #[case::empty_link_name(
        None,
        Some(""),
        "The column name set by `name:` of link 1 of table 'inner' is empty"
    )]
    fn column_names_are_unique_and_non_empty(
        #[case] inner_row_id: Option<RowId>,
        #[case] link_name: Option<&str>,
        #[case] expected: &str,
    ) {
        let mut config = linked(vec![parent_link("outer")], vec![]).unwrap();
        config.tables[1].links = Some(vec![Link {
            parent: Some("outer".to_string()),
            name: link_name.map(String::from),
            ..Default::default()
        }]);
        config.tables[1].row_id = inner_row_id;
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains(expected), "{err}");
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
        let config = Config::builder()
            .version(2)
            .table(sibling)
            .table(child)
            .build();
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
            Config::builder().version(2).table(table).build(),
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

    /// The table's own row element is a table's row element too: its counter
    /// is the one a version 1 table's last `levels` column reads.
    #[test]
    fn an_index_of_naming_the_tables_own_row_is_accepted() {
        let link = Link {
            index_of: Some("/a/station/ms/m".to_string()),
            ..Default::default()
        };
        assert!(linked(vec![link], vec![]).is_ok());
    }

    /// A table whose row is its own scope element holds one row per
    /// occurrence of it, and its count restarts at every occurrence, so a link
    /// counting it is a column of zeros. Rejected whether the link counts the
    /// table's own rows or an enclosing table's.
    #[rstest]
    #[case::its_own_rows(".", "/a/station/ms", "inner")]
    #[case::an_enclosing_tables_rows("m", "/a/station", "stations")]
    fn an_index_of_counting_a_row_dot_table_is_rejected(
        #[case] inner_row: &str,
        #[case] index_of: &str,
        #[case] expected_counted: &str,
    ) {
        // `stations` rows at its own scope element, as `inner` does in the
        // first case.
        let stations = TableConfig::builder("stations", "/a/station")
            .row(".")
            .build();
        let inner = TableConfig::builder("inner", "/a/station/ms")
            .row(inner_row)
            .links(vec![Link {
                index_of: Some(index_of.to_string()),
                ..Default::default()
            }])
            .field(
                FieldConfigBuilder::new("v", "v", DType::Int32)
                    .build()
                    .unwrap(),
            )
            .build();
        let config = Config::builder()
            .version(2)
            .table(stations)
            .table(inner)
            .build();
        let Err(Error::InvalidConfig {
            reason: ConfigIssue::IndexOfAlwaysZero { counted_table, .. },
        }) = &config
        else {
            panic!("expected IndexOfAlwaysZero, got {config:?}");
        };
        assert_eq!(counted_table, expected_counted);
    }

    /// The message names the fix: the same rows, scoped one level up, are
    /// counted within the element around them.
    #[test]
    fn the_always_zero_message_says_how_to_count_the_rows() {
        let err = ConfigIssue::IndexOfAlwaysZero {
            table: "readings".into(),
            index_of: "/r/g/h".into(),
            counted_table: "h".into(),
            scope: "/r/g/h".into(),
        };
        assert_eq!(
            err.to_string(),
            "Table 'readings' has index_of '/r/g/h', which counts the rows of table 'h'. That \
             table's row is its own scope element, '/r/g/h', so each occurrence holds one row \
             and every position would be 0; to count its rows within the element around them, \
             scope 'h' one level up: scope: /r/g, row: h"
        );
        // The document element has no level up worth suggesting: the document
        // holds it once.
        let top = ConfigIssue::IndexOfAlwaysZero {
            table: "t".into(),
            index_of: "/r".into(),
            counted_table: "t".into(),
            scope: "/r".into(),
        };
        assert!(
            top.to_string().ends_with("every position would be 0"),
            "{top}"
        );
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
            reason: ConfigIssue::ColumnNameCollision { column, .. },
        }) = config
        else {
            panic!("expected ColumnNameCollision, got {config:?}");
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
            version: 2
            tables:
              - name: t
                scope: /a
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
            .version(2)
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
            let mut config = Config::builder()
                .version(2)
                .table(migrated_root())
                .build()
                .unwrap();
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
                    reason: ConfigIssue::MissingRow { .. }
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
                    reason: ConfigIssue::ReplacedKey {
                        key: "levels",
                        replacement: "links",
                        ..
                    }
                })
            ));
        }

        /// Set after building, because `ConfigBuilder::build` moves a builder's
        /// `xml_path` to `scope` for a version 2 config.
        #[test]
        fn a_table_spelled_xml_path_is_rejected() {
            let mut config = Config::builder()
                .version(2)
                .table(migrated_root())
                .build()
                .unwrap();
            let table = &mut config.tables[0];
            table.scope = None;
            table.xml_path = Some("/report/stations".into());
            assert!(matches!(
                config.validate(),
                Err(Error::InvalidConfig {
                    reason: ConfigIssue::ReplacedKey {
                        key: "xml_path",
                        replacement: "scope",
                        ..
                    }
                })
            ));
        }

        /// `scope` and `xml_path` are two spellings of one element, so a table
        /// sets exactly one, as a field does for `path` and `xml_path`.
        #[rstest]
        #[case::both(Some("/r"), Some("/r"), "sets both")]
        #[case::neither(None, None, "sets neither")]
        fn a_table_must_name_its_element_exactly_once(
            #[case] scope: Option<&str>,
            #[case] xml_path: Option<&str>,
            #[case] expected: &str,
        ) {
            let mut table = migrated_root();
            table.scope = scope.map(String::from);
            table.xml_path = xml_path.map(String::from);
            let mut config = Config::builder().version(2).build().unwrap();
            config.tables.push(table);
            let err = config.validate().unwrap_err();
            assert!(err.to_string().contains(expected), "{err}");
        }

        /// Set after building, because `ConfigBuilder::build` moves a builder's
        /// `xml_path` to `path` for a version 2 config.
        #[test]
        fn a_field_spelled_xml_path_is_rejected() {
            let mut config = Config::builder()
                .version(2)
                .table(migrated_root())
                .build()
                .unwrap();
            let field = &mut config.tables[0].fields[0];
            field.path = None;
            field.xml_path = Some("/report/stations/station/@id".into());
            assert!(matches!(
                config.validate(),
                Err(Error::InvalidConfig {
                    reason: ConfigIssue::ReplacedKey {
                        key: "xml_path",
                        replacement: "path",
                        ..
                    }
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
                    reason: ConfigIssue::NestedTableWithoutLinks { .. }
                })
            ));
        }

        /// `links: []` is how a nested table says it deliberately has no
        /// link. Only *omitting* the key is rejected — the difference between
        /// "I decided" and "I forgot".
        #[test]
        fn an_empty_links_list_declares_a_deliberately_unlinked_table() {
            let child = TableConfig::builder("measurements", "/report/stations/station/ms")
                .row("m")
                .links(vec![])
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
            assert!(config.is_ok(), "{config:?}");
        }

        /// A table with one row per document can be spelled two ways, and
        /// they must behave alike: both make the tables below them nested,
        /// and both are satisfied by `links: []` there. The rule depends on
        /// where a table sits, never on how its path was written.
        #[rstest]
        #[case::root_path("/", "report")]
        #[case::row_dot("/report", ".")]
        fn both_spellings_of_a_document_level_table_are_treated_alike(
            #[case] xml_path: &str,
            #[case] row: &str,
        ) {
            let document = || {
                TableConfig::builder("report", xml_path)
                    .row(row)
                    .field(
                        FieldConfigBuilder::new("title", "/report/title", DType::Utf8)
                            .build()
                            .unwrap(),
                    )
                    .build()
            };
            let omitted = Config::builder()
                .version(2)
                .table(document())
                .table(migrated_root())
                .build();
            assert!(
                matches!(
                    &omitted,
                    Err(Error::InvalidConfig {
                        reason: ConfigIssue::NestedTableWithoutLinks { table, .. }
                    }) if table == "stations"
                ),
                "{omitted:?}"
            );

            let mut stations = migrated_root();
            stations.links = Some(vec![]);
            let declared = Config::builder()
                .version(2)
                .table(document())
                .table(stations)
                .build();
            assert!(declared.is_ok(), "{declared:?}");
        }

        /// The error names every way out, including the one that adds no
        /// column — otherwise a user who wants no link is left to guess.
        #[test]
        fn the_nested_table_error_offers_an_empty_links_list() {
            let child = TableConfig::builder("measurements", "/report/stations/station/ms")
                .row("m")
                .field(
                    FieldConfigBuilder::new("v", "v", DType::Int32)
                        .build()
                        .unwrap(),
                )
                .build();
            let err = Config::builder()
                .version(2)
                .table(migrated_root())
                .table(child)
                .build()
                .unwrap_err();
            assert!(err.to_string().contains("'links: []'"), "{err}");
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

        /// `outer` has one row per `<a>`, with a field at `path`; `inner` is
        /// nested inside it at `/r/a/bs`.
        fn outer_and_inner(path: &str) -> Result<Config> {
            let outer = TableConfig::builder("outer", "/r")
                .row("a")
                .field(
                    FieldConfigBuilder::new("f", path, DType::Int32)
                        .nullable(true)
                        .build()
                        .unwrap(),
                )
                .build();
            Config::builder()
                .version(2)
                .table(outer)
                .table(nested_table("inner", "/r/a/bs", "b"))
                .build()
        }

        fn nested_table(name: &str, xml_path: &str, row: &str) -> TableConfig {
            TableConfig::builder(name, xml_path)
                .row(row)
                .links(vec![])
                .field(
                    FieldConfigBuilder::new("v", "v", DType::Int32)
                        .build()
                        .unwrap(),
                )
                .build()
        }

        /// A nested table captures every value inside its `xml_path`, so a
        /// field of the enclosing table located there is never filled. That
        /// includes an attribute of the nested table's own element, which is
        /// read after that table opens, and the element's own text.
        #[rstest]
        #[case::attribute_of_the_nested_element("bs/@count")]
        #[case::text_of_the_nested_element("bs")]
        #[case::element_inside_the_nested_table("bs/b/v")]
        fn a_field_inside_a_nested_table_is_rejected(#[case] path: &str) {
            let config = outer_and_inner(path);
            let Err(Error::InvalidConfig {
                reason:
                    ConfigIssue::FieldInsideNestedTable {
                        table,
                        field,
                        field_path,
                        nested_table,
                    },
            }) = &config
            else {
                panic!("expected FieldInsideNestedTable, got {config:?}");
            };
            assert_eq!(
                (table.as_str(), field.as_str(), nested_table.as_str()),
                ("outer", "f", "inner")
            );
            assert_eq!(field_path, &format!("/r/a/{path}"));
        }

        /// The boundary is compared segment by segment: an element whose name
        /// only starts with the nested table's element name is outside it.
        #[rstest]
        #[case::sibling_element("name")]
        #[case::shared_name_prefix("bs_total")]
        fn a_field_beside_a_nested_table_is_accepted(#[case] path: &str) {
            let config = outer_and_inner(path);
            assert!(config.is_ok(), "{config:?}");
        }

        /// The innermost open table is the one that captures a value, so it is
        /// the table the error sends the field to.
        #[test]
        fn the_innermost_capturing_table_is_named() {
            let outer = TableConfig::builder("outer", "/r")
                .row("a")
                .field(
                    FieldConfigBuilder::new("f", "bs/b/cs/c/v", DType::Int32)
                        .build()
                        .unwrap(),
                )
                .build();
            let err = Config::builder()
                .version(2)
                .table(outer)
                .table(nested_table("middle", "/r/a/bs", "b"))
                .table(nested_table("innermost", "/r/a/bs/b/cs", "c"))
                .build()
                .unwrap_err();
            assert!(
                matches!(
                    &err,
                    Error::InvalidConfig {
                        reason: ConfigIssue::FieldInsideNestedTable { nested_table, .. }
                    } if nested_table == "innermost"
                ),
                "{err:?}"
            );
        }

        /// A root table is open for the whole document, and every other table
        /// is nested inside it.
        #[test]
        fn a_root_table_field_inside_a_nested_table_is_rejected() {
            let doc = TableConfig::builder("doc", "/")
                .row("r")
                .field(
                    FieldConfigBuilder::new("f", "/r/items/total", DType::Int32)
                        .build()
                        .unwrap(),
                )
                .build();
            let config = Config::builder()
                .version(2)
                .table(doc)
                .table(nested_table("items", "/r/items", "i"))
                .build();
            assert!(
                matches!(
                    config,
                    Err(Error::InvalidConfig {
                        reason: ConfigIssue::FieldInsideNestedTable { .. }
                    })
                ),
                "{config:?}"
            );
        }

        /// Two tables on one element: the one whose `xml_path` it is captures
        /// everything inside it, so the one whose row it is can hold no field
        /// there.
        #[test]
        fn a_table_rowed_at_another_tables_xml_path_cannot_read_inside_it() {
            let details = TableConfig::builder("details", "/report/stations/station")
                .row(".")
                .links(vec![])
                .field(
                    FieldConfigBuilder::new("name", "name", DType::Utf8)
                        .build()
                        .unwrap(),
                )
                .build();
            let err = Config::builder()
                .version(2)
                .table(migrated_root())
                .table(details)
                .build()
                .unwrap_err();
            assert!(
                matches!(
                    &err,
                    Error::InvalidConfig {
                        reason: ConfigIssue::FieldInsideNestedTable { table, nested_table, .. }
                    } if table == "stations" && nested_table == "details"
                ),
                "{err:?}"
            );
        }

        /// `stations` has one row per `<station>`, with a field at `path`.
        fn stations_with_field(path: &str) -> Result<Config> {
            let stations = TableConfig::builder("stations", "/report/stations")
                .row("station")
                .field(
                    FieldConfigBuilder::new("f", path, DType::Utf8)
                        .nullable(true)
                        .build()
                        .unwrap(),
                )
                .build();
            Config::builder().version(2).table(stations).build()
        }

        /// A value outside the row element attaches to whichever row ends
        /// next, so a value that appears once fills one row and leaves the rest
        /// empty. Each case is inside the table's `xml_path`, which is checked
        /// earlier and for every version, but outside its row.
        #[rstest]
        #[case::sibling_of_the_rows("/report/stations/summary")]
        #[case::attribute_of_the_container("/report/stations/@count")]
        #[case::text_of_the_container("/report/stations")]
        fn a_field_outside_its_row_is_rejected(#[case] path: &str) {
            let config = stations_with_field(path);
            let Err(Error::InvalidConfig {
                reason:
                    ConfigIssue::FieldOutsideRow {
                        table,
                        field,
                        field_path,
                        row_path,
                    },
            }) = &config
            else {
                panic!("expected FieldOutsideRow, got {config:?}");
            };
            assert_eq!(
                (table.as_str(), field.as_str(), field_path.as_str()),
                ("stations", "f", path)
            );
            assert_eq!(row_path, "/report/stations/station");
        }

        #[rstest]
        #[case::absolute_inside_the_row("/report/stations/station/name")]
        #[case::attribute_of_the_row_element("/report/stations/station/@id")]
        #[case::the_row_element_itself("/report/stations/station")]
        #[case::relative("name")]
        fn a_field_inside_its_row_is_accepted(#[case] path: &str) {
            let config = stations_with_field(path);
            assert!(config.is_ok(), "{config:?}");
        }

        /// A field a nested table captures never receives a value, and the fix
        /// is to move it to that table. Also rejecting it for lying outside its
        /// own row would send the author two ways at once, so it is reported
        /// for the capture alone.
        #[test]
        fn a_captured_field_is_not_also_rejected_for_lying_outside_its_row() {
            // `/r/bs/total` is outside the row `/r/a`, and inside `inner`.
            let outer = TableConfig::builder("outer", "/r")
                .row("a")
                .field(
                    FieldConfigBuilder::new("f", "/r/bs/total", DType::Int32)
                        .nullable(true)
                        .build()
                        .unwrap(),
                )
                .build();
            let config = Config::builder()
                .version(2)
                .table(outer)
                .table(nested_table("inner", "/r/bs", "b"))
                .build();
            assert!(
                matches!(
                    config,
                    Err(Error::InvalidConfig {
                        reason: ConfigIssue::FieldInsideNestedTable { .. }
                    })
                ),
                "{config:?}"
            );
        }

        /// A builder holds an element as `xml_path`, and a version 2 config
        /// spells a table's `scope` and a field's `path`, so that a config
        /// built in code writes out in the format it declares.
        #[test]
        fn building_a_version_2_config_moves_locations_to_the_version_2_keys() {
            let config = Config::builder()
                .version(2)
                .table(
                    TableConfig::builder("stations", "/report/stations")
                        .row("station")
                        .field(
                            FieldConfigBuilder::new("id", "@id", DType::Utf8)
                                .build()
                                .unwrap(),
                        )
                        .build(),
                )
                .build()
                .unwrap();
            let table = &config.tables[0];
            assert_eq!(
                (table.scope.as_deref(), table.xml_path.as_deref()),
                (Some("/report/stations"), None)
            );
            let field = &table.fields[0];
            assert_eq!(
                (field.path.as_deref(), field.xml_path.as_deref()),
                (Some("@id"), None)
            );
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

            let version_1 = TableConfig::new(
                "stations",
                "/report/stations",
                vec![],
                vec![
                    FieldConfigBuilder::new("id", "/report/stations/station/@id", DType::Utf8)
                        .build()
                        .unwrap(),
                ],
            );
            let unversioned = Config::builder().table(version_1).build().unwrap();
            assert!(
                !yaml_serde::to_string(&unversioned)
                    .unwrap()
                    .contains("version")
            );
        }
    }

    /// `metadata:` on tables and fields.
    mod metadata {
        use super::*;

        const YAML: &str = r#"
version: 2
tables:
  - name: readings
    scope: /report/readings
    row: reading
    metadata: {source: station export, revision: 1.50}
    fields:
      - name: value
        path: value
        data_type: Float64
        nullable: true
        trim: false
        metadata: {unit: hPa, code: 0x1F, calibrated: True, note: ""}
"#;

        fn entries(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
            pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect()
        }

        /// The parser never reads these values, so reading `1.50` as a number
        /// would only change what the author wrote. A field's metadata sits
        /// beside its flattened policies, where a value could have gone through
        /// a typed intermediate on the way in, so it is pinned as well.
        #[test]
        fn values_are_kept_exactly_as_written() {
            let config = Config::from_yaml_str(YAML).unwrap();
            let table = &config.tables[0];
            assert_eq!(
                table.metadata,
                entries(&[("source", "station export"), ("revision", "1.50")])
            );
            let field = &table.fields[0];
            assert_eq!(
                field.metadata,
                entries(&[
                    ("unit", "hPa"),
                    ("code", "0x1F"),
                    ("calibrated", "True"),
                    ("note", "")
                ])
            );
            assert_eq!(field.policies.trim, Some(false));
        }

        #[test]
        fn metadata_round_trips_and_is_left_out_when_empty() {
            let config = Config::from_yaml_str(YAML).unwrap();
            let yaml = yaml_serde::to_string(&config).unwrap();
            assert_eq!(Config::from_yaml_str(&yaml).unwrap(), config);

            let mut without = config.clone();
            without.tables[0].metadata.clear();
            without.tables[0].fields[0].metadata.clear();
            let yaml = yaml_serde::to_string(&without).unwrap();
            assert!(!yaml.contains("metadata"), "{yaml}");
        }

        #[rstest]
        #[case::a_list("tags: [a, b]")]
        #[case::a_mapping("owner: {team: data}")]
        fn a_value_that_is_not_a_scalar_is_rejected(#[case] entry: &str) {
            let yaml = format!(
                "version: 2\ntables:\n  - name: t\n    scope: /r\n    row: i\n    metadata:\n      {entry}\n    fields: [{{name: v, path: v, data_type: Int32}}]\n"
            );
            let err = Config::from_yaml_str(&yaml).unwrap_err();
            assert!(matches!(err, Error::Yaml(_)), "{err:?}");
            assert!(err.to_string().contains("expected a string"), "{err}");
        }

        fn field_with(metadata: &[(&str, &str)]) -> FieldConfig {
            FieldConfigBuilder::new("v", "v", DType::Int32)
                .metadata(metadata.iter().copied())
                .build()
                .unwrap()
        }

        /// Arrow reserves the `ARROW:` prefix, and gives some of its keys
        /// meaning, so a config setting one would change how readers interpret
        /// the output rather than annotate it.
        #[test]
        fn a_key_arrow_reserves_is_rejected_on_a_table_and_on_a_field() {
            let on_table = TableConfig::builder("t", "/r")
                .row("i")
                .metadata([("ARROW:extension:name", "x")])
                .field(field_with(&[]))
                .build();
            let err = Config::builder()
                .version(2)
                .table(on_table)
                .build()
                .unwrap_err();
            assert!(
                matches!(
                    &err,
                    Error::InvalidConfig {
                        reason: ConfigIssue::ReservedMetadataKey { field: None, key, .. }
                    } if key == "ARROW:extension:name"
                ),
                "{err:?}"
            );

            let on_field = TableConfig::builder("t", "/r")
                .row("i")
                .field(field_with(&[
                    ("unit", "hPa"),
                    ("ARROW:extension:metadata", "{}"),
                ]))
                .build();
            let err = Config::builder()
                .version(2)
                .table(on_field)
                .build()
                .unwrap_err();
            assert_eq!(
                err.to_string(),
                "Field 'v' in table 't' sets metadata key 'ARROW:extension:metadata', but keys \
                 beginning 'ARROW:' are reserved by the Arrow format, and some change how readers \
                 interpret the data; choose another key"
            );
        }

        /// Only the prefix exactly as Arrow reserves it: metadata keys are
        /// case-sensitive, and a key that merely mentions Arrow is the author's.
        #[rstest]
        #[case::other_case("arrow:extension:name")]
        #[case::not_a_prefix("source:ARROW:x")]
        #[case::no_colon("ARROW")]
        fn keys_outside_the_reserved_prefix_are_accepted(#[case] key: &str) {
            let table = TableConfig::builder("t", "/r")
                .row("i")
                .metadata([(key, "x")])
                .field(field_with(&[(key, "x")]))
                .build();
            let config = Config::builder().version(2).table(table).build();
            assert!(config.is_ok(), "{config:?}");
        }

        /// The builders add entries rather than replace them, like `field`
        /// and `level` do.
        #[test]
        fn builders_add_entries() {
            let field = FieldConfigBuilder::new("v", "v", DType::Int32)
                .metadata([("unit", "hPa")])
                .metadata([("description", "Pressure")])
                .build()
                .unwrap();
            assert_eq!(
                field.metadata,
                entries(&[("unit", "hPa"), ("description", "Pressure")])
            );
            let table = TableConfig::builder("t", "/r")
                .metadata([("a", "1")])
                .metadata([("b", "2")])
                .build();
            assert_eq!(table.metadata, entries(&[("a", "1"), ("b", "2")]));
        }
    }

    /// Keys the document sets that the configuration does not define.
    mod unknown_keys {
        use super::*;

        /// Wraps `table` in a version 2 config with one field, `v`, whose extra
        /// keys are `field_extra`.
        fn version_2(top: &str, table: &str, link: &str, field_extra: &str) -> String {
            format!(
                "version: 2\n{top}tables:\n  - name: stations\n    scope: /r/stations\n    row: station\n    row_id: true\n{table}    fields: [{{name: id, path: \"@id\", data_type: Utf8}}]\n  - name: readings\n    scope: /r/stations/station/readings\n    row: reading\n    links: [{{parent: stations{link}}}]\n    fields:\n      - {{name: v, path: v, data_type: Float64, nullable: true{field_extra}}}\n"
            )
        }

        #[rstest]
        #[case::top_level(
            version_2("defualts: {trim: true}\n", "", "", ""),
            "the top level",
            "defualts"
        )]
        #[case::parser_options(
            version_2("parser_options: {trim_txt: true}\n", "", "", ""),
            "`parser_options`",
            "trim_txt"
        )]
        #[case::defaults(
            version_2("defaults: {trimm: true}\n", "", "", ""),
            "`defaults`",
            "trimm"
        )]
        #[case::table(
            version_2("", "    rowz: station\n", "", ""),
            "table 'stations'",
            "rowz"
        )]
        #[case::link(
            version_2("", "", ", nme: key", ""),
            "link 1 of table 'readings'",
            "nme"
        )]
        #[case::field(
            version_2("", "", "", ", scal: 100.0"),
            "field 'v' of table 'readings'",
            "scal"
        )]
        #[case::field_policy(
            version_2("", "", "", ", on_missin: error"),
            "field 'v' of table 'readings'",
            "on_missin"
        )]
        fn version_2_rejects_an_unknown_key_and_says_where(
            #[case] yaml: String,
            #[case] expected_location: &str,
            #[case] expected_key: &str,
        ) {
            let err = Config::from_yaml_str(&yaml).unwrap_err();
            let Error::InvalidConfig {
                reason: ConfigIssue::UnknownKey { location, key },
            } = &err
            else {
                panic!("expected UnknownKey, got {err:?}");
            };
            assert_eq!(
                (location.as_str(), key.as_str()),
                (expected_location, expected_key)
            );
        }

        /// A version 1 config with the same misspelling.
        const VERSION_1: &str = "tables:\n  - name: readings\n    xml_path: /r/readings\n    levels: []\n    fields:\n      - {name: v, xml_path: /r/readings/reading/v, data_type: Float64, nullable: true, scal: 100.0}\n";

        /// Version 1 loads a config with an unknown key, as it always has.
        #[test]
        fn version_1_still_loads_a_config_with_an_unknown_key() {
            let config = Config::from_yaml_str(VERSION_1).unwrap();
            assert_eq!(
                config.unknown_keys().collect::<Vec<_>>(),
                vec![("field 'v' of table 'readings'".to_string(), "scal")]
            );
        }

        /// The guard against the deserialization twins falling behind the
        /// types: every key the configuration writes, set to something other
        /// than its default so none is left out, must read back as known.
        #[test]
        fn every_key_a_config_writes_reads_back_as_known() {
            // Every field named, with no `..Default::default()`, so an option
            // added later fails to compile here until this test sets it.
            let options = ParserOptions {
                trim_text: true,
                stop_at_paths: vec!["/r/end".into()],
                validate_closing_tags: false,
                validate_attributes: false,
                strip_namespaces: false,
                allow_truncated_input: true,
                error_on_unmatched_fields: true,
                max_value_bytes: Some(1024),
            };
            let policies = ValuePolicies {
                trim: Some(false),
                on_missing: Some(OnMissing::Null),
                on_invalid: Some(OnInvalid::Null),
                on_repeat: Some(OnRepeat::Last),
                null_values: Some(vec!["N/A".into()]),
            };
            let field = FieldConfigBuilder::new("v", "v", DType::Float64)
                .nullable(true)
                .scale(2.0)
                .offset(1.0)
                .policies(policies.clone())
                .metadata([("unit", "hPa")])
                .build()
                .unwrap();
            let stations = TableConfig::builder("stations", "/r/stations")
                .row("station")
                .row_id(RowId::Named("station_key".into()))
                .metadata([("source", "export")])
                .field(
                    // Nullable, because `defaults` asks for null on missing and
                    // invalid values, which only a nullable column can hold.
                    FieldConfigBuilder::new("id", "@id", DType::Utf8)
                        .nullable(true)
                        .build()
                        .unwrap(),
                )
                .build();
            let parent = Link {
                parent: Some("stations".into()),
                index_of: None,
                name: Some("station".into()),
            };
            let position = Link {
                parent: None,
                index_of: Some("/r/stations/station/readings/reading".into()),
                name: Some("position".into()),
            };
            let readings = TableConfig::builder("readings", "/r/stations/station/readings")
                .row("reading")
                .links([parent, position])
                .field(field)
                .build();
            let config = Config::builder()
                .version(2)
                .parser_options(options)
                .defaults(policies)
                .table(stations)
                .table(readings)
                .build()
                .unwrap();

            let yaml = yaml_serde::to_string(&config).unwrap();
            let read = Config::from_yaml_str(&yaml).unwrap();
            assert_eq!(read.unknown_keys().count(), 0, "{yaml}");
            assert_eq!(read, config, "{yaml}");

            // And the two keys only version 1 has.
            let legacy = Config::builder()
                .table(TableConfig::new(
                    "items",
                    "/r",
                    vec!["item".into()],
                    vec![FieldConfig {
                        path: None,
                        xml_path: Some("/r/item/v".into()),
                        ..FieldConfigBuilder::new("v", "/r/item/v", DType::Int32)
                            .build()
                            .unwrap()
                    }],
                ))
                .build()
                .unwrap();
            let yaml = yaml_serde::to_string(&legacy).unwrap();
            let read = Config::from_yaml_str(&yaml).unwrap();
            assert_eq!(read.unknown_keys().count(), 0, "{yaml}");
            assert_eq!(read, legacy, "{yaml}");
        }

        /// Reading a field through its explicit twin names the policy key a
        /// type error is in, and points at its value. The flattened read could
        /// only name the field, and pointed at the start of it.
        #[test]
        fn a_type_error_in_a_policy_names_the_key_and_its_position() {
            let err = Config::from_yaml_str(
                "tables:\n  - name: t\n    xml_path: /r\n    row: i\n    fields:\n      - {name: v, path: v, data_type: Int32, trim: maybe}\n",
            )
            .unwrap_err();
            assert!(matches!(err, Error::Yaml(_)), "{err:?}");
            let message = err.to_string();
            assert!(
                message.starts_with("tables[0].fields[0].trim:"),
                "{message}"
            );
            assert!(message.ends_with("at line 6 column 52"), "{message}");
        }

        /// Only where a key was is recorded, not its value, so nothing is
        /// written back; a config built in code has none to begin with.
        #[test]
        fn unknown_keys_are_not_written_back() {
            let config = Config::from_yaml_str(VERSION_1).unwrap();
            let written = yaml_serde::to_string(&config).unwrap();
            assert!(!written.contains("scal"), "{written}");
            assert!(!written.contains("unknown"), "{written}");
        }
    }

    /// Version 1 is exactly the format 0.19 read, so a config without
    /// `version: 2` may not set a key only version 2 defines.
    mod version_1 {
        use super::*;

        const TABLE: &str = "    xml_path: /r/s\n";
        const FIELD: &str = "{name: id, xml_path: /r/s/station/@id, data_type: Utf8";

        fn rejection(yaml: &str) -> (String, &'static str) {
            let err = Config::from_yaml_str(yaml).unwrap_err();
            let Error::InvalidConfig {
                reason: ConfigIssue::KeyRequiresVersion2 { location, key },
            } = err
            else {
                panic!("expected KeyRequiresVersion2, got {err:?}");
            };
            (location, key)
        }

        #[rstest]
        #[case::defaults("defaults: {trim: true}\n", "", "}", "the top level", "defaults")]
        #[case::scope("", "    scope: /r/s\n", "}", "table 't'", "scope")]
        #[case::row("", "    row: station\n", "}", "table 't'", "row")]
        #[case::links("", "    links: []\n", "}", "table 't'", "links")]
        #[case::row_id("", "    row_id: false\n", "}", "table 't'", "row_id")]
        #[case::table_metadata("", "    metadata: {a: b}\n", "}", "table 't'", "metadata")]
        #[case::trim("", "", ", trim: false}", "field 'id' of table 't'", "trim")]
        #[case::on_missing(
            "",
            "",
            ", on_missing: empty}",
            "field 'id' of table 't'",
            "on_missing"
        )]
        #[case::on_invalid(
            "",
            "",
            ", on_invalid: error}",
            "field 'id' of table 't'",
            "on_invalid"
        )]
        #[case::on_repeat("", "", ", on_repeat: first}", "field 'id' of table 't'", "on_repeat")]
        #[case::null_values(
            "",
            "",
            ", null_values: [NA]}",
            "field 'id' of table 't'",
            "null_values"
        )]
        #[case::field_metadata(
            "",
            "",
            ", metadata: {a: b}}",
            "field 'id' of table 't'",
            "metadata"
        )]
        fn a_version_2_key_is_rejected(
            #[case] top: &str,
            #[case] table: &str,
            #[case] field_end: &str,
            #[case] expected_location: &str,
            #[case] expected_key: &str,
        ) {
            let yaml = format!(
                "{top}tables:\n  - name: t\n{TABLE}{table}    fields:\n      - {FIELD}{field_end}\n"
            );
            assert_eq!(
                rejection(&yaml),
                (expected_location.to_string(), expected_key)
            );
            // Saying `version: 1` out loud is the same format.
            assert_eq!(
                rejection(&format!("version: 1\n{yaml}")),
                (expected_location.to_string(), expected_key)
            );
        }

        /// `path:` is the version 2 spelling of `xml_path:`, so a field using
        /// it is rejected rather than read as the same location.
        #[test]
        fn a_field_spelled_path_is_rejected() {
            let yaml = "tables:\n  - name: t\n    xml_path: /r/s\n    fields:\n      - {name: id, path: /r/s/station/@id, data_type: Utf8}\n";
            assert_eq!(
                rejection(yaml),
                ("field 'id' of table 't'".to_string(), "path")
            );
        }

        /// A builder field is spelled `xml_path`, so a version 1 config built
        /// in code loads, and a table builder setting a version 2 key does not.
        #[test]
        fn builders_follow_the_same_rule() {
            let field = FieldConfigBuilder::new("id", "/r/s/station/@id", DType::Utf8)
                .build()
                .unwrap();
            let table = TableConfig::builder("t", "/r/s")
                .field(field.clone())
                .build();
            assert!(Config::builder().table(table).build().is_ok());

            let with_row = TableConfig::builder("t", "/r/s")
                .row("station")
                .field(field)
                .build();
            assert!(matches!(
                Config::builder().table(with_row).build(),
                Err(Error::InvalidConfig {
                    reason: ConfigIssue::KeyRequiresVersion2 { key: "row", .. }
                })
            ));
        }

        #[test]
        fn the_message_names_the_key_and_the_line_that_allows_it() {
            let err = ConfigIssue::KeyRequiresVersion2 {
                location: "table 'readings'".to_string(),
                key: "row",
            };
            assert_eq!(
                err.to_string(),
                "The key 'row:' in table 'readings' is part of configuration format version 2; \
                 declare `version: 2` at the top of the config to use it"
            );
        }
    }

    /// Paths with a `.` or `..` segment, which no element can match.
    mod dot_segments {
        use super::*;

        fn rejection(yaml: &str) -> (String, String) {
            let err = Config::from_yaml_str(yaml).unwrap_err();
            let Error::InvalidConfig {
                reason: ConfigIssue::DotSegmentInPath { location, path },
            } = err
            else {
                panic!("expected DotSegmentInPath, got {err:?}");
            };
            (location, path)
        }

        /// Version 2 rejects a dot segment in the keys only it has.
        #[rstest]
        #[case::row(
            "row: ./station",
            r#"[{name: id, path: "@id", data_type: Utf8}]"#,
            "",
            "the row of table 't'",
            "./station"
        )]
        #[case::relative_path(
            "row: station",
            "[{name: up, path: ../x, data_type: Utf8}]",
            "",
            "field 'up' of table 't'",
            "../x"
        )]
        #[case::absolute_path(
            "row: station",
            "[{name: n, path: /r/s/./station/name, data_type: Utf8}]",
            "",
            "field 'n' of table 't'",
            "/r/s/./station/name"
        )]
        #[case::index_of(
            "row: station",
            r#"[{name: id, path: "@id", data_type: Utf8}]"#,
            ", links: [{index_of: /r/s/./station}]",
            "link 1 of table 't'",
            "/r/s/./station"
        )]
        fn a_version_2_key_is_rejected(
            #[case] row: &str,
            #[case] fields: &str,
            #[case] links: &str,
            #[case] expected_location: &str,
            #[case] expected_path: &str,
        ) {
            let yaml = format!(
                "version: 2\ntables:\n  - {{name: t, xml_path: /r/s, {row}{links}, fields: {fields}}}\n"
            );
            assert_eq!(
                rejection(&yaml),
                (expected_location.to_string(), expected_path.to_string())
            );
        }

        /// The keys version 1 has keep loading there, as they did in 0.19, and
        /// are rejected once a config declares version 2.
        #[rstest]
        #[case::table_xml_path(
            "tables:\n  - {name: t, xml_path: /r/../s, fields: [{name: id, xml_path: /r/../s/station/@id, data_type: Utf8}]}\n",
            "the xml_path of table 't'",
            "/r/../s"
        )]
        #[case::field_xml_path(
            "tables:\n  - {name: t, xml_path: /r/s, fields: [{name: id, xml_path: /r/s/./station/@id, data_type: Utf8}]}\n",
            "field 'id' of table 't'",
            "/r/s/./station/@id"
        )]
        #[case::stop_at_paths(
            "parser_options: {stop_at_paths: [/r/./end]}\ntables:\n  - {name: t, xml_path: /r/s, fields: [{name: id, xml_path: /r/s/station/@id, data_type: Utf8}]}\n",
            "`parser_options.stop_at_paths`",
            "/r/./end"
        )]
        fn a_version_1_key_is_rejected_in_version_2_only(
            #[case] yaml: &str,
            #[case] expected_location: &str,
            #[case] expected_path: &str,
        ) {
            assert!(Config::from_yaml_str(yaml).is_ok());
            assert_eq!(
                rejection(&format!("version: 2\n{yaml}")),
                (expected_location.to_string(), expected_path.to_string())
            );
        }

        /// `.` on its own names the table or row element, and a name may
        /// contain dots; only a whole `.` or `..` segment is rejected.
        #[test]
        fn a_whole_dot_and_names_with_dots_are_accepted() {
            let config = Config::from_yaml_str(
                "version: 2\ntables:\n  - {name: t, scope: /r/s.t, row: \".\", fields: [{name: v, path: \".\", data_type: Utf8}, {name: w, path: a.b/c.., data_type: Utf8}]}\n",
            );
            assert!(config.is_ok(), "{config:?}");
        }
    }
}
