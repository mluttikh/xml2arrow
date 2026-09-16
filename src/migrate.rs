//! Converting a configuration to format version 2.
//!
//! [`Config::to_version_2`] rewrites a configuration into format version 2
//! without changing what it produces: every document parses to the same tables,
//! columns, values and errors under the converted config as under the original.
//! That promise decides every rewrite below. A part whose only version 2
//! spellings would change the output is not rewritten; it is left as it was and
//! reported as an [`Unconverted`] entry, because choosing between those
//! spellings is the author's decision.
//!
//! The rewrites, in the order they are applied:
//!
//! 1. **Rows.** A table whose rows end at exactly one configured child element
//!    declares `row:` naming that element: it is the element that already ended
//!    every row. A table whose rows end at several child elements, or at none,
//!    has no such spelling.
//! 2. **Field paths.** `xml_path:` becomes `path:` with the same absolute value.
//! 3. **Levels.** Each `levels` entry becomes an `index_of:` link naming the row
//!    element of the table whose rows it counts, with `name:` keeping the
//!    `<level>` column name, so the column and its values are unchanged.
//! 4. **Links.** A nested table that had no `levels` declares `links: []`.
//! 5. **Value policies.** Version 2 trims `Utf8` values and makes a missing
//!    non-nullable `Utf8` value an error. Each `Utf8` field that relied on the
//!    version 1 behavior states it instead, as `trim: false` and, when the field
//!    is not nullable, `on_missing: empty`, so declaring version 2 changes no
//!    value.
//!
//! An unknown key is never removed. Version 1 ignores it and version 2 rejects
//! it, and while deleting it changes nothing, correcting a misspelling can:
//! which one was meant is the author's to say. It is reported instead.
//!
//! A path with a `.` or `..` segment in a table's `xml_path`, a field's
//! `xml_path` or `stop_at_paths` is not rewritten either. No element can match
//! it, and writing it correctly changes the output from nothing to something.
//! The field keeps its `xml_path:` spelling, because a `path:` with such a
//! segment is rejected in every version.
//!
//! Two kinds of field are never rewritten, because version 2 rejects both and
//! every way to satisfy it changes the output:
//!
//! - a field inside the `xml_path` of a table nested within its own. That
//!   table captures every value there, so the field is never filled.
//! - a field outside its table's row element, once the table has one. Its
//!   value attaches to whichever row ends next.
//!
//! Both are reported instead.
//!
//! When nothing is left over, the converted config declares `version: 2`.
//! Otherwise it keeps the original's version, with every other step already
//! taken, so what remains to do is exactly [`Conversion::unconverted`].
//!
//! The result is a [`Config`], not text. Writing it out, with
//! [`Config::to_yaml_file`] for example, produces a fresh file: the original's
//! comments and layout are not carried over.

use std::fmt;

use crate::config::{
    Config, DType, Link, OnMissing, TableConfig, has_dot_segment, path_is_strictly_under,
    path_segments, paths_equal,
};
use crate::errors::Result;

/// The result of [`Config::to_version_2`].
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct Conversion {
    /// The converted configuration. It parses every document exactly as the
    /// original does, and declares `version: 2` when
    /// [`unconverted`](Self::unconverted) is empty.
    pub config: Config,
    /// What could not be converted without changing the output, in table
    /// order. The converted config leaves each of these parts as it was.
    pub unconverted: Vec<Unconverted>,
}

/// A part of a configuration that [`Config::to_version_2`] left as it was,
/// because every version 2 spelling of it would change the output.
///
/// Marked `#[non_exhaustive]`: conversion may report more cases in a future
/// release.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Unconverted {
    /// The document sets a key the configuration does not define. Version 1
    /// ignores it and version 2 rejects it; removing it changes nothing, but if
    /// it is a misspelling, correcting it can.
    ///
    /// The converted config does not carry the key when written out, since
    /// only where it was is known, not its value.
    UnknownKey {
        /// Where the key is, as a reader would look for it.
        location: String,
        /// The unknown key, as written.
        key: String,
    },
    /// A path has a `.` or `..` segment, which no element can match, so the
    /// table has no rows, the field is never filled, or the stop path never
    /// stops the parse. Writing it correctly changes that.
    DotSegmentInPath {
        /// Where the path is.
        location: String,
        /// The path, as written.
        path: String,
    },
    /// The table's rows end at more than one configured child element, or at
    /// none, so no `row:` reproduces them.
    ///
    /// With several, the table produces one partially-filled row per child
    /// element; `row: "."` is usually what was meant, and it changes the row
    /// count. With none, the table produces no rows at all.
    RowNotDeclarable {
        /// The table whose rows are inferred.
        table: String,
        /// The table's `xml_path`.
        xml_path: String,
        /// The configured child elements at which its rows end, possibly none.
        child_elements: Vec<String>,
    },
    /// The table declares more `levels` than there are tables to count: the
    /// tables it sits inside, and itself. The surplus columns never receive a
    /// value.
    ExcessLevels {
        /// The table declaring the levels.
        table: String,
        /// How many `levels` it declares.
        declared: usize,
        /// How many tables can supply a value.
        available: usize,
    },
    /// A `levels` entry counts the rows of a table whose rows do not contain
    /// this table's, so no `index_of:` link can name that count.
    LevelCountsNonEnclosingTable {
        /// The table declaring the level.
        table: String,
        /// The level, as written in `levels`.
        level: String,
        /// The table whose rows the level counts.
        counted_table: String,
    },
    /// A `levels` entry counts the rows of a table that shares its row element
    /// with another table, so an `index_of:` link naming that element could
    /// resolve to either table's count.
    LevelCountsSharedRowElement {
        /// The table declaring the level.
        table: String,
        /// The level, as written in `levels`.
        level: String,
        /// The table whose rows the level counts.
        counted_table: String,
        /// The other table with the same row element.
        other_table: String,
    },
    /// A field lies inside the `xml_path` of a table nested within its own,
    /// which captures every value there, so the field is never filled.
    ///
    /// Version 2 rejects the field, and both ways to satisfy it change the
    /// columns: declaring it on the nested table moves the column, and removing
    /// it drops the column.
    FieldInsideNestedTable {
        /// The table the field is declared on.
        table: String,
        /// The field that is never filled.
        field: String,
        /// The nested table that captures values there.
        nested_table: String,
    },
    /// A field lies outside its table's row element, so its value attaches to
    /// whichever row ends next rather than to a row of its own.
    ///
    /// Version 2 rejects the field. Pointing it inside the row reads a
    /// different value, and giving it a table of its own moves the column, so
    /// both change the output.
    FieldOutsideRow {
        /// The table the field is declared on.
        table: String,
        /// The field outside the row element.
        field: String,
        /// The table's resolved row element.
        row_path: String,
    },
}

impl fmt::Display for Unconverted {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Unconverted::DotSegmentInPath { location, path } => write!(
                f,
                "{location}: the path '{path}' has a '.' or '..' segment, so it matches nothing; \
                 version 2 rejects it, and writing it correctly changes the output"
            ),
            Unconverted::UnknownKey { location, key } => write!(
                f,
                "{location}: '{key}' is not a configuration key; version 2 rejects it, and \
                 correcting a misspelling changes the output where removing it does not"
            ),
            Unconverted::RowNotDeclarable {
                table,
                xml_path,
                child_elements,
            } if child_elements.is_empty() => write!(
                f,
                "table '{table}': no child element of {xml_path} ends its rows, so it produces \
                 none, and any `row:` would change that"
            ),
            Unconverted::RowNotDeclarable {
                table,
                xml_path,
                child_elements,
            } => write!(
                f,
                "table '{table}': its rows end at {} child elements of {xml_path} ({}), and no \
                 `row:` keeps that; `row: \".\"` gives one row per <{}>, which changes the row \
                 count",
                child_elements.len(),
                child_elements.join(", "),
                path_segments(xml_path).next_back().unwrap_or("/"),
            ),
            Unconverted::ExcessLevels {
                table,
                declared,
                available,
            } => write!(
                f,
                "table '{table}': declares {declared} levels, but only {available} table(s) can \
                 supply one, counting itself; the surplus columns never receive a value"
            ),
            Unconverted::LevelCountsNonEnclosingTable {
                table,
                level,
                counted_table,
            } => write!(
                f,
                "table '{table}': level '{level}' counts the rows of '{counted_table}', which do \
                 not contain this table's rows, so no `index_of:` link can reproduce it"
            ),
            Unconverted::LevelCountsSharedRowElement {
                table,
                level,
                counted_table,
                other_table,
            } => write!(
                f,
                "table '{table}': level '{level}' counts the rows of '{counted_table}', which \
                 shares its row element with '{other_table}', so an `index_of:` link could not \
                 tell the two apart"
            ),
            Unconverted::FieldInsideNestedTable {
                table,
                field,
                nested_table,
            } => write!(
                f,
                "table '{table}': field '{field}' lies inside table '{nested_table}', which \
                 captures every value there, so it is never filled; version 2 rejects it, and \
                 moving it to '{nested_table}' or removing it changes the columns"
            ),
            Unconverted::FieldOutsideRow {
                table,
                field,
                row_path,
            } => write!(
                f,
                "table '{table}': field '{field}' lies outside the row element {row_path}, so \
                 its value attaches to whichever row ends next; version 2 rejects it, and \
                 pointing it inside the row or giving it a table of its own changes the output"
            ),
        }
    }
}

impl Config {
    /// Converts this configuration to format version 2 without changing what
    /// it produces.
    ///
    /// Every document parses to the same tables, columns, values and errors
    /// under [`Conversion::config`] as under `self`. Parts that version 2 can
    /// only spell in a way that changes the output are left as they were and
    /// listed in [`Conversion::unconverted`]; the converted config declares
    /// `version: 2` only when that list is empty. A config that already
    /// declares `version: 2` is returned unchanged.
    ///
    /// See the [`migrate`](crate::migrate) module for the rewrites.
    ///
    /// # Errors
    ///
    /// Returns an error if this configuration is not valid. The converted
    /// configuration is validated as well, so a returned [`Conversion`] always
    /// loads.
    ///
    /// # Example
    ///
    /// ```rust
    /// use xml2arrow::config_from_yaml;
    ///
    /// let config = config_from_yaml!(r#"
    /// tables:
    ///   - name: items
    ///     xml_path: /data
    ///     levels: [item]
    ///     fields:
    ///       - {name: value, xml_path: /data/item/value, data_type: Utf8}
    /// "#);
    ///
    /// let conversion = config.to_version_2()?;
    /// assert!(conversion.unconverted.is_empty());
    /// assert_eq!(conversion.config.version, Some(2));
    /// assert_eq!(conversion.config.tables[0].row.as_deref(), Some("item"));
    /// # Ok::<(), xml2arrow::Error>(())
    /// ```
    pub fn to_version_2(&self) -> Result<Conversion> {
        self.validate()?;
        if self.version == Some(2) {
            return Ok(Conversion {
                config: self.clone(),
                unconverted: Vec::new(),
            });
        }

        let mut config = self.clone();
        let mut unconverted: Vec<Unconverted> = self
            .unknown_keys()
            .map(|(location, key)| Unconverted::UnknownKey {
                location,
                key: key.to_string(),
            })
            .collect();
        unconverted.extend(self.dot_segment_paths().map(|(location, path)| {
            Unconverted::DotSegmentInPath {
                location,
                path: path.to_string(),
            }
        }));
        declare_rows(self, &mut config, &mut unconverted);
        rename_field_paths(&mut config);
        replace_levels(self, &mut config, &mut unconverted);
        link_nested_tables(&mut config);
        state_utf8_policies(&mut config);
        report_misplaced_fields(&config, &mut unconverted);

        if unconverted.is_empty() {
            // Every rewrite was taken, so nothing version 2 rejects is left. A
            // step that is left anyway is a converter bug, which validation
            // below then reports rather than returning a config that lies.
            debug_assert_eq!(config.version_2_steps(), Vec::new());
            config.version = Some(2);
        }
        config.validate()?;
        Ok(Conversion {
            config,
            unconverted,
        })
    }
}

// --- Rewrites, in the order `to_version_2` applies them ---------------------

/// Declares `row:` on every table whose rows end at exactly one child element.
fn declare_rows(original: &Config, config: &mut Config, unconverted: &mut Vec<Unconverted>) {
    for table in config.tables.iter_mut().filter(|t| t.row.is_none()) {
        // Read from the original: which children end a row depends on every
        // configured path as written, before any of them is rewritten.
        let children = original.row_delimiting_children(&table.xml_path);
        if let [only] = children.as_slice() {
            table.row = Some(only.clone());
        } else {
            unconverted.push(Unconverted::RowNotDeclarable {
                table: table.name.clone(),
                xml_path: table.xml_path.clone(),
                child_elements: children,
            });
        }
    }
}

/// Renames every field's `xml_path:` to `path:`.
fn rename_field_paths(config: &mut Config) {
    for field in config.tables.iter_mut().flat_map(|t| t.fields.iter_mut()) {
        // Left spelled `xml_path:` when it has a `.` or `..` segment, which a
        // `path:` may not have in any version; it is reported instead.
        if field.xml_path.as_deref().is_some_and(has_dot_segment) {
            continue;
        }
        if let Some(xml_path) = field.xml_path.take() {
            // `xml_path` ignores a leading slash, but `path` reads it as the
            // difference between absolute and relative, so it is written in.
            field.path = Some(absolute(&xml_path));
        }
    }
}

/// Replaces each table's `levels` with the `index_of:` links that produce the
/// same columns.
fn replace_levels(original: &Config, config: &mut Config, unconverted: &mut Vec<Unconverted>) {
    // Row elements as declared by `declare_rows`: they are what `index_of:`
    // names, and what it is resolved against when the config loads.
    let scopes: Vec<String> = config
        .tables
        .iter()
        .map(TableConfig::link_scope_path)
        .collect();
    for idx in 0..config.tables.len() {
        // A table whose row could not be declared keeps its `levels` too:
        // whether they convert depends on the row element the author chooses,
        // and reporting them now would describe a problem that choice may
        // remove. Converting again after choosing takes care of them.
        let table = &config.tables[idx];
        if table.levels.is_empty() || table.row.is_none() {
            continue;
        }
        let counted = counted_tables(original, idx);
        // An `index_of:` may not have a `.` or `..` segment in any version, so
        // a level counting a table whose path has one stays a level; that path
        // is reported on its own.
        if counted
            .iter()
            .any(|&counted_idx| has_dot_segment(&scopes[counted_idx]))
        {
            continue;
        }
        match level_links(config, &scopes, idx, &counted) {
            Ok(links) => {
                let table = &mut config.tables[idx];
                table.levels.clear();
                table.links = Some(links);
            }
            Err(reason) => unconverted.push(reason),
        }
    }
}

/// Declares `links: []` on each nested table that has neither `levels` nor
/// `links`, which is what it produced: no link column.
fn link_nested_tables(config: &mut Config) {
    let unlinked: Vec<usize> = (0..config.tables.len())
        .filter(|&idx| {
            let table = &config.tables[idx];
            table.links.is_none()
                && table.levels.is_empty()
                && config.enclosing_table_of(table).is_some()
        })
        .collect();
    for idx in unlinked {
        config.tables[idx].links = Some(Vec::new());
    }
}

/// States the version 1 value handling on each `Utf8` field that relied on it.
fn state_utf8_policies(config: &mut Config) {
    let defaults = config.defaults.clone().unwrap_or_default();
    for field in config.tables.iter_mut().flat_map(|t| t.fields.iter_mut()) {
        if field.data_type != DType::Utf8 {
            continue;
        }
        // Only what neither the field nor `defaults:` states: a stated policy
        // already means the same under either version.
        let stated = field.policies.over(&defaults);
        if stated.trim.is_none() {
            field.policies.trim = Some(false);
        }
        if !field.nullable && stated.on_missing.is_none() {
            field.policies.on_missing = Some(OnMissing::Empty);
        }
    }
}

/// Reports each field that version 2 rejects for where it lies: inside a
/// nested table, or outside its own table's row element. Nothing is rewritten.
///
/// Read from the converted config rather than the original, so that it asks
/// exactly what version 2 validation will. The rewrites above leave every
/// resolved path where it was, but `declare_rows` gives tables the row element
/// the outside-the-row check is made against.
///
/// A field is reported for one reason at most, as validation lists it: one a
/// nested table captures never receives a value, and moving it to that table
/// is the decision to make.
fn report_misplaced_fields(config: &Config, unconverted: &mut Vec<Unconverted>) {
    for table in &config.tables {
        let row_path = table.row_path();
        for field in &table.fields {
            if let Some((_, nested)) = config.nested_table_capturing(table, field) {
                unconverted.push(Unconverted::FieldInsideNestedTable {
                    table: table.name.clone(),
                    field: field.name.clone(),
                    nested_table: nested.name.clone(),
                });
            } else if let Some(row_path) = &row_path
                && field.location_outside(row_path).is_some()
            {
                unconverted.push(Unconverted::FieldOutsideRow {
                    table: table.name.clone(),
                    field: field.name.clone(),
                    row_path: row_path.clone(),
                });
            }
        }
    }
}

// --- Helpers ----------------------------------------------------------------

/// The tables whose row counters fill a table's `levels` columns, in column
/// order: each table whose `xml_path` strictly encloses this table's, outermost
/// first, then the table itself.
///
/// This mirrors the parser, which reads one counter per open table when a row
/// ends. A table at `xml_path: /` supplies none, as the document root is not a
/// repeating scope.
fn counted_tables(config: &Config, idx: usize) -> Vec<usize> {
    let is_root = |path: &str| path_segments(path).next().is_none();
    let table_path = config.tables[idx].xml_path.as_str();
    let mut counted: Vec<usize> = (0..config.tables.len())
        .filter(|&other| {
            let other_path = &config.tables[other].xml_path;
            !is_root(other_path) && path_is_strictly_under(table_path, other_path)
        })
        .collect();
    counted.sort_by_key(|&other| path_segments(&config.tables[other].xml_path).count());
    if !is_root(table_path) {
        counted.push(idx);
    }
    counted
}

/// The `index_of:` links for a table's `levels`, or why they cannot be written
/// without changing the columns.
fn level_links(
    config: &Config,
    scopes: &[String],
    idx: usize,
    counted: &[usize],
) -> std::result::Result<Vec<Link>, Unconverted> {
    let table = &config.tables[idx];
    if table.levels.len() > counted.len() {
        return Err(Unconverted::ExcessLevels {
            table: table.name.clone(),
            declared: table.levels.len(),
            available: counted.len(),
        });
    }
    table
        .levels
        .iter()
        .zip(counted)
        .map(|(level, &counted_idx)| {
            let counted_scope = &scopes[counted_idx];
            // A table's own row element always names its own count. Another
            // table's must contain this table's rows, and be the only table
            // with that row element, for `index_of:` to resolve to its count.
            if counted_idx != idx {
                if !path_is_strictly_under(&scopes[idx], counted_scope) {
                    return Err(Unconverted::LevelCountsNonEnclosingTable {
                        table: table.name.clone(),
                        level: level.clone(),
                        counted_table: config.tables[counted_idx].name.clone(),
                    });
                }
                if let Some(other) = (0..config.tables.len()).find(|&other| {
                    other != counted_idx && paths_equal(&scopes[other], counted_scope)
                }) {
                    return Err(Unconverted::LevelCountsSharedRowElement {
                        table: table.name.clone(),
                        level: level.clone(),
                        counted_table: config.tables[counted_idx].name.clone(),
                        other_table: config.tables[other].name.clone(),
                    });
                }
            }
            Ok(Link {
                parent: None,
                index_of: Some(absolute(counted_scope)),
                name: Some(format!("<{level}>")),
            })
        })
        .collect()
}

/// `path` with exactly one leading slash and no empty segments.
fn absolute(path: &str) -> String {
    let mut absolute = String::with_capacity(path.len() + 1);
    for segment in path_segments(path) {
        absolute.push('/');
        absolute.push_str(segment);
    }
    if absolute.is_empty() {
        absolute.push('/');
    }
    absolute
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::OnMissing;
    use crate::config_from_yaml;

    fn index_of(path: &str, name: &str) -> Link {
        Link {
            parent: None,
            index_of: Some(path.to_string()),
            name: Some(name.to_string()),
        }
    }

    /// The migration guide's version 1 example: every part converts except the
    /// header, whose rows end at two child elements.
    #[test]
    fn converts_everything_but_a_table_with_split_rows() {
        let config = config_from_yaml!(
            r#"
            tables:
              - name: header
                xml_path: /report/header
                levels: []
                fields:
                  - {name: title, xml_path: /report/header/title, data_type: Utf8, nullable: true}
                  - {name: created, xml_path: /report/header/created, data_type: Utf8, nullable: true}
              - name: stations
                xml_path: /report/stations
                levels: [station]
                fields:
                  - {name: id, xml_path: /report/stations/station/@id, data_type: Utf8}
              - name: readings
                xml_path: /report/stations/station/readings
                levels: [station, reading]
                fields:
                  - {name: value, xml_path: /report/stations/station/readings/reading/value, data_type: Float64}
            "#
        );
        let conversion = config.to_version_2().unwrap();

        assert_eq!(
            conversion.unconverted,
            vec![Unconverted::RowNotDeclarable {
                table: "header".to_string(),
                xml_path: "/report/header".to_string(),
                child_elements: vec!["title".to_string(), "created".to_string()],
            }]
        );
        let converted = &conversion.config;
        assert_eq!(converted.version, None);
        let [header, stations, readings] = converted.tables.as_slice() else {
            panic!("expected three tables");
        };
        assert_eq!(header.row, None);
        assert_eq!(stations.row.as_deref(), Some("station"));
        assert_eq!(readings.row.as_deref(), Some("reading"));
        assert_eq!(
            stations.links,
            Some(vec![index_of("/report/stations/station", "<station>")])
        );
        assert_eq!(
            readings.links,
            Some(vec![
                index_of("/report/stations/station", "<station>"),
                index_of("/report/stations/station/readings/reading", "<reading>"),
            ])
        );
        assert!(converted.tables.iter().all(|t| t.levels.is_empty()));
        assert_eq!(
            readings.fields[0].path.as_deref(),
            Some("/report/stations/station/readings/reading/value")
        );
        assert!(
            converted
                .tables
                .iter()
                .flat_map(|t| &t.fields)
                .all(|f| f.xml_path.is_none())
        );
    }

    #[test]
    fn a_fully_converted_config_declares_version_2() {
        let config = config_from_yaml!(
            r#"
            tables:
              - name: items
                xml_path: /data
                levels: []
                fields:
                  - {name: v, xml_path: /data/item/v, data_type: Int32}
            "#
        );
        let conversion = config.to_version_2().unwrap();
        assert!(conversion.unconverted.is_empty());
        assert_eq!(conversion.config.version, Some(2));
    }

    /// `xml_path` ignores a leading slash; `path` would read its absence as
    /// "relative", so the converted path always has one.
    #[test]
    fn a_path_without_a_leading_slash_is_written_absolute() {
        let config = config_from_yaml!(
            r#"
            tables:
              - name: items
                xml_path: /data
                fields:
                  - {name: v, xml_path: data/item/v, data_type: Int32}
            "#
        );
        let converted = config.to_version_2().unwrap().config;
        assert_eq!(
            converted.tables[0].fields[0].path.as_deref(),
            Some("/data/item/v")
        );
    }

    #[test]
    fn a_nested_table_without_levels_declares_that_it_has_no_link() {
        let config = config_from_yaml!(
            r#"
            tables:
              - name: stations
                xml_path: /report/stations
                fields:
                  - {name: id, xml_path: /report/stations/station/@id, data_type: Int32}
              - name: readings
                xml_path: /report/stations/station/readings
                fields:
                  - {name: v, xml_path: /report/stations/station/readings/reading/v, data_type: Int32}
            "#
        );
        let converted = config.to_version_2().unwrap().config;
        assert_eq!(converted.tables[0].links, None);
        assert_eq!(converted.tables[1].links, Some(Vec::new()));
    }

    /// Version 2 would trim `Utf8` values and reject a missing non-nullable
    /// one, so each `Utf8` field states the version 1 behavior it relied on,
    /// and keeps whatever it already stated.
    #[test]
    fn utf8_fields_state_the_version_1_value_handling() {
        let config = config_from_yaml!(
            r#"
            tables:
              - name: items
                xml_path: /data
                row: item
                fields:
                  - {name: required, path: required, data_type: Utf8}
                  - {name: optional, path: optional, data_type: Utf8, nullable: true}
                  - {name: trimmed, path: trimmed, data_type: Utf8, trim: true}
                  - {name: count, path: count, data_type: Int32}
            "#
        );
        let converted = config.to_version_2().unwrap().config;
        let policies: Vec<_> = converted.tables[0]
            .fields
            .iter()
            .map(|f| (f.policies.trim, f.policies.on_missing))
            .collect();
        assert_eq!(
            policies,
            vec![
                (Some(false), Some(OnMissing::Empty)),
                (Some(false), None),
                (Some(true), Some(OnMissing::Empty)),
                (None, None),
            ]
        );
    }

    #[test]
    fn a_policy_stated_in_defaults_is_not_repeated_on_fields() {
        let config = config_from_yaml!(
            r#"
            defaults:
              trim: true
            tables:
              - name: items
                xml_path: /data
                row: item
                fields:
                  - {name: s, path: s, data_type: Utf8}
            "#
        );
        let field = &config.to_version_2().unwrap().config.tables[0].fields[0];
        assert_eq!(field.policies.trim, None);
        assert_eq!(field.policies.on_missing, Some(OnMissing::Empty));
    }

    #[test]
    fn surplus_levels_are_left_and_reported() {
        let config = config_from_yaml!(
            r#"
            tables:
              - name: items
                xml_path: /data
                levels: [data, item]
                fields:
                  - {name: v, xml_path: /data/item/v, data_type: Int32}
            "#
        );
        let conversion = config.to_version_2().unwrap();
        assert_eq!(
            conversion.unconverted,
            vec![Unconverted::ExcessLevels {
                table: "items".to_string(),
                declared: 2,
                available: 1,
            }]
        );
        assert_eq!(conversion.config.tables[0].levels, ["data", "item"]);
        assert_eq!(conversion.config.version, None);
    }

    /// Only a config that already declares `row:` can get here: a level whose
    /// counted table's rows sit beside this table's rather than around them.
    #[test]
    fn a_level_counting_a_table_that_does_not_contain_the_rows_is_left_and_reported() {
        let config = config_from_yaml!(
            r#"
            tables:
              - name: outer
                xml_path: /r
                row: a
                fields:
                  - {name: x, path: x, data_type: Int32}
              - name: inner
                xml_path: /r/b
                levels: [outer, inner]
                fields:
                  - {name: v, xml_path: /r/b/item/v, data_type: Int32}
            "#
        );
        let conversion = config.to_version_2().unwrap();
        assert_eq!(
            conversion.unconverted,
            vec![Unconverted::LevelCountsNonEnclosingTable {
                table: "inner".to_string(),
                level: "outer".to_string(),
                counted_table: "outer".to_string(),
            }]
        );
        assert_eq!(conversion.config.tables[1].levels, ["outer", "inner"]);
    }

    /// Whether a table's levels convert depends on its row element, so a table
    /// whose row is left to the author keeps its levels, and only the row is
    /// reported.
    #[test]
    fn a_table_whose_row_is_left_keeps_its_levels() {
        let config = config_from_yaml!(
            r#"
            tables:
              - name: items
                xml_path: /data
                levels: [data]
                fields:
                  - {name: a, xml_path: /data/a, data_type: Int32, nullable: true}
                  - {name: b, xml_path: /data/b, data_type: Int32, nullable: true}
            "#
        );
        let conversion = config.to_version_2().unwrap();
        assert!(matches!(
            conversion.unconverted.as_slice(),
            [Unconverted::RowNotDeclarable { .. }]
        ));
        assert_eq!(conversion.config.tables[0].levels, ["data"]);
        assert_eq!(conversion.config.tables[0].links, None);
    }

    /// Version 2 rejects a field that a nested table captures, so declaring it
    /// would make the converted config fail to load. The field is kept and
    /// reported, and every other part is still converted.
    #[test]
    fn a_field_inside_a_nested_table_is_left_and_reported() {
        let config = config_from_yaml!(
            r#"
            tables:
              - name: outer
                xml_path: /r
                levels: []
                fields:
                  - {name: count, xml_path: /r/a/bs/@count, data_type: Int32, nullable: true}
                  - {name: name, xml_path: /r/a/name, data_type: Int32, nullable: true}
              - name: inner
                xml_path: /r/a/bs
                levels: []
                fields:
                  - {name: v, xml_path: /r/a/bs/b/v, data_type: Int32}
            "#
        );
        let conversion = config.to_version_2().unwrap();
        assert_eq!(
            conversion.unconverted,
            vec![Unconverted::FieldInsideNestedTable {
                table: "outer".to_string(),
                field: "count".to_string(),
                nested_table: "inner".to_string(),
            }]
        );
        let converted = &conversion.config;
        assert_eq!(converted.version, None);
        assert_eq!(converted.tables[0].fields.len(), 2);
        assert_eq!(converted.tables[0].row.as_deref(), Some("a"));
        assert_eq!(converted.tables[1].links, Some(vec![]));
    }

    /// The frozen corpus's `attribute_child_of_table`: rows end only at
    /// `<item>`, so `row: item` is declared as usual, but the container's own
    /// attribute lies outside that row. Version 2 rejects the field, so it is
    /// kept and reported, and the table keeps its declared row.
    #[test]
    fn a_field_outside_the_declared_row_is_left_and_reported() {
        let config = config_from_yaml!(
            r#"
            tables:
              - name: items
                xml_path: /data
                levels: []
                fields:
                  - {name: id, xml_path: /data/@id, data_type: Utf8, nullable: true}
                  - {name: v, xml_path: /data/item/v, data_type: Int32, nullable: true}
            "#
        );
        let conversion = config.to_version_2().unwrap();
        assert_eq!(
            conversion.unconverted,
            vec![Unconverted::FieldOutsideRow {
                table: "items".to_string(),
                field: "id".to_string(),
                row_path: "/data/item".to_string(),
            }]
        );
        let items = &conversion.config.tables[0];
        assert_eq!(conversion.config.version, None);
        assert_eq!(items.row.as_deref(), Some("item"));
        assert_eq!(items.fields[0].path.as_deref(), Some("/data/@id"));
    }

    /// Removing an unknown key would change nothing, but correcting a
    /// misspelling can, so the key is reported and the config keeps its
    /// version. Everything else still converts.
    #[test]
    fn an_unknown_key_is_reported_and_the_rest_converts() {
        let config = config_from_yaml!(
            r#"
            tables:
              - name: items
                xml_path: /data
                levels: []
                fields:
                  - {name: v, xml_path: /data/item/v, data_type: Float64, scal: 100.0}
            "#
        );
        let conversion = config.to_version_2().unwrap();
        assert_eq!(
            conversion.unconverted,
            vec![Unconverted::UnknownKey {
                location: "field 'v' of table 'items'".to_string(),
                key: "scal".to_string(),
            }]
        );
        let items = &conversion.config.tables[0];
        assert_eq!(conversion.config.version, None);
        assert_eq!(items.row.as_deref(), Some("item"));
        assert_eq!(items.fields[0].path.as_deref(), Some("/data/item/v"));
    }

    /// A `path:` may not have a `.` or `..` segment in any version, so a field
    /// whose `xml_path` has one keeps that spelling and is reported, while the
    /// rest of the config converts. A level counting a table on such a path
    /// stays a level, rather than become an `index_of:` that would not load.
    #[test]
    fn a_path_with_a_dot_segment_is_left_and_reported() {
        let config = config_from_yaml!(
            r#"
            tables:
              - name: items
                xml_path: /data
                levels: []
                fields:
                  - {name: v, xml_path: /data/item/v, data_type: Utf8, nullable: true}
                  - {name: w, xml_path: /data/./item/w, data_type: Utf8, nullable: true}
              - name: dotted
                xml_path: /other/./group
                levels: [group]
                fields:
                  - {name: x, xml_path: /other/./group/entry/x, data_type: Utf8, nullable: true}
            "#
        );
        let conversion = config.to_version_2().unwrap();
        assert_eq!(
            conversion.unconverted,
            vec![
                Unconverted::DotSegmentInPath {
                    location: "field 'w' of table 'items'".to_string(),
                    path: "/data/./item/w".to_string(),
                },
                Unconverted::DotSegmentInPath {
                    location: "the xml_path of table 'dotted'".to_string(),
                    path: "/other/./group".to_string(),
                },
                Unconverted::DotSegmentInPath {
                    location: "field 'x' of table 'dotted'".to_string(),
                    path: "/other/./group/entry/x".to_string(),
                },
            ]
        );
        let [items, dotted] = conversion.config.tables.as_slice() else {
            panic!("expected two tables");
        };
        assert_eq!(items.row.as_deref(), Some("item"));
        assert_eq!(items.fields[0].path.as_deref(), Some("/data/item/v"));
        assert_eq!(items.fields[1].xml_path.as_deref(), Some("/data/./item/w"));
        assert_eq!(dotted.levels, ["group"]);
        assert_eq!(conversion.config.version, None);
    }

    #[test]
    fn a_version_2_config_is_returned_unchanged() {
        let config = config_from_yaml!(
            r#"
            version: 2
            tables:
              - name: items
                xml_path: /data
                row: item
                fields:
                  - {name: s, path: s, data_type: Utf8}
            "#
        );
        let conversion = config.to_version_2().unwrap();
        assert_eq!(conversion.config, config);
        assert!(conversion.unconverted.is_empty());
    }

    #[test]
    fn an_unconverted_part_says_what_would_change() {
        let message = Unconverted::RowNotDeclarable {
            table: "header".to_string(),
            xml_path: "/report/header".to_string(),
            child_elements: vec!["title".to_string(), "created".to_string()],
        }
        .to_string();
        assert_eq!(
            message,
            "table 'header': its rows end at 2 child elements of /report/header (title, \
             created), and no `row:` keeps that; `row: \".\"` gives one row per <header>, which \
             changes the row count"
        );
    }
}
