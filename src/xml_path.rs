use std::fmt;
use string_cache::DefaultAtom as Atom;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct XmlPath {
    parts: Vec<Atom>,
}

impl XmlPath {
    pub fn new(path_str: &str) -> Self {
        Self {
            parts: path_str
                .trim_start_matches('/')
                .split('/')
                .filter(|s| !s.is_empty())
                .map(Atom::from)
                .collect(),
        }
    }

    pub fn append_node(&mut self, node: &str) {
        self.parts.push(Atom::from(node));
    }

    pub fn remove_node(&mut self) -> Option<Atom> {
        self.parts.pop()
    }
}

impl fmt::Display for XmlPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "/{}",
            self.parts
                .iter()
                .map(|atom| atom.as_ref())
                .collect::<Vec<_>>()
                .join("/")
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    use crate::xml_path::XmlPath;

    #[rstest]
    #[case("/root/node1", vec!["root", "node1"])]
    #[case("/path/with/empty/segments//more/nodes", vec!["path", "with", "empty", "segments", "more", "nodes"])]
    #[case("/library/books/book/@id", vec!["library", "books", "book", "@id"])]
    #[case("", vec![])]
    fn test_new_from_string(#[case] path_str: &str, #[case] expected_parts: Vec<&str>) {
        let path = XmlPath::new(path_str);
        assert_eq!(
            path.parts
                .iter()
                .map(|atom| atom.as_ref())
                .collect::<Vec<_>>(),
            expected_parts
        );
    }

    #[rstest]
    #[case("/root/node1", "new_node", "/root/node1/new_node")]
    #[case("/path/with/segments", "new_node", "/path/with/segments/new_node")]
    #[case("/library", "@id", "/library/@id")]
    #[case("", "new_node", "/new_node")]
    fn test_append_node(
        #[case] path_str: &str,
        #[case] new_node: &str,
        #[case] expected_path: &str,
    ) {
        let mut path = XmlPath::new(path_str);
        path.append_node(new_node);
        assert_eq!(path.to_string(), expected_path);
    }

    #[rstest]
    #[case("/root/node1/node2", "/root/node1", Some(Atom::from("node2")))]
    #[case("/path/only", "/path", Some(Atom::from("only")))]
    #[case("/library/@id", "/library", Some(Atom::from("@id")))]
    #[case("", "/", None)]
    fn test_remove_node(
        #[case] path_str: &str,
        #[case] expected_path: &str,
        #[case] expected_removed: Option<Atom>,
    ) {
        let mut path = XmlPath::new(path_str);
        let removed = path.remove_node();
        assert_eq!(path.to_string(), expected_path); // Use string comparison
        assert_eq!(removed, expected_removed);
    }
}
