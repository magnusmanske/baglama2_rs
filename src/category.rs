//! Commons category titles, and walking a category tree.

use anyhow::{anyhow, Result};
use std::collections::HashSet;
use std::fmt;
use std::future::Future;
use std::str::FromStr;

/// Characters MediaWiki does not allow in page titles.
const ILLEGAL: &[char] = &['#', '<', '>', '[', ']', '|', '{', '}'];

/// A Commons category title without the `Category:` prefix, in the form
/// MediaWiki uses: single spaces, none leading or trailing, first letter in
/// upper case. Only [`CategoryTitle::parse`] builds one, so two spellings of
/// a category compare equal and an illegal title cannot exist.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct CategoryTitle(String);

impl CategoryTitle {
    /// Accepts a title as people and databases write it: spaces or
    /// underscores, runs of either, with or without a `Category:` prefix.
    pub fn parse(s: &str) -> Result<Self> {
        // Before collapsing whitespace, which would hide a newline or tab.
        if let Some(c) = s.chars().find(|c| ILLEGAL.contains(c) || c.is_control()) {
            return Err(anyhow!("{c:?} is not allowed in a category title: {s:?}"));
        }
        let spaced = s.replace('_', " ");
        let collapsed = spaced.split_whitespace().collect::<Vec<_>>().join(" ");
        let name = match collapsed.get(..9) {
            Some(prefix) if prefix.eq_ignore_ascii_case("category:") => collapsed[9..].trim_start(),
            _ => collapsed.as_str(),
        };
        let mut chars = name.chars();
        let first = chars
            .next()
            .ok_or_else(|| anyhow!("empty category title: {s:?}"))?;
        Ok(Self(first.to_uppercase().chain(chars).collect()))
    }

    /// With underscores, as in `page_title` and `lt_title`.
    pub fn db_key(&self) -> String {
        self.0.replace(' ', "_")
    }
}

impl FromStr for CategoryTitle {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        Self::parse(s)
    }
}

impl fmt::Display for CategoryTitle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// The DB keys of a category tree: `root` plus `depth` levels of
/// subcategories, or all levels if `depth` is negative. So depth 0 is the
/// category alone. The same rule as `ToolforgeCommon::findSubcats`, which
/// the legacy pipelines used. `subcats` returns the subcategories (DB keys)
/// of a set of categories.
pub async fn category_tree<F, Fut>(
    root: &CategoryTitle,
    depth: isize,
    mut subcats: F,
) -> Result<Vec<String>>
where
    F: FnMut(Vec<String>) -> Fut,
    Fut: Future<Output = Result<Vec<String>>>,
{
    let mut seen: HashSet<String> = HashSet::new();
    let mut level = vec![root.db_key()];
    let mut depth = depth;
    loop {
        // Only categories not seen yet: trees can have cycles.
        level.retain(|category| seen.insert(category.clone()));
        if level.is_empty() || depth == 0 {
            break;
        }
        level = subcats(level).await?;
        depth -= 1;
    }
    let mut ret: Vec<String> = seen.into_iter().collect();
    ret.sort();
    Ok(ret)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn title(s: &str) -> String {
        CategoryTitle::parse(s).unwrap().to_string()
    }

    #[test]
    fn test_parse_normalizes() {
        assert_eq!(title("Images from NASA"), "Images from NASA");
        assert_eq!(title("Images_from_NASA"), "Images from NASA");
        assert_eq!(title("  Image  Files "), "Image Files");
        assert_eq!(title("Category:Foo_bar"), "Foo bar");
        assert_eq!(title("category: Foo"), "Foo");
        assert_eq!(title("CATEGORY:foo"), "Foo");
        assert_eq!(title("élan"), "Élan");
        assert_eq!(title("A\u{00A0}b"), "A b");
        assert_eq!(
            title("Fondazione Torino Musei - Fondo Gabinio"),
            "Fondazione Torino Musei - Fondo Gabinio"
        );
        // A colon elsewhere is part of the title.
        assert_eq!(
            title("Media contributed by Tekniska museet: 2020-02"),
            "Media contributed by Tekniska museet: 2020-02"
        );
        assert_eq!(
            CategoryTitle::parse("Image  Files").unwrap(),
            CategoryTitle::parse("Image_Files").unwrap()
        );
        assert_eq!(CategoryTitle::parse("Foo bar").unwrap().db_key(), "Foo_bar");
    }

    #[test]
    fn test_parse_rejects() {
        for bad in [
            "",
            "   ",
            "Category:",
            "_",
            "A#b",
            "A|b",
            "A[b]",
            "A{b}",
            "A<b>",
            "A\nb",
        ] {
            assert!(CategoryTitle::parse(bad).is_err(), "{bad:?}");
        }
    }

    /// A → B, C; B → D; D → E; C → A (a cycle).
    async fn tree(depth: isize) -> Vec<String> {
        let children: HashMap<&str, Vec<&str>> = [
            ("A", vec!["B", "C"]),
            ("B", vec!["D"]),
            ("C", vec!["A"]),
            ("D", vec!["E"]),
        ]
        .into_iter()
        .collect();
        let mut queries = 0;
        let ret = category_tree(&CategoryTitle::parse("A").unwrap(), depth, |cats| {
            queries += 1;
            assert!(queries < 10, "no end");
            let found = cats
                .iter()
                .flat_map(|c| children.get(c.as_str()).cloned().unwrap_or_default())
                .map(String::from)
                .collect();
            async move { Ok(found) }
        })
        .await
        .unwrap();
        ret
    }

    #[tokio::test]
    async fn test_category_tree_depth() {
        assert_eq!(tree(0).await, ["A"]);
        assert_eq!(tree(1).await, ["A", "B", "C"]);
        assert_eq!(tree(2).await, ["A", "B", "C", "D"]);
        assert_eq!(tree(3).await, ["A", "B", "C", "D", "E"]);
        assert_eq!(tree(9).await, ["A", "B", "C", "D", "E"]);
        assert_eq!(tree(-1).await, ["A", "B", "C", "D", "E"]);
    }
}
