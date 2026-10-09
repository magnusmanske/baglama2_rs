//! The two names this tool has for a wiki: its database name (`enwiki`, as
//! in `globalimagelinks.gil_wiki`, the `sites` table and gzb files) and its
//! code in the pageview dumps (`en.wikipedia`). Separate types, so one cannot
//! be used for the other: a mix-up compiles fine with strings and silently
//! matches no views.

use anyhow::{anyhow, Result};
use std::borrow::Borrow;
use std::fmt;
use std::str::FromStr;

/// A wiki's database name, e.g. `enwiki` or `zh_min_nanwiki`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Dbname(String);

impl Dbname {
    /// Lower-case ASCII letters, digits and underscores, as MediaWiki uses.
    pub fn parse(s: &str) -> Result<Self> {
        if s.is_empty()
            || !s
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
        {
            return Err(anyhow!("not a wiki database name: {s:?}"));
        }
        Ok(Self(s.to_string()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Lets a map keyed by `Dbname` be searched with a `&str` read from a file,
/// without parsing (and allocating) for every line.
impl Borrow<str> for Dbname {
    fn borrow(&self) -> &str {
        &self.0
    }
}

impl FromStr for Dbname {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        Self::parse(s)
    }
}

impl fmt::Display for Dbname {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// A wiki's code in the pageview dumps: its host name without `.org` and
/// without a leading `www.`, e.g. `en.wikipedia`, `commons.wikimedia`,
/// `wikidata`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct DumpCode(String);

impl DumpCode {
    /// Lower-case ASCII letters, digits, dots and hyphens.
    pub fn parse(s: &str) -> Result<Self> {
        if s.is_empty()
            || !s
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'.' || b == b'-')
        {
            return Err(anyhow!("not a pageview dump code: {s:?}"));
        }
        Ok(Self(s.to_string()))
    }

    /// From a server URL or host name: `https://www.wikidata.org` →
    /// `wikidata`. `None` for hosts outside `.org`.
    pub fn from_server(url: &str) -> Option<Self> {
        let host = url.split("://").last()?.trim_end_matches('/');
        let host = host.strip_prefix("www.").unwrap_or(host);
        Self::parse(host.strip_suffix(".org")?).ok()
    }

    /// The bytes as they appear in the dump; page keys are built from these.
    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

impl fmt::Display for DumpCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_dbname() {
        for ok in [
            "enwiki",
            "zh_min_nanwiki",
            "be_x_oldwiki",
            "wikimania2014wiki",
        ] {
            assert_eq!(Dbname::parse(ok).unwrap().as_str(), ok);
        }
        for bad in ["", "en.wikipedia", "EnWiki", "en-wiki", "enwiki ", "énwiki"] {
            assert!(Dbname::parse(bad).is_err(), "{bad:?}");
        }
        // Searchable by &str.
        let map: HashMap<Dbname, u8> = [(Dbname::parse("enwiki").unwrap(), 1)].into();
        assert_eq!(map.get("enwiki"), Some(&1));
    }

    #[test]
    fn test_dump_code_from_server() {
        let code = |url: &str| DumpCode::from_server(url).map(|c| c.to_string());
        assert_eq!(
            code("https://en.wikipedia.org").as_deref(),
            Some("en.wikipedia")
        );
        assert_eq!(
            code("https://en.wikipedia.org/").as_deref(),
            Some("en.wikipedia")
        );
        assert_eq!(code("en.wikipedia.org").as_deref(), Some("en.wikipedia"));
        assert_eq!(
            code("https://www.wikidata.org").as_deref(),
            Some("wikidata")
        );
        assert_eq!(
            code("https://zh-min-nan.wiktionary.org").as_deref(),
            Some("zh-min-nan.wiktionary")
        );
        assert_eq!(
            code("https://commons.wikimedia.org").as_deref(),
            Some("commons.wikimedia")
        );
        assert_eq!(code("https://example.com"), None);
        assert_eq!(code("https://.org"), None);
    }

    #[test]
    fn test_dump_code_bytes() {
        assert_eq!(
            DumpCode::parse("en.wikipedia").unwrap().as_bytes(),
            b"en.wikipedia"
        );
        assert!(DumpCode::parse("en_wikipedia").is_err());
        assert!(DumpCode::parse("").is_err());
    }
}
