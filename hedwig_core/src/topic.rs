use smallstr::SmallString;

/// A message queue topic name to which messages can be published
// A survey of common topics found lengths between 16 and 35 bytes
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Topic(SmallString<[u8; 36]>);

impl Default for Topic {
    fn default() -> Self {
        Topic(SmallString::new())
    }
}

impl std::fmt::Display for Topic {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        std::fmt::Display::fmt(self.0.as_str(), f)
    }
}

impl<'a> From<&'a str> for Topic {
    fn from(s: &'a str) -> Topic {
        Topic(s.into())
    }
}

impl From<String> for Topic {
    fn from(s: String) -> Topic {
        Topic(s.into())
    }
}

impl AsRef<str> for Topic {
    fn as_ref(&self) -> &str {
        self.0.as_ref()
    }
}
