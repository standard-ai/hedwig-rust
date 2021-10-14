/// A message queue topic name to which messages can be published
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Topic(&'static str);

impl std::fmt::Display for Topic {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        std::fmt::Display::fmt(self.0, f)
    }
}

impl From<&'static str> for Topic {
    fn from(s: &'static str) -> Topic {
        Topic(s)
    }
}

impl AsRef<str> for Topic {
    fn as_ref(&self) -> &str {
        &self.0
    }
}
