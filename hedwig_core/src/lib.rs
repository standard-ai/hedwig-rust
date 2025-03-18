//! The core set of traits and types used in the hedwig format.
//!
//! This crate aims to provide better version stability over the primary batteries-included
//! `hedwig` crate. Top-level applications should typically use `hedwig`, while crates that define
//! message types should use `hedwig_core`

mod topic;
pub use topic::Topic;
pub mod message;

/// Custom headers associated with a message.
pub type Headers = std::collections::BTreeMap<String, String>;

/// A validated message.
pub type ValidatedMessage = message::ValidatedMessage<bytes::Bytes>;

