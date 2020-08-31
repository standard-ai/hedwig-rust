//! Implementations of validators.
//!
//! Validators are responsible for ensuring the message payload is valid according some description
//! and then constructing instances of [`ValidatedMessage`] that contain the encoded data in some
//! on-wire format.
//!
//! [`ValidatedMessage`]: crate::ValidatedMessage

#[cfg(feature = "json-schema")]
mod json_schema;
#[cfg(feature = "json-schema")]
pub use self::json_schema::*;

#[cfg(feature = "prost")]
mod prost;
#[cfg(feature = "prost")]
pub use self::prost::*;
// #[cfg_attr(docsrs, doc(cfg(feature = "prost")))]
// pub use self::prost::ProstValidator;
