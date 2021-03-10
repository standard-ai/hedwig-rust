//! [`Publisher`](crate::publish::Publisher) implementations.

mod null;

// unreachable_pub doesn't work through glob imports.
// see https://github.com/rust-lang/rust/blob/0148b971c921a0831fbf3357e5936eec724e3566/compiler/rustc_privacy/src/lib.rs#L590
#[allow(unreachable_pub)]
pub use null::*;

mod mock;
#[allow(unreachable_pub)]
pub use mock::*;

#[cfg(feature = "google")]
mod googlepubsub;
#[cfg(feature = "google")]
#[allow(unreachable_pub)]
pub use googlepubsub::*;
