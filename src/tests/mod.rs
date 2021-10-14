#![cfg(test)]

pub(crate) mod google;
pub(crate) mod json;

pub(crate) fn assert_error<T: std::error::Error + Send + Sync + 'static>() {}
pub(crate) fn assert_send_val<T: Send>(_: &T) {}
