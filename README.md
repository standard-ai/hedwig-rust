# Hedwig library for Rust

[![Build Status](https://travis-ci.com/standard-ai/hedwig-rust.svg?branch=master)](https://travis-ci.com/standard-ai/hedwig-rust)
[![Latest Version](https://img.shields.io/crates/v/hedwig.svg?style=flat-square)](https://crates.io/crates/hedwig)
[![Docs](https://docs.rs/hedwig/badge.svg)](https://docs.rs/hedwig)

## What is it?

Hedwig is a inter-service communication bus that works on AWS and GCP, while keeping things pretty
simple and straight forward. It uses [json schema](https://json-schema.org/) [draft
v4](https://json-schema.org/specification-links.html#draft-4) for schema validation so all incoming
and outgoing messages are validated against pre-defined schema.

Hedwig allows separation of concerns between consumers and publishers so your services are loosely
coupled, and the contract is enforced by the schema validation. Hedwig may also be used to build
asynchronous APIs.

Support exists for [Python](https://github.com/Automatic/hedwig-python) and
[Golang](https://github.com/Automatic/hedwig-go).

For intra-service messaging, see [Taskhawk](https://github.com/Automatic/taskhawk-python).

## Quick Start

### Installation

Add to Cargo.toml:

```toml
[dependencies]
hedwig = "1"
```

To use the Google Publisher, enable the `google` feature as well:

```toml
[dependencies]
hedwig = { version = "1", features = ["google"] }
```

### Usage

See [the examples](https://github.com/standard-ai/hedwig-rust/tree/master/examples) in the
repository as well as [the crate-level documentation](https://docs.rs/hedwig/) for
usage examples.

## Getting Help

We use GitHub issues for tracking bugs and feature requests.

* If it turns out that you may have found a bug, please [open an
issue](https://github.com/standard-ai/hedwig-rust/issues/new)
