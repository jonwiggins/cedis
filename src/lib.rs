//! # Cedis
//!
//! A Redis-compatible in-memory data store built from scratch in Rust.
//!
//! Cedis speaks the RESP2 protocol and implements Redis's core data structures
//! (strings, lists, hashes, sets, sorted sets) along with key expiration,
//! transactions, and a configurable server — all without using any existing
//! Redis or RESP libraries.

pub mod command;
pub mod config;
pub mod connection;
pub mod error;
pub mod glob;
pub mod resp;
pub mod server;
pub mod store;
pub mod types;
