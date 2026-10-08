//! Pageview count loading from the monthly dump.
//!
//! The dump reader (`dump_reader`) is designed to be self-contained with
//! no MySQL dependency, making it easy to extract into a standalone library.

pub mod dump_reader;
