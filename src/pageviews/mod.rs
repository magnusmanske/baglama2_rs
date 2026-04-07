//! Pageview count loading — dump-based bulk path and per-page API fallback.
//!
//! The dump reader (`dump_reader`) is designed to be self-contained with
//! no MySQL dependency, making it easy to extract into a standalone library.

pub mod api_fallback;
pub mod dump_reader;
