//! Fuzz target: the SQL lexer + parser pipeline.
//!
//! The parser (`src/sql/parser.rs`, ~1780 lines) is a hand-written recursive
//! descent parser. Historically it had a stack-overflow on deeply nested
//! expressions (fixed by lowering MAX_RECURSION_DEPTH to 64). This target
//! throws arbitrary bytes at the lexer → parser and asserts:
//!   - no panic / abort / stack overflow (the main risk for a hand-written
//!     parser on untrusted input)
//!   - the parser either returns Ok(Statement) or Err, never unwinds.
//!
//! We deliberately fuzz ONLY lex+parse here (not execute) so a finding points
//! cleanly at the parser rather than the executor. The full parse→execute path
//! is covered by the regular test suite; this target focuses on robustness of
//! the untrusted-input boundary.

#![no_main]

use libfuzzer_sys::fuzz_target;
use motedb::sql::{Lexer, Parser};

fuzz_target!(|data: &[u8]| {
    // The lexer/parser operate on &str; treat the bytes as UTF-8 (lossy). A
    // fuzzer corpus will evolve valid-ish SQL over time, but we must not crash
    // on arbitrary bytes — the parser must reject them cleanly.
    let sql = String::from_utf8_lossy(data);

    // Stage 1: lex. tokenize() must not panic on arbitrary input.
    let tokens = match Lexer::new(&sql).tokenize() {
        Ok(t) => t,
        Err(_) => return, // a lex error is an acceptable outcome
    };

    // Stage 2: parse. Must not panic, stack-overflow, or abort — even on
    // pathological nesting. The depth guard (MAX_RECURSION_DEPTH = 64) bounds
    // the recursion; this target verifies that bound holds under fuzzing.
    let _ = Parser::new(tokens).parse();
});
