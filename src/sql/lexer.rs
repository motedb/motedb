/// SQL Lexer - converts SQL string into tokens
use super::token::{Token, TokenType};
use crate::error::{MoteDBError, Result};

pub struct Lexer<'a> {
    input: &'a str,
    bytes: &'a [u8],
    position: usize,
    line: usize,
    column: usize,
}

impl<'a> Lexer<'a> {
    pub fn new(input: &'a str) -> Self {
        Self {
            input,
            bytes: input.as_bytes(),
            position: 0,
            line: 1,
            column: 1,
        }
    }

    pub fn tokenize(&mut self) -> Result<Vec<Token>> {
        // 🚀 P1.2: Pre-allocate tokens based on input size
        let estimated_tokens = self.input.len() / 4 + 10;
        let mut tokens = Vec::with_capacity(estimated_tokens);

        loop {
            let token = self.next_token()?;
            let is_eof = matches!(token.token_type, TokenType::Eof);
            tokens.push(token);
            if is_eof {
                break;
            }
        }

        Ok(tokens)
    }

    pub fn next_token(&mut self) -> Result<Token> {
        self.skip_whitespace();

        let line = self.line;
        let column = self.column;

        if self.is_eof() {
            return Ok(Token::new(TokenType::Eof, line, column));
        }

        let ch = self.current_char();

        // Skip comments
        if ch == '-' && self.peek_char() == Some('-') {
            self.skip_line_comment();
            return self.next_token();
        }

        if ch == '/' && self.peek_char() == Some('*') {
            self.skip_block_comment()?;
            return self.next_token();
        }

        let token_type = match ch {
            // String literals
            '\'' | '"' => self.read_string(ch)?,

            // Numbers
            '0'..='9' => self.read_number()?,

            // Identifiers and keywords
            'a'..='z' | 'A'..='Z' | '_' => self.read_identifier()?,

            // Operators and delimiters
            '=' => {
                self.advance();
                TokenType::Eq
            }
            '!' => {
                self.advance();
                if self.current_char() == '=' {
                    self.advance();
                    TokenType::Ne
                } else {
                    return Err(MoteDBError::ParseError(format!(
                        "Unexpected character '!' at {}:{}",
                        line, column
                    )));
                }
            }
            '<' => {
                self.advance();
                if self.current_char() == '=' {
                    self.advance();
                    // Check for <=> (cosine distance)
                    if self.current_char() == '>' {
                        self.advance();
                        TokenType::CosineDistance
                    } else {
                        TokenType::Le
                    }
                } else if self.current_char() == '>' {
                    self.advance();
                    TokenType::Ne
                } else if self.current_char() == '-' {
                    self.advance();
                    // Check for <-> (L2 distance)
                    if self.current_char() == '>' {
                        self.advance();
                        TokenType::L2Distance
                    } else {
                        return Err(MoteDBError::ParseError(format!(
                            "Unexpected sequence '<-' at {}:{}",
                            line, column
                        )));
                    }
                } else if self.current_char() == '#' {
                    self.advance();
                    // Check for <#> (dot product)
                    if self.current_char() == '>' {
                        self.advance();
                        TokenType::DotProduct
                    } else {
                        return Err(MoteDBError::ParseError(format!(
                            "Unexpected sequence '<#' at {}:{}",
                            line, column
                        )));
                    }
                } else {
                    TokenType::Lt
                }
            }
            '>' => {
                self.advance();
                if self.current_char() == '=' {
                    self.advance();
                    TokenType::Ge
                } else {
                    TokenType::Gt
                }
            }
            '+' => {
                self.advance();
                TokenType::Plus
            }
            '-' => {
                self.advance();
                TokenType::Minus
            }
            '*' => {
                self.advance();
                TokenType::Star
            }
            '/' => {
                self.advance();
                TokenType::Slash
            }
            '%' => {
                self.advance();
                TokenType::Percent
            }
            '(' => {
                self.advance();
                TokenType::LParen
            }
            ')' => {
                self.advance();
                TokenType::RParen
            }
            '[' => {
                self.advance();
                TokenType::LBracket
            }
            ']' => {
                self.advance();
                TokenType::RBracket
            }
            ',' => {
                self.advance();
                TokenType::Comma
            }
            ';' => {
                self.advance();
                TokenType::Semicolon
            }
            '.' => {
                self.advance();
                TokenType::Dot
            }
            '?' => {
                self.advance();
                // Check for ?N (numbered parameter like ?1, ?2)
                if !self.is_eof() && self.current_char().is_ascii_digit() {
                    let mut num = String::new();
                    while !self.is_eof() && self.current_char().is_ascii_digit() {
                        num.push(self.current_char());
                        self.advance();
                    }
                    let idx: usize = num.parse().unwrap_or(1);
                    TokenType::Parameter(idx)
                } else {
                    // Unnamed ? — gets sequential number resolved later
                    TokenType::Parameter(0) // 0 = auto-assign
                }
            }
            _ => {
                return Err(MoteDBError::ParseError(format!(
                    "Unexpected character '{}' at {}:{}",
                    ch, line, column
                )));
            }
        };

        Ok(Token::new(token_type, line, column))
    }

    fn current_char(&self) -> char {
        if self.is_eof() {
            '\0'
        } else {
            // 🚀 P1.1: Direct byte access (O(1))
            self.bytes[self.position] as char
        }
    }

    /// Decode the current UTF-8 character (handles multi-byte sequences correctly).
    fn current_utf8_char(&self) -> char {
        if self.is_eof() {
            return '\0';
        }
        // Decode from the raw byte slice at `position`. We must NOT slice
        // `self.input[..]` (a &str) by a byte index — if `position` lands in
        // the middle of a multi-byte UTF-8 sequence (which happens whenever a
        // caller advances past a non-ASCII byte one byte at a time), str
        // slicing panics. Decoding from bytes is panic-free: a lone
        // continuation byte decodes to a replacement char, which is harmless
        // for lexing (it just yields an Unexpected-token error).
        let rest = &self.bytes[self.position..];
        match std::str::from_utf8(rest) {
            Ok(s) => s.chars().next().unwrap_or('\0'),
            Err(_) => {
                // `rest` starts with invalid UTF-8. If the first byte is a
                // leading byte of a valid sequence that's simply truncated,
                // decode just that byte's expected length; otherwise treat the
                // single byte as a byte char.
                let b = rest[0];
                // Decode the first code point from up to 4 bytes.
                decode_first_codepoint(rest).unwrap_or(b as char)
            }
        }
    }

    /// Advance past the current UTF-8 character (1-4 bytes).
    fn advance_utf8(&mut self) {
        if self.is_eof() {
            return;
        }
        // Compute the UTF-8 length of the code point starting at `position`
        // WITHOUT slicing self.input (which panics on intra-character byte
        // offsets). Decode from the byte slice.
        let rest = &self.bytes[self.position..];
        let char_len = decode_first_codepoint(rest)
            .map(|c| c.len_utf8())
            .unwrap_or(1);
        for _ in 0..char_len {
            self.advance();
        }
    }

    fn peek_char(&self) -> Option<char> {
        if self.position + 1 < self.bytes.len() {
            Some(self.bytes[self.position + 1] as char)
        } else {
            None
        }
    }

    fn advance(&mut self) {
        if !self.is_eof() {
            if self.bytes[self.position] == b'\n' {
                self.line += 1;
                self.column = 1;
            } else {
                self.column += 1;
            }
            self.position += 1;
        }
    }

    fn is_eof(&self) -> bool {
        self.position >= self.bytes.len()
    }

    fn skip_whitespace(&mut self) {
        while !self.is_eof() && self.current_char().is_whitespace() {
            self.advance();
        }
    }

    fn skip_line_comment(&mut self) {
        while !self.is_eof() && self.current_char() != '\n' {
            self.advance();
        }
        if !self.is_eof() {
            self.advance(); // skip newline
        }
    }

    fn skip_block_comment(&mut self) -> Result<()> {
        self.advance(); // skip '/'
        self.advance(); // skip '*'

        while !self.is_eof() {
            if self.current_char() == '*' && self.peek_char() == Some('/') {
                self.advance(); // skip '*'
                self.advance(); // skip '/'
                return Ok(());
            }
            self.advance();
        }

        Err(MoteDBError::ParseError(
            "Unterminated block comment".to_string(),
        ))
    }

    fn read_string(&mut self, quote: char) -> Result<TokenType> {
        self.advance(); // skip opening quote
        let mut value = String::with_capacity(32);
        const MAX_STRING_LEN: usize = 16 * 1024 * 1024; // 16 MiB limit prevents OOM

        while !self.is_eof() {
            if value.len() >= MAX_STRING_LEN {
                return Err(MoteDBError::ParseError(
                    "String literal exceeds maximum length (16 MiB)".to_string(),
                ));
            }
            let ch = self.current_utf8_char();

            if ch == quote {
                // SQL standard: doubled quote escapes the quote ('it''s' → it's)
                // Check if next char is also a quote
                let after_quote = self.position + 1;
                if after_quote < self.bytes.len() && self.bytes[after_quote] == quote as u8 {
                    value.push(quote);
                    self.advance(); // skip first quote
                    self.advance(); // skip second quote
                    continue;
                }
                // Single quote = end of string
                break;
            }

            if ch == '\\' {
                self.advance();
                if self.is_eof() {
                    return Err(MoteDBError::ParseError("Unterminated string".to_string()));
                }
                let escaped = match self.current_char() {
                    'n' => '\n',
                    't' => '\t',
                    'r' => '\r',
                    '\\' => '\\',
                    '\'' => '\'',
                    '"' => '"',
                    c => c,
                };
                value.push(escaped);
                self.advance();
            } else {
                value.push(ch);
                self.advance_utf8();
            }
        }

        if self.is_eof() {
            return Err(MoteDBError::ParseError("Unterminated string".to_string()));
        }

        self.advance(); // skip closing quote
        Ok(TokenType::String(value))
    }

    fn read_number(&mut self) -> Result<TokenType> {
        let mut value = String::with_capacity(16);

        while !self.is_eof() && (self.current_char().is_numeric() || self.current_char() == '.') {
            value.push(self.current_char());
            self.advance();
        }

        // Handle scientific notation (e.g., 1.5e10)
        if !self.is_eof() && (self.current_char() == 'e' || self.current_char() == 'E') {
            value.push(self.current_char());
            self.advance();
            if !self.is_eof() && (self.current_char() == '+' || self.current_char() == '-') {
                value.push(self.current_char());
                self.advance();
            }
            while !self.is_eof() && self.current_char().is_numeric() {
                value.push(self.current_char());
                self.advance();
            }
        }

        let num = value
            .parse::<f64>()
            .map_err(|_| MoteDBError::ParseError(format!("Invalid number: {}", value)))?;
        if num.is_infinite() || num.is_nan() {
            return Err(MoteDBError::ParseError(format!(
                "Number out of range: {}",
                value
            )));
        }
        // 🔑 Pure integers: prefer exact i64, then i128, to avoid f64 precision
        // loss at the i64 boundary (i64::MAX and 2^63 are the same f64). This
        // matters for `-9223372036854775808` (i64::MIN): as f64 it equals 2^63,
        // which the parser clamps to i64::MAX, and negating gives i64::MIN+1.
        // By keeping the exact i128, the parser can fold -2^63 → i64::MIN.
        let is_pure_int = !value.contains('.') && !value.contains('e') && !value.contains('E');
        if is_pure_int {
            if let Ok(i) = value.parse::<i64>() {
                return Ok(TokenType::Number(i as f64));
            }
            if let Ok(big) = value.parse::<i128>() {
                return Ok(TokenType::OverflowInteger(big));
            }
        }
        Ok(TokenType::Number(num))
    }

    fn read_identifier(&mut self) -> Result<TokenType> {
        let start = self.position;

        while !self.is_eof() {
            let ch = self.current_utf8_char();
            if ch.is_alphanumeric() || ch == '_' {
                self.advance_utf8();
            } else {
                break;
            }
        }

        let word = &self.input[start..self.position];

        // Guard against DoS via extremely long identifiers (4KB limit)
        if word.len() > 4096 {
            return Err(MoteDBError::ParseError("Identifier too long".into()));
        }

        // Zero-allocation keyword check (from_keyword uses stack buffer)
        Ok(
            TokenType::from_keyword(word)
                .unwrap_or_else(|| TokenType::Identifier(word.to_string())),
        )
    }
}

/// Decode the first Unicode code point from the start of `bytes`, tolerating
/// invalid/truncated UTF-8. Used by the lexer to inspect the current character
/// WITHOUT slicing a `&str` (which would panic if the byte offset lands inside
/// a multi-byte sequence). Returns `None` only if `bytes` is empty.
///
/// For valid UTF-8 this matches `str::chars().next()`. For invalid sequences
/// (lone continuation bytes, truncated leaders) it falls back to the first
/// byte as a `char` — the lexer then yields an Unexpected-token error, which
/// is the correct behavior for malformed input.
fn decode_first_codepoint(bytes: &[u8]) -> Option<char> {
    if bytes.is_empty() {
        return None;
    }
    let b0 = bytes[0];
    // ASCII fast path.
    if b0 < 0x80 {
        return Some(b0 as char);
    }
    // Determine expected length from the leading byte.
    let len = if b0 & 0xE0 == 0xC0 {
        2
    } else if b0 & 0xF0 == 0xE0 {
        3
    } else if b0 & 0xF8 == 0xF0 {
        4
    } else {
        // Lone continuation byte (10xxxxxx) or invalid leader.
        return Some(b0 as char);
    };
    if bytes.len() < len {
        // Truncated sequence — not enough bytes. Decode what we have as a
        // single byte so the lexer advances one byte and recovers.
        return Some(b0 as char);
    }
    // Try to decode the full sequence; if any continuation byte is invalid,
    // fall back to the leading byte.
    let mut code = (b0 as u32)
        & match len {
            2 => 0x1F,
            3 => 0x0F,
            4 => 0x07,
            _ => 0x7F,
        };
    for i in 1..len {
        let b = bytes[i];
        if b & 0xC0 != 0x80 {
            return Some(b0 as char);
        }
        code = (code << 6) | (b as u32 & 0x3F);
    }
    char::from_u32(code).or(Some(b0 as char))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lexer_simple_select() {
        let mut lexer = Lexer::new("SELECT * FROM users");
        let tokens = lexer.tokenize().unwrap();

        assert_eq!(tokens.len(), 5); // SELECT, *, FROM, users, EOF
        assert!(matches!(tokens[0].token_type, TokenType::Select));
        assert!(matches!(tokens[1].token_type, TokenType::Star));
        assert!(matches!(tokens[2].token_type, TokenType::From));
        assert!(matches!(tokens[3].token_type, TokenType::Identifier(_)));
        assert!(matches!(tokens[4].token_type, TokenType::Eof));
    }

    /// Regression: the lexer must not panic on bytes that contain multi-byte
    /// UTF-8 characters. Found by the fuzz_sql_parser target (the byte sequence
    /// `]]'Ü\©\g` crashed current_utf8_char via a str slice on an
    /// intra-character byte offset). Tokenizing may succeed or return an error,
    /// but it must never panic.
    #[test]
    fn test_lexer_multibyte_utf8_no_panic() {
        // The exact crashing input from the fuzzer.
        let crashing: &[u8] = &[93, 93, 39, 220, 92, 169, 92, 103];
        let sql = String::from_utf8_lossy(crashing);
        let mut lexer = Lexer::new(&sql);
        // Must not panic — we ignore the result (Ok or Err both fine).
        let _ = lexer.tokenize();

        // A few more multi-byte sequences that previously risked panicking
        // when position landed inside a code point.
        for s in &["café", "日本語", "a\0b", "表情🎓", "\u{1F916}robot"] {
            let _ = Lexer::new(s).tokenize();
        }
    }

    #[test]
    fn test_lexer_with_where() {
        let mut lexer = Lexer::new("SELECT id FROM users WHERE age > 18");
        let tokens = lexer.tokenize().unwrap();

        // SELECT, id, FROM, users, WHERE, age, >, 18, EOF
        assert_eq!(tokens.len(), 9);
        assert!(matches!(tokens[5].token_type, TokenType::Identifier(_)));
        assert!(matches!(tokens[6].token_type, TokenType::Gt));
        assert!(matches!(tokens[7].token_type, TokenType::Number(_)));
    }

    #[test]
    fn test_lexer_string_literal() {
        let mut lexer = Lexer::new("SELECT * FROM users WHERE name = 'John'");
        let tokens = lexer.tokenize().unwrap();

        // SELECT, *, FROM, users, WHERE, name, =, 'John', EOF
        // Index: 0,1,2,3,4,5,6,7,8
        assert!(matches!(tokens[7].token_type, TokenType::String(ref s) if s == "John"));
    }

    #[test]
    fn test_lexer_operators() {
        let mut lexer = Lexer::new("= != < > <= >= + - * /");
        let tokens = lexer.tokenize().unwrap();

        assert!(matches!(tokens[0].token_type, TokenType::Eq));
        assert!(matches!(tokens[1].token_type, TokenType::Ne));
        assert!(matches!(tokens[2].token_type, TokenType::Lt));
        assert!(matches!(tokens[3].token_type, TokenType::Gt));
        assert!(matches!(tokens[4].token_type, TokenType::Le));
        assert!(matches!(tokens[5].token_type, TokenType::Ge));
    }

    #[test]
    fn test_lexer_comment() {
        let mut lexer = Lexer::new("SELECT * -- this is a comment\nFROM users");
        let tokens = lexer.tokenize().unwrap();

        assert_eq!(tokens.len(), 5); // Comment should be skipped
        assert!(matches!(tokens[2].token_type, TokenType::From));
    }
}
