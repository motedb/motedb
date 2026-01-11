//! Text data type implementation

use serde::{Deserialize, Serialize};

/// Text data type for full-text search
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Text {
    /// Raw text content
    content: String,
}

impl Text {
    /// Create a new text instance
    pub fn new(content: String) -> Self {
        Self { content }
    }

    /// Get text content
    pub fn content(&self) -> &str {
        &self.content
    }

    /// Tokenize text into words (simple whitespace split)
    pub fn tokenize(&self) -> Vec<String> {
        self.content
            .split_whitespace()
            .map(|s| s.to_lowercase())
            .collect()
    }

    /// Check if text contains a substring (case-insensitive)
    pub fn contains(&self, query: &str) -> bool {
        self.content.to_lowercase().contains(&query.to_lowercase())
    }

    /// Get text length in bytes
    pub fn len(&self) -> usize {
        self.content.len()
    }

    /// Check if text is empty
    pub fn is_empty(&self) -> bool {
        self.content.is_empty()
    }
}

impl From<String> for Text {
    fn from(s: String) -> Self {
        Self::new(s)
    }
}

impl From<&str> for Text {
    fn from(s: &str) -> Self {
        Self::new(s.to_string())
    }
}

/// Text document type (alias for compatibility)
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct TextDoc {
    pub content: String,
}

impl From<String> for TextDoc {
    fn from(s: String) -> Self {
        Self { content: s }
    }
}

impl From<&str> for TextDoc {
    fn from(s: &str) -> Self {
        Self { content: s.to_string() }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_text_creation() {
        let text = Text::new("Hello World".to_string());
        assert_eq!(text.content(), "Hello World");
    }

    #[test]
    fn test_tokenize() {
        let text = Text::new("The quick brown fox".to_string());
        let tokens = text.tokenize();
        assert_eq!(tokens, vec!["the", "quick", "brown", "fox"]);
    }

    #[test]
    fn test_contains() {
        let text = Text::new("MoteDB Storage Engine".to_string());
        assert!(text.contains("storage"));
        assert!(text.contains("STORAGE")); // Case-insensitive
        assert!(!text.contains("postgres"));
    }
}
