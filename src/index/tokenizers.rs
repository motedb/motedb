//! 🔌 MoteDB 分词器插件系统
//!
//! 架构设计：
//! - **Core Trait**: `Tokenizer` trait 定义分词器接口
//! - **Built-in**: 内置轻量级分词器（Whitespace, Ngram）
//! - **Plugin System**: 通过 Feature Flags 启用第三方分词器
//! - **Runtime Registry**: 用户可注册自定义分词器
//!
//! ## 使用方式
//!
//! ### 1. 使用内置分词器（无需额外依赖）
//! ```ignore
//! use motedb::tokenizers::{WhitespaceTokenizer, NgramTokenizer};
//!
//! // 空格分词（英文）
//! let tokenizer = WhitespaceTokenizer::default();
//!
//! // N-gram 分词（CJK）
//! let tokenizer = NgramTokenizer::new(2);  // bigram
//! ```
//!
//! ### 2. 使用第三方分词器插件（Feature Flag）
//! ```ignore
//! # Cargo.toml
//! [dependencies]
//! motedb = { version = "0.1", features = ["tokenizer-jieba"] }
//! ```
//!
//! ```ignore
//! use motedb::tokenizers::JiebaTokenizer;
//!
//! // 中文分词（Jieba）
//! let tokenizer = JiebaTokenizer::default();
//! let tokens = tokenizer.tokenize("我爱自然语言处理");
//! ```
//!
//! ### 3. 自定义分词器（用户扩展）
//! ```ignore
//! use motedb::tokenizers::{Tokenizer, Token};
//!
//! struct MyCustomTokenizer;
//!
//! impl Tokenizer for MyCustomTokenizer {
//!     fn tokenize(&self, text: &str) -> Vec<Token> {
//!         // 自定义分词逻辑
//!         text.chars()
//!             .enumerate()
//!             .map(|(i, c)| Token {
//!                 text: c.to_string(),
//!                 position: i as u32,
//!             })
//!             .collect()
//!     }
//!
//!     fn name(&self) -> &str {
//!         "custom"
//!     }
//! }
//! ```
// 导出核心 trait 和数据结构
pub use crate::index::text_types::{Tokenizer, Token, Position};

// 导出内置分词器（零依赖，始终可用）
pub use crate::index::text_types::{WhitespaceTokenizer, NgramTokenizer};

//=============================================================================
// 🔌 Plugin: Jieba 中文分词器（可选）
//=============================================================================

#[cfg(feature = "tokenizer-jieba")]
mod jieba_plugin {
    use super::*;
    use jieba_rs::Jieba;
    use std::sync::Arc;

    /// Jieba 中文分词器（基于 jieba-rs）
    ///
    /// 特性：
    /// - 支持精确模式、全模式、搜索引擎模式
    /// - 支持自定义词典
    /// - HMM 新词发现
    ///
    /// 性能：
    /// - 编译时间：+2-3秒（仅在启用 feature 时）
    /// - 二进制大小：+350KB（仅在启用 feature 时）
    /// - 运行时：~50-100K tokens/sec
    ///
    /// 使用示例：
    /// ```ignore
    /// use motedb::tokenizers::JiebaTokenizer;
    ///
    /// let tokenizer = JiebaTokenizer::default();
    /// let tokens = tokenizer.tokenize("我爱自然语言处理");
    /// // => ["我", "爱", "自然语言", "处理"]
    /// ```
    pub struct JiebaTokenizer {
        jieba: Arc<Jieba>,
        mode: JiebaMode,
        case_sensitive: bool,
        min_len: usize,
        max_len: usize,
    }

    /// Jieba 分词模式
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum JiebaMode {
        /// 精确模式（默认）：试图将句子最精确地切开
        Precise,
        /// 全模式：把句子中所有可能的词语都扫描出来
        Full,
        /// 搜索引擎模式：在精确模式基础上，对长词再次切分
        Search,
    }

    impl Default for JiebaTokenizer {
        fn default() -> Self {
            Self {
                jieba: Arc::new(Jieba::new()),
                mode: JiebaMode::Search,  // 默认搜索模式，适合全文检索
                case_sensitive: false,
                min_len: 1,
                max_len: 64,
            }
        }
    }

    impl JiebaTokenizer {
        /// 创建新的 Jieba 分词器
        pub fn new() -> Self {
            Self::default()
        }

        /// 设置分词模式
        pub fn with_mode(mut self, mode: JiebaMode) -> Self {
            self.mode = mode;
            self
        }

        /// 设置大小写敏感
        pub fn case_sensitive(mut self, sensitive: bool) -> Self {
            self.case_sensitive = sensitive;
            self
        }
    }

    impl Tokenizer for JiebaTokenizer {
        fn tokenize(&self, text: &str) -> Vec<Token> {
            // 根据模式选择分词方法
            let words = match self.mode {
                JiebaMode::Precise => self.jieba.cut(text, false),
                JiebaMode::Full => self.jieba.cut(text, true),
                JiebaMode::Search => self.jieba.cut_for_search(text, false),
            };

            words
                .into_iter()
                .enumerate()
                .filter_map(|(i, word)| {
                    let word_str = word.trim();
                    if word_str.is_empty() {
                        return None;
                    }

                    let len = word_str.chars().count();
                    if len < self.min_len || len > self.max_len {
                        return None;
                    }

                    let text = if self.case_sensitive {
                        word_str.to_string()
                    } else {
                        word_str.to_lowercase()
                    };

                    Some(Token {
                        text,
                        position: i as u32,
                    })
                })
                .collect()
        }

        fn name(&self) -> &str {
            "jieba"
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn test_jieba_tokenizer() {
            let tokenizer = JiebaTokenizer::default();
            let tokens = tokenizer.tokenize("我爱自然语言处理");
            
            debug_log!("Tokens: {:?}", tokens.iter().map(|t| &t.text).collect::<Vec<_>>());
            assert!(!tokens.is_empty());
            assert!(tokens.iter().any(|t| t.text == "自然语言"));
        }

        #[test]
        fn test_jieba_modes() {
            let text = "我来到北京清华大学";

            // 精确模式
            let precise = JiebaTokenizer::default().with_mode(JiebaMode::Precise);
            let tokens = precise.tokenize(text);
            debug_log!("Precise: {:?}", tokens.iter().map(|t| &t.text).collect::<Vec<_>>());

            // 全模式
            let full = JiebaTokenizer::default().with_mode(JiebaMode::Full);
            let tokens = full.tokenize(text);
            debug_log!("Full: {:?}", tokens.iter().map(|t| &t.text).collect::<Vec<_>>());

            // 搜索模式
            let search = JiebaTokenizer::default().with_mode(JiebaMode::Search);
            let tokens = search.tokenize(text);
            debug_log!("Search: {:?}", tokens.iter().map(|t| &t.text).collect::<Vec<_>>());
        }
    }
}

#[cfg(feature = "tokenizer-jieba")]
pub use jieba_plugin::{JiebaTokenizer, JiebaMode};

//=============================================================================
// 🔌 Future Plugins (Placeholder)
//=============================================================================

// 未来可添加更多分词器：
// - Tantivy 分词器（基于 Tantivy）
// - ICU 分词器（基于 icu-rust）
// - Mecab 分词器（日语）
// - OpenCC 分词器（繁简转换）

//=============================================================================
// 📚 分词器工厂（方便用户选择）
//=============================================================================

/// 分词器类型枚举
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TokenizerType {
    /// 空格分词（英文）
    Whitespace,
    /// N-gram 分词（CJK）
    Ngram,
    /// Jieba 中文分词（需要 `tokenizer-jieba` feature）
    #[cfg(feature = "tokenizer-jieba")]
    Jieba,
}

/// 分词器工厂
pub struct TokenizerFactory;

impl TokenizerFactory {
    /// 根据类型创建分词器
    pub fn create(tokenizer_type: TokenizerType) -> Box<dyn Tokenizer> {
        match tokenizer_type {
            TokenizerType::Whitespace => Box::new(WhitespaceTokenizer::default()),
            TokenizerType::Ngram => Box::new(NgramTokenizer::new(2)),
            #[cfg(feature = "tokenizer-jieba")]
            TokenizerType::Jieba => Box::new(JiebaTokenizer::default()),
        }
    }

    /// 从字符串名称创建分词器
    pub fn from_name(name: &str) -> Option<Box<dyn Tokenizer>> {
        match name {
            "whitespace" => Some(Box::new(WhitespaceTokenizer::default())),
            "ngram" | "ngram2" => Some(Box::new(NgramTokenizer::new(2))),
            "ngram3" => Some(Box::new(NgramTokenizer::new(3))),
            #[cfg(feature = "tokenizer-jieba")]
            "jieba" => Some(Box::new(JiebaTokenizer::default())),
            _ => None,
        }
    }

    /// 列出所有可用分词器
    pub fn available_tokenizers() -> Vec<&'static str> {
        let tokenizers = vec!["whitespace", "ngram"];
        #[cfg(feature = "tokenizer-jieba")]
        let tokenizers = {
            let mut t = tokenizers;
            t.push("jieba");
            t
        };
        tokenizers
    }
}

//=============================================================================
// 📖 文档和示例
//=============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_whitespace_tokenizer() {
        let tokenizer = WhitespaceTokenizer::default();
        let tokens = tokenizer.tokenize("Hello World Test");
        assert_eq!(tokens.len(), 3);
        assert_eq!(tokens[0].text, "hello");
        assert_eq!(tokens[1].text, "world");
    }

    #[test]
    fn test_ngram_tokenizer() {
        let tokenizer = NgramTokenizer::new(2);
        let tokens = tokenizer.tokenize("你好世界");
        assert!(!tokens.is_empty());
    }

    #[test]
    fn test_tokenizer_factory() {
        // 测试工厂创建
        let tokenizer = TokenizerFactory::from_name("whitespace").unwrap();
        let tokens = tokenizer.tokenize("test");
        assert_eq!(tokens.len(), 1);

        // 测试可用分词器列表
        let available = TokenizerFactory::available_tokenizers();
        debug_log!("Available tokenizers: {:?}", available);
        assert!(available.contains(&"whitespace"));
        assert!(available.contains(&"ngram"));
    }

    #[test]
    fn test_custom_tokenizer() {
        // 用户自定义分词器示例
        struct CharTokenizer;
        
        impl Tokenizer for CharTokenizer {
            fn tokenize(&self, text: &str) -> Vec<Token> {
                text.chars()
                    .enumerate()
                    .map(|(i, c)| Token {
                        text: c.to_string(),
                        position: i as u32,
                    })
                    .collect()
            }
            
            fn name(&self) -> &str {
                "char"
            }
        }

        let tokenizer = CharTokenizer;
        let tokens = tokenizer.tokenize("ABC");
        assert_eq!(tokens.len(), 3);
        assert_eq!(tokens[0].text, "A");
        assert_eq!(tokens[1].text, "B");
        assert_eq!(tokens[2].text, "C");
    }
}
