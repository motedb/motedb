/// Token types for SQL lexer
use phf::phf_map;

// 🚀 P1.3: Perfect hash map for O(1) keyword lookup
static KEYWORDS: phf::Map<&'static str, TokenType> = phf_map! {
    "select" => TokenType::Select,
    "from" => TokenType::From,
    "where" => TokenType::Where,
    "insert" => TokenType::Insert,
    "into" => TokenType::Into,
    "values" => TokenType::Values,
    "update" => TokenType::Update,
    "set" => TokenType::Set,
    "delete" => TokenType::Delete,
    "create" => TokenType::Create,
    "table" => TokenType::Table,
    "index" => TokenType::Index,
    "drop" => TokenType::Drop,
    "and" => TokenType::And,
    "or" => TokenType::Or,
    "not" => TokenType::Not,
    "like" => TokenType::Like,
    "in" => TokenType::In,
    "between" => TokenType::Between,
    "is" => TokenType::Is,
    "null" => TokenType::Null,
    "as" => TokenType::As,
    "order" => TokenType::Order,
    "by" => TokenType::By,
    "asc" => TokenType::Asc,
    "desc" => TokenType::Desc,
    "limit" => TokenType::Limit,
    "offset" => TokenType::Offset,
    "latest" => TokenType::Latest,
    "distinct" => TokenType::Distinct,
    "group" => TokenType::Group,
    "having" => TokenType::Having,
    "join" => TokenType::Join,
    "left" => TokenType::Left,
    "right" => TokenType::Right,
    "inner" => TokenType::Inner,
    "outer" => TokenType::Outer,
    "full" => TokenType::Full,
    "on" => TokenType::On,
    "primary" => TokenType::Primary,
    "key" => TokenType::Key,
    "using" => TokenType::Using,
    "array" => TokenType::Array,
    "show" => TokenType::Show,
    "describe" => TokenType::Describe,
    "tables" => TokenType::Tables,
    "integer" => TokenType::Integer,
    "int" => TokenType::Integer,
    "float" => TokenType::Float,
    "real" => TokenType::Float,
    "double" => TokenType::Float,
    "text" => TokenType::Text,
    "varchar" => TokenType::Text,
    "string" => TokenType::Text,
    "timestamp" => TokenType::Timestamp,
    "datetime" => TokenType::Timestamp,
    "vector" => TokenType::Vector,
    "geometry" => TokenType::Geometry,
    "geom" => TokenType::Geometry,
    "boolean" => TokenType::Boolean,
    "bool" => TokenType::Boolean,
    "true" => TokenType::True,
    "false" => TokenType::False,
};

#[derive(Debug, Clone, PartialEq)]
pub enum TokenType {
    // Keywords
    Select,
    From,
    Where,
    Insert,
    Into,
    Values,
    Update,
    Set,
    Delete,
    Create,
    Table,
    Index,
    Drop,
    And,
    Or,
    Not,
    Like,
    In,
    Between,
    Is,
    Null,
    As,
    Order,
    By,
    Asc,
    Desc,
    Limit,
    Offset,
    Latest,   // LATEST (for LATEST BY)
    Distinct, // DISTINCT
    Group,
    Having,
    Join,
    Left,
    Right,
    Inner,
    Full,
    Outer,
    On,
    Primary,  // PRIMARY
    Key,      // KEY
    Using,    // USING (for CREATE INDEX ... USING type)
    Array,    // ARRAY (for array literals)
    Show,     // SHOW
    Describe, // DESCRIBE or DESC
    Tables,   // TABLES
    
    // Data types
    Integer,
    Float,
    Text,
    Timestamp,
    Vector,
    Geometry,
    Boolean,
    
    // Operators
    Eq,           // =
    Ne,           // != or <>
    Lt,           // <
    Gt,           // >
    Le,           // <=
    Ge,           // >=
    Plus,         // +
    Minus,        // -
    Star,         // *
    Slash,        // /
    Percent,      // %
    
    // E-SQL Vector Distance Operators
    L2Distance,      // <-> (Euclidean distance)
    CosineDistance,  // <=> (Cosine distance)
    DotProduct,      // <#> (Inner product)
    
    // Delimiters
    LParen,       // (
    RParen,       // )
    LBracket,     // [
    RBracket,     // ]
    Comma,        // ,
    Semicolon,    // ;
    Dot,          // .
    
    // Literals
    Number(f64),
    String(String),
    Identifier(String),
    True,
    False,
    
    // Special
    Eof,
}

#[derive(Debug, Clone)]
pub struct Token {
    pub token_type: TokenType,
    pub line: usize,
    pub column: usize,
}

impl Token {
    pub fn new(token_type: TokenType, line: usize, column: usize) -> Self {
        Self { token_type, line, column }
    }
}

impl TokenType {
    /// Check if this token is a keyword (🚀 P1.3: O(1) perfect hash lookup)
    pub fn from_keyword(s: &str) -> Option<Self> {
        // Convert to lowercase for case-insensitive matching
        let lowercase = s.to_lowercase();
        KEYWORDS.get(lowercase.as_str()).cloned()
    }
}
