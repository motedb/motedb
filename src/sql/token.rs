/// Token types for SQL lexer

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
    /// Check if this token is a keyword
    pub fn from_keyword(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "select" => Some(TokenType::Select),
            "from" => Some(TokenType::From),
            "where" => Some(TokenType::Where),
            "insert" => Some(TokenType::Insert),
            "into" => Some(TokenType::Into),
            "values" => Some(TokenType::Values),
            "update" => Some(TokenType::Update),
            "set" => Some(TokenType::Set),
            "delete" => Some(TokenType::Delete),
            "create" => Some(TokenType::Create),
            "table" => Some(TokenType::Table),
            "index" => Some(TokenType::Index),
            "drop" => Some(TokenType::Drop),
            "and" => Some(TokenType::And),
            "or" => Some(TokenType::Or),
            "not" => Some(TokenType::Not),
            "like" => Some(TokenType::Like),
            "in" => Some(TokenType::In),
            "between" => Some(TokenType::Between),
            "is" => Some(TokenType::Is),
            "null" => Some(TokenType::Null),
            "as" => Some(TokenType::As),
            "order" => Some(TokenType::Order),
            "by" => Some(TokenType::By),
            "asc" => Some(TokenType::Asc),
            "desc" => Some(TokenType::Desc),
            "limit" => Some(TokenType::Limit),
            "offset" => Some(TokenType::Offset),
            "latest" => Some(TokenType::Latest),
            "distinct" => Some(TokenType::Distinct),
            "group" => Some(TokenType::Group),
            "having" => Some(TokenType::Having),
            "join" => Some(TokenType::Join),
            "left" => Some(TokenType::Left),
            "right" => Some(TokenType::Right),
            "inner" => Some(TokenType::Inner),
            "outer" => Some(TokenType::Outer),
            "full" => Some(TokenType::Full),
            "on" => Some(TokenType::On),
            "primary" => Some(TokenType::Primary),
            "key" => Some(TokenType::Key),
            "using" => Some(TokenType::Using),
            "array" => Some(TokenType::Array),
            "show" => Some(TokenType::Show),
            "describe" => Some(TokenType::Describe),
            "tables" => Some(TokenType::Tables),
            "integer" | "int" => Some(TokenType::Integer),
            "float" | "real" | "double" => Some(TokenType::Float),
            "text" | "varchar" | "string" => Some(TokenType::Text),
            "timestamp" | "datetime" => Some(TokenType::Timestamp),
            "vector" => Some(TokenType::Vector),
            "geometry" | "geom" => Some(TokenType::Geometry),
            "boolean" | "bool" => Some(TokenType::Boolean),
            "true" => Some(TokenType::True),
            "false" => Some(TokenType::False),
            _ => None,
        }
    }
}
