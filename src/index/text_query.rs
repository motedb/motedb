//! MATCH query syntax — an FTS5-compatible boolean subset.
//!
//! Grammar (whitespace/punctuation separated words):
//! * plain words within a group are ANDed (the default conjunction);
//! * an uppercase standalone `OR` closes the current group and unions it
//!   with the next — implicit AND binds tighter than OR, exactly like FTS5
//!   (`a b OR c` ⇒ `(a AND b) OR c`);
//! * uppercase `AND` is accepted as an explicit no-op separator;
//! * lowercase `or`/`and` are ordinary terms.
//!
//! The parser yields groups of RAW words (operators removed); callers expand
//! each word through their tokenizer (lowercasing, length filtering, ngrams)
//! before dictionary lookup, so index-side and fallback-side parsing stay
//! identical.

/// Parse a MATCH query into OR-of-AND-groups of raw words.
///
/// Empty groups (dangling `OR`, operator-only queries) are dropped; a query
/// with no usable words yields an empty Vec (matches nothing).
pub fn parse_query_groups(query: &str) -> Vec<Vec<String>> {
    let mut groups: Vec<Vec<String>> = Vec::new();
    let mut current: Vec<String> = Vec::new();

    for word in query.split(|c: char| !c.is_alphanumeric() && c != '_') {
        match word {
            "OR" => {
                if !current.is_empty() {
                    groups.push(std::mem::take(&mut current));
                }
            }
            "AND" => {}
            _ => {
                if !word.is_empty() {
                    current.push(word.to_string());
                }
            }
        }
    }
    if !current.is_empty() {
        groups.push(current);
    }
    groups
}

/// Expand parsed raw-word groups through a tokenizer: one raw word may yield
/// several tokens (ngram) or none (length filter) — all tokens of a word are
/// ANDed within their group. Duplicates are dropped; empty groups removed.
pub fn expand_groups<F>(groups: Vec<Vec<String>>, tokenize_word: F) -> Vec<Vec<String>>
where
    F: Fn(&str) -> Vec<String>,
{
    groups
        .into_iter()
        .map(|group| {
            let mut tokens: Vec<String> = group.iter().flat_map(|w| tokenize_word(w)).collect();
            tokens.sort();
            tokens.dedup();
            tokens
        })
        .filter(|g| !g.is_empty())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn g(query: &str) -> Vec<Vec<String>> {
        parse_query_groups(query)
    }

    #[test]
    fn default_conjunction_is_one_group() {
        assert_eq!(g("apple banana"), vec![vec!["apple", "banana"]]);
        assert_eq!(
            g("  apple   banana cherry "),
            vec![vec!["apple", "banana", "cherry"]]
        );
    }

    #[test]
    fn explicit_or_splits_groups() {
        assert_eq!(g("a OR b"), vec![vec!["a"], vec!["b"]]);
        assert_eq!(g("a b OR c"), vec![vec!["a", "b"], vec!["c"]]);
    }

    #[test]
    fn uppercase_and_is_noop() {
        assert_eq!(g("a AND b"), vec![vec!["a", "b"]]);
        assert_eq!(g("a AND b OR c"), vec![vec!["a", "b"], vec!["c"]]);
    }

    #[test]
    fn lowercase_or_and_are_terms() {
        assert_eq!(g("a or b"), vec![vec!["a", "or", "b"]]);
        assert_eq!(g("this and that"), vec![vec!["this", "and", "that"]]);
    }

    #[test]
    fn separators_and_punctuation_split_words() {
        assert_eq!(
            g("apple, banana! cherry"),
            vec![vec!["apple", "banana", "cherry"]]
        );
        assert_eq!(g("a-OR-b"), vec![vec!["a"], vec!["b"]]);
    }

    #[test]
    fn dangling_operators_are_dropped() {
        assert_eq!(g("OR a"), vec![vec!["a"]]);
        assert_eq!(g("a OR"), vec![vec!["a"]]);
        assert_eq!(g("a OR OR b"), vec![vec!["a"], vec!["b"]]);
        assert!(g("OR AND").is_empty());
        assert!(g("!!! ...").is_empty());
        assert!(g("").is_empty());
    }

    #[test]
    fn operators_must_be_exactly_uppercase() {
        // "Or"/"oR" are terms, not operators (FTS5 rule).
        assert_eq!(g("a Or b"), vec![vec!["a", "Or", "b"]]);
    }

    #[test]
    fn expand_dedups_and_drops_empty() {
        let out = expand_groups(parse_query_groups("apple apple OR x"), |w| {
            vec![w.to_lowercase()]
        });
        assert_eq!(out, vec![vec!["apple"], vec!["x"]]);
        // A word the tokenizer drops empties the group.
        let out = expand_groups(parse_query_groups("apple zzz"), |w| {
            if w == "zzz" {
                vec![]
            } else {
                vec![w.to_string()]
            }
        });
        assert_eq!(out, vec![vec!["apple"]]);
    }
}
