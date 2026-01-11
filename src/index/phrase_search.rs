//! Phrase Search for TextFTSIndex
//!
//! Implements exact phrase matching using positional indexes:
//! - "machine learning" → find docs where "machine" immediately precedes "learning"
//! - Uses inverted index positions for fast matching
//! - O(n) complexity where n = docs containing first term

use crate::{Result, StorageError};
use crate::index::text_types::{DocId, Position, PostingList};
use std::collections::HashMap;

/// Phrase search executor
pub struct PhraseSearcher;

impl PhraseSearcher {
    /// Search for exact phrase in a TextFTSIndex
    /// 
    /// High-level API that works with TextFTSIndex
    pub fn search_phrase_in_index(
        index: &crate::index::TextFTSIndex,
        phrase: &str,
    ) -> Result<Vec<(DocId, u32)>> {
        use crate::index::text_types::Tokenizer;
        
        // Tokenize phrase
        let tokenizer = index.get_tokenizer();
        let tokens = tokenizer.tokenize(phrase);
        
        if tokens.is_empty() {
            return Ok(vec![]);
        }
        
        // Get posting lists for each term
        let mut term_postings = Vec::new();
        for token in &tokens {
            if let Some(posting) = index.get_posting_list(&token.text)? {
                term_postings.push(posting);
            } else {
                // Term not found, no matches
                return Ok(vec![]);
            }
        }
        
        // Convert to references for search
        let posting_refs: Vec<&PostingList> = term_postings.iter().collect();
        Self::search(&posting_refs)
    }
    
    /// Search with proximity in a TextFTSIndex
    pub fn search_proximity_in_index(
        index: &crate::index::TextFTSIndex,
        terms: &str,
        max_distance: u32,
    ) -> Result<Vec<(DocId, u32)>> {
        use crate::index::text_types::Tokenizer;
        
        // Tokenize terms
        let tokenizer = index.get_tokenizer();
        let tokens = tokenizer.tokenize(terms);
        if tokens.is_empty() {
            return Ok(vec![]);
        }
        
        // Get posting lists for each term
        let mut term_postings = Vec::new();
        for token in &tokens {
            if let Some(posting) = index.get_posting_list(&token.text)? {
                term_postings.push(posting);
            } else {
                return Ok(vec![]);
            }
        }
        
        // Convert to references for search
        let posting_refs: Vec<&PostingList> = term_postings.iter().collect();
        Self::search_with_proximity(&posting_refs, max_distance)
    }
    
    /// Search for exact phrase in posting lists
    /// 
    /// Algorithm:
    /// 1. Find docs containing all terms
    /// 2. For each doc, verify positions are consecutive
    /// 
    /// # Arguments
    /// * `term_postings` - Posting lists for each term in phrase (in order)
    /// 
    /// # Returns
    /// Vec of (doc_id, match_count) pairs
    /// 
    /// # Example
    /// ```ignore
    /// // Search "machine learning"
    /// let postings = vec![machine_postings, learning_postings];
    /// let results = PhraseSearcher::search(&postings)?;
    /// ```
    pub fn search(term_postings: &[&PostingList]) -> Result<Vec<(DocId, u32)>> {
        if term_postings.is_empty() {
            return Ok(vec![]);
        }
        
        if term_postings.len() == 1 {
            // Single term: return all docs
            return Ok(term_postings[0]
                .doc_ids()
                .iter()
                .map(|&doc_id| (doc_id, 1))
                .collect());
        }
        
        // Find candidate docs (intersection of all term doc_ids)
        let candidate_docs = Self::find_candidate_docs(term_postings)?;
        
        // Verify phrase matches in each candidate doc
        // 🚀 P1 优化：预分配候选文档数量
        let mut results = Vec::with_capacity(candidate_docs.len());
        for doc_id in candidate_docs {
            if let Some(count) = Self::verify_phrase_in_doc(doc_id, term_postings)? {
                results.push((doc_id, count));
            }
        }
        
        Ok(results)
    }
    
    /// Find docs containing all terms (intersection)
    fn find_candidate_docs(term_postings: &[&PostingList]) -> Result<Vec<DocId>> {
        let mut candidate_docs: Vec<DocId> = term_postings[0]
            .doc_ids()
            .iter()
            .copied()
            .collect();
        
        // Intersect with remaining terms
        for posting in &term_postings[1..] {
            let doc_set: std::collections::HashSet<DocId> = 
                posting.doc_ids().iter().copied().collect();
            
            candidate_docs.retain(|doc_id| doc_set.contains(doc_id));
            
            if candidate_docs.is_empty() {
                break; // Early exit
            }
        }
        
        Ok(candidate_docs)
    }
    
    /// Verify phrase exists in document by checking consecutive positions
    /// 
    /// Returns: Some(match_count) if phrase found, None otherwise
    fn verify_phrase_in_doc(
        doc_id: DocId,
        term_postings: &[&PostingList],
    ) -> Result<Option<u32>> {
        // Get positions for each term in this doc
        let mut positions_per_term: Vec<Vec<Position>> = Vec::new();
        
        for posting in term_postings {
            match posting.get_positions(doc_id) {
                Some(positions) => positions_per_term.push(positions.to_vec()),
                None => {
                    // Position index disabled or doc not found
                    return Err(StorageError::InvalidData(
                        "❌ PHRASE_SEARCH requires position indexing to be enabled.\n\
                         💡 Solution: Re-create the TEXT index with:\n\
                         \n\
                         DROP INDEX <index_name>;\n\
                         CREATE TEXT INDEX <index_name> ON <table>(<column>) WITH POSITIONS;\n\
                         \n\
                         Note: Position indexing adds ~40% memory overhead but enables phrase/proximity search.".to_string()
                    ).into());
                }
            }
        }
        
        // Find consecutive matches
        let match_count = Self::count_consecutive_matches(&positions_per_term);
        
        if match_count > 0 {
            Ok(Some(match_count))
        } else {
            Ok(None)
        }
    }
    
    /// Count consecutive position matches
    /// 
    /// Example: 
    /// - term[0] positions: [5, 10, 20]
    /// - term[1] positions: [6, 21]
    /// → Matches at (5,6) and (20,21) → count = 2
    fn count_consecutive_matches(positions_per_term: &[Vec<Position>]) -> u32 {
        if positions_per_term.is_empty() {
            return 0;
        }
        
        let mut match_count = 0;
        
        // For each position of first term, try to find consecutive chain
        for &first_pos in &positions_per_term[0] {
            let mut current_pos = first_pos;
            let mut matched = true;
            
            // Check if subsequent terms appear at consecutive positions
            for term_idx in 1..positions_per_term.len() {
                let expected_pos = current_pos + 1;
                
                if positions_per_term[term_idx].contains(&expected_pos) {
                    current_pos = expected_pos;
                } else {
                    matched = false;
                    break;
                }
            }
            
            if matched {
                match_count += 1;
            }
        }
        
        match_count
    }
    
    /// Search with proximity: terms within N positions of each other
    /// 
    /// Example: "machine learning"~5 → terms within 5 words
    pub fn search_with_proximity(
        term_postings: &[&PostingList],
        max_distance: u32,
    ) -> Result<Vec<(DocId, u32)>> {
        if term_postings.is_empty() {
            return Ok(vec![]);
        }
        
        let candidate_docs = Self::find_candidate_docs(term_postings)?;
        
        // 🚀 P1 优化：预分配候选文档数量
        let mut results = Vec::with_capacity(candidate_docs.len());
        for doc_id in candidate_docs {
            if let Some(count) = Self::verify_proximity_in_doc(doc_id, term_postings, max_distance)? {
                results.push((doc_id, count));
            }
        }
        
        Ok(results)
    }
    
    /// Verify terms appear within max_distance of each other
    fn verify_proximity_in_doc(
        doc_id: DocId,
        term_postings: &[&PostingList],
        max_distance: u32,
    ) -> Result<Option<u32>> {
        let mut positions_per_term: Vec<Vec<Position>> = Vec::new();
        
        for posting in term_postings {
            match posting.get_positions(doc_id) {
                Some(positions) => positions_per_term.push(positions.to_vec()),
                None => return Ok(None),
            }
        }
        
        let match_count = Self::count_proximity_matches(&positions_per_term, max_distance);
        
        if match_count > 0 {
            Ok(Some(match_count))
        } else {
            Ok(None)
        }
    }
    
    /// Count matches within proximity distance
    fn count_proximity_matches(positions_per_term: &[Vec<Position>], max_distance: u32) -> u32 {
        if positions_per_term.is_empty() {
            return 0;
        }
        
        let mut match_count = 0;
        
        // For each position of first term
        for &first_pos in &positions_per_term[0] {
            let mut all_within_range = true;
            
            // Check if all other terms appear within max_distance
            for term_idx in 1..positions_per_term.len() {
                let within_range = positions_per_term[term_idx].iter().any(|&pos| {
                    let distance = if pos > first_pos {
                        pos - first_pos
                    } else {
                        first_pos - pos
                    };
                    distance <= max_distance
                });
                
                if !within_range {
                    all_within_range = false;
                    break;
                }
            }
            
            if all_within_range {
                match_count += 1;
            }
        }
        
        match_count
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::text_types::PostingList;
    
    #[test]
    fn test_consecutive_matches() {
        let positions = vec![
            vec![5, 10, 20],  // "machine"
            vec![6, 21],       // "learning"
        ];
        
        let count = PhraseSearcher::count_consecutive_matches(&positions);
        assert_eq!(count, 2); // (5,6) and (20,21)
    }
    
    #[test]
    fn test_no_consecutive_matches() {
        let positions = vec![
            vec![5, 10],  // "machine"
            vec![7, 21],  // "learning" (not consecutive)
        ];
        
        let count = PhraseSearcher::count_consecutive_matches(&positions);
        assert_eq!(count, 0);
    }
    
    #[test]
    fn test_proximity_matches() {
        let positions = vec![
            vec![5, 20],  // "machine"
            vec![8, 22],  // "learning" (distance 3 and 2)
        ];
        
        let count = PhraseSearcher::count_proximity_matches(&positions, 5);
        assert_eq!(count, 2); // Both within distance 5
        
        let count = PhraseSearcher::count_proximity_matches(&positions, 2);
        assert_eq!(count, 1); // Only (20,22) within distance 2
    }
}
