//! Vamana configuration parameters

/// Vamana index configuration
#[derive(Debug, Clone)]
pub struct VamanaConfig {
    /// Maximum degree (R parameter) - max neighbors per node
    pub max_degree: usize,
    
    /// Search list size during construction (L parameter)
    pub search_list_size: usize,
    
    /// Alpha parameter for pruning (typically 1.2)
    pub alpha: f32,
    
    /// Beam width for search
    pub beam_width: usize,
}

impl Default for VamanaConfig {
    fn default() -> Self {
        Self {
            max_degree: 64,
            search_list_size: 180,  // 🔧 折中: 128 → 180 (介于128和256之间)
            alpha: 1.2,
            beam_width: 48,         // 🔧 折中: 32 → 48 (介于32和64之间)
        }
    }
}

impl VamanaConfig {
    /// Create a new configuration
    pub fn new(max_degree: usize, search_list_size: usize, alpha: f32) -> Self {
        Self {
            max_degree,
            search_list_size,
            alpha,
            beam_width: max_degree / 2,
        }
    }

    /// Create configuration optimized for embedded environments
    pub fn embedded(dimension: usize) -> Self {
        // Lower parameters for memory efficiency
        let max_degree = if dimension <= 128 {
            32
        } else if dimension <= 384 {
            48
        } else {
            64
        };

        Self {
            max_degree,
            search_list_size: max_degree * 2,
            alpha: 1.2,
            beam_width: max_degree / 2,
        }
    }

    /// Create configuration optimized for performance
    pub fn performance(dimension: usize) -> Self {
        let max_degree = if dimension <= 128 {
            64
        } else if dimension <= 384 {
            96
        } else {
            128
        };

        Self {
            max_degree,
            search_list_size: max_degree * 3,
            alpha: 1.2,
            beam_width: max_degree,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = VamanaConfig::default();
        assert_eq!(config.max_degree, 64);
        assert_eq!(config.search_list_size, 180);  // Updated to match actual default
        assert!((config.alpha - 1.2).abs() < 0.001);
    }

    #[test]
    fn test_embedded_config() {
        let config = VamanaConfig::embedded(384);
        assert_eq!(config.max_degree, 48);
        assert!(config.max_degree < 64); // Should be more conservative
    }

    #[test]
    fn test_performance_config() {
        let config = VamanaConfig::performance(384);
        assert_eq!(config.max_degree, 96);
        assert!(config.max_degree > 64); // Should be more aggressive
    }
}
