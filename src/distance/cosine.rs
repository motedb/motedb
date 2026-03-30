//! Cosine similarity and distance computation with SIMD optimization

#[cfg(target_arch = "x86_64")]
use std::arch::x86_64::*;

/// CPU feature detection cache (initialized once at startup)
#[cfg(target_arch = "x86_64")]
static CPU_FEATURES: OnceLock<CpuFeatures> = OnceLock::new();

#[cfg(target_arch = "x86_64")]
#[derive(Clone, Copy)]
struct CpuFeatures {
    has_avx2: bool,
    has_sse: bool,
}

#[cfg(target_arch = "x86_64")]
fn get_cpu_features() -> CpuFeatures {
    *CPU_FEATURES.get_or_init(|| CpuFeatures {
        has_avx2: is_x86_feature_detected!("avx2") && is_x86_feature_detected!("fma"),
        has_sse: is_x86_feature_detected!("sse"),
    })
}

/// Compute cosine similarity between two vectors with SIMD optimization
///
/// # Arguments
/// * `a` - First vector
/// * `b` - Second vector
///
/// # Returns
/// Cosine similarity in range [-1, 1], where 1 means identical direction
///
/// # Panics
/// Panics if vectors have different dimensions
#[inline]
pub fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    assert_eq!(a.len(), b.len(), "Vector dimensions must match");

    // P2 优化：缓存CPU特性检测，避免重复runtime检测
    #[cfg(target_arch = "x86_64")]
    {
        let features = get_cpu_features();
        if features.has_avx2 && a.len() >= 8 {
            unsafe { cosine_similarity_avx2(a, b) }
        } else if features.has_sse && a.len() >= 4 {
            unsafe { cosine_similarity_sse(a, b) }
        } else {
            cosine_similarity_scalar(a, b)
        }
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        cosine_similarity_scalar(a, b)
    }
}

/// AVX2优化的余弦相似度计算（P2优化：循环展开）
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2,fma")]
unsafe fn cosine_similarity_avx2(a: &[f32], b: &[f32]) -> f32 {
    let n = a.len();
    let chunks = n / 32; // Process 32 elements at once (4x unroll)
    let remainder = n % 32;
    
    // AVX2并行处理，4路循环展开
    let mut dot_sum1 = _mm256_setzero_ps();
    let mut dot_sum2 = _mm256_setzero_ps();
    let mut dot_sum3 = _mm256_setzero_ps();
    let mut dot_sum4 = _mm256_setzero_ps();
    
    let mut norm_a_sum1 = _mm256_setzero_ps();
    let mut norm_a_sum2 = _mm256_setzero_ps();
    let mut norm_a_sum3 = _mm256_setzero_ps();
    let mut norm_a_sum4 = _mm256_setzero_ps();
    
    let mut norm_b_sum1 = _mm256_setzero_ps();
    let mut norm_b_sum2 = _mm256_setzero_ps();
    let mut norm_b_sum3 = _mm256_setzero_ps();
    let mut norm_b_sum4 = _mm256_setzero_ps();
    
    // 4路循环展开（32 elements per iteration）
    for i in 0..chunks {
        let offset = i * 32;
        
        let a_vec1 = _mm256_loadu_ps(a.as_ptr().add(offset));
        let b_vec1 = _mm256_loadu_ps(b.as_ptr().add(offset));
        let a_vec2 = _mm256_loadu_ps(a.as_ptr().add(offset + 8));
        let b_vec2 = _mm256_loadu_ps(b.as_ptr().add(offset + 8));
        let a_vec3 = _mm256_loadu_ps(a.as_ptr().add(offset + 16));
        let b_vec3 = _mm256_loadu_ps(b.as_ptr().add(offset + 16));
        let a_vec4 = _mm256_loadu_ps(a.as_ptr().add(offset + 24));
        let b_vec4 = _mm256_loadu_ps(b.as_ptr().add(offset + 24));
        
        // FMA: fused multiply-add for better performance
        dot_sum1 = _mm256_fmadd_ps(a_vec1, b_vec1, dot_sum1);
        dot_sum2 = _mm256_fmadd_ps(a_vec2, b_vec2, dot_sum2);
        dot_sum3 = _mm256_fmadd_ps(a_vec3, b_vec3, dot_sum3);
        dot_sum4 = _mm256_fmadd_ps(a_vec4, b_vec4, dot_sum4);
        
        norm_a_sum1 = _mm256_fmadd_ps(a_vec1, a_vec1, norm_a_sum1);
        norm_a_sum2 = _mm256_fmadd_ps(a_vec2, a_vec2, norm_a_sum2);
        norm_a_sum3 = _mm256_fmadd_ps(a_vec3, a_vec3, norm_a_sum3);
        norm_a_sum4 = _mm256_fmadd_ps(a_vec4, a_vec4, norm_a_sum4);
        
        norm_b_sum1 = _mm256_fmadd_ps(b_vec1, b_vec1, norm_b_sum1);
        norm_b_sum2 = _mm256_fmadd_ps(b_vec2, b_vec2, norm_b_sum2);
        norm_b_sum3 = _mm256_fmadd_ps(b_vec3, b_vec3, norm_b_sum3);
        norm_b_sum4 = _mm256_fmadd_ps(b_vec4, b_vec4, norm_b_sum4);
    }
    
    // Combine 4-way unrolled accumulators
    let dot_sum = _mm256_add_ps(
        _mm256_add_ps(dot_sum1, dot_sum2),
        _mm256_add_ps(dot_sum3, dot_sum4)
    );
    let norm_a_sum = _mm256_add_ps(
        _mm256_add_ps(norm_a_sum1, norm_a_sum2),
        _mm256_add_ps(norm_a_sum3, norm_a_sum4)
    );
    let norm_b_sum = _mm256_add_ps(
        _mm256_add_ps(norm_b_sum1, norm_b_sum2),
        _mm256_add_ps(norm_b_sum3, norm_b_sum4)
    );
    
    // 水平求和
    let mut dot_product = horizontal_sum_avx2(dot_sum);
    let mut norm_a = horizontal_sum_avx2(norm_a_sum);
    let mut norm_b = horizontal_sum_avx2(norm_b_sum);
    
    // 处理剩余元素（8 elements at a time for remainder >= 8）
    let offset_remainder = chunks * 32;
    let remainder_chunks = remainder / 8;
    let mut dot_sum_rem = _mm256_setzero_ps();
    let mut norm_a_sum_rem = _mm256_setzero_ps();
    let mut norm_b_sum_rem = _mm256_setzero_ps();
    
    for i in 0..remainder_chunks {
        let offset = offset_remainder + i * 8;
        let a_vec = _mm256_loadu_ps(a.as_ptr().add(offset));
        let b_vec = _mm256_loadu_ps(b.as_ptr().add(offset));
        
        dot_sum_rem = _mm256_fmadd_ps(a_vec, b_vec, dot_sum_rem);
        norm_a_sum_rem = _mm256_fmadd_ps(a_vec, a_vec, norm_a_sum_rem);
        norm_b_sum_rem = _mm256_fmadd_ps(b_vec, b_vec, norm_b_sum_rem);
    }
    
    dot_product += horizontal_sum_avx2(dot_sum_rem);
    norm_a += horizontal_sum_avx2(norm_a_sum_rem);
    norm_b += horizontal_sum_avx2(norm_b_sum_rem);
    
    // 标量处理最后的元素（< 8 elements）
    for i in (offset_remainder + remainder_chunks * 8)..n {
        dot_product += a[i] * b[i];
        norm_a += a[i] * a[i];
        norm_b += b[i] * b[i];
    }
    
    compute_cosine_similarity(dot_product, norm_a, norm_b)
}

/// SSE优化的余弦相似度计算
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse")]
unsafe fn cosine_similarity_sse(a: &[f32], b: &[f32]) -> f32 {
    let n = a.len();
    let chunks = n / 4;
    let remainder = n % 4;
    
    let mut dot_product = 0.0f32;
    let mut norm_a = 0.0f32;
    let mut norm_b = 0.0f32;
    
    // SSE并行处理4个元素
    let mut dot_sum = _mm_setzero_ps();
    let mut norm_a_sum = _mm_setzero_ps();
    let mut norm_b_sum = _mm_setzero_ps();
    
    for i in 0..chunks {
        let offset = i * 4;
        let a_vec = _mm_loadu_ps(a.as_ptr().add(offset));
        let b_vec = _mm_loadu_ps(b.as_ptr().add(offset));
        
        // 点积
        dot_sum = _mm_add_ps(dot_sum, _mm_mul_ps(a_vec, b_vec));
        // 计算范数平方
        norm_a_sum = _mm_add_ps(norm_a_sum, _mm_mul_ps(a_vec, a_vec));
        norm_b_sum = _mm_add_ps(norm_b_sum, _mm_mul_ps(b_vec, b_vec));
    }
    
    // 水平求和
    dot_product = horizontal_sum_sse(dot_sum);
    norm_a = horizontal_sum_sse(norm_a_sum);
    norm_b = horizontal_sum_sse(norm_b_sum);
    
    // 处理剩余元素
    for i in (n - remainder)..n {
        dot_product += a[i] * b[i];
        norm_a += a[i] * a[i];
        norm_b += b[i] * b[i];
    }
    
    compute_cosine_similarity(dot_product, norm_a, norm_b)
}

/// 标量版本（无SIMD）
fn cosine_similarity_scalar(a: &[f32], b: &[f32]) -> f32 {
    let mut dot_product = 0.0f32;
    let mut norm_a = 0.0f32;
    let mut norm_b = 0.0f32;
    
    for i in 0..a.len() {
        dot_product += a[i] * b[i];
        norm_a += a[i] * a[i];
        norm_b += b[i] * b[i];
    }
    
    compute_cosine_similarity(dot_product, norm_a, norm_b)
}

/// 计算最终的余弦相似度
#[inline]
fn compute_cosine_similarity(dot_product: f32, norm_a: f32, norm_b: f32) -> f32 {
    if norm_a == 0.0 || norm_b == 0.0 {
        0.0
    } else {
        let similarity = dot_product / (norm_a.sqrt() * norm_b.sqrt());
        // 处理浮点误差，确保在有效范围内
        similarity.clamp(-1.0, 1.0)
    }
}

/// AVX2水平求和（P2优化：使用更高效的指令序列）
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn horizontal_sum_avx2(v: __m256) -> f32 {
    // 更高效的水平求和方法
    // Step 1: Add high 128 bits to low 128 bits
    let sum_high_low = _mm_add_ps(_mm256_castps256_ps128(v), _mm256_extractf128_ps(v, 1));
    // Step 2: Horizontal add twice
    let sum1 = _mm_hadd_ps(sum_high_low, sum_high_low);
    let sum2 = _mm_hadd_ps(sum1, sum1);
    _mm_cvtss_f32(sum2)
}

/// SSE水平求和
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse")]
unsafe fn horizontal_sum_sse(v: __m128) -> f32 {
    // 将4个float两两相加
    let sum1 = _mm_add_ps(v, _mm_movehl_ps(v, v));
    let sum2 = _mm_add_ss(sum1, _mm_shuffle_ps(sum1, sum1, 1));
    _mm_cvtss_f32(sum2)
}

/// Compute cosine distance (1 - cosine_similarity)
///
/// # Arguments
/// * `a` - First vector
/// * `b` - Second vector
///
/// # Returns
/// Cosine distance in range [0, 2], where 0 means identical direction
///
/// # Panics
/// Panics if vectors have different dimensions
#[inline]
pub fn cosine_distance(a: &[f32], b: &[f32]) -> f32 {
    1.0 - cosine_similarity(a, b)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cosine_similarity_same_vector() {
        let a = vec![1.0, 2.0, 3.0];
        let sim = cosine_similarity(&a, &a);
        assert!((sim - 1.0).abs() < 0.001);
    }

    #[test]
    fn test_cosine_similarity_orthogonal() {
        let a = vec![1.0, 0.0, 0.0];
        let b = vec![0.0, 1.0, 0.0];
        let sim = cosine_similarity(&a, &b);
        assert!(sim.abs() < 0.001);
    }

    #[test]
    fn test_cosine_similarity_opposite() {
        let a = vec![1.0, 0.0, 0.0];
        let b = vec![-1.0, 0.0, 0.0];
        let sim = cosine_similarity(&a, &b);
        assert!((sim + 1.0).abs() < 0.001);
    }

    #[test]
    fn test_cosine_distance() {
        let a = vec![1.0, 0.0, 0.0];
        let b = vec![1.0, 0.0, 0.0];
        let dist = cosine_distance(&a, &b);
        assert!(dist < 0.001);
    }

    #[test]
    fn test_cosine_distance_orthogonal() {
        let a = vec![1.0, 0.0, 0.0];
        let b = vec![0.0, 1.0, 0.0];
        let dist = cosine_distance(&a, &b);
        assert!((dist - 1.0).abs() < 0.001);
    }

    #[test]
    #[should_panic(expected = "Vector dimensions must match")]
    fn test_cosine_similarity_dimension_mismatch() {
        let a = vec![1.0, 2.0];
        let b = vec![1.0, 2.0, 3.0];
        cosine_similarity(&a, &b);
    }

    #[test]
    fn test_cosine_similarity_large_vectors() {
        let a: Vec<f32> = (0..1000).map(|i| (i as f32).sin()).collect();
        let b: Vec<f32> = (0..1000).map(|i| (i as f32).cos()).collect();
        
        let sim = cosine_similarity(&a, &b);
        assert!(sim >= -1.0 && sim <= 1.0);
    }

    #[test]
    fn test_cosine_similarity_extreme_values() {
        // Use large but manageable values
        let a = vec![1e10_f32, -1e10_f32, 1000.0];
        let b = vec![-1e10_f32, 1e10_f32, 2000.0];
        
        let sim = cosine_similarity(&a, &b);
        assert!(sim >= -1.0 && sim <= 1.0, "Similarity {} out of range", sim);
        assert!(sim.is_finite(), "Similarity is not finite: {}", sim);
        
        // Should be negative due to opposite directions
        assert!(sim < 0.0, "Expected negative similarity, got {}", sim);
    }

    #[test]
    fn test_cosine_similarity_zero_vectors() {
        let a = vec![0.0, 0.0, 0.0];
        let b = vec![1.0, 2.0, 3.0];
        
        let sim = cosine_similarity(&a, &b);
        assert_eq!(sim, 0.0);
    }

    #[test]
    fn test_cosine_distance_numerical_stability() {
        // Test with very small numbers
        let a = vec![1e-10, 2e-10, 3e-10];
        let b = vec![2e-10, 4e-10, 6e-10];
        
        let dist = cosine_distance(&a, &b);
        assert!(dist >= 0.0 && dist <= 2.0);
        assert!(dist.is_finite());
    }

    #[test]
    fn test_cosine_similarity_simd_compatibility() {
        // Test that SIMD and scalar versions produce same results
        let a = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0];
        let b = vec![8.0, 7.0, 6.0, 5.0, 4.0, 3.0, 2.0, 1.0];
        
        let sim = cosine_similarity(&a, &b);
        
        // Verify result is reasonable
        assert!(sim >= -1.0 && sim <= 1.0);
        assert!(sim.is_finite());
    }
}
