//! Euclidean distance computation with SIMD optimization

#[cfg(target_arch = "aarch64")]
use std::arch::aarch64::*;
#[cfg(target_arch = "x86_64")]
use std::arch::x86_64::*;

/// Compute Euclidean distance between two vectors with SIMD optimization
///
/// # Arguments
/// * `a` - First vector
/// * `b` - Second vector
///
/// # Returns
/// The Euclidean distance (L2 norm) between vectors
///
/// # Panics
/// Panics if vectors have different dimensions
#[inline]
pub fn euclidean_distance(a: &[f32], b: &[f32]) -> f32 {
    euclidean_distance_squared(a, b).sqrt()
}

/// Compute squared Euclidean distance (avoids sqrt for comparison purposes) with SIMD optimization
#[inline]
pub fn euclidean_distance_squared(a: &[f32], b: &[f32]) -> f32 {
    assert_eq!(a.len(), b.len(), "Vector dimensions must match");

    // 🚀 编译时启用 AVX2（避免运行时检测开销）
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    {
        unsafe { euclidean_distance_squared_avx2(a, b) }
    }

    // 否则，运行时检测
    #[cfg(all(target_arch = "x86_64", not(target_feature = "avx2")))]
    {
        if is_x86_feature_detected!("avx2") && a.len() >= 8 {
            unsafe { euclidean_distance_squared_avx2(a, b) }
        } else if is_x86_feature_detected!("sse") && a.len() >= 4 {
            unsafe { euclidean_distance_squared_sse(a, b) }
        } else {
            euclidean_distance_squared_scalar(a, b)
        }
    }

    #[cfg(target_arch = "aarch64")]
    {
        if a.len() >= 4 {
            unsafe { euclidean_distance_squared_neon(a, b) }
        } else {
            euclidean_distance_squared_scalar(a, b)
        }
    }

    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    {
        euclidean_distance_squared_scalar(a, b)
    }
}

/// AVX2优化的平方欧几里得距离计算
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn euclidean_distance_squared_avx2(a: &[f32], b: &[f32]) -> f32 {
    let n = a.len();
    let chunks = n / 8;
    let remainder = n % 8;

    // AVX2并行处理8个元素
    let mut sum_vec = _mm256_setzero_ps();

    for i in 0..chunks {
        let offset = i * 8;
        let a_vec = _mm256_loadu_ps(a.as_ptr().add(offset));
        let b_vec = _mm256_loadu_ps(b.as_ptr().add(offset));

        // 计算差值
        let diff = _mm256_sub_ps(a_vec, b_vec);
        // 计算平方并累加
        let sq = _mm256_mul_ps(diff, diff);
        sum_vec = _mm256_add_ps(sum_vec, sq);
    }

    // 🚀 优化：直接水平求和（避免存储到数组）
    let mut sum_squared = horizontal_sum_avx2_fast(sum_vec);

    // 处理剩余元素
    for i in (n - remainder)..n {
        let diff = a[i] - b[i];
        sum_squared += diff * diff;
    }

    sum_squared
}

/// SSE优化的平方欧几里得距离计算
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse")]
unsafe fn euclidean_distance_squared_sse(a: &[f32], b: &[f32]) -> f32 {
    let n = a.len();
    let chunks = n / 4;
    let remainder = n % 4;

    #[allow(unused_assignments)]
    let mut sum_squared = 0.0f32;

    // SSE并行处理4个元素
    let mut sum_vec = _mm_setzero_ps();

    for i in 0..chunks {
        let offset = i * 4;
        let a_vec = _mm_loadu_ps(a.as_ptr().add(offset));
        let b_vec = _mm_loadu_ps(b.as_ptr().add(offset));

        // 计算差值
        let diff = _mm_sub_ps(a_vec, b_vec);
        // 计算平方并累加
        sum_vec = _mm_add_ps(sum_vec, _mm_mul_ps(diff, diff));
    }

    // 水平求和
    sum_squared = horizontal_sum_sse(sum_vec);

    // 处理剩余元素
    for i in (n - remainder)..n {
        let diff = a[i] - b[i];
        sum_squared += diff * diff;
    }

    sum_squared
}

/// 标量版本（无SIMD）
fn euclidean_distance_squared_scalar(a: &[f32], b: &[f32]) -> f32 {
    let mut sum_squared = 0.0f32;

    for i in 0..a.len() {
        let diff = a[i] - b[i];
        sum_squared += diff * diff;
    }

    sum_squared
}

/// ARM NEON optimized squared Euclidean distance (4-way loop unroll)
#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "neon")]
#[inline]
unsafe fn euclidean_distance_squared_neon(a: &[f32], b: &[f32]) -> f32 {
    let n = a.len();
    let chunks = n / 16;
    let remainder = n % 16;

    let mut sum1 = vdupq_n_f32(0.0);
    let mut sum2 = vdupq_n_f32(0.0);
    let mut sum3 = vdupq_n_f32(0.0);
    let mut sum4 = vdupq_n_f32(0.0);

    // 4-way unrolled loop (16 floats per iteration)
    for i in 0..chunks {
        let offset = i * 16;

        let a_vec1 = vld1q_f32(a.as_ptr().add(offset));
        let b_vec1 = vld1q_f32(b.as_ptr().add(offset));
        let a_vec2 = vld1q_f32(a.as_ptr().add(offset + 4));
        let b_vec2 = vld1q_f32(b.as_ptr().add(offset + 4));
        let a_vec3 = vld1q_f32(a.as_ptr().add(offset + 8));
        let b_vec3 = vld1q_f32(b.as_ptr().add(offset + 8));
        let a_vec4 = vld1q_f32(a.as_ptr().add(offset + 12));
        let b_vec4 = vld1q_f32(b.as_ptr().add(offset + 12));

        let diff1 = vsubq_f32(a_vec1, b_vec1);
        let diff2 = vsubq_f32(a_vec2, b_vec2);
        let diff3 = vsubq_f32(a_vec3, b_vec3);
        let diff4 = vsubq_f32(a_vec4, b_vec4);

        sum1 = vfmaq_f32(sum1, diff1, diff1);
        sum2 = vfmaq_f32(sum2, diff2, diff2);
        sum3 = vfmaq_f32(sum3, diff3, diff3);
        sum4 = vfmaq_f32(sum4, diff4, diff4);
    }

    let combined = vaddq_f32(vaddq_f32(sum1, sum2), vaddq_f32(sum3, sum4));
    let mut total = vaddvq_f32(combined);

    // Process remainder (4 floats at a time)
    let offset_remainder = chunks * 16;
    let remainder_chunks = remainder / 4;

    let mut sum_rem = vdupq_n_f32(0.0);
    for i in 0..remainder_chunks {
        let offset = offset_remainder + i * 4;
        let a_vec = vld1q_f32(a.as_ptr().add(offset));
        let b_vec = vld1q_f32(b.as_ptr().add(offset));
        let diff = vsubq_f32(a_vec, b_vec);
        sum_rem = vfmaq_f32(sum_rem, diff, diff);
    }
    total += vaddvq_f32(sum_rem);

    // Scalar tail (< 4 elements)
    for i in (offset_remainder + remainder_chunks * 4)..n {
        let diff = a[i] - b[i];
        total += diff * diff;
    }

    total
}

/// AVX2水平求和（优化版本）
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
#[inline]
unsafe fn horizontal_sum_avx2_fast(v: __m256) -> f32 {
    // 将高128位和低128位相加
    let high = _mm256_extractf128_ps(v, 1);
    let low = _mm256_castps256_ps128(v);
    let sum128 = _mm_add_ps(high, low);

    // 水平求和128位
    let shuf = _mm_movehdup_ps(sum128);
    let sum64 = _mm_add_ps(sum128, shuf);
    let shuf2 = _mm_movehl_ps(shuf, sum64);
    let sum32 = _mm_add_ss(sum64, shuf2);

    _mm_cvtss_f32(sum32)
}

/// AVX2水平求和（原版本，保留用于对比）
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn horizontal_sum_avx2(v: __m256) -> f32 {
    // 将8个float两两相加
    let sum1 = _mm256_add_ps(v, _mm256_permute2f128_ps(v, v, 0x01));
    let sum2 = _mm256_hadd_ps(sum1, sum1);
    let sum3 = _mm256_hadd_ps(sum2, sum2);

    // 取第一个元素（总和）
    _mm256_cvtss_f32(sum3)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_euclidean_distance() {
        let a = vec![0.0, 0.0];
        let b = vec![3.0, 4.0];
        let dist = euclidean_distance(&a, &b);
        assert!((dist - 5.0).abs() < 0.001);
    }

    #[test]
    fn test_euclidean_distance_same_vector() {
        let a = vec![1.0, 2.0, 3.0];
        let dist = euclidean_distance(&a, &a);
        assert!(dist < 0.001);
    }

    #[test]
    fn test_euclidean_distance_squared() {
        let a = vec![0.0, 0.0];
        let b = vec![3.0, 4.0];
        let dist_sq = euclidean_distance_squared(&a, &b);
        assert!((dist_sq - 25.0).abs() < 0.001);
    }

    #[test]
    #[should_panic(expected = "Vector dimensions must match")]
    fn test_euclidean_distance_dimension_mismatch() {
        let a = vec![1.0, 2.0];
        let b = vec![1.0, 2.0, 3.0];
        euclidean_distance(&a, &b);
    }

    #[test]
    fn test_euclidean_distance_large_vectors() {
        let a: Vec<f32> = (0..1000).map(|i| i as f32).collect();
        let b: Vec<f32> = (0..1000).map(|i| (i * 2) as f32).collect();

        let dist = euclidean_distance(&a, &b);
        assert!(dist.is_finite());
        assert!(dist > 0.0);
    }

    #[test]
    fn test_euclidean_distance_extreme_values() {
        // Use large but manageable values
        let a = vec![1e10_f32, -1e10_f32, 1000.0];
        let b = vec![-1e10_f32, 1e10_f32, 2000.0];

        let dist = euclidean_distance(&a, &b);
        assert!(dist.is_finite(), "Distance is not finite: {}", dist);
        assert!(dist >= 0.0, "Distance is negative: {}", dist);
        assert!(
            dist > 0.0,
            "Distance should be positive for different vectors"
        );
    }

    #[test]
    fn test_euclidean_distance_numerical_stability() {
        // Test with very small numbers
        let a = vec![1e-10, 2e-10, 3e-10];
        let b = vec![2e-10, 4e-10, 6e-10];

        let dist = euclidean_distance(&a, &b);
        assert!(dist.is_finite());
        assert!(dist >= 0.0);
    }

    #[test]
    fn test_euclidean_distance_simd_compatibility() {
        // Test with vector size that should trigger SIMD optimizations
        let a = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0];
        let b = vec![8.0, 7.0, 6.0, 5.0, 4.0, 3.0, 2.0, 1.0];

        let dist = euclidean_distance(&a, &b);
        assert!(dist.is_finite());
        assert!(dist >= 0.0);
    }

    #[test]
    fn test_euclidean_distance_squared_vs_sqrt() {
        let a = vec![3.0, 4.0];
        let b = vec![0.0, 0.0];

        let dist_sq = euclidean_distance_squared(&a, &b);
        let dist = euclidean_distance(&a, &b);

        assert_eq!(dist_sq, 25.0);
        assert!((dist - 5.0).abs() < 1e-6);
    }
}

/// b 侧为盘上 LE f32 字节切片 (任意对齐) 的欧氏平方距离变体 — 流式 knn
/// 直接喂 mmap 借出的列字节, 免对齐缓冲复制趟 (154MB 表 ~8-12ms)。
/// aarch64 vld1q / x86 loadu 非对齐加载原生支持; 标量回退用 from_le_bytes
/// (可移植, 慢路径)。
pub fn euclidean_distance_squared_bytes(a: &[f32], b: &[u8]) -> f32 {
    let n = a.len();
    debug_assert_eq!(b.len(), n * 4, "byte slice must hold exactly n f32 (LE)");

    #[cfg(target_arch = "aarch64")]
    {
        unsafe { euclid_sq_neon_ab(a, b.as_ptr()) }
    }
    #[cfg(target_arch = "x86_64")]
    {
        if std::arch::is_x86_feature_detected!("avx2") && n >= 8 {
            unsafe { euclid_sq_avx2_ab(a, b.as_ptr()) }
        } else {
            euclid_sq_scalar_bytes(a, b)
        }
    }
    #[cfg(not(any(target_arch = "aarch64", target_arch = "x86_64")))]
    {
        euclid_sq_scalar_bytes(a, b)
    }
}

/// 可移植标量回退 (BE 主机亦正确)。
fn euclid_sq_scalar_bytes(a: &[f32], b: &[u8]) -> f32 {
    let mut sum = 0.0f32;
    for (i, av) in a.iter().enumerate() {
        let o = i * 4;
        let bv = f32::from_le_bytes([b[o], b[o + 1], b[o + 2], b[o + 3]]);
        let diff = av - bv;
        sum += diff * diff;
    }
    sum
}

/// NEON: b 侧非对齐字节指针 (vld1q 无对齐要求)。
#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "neon")]
#[inline]
unsafe fn euclid_sq_neon_ab(a: &[f32], b: *const u8) -> f32 {
    use std::arch::aarch64::*;
    let n = a.len();
    let chunks = n / 16;
    let bp = b as *const f32;
    let mut sum1 = vdupq_n_f32(0.0);
    let mut sum2 = vdupq_n_f32(0.0);
    let mut sum3 = vdupq_n_f32(0.0);
    let mut sum4 = vdupq_n_f32(0.0);
    for i in 0..chunks {
        let o = i * 16;
        let a1 = vld1q_f32(a.as_ptr().add(o));
        let b1 = vld1q_f32(bp.add(o));
        let a2 = vld1q_f32(a.as_ptr().add(o + 4));
        let b2 = vld1q_f32(bp.add(o + 4));
        let a3 = vld1q_f32(a.as_ptr().add(o + 8));
        let b3 = vld1q_f32(bp.add(o + 8));
        let a4 = vld1q_f32(a.as_ptr().add(o + 12));
        let b4 = vld1q_f32(bp.add(o + 12));
        let d1 = vsubq_f32(a1, b1);
        let d2 = vsubq_f32(a2, b2);
        let d3 = vsubq_f32(a3, b3);
        let d4 = vsubq_f32(a4, b4);
        sum1 = vfmaq_f32(sum1, d1, d1);
        sum2 = vfmaq_f32(sum2, d2, d2);
        sum3 = vfmaq_f32(sum3, d3, d3);
        sum4 = vfmaq_f32(sum4, d4, d4);
    }
    let combined = vaddq_f32(vaddq_f32(sum1, sum2), vaddq_f32(sum3, sum4));
    let mut total = vaddvq_f32(combined);
    let off = chunks * 16;
    let rem4 = (n - off) / 4;
    let mut sum_rem = vdupq_n_f32(0.0);
    for i in 0..rem4 {
        let o = off + i * 4;
        let av = vld1q_f32(a.as_ptr().add(o));
        let bv = vld1q_f32(bp.add(o));
        let d = vsubq_f32(av, bv);
        sum_rem = vfmaq_f32(sum_rem, d, d);
    }
    total += vaddvq_f32(sum_rem);
    for i in (off + rem4 * 4)..n {
        let bo = i * 4;
        let bv = f32::from_le_bytes(std::ptr::read_unaligned(b.add(bo) as *const [u8; 4]));
        let d = a[i] - bv;
        total += d * d;
    }
    total
}

/// AVX2: b 侧非对齐 (loadu)。
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
#[inline]
unsafe fn euclid_sq_avx2_ab(a: &[f32], b: *const u8) -> f32 {
    use std::arch::x86_64::*;
    let n = a.len();
    let bp = b as *const f32;
    let chunks = n / 8;
    let mut sum = _mm256_setzero_ps();
    for i in 0..chunks {
        let o = i * 8;
        let av = _mm256_loadu_ps(a.as_ptr().add(o));
        let bv = _mm256_loadu_ps(bp.add(o));
        let d = _mm256_sub_ps(av, bv);
        sum = _mm256_fmadd_ps(d, d, sum);
    }
    let mut arr = [0f32; 8];
    _mm256_storeu_ps(arr.as_mut_ptr(), sum);
    let mut total: f32 = arr.iter().sum();
    for i in (chunks * 8)..n {
        let bo = i * 4;
        let bv = f32::from_le_bytes(std::ptr::read_unaligned(b.add(bo) as *const [u8; 4]));
        let d = a[i] - bv;
        total += d * d;
    }
    total
}
