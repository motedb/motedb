//! SIMD 加速的 SQL 算子
//!
//! 提供高性能的聚合、过滤和扫描操作：
//! - x86_64: AVX2/SSE2
//! - aarch64: NEON
//! - fallback: 标量实现

use crate::types::Value;

/// SIMD 加速的 SUM 聚合
pub fn simd_sum_i64(values: &[i64]) -> i64 {
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    {
        unsafe { simd_sum_i64_avx2(values) }
    }
    
    #[cfg(all(
        target_arch = "aarch64",
        target_feature = "neon",
        not(all(target_arch = "x86_64", target_feature = "avx2"))
    ))]
    {
        unsafe { simd_sum_i64_neon(values) }
    }
    
    #[cfg(not(any(
        all(target_arch = "x86_64", target_feature = "avx2"),
        all(target_arch = "aarch64", target_feature = "neon")
    )))]
    {
        simd_sum_i64_fallback(values)
    }
}

/// SIMD 加速的 SUM (f64)
pub fn simd_sum_f64(values: &[f64]) -> f64 {
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    {
        unsafe { simd_sum_f64_avx2(values) }
    }
    
    #[cfg(all(
        target_arch = "aarch64",
        target_feature = "neon",
        not(all(target_arch = "x86_64", target_feature = "avx2"))
    ))]
    {
        unsafe { simd_sum_f64_neon(values) }
    }
    
    #[cfg(not(any(
        all(target_arch = "x86_64", target_feature = "avx2"),
        all(target_arch = "aarch64", target_feature = "neon")
    )))]
    {
        simd_sum_f64_fallback(values)
    }
}

/// SIMD 加速的 MIN (i64)
pub fn simd_min_i64(values: &[i64]) -> Option<i64> {
    if values.is_empty() {
        return None;
    }
    
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    {
        Some(unsafe { simd_min_i64_avx2(values) })
    }
    
    #[cfg(all(
        target_arch = "aarch64",
        target_feature = "neon",
        not(all(target_arch = "x86_64", target_feature = "avx2"))
    ))]
    {
        Some(unsafe { simd_min_i64_neon(values) })
    }
    
    #[cfg(not(any(
        all(target_arch = "x86_64", target_feature = "avx2"),
        all(target_arch = "aarch64", target_feature = "neon")
    )))]
    {
        Some(simd_min_i64_fallback(values))
    }
}

/// SIMD 加速的 MAX (i64)
pub fn simd_max_i64(values: &[i64]) -> Option<i64> {
    if values.is_empty() {
        return None;
    }
    
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    {
        Some(unsafe { simd_max_i64_avx2(values) })
    }
    
    #[cfg(all(
        target_arch = "aarch64",
        target_feature = "neon",
        not(all(target_arch = "x86_64", target_feature = "avx2"))
    ))]
    {
        Some(unsafe { simd_max_i64_neon(values) })
    }
    
    #[cfg(not(any(
        all(target_arch = "x86_64", target_feature = "avx2"),
        all(target_arch = "aarch64", target_feature = "neon")
    )))]
    {
        Some(simd_max_i64_fallback(values))
    }
}

/// SIMD 加速的 WHERE 过滤 (等值比较)
/// 返回满足条件的索引
pub fn simd_filter_eq_i64(values: &[i64], target: i64) -> Vec<usize> {
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    {
        unsafe { simd_filter_eq_i64_avx2(values, target) }
    }
    
    #[cfg(all(
        target_arch = "aarch64",
        target_feature = "neon",
        not(all(target_arch = "x86_64", target_feature = "avx2"))
    ))]
    {
        unsafe { simd_filter_eq_i64_neon(values, target) }
    }
    
    #[cfg(not(any(
        all(target_arch = "x86_64", target_feature = "avx2"),
        all(target_arch = "aarch64", target_feature = "neon")
    )))]
    {
        simd_filter_eq_i64_fallback(values, target)
    }
}

/// SIMD 加速的范围过滤 (min <= value <= max)
pub fn simd_filter_range_i64(values: &[i64], min: i64, max: i64) -> Vec<usize> {
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    {
        unsafe { simd_filter_range_i64_avx2(values, min, max) }
    }
    
    #[cfg(all(
        target_arch = "aarch64",
        target_feature = "neon",
        not(all(target_arch = "x86_64", target_feature = "avx2"))
    ))]
    {
        unsafe { simd_filter_range_i64_neon(values, min, max) }
    }
    
    #[cfg(not(any(
        all(target_arch = "x86_64", target_feature = "avx2"),
        all(target_arch = "aarch64", target_feature = "neon")
    )))]
    {
        simd_filter_range_i64_fallback(values, min, max)
    }
}

//=============================================================================
// Fallback implementations (标量)
//=============================================================================

fn simd_sum_i64_fallback(values: &[i64]) -> i64 {
    values.iter().sum()
}

fn simd_sum_f64_fallback(values: &[f64]) -> f64 {
    values.iter().sum()
}

fn simd_min_i64_fallback(values: &[i64]) -> i64 {
    values.iter().min().copied().unwrap_or(i64::MAX)
}

fn simd_max_i64_fallback(values: &[i64]) -> i64 {
    values.iter().max().copied().unwrap_or(i64::MIN)
}

fn simd_filter_eq_i64_fallback(values: &[i64], target: i64) -> Vec<usize> {
    values.iter()
        .enumerate()
        .filter_map(|(i, &v)| if v == target { Some(i) } else { None })
        .collect()
}

fn simd_filter_range_i64_fallback(values: &[i64], min: i64, max: i64) -> Vec<usize> {
    values.iter()
        .enumerate()
        .filter_map(|(i, &v)| if v >= min && v <= max { Some(i) } else { None })
        .collect()
}

//=============================================================================
// AVX2 implementations (x86_64)
//=============================================================================

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn simd_sum_i64_avx2(values: &[i64]) -> i64 {
    #[cfg(target_arch = "x86_64")]
    use std::arch::x86_64::*;
    
    let len = values.len();
    let mut sum = _mm256_setzero_si256();
    
    // 处理 4 个 i64 的块 (AVX2 = 256-bit = 4 × 64-bit)
    let chunks = len / 4;
    let remainder = len % 4;
    
    for i in 0..chunks {
        let idx = i * 4;
        let data = _mm256_loadu_si256(values.as_ptr().add(idx) as *const __m256i);
        sum = _mm256_add_epi64(sum, data);
    }
    
    // 水平求和
    let mut result = [0i64; 4];
    _mm256_storeu_si256(result.as_mut_ptr() as *mut __m256i, sum);
    let mut total = result.iter().sum::<i64>();
    
    // 处理剩余元素
    for i in (chunks * 4)..len {
        total += values[i];
    }
    
    total
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn simd_sum_f64_avx2(values: &[f64]) -> f64 {
    #[cfg(target_arch = "x86_64")]
    use std::arch::x86_64::*;
    
    let len = values.len();
    let mut sum = _mm256_setzero_pd();
    
    // 处理 4 个 f64 的块
    let chunks = len / 4;
    let remainder = len % 4;
    
    for i in 0..chunks {
        let idx = i * 4;
        let data = _mm256_loadu_pd(values.as_ptr().add(idx));
        sum = _mm256_add_pd(sum, data);
    }
    
    // 水平求和
    let mut result = [0.0f64; 4];
    _mm256_storeu_pd(result.as_mut_ptr(), sum);
    let mut total = result.iter().sum::<f64>();
    
    // 处理剩余元素
    for i in (chunks * 4)..len {
        total += values[i];
    }
    
    total
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn simd_min_i64_avx2(values: &[i64]) -> i64 {
    #[cfg(target_arch = "x86_64")]
    use std::arch::x86_64::*;
    
    let len = values.len();
    if len == 0 {
        return i64::MAX;
    }
    
    // 初始化为第一个元素
    let mut min_vec = _mm256_set1_epi64x(values[0]);
    
    // 处理 4 个 i64 的块
    let chunks = len / 4;
    
    for i in 0..chunks {
        let idx = i * 4;
        let data = _mm256_loadu_si256(values.as_ptr().add(idx) as *const __m256i);
        
        // AVX2 没有直接的 i64 min，使用比较 + blend
        let cmp = _mm256_cmpgt_epi64(min_vec, data);
        min_vec = _mm256_blendv_epi8(min_vec, data, cmp);
    }
    
    // 提取最小值
    let mut result = [0i64; 4];
    _mm256_storeu_si256(result.as_mut_ptr() as *mut __m256i, min_vec);
    let mut min_val = result.iter().min().copied().unwrap_or(i64::MAX);
    
    // 处理剩余元素
    for i in (chunks * 4)..len {
        min_val = min_val.min(values[i]);
    }
    
    min_val
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn simd_max_i64_avx2(values: &[i64]) -> i64 {
    #[cfg(target_arch = "x86_64")]
    use std::arch::x86_64::*;
    
    let len = values.len();
    if len == 0 {
        return i64::MIN;
    }
    
    let mut max_vec = _mm256_set1_epi64x(values[0]);
    
    let chunks = len / 4;
    
    for i in 0..chunks {
        let idx = i * 4;
        let data = _mm256_loadu_si256(values.as_ptr().add(idx) as *const __m256i);
        
        // 使用比较 + blend
        let cmp = _mm256_cmpgt_epi64(data, max_vec);
        max_vec = _mm256_blendv_epi8(max_vec, data, cmp);
    }
    
    let mut result = [0i64; 4];
    _mm256_storeu_si256(result.as_mut_ptr() as *mut __m256i, max_vec);
    let mut max_val = result.iter().max().copied().unwrap_or(i64::MIN);
    
    for i in (chunks * 4)..len {
        max_val = max_val.max(values[i]);
    }
    
    max_val
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn simd_filter_eq_i64_avx2(values: &[i64], target: i64) -> Vec<usize> {
    #[cfg(target_arch = "x86_64")]
    use std::arch::x86_64::*;
    
    let len = values.len();
    // 🚀 P1 优化：预分配容量（估算 25% 匹配率）
    let mut results = Vec::with_capacity(len / 4);
    
    let target_vec = _mm256_set1_epi64x(target);
    let chunks = len / 4;
    
    for i in 0..chunks {
        let idx = i * 4;
        let data = _mm256_loadu_si256(values.as_ptr().add(idx) as *const __m256i);
        
        // 比较相等
        let cmp = _mm256_cmpeq_epi64(data, target_vec);
        let mask = _mm256_movemask_pd(_mm256_castsi256_pd(cmp));
        
        // 检查每个位
        if mask & 0x1 != 0 { results.push(idx); }
        if mask & 0x2 != 0 { results.push(idx + 1); }
        if mask & 0x4 != 0 { results.push(idx + 2); }
        if mask & 0x8 != 0 { results.push(idx + 3); }
    }
    
    // 处理剩余元素
    for i in (chunks * 4)..len {
        if values[i] == target {
            results.push(i);
        }
    }
    
    results
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn simd_filter_range_i64_avx2(values: &[i64], min: i64, max: i64) -> Vec<usize> {
    #[cfg(target_arch = "x86_64")]
    use std::arch::x86_64::*;
    
    let len = values.len();
    // 🚀 P1 优化：预分配容量（估算 50% 匹配率）
    let mut results = Vec::with_capacity(len / 2);
    
    let min_vec = _mm256_set1_epi64x(min);
    let max_vec = _mm256_set1_epi64x(max);
    let chunks = len / 4;
    
    for i in 0..chunks {
        let idx = i * 4;
        let data = _mm256_loadu_si256(values.as_ptr().add(idx) as *const __m256i);
        
        // value >= min
        let cmp_min = _mm256_or_si256(
            _mm256_cmpgt_epi64(data, min_vec),
            _mm256_cmpeq_epi64(data, min_vec)
        );
        
        // value <= max
        let cmp_max = _mm256_or_si256(
            _mm256_cmpgt_epi64(max_vec, data),
            _mm256_cmpeq_epi64(data, max_vec)
        );
        
        // min <= value <= max
        let cmp = _mm256_and_si256(cmp_min, cmp_max);
        let mask = _mm256_movemask_pd(_mm256_castsi256_pd(cmp));
        
        if mask & 0x1 != 0 { results.push(idx); }
        if mask & 0x2 != 0 { results.push(idx + 1); }
        if mask & 0x4 != 0 { results.push(idx + 2); }
        if mask & 0x8 != 0 { results.push(idx + 3); }
    }
    
    // 处理剩余元素
    for i in (chunks * 4)..len {
        if values[i] >= min && values[i] <= max {
            results.push(i);
        }
    }
    
    results
}

//=============================================================================
// NEON implementations (aarch64)
//=============================================================================

#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "neon")]
unsafe fn simd_sum_i64_neon(values: &[i64]) -> i64 {
    #[cfg(target_arch = "aarch64")]
    use std::arch::aarch64::*;
    
    let len = values.len();
    let mut sum = vdupq_n_s64(0);
    
    // 处理 2 个 i64 的块 (NEON = 128-bit = 2 × 64-bit)
    let chunks = len / 2;
    let remainder = len % 2;
    
    for i in 0..chunks {
        let idx = i * 2;
        let data = vld1q_s64(values.as_ptr().add(idx));
        sum = vaddq_s64(sum, data);
    }
    
    // 提取结果
    let result = [vgetq_lane_s64(sum, 0), vgetq_lane_s64(sum, 1)];
    let mut total = result.iter().sum::<i64>();
    
    // 处理剩余元素
    for i in (chunks * 2)..len {
        total += values[i];
    }
    
    total
}

#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "neon")]
unsafe fn simd_sum_f64_neon(values: &[f64]) -> f64 {
    #[cfg(target_arch = "aarch64")]
    use std::arch::aarch64::*;
    
    let len = values.len();
    let mut sum = vdupq_n_f64(0.0);
    
    let chunks = len / 2;
    
    for i in 0..chunks {
        let idx = i * 2;
        let data = vld1q_f64(values.as_ptr().add(idx));
        sum = vaddq_f64(sum, data);
    }
    
    let result = [vgetq_lane_f64(sum, 0), vgetq_lane_f64(sum, 1)];
    let mut total = result.iter().sum::<f64>();
    
    for i in (chunks * 2)..len {
        total += values[i];
    }
    
    total
}

#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "neon")]
unsafe fn simd_min_i64_neon(values: &[i64]) -> i64 {
    // NEON 不直接支持 i64 min，回退到标量
    simd_min_i64_fallback(values)
}

#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "neon")]
unsafe fn simd_max_i64_neon(values: &[i64]) -> i64 {
    // NEON 不直接支持 i64 max，回退到标量
    simd_max_i64_fallback(values)
}

#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "neon")]
unsafe fn simd_filter_eq_i64_neon(values: &[i64], target: i64) -> Vec<usize> {
    #[cfg(target_arch = "aarch64")]
    use std::arch::aarch64::*;
    
    let len = values.len();
    // 🚀 P1 优化：预分配容量（估算 25% 匹配率）
    let mut results = Vec::with_capacity(len / 4);
    
    let target_vec = vdupq_n_s64(target);
    let chunks = len / 2;
    
    for i in 0..chunks {
        let idx = i * 2;
        let data = vld1q_s64(values.as_ptr().add(idx));
        let cmp = vceqq_s64(data, target_vec);
        
        // 提取比较结果 (cmp is uint64x2_t from vceqq_s64)
        let result_arr = [vgetq_lane_u64(cmp, 0), vgetq_lane_u64(cmp, 1)];
        
        if result_arr[0] != 0 {
            results.push(idx);
        }
        if result_arr[1] != 0 {
            results.push(idx + 1);
        }
    }
    
    for i in (chunks * 2)..len {
        if values[i] == target {
            results.push(i);
        }
    }
    
    results
}

#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "neon")]
unsafe fn simd_filter_range_i64_neon(values: &[i64], min: i64, max: i64) -> Vec<usize> {
    #[cfg(target_arch = "aarch64")]
    use std::arch::aarch64::*;
    
    let len = values.len();
    // 🚀 P1 优化：预分配容量（估算 50% 匹配率）
    let mut results = Vec::with_capacity(len / 2);
    
    let min_vec = vdupq_n_s64(min);
    let max_vec = vdupq_n_s64(max);
    let chunks = len / 2;
    
    for i in 0..chunks {
        let idx = i * 2;
        let data = vld1q_s64(values.as_ptr().add(idx));
        
        // value >= min
        let cmp_min = vcgeq_s64(data, min_vec);
        // value <= max
        let cmp_max = vcleq_s64(data, max_vec);
        // AND
        let cmp = vandq_u64(cmp_min, cmp_max);
        
        if vgetq_lane_u64(cmp, 0) != 0 {
            results.push(idx);
        }
        if vgetq_lane_u64(cmp, 1) != 0 {
            results.push(idx + 1);
        }
    }
    
    for i in (chunks * 2)..len {
        if values[i] >= min && values[i] <= max {
            results.push(i);
        }
    }
    
    results
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_simd_sum_i64() {
        let values = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        let sum = simd_sum_i64(&values);
        assert_eq!(sum, 55);
    }
    
    #[test]
    fn test_simd_sum_f64() {
        let values = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let sum = simd_sum_f64(&values);
        assert!((sum - 15.0).abs() < 1e-10);
    }
    
    #[test]
    fn test_simd_min_max() {
        let values = vec![5, 2, 9, 1, 7, 3];
        assert_eq!(simd_min_i64(&values), Some(1));
        assert_eq!(simd_max_i64(&values), Some(9));
    }
    
    #[test]
    fn test_simd_filter_eq() {
        let values = vec![1, 2, 3, 2, 4, 2, 5];
        let indices = simd_filter_eq_i64(&values, 2);
        assert_eq!(indices, vec![1, 3, 5]);
    }
    
    #[test]
    fn test_simd_filter_range() {
        let values = vec![1, 5, 10, 15, 20, 25, 30];
        let indices = simd_filter_range_i64(&values, 10, 20);
        assert_eq!(indices, vec![2, 3, 4]);
    }
    
    #[test]
    fn test_large_dataset() {
        // 测试大数据集 (100K 元素)
        let values: Vec<i64> = (0..100_000).collect();
        let sum = simd_sum_i64(&values);
        assert_eq!(sum, 4_999_950_000);
    }
}
