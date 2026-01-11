//! Checksum 验证模块
//!
//! 提供统一的数据完整性校验，防止数据损坏和静默错误。
//! 
//! ## 使用场景
//! - Superblock 元数据校验
//! - LSM 文件数据块校验
//! - WAL 日志记录校验
//! - Manifest 文件校验
//!
//! ## 算法选择
//! - **CRC32C**: 硬件加速（SSE4.2），适合频繁校验（默认）
//! - **xxHash**: 极速，适合大数据块
//!
//! ## 使用示例
//! ```ignore
//! use motedb::storage::checksum::{Checksum, ChecksumType};
//!
//! // 写入时计算 checksum
//! let data = b"Hello, MoteDB!";
//! let checksum = Checksum::compute(ChecksumType::CRC32C, data);
//!
//! // 读取时验证 checksum
//! Checksum::verify(ChecksumType::CRC32C, data, checksum)?;
//! ```

use crc32fast::Hasher;
use std::io::{self, Write};

/// Checksum 类型
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChecksumType {
    /// CRC32C (硬件加速，SSE4.2)
    CRC32C,
    /// 无校验（仅测试用）
    None,
}

impl Default for ChecksumType {
    fn default() -> Self {
        Self::CRC32C
    }
}

/// Checksum 计算器
pub struct Checksum;

impl Checksum {
    /// 计算数据的 checksum
    ///
    /// # 参数
    /// - `checksum_type`: 校验类型
    /// - `data`: 待校验的数据
    ///
    /// # 返回
    /// - 32-bit checksum 值
    ///
    /// # 示例
    /// ```ignore
    /// let checksum = Checksum::compute(ChecksumType::CRC32C, b"data");
    /// ```
    pub fn compute(checksum_type: ChecksumType, data: &[u8]) -> u32 {
        match checksum_type {
            ChecksumType::CRC32C => {
                let mut hasher = Hasher::new();
                hasher.update(data);
                hasher.finalize()
            }
            ChecksumType::None => 0,
        }
    }

    /// 验证数据的 checksum
    ///
    /// # 参数
    /// - `checksum_type`: 校验类型
    /// - `data`: 待验证的数据
    /// - `expected`: 期望的 checksum 值
    ///
    /// # 返回
    /// - `Ok(())`: 校验通过
    /// - `Err(ChecksumError)`: 校验失败
    ///
    /// # 示例
    /// ```ignore
    /// Checksum::verify(ChecksumType::CRC32C, b"data", expected_checksum)?;
    /// ```
    pub fn verify(
        checksum_type: ChecksumType,
        data: &[u8],
        expected: u32,
    ) -> Result<(), ChecksumError> {
        if checksum_type == ChecksumType::None {
            return Ok(());
        }

        let actual = Self::compute(checksum_type, data);
        if actual != expected {
            return Err(ChecksumError::Mismatch {
                expected,
                actual,
                data_len: data.len(),
            });
        }

        Ok(())
    }

    /// 增量计算 checksum（用于流式数据）
    ///
    /// # 示例
    /// ```ignore
    /// let mut builder = ChecksumBuilder::new(ChecksumType::CRC32C);
    /// builder.update(b"Hello, ");
    /// builder.update(b"World!");
    /// let checksum = builder.finalize();
    /// ```
    pub fn builder(checksum_type: ChecksumType) -> ChecksumBuilder {
        ChecksumBuilder::new(checksum_type)
    }

    /// 为带 checksum 的数据块编码
    ///
    /// 格式: [data_len: u32][data: [u8]][checksum: u32]
    ///
    /// # 示例
    /// ```ignore
    /// let encoded = Checksum::encode_with_checksum(ChecksumType::CRC32C, b"data");
    /// ```
    pub fn encode_with_checksum(checksum_type: ChecksumType, data: &[u8]) -> Vec<u8> {
        let checksum = Self::compute(checksum_type, data);
        let mut encoded = Vec::with_capacity(4 + data.len() + 4);
        
        // Data length
        encoded.extend_from_slice(&(data.len() as u32).to_le_bytes());
        
        // Data
        encoded.extend_from_slice(data);
        
        // Checksum
        encoded.extend_from_slice(&checksum.to_le_bytes());
        
        encoded
    }

    /// 解码并验证带 checksum 的数据块
    ///
    /// # 返回
    /// - `Ok(Vec<u8>)`: 验证通过的数据
    /// - `Err(ChecksumError)`: 解码失败或校验失败
    ///
    /// # 示例
    /// ```ignore
    /// let data = Checksum::decode_with_checksum(ChecksumType::CRC32C, &encoded)?;
    /// ```
    pub fn decode_with_checksum(
        checksum_type: ChecksumType,
        encoded: &[u8],
    ) -> Result<Vec<u8>, ChecksumError> {
        if encoded.len() < 8 {
            return Err(ChecksumError::InvalidFormat("Data too short".to_string()));
        }

        // Parse data length
        let data_len = u32::from_le_bytes([encoded[0], encoded[1], encoded[2], encoded[3]]) as usize;
        
        if encoded.len() != 4 + data_len + 4 {
            return Err(ChecksumError::InvalidFormat(format!(
                "Expected {} bytes, got {}",
                4 + data_len + 4,
                encoded.len()
            )));
        }

        // Extract data and checksum
        let data = &encoded[4..4 + data_len];
        let expected_checksum = u32::from_le_bytes([
            encoded[4 + data_len],
            encoded[4 + data_len + 1],
            encoded[4 + data_len + 2],
            encoded[4 + data_len + 3],
        ]);

        // Verify checksum
        Self::verify(checksum_type, data, expected_checksum)?;

        Ok(data.to_vec())
    }
}

/// Checksum 增量构建器（用于流式数据）
pub struct ChecksumBuilder {
    checksum_type: ChecksumType,
    hasher: Option<Hasher>,
}

impl ChecksumBuilder {
    /// 创建新的构建器
    pub fn new(checksum_type: ChecksumType) -> Self {
        let hasher = match checksum_type {
            ChecksumType::CRC32C => Some(Hasher::new()),
            ChecksumType::None => None,
        };

        Self {
            checksum_type,
            hasher,
        }
    }

    /// 更新数据
    pub fn update(&mut self, data: &[u8]) {
        if let Some(hasher) = &mut self.hasher {
            hasher.update(data);
        }
    }

    /// 完成并返回 checksum
    pub fn finalize(self) -> u32 {
        match self.hasher {
            Some(hasher) => hasher.finalize(),
            None => 0,
        }
    }
}

impl Write for ChecksumBuilder {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.update(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Checksum 错误类型
#[derive(Debug, thiserror::Error)]
pub enum ChecksumError {
    #[error("Checksum mismatch: expected {expected:#010x}, got {actual:#010x} (data_len={data_len})")]
    Mismatch {
        expected: u32,
        actual: u32,
        data_len: usize,
    },

    #[error("Invalid checksum format: {0}")]
    InvalidFormat(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_checksum_basic() {
        let data = b"Hello, MoteDB!";
        let checksum = Checksum::compute(ChecksumType::CRC32C, data);
        
        // 验证成功
        assert!(Checksum::verify(ChecksumType::CRC32C, data, checksum).is_ok());
        
        // 验证失败（错误的 checksum）
        assert!(Checksum::verify(ChecksumType::CRC32C, data, checksum + 1).is_err());
        
        // 验证失败（数据被篡改）
        let corrupted = b"Hello, MoteDB?";
        assert!(Checksum::verify(ChecksumType::CRC32C, corrupted, checksum).is_err());
    }

    #[test]
    fn test_checksum_none() {
        let data = b"Hello, MoteDB!";
        let checksum = Checksum::compute(ChecksumType::None, data);
        assert_eq!(checksum, 0);
        
        // None 类型总是验证通过
        assert!(Checksum::verify(ChecksumType::None, data, 12345).is_ok());
    }

    #[test]
    fn test_checksum_builder() {
        let data1 = b"Hello, ";
        let data2 = b"MoteDB!";
        
        // 增量计算
        let mut builder = Checksum::builder(ChecksumType::CRC32C);
        builder.update(data1);
        builder.update(data2);
        let checksum1 = builder.finalize();
        
        // 一次性计算
        let checksum2 = Checksum::compute(ChecksumType::CRC32C, b"Hello, MoteDB!");
        
        // 两种方式结果相同
        assert_eq!(checksum1, checksum2);
    }

    #[test]
    fn test_checksum_encode_decode() {
        let data = b"Hello, MoteDB! This is a test message.";
        
        // 编码
        let encoded = Checksum::encode_with_checksum(ChecksumType::CRC32C, data);
        
        // 解码
        let decoded = Checksum::decode_with_checksum(ChecksumType::CRC32C, &encoded).unwrap();
        
        assert_eq!(data, decoded.as_slice());
    }

    #[test]
    fn test_checksum_decode_corrupted() {
        let data = b"Hello, MoteDB!";
        let mut encoded = Checksum::encode_with_checksum(ChecksumType::CRC32C, data);
        
        // 篡改数据
        encoded[10] ^= 0xFF;
        
        // 解码失败
        let result = Checksum::decode_with_checksum(ChecksumType::CRC32C, &encoded);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), ChecksumError::Mismatch { .. }));
    }

    #[test]
    fn test_checksum_decode_invalid_format() {
        // 数据太短
        let short_data = b"abc";
        let result = Checksum::decode_with_checksum(ChecksumType::CRC32C, short_data);
        assert!(result.is_err());
        
        // 长度不匹配
        let mut invalid = vec![0u8; 20];
        invalid[0] = 100; // 声称有 100 字节数据，但实际只有 20 字节
        let result = Checksum::decode_with_checksum(ChecksumType::CRC32C, &invalid);
        assert!(result.is_err());
    }

    #[test]
    fn test_checksum_deterministic() {
        let data = b"Deterministic test";
        
        // 多次计算应该得到相同结果
        let checksum1 = Checksum::compute(ChecksumType::CRC32C, data);
        let checksum2 = Checksum::compute(ChecksumType::CRC32C, data);
        let checksum3 = Checksum::compute(ChecksumType::CRC32C, data);
        
        assert_eq!(checksum1, checksum2);
        assert_eq!(checksum2, checksum3);
    }

    #[test]
    fn test_checksum_empty_data() {
        let data = b"";
        let checksum = Checksum::compute(ChecksumType::CRC32C, data);
        
        // CRC32 对空数据返回 0（这是正确的行为）
        assert_eq!(checksum, 0);
        assert!(Checksum::verify(ChecksumType::CRC32C, data, checksum).is_ok());
    }

    #[test]
    fn test_checksum_builder_write_trait() {
        use std::io::Write;
        
        let mut builder = Checksum::builder(ChecksumType::CRC32C);
        
        // 使用 Write trait
        builder.write_all(b"Hello, ").unwrap();
        builder.write_all(b"MoteDB!").unwrap();
        builder.flush().unwrap();
        
        let checksum = builder.finalize();
        
        // 验证结果
        let expected = Checksum::compute(ChecksumType::CRC32C, b"Hello, MoteDB!");
        assert_eq!(checksum, expected);
    }
}
