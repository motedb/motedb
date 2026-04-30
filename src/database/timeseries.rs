//! Time-Series Stream Ingestion API
//!
//! Optimized for high-frequency sensor data (IMU 100Hz-1kHz, motor controllers).
//! Bypasses SQL parser for minimal overhead.

#[cfg(test)]
mod tests {
    use crate::config::DBConfig;
    use crate::database::write_controller::BackpressureSignal;

    #[test]
    fn test_robotics_config() {
        let config = DBConfig::for_robotics();
        assert_eq!(config.num_partitions, 2);
        assert!(matches!(
            config.wal_config.durability_level,
            crate::config::DurabilityLevel::Periodic { interval_ms: 50 }
        ));
        assert_eq!(
            config.index_update_strategy,
            crate::config::IndexUpdateStrategy::BatchOnly
        );
    }

    #[test]
    fn test_backpressure_signal_display() {
        assert_eq!(format!("{}", BackpressureSignal::Normal), "Normal");
        assert_eq!(format!("{}", BackpressureSignal::SlowDown), "SlowDown");
        assert_eq!(format!("{}", BackpressureSignal::Stop), "Stop");
    }
}
