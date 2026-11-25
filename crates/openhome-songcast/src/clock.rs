//! Songcast clock tick math and sender↔receiver drift/offset estimator.
//!
//! Matches `ohPipeline` `Jiffies::SongcastTicksPerSecond`:
//! - 44.1 kHz family (`sample_rate % 441 == 0`): `44_100 * 256 = 11_289_600` ticks/sec
//! - 48 kHz family (all other rates): `48_000 * 256 = 12_288_000` ticks/sec

use std::time::{Duration, Instant};

/// Songcast ticks per second for the 44.1 kHz clock family (`44_100 * 256`).
pub const TICKS_PER_SEC_44K: u64 = 44_100 * 256;

/// Songcast ticks per second for the 48 kHz clock family (`48_000 * 256`).
pub const TICKS_PER_SEC_48K: u64 = 48_000 * 256;

/// Default Songcast pipeline latency in milliseconds when `media_latency == 0`.
pub const DEFAULT_LATENCY_MS: u32 = 100;

/// Return the Songcast tick rate (ticks/sec) for `sample_rate`, matching
/// `ohPipeline`'s `Jiffies::SongcastTicksPerSecond`.
pub fn songcast_ticks_per_sec(sample_rate: u32) -> u64 {
    if sample_rate > 0
        && (matches!(
            sample_rate,
            7_350 | 11_025 | 14_700 | 22_050 | 29_400 | 44_100 | 88_200 | 176_400 | 352_800
        ) || sample_rate.is_multiple_of(441))
    {
        TICKS_PER_SEC_44K
    } else {
        TICKS_PER_SEC_48K
    }
}

/// Convert a latency in milliseconds to Songcast ticks for `sample_rate`.
pub fn latency_ms_to_ticks(latency_ms: u32, sample_rate: u32) -> u32 {
    let ticks = (u64::from(latency_ms) * songcast_ticks_per_sec(sample_rate)) / 1_000;
    ticks.min(u64::from(u32::MAX)) as u32
}

/// Convert Songcast ticks to a `Duration` for `sample_rate`.
pub fn ticks_to_duration(ticks: u32, sample_rate: u32) -> Duration {
    let tps = songcast_ticks_per_sec(sample_rate);
    let nanos = (u128::from(ticks) * 1_000_000_000) / u128::from(tps);
    Duration::from_nanos(nanos.min(u128::from(u64::MAX)) as u64)
}

/// Convert a `Duration` since epoch into a wrapping 32-bit Songcast timestamp.
pub fn duration_to_ticks(elapsed: Duration, sample_rate: u32) -> u32 {
    let tps = u128::from(songcast_ticks_per_sec(sample_rate));
    let ticks = (elapsed.as_nanos() * tps) / 1_000_000_000;
    (ticks & 0xffff_ffff) as u32
}

/// Tracks the offset and clock-skew (in PPM) between a remote Songcast sender's
/// 32-bit wrapping `network_timestamp` and the local monotonic clock.
#[derive(Debug, Clone)]
pub struct ClockSync {
    sample_rate: u32,
    epoch: Instant,
    last_remote_u32: Option<u32>,
    remote_unwrapped: i128,
    base_remote: i128,
    base_local_ns: i128,
    /// Exponentially smoothed offset: `local_ns - remote_ns`.
    smoothed_offset_ns: f64,
    /// Estimated clock skew in parts-per-million (`> 0` means sender clock runs faster than local).
    skew_ppm: f64,
    samples_seen: u64,
}

impl ClockSync {
    pub fn new(sample_rate: u32) -> Self {
        Self::with_epoch(sample_rate, Instant::now())
    }

    pub fn with_epoch(sample_rate: u32, epoch: Instant) -> Self {
        Self {
            sample_rate,
            epoch,
            last_remote_u32: None,
            remote_unwrapped: 0,
            base_remote: 0,
            base_local_ns: 0,
            smoothed_offset_ns: 0.0,
            skew_ppm: 0.0,
            samples_seen: 0,
        }
    }

    /// Reset synchronization state (e.g. on sample-rate change or sender switch).
    pub fn reset(&mut self, sample_rate: u32) {
        self.sample_rate = sample_rate;
        self.last_remote_u32 = None;
        self.remote_unwrapped = 0;
        self.base_remote = 0;
        self.base_local_ns = 0;
        self.smoothed_offset_ns = 0.0;
        self.skew_ppm = 0.0;
        self.samples_seen = 0;
    }

    /// Record an observed `(remote_network_timestamp, local_arrival_instant)` pair
    /// and return the estimated local playout `Instant` given `media_latency` ticks.
    pub fn observe(
        &mut self,
        remote_timestamp: u32,
        media_latency: u32,
        local_arrival: Instant,
    ) -> Instant {
        let local_ns = local_arrival
            .saturating_duration_since(self.epoch)
            .as_nanos() as i128;
        let tps = i128::from(songcast_ticks_per_sec(self.sample_rate));

        match self.last_remote_u32 {
            None => {
                self.last_remote_u32 = Some(remote_timestamp);
                self.remote_unwrapped = i128::from(remote_timestamp);
                self.base_remote = self.remote_unwrapped;
                self.base_local_ns = local_ns;
                self.smoothed_offset_ns = 0.0;
                self.skew_ppm = 0.0;
                self.samples_seen = 1;
            }
            Some(prev) => {
                // Signed 32-bit wrap-safe delta.
                let delta_ticks = remote_timestamp.wrapping_sub(prev) as i32;
                self.last_remote_u32 = Some(remote_timestamp);
                self.remote_unwrapped += i128::from(delta_ticks);

                let remote_elapsed_ticks = self.remote_unwrapped - self.base_remote;
                let remote_elapsed_ns = (remote_elapsed_ticks * 1_000_000_000) / tps;
                let local_elapsed_ns = local_ns - self.base_local_ns;
                let instant_offset_ns = (local_elapsed_ns - remote_elapsed_ns) as f64;

                // Minimum-delay tracking / slow EWMA smoothing to reject UDP jitter spikes.
                let alpha = if self.samples_seen < 16 { 0.25 } else { 0.05 };
                self.smoothed_offset_ns += alpha * (instant_offset_ns - self.smoothed_offset_ns);
                self.samples_seen += 1;

                if local_elapsed_ns >= 500_000_000 {
                    // Positive skew => remote ticks advance faster than local wall time.
                    let diff_ns = (remote_elapsed_ns - local_elapsed_ns) as f64;
                    let raw_ppm = (diff_ns / (local_elapsed_ns as f64)) * 1_000_000.0;
                    self.skew_ppm = raw_ppm.clamp(-5000.0, 5000.0);
                }
            }
        }

        let latency = if media_latency > 0 {
            ticks_to_duration(media_latency, self.sample_rate)
        } else {
            Duration::from_millis(u64::from(DEFAULT_LATENCY_MS))
        };

        let remote_elapsed_ticks = self.remote_unwrapped - self.base_remote;
        let remote_elapsed_ns = (remote_elapsed_ticks * 1_000_000_000) / tps;
        let target_ns = self.base_local_ns + remote_elapsed_ns + (self.smoothed_offset_ns as i128);
        let base_instant = if target_ns >= 0 {
            self.epoch + Duration::from_nanos(target_ns as u64)
        } else {
            local_arrival
        };
        base_instant + latency
    }

    /// Estimated clock skew in PPM (`> 0` means remote clock runs faster than local).
    pub fn skew_ppm(&self) -> f64 {
        self.skew_ppm
    }

    /// Number of timestamp observations recorded since creation / last `reset()`.
    pub fn observations(&self) -> u64 {
        self.samples_seen
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tick_math_matches_44k_and_48k_families() {
        for r44 in [
            7_350, 11_025, 14_700, 22_050, 29_400, 44_100, 88_200, 176_400,
        ] {
            assert_eq!(songcast_ticks_per_sec(r44), TICKS_PER_SEC_44K, "rate {r44}");
        }
        for r48 in [8_000, 16_000, 32_000, 48_000, 96_000, 192_000] {
            assert_eq!(songcast_ticks_per_sec(r48), TICKS_PER_SEC_48K, "rate {r48}");
        }

        let ticks = latency_ms_to_ticks(100, 48_000);
        assert_eq!(ticks, 1_228_800);
        assert_eq!(ticks_to_duration(ticks, 48_000), Duration::from_millis(100));

        let wrapped = duration_to_ticks(Duration::from_millis(50), 44_100);
        assert_eq!(wrapped, latency_ms_to_ticks(50, 44_100));
    }

    #[test]
    fn clock_sync_handles_u32_wrap_and_estimates_skew() {
        let epoch = Instant::now();
        let mut sync = ClockSync::with_epoch(48_000, epoch);

        // Start near u32::MAX to exercise 32-bit wraparound.
        let start_ts: u32 = u32::MAX - 500_000;
        let latency = latency_ms_to_ticks(100, 48_000);

        // Simulate a sender running +100 PPM faster than local clock over 1 second.
        for i in 0..=100u64 {
            let local_elapsed = Duration::from_millis(i * 10);
            let remote_elapsed_ns = (local_elapsed.as_nanos() as f64 * 1.0001) as u128;
            let remote_ticks =
                ((remote_elapsed_ns * u128::from(TICKS_PER_SEC_48K)) / 1_000_000_000) as u32;
            let ts = start_ts.wrapping_add(remote_ticks);
            let playout = sync.observe(ts, latency, epoch + local_elapsed);
            assert!(playout >= epoch + local_elapsed + Duration::from_millis(90));
        }

        assert_eq!(sync.observations(), 101);
        assert!(
            (sync.skew_ppm() - 100.0).abs() < 15.0,
            "expected ~100ppm skew, got {}",
            sync.skew_ppm()
        );

        sync.reset(44_100);
        assert_eq!(sync.observations(), 0);
        assert_eq!(sync.skew_ppm(), 0.0);
    }
}
