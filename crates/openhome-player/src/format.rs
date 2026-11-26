//! PCM format descriptors, OpenHome volume/balance gain scaling, and Songcast PCM conversion.

use openhome_core::types::VolumeState;

/// Interleaved PCM stream parameters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PcmSpec {
    /// Sample rate in Hz (e.g. `44_100`, `48_000`, `96_000`).
    pub sample_rate: u32,
    /// Number of interleaved audio channels (e.g. `2` for stereo).
    pub channels: u16,
    /// Source bit depth (e.g. `16`, `24`, `32`).
    pub bits_per_sample: u16,
}

impl PcmSpec {
    /// Create a new [`PcmSpec`].
    pub const fn new(sample_rate: u32, channels: u16, bits_per_sample: u16) -> Self {
        Self {
            sample_rate,
            channels,
            bits_per_sample,
        }
    }

    /// Number of sample frames represented by `interleaved_samples` floats.
    pub fn frames_in(&self, interleaved_samples: usize) -> u64 {
        let ch = usize::from(self.channels.max(1));
        (interleaved_samples / ch) as u64
    }
}

/// One decoded or captured block of interleaved normalized `f32` PCM samples (`-1.0..=1.0`).
#[derive(Debug, Clone, PartialEq)]
pub struct PcmFrame {
    /// Stream format of `samples`.
    pub spec: PcmSpec,
    /// Interleaved normalized `f32` samples.
    pub samples: Vec<f32>,
}

/// Compute linear amplitude gain for OpenHome `Volume` (`0..=100`) and `Mute`.
///
/// Uses a smooth perceptual quadratic taper (`(v / 100)^2`) so low volumes remain
/// finely controllable, returning `0.0` when muted or at volume `0`.
pub fn volume_gain(volume: u32, mute: bool) -> f32 {
    if mute || volume == 0 {
        return 0.0;
    }
    let norm = (volume.min(100) as f32) / 100.0;
    norm * norm
}

/// Apply OpenHome [`VolumeState`] (`volume`, `mute`, `balance`) in-place to interleaved `f32` samples.
///
/// `balance` is treated as `-100..=100` (negative attenuates right, positive attenuates left)
/// when `channels == 2`.
pub fn apply_volume_in_place(samples: &mut [f32], channels: u16, state: &VolumeState) {
    let base = volume_gain(state.volume, state.mute);
    if base == 0.0 {
        samples.fill(0.0);
        return;
    }
    if channels == 2 && state.balance != 0 {
        let b = (state.balance.clamp(-100, 100) as f32) / 100.0;
        let left_gain = if b > 0.0 { base * (1.0 - b) } else { base };
        let right_gain = if b < 0.0 { base * (1.0 + b) } else { base };
        for pair in samples.as_chunks_mut::<2>().0 {
            pair[0] *= left_gain;
            pair[1] *= right_gain;
        }
    } else if (base - 1.0).abs() > f32::EPSILON {
        for s in samples.iter_mut() {
            *s *= base;
        }
    }
}

/// Convert big-endian signed integer Songcast PCM bytes (`bit_depth` of `16`, `24`, or `32`)
/// into interleaved normalized `f32` samples in `-1.0..=1.0`.
pub fn songcast_pcm_be_to_f32(bytes: &[u8], bit_depth: u8) -> Vec<f32> {
    match bit_depth {
        16 => bytes
            .as_chunks::<2>()
            .0
            .iter()
            .map(|&c| i16::from_be_bytes(c) as f32 / 32_768.0)
            .collect(),
        24 => bytes
            .as_chunks::<3>()
            .0
            .iter()
            .map(|&[b0, b1, b2]| {
                // Sign-extend 24-bit big-endian integer into i32.
                let raw = i32::from_be_bytes([b0, b1, b2, 0]) >> 8;
                raw as f32 / 8_388_608.0
            })
            .collect(),
        32 => bytes
            .as_chunks::<4>()
            .0
            .iter()
            .map(|&c| i32::from_be_bytes(c) as f32 / 2_147_483_648.0)
            .collect(),
        _ => Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn volume_gain_and_balance_scale_samples_accurately() {
        assert_eq!(volume_gain(0, false), 0.0);
        assert_eq!(volume_gain(80, true), 0.0);
        assert_eq!(volume_gain(100, false), 1.0);
        assert!((volume_gain(50, false) - 0.25).abs() < 1e-6);

        let mut stereo = vec![1.0f32, -1.0, 0.5, -0.5];
        apply_volume_in_place(
            &mut stereo,
            2,
            &VolumeState {
                volume: 100,
                mute: false,
                balance: 50,
                fade: 0,
                volume_limit: 100,
            },
        );
        assert!((stereo[0] - 0.5).abs() < 1e-6);
        assert!((stereo[1] + 1.0).abs() < 1e-6);

        apply_volume_in_place(
            &mut stereo,
            2,
            &VolumeState {
                volume: 80,
                mute: true,
                balance: 0,
                fade: 0,
                volume_limit: 100,
            },
        );
        assert!(stereo.iter().all(|&s| s == 0.0));
    }

    #[test]
    fn songcast_be_pcm_converts_16_24_and_32_bit() {
        let s16 = songcast_pcm_be_to_f32(&[0x7F, 0xFF, 0x80, 0x00], 16);
        assert_eq!(s16.len(), 2);
        assert!(s16[0] > 0.999 && (s16[1] + 1.0).abs() < 1e-6);

        let s24 = songcast_pcm_be_to_f32(&[0x7F, 0xFF, 0xFF, 0x80, 0x00, 0x00], 24);
        assert_eq!(s24.len(), 2);
        assert!(s24[0] > 0.999 && (s24[1] + 1.0).abs() < 1e-6);

        let s32 = songcast_pcm_be_to_f32(&[0x40, 0x00, 0x00, 0x00], 32);
        assert_eq!(s32.len(), 1);
        assert!((s32[0] - 0.5).abs() < 1e-6);

        assert!(songcast_pcm_be_to_f32(&[1, 2, 3], 8).is_empty());
    }
}
