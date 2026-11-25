//! Ordered audio frame buffer with gap detection and resend scheduling,
//! matching `ohSongcast` / `ohPipeline`'s `kMaxRepairBacklogFrames = 200` and
//! `kMaxRepairMissedFrames = 20`.

use std::collections::BTreeMap;

use crate::ohm::OhmAudio;

/// Maximum buffered out-of-order frames before declaring a stream discontinuity (`ohSongcast`).
pub const DEFAULT_MAX_BACKLOG_FRAMES: usize = 200;

/// Maximum missing frame numbers packed into a single `OhmMessage::Resend` (`ohSongcast`).
pub const MAX_RESEND_FRAMES_PER_REQ: usize = 20;

/// Outcome of pushing an incoming `OhmAudio` frame into [`ReorderBuffer`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PushOutcome {
    /// Zero or more contiguous frames are ready for immediate playout.
    Ready(Vec<OhmAudio>),
    /// The frame arrived after a gap and was buffered; caller should schedule a `Resend`.
    GapDetected,
    /// The frame was a duplicate or stale retransmission and was dropped.
    Duplicate,
    /// The backlog overflowed or the sender jumped backwards without `RESENT`;
    /// the buffer was resynchronized to this frame and the caller should emit `Disconnected`.
    Resynced(OhmAudio),
}

/// Reorders incoming Songcast audio frames and tracks missing sequence numbers.
#[derive(Debug)]
pub struct ReorderBuffer {
    max_backlog: usize,
    last_emitted: Option<u32>,
    pending: BTreeMap<u32, OhmAudio>,
}

impl Default for ReorderBuffer {
    fn default() -> Self {
        Self::new(DEFAULT_MAX_BACKLOG_FRAMES)
    }
}

impl ReorderBuffer {
    pub fn new(max_backlog: usize) -> Self {
        Self {
            max_backlog: max_backlog.max(1),
            last_emitted: None,
            pending: BTreeMap::new(),
        }
    }

    /// Clear all buffered state (e.g. on disconnect or stream switch).
    pub fn clear(&mut self) {
        self.last_emitted = None;
        self.pending.clear();
    }

    /// Push an incoming `OhmAudio` frame.
    pub fn push(&mut self, frame: OhmAudio) -> PushOutcome {
        let Some(last) = self.last_emitted else {
            self.last_emitted = Some(frame.frame);
            return PushOutcome::Ready(vec![frame]);
        };

        // Signed 32-bit wrap-safe difference.
        let diff = frame.frame.wrapping_sub(last) as i32;

        if diff <= 0 {
            // Stale duplicate or sender reset.
            // Per ohPipeline: if a non-resent frame jumps backwards significantly, treat as sender reset.
            if !frame.flags.is_resent() && diff < -(self.max_backlog as i32) {
                self.pending.clear();
                self.last_emitted = Some(frame.frame);
                return PushOutcome::Resynced(frame);
            }
            return PushOutcome::Duplicate;
        }

        if diff == 1 {
            let mut ready = vec![frame];
            let mut next_expected = last.wrapping_add(2);
            while let Some(buffered) = self.pending.remove(&next_expected) {
                ready.push(buffered);
                next_expected = next_expected.wrapping_add(1);
            }
            self.last_emitted = Some(ready.last().unwrap().frame);
            return PushOutcome::Ready(ready);
        }

        // Frame arrived ahead of expected sequence (gap).
        if self.pending.contains_key(&frame.frame) {
            return PushOutcome::Duplicate;
        }
        if (diff as usize) > self.max_backlog || self.pending.len() >= self.max_backlog {
            // Backlog overflow -> resync to the new frame (matches ohSongcast RepairReset).
            self.pending.clear();
            self.last_emitted = Some(frame.frame);
            return PushOutcome::Resynced(frame);
        }

        self.pending.insert(frame.frame, frame);
        PushOutcome::GapDetected
    }

    /// Return `true` if one or more frames are currently missing ahead of `last_emitted`.
    pub fn has_gaps(&self) -> bool {
        !self.pending.is_empty()
    }

    /// Return up to `max_frames` (capped at [`MAX_RESEND_FRAMES_PER_REQ`]) missing
    /// frame numbers in ascending order for an `OhmMessage::Resend` request.
    pub fn missing_frames(&self, max_frames: usize) -> Vec<u32> {
        let Some(last) = self.last_emitted else {
            return Vec::new();
        };
        if self.pending.is_empty() {
            return Vec::new();
        }
        let limit = max_frames.min(MAX_RESEND_FRAMES_PER_REQ);
        let mut missing = Vec::with_capacity(limit);

        // Walk from last + 1 up to the highest pending frame (wrap-safe via signed diff).
        let max_offset = self
            .pending
            .keys()
            .map(|&f| f.wrapping_sub(last) as i32)
            .filter(|&d| d > 0)
            .max()
            .unwrap_or(0);

        for off in 1..max_offset {
            let candidate = last.wrapping_add(off as u32);
            if !self.pending.contains_key(&candidate) {
                missing.push(candidate);
                if missing.len() >= limit {
                    break;
                }
            }
        }
        missing
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ohm::AudioFlags;

    fn mk_frame(seq: u32, resent: bool) -> OhmAudio {
        OhmAudio {
            flags: if resent {
                AudioFlags(AudioFlags::RESENT)
            } else {
                AudioFlags(0)
            },
            samples: 240,
            frame: seq,
            network_timestamp: 0,
            media_latency: 0,
            media_timestamp: 0,
            sample_start: 0,
            samples_total: 0,
            sample_rate: 48_000,
            bit_rate: 1_536_000,
            volume_offset: 0,
            bit_depth: 16,
            channels: 2,
            codec: "PCM".into(),
            audio: vec![seq as u8],
        }
    }

    #[test]
    fn in_order_gap_repair_duplicate_and_resync() {
        let mut buf = ReorderBuffer::new(10);

        // First frame initializes sequence.
        assert_eq!(
            buf.push(mk_frame(100, false)),
            PushOutcome::Ready(vec![mk_frame(100, false)])
        );
        assert!(!buf.has_gaps());
        assert!(buf.missing_frames(20).is_empty());

        // Duplicate frame dropped.
        assert_eq!(buf.push(mk_frame(100, true)), PushOutcome::Duplicate);

        // Gap: 101 and 102 missing, 103 arrives.
        assert_eq!(buf.push(mk_frame(103, false)), PushOutcome::GapDetected);
        // Duplicate of an already-pending frame is dropped without re-triggering resend.
        assert_eq!(buf.push(mk_frame(103, false)), PushOutcome::Duplicate);
        assert!(buf.has_gaps());
        assert_eq!(buf.missing_frames(20), vec![101, 102]);
        assert_eq!(buf.missing_frames(1), vec![101]);

        // 102 arrives out of order -> still waiting on 101.
        assert_eq!(buf.push(mk_frame(102, true)), PushOutcome::GapDetected);
        assert_eq!(buf.missing_frames(20), vec![101]);

        // 101 arrives -> drains 101, 102, 103 in order!
        assert_eq!(
            buf.push(mk_frame(101, true)),
            PushOutcome::Ready(vec![
                mk_frame(101, true),
                mk_frame(102, true),
                mk_frame(103, false),
            ])
        );
        assert!(!buf.has_gaps());

        // Large forward jump > max_backlog triggers Resynced.
        assert_eq!(
            buf.push(mk_frame(500, false)),
            PushOutcome::Resynced(mk_frame(500, false))
        );

        // Large backward jump (sender restart) triggers Resynced.
        assert_eq!(
            buf.push(mk_frame(1, false)),
            PushOutcome::Resynced(mk_frame(1, false))
        );

        buf.clear();
        assert!(!buf.has_gaps());
    }
}
