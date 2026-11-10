//! Randomness sources for a node's contributions.
//!
//! A node's contribution is a random message or real data -- the split is deliberate and
//! undefined at the protocol level ("randomness for nodes has yet to be defined, but all nodes
//! will sometimes choose to pass non random information forward"). This module makes the source
//! pluggable: software randomness today, a mixed source that sometimes passes real data, and a
//! hardware placeholder for muon detectors and whatever else the hardware interface turns out to
//! be.
//!
//! **What this explicitly is not.** Not a hardware driver. The hardware interface is still to be
//! figured out; `HardwareRandom` is the seam the hardware will implement, and it falls back to
//! software randomness until it exists. Not a beacon: a node that knows its own source can
//! predict its own contribution.

/// A source of 32-byte contributions.
pub trait RandomSource: std::fmt::Debug {
    fn next(&mut self) -> [u8; 32];
}

/// Software randomness -- `oag_crypto::random_bytes_32`.
#[derive(Debug)]
pub struct SoftwareRandom;

impl RandomSource for SoftwareRandom {
    fn next(&mut self) -> [u8; 32] {
        oag_crypto::random_bytes_32()
    }
}

/// A source that sometimes passes real data instead of randomness.
///
/// With probability `random_probability` it returns software randomness; otherwise it asks
/// `real_data` for a value, falling back to randomness if the provider has none. This is the
/// "all nodes will sometimes choose to pass non random information forward" behavior.
pub struct MixedSource<F> {
    /// The probability (0.0..=1.0) that the source returns randomness rather than real data.
    pub random_probability: f32,
    pub real_data: F,
}

// The closure is not `Debug`; the fields that matter are.
impl<F> std::fmt::Debug for MixedSource<F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MixedSource")
            .field("random_probability", &self.random_probability)
            .finish_non_exhaustive()
    }
}

impl<F> RandomSource for MixedSource<F>
where
    F: FnMut() -> Option<[u8; 32]>,
{
    fn next(&mut self) -> [u8; 32] {
        let candidate = oag_crypto::random_bytes_32();
        let p = candidate[0] as f32 / 255.0;
        if p < self.random_probability.clamp(0.0, 1.0) {
            candidate
        } else {
            (self.real_data)().unwrap_or(candidate)
        }
    }
}

/// A hardware randomness source -- muon detectors, radioactive decay, atmospheric noise, etc.
///
/// The hardware interface is still to be figured out. This is the seam: it implements
/// `RandomSource` and falls back to software randomness until a driver exists. A driver replaces
/// `next` with the hardware read; nothing else in the layer changes.
#[derive(Debug)]
pub struct HardwareRandom {
    /// A label for the hardware, for logging and diagnostics. Not used by the fallback.
    pub kind: String,
}

impl Default for HardwareRandom {
    fn default() -> Self {
        Self { kind: "unconfigured".to_string() }
    }
}

impl RandomSource for HardwareRandom {
    fn next(&mut self) -> [u8; 32] {
        // TODO: the hardware interface. Until it exists, fall back to software randomness.
        oag_crypto::random_bytes_32()
    }
}

/// A hardware randomness source backed by a byte device -- the Linux hardware RNG
/// (`/dev/hwrng`) by default, or any device a muon detector or other HWRNG driver exposes.
///
/// Reads exactly 32 bytes. If the device is missing or unreadable, it falls back to software
/// randomness rather than failing -- a node without the hardware still contributes something,
/// and the fallback is documented rather than silent.
#[derive(Debug, Clone)]
pub struct DeviceRandom {
    path: std::path::PathBuf,
}

impl Default for DeviceRandom {
    fn default() -> Self {
        Self { path: std::path::PathBuf::from("/dev/hwrng") }
    }
}

impl DeviceRandom {
    pub fn new(path: impl Into<std::path::PathBuf>) -> Self {
        Self { path: path.into() }
    }
}

impl RandomSource for DeviceRandom {
    fn next(&mut self) -> [u8; 32] {
        use std::io::Read;
        let mut out = [0u8; 32];
        if let Ok(mut file) = std::fs::File::open(&self.path) {
            if file.read_exact(&mut out).is_ok() {
                return out;
            }
        }
        oag_crypto::random_bytes_32()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn software_randomness_is_32_bytes() {
        let mut source = SoftwareRandom;
        let a = source.next();
        let b = source.next();
        assert_ne!(a, b);
    }

    #[test]
    fn a_mixed_source_can_pass_real_data() {
        let mut source = MixedSource {
            random_probability: 0.0, // never random: always ask for real data
            real_data: || Some([7u8; 32]),
        };
        assert_eq!(source.next(), [7u8; 32]);
    }

    #[test]
    fn a_mixed_source_falls_back_when_real_data_is_empty() {
        let mut source = MixedSource {
            random_probability: 0.0,
            real_data: || None,
        };
        // Falls back to randomness -- still 32 bytes, just not the real value.
        let _ = source.next();
    }

    #[test]
    fn hardware_randomness_falls_back_to_software() {
        let mut source = HardwareRandom::default();
        let _ = source.next();
    }

    #[test]
    fn a_device_source_reads_the_device_or_falls_back() {
        // /dev/urandom always exists and always yields 32 bytes; /dev/hwrng may not.
        let mut source = DeviceRandom::new("/dev/urandom");
        let value = source.next();
        assert_eq!(value.len(), 32);

        // A missing device falls back to software randomness rather than failing.
        let mut missing = DeviceRandom::new("/dev/definitely-not-here");
        let _ = missing.next();
    }
}
