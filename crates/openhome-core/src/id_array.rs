//! Base64 big-endian `u32` codec for OpenHome `Playlist.IdArray`.

use crate::error::Error;

const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

/// Encode a slice of OpenHome track IDs as a base64 string of big-endian `u32` values.
pub fn encode_id_array(ids: &[u32]) -> String {
    let mut raw = Vec::with_capacity(ids.len() * 4);
    for id in ids {
        raw.extend_from_slice(&id.to_be_bytes());
    }
    base64_encode(&raw)
}

/// Decode a base64 `Playlist.IdArray` string into a `Vec<u32>`.
pub fn decode_id_array(encoded: &str) -> Result<Vec<u32>, Error> {
    let raw = base64_decode(encoded)?;
    if !raw.len().is_multiple_of(4) {
        return Err(Error::InvalidIdArray(format!(
            "decoded byte length {} is not a multiple of 4",
            raw.len()
        )));
    }
    Ok(raw
        .as_chunks::<4>()
        .0
        .iter()
        .map(|c| u32::from_be_bytes(*c))
        .collect())
}

fn base64_encode(input: &[u8]) -> String {
    if input.is_empty() {
        return String::new();
    }
    let mut out = String::with_capacity(input.len().div_ceil(3) * 4);
    for chunk in input.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = *chunk.get(1).unwrap_or(&0) as u32;
        let b2 = *chunk.get(2).unwrap_or(&0) as u32;
        let n = (b0 << 16) | (b1 << 8) | b2;
        out.push(ALPHABET[((n >> 18) & 0x3f) as usize] as char);
        out.push(ALPHABET[((n >> 12) & 0x3f) as usize] as char);
        if chunk.len() > 1 {
            out.push(ALPHABET[((n >> 6) & 0x3f) as usize] as char);
        } else {
            out.push('=');
        }
        if chunk.len() > 2 {
            out.push(ALPHABET[(n & 0x3f) as usize] as char);
        } else {
            out.push('=');
        }
    }
    out
}

fn decode_val(b: u8) -> Option<u8> {
    match b {
        b'A'..=b'Z' => Some(b - b'A'),
        b'a'..=b'z' => Some(b - b'a' + 26),
        b'0'..=b'9' => Some(b - b'0' + 52),
        b'+' => Some(62),
        b'/' => Some(63),
        _ => None,
    }
}

fn base64_decode(input: &str) -> Result<Vec<u8>, Error> {
    let filtered: Vec<u8> = input.bytes().filter(|b| !b.is_ascii_whitespace()).collect();
    if filtered.is_empty() {
        return Ok(Vec::new());
    }
    if !filtered.len().is_multiple_of(4) {
        return Err(Error::InvalidIdArray("length not a multiple of 4".into()));
    }
    let mut out = Vec::with_capacity(filtered.len() / 4 * 3);
    for (idx, chunk) in filtered.as_chunks::<4>().0.iter().enumerate() {
        let is_last = (idx + 1) * 4 == filtered.len();
        let v0 = decode_val(chunk[0])
            .ok_or_else(|| Error::InvalidIdArray("invalid base64 character".into()))?;
        let v1 = decode_val(chunk[1])
            .ok_or_else(|| Error::InvalidIdArray("invalid base64 character".into()))?;
        out.push((v0 << 2) | (v1 >> 4));

        if chunk[2] == b'=' {
            if !is_last || chunk[3] != b'=' {
                return Err(Error::InvalidIdArray("invalid base64 padding".into()));
            }
            break;
        }
        let v2 = decode_val(chunk[2])
            .ok_or_else(|| Error::InvalidIdArray("invalid base64 character".into()))?;
        out.push((v1 << 4) | (v2 >> 2));

        if chunk[3] == b'=' {
            if !is_last {
                return Err(Error::InvalidIdArray("invalid base64 padding".into()));
            }
            break;
        }
        let v3 = decode_val(chunk[3])
            .ok_or_else(|| Error::InvalidIdArray("invalid base64 character".into()))?;
        out.push((v2 << 6) | v3);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_empty_and_multiple_ids() {
        assert_eq!(encode_id_array(&[]), "");
        assert_eq!(decode_id_array("").unwrap(), Vec::<u32>::new());

        let ids = vec![1, 2, 256, 0x0102_0304, u32::MAX];
        let enc = encode_id_array(&ids);
        // First u32 = 1 => 00 00 00 01 => "AAAAAQ..."
        assert!(enc.starts_with("AAAAAQ"));
        assert_eq!(decode_id_array(&enc).unwrap(), ids);
    }

    #[test]
    fn rejects_non_multiple_of_4_bytes() {
        // "AA==" decodes to 1 byte, not a valid u32 array
        assert!(decode_id_array("AA==").is_err());
    }
}
