//! RFC 8291 message encryption for Web Push (`aes128gcm`, RFC 8188, one
//! record).
//!
//! Every push gets a fresh ephemeral P-256 key and a random 16-byte salt; the
//! content key comes from ECDH with the subscription's `p256dh` and the RFC's
//! two-stage HKDF keyed with its `auth` secret. [`encrypt_with`] takes the
//! ephemeral key and salt as inputs so the RFC's Appendix A vector can be
//! reproduced byte for byte.

use aes_gcm::aead::{Aead, KeyInit};
use aes_gcm::{Aes128Gcm, Nonce};
use hkdf::Hkdf;
use p256::elliptic_curve::sec1::ToEncodedPoint;
use p256::{PublicKey, SecretKey};
use rand_core::{OsRng, RngCore};
use sha2::Sha256;

use crate::util::decode_b64_any;

/// RFC 8188 record size. One record carries the whole message.
pub const RECORD_SIZE: u32 = 4096;

/// Header: salt (16) + record size (4) + key id length (1) + key id (65).
pub const HEADER_LEN: usize = 86;

/// Largest plaintext that fits one 4096-byte record: minus the header, the
/// 16-byte GCM tag and the 1-byte padding delimiter. Push services accept at
/// least 4096 bytes of body (RFC 8030), so this is the portable ceiling.
pub const MAX_PLAINTEXT: usize = RECORD_SIZE as usize - HEADER_LEN - 16 - 1;

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum EceError {
    #[error("invalid p256dh: {0}")]
    BadPublicKey(String),
    #[error("invalid auth secret: {0}")]
    BadAuth(String),
    #[error("plaintext of {0} bytes exceeds {MAX_PLAINTEXT}")]
    TooLarge(usize),
    #[error("encryption failed")]
    Crypto,
}

/// Encrypt one push for a subscription. `ua_public` / `auth` are the
/// subscription's `keys.p256dh` / `keys.auth` in base64 (url-safe or not,
/// padded or not).
pub fn encrypt(plaintext: &[u8], ua_public: &str, auth: &str) -> Result<Vec<u8>, EceError> {
    let ua_public = decode_b64_any(ua_public).map_err(EceError::BadPublicKey)?;
    let auth = decode_b64_any(auth).map_err(EceError::BadAuth)?;
    let as_private = SecretKey::random(&mut OsRng);
    let mut salt = [0u8; 16];
    OsRng.fill_bytes(&mut salt);
    encrypt_with(plaintext, &ua_public, &auth, &as_private, &salt)
}

/// The deterministic core of [`encrypt`].
pub fn encrypt_with(
    plaintext: &[u8],
    ua_public: &[u8],
    auth: &[u8],
    as_private: &SecretKey,
    salt: &[u8; 16],
) -> Result<Vec<u8>, EceError> {
    if plaintext.len() > MAX_PLAINTEXT {
        return Err(EceError::TooLarge(plaintext.len()));
    }
    if auth.len() != 16 {
        return Err(EceError::BadAuth(format!(
            "expected 16 bytes, got {}",
            auth.len()
        )));
    }
    let ua_key = PublicKey::from_sec1_bytes(ua_public)
        .map_err(|_| EceError::BadPublicKey("not a P-256 point".into()))?;
    // key_info needs the uncompressed form even if the browser sent another.
    let ua_public = ua_key.to_encoded_point(false);
    let as_public = as_private.public_key().to_encoded_point(false);

    let shared = p256::ecdh::diffie_hellman(as_private.to_nonzero_scalar(), ua_key.as_affine());
    let (cek, nonce) = derive(
        shared.raw_secret_bytes(),
        auth,
        ua_public.as_bytes(),
        as_public.as_bytes(),
        salt,
    )?;

    let mut record = Vec::with_capacity(plaintext.len() + 1);
    record.extend_from_slice(plaintext);
    // Last (and only) record: delimiter 0x02, no further padding.
    record.push(0x02);
    let cipher = Aes128Gcm::new_from_slice(&cek).map_err(|_| EceError::Crypto)?;
    let ciphertext = cipher
        .encrypt(Nonce::from_slice(&nonce), record.as_ref())
        .map_err(|_| EceError::Crypto)?;

    let mut body = Vec::with_capacity(HEADER_LEN + ciphertext.len());
    body.extend_from_slice(salt);
    body.extend_from_slice(&RECORD_SIZE.to_be_bytes());
    body.push(as_public.as_bytes().len() as u8);
    body.extend_from_slice(as_public.as_bytes());
    body.extend_from_slice(&ciphertext);
    Ok(body)
}

/// RFC 8291 section 3.4 then RFC 8188 section 2.2: (CEK, NONCE).
fn derive(
    ecdh_secret: &[u8],
    auth: &[u8],
    ua_public: &[u8],
    as_public: &[u8],
    salt: &[u8],
) -> Result<([u8; 16], [u8; 12]), EceError> {
    let mut key_info = Vec::with_capacity(14 + 65 + 65);
    key_info.extend_from_slice(b"WebPush: info\0");
    key_info.extend_from_slice(ua_public);
    key_info.extend_from_slice(as_public);
    let mut ikm = [0u8; 32];
    Hkdf::<Sha256>::new(Some(auth), ecdh_secret)
        .expand(&key_info, &mut ikm)
        .map_err(|_| EceError::Crypto)?;

    let hk = Hkdf::<Sha256>::new(Some(salt), &ikm);
    let mut cek = [0u8; 16];
    hk.expand(b"Content-Encoding: aes128gcm\0", &mut cek)
        .map_err(|_| EceError::Crypto)?;
    let mut nonce = [0u8; 12];
    hk.expand(b"Content-Encoding: nonce\0", &mut nonce)
        .map_err(|_| EceError::Crypto)?;
    Ok((cek, nonce))
}

/// What a browser does with the body: the other half of [`encrypt`], for
/// tests (and a future `spky push decrypt` debugging aid).
pub fn decrypt(body: &[u8], ua_private: &SecretKey, auth: &[u8]) -> Result<Vec<u8>, String> {
    if body.len() < 21 {
        return Err("body shorter than the header".into());
    }
    let salt = &body[..16];
    let rs = u32::from_be_bytes([body[16], body[17], body[18], body[19]]);
    let idlen = body[20] as usize;
    if body.len() < 21 + idlen {
        return Err("truncated key id".into());
    }
    let as_public_bytes = &body[21..21 + idlen];
    let ciphertext = &body[21 + idlen..];
    if ciphertext.len() > rs as usize {
        return Err("more than one record".into());
    }
    let as_public =
        PublicKey::from_sec1_bytes(as_public_bytes).map_err(|_| "bad key id".to_string())?;
    let ua_public = ua_private.public_key().to_encoded_point(false);
    let shared = p256::ecdh::diffie_hellman(ua_private.to_nonzero_scalar(), as_public.as_affine());
    let (cek, nonce) = derive(
        shared.raw_secret_bytes(),
        auth,
        ua_public.as_bytes(),
        as_public_bytes,
        salt,
    )
    .map_err(|e| e.to_string())?;
    let cipher = Aes128Gcm::new_from_slice(&cek).map_err(|e| e.to_string())?;
    let mut record = cipher
        .decrypt(Nonce::from_slice(&nonce), ciphertext)
        .map_err(|_| "authentication failed".to_string())?;
    while record.last() == Some(&0) {
        record.pop();
    }
    match record.pop() {
        Some(0x02) => Ok(record),
        _ => Err("missing last-record delimiter".into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::b64url;

    fn b(s: &str) -> Vec<u8> {
        decode_b64_any(s).unwrap()
    }

    // RFC 8291 Appendix A.
    const PLAINTEXT: &str = "When I grow up, I want to be a watermelon";
    const AS_PRIVATE: &str = "yfWPiYE-n46HLnH0KqZOF1fJJU3MYrct3AELtAQ-oRw";
    const AS_PUBLIC: &str =
        "BP4z9KsN6nGRTbVYI_c7VJSPQTBtkgcy27mlmlMoZIIgDll6e3vCYLocInmYWAmS6TlzAC8wEqKK6PBru3jl7A8";
    const UA_PRIVATE: &str = "q1dXpw3UpT5VOmu_cf_v6ih07Aems3njxI-JWgLcM94";
    const UA_PUBLIC: &str =
        "BCVxsr7N_eNgVRqvHtD0zTZsEc6-VV-JvLexhqUzORcxaOzi6-AYWXvTBHm4bjyPjs7Vd8pZGH6SRpkNtoIAiw4";
    const SALT: &str = "DGv6ra1nlYgDCS1FRnbzlw";
    const AUTH: &str = "BTBZMqHH6r4Tts7J_aSIgg";
    const HEADER: &str = "DGv6ra1nlYgDCS1FRnbzlwAAEABBBP4z9KsN6nGRTbVYI_c7VJSPQTBtkgcy27mlmlMoZIIgDll6e3vCYLocInmYWAmS6TlzAC8wEqKK6PBru3jl7A8";
    const CIPHERTEXT: &str =
        "8pfeW0KbunFT06SuDKoJH9Ql87S1QUrdirN6GcG7sFz1y1sqLgVi1VhjVkHsUoEsbI_0LpXMuGvnzQ";
    const BODY: &str = "DGv6ra1nlYgDCS1FRnbzlwAAEABBBP4z9KsN6nGRTbVYI_c7VJSPQTBtkgcy27mlmlMoZIIgDll6e3vCYLocInmYWAmS6TlzAC8wEqKK6PBru3jl7A_yl95bQpu6cVPTpK4Mqgkf1CXztLVBSt2Ks3oZwbuwXPXLWyouBWLVWGNWQexSgSxsj_Qulcy4a-fN";

    fn rfc_encrypt() -> Vec<u8> {
        let as_private = SecretKey::from_slice(&b(AS_PRIVATE)).unwrap();
        let salt: [u8; 16] = b(SALT).try_into().unwrap();
        encrypt_with(
            PLAINTEXT.as_bytes(),
            &b(UA_PUBLIC),
            &b(AUTH),
            &as_private,
            &salt,
        )
        .unwrap()
    }

    #[test]
    fn rfc8291_appendix_a_byte_for_byte() {
        let as_private = SecretKey::from_slice(&b(AS_PRIVATE)).unwrap();
        assert_eq!(
            b64url(as_private.public_key().to_encoded_point(false).as_bytes()),
            AS_PUBLIC
        );
        let body = rfc_encrypt();
        assert_eq!(body.len(), HEADER_LEN + PLAINTEXT.len() + 1 + 16);
        assert_eq!(b64url(&body[..HEADER_LEN]), HEADER);
        assert_eq!(b64url(&body[HEADER_LEN..]), CIPHERTEXT);
        assert_eq!(b64url(&body), BODY);
    }

    #[test]
    fn rfc_vector_decrypts_with_the_ua_key() {
        let ua_private = SecretKey::from_slice(&b(UA_PRIVATE)).unwrap();
        assert_eq!(
            b64url(ua_private.public_key().to_encoded_point(false).as_bytes()),
            UA_PUBLIC
        );
        let plain = decrypt(&b(BODY), &ua_private, &b(AUTH)).unwrap();
        assert_eq!(plain, PLAINTEXT.as_bytes());
    }

    #[test]
    fn random_encryption_round_trips() {
        let ua_private = SecretKey::random(&mut OsRng);
        let ua_public = b64url(ua_private.public_key().to_encoded_point(false).as_bytes());
        let mut auth = [0u8; 16];
        OsRng.fill_bytes(&mut auth);
        let msg = br#"{"v":1,"kind":"rule"}"#;
        let a = encrypt(msg, &ua_public, &b64url(&auth)).unwrap();
        let b2 = encrypt(msg, &ua_public, &b64url(&auth)).unwrap();
        assert_ne!(a, b2, "fresh salt and key every time");
        assert_eq!(decrypt(&a, &ua_private, &auth).unwrap(), msg);
        assert_eq!(decrypt(&b2, &ua_private, &auth).unwrap(), msg);
        let mut wrong = auth;
        wrong[0] ^= 1;
        assert!(decrypt(&a, &ua_private, &wrong).is_err());
    }

    #[test]
    fn accepts_padded_and_standard_base64() {
        let ua_private = SecretKey::from_slice(&b(UA_PRIVATE)).unwrap();
        let std_pub =
            base64::Engine::encode(&base64::engine::general_purpose::STANDARD, b(UA_PUBLIC));
        let std_auth = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, b(AUTH));
        assert!(std_pub.contains('+') || std_pub.contains('/') || std_pub.ends_with('='));
        let body = encrypt(b"hi", &std_pub, &std_auth).unwrap();
        assert_eq!(decrypt(&body, &ua_private, &b(AUTH)).unwrap(), b"hi");
        let body = encrypt(b"hi", &format!("{UA_PUBLIC}="), AUTH).unwrap();
        assert_eq!(decrypt(&body, &ua_private, &b(AUTH)).unwrap(), b"hi");
    }

    #[test]
    fn size_cap_and_bad_keys() {
        assert_eq!(MAX_PLAINTEXT, 3993);
        let ua_private = SecretKey::random(&mut OsRng);
        let ua_public = b64url(ua_private.public_key().to_encoded_point(false).as_bytes());
        let body = encrypt(&vec![b'x'; MAX_PLAINTEXT], &ua_public, AUTH).unwrap();
        assert_eq!(body.len(), RECORD_SIZE as usize);
        assert_eq!(
            encrypt(&vec![b'x'; MAX_PLAINTEXT + 1], &ua_public, AUTH),
            Err(EceError::TooLarge(3994))
        );
        assert!(matches!(
            encrypt(b"x", "AAAA", AUTH),
            Err(EceError::BadPublicKey(_))
        ));
        assert!(matches!(
            encrypt(b"x", "***", AUTH),
            Err(EceError::BadPublicKey(_))
        ));
        assert!(matches!(
            encrypt(b"x", &ua_public, "AAAA"),
            Err(EceError::BadAuth(_))
        ));
    }
}
