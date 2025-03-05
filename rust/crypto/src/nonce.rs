use ring::aead;

pub const NONCE_LEN: usize = 96 / 8;
pub type Nonce = [u8; NONCE_LEN];

// A simple NonceSequence that only returns the nonce once. This is
// sufficient for our use case as we only encrypt one block at a time.
pub struct OneShot<'a> {
    nonce: Option<&'a Nonce>,
}

impl<'a> OneShot<'a> {
    pub fn new(nonce: &'a Nonce) -> OneShot<'a> {
        Self { nonce: Some(nonce) }
    }
}

impl aead::NonceSequence for OneShot<'_> {
    fn advance(&mut self) -> Result<aead::Nonce, ring::error::Unspecified> {
        match self.nonce {
            None => Err(ring::error::Unspecified),
            Some(nonce) => {
                self.nonce = None;
                Ok(aead::Nonce::assume_unique_for_key(*nonce))
            }
        }
    }
}

#[cfg(test)]
mod test {
    use ring::aead::NonceSequence;

    use super::*;

    #[test]
    fn test_one_shot_nonce() {
        let nonce = [0u8; NONCE_LEN];
        let mut seq = OneShot::new(&nonce);
        assert!(seq.advance().is_ok());
        assert!(seq.advance().is_err());
    }
}
