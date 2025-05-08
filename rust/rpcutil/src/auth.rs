use std::{fmt, sync::Arc};
use tonic::{
    Request, Status,
    service::{self},
    transport::CertificateDer,
};
use x509_parser::prelude::X509Error;

/// A Peer represents the authenticated identity of a TLS connection.
///
/// Peers come in different types, based on their role in the system.
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum Peer {
    User(User),
    Source(Source),
    Sink(Sink),
}

/// A User represents information about the _person_ behind a set of
/// Sources and Sinks, not the processes themselves.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct User {
    pub email: String,
}

/// A Source is a process running on behalf of a User, which provides
/// data to back up to a Sink.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Source {
    pub id: String,
}

/// A Sink is a process running on behalf of a User, which provides
/// storage for Source data.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Sink {
    pub id: String,
}

/// Returns the Peer extracted by layer() or an error if it's unavailable.
pub fn peer<T>(request: &Request<T>) -> Result<&Peer, Status> {
    Ok(request
        .extensions()
        .get::<Peer>()
        .ok_or(AuthError::NoClientCert)?)
}

#[derive(Debug, Default, Clone)]
pub struct AuthInterceptor {}

impl service::Interceptor for AuthInterceptor {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        let peer = get_peer(&request.peer_certs())?;
        request.extensions_mut().insert(peer);
        Ok(request)
    }
}

/// Return the client who performed a request, based on their certificates.
///
/// ATTENTION: this assumes Tonic performed proper cert chain validation. The
/// only additional validations are based on the Piston-specific assumptions
/// regarding client certs: single CN with a single string value being the
/// email.
fn get_peer(certs: &Option<Arc<Vec<CertificateDer>>>) -> Result<Peer, AuthError> {
    let certs = certs.as_ref().ok_or(AuthError::NoClientCert)?;
    if certs.len() < 1 {
        return Err(AuthError::NoClientCert);
    }
    // The leaf of the certificate chain must be provided first, with the links to the root following.
    // Note that we rely on Tonic to have validated the chain already.
    let (left, cert) = x509_parser::parse_x509_certificate(&certs[0])?;
    if !left.is_empty() {
        return Err(AuthError::CertWithBytesLeft);
    }
    // We don't want multiple CNs, as it would have ambiguous semantics.
    let mut cn_iter = cert.subject().iter_common_name();
    if cn_iter.next().and_then(|_| cn_iter.next()).is_some() {
        return Err(AuthError::MultipleCommonNames);
    }
    // The one CN better be of the right type then.
    let cn = cert.subject().iter_common_name().next();
    let cn = cn.ok_or(AuthError::NoCommonName)?;

    cn_to_peer(cn.as_str()?)
}

/// Custom errors for peer parsing.
#[derive(Debug, thiserror::Error)]
pub enum AuthError {
    #[error("Request contains no client certificate")]
    NoClientCert,
    #[error("Certificate contains stray bytes")]
    CertWithBytesLeft,
    #[error("Certificate contains no Common Name")]
    NoCommonName,
    #[error("Certificate contains multiple Common Names")]
    MultipleCommonNames,
    #[error("Unexpected Common Name [{0}]")]
    UnexpectedCommonName(String),
    #[error("X509-related error")]
    CertError(#[from] X509Error),
    #[error("X509 parsing-related error")]
    CertParsingError(#[from] x509_parser::nom::Err<X509Error>),
}

/// Curtesy mapping into a Status, as most permission checks are
/// done in the context of an RPC.
impl From<AuthError> for Status {
    fn from(value: AuthError) -> Self {
        Status::permission_denied(value.to_string())
    }
}

impl User {
    pub fn new<S: Into<String>>(email: S) -> Self {
        User {
            email: email.into(),
        }
    }

    /// Get the user's email, as its primary identifier.
    pub fn email(&self) -> &str {
        &self.email
    }
}

impl Source {
    pub fn new<S: Into<String>>(id: S) -> Self {
        Source { id: id.into() }
    }

    /// Get the unique Source's id.
    pub fn id(&self) -> &str {
        &self.id
    }
}

impl Sink {
    pub fn new<S: Into<String>>(id: S) -> Self {
        Sink { id: id.into() }
    }

    /// Get the unique Sink's id.
    pub fn id(&self) -> &str {
        &self.id
    }
}

impl fmt::Display for Peer {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Peer::User(user) => write!(f, "User {}", user),
            Peer::Source(source) => write!(f, "Source {}", source),
            Peer::Sink(sink) => write!(f, "Sink {}", sink),
        }
    }
}

impl fmt::Display for User {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.email)
    }
}

impl fmt::Display for Sink {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.id)
    }
}

impl fmt::Display for Source {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.id)
    }
}

const DOMAIN: &str = ".epthos.net";

/// Converts a Common Name into a Peer.
fn cn_to_peer<S: Into<String>>(cn: S) -> Result<Peer, AuthError> {
    let cn = cn.into();
    if cn.ends_with(DOMAIN) {
        let parts: Vec<&str> = cn.split('.').collect();
        match parts.len() {
            4 => {
                if parts[1] == "snk" {
                    Ok(Peer::Sink(Sink::new(cn)))
                } else if parts[1] == "src" {
                    Ok(Peer::Source(Source::new(cn)))
                } else {
                    Err(AuthError::UnexpectedCommonName(cn))
                }
            }
            _ => Err(AuthError::UnexpectedCommonName(cn)),
        }
    } else if cn.is_empty() {
        Err(AuthError::UnexpectedCommonName(cn))
    } else {
        // TODO: maybe validate correct email
        Ok(Peer::User(User::new(cn)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{io::BufReader, sync::Arc};

    use rustls_pemfile::Item;

    // TODO: test incorrect certificates

    #[test]
    fn extract_email_from_peer() -> anyhow::Result<()> {
        let pem = r#"-----BEGIN CERTIFICATE-----
MIIC7jCCAdagAwIBAgIUPNlLVd20i18gv9cGE8UtFeqq1m4wDQYJKoZIhvcNAQEL
BQAwITEfMB0GA1UEAwwWSW50ZXJtZWRpYXRlIFBpc3RvbiBDQTAeFw0yMzAyMjEw
MTMxNDhaFw0yNDAyMjEwMTMxNDhaMBoxGDAWBgNVBAMMD2JvYkBleGFtcGxlLmNv
bTCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBAM9xpfgjiWpL5Ks2w76t
b693F5rGooLCsUXD2uvEg7AYNSgEgzbCrQzigV+C2imbozqVmR+Qp/PEmROp4pM9
wDUP5Fwu4O3X0h2fiBM3C4vhbYKOYQ1z+pGEDpNg0sLR9KS7Eq6UUc5HcKnAXqVt
Xz/ggLRwENmc5ThnHifXicLdhD1pEeL7We2+X4EpsC0vl+ACBugEcSt6f1cyEXFd
p8F7MU5/ax+AQkpxtmAp9DmytL0AebTdQY8NxB7zrj8ybYfxpgSL4RJsjU2A5Zji
4n/GutyWLZgdEVrX2R2p3AyTUefZRXXQdAf+QrUEeclXdTgbhd/Z7X9Wq1Q1R6X3
GscCAwEAAaMlMCMwDAYDVR0TAQH/BAIwADATBgNVHSUEDDAKBggrBgEFBQcDAjAN
BgkqhkiG9w0BAQsFAAOCAQEAA0g1DqRri9hwVr3hJaltXz5r4Zzx/uWgpcT+yuu9
DQYLhmPS+eHSNWwOa34iXTW7AlDdyyTT+SgkJSw/fJdUQsw+qdKL/untrrrqaXII
096X3acL1gs0+dQ8g+mesPKn3tmowsAKhR129b86U5IHiYaAP8xATOhnwKc/VWHn
P7NuzZ4SPgwKEeRfkJqQEnM2qlXRbr7hiEViAtOCQFCePuMcAnMLSreRrM2tbSqy
iIa5s9FrDMIYblYZYL3Vko5rvnTtUcrK41ckvxetCMgWt0sdw7cp6iYvnW8qLuWO
Nouhq/IRpbMgKqD5JQiLATr1SX+TAtQwGrzW+JNbPVgScw==
-----END CERTIFICATE-----"#;
        let mut io = BufReader::new(pem.as_bytes());
        if let Some(Item::X509Certificate(cert)) = rustls_pemfile::read_one(&mut io)? {
            let peer = get_peer(&Some(Arc::new(vec![cert])))?;
            assert_eq!(peer, Peer::User(User::new("bob@example.com")));
        } else {
            panic!("failed to parse")
        }
        Ok(())
    }

    #[test]
    fn reject_empty_certs() {
        assert!(get_peer(&Some(Arc::new(vec![]))).is_err());
    }

    #[test]
    fn reject_missing_certs() {
        assert!(get_peer(&None).is_err());
    }
    #[test]
    fn parse_user_cn() -> anyhow::Result<()> {
        assert_eq!(
            cn_to_peer("bob@example.com")?,
            Peer::User(User::new("bob@example.com"))
        );
        Ok(())
    }

    #[test]
    fn parse_sink_cn() -> anyhow::Result<()> {
        let cn = "123.snk.epthos.net";
        assert_eq!(cn_to_peer(cn)?, Peer::Sink(Sink::new("123.snk.epthos.net")));
        Ok(())
    }

    #[test]
    fn parse_source_cn() -> anyhow::Result<()> {
        let cn = "321.src.epthos.net";
        assert_eq!(
            cn_to_peer(cn)?,
            Peer::Source(Source::new("321.src.epthos.net"))
        );
        Ok(())
    }

    #[test]
    fn reject_missing_bits() {
        assert!(cn_to_peer("").is_err());
    }

    #[test]
    fn reject_too_many_bits() {
        assert!(cn_to_peer("1233.source.blag.epthos.net").is_err());
    }
}
