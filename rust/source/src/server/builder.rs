use super::Server;
use settings::{client, connection};
use source_settings::Settings;
use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
};
use storage::fingerprint;
use tokio_util::sync::CancellationToken;

#[derive(Default)]
pub struct Builder {
    token: CancellationToken,
    roots: Vec<PathBuf>,
    address: Option<SocketAddr>,
    connection: Option<connection::Info>,
    broker: Option<client::Settings>,
    filestore: PathBuf,
    datastore: PathBuf,
    rnd: Option<crypto::SharedRandom>,
    source_key: Option<crypto::Keys>,
}

#[derive(thiserror::Error, Debug)]
pub enum BuilderError {
    #[error("Missing address")]
    MissingAddress,
    #[error("Missing connection settings")]
    MissingConnection,
    #[error("Missing Broker info")]
    MissingBrokerInfo,
    #[error("Missing Crypto")]
    MissingCrypto,
    #[error("Invalid fingerprinter")]
    FingerprinterError(#[from] fingerprint::FingerprinterError),
    #[error("Unknown error")]
    UnknownError(#[from] anyhow::Error),
}

impl Builder {
    pub fn new(token: CancellationToken) -> Builder {
        Builder {
            token,
            ..Builder::default()
        }
    }
    pub fn settings(self, settings: &Settings) -> Builder {
        self.roots(settings.backup().roots().clone())
            .address(*settings.server().address())
            .connection(settings.connection())
            .broker(settings.broker())
            .filestore(settings.filestore().db())
            .datastore(settings.datastore().db())
    }

    pub fn roots(mut self, roots: Vec<PathBuf>) -> Builder {
        self.roots = roots;
        self
    }

    pub fn address(mut self, address: SocketAddr) -> Builder {
        self.address = Some(address);
        self
    }

    pub fn connection(mut self, connection: &connection::Settings) -> Builder {
        self.connection = Some(connection.info().clone());
        self
    }

    pub fn broker(mut self, broker: &client::Settings) -> Builder {
        self.broker = Some((*broker).clone());
        self
    }

    pub fn filestore(mut self, path: &Path) -> Builder {
        self.filestore = path.to_path_buf();
        self
    }

    pub fn datastore(mut self, path: &Path) -> Builder {
        self.datastore = path.to_path_buf();
        self
    }

    pub fn crypto(mut self, rnd: crypto::SharedRandom, source_key: crypto::Keys) -> Builder {
        self.rnd = Some(rnd);
        self.source_key = Some(source_key);
        self
    }

    pub async fn build(self) -> Result<Server, BuilderError> {
        // "Building" the server here means ensuring all the required fields have been provided
        // and are valid. All the actor management is done when the server actually starts.
        let connection = self.connection.ok_or(BuilderError::MissingConnection)?;
        let address = self.address.ok_or(BuilderError::MissingAddress)?;
        let broker_info = self.broker.ok_or(BuilderError::MissingBrokerInfo)?;
        let rnd = self.rnd.ok_or(BuilderError::MissingCrypto)?;
        let _source_key = self.source_key.ok_or(BuilderError::MissingCrypto)?;
        Ok(Server {
            connection,
            broker_info,
            address,
            roots: self.roots,
            rnd,
            datastore_path: self.datastore,
            filestore_path: self.filestore,
            token: self.token,
        })
    }
}
