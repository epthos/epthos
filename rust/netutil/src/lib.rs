use std::net::IpAddr;

use anyhow::Context;

#[cfg(windows)]
use windows_sys::Win32::Networking::WinSock::{WSACleanup, WSAStartup};

#[derive(Debug, thiserror::Error)]
pub enum IntrospectError {
    #[error("Initialization failed")]
    InitFailed,
}

pub struct Introspect {}

impl Introspect {
    /// Create a new Introspect struct.
    pub fn new() -> Result<Introspect, IntrospectError> {
        Self::init().and(Ok(Introspect {}))
    }

    /// Resolve this machine's public IP addresses.
    pub fn resolve(&self) -> anyhow::Result<Vec<IpAddr>> {
        let ips = local_ip_address::list_afinet_netifas()
            .context("failed to lookup this machine's IP addresses")?;

        Ok(ips
            .into_iter()
            .map(|(_, ip)| ip)
            .filter(|ip| !ip.is_loopback())
            .collect())
    }

    fn init() -> Result<(), IntrospectError> {
        #[cfg(windows)]
        unsafe {
            let mut init = std::mem::zeroed();
            let status = WSAStartup(0x22, &mut init);
            if status != 0 {
                tracing::error!("WSAStartup failed with {:?}", status);
                return Err(IntrospectError::InitFailed);
            }
        }
        // Nothing to do on other platforms.
        Ok(())
    }
}

impl Drop for Introspect {
    fn drop(&mut self) {
        #[cfg(windows)]
        unsafe {
            WSACleanup();
        }
    }
}
