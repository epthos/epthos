use settings::connection::Info;
use tonic::transport::{Certificate, Identity};

pub fn broker_info() -> Info {
    Info::new(
        Identity::from_pem(
            include_str!("certs/broker-cert.pem"),
            include_str!("certs/broker-key.pem"),
        ),
        Certificate::from_pem(include_str!("certs/root.pem")),
    )
}
