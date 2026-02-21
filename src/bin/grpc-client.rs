// src/bin/grpc-client.rs
use std::sync::Arc;
use std::time::Instant;

use clap::Parser;
use http::Uri;
use hyper_util::rt::TokioIo;
use rustls::pki_types::ServerName;
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use tonic::transport::Endpoint;
use tower::service_fn;

pub mod hello {
    tonic::include_proto!("hello");
}

use hello::greeter_client::GreeterClient;
use hello::HelloRequest;

#[derive(Parser, Debug)]
#[command(name = "grpc-client", about = "gRPC client")]
struct Args {
    /// Host to connect to
    #[arg(short = 'H', long, default_value = "127.0.0.1")]
    host: String,

    /// Port to connect to
    #[arg(short, long, default_value = "50051")]
    port: u16,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("Failed to install rustls crypto provider");

    // Convert 0.0.0.0 to 127.0.0.1 since 0.0.0.0 is not a valid destination
    let host = if args.host == "0.0.0.0" {
        "127.0.0.1".to_string()
    } else {
        args.host
    };
    let port = args.port;

    let start_time = Instant::now();

    // Configure TLS to skip verification (for self-signed certs)
    let mut tls_config = rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(SkipVerification))
        .with_no_client_auth();
    tls_config.alpn_protocols = vec![b"h2".to_vec()];

    let tls_connector = TlsConnector::from(Arc::new(tls_config));

    // Create a custom connector that handles TLS
    let connector = tower::ServiceBuilder::new().service(service_fn(move |_: Uri| {
        let tls_connector = tls_connector.clone();
        let host = host.clone();
        async move {
            let stream = TcpStream::connect(format!("{}:{}", host, port)).await?;
            let server_name = ServerName::try_from("localhost")
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?
                .to_owned();
            let tls_stream = tls_connector.connect(server_name, stream).await?;
            Ok::<_, std::io::Error>(TokioIo::new(tls_stream))
        }
    }));

    // Use http:// scheme to bypass tonic's TLS check - we handle TLS ourselves
    let channel = Endpoint::from_static("http://[::]:50051")
        .connect_with_connector(connector)
        .await?;

    println!("gRPC channel established ({}ms)", start_time.elapsed().as_millis());

    let mut client = GreeterClient::new(channel);

    let request = tonic::Request::new(HelloRequest {
        name: "World".into(),
    });

    let response = client.say_hello(request).await?;
    println!("Response: {}", response.into_inner().message);

    let total_latency = start_time.elapsed();
    println!("Total latency: {:.2}ms", total_latency.as_secs_f64() * 1000.0);

    Ok(())
}

#[derive(Debug)]
struct SkipVerification;

impl rustls::client::danger::ServerCertVerifier for SkipVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _: &[u8],
        _: &rustls::pki_types::CertificateDer<'_>,
        _: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _: &[u8],
        _: &rustls::pki_types::CertificateDer<'_>,
        _: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        vec![
            rustls::SignatureScheme::RSA_PKCS1_SHA256,
            rustls::SignatureScheme::ECDSA_NISTP256_SHA256,
            rustls::SignatureScheme::ED25519,
        ]
    }
}
