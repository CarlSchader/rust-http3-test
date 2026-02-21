// src/bin/http2-client.rs
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use clap::Parser;
use http_body_util::{BodyExt, Empty};
use hyper::client::conn::http2;
use hyper::Request;
use hyper_util::rt::{TokioExecutor, TokioIo};
use rustls::pki_types::ServerName;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;

#[derive(Parser, Debug)]
#[command(name = "http2-client", about = "HTTP/2 client")]
struct Args {
    /// Host to connect to
    #[arg(short = 'H', long, default_value = "127.0.0.1")]
    host: String,

    /// Port to connect to
    #[arg(short, long, default_value = "8443")]
    port: u16,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("Failed to install rustls crypto provider");

    let mut tls_config = rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(SkipVerification))
        .with_no_client_auth();

    tls_config.alpn_protocols = vec![b"h2".to_vec()];

    let tls_connector = TlsConnector::from(Arc::new(tls_config));

    // Convert 0.0.0.0 to 127.0.0.1 since 0.0.0.0 is not a valid destination
    let host = if args.host == "0.0.0.0" {
        "127.0.0.1".to_string()
    } else {
        args.host
    };

    let start_time = Instant::now();

    let addr: SocketAddr = format!("{}:{}", host, args.port).parse()?;
    let stream = TcpStream::connect(addr).await?;

    let server_name = ServerName::try_from(host.clone())?.to_owned();
    let tls_stream = tls_connector.connect(server_name, stream).await?;
    println!("TLS connection established ({}ms)", start_time.elapsed().as_millis());

    let (mut sender, conn) = http2::handshake(TokioExecutor::new(), TokioIo::new(tls_stream)).await?;
    println!("HTTP/2 connection established ({}ms)", start_time.elapsed().as_millis());

    tokio::spawn(async move {
        if let Err(e) = conn.await {
            eprintln!("Connection error: {}", e);
        }
    });

    let uri = format!("https://{}:{}/", host, args.port);
    let req = Request::builder()
        .uri(&uri)
        .method("GET")
        .body(Empty::<Bytes>::new())?;

    let resp = sender.send_request(req).await?;
    println!("Response: {:?} {}", resp.version(), resp.status());

    let body = resp.into_body().collect().await?.to_bytes();
    let mut stdout = tokio::io::stdout();
    stdout.write_all(&body).await?;
    stdout.flush().await?;

    let total_latency = start_time.elapsed();
    println!("\nTotal latency: {:.2}ms", total_latency.as_secs_f64() * 1000.0);

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
