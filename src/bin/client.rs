// src/bin/client.rs
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;

use rustls::pki_types::ServerName;
use tokio::io::AsyncWriteExt;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("Failed to install rustls crypto provider");

    let mut tls_config = rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(SkipVerification))
        .with_no_client_auth();

    tls_config.alpn_protocols = vec![b"h3".to_vec()];

    let client_config = quinn::ClientConfig::new(Arc::new(
        quinn::crypto::rustls::QuicClientConfig::try_from(tls_config)?,
    ));

    let mut endpoint = quinn::Endpoint::client("0.0.0.0:0".parse()?)?;
    endpoint.set_default_client_config(client_config);

    let start_time = Instant::now();

    let addr: SocketAddr = "127.0.0.1:4433".parse()?;
    let conn = endpoint.connect(addr, "localhost")?.await?;
    println!("QUIC connection established ({}ms)", start_time.elapsed().as_millis());

    let quinn_conn = h3_quinn::Connection::new(conn);
    let (mut driver, mut send_request) = h3::client::new(quinn_conn).await?;

    let drive = tokio::spawn(async move {
        futures::future::poll_fn(|cx| driver.poll_close(cx)).await
    });

    let req = http::Request::builder()
        .uri("https://localhost:4433/")
        .method("GET")
        .body(())?;

    let mut stream = send_request.send_request(req).await?;
    stream.finish().await?;

    let resp = stream.recv_response().await?;
    println!("Response: {:?} {}", resp.version(), resp.status());

    while let Some(mut chunk) = stream.recv_data().await? {
        let mut stdout = tokio::io::stdout();
        stdout.write_all_buf(&mut chunk).await?;
        stdout.flush().await?;
    }

    let total_latency = start_time.elapsed();
    println!("\nTotal latency: {:.2}ms", total_latency.as_secs_f64() * 1000.0);

    drop(send_request);
    drive.await?;  // fixed: only one ?, ConnectionError doesn't impl Try
    endpoint.wait_idle().await;

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

    fn verify_tls12_signature(&self, _: &[u8], _: &rustls::pki_types::CertificateDer<'_>, _: &rustls::DigitallySignedStruct) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(&self, _: &[u8], _: &rustls::pki_types::CertificateDer<'_>, _: &rustls::DigitallySignedStruct) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
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
