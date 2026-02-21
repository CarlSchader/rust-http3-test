// src/bin/client.rs - HTTP/3 benchmark client
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;

use bytes::{Buf, Bytes};
use clap::Parser;
use rustls::pki_types::ServerName;

const SMALL_SIZE: usize = 20;
const LARGE_SIZE: usize = 1_048_576; // 1 MB
const STREAM_CHUNKS: usize = 100;
const STREAM_CHUNK_SIZE: usize = 10_240; // 10 KB
const MANY_COUNT: usize = 1000;

#[derive(Parser, Debug)]
#[command(name = "h3-client", about = "HTTP/3 benchmark client")]
struct Args {
    /// Host to connect to
    #[arg(short = 'H', long, default_value = "127.0.0.1")]
    host: String,

    /// Port to connect to
    #[arg(short, long, default_value = "4433")]
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
    tls_config.alpn_protocols = vec![b"h3".to_vec()];

    let client_config = quinn::ClientConfig::new(Arc::new(
        quinn::crypto::rustls::QuicClientConfig::try_from(tls_config)?,
    ));

    let host = if args.host == "0.0.0.0" {
        "127.0.0.1".to_string()
    } else {
        args.host
    };

    let addr: SocketAddr = format!("{}:{}", host, args.port).parse()?;

    let mut endpoint = quinn::Endpoint::client("0.0.0.0:0".parse()?)?;
    endpoint.set_default_client_config(client_config);

    let conn = endpoint.connect(addr, &host)?.await?;

    let quinn_conn = h3_quinn::Connection::new(conn);
    let (mut driver, send_request) = h3::client::new(quinn_conn).await?;

    let drive = tokio::spawn(async move {
        futures::future::poll_fn(|cx| driver.poll_close(cx)).await
    });

    let base_uri = format!("https://{}:{}", host, args.port);

    println!("=== HTTP/3 Benchmark ===\n");

    // 1. Small payload
    test_small_payload(&send_request, &base_uri).await?;

    // 2. Large payload
    test_large_payload(&send_request, &base_uri).await?;

    // 3. Server streaming
    test_server_stream(&send_request, &base_uri).await?;

    // 4. Client streaming
    test_client_stream(&send_request, &base_uri).await?;

    // 5. Bidirectional streaming
    test_bidi_stream(&send_request, &base_uri).await?;

    // 6. Many small requests
    test_many_requests(&send_request, &base_uri).await?;

    drop(send_request);
    drive.await?;
    endpoint.wait_idle().await;

    Ok(())
}

async fn test_small_payload(
    send_request: &h3::client::SendRequest<h3_quinn::OpenStreams, bytes::Bytes>,
    base_uri: &str,
) -> anyhow::Result<()> {
    let data = Bytes::from(vec![0x42u8; SMALL_SIZE]);
    let start = Instant::now();

    let req = http::Request::post(format!("{}/small", base_uri)).body(())?;
    let mut stream = send_request.clone().send_request(req).await?;
    stream.send_data(data).await?;
    stream.finish().await?;

    let _resp = stream.recv_response().await?;
    let mut recv_size = 0;
    while let Some(mut chunk) = stream.recv_data().await? {
        while chunk.has_remaining() {
            let len = chunk.chunk().len();
            recv_size += len;
            chunk.advance(len);
        }
    }

    let elapsed = start.elapsed();
    println!(
        "Small payload ({} B):    {:.2}ms  (sent {} B, recv {} B)",
        SMALL_SIZE,
        elapsed.as_secs_f64() * 1000.0,
        SMALL_SIZE,
        recv_size,
    );
    Ok(())
}

async fn test_large_payload(
    send_request: &h3::client::SendRequest<h3_quinn::OpenStreams, bytes::Bytes>,
    base_uri: &str,
) -> anyhow::Result<()> {
    let data = Bytes::from(vec![0x42u8; LARGE_SIZE]);
    let start = Instant::now();

    let req = http::Request::post(format!("{}/large", base_uri)).body(())?;
    let mut stream = send_request.clone().send_request(req).await?;
    stream.send_data(data).await?;
    stream.finish().await?;

    let _resp = stream.recv_response().await?;
    let mut recv_size = 0;
    while let Some(mut chunk) = stream.recv_data().await? {
        while chunk.has_remaining() {
            let len = chunk.chunk().len();
            recv_size += len;
            chunk.advance(len);
        }
    }

    let elapsed = start.elapsed();
    let total_bytes = (LARGE_SIZE + recv_size) as f64;
    let throughput = total_bytes / elapsed.as_secs_f64() / 1_048_576.0;
    println!(
        "Large payload ({:.0} KB):  {:.2}ms  ({:.1} MB/s)",
        LARGE_SIZE as f64 / 1024.0,
        elapsed.as_secs_f64() * 1000.0,
        throughput,
    );
    Ok(())
}

async fn test_server_stream(
    send_request: &h3::client::SendRequest<h3_quinn::OpenStreams, bytes::Bytes>,
    base_uri: &str,
) -> anyhow::Result<()> {
    let start = Instant::now();

    let params = format!("{}:{}", STREAM_CHUNKS, STREAM_CHUNK_SIZE);
    let req = http::Request::post(format!("{}/server-stream", base_uri)).body(())?;
    let mut stream = send_request.clone().send_request(req).await?;
    stream.send_data(Bytes::from(params)).await?;
    stream.finish().await?;

    let _resp = stream.recv_response().await?;
    let mut total_bytes: usize = 0;
    let mut count: usize = 0;
    while let Some(mut chunk) = stream.recv_data().await? {
        while chunk.has_remaining() {
            let len = chunk.chunk().len();
            total_bytes += len;
            chunk.advance(len);
        }
        count += 1;
    }

    let elapsed = start.elapsed();
    let throughput = total_bytes as f64 / elapsed.as_secs_f64() / 1_048_576.0;
    println!(
        "Server stream ({} x {} KB): {:.2}ms  ({:.1} MB/s, {} chunks, {} B total)",
        STREAM_CHUNKS,
        STREAM_CHUNK_SIZE / 1024,
        elapsed.as_secs_f64() * 1000.0,
        throughput,
        count,
        total_bytes,
    );
    Ok(())
}

async fn test_client_stream(
    send_request: &h3::client::SendRequest<h3_quinn::OpenStreams, bytes::Bytes>,
    base_uri: &str,
) -> anyhow::Result<()> {
    let start = Instant::now();

    let req = http::Request::post(format!("{}/client-stream", base_uri)).body(())?;
    let mut stream = send_request.clone().send_request(req).await?;

    let chunk = Bytes::from(vec![0x42u8; STREAM_CHUNK_SIZE]);
    for _ in 0..STREAM_CHUNKS {
        stream.send_data(chunk.clone()).await?;
    }
    stream.finish().await?;

    let _resp = stream.recv_response().await?;
    let mut body = Vec::new();
    while let Some(mut chunk) = stream.recv_data().await? {
        while chunk.has_remaining() {
            let bytes = chunk.chunk();
            body.extend_from_slice(bytes);
            let len = bytes.len();
            chunk.advance(len);
        }
    }

    let elapsed = start.elapsed();
    let total_bytes = STREAM_CHUNKS * STREAM_CHUNK_SIZE;
    let throughput = total_bytes as f64 / elapsed.as_secs_f64() / 1_048_576.0;
    println!(
        "Client stream ({} x {} KB): {:.2}ms  ({:.1} MB/s, server resp: {})",
        STREAM_CHUNKS,
        STREAM_CHUNK_SIZE / 1024,
        elapsed.as_secs_f64() * 1000.0,
        throughput,
        String::from_utf8_lossy(&body),
    );
    Ok(())
}

async fn test_bidi_stream(
    send_request: &h3::client::SendRequest<h3_quinn::OpenStreams, bytes::Bytes>,
    base_uri: &str,
) -> anyhow::Result<()> {
    let bidi_chunks = 50;
    let start = Instant::now();

    let req = http::Request::post(format!("{}/bidi", base_uri)).body(())?;
    let mut stream = send_request.clone().send_request(req).await?;

    // Send all data, then finish, then read back
    // (h3 API doesn't easily allow concurrent send/recv on the same RequestStream
    //  without splitting, so we send then recv - the server echoes after reading)
    let chunk = Bytes::from(vec![0x42u8; STREAM_CHUNK_SIZE]);
    for _ in 0..bidi_chunks {
        stream.send_data(chunk.clone()).await?;
    }
    stream.finish().await?;

    let _resp = stream.recv_response().await?;
    let mut recv_count: usize = 0;
    let mut recv_bytes: usize = 0;
    while let Some(mut chunk) = stream.recv_data().await? {
        while chunk.has_remaining() {
            let len = chunk.chunk().len();
            recv_bytes += len;
            chunk.advance(len);
        }
        recv_count += 1;
    }

    let elapsed = start.elapsed();
    let total_bytes = (bidi_chunks * STREAM_CHUNK_SIZE + recv_bytes) as f64;
    let throughput = total_bytes / elapsed.as_secs_f64() / 1_048_576.0;
    println!(
        "Bidi stream ({} each way): {:.2}ms  ({:.1} MB/s, sent {}, recv {} chunks)",
        bidi_chunks,
        elapsed.as_secs_f64() * 1000.0,
        throughput,
        bidi_chunks,
        recv_count,
    );
    Ok(())
}

async fn test_many_requests(
    send_request: &h3::client::SendRequest<h3_quinn::OpenStreams, bytes::Bytes>,
    base_uri: &str,
) -> anyhow::Result<()> {
    let data = Bytes::from(vec![0x42u8; SMALL_SIZE]);
    let start = Instant::now();

    for _ in 0..MANY_COUNT {
        let req = http::Request::post(format!("{}/small", base_uri)).body(())?;
        let mut stream = send_request.clone().send_request(req).await?;
        stream.send_data(data.clone()).await?;
        stream.finish().await?;

        let _resp = stream.recv_response().await?;
        while let Some(mut chunk) = stream.recv_data().await? {
            while chunk.has_remaining() {
                let len = chunk.chunk().len();
                chunk.advance(len);
            }
        }
    }

    let elapsed = start.elapsed();
    let rps = MANY_COUNT as f64 / elapsed.as_secs_f64();
    println!(
        "Many requests ({} reqs):  {:.2}ms  ({:.0} req/s)",
        MANY_COUNT,
        elapsed.as_secs_f64() * 1000.0,
        rps,
    );
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
