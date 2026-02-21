// src/bin/http2-client.rs
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use clap::Parser;
use http_body_util::{BodyExt, Full, StreamBody};
use hyper::body::Frame;
use hyper::client::conn::http2;
use hyper::Request;
use hyper_util::rt::{TokioExecutor, TokioIo};
use rustls::pki_types::ServerName;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_rustls::TlsConnector;
use tokio_stream::StreamExt;

type BoxBody = http_body_util::combinators::BoxBody<Bytes, anyhow::Error>;

fn full_body(data: Bytes) -> BoxBody {
    Full::new(data).map_err(|e| anyhow::anyhow!(e)).boxed()
}

fn stream_body(rx: mpsc::Receiver<Bytes>) -> BoxBody {
    let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
    let stream = stream.map(|b| Ok::<_, anyhow::Error>(Frame::data(b)));
    StreamBody::new(stream).boxed()
}

const SMALL_SIZE: usize = 20;
const LARGE_SIZE: usize = 1_048_576; // 1 MB
const STREAM_CHUNKS: usize = 100;
const STREAM_CHUNK_SIZE: usize = 10_240; // 10 KB
const MANY_COUNT: usize = 1000;

#[derive(Parser, Debug)]
#[command(name = "http2-client", about = "HTTP/2 benchmark client")]
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

    let host = if args.host == "0.0.0.0" {
        "127.0.0.1".to_string()
    } else {
        args.host
    };

    let addr: SocketAddr = format!("{}:{}", host, args.port).parse()?;
    let stream = TcpStream::connect(addr).await?;

    let server_name = ServerName::try_from(host.clone())?.to_owned();
    let tls_stream = tls_connector.connect(server_name, stream).await?;

    let (sender, conn) =
        http2::handshake(TokioExecutor::new(), TokioIo::new(tls_stream)).await?;

    tokio::spawn(async move {
        if let Err(e) = conn.await {
            eprintln!("Connection error: {}", e);
        }
    });

    let base_uri = format!("https://{}:{}", host, args.port);

    println!("=== HTTP/2 Benchmark ===\n");

    // 1. Small payload
    test_small_payload(&sender, &base_uri).await?;

    // 2. Large payload
    test_large_payload(&sender, &base_uri).await?;

    // 3. Server streaming
    test_server_stream(&sender, &base_uri).await?;

    // 4. Client streaming
    test_client_stream(&sender, &base_uri).await?;

    // 5. Bidirectional streaming
    test_bidi_stream(&sender, &base_uri).await?;

    // 6. Many small requests
    test_many_requests(&sender, &base_uri).await?;

    Ok(())
}

async fn test_small_payload(
    sender: &http2::SendRequest<BoxBody>,
    base_uri: &str,
) -> anyhow::Result<()> {
    let data = Bytes::from(vec![0x42u8; SMALL_SIZE]);
    let start = Instant::now();

    let req = Request::post(format!("{}/small", base_uri))
        .body(full_body(data))?;

    let resp = sender.clone().send_request(req).await?;
    let body = resp.into_body().collect().await?.to_bytes();

    let elapsed = start.elapsed();
    println!(
        "Small payload ({} B):    {:.2}ms  (sent {} B, recv {} B)",
        SMALL_SIZE,
        elapsed.as_secs_f64() * 1000.0,
        SMALL_SIZE,
        body.len(),
    );
    Ok(())
}

async fn test_large_payload(
    sender: &http2::SendRequest<BoxBody>,
    base_uri: &str,
) -> anyhow::Result<()> {
    let data = Bytes::from(vec![0x42u8; LARGE_SIZE]);
    let start = Instant::now();

    let req = Request::post(format!("{}/large", base_uri))
        .body(full_body(data))?;

    let resp = sender.clone().send_request(req).await?;
    let body = resp.into_body().collect().await?.to_bytes();

    let elapsed = start.elapsed();
    let total_bytes = (LARGE_SIZE + body.len()) as f64;
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
    sender: &http2::SendRequest<BoxBody>,
    base_uri: &str,
) -> anyhow::Result<()> {
    let start = Instant::now();

    let params = format!("{}:{}", STREAM_CHUNKS, STREAM_CHUNK_SIZE);
    let req = Request::post(format!("{}/server-stream", base_uri))
        .body(full_body(Bytes::from(params)))?;

    let resp = sender.clone().send_request(req).await?;
    let mut body = resp.into_body();

    let mut total_bytes: usize = 0;
    let mut count: usize = 0;
    while let Some(frame) = body.frame().await {
        let frame = frame?;
        if let Some(data) = frame.data_ref() {
            total_bytes += data.len();
            count += 1;
        }
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
    sender: &http2::SendRequest<BoxBody>,
    base_uri: &str,
) -> anyhow::Result<()> {
    let start = Instant::now();

    let (tx, rx) = mpsc::channel::<Bytes>(32);

    let req = Request::post(format!("{}/client-stream", base_uri))
        .body(stream_body(rx))?;

    let resp_fut = sender.clone().send_request(req);

    // Send chunks
    let send_handle = tokio::spawn(async move {
        let chunk = Bytes::from(vec![0x42u8; STREAM_CHUNK_SIZE]);
        for _ in 0..STREAM_CHUNKS {
            if tx.send(chunk.clone()).await.is_err() {
                break;
            }
        }
    });

    let resp = resp_fut.await?;
    send_handle.await?;
    let body = resp.into_body().collect().await?.to_bytes();

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
    sender: &http2::SendRequest<BoxBody>,
    base_uri: &str,
) -> anyhow::Result<()> {
    let bidi_chunks = 50;
    let start = Instant::now();

    let (tx, rx) = mpsc::channel::<Bytes>(32);

    let req = Request::post(format!("{}/bidi", base_uri))
        .body(stream_body(rx))?;

    let resp_fut = sender.clone().send_request(req);

    // Sender task
    let send_handle = tokio::spawn(async move {
        let chunk = Bytes::from(vec![0x42u8; STREAM_CHUNK_SIZE]);
        for _ in 0..bidi_chunks {
            if tx.send(chunk.clone()).await.is_err() {
                break;
            }
        }
    });

    let resp = resp_fut.await?;
    let mut body = resp.into_body();

    let mut recv_count: usize = 0;
    let mut recv_bytes: usize = 0;
    while let Some(frame) = body.frame().await {
        let frame = frame?;
        if let Some(data) = frame.data_ref() {
            recv_bytes += data.len();
            recv_count += 1;
        }
    }

    send_handle.await?;

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
    sender: &http2::SendRequest<BoxBody>,
    base_uri: &str,
) -> anyhow::Result<()> {
    let data = Bytes::from(vec![0x42u8; SMALL_SIZE]);
    let start = Instant::now();

    for _ in 0..MANY_COUNT {
        let req = Request::post(format!("{}/small", base_uri))
            .body(full_body(data.clone()))?;
        let resp = sender.clone().send_request(req).await?;
        resp.into_body().collect().await?;
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
