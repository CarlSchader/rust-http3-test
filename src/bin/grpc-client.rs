// src/bin/grpc-client.rs
use std::sync::Arc;
use std::time::Instant;

use clap::Parser;
use http::Uri;
use hyper_util::rt::TokioIo;
use rustls::pki_types::ServerName;
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use tokio_stream::StreamExt;
use tonic::transport::Endpoint;
use tower::service_fn;

pub mod bench {
    tonic::include_proto!("hello");
}

use bench::bench_client::BenchClient;
use bench::{Payload, StreamRequest};

const SMALL_SIZE: usize = 20;
const LARGE_SIZE: usize = 1_048_576; // 1 MB
const STREAM_CHUNKS: usize = 100;
const STREAM_CHUNK_SIZE: usize = 10_240; // 10 KB
const MANY_COUNT: usize = 1000;

#[derive(Parser, Debug)]
#[command(name = "grpc-client", about = "gRPC benchmark client")]
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

    let host = if args.host == "0.0.0.0" {
        "127.0.0.1".to_string()
    } else {
        args.host
    };
    let port = args.port;

    // Configure TLS
    let mut tls_config = rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(SkipVerification))
        .with_no_client_auth();
    tls_config.alpn_protocols = vec![b"h2".to_vec()];

    let tls_connector = TlsConnector::from(Arc::new(tls_config));

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

    let channel = Endpoint::from_static("http://[::]:50051")
        .connect_with_connector(connector)
        .await?;

    let mut client = BenchClient::new(channel);

    println!("=== gRPC Benchmark ===\n");

    // 1. Small payload
    test_small_payload(&mut client).await?;

    // 2. Large payload
    test_large_payload(&mut client).await?;

    // 3. Server streaming
    test_server_stream(&mut client).await?;

    // 4. Client streaming
    test_client_stream(&mut client).await?;

    // 5. Bidirectional streaming
    test_bidi_stream(&mut client).await?;

    // 6. Many small requests
    test_many_requests(&mut client).await?;

    Ok(())
}

async fn test_small_payload(
    client: &mut BenchClient<tonic::transport::Channel>,
) -> anyhow::Result<()> {
    let data = vec![0x42u8; SMALL_SIZE];
    let start = Instant::now();

    let resp = client
        .small_payload(Payload { data: data.clone() })
        .await?;

    let elapsed = start.elapsed();
    let resp_size = resp.into_inner().data.len();
    println!(
        "Small payload ({} B):    {:.2}ms  (sent {} B, recv {} B)",
        SMALL_SIZE,
        elapsed.as_secs_f64() * 1000.0,
        SMALL_SIZE,
        resp_size,
    );
    Ok(())
}

async fn test_large_payload(
    client: &mut BenchClient<tonic::transport::Channel>,
) -> anyhow::Result<()> {
    let data = vec![0x42u8; LARGE_SIZE];
    let start = Instant::now();

    let resp = client.large_payload(Payload { data }).await?;

    let elapsed = start.elapsed();
    let resp_size = resp.into_inner().data.len();
    let total_bytes = (LARGE_SIZE + resp_size) as f64;
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
    client: &mut BenchClient<tonic::transport::Channel>,
) -> anyhow::Result<()> {
    let start = Instant::now();

    let mut stream = client
        .server_stream(StreamRequest {
            chunks: STREAM_CHUNKS as i32,
            chunk_size: STREAM_CHUNK_SIZE as i32,
        })
        .await?
        .into_inner();

    let mut total_bytes: usize = 0;
    let mut count: usize = 0;
    while let Some(payload) = stream.next().await {
        let payload = payload?;
        total_bytes += payload.data.len();
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
    client: &mut BenchClient<tonic::transport::Channel>,
) -> anyhow::Result<()> {
    let start = Instant::now();

    let chunk_data = vec![0x42u8; STREAM_CHUNK_SIZE];
    let stream = tokio_stream::iter((0..STREAM_CHUNKS).map(move |_| Payload {
        data: chunk_data.clone(),
    }));

    let resp = client.client_stream(stream).await?;
    let resp = resp.into_inner();

    let elapsed = start.elapsed();
    let total_bytes = STREAM_CHUNKS * STREAM_CHUNK_SIZE;
    let throughput = total_bytes as f64 / elapsed.as_secs_f64() / 1_048_576.0;
    println!(
        "Client stream ({} x {} KB): {:.2}ms  ({:.1} MB/s, server got {} chunks, {} B)",
        STREAM_CHUNKS,
        STREAM_CHUNK_SIZE / 1024,
        elapsed.as_secs_f64() * 1000.0,
        throughput,
        resp.total_chunks,
        resp.total_bytes,
    );
    Ok(())
}

async fn test_bidi_stream(
    client: &mut BenchClient<tonic::transport::Channel>,
) -> anyhow::Result<()> {
    let bidi_chunks = 50;
    let start = Instant::now();

    let chunk_data = vec![0x42u8; STREAM_CHUNK_SIZE];
    let (tx, rx) = tokio::sync::mpsc::channel(32);

    // Sender task
    let send_data = chunk_data.clone();
    let send_handle = tokio::spawn(async move {
        for _ in 0..bidi_chunks {
            if tx
                .send(Payload {
                    data: send_data.clone(),
                })
                .await
                .is_err()
            {
                break;
            }
        }
    });

    let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
    let mut resp_stream = client.bidi_stream(stream).await?.into_inner();

    let mut recv_count: usize = 0;
    let mut recv_bytes: usize = 0;
    while let Some(payload) = resp_stream.next().await {
        let payload = payload?;
        recv_bytes += payload.data.len();
        recv_count += 1;
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
    client: &mut BenchClient<tonic::transport::Channel>,
) -> anyhow::Result<()> {
    let data = vec![0x42u8; SMALL_SIZE];
    let start = Instant::now();

    for _ in 0..MANY_COUNT {
        client
            .small_payload(Payload { data: data.clone() })
            .await?;
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
