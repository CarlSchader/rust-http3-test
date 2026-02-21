// src/bin/grpc-client.rs
use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::Parser;
use http::Uri;
use hyper_util::rt::TokioIo;
use indicatif::{ProgressBar, ProgressStyle};
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
const BIDI_CHUNKS: usize = 50;

struct TestResult {
    name: String,
    latency: Duration,
    throughput_mbs: Option<f64>,
    details: String,
}

fn make_pb(len: u64) -> ProgressBar {
    let pb = ProgressBar::new(len);
    pb.set_style(
        ProgressStyle::with_template("  [{bar:40}] {pos}/{len}")
            .unwrap()
            .progress_chars("##-"),
    );
    pb
}

fn print_summary(title: &str, results: &[TestResult]) {
    println!("\n{}", "=".repeat(72));
    println!("{}", title);
    println!("{}", "=".repeat(72));
    println!(
        "{:<30} {:>10} {:>14}   {}",
        "Test", "Latency", "Throughput", "Details"
    );
    println!("{}", "-".repeat(72));
    for r in results {
        let latency = format!("{:.2}ms", r.latency.as_secs_f64() * 1000.0);
        let throughput = match r.throughput_mbs {
            Some(t) => format!("{:.1} MB/s", t),
            None => "-".to_string(),
        };
        println!(
            "{:<30} {:>10} {:>14}   {}",
            r.name, latency, throughput, r.details,
        );
    }
    println!("{}", "=".repeat(72));
}

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

    let mut results = Vec::new();

    println!("[1/6] Small payload...");
    results.push(test_small_payload(&mut client).await?);

    println!("[2/6] Large payload...");
    results.push(test_large_payload(&mut client).await?);

    println!("[3/6] Server streaming...");
    results.push(test_server_stream(&mut client).await?);

    println!("[4/6] Client streaming...");
    results.push(test_client_stream(&mut client).await?);

    println!("[5/6] Bidirectional streaming...");
    results.push(test_bidi_stream(&mut client).await?);

    println!("[6/6] Many small requests...");
    results.push(test_many_requests(&mut client).await?);

    print_summary("gRPC Benchmark Results", &results);

    Ok(())
}

async fn test_small_payload(
    client: &mut BenchClient<tonic::transport::Channel>,
) -> anyhow::Result<TestResult> {
    let data = vec![0x42u8; SMALL_SIZE];
    let start = Instant::now();

    let resp = client
        .small_payload(Payload { data: data.clone() })
        .await?;

    let latency = start.elapsed();
    let resp_size = resp.into_inner().data.len();
    let details = format!("sent {} B, recv {} B", SMALL_SIZE, resp_size);
    println!(
        "  {:.2}ms  ({})",
        latency.as_secs_f64() * 1000.0,
        details,
    );
    Ok(TestResult {
        name: format!("Small payload ({} B)", SMALL_SIZE),
        latency,
        throughput_mbs: None,
        details,
    })
}

async fn test_large_payload(
    client: &mut BenchClient<tonic::transport::Channel>,
) -> anyhow::Result<TestResult> {
    let data = vec![0x42u8; LARGE_SIZE];
    let start = Instant::now();

    let resp = client.large_payload(Payload { data }).await?;

    let latency = start.elapsed();
    let resp_size = resp.into_inner().data.len();
    let total_bytes = (LARGE_SIZE + resp_size) as f64;
    let throughput = total_bytes / latency.as_secs_f64() / 1_048_576.0;
    println!(
        "  {:.2}ms  ({:.1} MB/s)",
        latency.as_secs_f64() * 1000.0,
        throughput,
    );
    Ok(TestResult {
        name: format!("Large payload ({:.0} KB)", LARGE_SIZE as f64 / 1024.0),
        latency,
        throughput_mbs: Some(throughput),
        details: String::new(),
    })
}

async fn test_server_stream(
    client: &mut BenchClient<tonic::transport::Channel>,
) -> anyhow::Result<TestResult> {
    let pb = make_pb(STREAM_CHUNKS as u64);
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
        pb.set_position(count as u64);
    }

    let latency = start.elapsed();
    pb.finish_and_clear();

    let throughput = total_bytes as f64 / latency.as_secs_f64() / 1_048_576.0;
    let details = format!("{} chunks, {} B", count, total_bytes);
    println!(
        "  {:.2}ms  ({:.1} MB/s, {})",
        latency.as_secs_f64() * 1000.0,
        throughput,
        details,
    );
    Ok(TestResult {
        name: format!("Server stream ({}x{} KB)", STREAM_CHUNKS, STREAM_CHUNK_SIZE / 1024),
        latency,
        throughput_mbs: Some(throughput),
        details,
    })
}

async fn test_client_stream(
    client: &mut BenchClient<tonic::transport::Channel>,
) -> anyhow::Result<TestResult> {
    let pb = make_pb(STREAM_CHUNKS as u64);
    let start = Instant::now();

    let chunk_data = vec![0x42u8; STREAM_CHUNK_SIZE];
    let pb_clone = pb.clone();
    let stream = tokio_stream::iter((0..STREAM_CHUNKS).map(move |i| {
        pb_clone.set_position((i + 1) as u64);
        Payload {
            data: chunk_data.clone(),
        }
    }));

    let resp = client.client_stream(stream).await?;
    let resp = resp.into_inner();

    let latency = start.elapsed();
    pb.finish_and_clear();

    let total_bytes = STREAM_CHUNKS * STREAM_CHUNK_SIZE;
    let throughput = total_bytes as f64 / latency.as_secs_f64() / 1_048_576.0;
    let details = format!("server got {} chunks, {} B", resp.total_chunks, resp.total_bytes);
    println!(
        "  {:.2}ms  ({:.1} MB/s, {})",
        latency.as_secs_f64() * 1000.0,
        throughput,
        details,
    );
    Ok(TestResult {
        name: format!("Client stream ({}x{} KB)", STREAM_CHUNKS, STREAM_CHUNK_SIZE / 1024),
        latency,
        throughput_mbs: Some(throughput),
        details,
    })
}

async fn test_bidi_stream(
    client: &mut BenchClient<tonic::transport::Channel>,
) -> anyhow::Result<TestResult> {
    let pb = make_pb((BIDI_CHUNKS * 2) as u64);
    let start = Instant::now();

    let chunk_data = vec![0x42u8; STREAM_CHUNK_SIZE];
    let (tx, rx) = tokio::sync::mpsc::channel(32);

    let send_data = chunk_data.clone();
    let pb_send = pb.clone();
    let send_handle = tokio::spawn(async move {
        for _ in 0..BIDI_CHUNKS {
            if tx
                .send(Payload {
                    data: send_data.clone(),
                })
                .await
                .is_err()
            {
                break;
            }
            pb_send.inc(1);
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
        pb.inc(1);
    }

    send_handle.await?;

    let latency = start.elapsed();
    pb.finish_and_clear();

    let total_bytes = (BIDI_CHUNKS * STREAM_CHUNK_SIZE + recv_bytes) as f64;
    let throughput = total_bytes / latency.as_secs_f64() / 1_048_576.0;
    let details = format!("sent {}, recv {} chunks", BIDI_CHUNKS, recv_count);
    println!(
        "  {:.2}ms  ({:.1} MB/s, {})",
        latency.as_secs_f64() * 1000.0,
        throughput,
        details,
    );
    Ok(TestResult {
        name: format!("Bidi stream ({} each way)", BIDI_CHUNKS),
        latency,
        throughput_mbs: Some(throughput),
        details,
    })
}

async fn test_many_requests(
    client: &mut BenchClient<tonic::transport::Channel>,
) -> anyhow::Result<TestResult> {
    let pb = make_pb(MANY_COUNT as u64);
    let data = vec![0x42u8; SMALL_SIZE];
    let start = Instant::now();

    for _ in 0..MANY_COUNT {
        client
            .small_payload(Payload { data: data.clone() })
            .await?;
        pb.inc(1);
    }

    let latency = start.elapsed();
    pb.finish_and_clear();

    let rps = MANY_COUNT as f64 / latency.as_secs_f64();
    let details = format!("{:.0} req/s", rps);
    println!(
        "  {:.2}ms  ({})",
        latency.as_secs_f64() * 1000.0,
        details,
    );
    Ok(TestResult {
        name: format!("Many requests ({})", MANY_COUNT),
        latency,
        throughput_mbs: None,
        details,
    })
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
