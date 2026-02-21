// src/bin/server.rs - HTTP/3 benchmark server
use std::net::SocketAddr;
use std::sync::Arc;

use bytes::{Buf, Bytes};
use clap::Parser;
use quinn::{Endpoint, ServerConfig};
use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};

#[derive(Parser, Debug)]
#[command(name = "h3-server", about = "HTTP/3 benchmark server")]
struct Args {
    /// Port to listen on
    #[arg(short, long, default_value = "4433")]
    port: u16,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("Failed to install rustls crypto provider");

    let rcgen::CertifiedKey { cert, signing_key } =
        rcgen::generate_simple_self_signed(vec!["localhost".into()])?;

    let cert_der = CertificateDer::from(cert);
    let key_der = PrivatePkcs8KeyDer::from(signing_key.serialize_der());

    let mut tls_config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert_der], key_der.into())?;
    tls_config.max_early_data_size = u32::MAX;
    tls_config.alpn_protocols = vec![b"h3".to_vec()];

    let server_config = ServerConfig::with_crypto(Arc::new(
        h3_quinn::quinn::crypto::rustls::QuicServerConfig::try_from(tls_config)?,
    ));

    let addr: SocketAddr = format!("0.0.0.0:{}", args.port).parse()?;
    let endpoint = Endpoint::server(server_config, addr)?;
    println!("HTTP/3 benchmark server listening on {}", addr);

    while let Some(conn) = endpoint.accept().await {
        tokio::spawn(async move {
            if let Err(e) = handle_connection(conn.await.unwrap()).await {
                eprintln!("Connection error: {}", e);
            }
        });
    }

    Ok(())
}

async fn handle_connection(conn: h3_quinn::quinn::Connection) -> anyhow::Result<()> {
    let mut h3_conn = h3::server::builder()
        .build(h3_quinn::Connection::new(conn))
        .await?;

    loop {
        match h3_conn.accept().await? {
            Some(resolver) => {
                let (req, stream) = resolver.resolve_request().await?;
                tokio::spawn(async move {
                    let path = req.uri().path().to_string();
                    if let Err(e) = route_request(&path, req, stream).await {
                        eprintln!("Request error on {}: {}", path, e);
                    }
                });
            }
            None => break,
        }
    }
    Ok(())
}

async fn route_request(
    path: &str,
    _req: http::Request<()>,
    stream: h3::server::RequestStream<h3_quinn::BidiStream<bytes::Bytes>, bytes::Bytes>,
) -> anyhow::Result<()> {
    match path {
        "/small" => handle_small(stream).await,
        "/large" => handle_large(stream).await,
        "/server-stream" => handle_server_stream(stream).await,
        "/client-stream" => handle_client_stream(stream).await,
        "/bidi" => handle_bidi(stream).await,
        _ => {
            let mut stream = stream;
            let resp = http::Response::builder().status(404).body(())?;
            stream.send_response(resp).await?;
            stream.send_data(Bytes::from("Not Found\n")).await?;
            stream.finish().await?;
            Ok(())
        }
    }
}

async fn handle_small(
    mut stream: h3::server::RequestStream<h3_quinn::BidiStream<bytes::Bytes>, bytes::Bytes>,
) -> anyhow::Result<()> {
    // Read request body
    let mut data = Vec::new();
    while let Some(mut chunk) = stream.recv_data().await? {
        while chunk.has_remaining() {
            let bytes = chunk.chunk();
            data.extend_from_slice(bytes);
            let len = bytes.len();
            chunk.advance(len);
        }
    }

    let resp = http::Response::builder().status(200).body(())?;
    stream.send_response(resp).await?;
    stream.send_data(Bytes::from(data)).await?;
    stream.finish().await?;
    Ok(())
}

async fn handle_large(
    mut stream: h3::server::RequestStream<h3_quinn::BidiStream<bytes::Bytes>, bytes::Bytes>,
) -> anyhow::Result<()> {
    // Read request body to get size
    let mut total_size = 0;
    while let Some(mut chunk) = stream.recv_data().await? {
        while chunk.has_remaining() {
            let len = chunk.chunk().len();
            total_size += len;
            chunk.advance(len);
        }
    }

    let resp = http::Response::builder().status(200).body(())?;
    stream.send_response(resp).await?;
    let data = vec![0xABu8; total_size];
    stream.send_data(Bytes::from(data)).await?;
    stream.finish().await?;
    Ok(())
}

async fn handle_server_stream(
    mut stream: h3::server::RequestStream<h3_quinn::BidiStream<bytes::Bytes>, bytes::Bytes>,
) -> anyhow::Result<()> {
    // Read request body to get params
    let mut body = Vec::new();
    while let Some(mut chunk) = stream.recv_data().await? {
        while chunk.has_remaining() {
            let bytes = chunk.chunk();
            body.extend_from_slice(bytes);
            let len = bytes.len();
            chunk.advance(len);
        }
    }

    let params = String::from_utf8_lossy(&body);
    let parts: Vec<&str> = params.split(':').collect();
    let chunks: usize = parts.first().and_then(|s| s.parse().ok()).unwrap_or(100);
    let chunk_size: usize = parts.get(1).and_then(|s| s.parse().ok()).unwrap_or(10240);

    let resp = http::Response::builder().status(200).body(())?;
    stream.send_response(resp).await?;

    let chunk_data = Bytes::from(vec![0xABu8; chunk_size]);
    for _ in 0..chunks {
        stream.send_data(chunk_data.clone()).await?;
    }
    stream.finish().await?;
    Ok(())
}

async fn handle_client_stream(
    mut stream: h3::server::RequestStream<h3_quinn::BidiStream<bytes::Bytes>, bytes::Bytes>,
) -> anyhow::Result<()> {
    let mut total_bytes: usize = 0;
    let mut total_chunks: usize = 0;

    while let Some(mut chunk) = stream.recv_data().await? {
        while chunk.has_remaining() {
            let len = chunk.chunk().len();
            total_bytes += len;
            chunk.advance(len);
        }
        total_chunks += 1;
    }

    let resp = http::Response::builder().status(200).body(())?;
    stream.send_response(resp).await?;
    let result = format!("{}:{}", total_bytes, total_chunks);
    stream.send_data(Bytes::from(result)).await?;
    stream.finish().await?;
    Ok(())
}

async fn handle_bidi(
    mut stream: h3::server::RequestStream<h3_quinn::BidiStream<bytes::Bytes>, bytes::Bytes>,
) -> anyhow::Result<()> {
    // Send response headers immediately so client can start reading
    let resp = http::Response::builder().status(200).body(())?;
    stream.send_response(resp).await?;

    // Read incoming data and echo it back
    while let Some(mut chunk) = stream.recv_data().await? {
        while chunk.has_remaining() {
            let bytes = chunk.chunk();
            let data = Bytes::copy_from_slice(bytes);
            let len = bytes.len();
            chunk.advance(len);
            stream.send_data(data).await?;
        }
    }

    stream.finish().await?;
    Ok(())
}
