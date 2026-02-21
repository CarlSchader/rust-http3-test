// src/bin/http2-server.rs
use std::net::SocketAddr;
use std::sync::Arc;

use bytes::Bytes;
use clap::Parser;
use http_body_util::{BodyExt, Full, StreamBody};
use hyper::body::Frame;
use hyper::server::conn::http2;
use hyper::service::service_fn;
use hyper::{Request, Response};
use hyper_util::rt::{TokioExecutor, TokioIo};
use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_rustls::TlsAcceptor;
use tokio_stream::StreamExt;

type BoxBody = http_body_util::combinators::BoxBody<Bytes, anyhow::Error>;

fn full_body(data: Bytes) -> BoxBody {
    Full::new(data).map_err(|e| anyhow::anyhow!(e)).boxed()
}

fn stream_body(
    rx: mpsc::Receiver<Bytes>,
) -> BoxBody {
    let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
    let stream = stream.map(|b| Ok::<_, anyhow::Error>(Frame::data(b)));
    StreamBody::new(stream).boxed()
}

#[derive(Parser, Debug)]
#[command(name = "http2-server", about = "HTTP/2 benchmark server")]
struct Args {
    /// Port to listen on
    #[arg(short, long, default_value = "8443")]
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
    tls_config.alpn_protocols = vec![b"h2".to_vec()];

    let tls_acceptor = TlsAcceptor::from(Arc::new(tls_config));

    let addr: SocketAddr = format!("0.0.0.0:{}", args.port).parse()?;
    let listener = TcpListener::bind(addr).await?;
    println!("HTTP/2 benchmark server listening on {}", addr);

    loop {
        let (stream, _peer_addr) = listener.accept().await?;
        let tls_acceptor = tls_acceptor.clone();

        tokio::spawn(async move {
            if let Err(e) = handle_connection(stream, tls_acceptor).await {
                eprintln!("Connection error: {}", e);
            }
        });
    }
}

async fn handle_connection(
    stream: tokio::net::TcpStream,
    tls_acceptor: TlsAcceptor,
) -> anyhow::Result<()> {
    let tls_stream = tls_acceptor.accept(stream).await?;

    http2::Builder::new(TokioExecutor::new())
        .serve_connection(TokioIo::new(tls_stream), service_fn(handle_request))
        .await?;

    Ok(())
}

async fn handle_request(
    req: Request<hyper::body::Incoming>,
) -> Result<Response<BoxBody>, anyhow::Error> {
    let path = req.uri().path().to_string();

    match path.as_str() {
        "/small" => handle_small(req).await,
        "/large" => handle_large(req).await,
        "/server-stream" => handle_server_stream(req).await,
        "/client-stream" => handle_client_stream(req).await,
        "/bidi" => handle_bidi(req).await,
        _ => {
            let resp = Response::builder()
                .status(404)
                .body(full_body(Bytes::from("Not Found\n")))?;
            Ok(resp)
        }
    }
}

async fn handle_small(
    req: Request<hyper::body::Incoming>,
) -> Result<Response<BoxBody>, anyhow::Error> {
    let body = req.collect().await?.to_bytes();
    Ok(Response::builder()
        .status(200)
        .body(full_body(body))?)
}

async fn handle_large(
    req: Request<hyper::body::Incoming>,
) -> Result<Response<BoxBody>, anyhow::Error> {
    let body = req.collect().await?.to_bytes();
    let size = body.len();
    let data = vec![0xABu8; size];
    Ok(Response::builder()
        .status(200)
        .body(full_body(Bytes::from(data)))?)
}

async fn handle_server_stream(
    req: Request<hyper::body::Incoming>,
) -> Result<Response<BoxBody>, anyhow::Error> {
    // Read request body to get params: "chunks:chunk_size"
    let body = req.collect().await?.to_bytes();
    let params = String::from_utf8_lossy(&body);
    let parts: Vec<&str> = params.split(':').collect();
    let chunks: usize = parts.first().and_then(|s| s.parse().ok()).unwrap_or(100);
    let chunk_size: usize = parts.get(1).and_then(|s| s.parse().ok()).unwrap_or(10240);

    let (tx, rx) = mpsc::channel::<Bytes>(32);

    tokio::spawn(async move {
        let chunk_data = Bytes::from(vec![0xABu8; chunk_size]);
        for _ in 0..chunks {
            if tx.send(chunk_data.clone()).await.is_err() {
                break;
            }
        }
    });

    Ok(Response::builder()
        .status(200)
        .body(stream_body(rx))?)
}

async fn handle_client_stream(
    req: Request<hyper::body::Incoming>,
) -> Result<Response<BoxBody>, anyhow::Error> {
    let mut body = req.into_body();
    let mut total_bytes: usize = 0;
    let mut total_chunks: usize = 0;

    while let Some(frame) = body.frame().await {
        let frame = frame?;
        if let Some(data) = frame.data_ref() {
            total_bytes += data.len();
            total_chunks += 1;
        }
    }

    let resp = format!("{}:{}", total_bytes, total_chunks);
    Ok(Response::builder()
        .status(200)
        .body(full_body(Bytes::from(resp)))?)
}

async fn handle_bidi(
    req: Request<hyper::body::Incoming>,
) -> Result<Response<BoxBody>, anyhow::Error> {
    let mut body = req.into_body();
    let (tx, rx) = mpsc::channel::<Bytes>(32);

    // Read incoming chunks and echo them back via the response stream
    tokio::spawn(async move {
        while let Some(frame) = body.frame().await {
            match frame {
                Ok(frame) => {
                    if let Some(data) = frame.data_ref() {
                        if tx.send(data.clone()).await.is_err() {
                            break;
                        }
                    }
                }
                Err(_) => break,
            }
        }
    });

    Ok(Response::builder()
        .status(200)
        .body(stream_body(rx))?)
}
