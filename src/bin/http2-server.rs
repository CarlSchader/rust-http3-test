// src/bin/http2-server.rs
use std::net::SocketAddr;
use std::sync::Arc;

use bytes::Bytes;
use clap::Parser;
use http_body_util::Full;
use hyper::server::conn::http2;
use hyper::service::service_fn;
use hyper::{Request, Response};
use hyper_util::rt::{TokioExecutor, TokioIo};
use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;

#[derive(Parser, Debug)]
#[command(name = "http2-server", about = "HTTP/2 server")]
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

    // Generate self-signed cert
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
    println!("Listening on {}", addr);

    loop {
        let (stream, peer_addr) = listener.accept().await?;
        let tls_acceptor = tls_acceptor.clone();

        tokio::spawn(async move {
            if let Err(e) = handle_connection(stream, tls_acceptor, peer_addr).await {
                eprintln!("Connection error: {}", e);
            }
        });
    }
}

async fn handle_connection(
    stream: tokio::net::TcpStream,
    tls_acceptor: TlsAcceptor,
    _peer_addr: SocketAddr,
) -> anyhow::Result<()> {
    let tls_stream = tls_acceptor.accept(stream).await?;

    http2::Builder::new(TokioExecutor::new())
        .serve_connection(TokioIo::new(tls_stream), service_fn(handle_request))
        .await?;

    Ok(())
}

async fn handle_request(
    req: Request<hyper::body::Incoming>,
) -> Result<Response<Full<Bytes>>, hyper::Error> {
    println!("{} {}", req.method(), req.uri());

    let response = Response::builder()
        .status(200)
        .header("content-type", "text/plain")
        .body(Full::new(Bytes::from("Hello from HTTP/2!\n")))
        .unwrap();

    Ok(response)
}
