use std::net::SocketAddr;
use std::sync::Arc;

use bytes::Bytes;
use clap::Parser;
use quinn::{Endpoint, ServerConfig};
use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};

#[derive(Parser, Debug)]
#[command(name = "h3-server", about = "HTTP/3 server")]
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

    // Generate self-signed cert
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
    println!("Listening on {}", addr);

    while let Some(conn) = endpoint.accept().await {
        tokio::spawn(handle_connection(conn.await?));
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
                let (req, mut stream) = resolver.resolve_request().await?;  // fixed
                tokio::spawn(async move {
                    println!("{} {}", req.method(), req.uri());

                    let response = http::Response::builder()
                        .status(200)
                        .header("content-type", "text/plain")
                        .body(())?;

                    stream.send_response(response).await?;
                    stream.send_data(Bytes::from("Hello from HTTP/3!\n")).await?;
                    stream.finish().await?;
                    Ok::<_, anyhow::Error>(())
                });
            }
            None => break,
        }
    }
    Ok(())
}
