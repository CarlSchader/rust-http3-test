// src/bin/grpc-server.rs
use std::net::SocketAddr;

use clap::Parser;
use tonic::{transport::Server, Request, Response, Status};

pub mod hello {
    tonic::include_proto!("hello");
}

use hello::greeter_server::{Greeter, GreeterServer};
use hello::{HelloRequest, HelloResponse};

#[derive(Parser, Debug)]
#[command(name = "grpc-server", about = "gRPC server")]
struct Args {
    /// Port to listen on
    #[arg(short, long, default_value = "50051")]
    port: u16,
}

#[derive(Debug, Default)]
pub struct MyGreeter {}

#[tonic::async_trait]
impl Greeter for MyGreeter {
    async fn say_hello(
        &self,
        request: Request<HelloRequest>,
    ) -> Result<Response<HelloResponse>, Status> {
        println!("Got request from {:?}", request.remote_addr());

        let reply = HelloResponse {
            message: format!("Hello from gRPC, {}!", request.into_inner().name),
        };

        Ok(Response::new(reply))
    }
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

    // Convert to PEM format for tonic
    let cert_pem = cert.pem();
    let key_pem = signing_key.serialize_pem();

    let tls_config = tonic::transport::ServerTlsConfig::new()
        .identity(tonic::transport::Identity::from_pem(&cert_pem, &key_pem));

    let addr: SocketAddr = format!("0.0.0.0:{}", args.port).parse()?;
    let greeter = MyGreeter::default();

    println!("Listening on {}", addr);

    Server::builder()
        .tls_config(tls_config)?
        .add_service(GreeterServer::new(greeter))
        .serve(addr)
        .await?;

    Ok(())
}
