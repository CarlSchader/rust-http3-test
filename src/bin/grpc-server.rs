// src/bin/grpc-server.rs
use std::net::SocketAddr;
use std::pin::Pin;

use clap::Parser;
use tokio::sync::mpsc;
use tokio_stream::{wrappers::ReceiverStream, Stream, StreamExt};
use tonic::{transport::Server, Request, Response, Status, Streaming};

pub mod bench {
    tonic::include_proto!("hello");
}

use bench::bench_server::{Bench, BenchServer};
use bench::{Payload, StreamRequest, StreamResponse};

#[derive(Parser, Debug)]
#[command(name = "grpc-server", about = "gRPC benchmark server")]
struct Args {
    /// Port to listen on
    #[arg(short, long, default_value = "50051")]
    port: u16,
}

#[derive(Debug, Default)]
pub struct BenchService {}

#[tonic::async_trait]
impl Bench for BenchService {
    async fn small_payload(
        &self,
        request: Request<Payload>,
    ) -> Result<Response<Payload>, Status> {
        Ok(Response::new(Payload {
            data: request.into_inner().data,
        }))
    }

    async fn large_payload(
        &self,
        request: Request<Payload>,
    ) -> Result<Response<Payload>, Status> {
        // Echo back same-sized payload
        let size = request.into_inner().data.len();
        let data = vec![0xABu8; size];
        Ok(Response::new(Payload { data }))
    }

    type ServerStreamStream =
        Pin<Box<dyn Stream<Item = Result<Payload, Status>> + Send + 'static>>;

    async fn server_stream(
        &self,
        request: Request<StreamRequest>,
    ) -> Result<Response<Self::ServerStreamStream>, Status> {
        let req = request.into_inner();
        let chunks = req.chunks as usize;
        let chunk_size = req.chunk_size as usize;

        let (tx, rx) = mpsc::channel(32);

        tokio::spawn(async move {
            let chunk_data = vec![0xABu8; chunk_size];
            for _ in 0..chunks {
                if tx
                    .send(Ok(Payload {
                        data: chunk_data.clone(),
                    }))
                    .await
                    .is_err()
                {
                    break;
                }
            }
        });

        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    async fn client_stream(
        &self,
        request: Request<Streaming<Payload>>,
    ) -> Result<Response<StreamResponse>, Status> {
        let mut stream = request.into_inner();
        let mut total_bytes: i64 = 0;
        let mut total_chunks: i32 = 0;

        while let Some(payload) = stream.next().await {
            let payload = payload?;
            total_bytes += payload.data.len() as i64;
            total_chunks += 1;
        }

        Ok(Response::new(StreamResponse {
            total_bytes,
            total_chunks,
        }))
    }

    type BidiStreamStream =
        Pin<Box<dyn Stream<Item = Result<Payload, Status>> + Send + 'static>>;

    async fn bidi_stream(
        &self,
        request: Request<Streaming<Payload>>,
    ) -> Result<Response<Self::BidiStreamStream>, Status> {
        let mut stream = request.into_inner();
        let (tx, rx) = mpsc::channel(32);

        tokio::spawn(async move {
            while let Some(Ok(payload)) = stream.next().await {
                // Echo each message back
                if tx.send(Ok(payload)).await.is_err() {
                    break;
                }
            }
        });

        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
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

    let cert_pem = cert.pem();
    let key_pem = signing_key.serialize_pem();

    let tls_config = tonic::transport::ServerTlsConfig::new()
        .identity(tonic::transport::Identity::from_pem(&cert_pem, &key_pem));

    let addr: SocketAddr = format!("0.0.0.0:{}", args.port).parse()?;
    let service = BenchService::default();

    println!("gRPC benchmark server listening on {}", addr);

    Server::builder()
        .tls_config(tls_config)?
        .add_service(BenchServer::new(service))
        .serve(addr)
        .await?;

    Ok(())
}
