use std::env;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;

use tonic::{transport::Server, Request, Response, Status};

use tonic::Streaming;
use wolfey_metrics::{
    wolfey_metrics_server::{WolfeyMetrics, WolfeyMetricsServer},
    AggChartReply, AggChartRequest, ImportReply, ImportRequest,
};
use wolfey_metrics::{NonAggChartReply, NonAggChartRequest};
use wolfeystorage::{Storage, StorageConfig};

mod wolfey_metrics {
    tonic::include_proto!("wolfeymetrics"); // The string specified here must match the proto package name
}
mod wolfeystorage;

struct WolfeyMetricsService {
    storage: Arc<Storage>,
}

#[tonic::async_trait]
impl WolfeyMetrics for WolfeyMetricsService {
    async fn import(
        &self,
        request: Request<Streaming<ImportRequest>>, // Accept request of type HelloRequest
    ) -> Result<Response<ImportReply>, Status> {
        // Return an instance of type HelloReply
        println!("Got a request: {:?}", request);
        let req = request.into_inner();

        let reply = ImportReply {
            message: format!("got req {req:?}!"), // We must use .into_inner() as the fields of gRPC requests and responses are private
        };

        Ok(Response::new(reply)) // Send back our formatted greeting
    }

    async fn non_agg_chart(
        &self,
        request: Request<NonAggChartRequest>,
    ) -> Result<Response<NonAggChartReply>, Status> {
        todo!()
    }

    async fn agg_chart(
        &self,
        request: Request<AggChartRequest>,
    ) -> Result<Response<AggChartReply>, Status> {
        todo!()
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let port = match env::var("PORT") {
        Ok(x) => x.parse()?,
        Err(_) => 50051,
    };
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), port);

    let config = envy::from_env::<StorageConfig>()?;
    let storage = Storage::new(config);
    let service = WolfeyMetricsService {
        storage: storage.into(),
    };
    let server = WolfeyMetricsServer::new(service);

    Server::builder().add_service(server).serve(addr).await?;

    Ok(())
}
