use std::env;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;

use tokio::runtime::Handle;
use tonic::{transport::Server, Request, Response, Status};

use crunchert::{
    crunchert_server::{Crunchert, CrunchertServer},
    AggChartReplyProto, AggChartRequestProto, ImportReplyProto, ImportRequestProto,
    NonAggChartReplyProto, NonAggChartRequestProto,
};
use tonic::Streaming;

use crunchert_storage::{Storage, StorageConfig};

mod crunchert {
    tonic::include_proto!("crunchert"); // The string specified here must match the proto package name
}

#[derive(Clone)]
struct CrunchertService {
    storage: Arc<Storage>,
}

#[tonic::async_trait]
impl Crunchert for CrunchertService {
    async fn import(
        &self,
        request: Request<ImportRequestProto>, // Accept request of type HelloRequest
    ) -> Result<Response<ImportReplyProto>, Status> {
        // Return an instance of type HelloReply
        println!("Got a request: {:?}", request);
        let req = request.into_inner();

        let reply = ImportReplyProto {
            message: format!("got req {req:?}!"), // We must use .into_inner() as the fields of gRPC requests and responses are private
        };

        Ok(Response::new(reply)) // Send back our formatted greeting
    }

    async fn non_agg_chart(
        &self,
        request: Request<NonAggChartRequestProto>,
    ) -> Result<Response<NonAggChartReplyProto>, Status> {
        todo!()
    }

    async fn agg_chart(
        &self,
        request: Request<AggChartRequestProto>,
    ) -> Result<Response<AggChartReplyProto>, Status> {
        todo!()
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let metrics = Handle::current().metrics();

    let num_threads = metrics.num_workers();
    let port = match env::var("PORT") {
        Ok(x) => x.parse()?,
        Err(_) => 50051,
    };
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), port);

    let config = envy::from_env::<StorageConfig>()?;
    let storage = Storage::new(config, num_threads)?;
    let service = CrunchertService {
        storage: storage.into(),
    };
    let server = CrunchertServer::new(service);

    Server::builder().add_service(server).serve(addr).await?;

    Ok(())
}
