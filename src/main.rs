use std::{env, fs::File, io::Write};

use async_graphql::{
    extensions::Logger, http::GraphiQLSource, EmptyMutation, EmptySubscription, SDLExportOptions,
    Schema,
};
use async_graphql_axum::{GraphQLRequest, GraphQLResponse};

use axum::{
    extract::State,
    http::StatusCode,
    response::{self, IntoResponse},
    routing::{get, post},
    Router,
};
use clap::{arg, command, Parser};

use log::info;
use mongodb::{options::ClientOptions, Client, Database};

use once_cell::sync::Lazy;
use axum_otel_metrics::HttpMetricsLayerBuilder;
use axum_otel_metrics::HttpMetricsLayer;

use opentelemetry::global;
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider, Temporality};
use opentelemetry_sdk::Resource;
use opentelemetry_otlp::WithExportConfig;

mod event;
mod graphql;

use event::http_event_service::{
    list_topic_subscriptions, on_discount_order_validation_succeeded_event,
    on_user_address_archived_event, on_user_address_creation_event, on_user_created_event,
    on_vendor_address_created_event, HttpEventServiceState,
};
use graphql::model::{
    foreign_types::{User, VendorAddress},
    invoice::Invoice,
};
use graphql::query::Query;

/// Builds the GraphiQL frontend.
async fn graphiql() -> impl IntoResponse {
    response::Html(GraphiQLSource::build().endpoint("/").finish())
}

/// Establishes database connection and returns the client.
async fn db_connection() -> Client {
    let uri = match env::var_os("MONGODB_URI") {
        Some(uri) => uri.into_string().unwrap(),
        None => panic!("$MONGODB_URI is not set."),
    };

    // Parse a connection string into an options struct.
    let mut client_options = ClientOptions::parse(uri).await.unwrap();

    // Manually set an option.
    client_options.app_name = Some("Invoice".to_string());

    // Get a handle to the deployment.
    Client::with_options(client_options).unwrap()
}

/// Returns Router that establishes connection to Dapr.
///
/// Adds endpoints to define pub/sub interaction with Dapr.
async fn build_dapr_router(db_client: Database) -> Router {
    let invoice_collection: mongodb::Collection<Invoice> =
        db_client.collection::<Invoice>("invoices");
    let vendor_address_collection: mongodb::Collection<VendorAddress> =
        db_client.collection::<VendorAddress>("vendor_address");
    let user_collection: mongodb::Collection<User> = db_client.collection::<User>("user");

    // Define routes.
    let app = Router::new()
        .route("/dapr/subscribe", get(list_topic_subscriptions))
        .route(
            "/on-discount-validation-succeded",
            post(on_discount_order_validation_succeeded_event),
        )
        .route(
            "/on-vendor-address-creation-event",
            post(on_vendor_address_created_event),
        )
        .route("/on-user-creation-event", post(on_user_created_event))
        .route(
            "/on-user-address-creation-event",
            post(on_user_address_creation_event),
        )
        .route(
            "/on-user-address-archived-event",
            post(on_user_address_archived_event),
        )
        .with_state(HttpEventServiceState {
            invoice_collection,
            vendor_address_collection,
            user_collection,
        });
    app
}

/// Command line argument to toggle schema generation instead of service execution.
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Generates GraphQL schema in `./schemas/invoice.graphql`.
    #[arg(long)]
    generate_schema: bool,
}

/// Activates logger and parses argument for optional schema generation. Otherwise starts gRPC and GraphQL server.
#[tokio::main]
async fn main() -> std::io::Result<()> {
    env_logger::init();
    info!("Invoice service starting");

    let args = Args::parse();
    if args.generate_schema {
        let schema = Schema::build(Query, EmptyMutation, EmptySubscription).finish();
        let mut file = File::create("./schemas/invoice.graphql")?;
        let sdl_export_options = SDLExportOptions::new().federation();
        let schema_sdl = schema.sdl_with_options(sdl_export_options);
        file.write_all(schema_sdl.as_bytes())?;
        info!("GraphQL schema: ./schemas/invoice.graphql was successfully generated!");
    } else {
        start_service().await;
    }
    Ok(())
}

/// Describes the handler for GraphQL requests.
///
/// Executes the GraphQL schema with the request.
async fn graphql_handler(
    State(schema): State<Schema<Query, EmptyMutation, EmptySubscription>>,
    req: GraphQLRequest,
) -> GraphQLResponse {
    let req = req.into_inner();
    schema.execute(req).await.into()
}

static RESOURCE: Lazy<Resource> = Lazy::new(|| {
    Resource::builder()
        .with_service_name("invoice")
        .build()
});

/// Initializes OpenTelemetry metrics exporter and sets the global meter provider.
fn init_otlp() -> HttpMetricsLayer {
    let exporter = opentelemetry_otlp::MetricExporter::builder()
        .with_http()
        .with_endpoint("http://otel-collector:4318/v1/metrics")
        .with_temporality(Temporality::default())
        .build()
        .unwrap();

    let reader = PeriodicReader::builder(exporter)
        .with_interval(std::time::Duration::from_secs(5))
        .build();

    let provider = SdkMeterProvider::builder()
        .with_reader(reader)
        .with_resource(RESOURCE.clone())
        .build();

    global::set_meter_provider(provider.clone());

    HttpMetricsLayerBuilder::new()
        .with_provider(provider.clone())
        .build()
}

/// Starts invoice service on port 8000.
async fn start_service() {
    let client = db_connection().await;
    let db_client: Database = client.database("invoice-database");

    let schema = Schema::build(Query, EmptyMutation, EmptySubscription)
        .extension(Logger)
        .data(db_client.clone())
        .enable_federation()
        .finish();

    let graphiql = Router::new()
        .route("/", get(graphiql).post(graphql_handler))
        .route("/health", get(StatusCode::OK))
        .with_state(schema);
    let dapr_router = build_dapr_router(db_client).await;
    let metrics = init_otlp();

    let app = Router::new()
        .merge(graphiql)
        .merge(dapr_router)
        .layer(metrics);

    info!("GraphiQL IDE: http://0.0.0.0:8080");

    let listener = tokio::net::TcpListener::bind("0.0.0.0:8080").await.unwrap();
    axum::serve(listener, app)
        .await
        .unwrap();
}
