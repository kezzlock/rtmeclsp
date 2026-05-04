mod api;
mod config;
mod domain;
mod infra;
mod state;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    tracing::info!("rtmeclsp starting");
}
