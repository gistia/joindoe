use clap::Parser;
use std::result::Result;
use tokio_postgres::Error;

mod collect;
mod config;
mod db;
mod loader;
mod transform;
mod transformer;

#[derive(Parser, Debug)]
#[clap(author, version, about)]
struct Args {
    // Configuration file
    #[clap(short, long)]
    config: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    dotenv::dotenv().ok();
    env_logger::init_from_env(
        env_logger::Env::default().filter_or(env_logger::DEFAULT_FILTER_ENV, "info"),
    );

    let args = Args::parse();

    let config = config::Config::new(&args.config.unwrap());
    let _result = collect::collect(&config).await;
    let _transform = transform::transform(&config).await;
    let _load = loader::load(&config).await;

    Ok(())
}
