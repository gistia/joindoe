use clap::Parser;

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
    config: String,

    // Skips the collection stage
    #[clap(long)]
    skip_collect: bool,

    // Skips the transformation stage
    #[clap(long)]
    skip_transform: bool,
}

#[tokio::main]
async fn main() -> () {
    dotenv::dotenv().ok();
    env_logger::init_from_env(
        env_logger::Env::default().filter_or(env_logger::DEFAULT_FILTER_ENV, "info"),
    );

    let args = Args::parse();

    let config = config::Config::new(&args.config);
    if !args.skip_collect {
        match collect::collect(&config).await {
            Ok(_) => log::debug!("Collection phase completed"),
            Err(e) => {
                log::error!("Collection phase failed: {}", e);
                std::process::exit(1);
            }
        };
    }
    if !args.skip_transform {
        match transform::transform(&config).await {
            Ok(_) => log::debug!("Transformation phase completed"),
            Err(e) => {
                log::error!("Transformation phase failed: {}", e);
                std::process::exit(1);
            }
        }
    }
    loader::load(&config).await;
}
