use clap::Parser;

mod collect;
mod config;
mod db;
mod loader;
mod transform;
mod transformer;

#[derive(Parser, Debug)]
#[clap(author = "Felipe Coury <felipe.coury@gmail.com>", version, about)]
struct Args {
    /// Configuration file
    #[clap(short, long)]
    config: String,

    /// Skip the collection stage
    #[clap(long)]
    skip_collect: bool,

    /// Skip the transformation stage
    #[clap(long)]
    skip_transform: bool,

    /// Show debugging information
    #[clap(short, long)]
    debug: bool,
}

#[tokio::main]
async fn main() -> () {
    dotenv::dotenv().ok();

    let args = Args::parse();

    let log_level = if args.debug { "joindoe=debug" } else { "info" };

    env_logger::init_from_env(
        env_logger::Env::default().filter_or(env_logger::DEFAULT_FILTER_ENV, log_level),
    );

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
