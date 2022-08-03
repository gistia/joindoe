use clap::{AppSettings, Parser, Subcommand, ValueEnum};
use colored::Colorize;

mod collect;
mod config;
mod db;
mod loader;
mod postprocess;
mod transform;
mod transformer;

#[derive(ValueEnum, Clone, Debug, PartialEq)]
pub enum Stage {
    Collect,
    Transform,
    Load,
    Postprocess,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// List all available transformers
    Transformers {},
}

#[derive(Parser, Debug)]
#[clap(author, version, about)]
#[clap(global_setting(AppSettings::SubcommandsNegateReqs))]
struct Args {
    /// Optional subcommand
    #[clap(subcommand)]
    command: Option<Commands>,

    /// Configuration file
    #[clap(short, long, value_name = "FILE", required = true)]
    config: Option<String>,

    /// Skip the collection stage
    #[clap(long)]
    skip_collect: bool,

    /// Skip the transformation stage
    #[clap(long)]
    skip_transform: bool,

    /// Skip loading stage
    #[clap(long)]
    skip_load: bool,

    /// Skip post-processing
    #[clap(long)]
    skip_postprocess: bool,

    /// Only runs specific stage(s)
    #[clap(long, value_enum)]
    only: Option<Vec<Stage>>,

    /// Show debugging information
    #[clap(short, long)]
    debug: bool,
}

#[tokio::main]
async fn main() {
    dotenv::dotenv().ok();

    let args = Args::parse();

    let log_level = if args.debug { "joindoe=debug" } else { "info" };

    env_logger::init_from_env(
        env_logger::Env::default().filter_or(env_logger::DEFAULT_FILTER_ENV, log_level),
    );

    match &args.command {
        Some(Commands::Transformers {}) => {
            let transformers = transformer::transformers_info();
            println!("{}", "TRANSFORMERS:".yellow().bold());
            for transformer in transformers {
                println!("    {: <20}{}", transformer.0.green(), transformer.1);
            }
            return;
        }
        None => {}
    }

    let config = args.config.unwrap();
    let config = config::Config::new(&config);
    if !args.skip_collect && allow_run(&args.only, Stage::Collect) {
        match collect::collect(&config).await {
            Ok(_) => log::info!("Collection phase completed"),
            Err(e) => {
                log::error!("Collection phase failed: {}", e);
                std::process::exit(1);
            }
        };
    }
    if !args.skip_transform && allow_run(&args.only, Stage::Transform) {
        match transform::transform(&config).await {
            Ok(_) => log::info!("Transformation phase completed"),
            Err(e) => {
                log::error!("Transformation phase failed: {}", e);
                std::process::exit(1);
            }
        }
    }
    if !args.skip_load && allow_run(&args.only, Stage::Load) {
        match loader::load(&config).await {
            Ok(_) => log::info!("Load phase completed"),
            Err(e) => {
                log::error!("Load phase failed: {}", e);
                std::process::exit(1);
            }
        }
    }
    if !args.skip_postprocess && allow_run(&args.only, Stage::Postprocess) {
        match postprocess::run(&config).await {
            Ok(_) => log::info!("Post-processing completed"),
            Err(e) => {
                log::error!("Post-processing failed: {}", e);
                std::process::exit(1);
            }
        }
    }
}

fn allow_run(only: &Option<Vec<Stage>>, stage: Stage) -> bool {
    if let Some(v) = only {
        v.contains(&stage)
    } else {
        true
    }
}
