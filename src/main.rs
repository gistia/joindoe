use clap::Parser;
use config::Config;
use s3::creds::Credentials;
use s3::{bucket::Bucket, serde_types::Object};
use std::env;
use std::io::BufReader;
use std::result::Result;
use std::time::Instant;
use tokio_postgres::{Error, NoTls};

mod config;

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
    let _result = collect(&config).await;
    let _transform = transform(&config).await;

    Ok(())
}

async fn transform(config: &Config) -> Result<(), Error> {
    let store = &config.store;
    let tables = &config.source.tables;

    let credentials = Credentials::new(
        Some(&store.aws_access_key_id.clone()),
        Some(&store.aws_secret_access_key.clone()),
        None,
        None,
        None,
    )
    .unwrap();

    let region = "us-east-1".parse().unwrap();
    let bucket = Bucket::new(&store.bucket, region, credentials).unwrap();

    for table in tables {
        let results = bucket
            .list(format!("in/{}_", table), Some("/".to_string()))
            .await
            .unwrap();

        if results.len() < 1 {
            log::info!("No records to process, exiting");
            return Ok(());
        }

        for result in results
            .into_iter()
            .flat_map(|r| r.contents)
            .collect::<Vec<Object>>()
        {
            let res = bucket.get_object(result.key.clone()).await.unwrap();
            let buf_reader = BufReader::new(res.bytes());
            let mut reader = csv::Reader::from_reader(buf_reader);
            for result in reader.records() {
                let record = result.unwrap();
                log::info!("{:?}", record);
            }
        }
    }

    Ok(())
}

async fn collect(config: &Config) -> Result<(), Error> {
    let source = &config.source;
    let (client, connection) = tokio_postgres::connect(&source.connection_uri, NoTls).await?;

    let sanitized_url = source.connection_uri.split("@").collect::<Vec<_>>()[1];
    log::debug!("Connecting to source: postgres://*****@{}", sanitized_url);

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    println!("source = {:#?}", source);
    for table in &source.tables {
        log::debug!("Started processing table {}", table);

        let count = client
            .query_one(&format!("SELECT COUNT(*) FROM {}", table), &[])
            .await?;
        let count: i64 = count.get(0);

        log::debug!("Processing table {} with {} rows", table, count);

        let fields = client
            .query(
                "SELECT column_name FROM information_schema.columns WHERE table_name = $1",
                &[&table],
            )
            .await?;
        let fields: Vec<String> = fields.iter().map(|row| row.get(0)).collect();

        let now = Instant::now();
        let sql = &format!(
            r#"
                UNLOAD ('SELECT {} FROM {}') TO 's3://{}/in/{}_'
                CREDENTIALS 'aws_access_key_id={};aws_secret_access_key={}'
                PARQUET ALLOWOVERWRITE PARALLEL OFF;
            "#,
            fields.join(", "),
            table,
            config.store.bucket,
            table,
            env::var("AWS_ACCESS_KEY_ID").unwrap(),
            env::var("AWS_SECRET_ACCESS_KEY").unwrap()
        );

        println!("sql = {}", sql);
        client.execute(sql, &[]).await?;
        let elapsed = now.elapsed();
        log::info!(
            "Finished {} in {}.{:02}s",
            table,
            elapsed.as_secs(),
            elapsed.subsec_micros()
        );
    }

    Ok(())
}

async fn _old_main() -> Result<(), Error> {
    dotenv::dotenv().ok();

    println!("DATABAE_URL: {}", env::var("DATABASE_URL").unwrap());

    let (client, connection) =
        tokio_postgres::connect(&env::var("DATABASE_URL").unwrap(), NoTls).await?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    let count: i64 = client
        .query_one("SELECT COUNT(*) FROM providers", &[])
        .await
        .unwrap()
        .get(0);

    println!("{} providers", count);

    let res = client
        .execute(
            &format!(
                r#"
                    UNLOAD ('SELECT * FROM providers') TO 's3://nw-data-transfer/providers_'
                    CREDENTIALS 'aws_access_key_id={};aws_secret_access_key={}'
                    DELIMITER AS ',' ALLOWOVERWRITE;
                "#,
                env::var("AWS_ACCESS_KEY_ID").unwrap(),
                env::var("AWS_SECRET_ACCESS_KEY").unwrap()
            ),
            &[],
        )
        .await?;

    println!("{:?}", res);

    Ok(())
}
