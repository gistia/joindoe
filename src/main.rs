use clap::Parser;
use config::Config;
use parquet::column::writer::ColumnWriter;
use parquet::data_type::{Int96, Int96Type};
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::file::{properties::WriterProperties, writer::SerializedFileWriter};
use parquet::record::Field;
use s3::creds::Credentials;
use s3::{bucket::Bucket, serde_types::Object};
use std::env;
use std::fs;
use std::fs::File;
use std::result::Result;
use std::sync::Arc;
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
    // let _result = collect(&config).await;
    let _transform = transform(&config).await;

    Ok(())
}

/// Converts this INT96 into an i64 representing the number of MILLISECONDS since Epoch
pub fn from_i64(milliseconds: i64) -> Int96 {
    const JULIAN_DAY_OF_EPOCH: i64 = 2_440_588;
    const SECONDS_PER_DAY: i64 = 86_400;
    const MILLIS_PER_SECOND: i64 = 1_000;

    let day = milliseconds / MILLIS_PER_SECOND / SECONDS_PER_DAY + JULIAN_DAY_OF_EPOCH;
    let nanoseconds = (milliseconds % MILLIS_PER_SECOND) * 1_000_000;

    let mut int96 = Int96::new();
    int96.set_data(nanoseconds as u32, (nanoseconds >> 32) as u32, day as u32);
    println!("value: {}", milliseconds);
    println!("int96: {}", int96.to_i64());
    int96
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

            let file_name = result.key.split("/").collect::<Vec<&str>>()[1];
            fs::write(file_name, res.bytes()).unwrap();

            let in_file = File::open(file_name).unwrap();
            let reader = SerializedFileReader::new(in_file).unwrap();
            let metadata = reader.metadata();
            println!("metadata = {:#?}", metadata);

            let out_file_name = format!("out_{}", file_name);
            let out_file = File::create(out_file_name).unwrap();
            let props = Arc::new(WriterProperties::builder().build());
            let schema = Arc::new(metadata.file_metadata().schema().clone());
            let mut writer = SerializedFileWriter::new(out_file, schema, props).unwrap();

            for row in reader.into_iter() {
                let mut row_group_writer = writer.next_row_group().unwrap();
                for col in row.get_column_iter() {
                    if let Some(mut col_writer) = row_group_writer.next_column().unwrap() {
                        match col_writer.untyped() {
                            ColumnWriter::BoolColumnWriter(ref mut cw) => {
                                if let Field::Bool(b) = col.1 {
                                    cw.write_batch(&[b.to_owned()], None, None).unwrap();
                                }
                            }
                            ColumnWriter::Int32ColumnWriter(ref mut cw) => {
                                if let Field::Int(i) = col.1 {
                                    cw.write_batch(&[i.to_owned()], None, None).unwrap();
                                }
                            }
                            ColumnWriter::Int64ColumnWriter(ref mut cw) => {
                                if let Field::Int(i) = col.1 {
                                    cw.write_batch(&[i.to_owned().try_into().unwrap()], None, None)
                                        .unwrap();
                                }
                            }
                            ColumnWriter::Int96ColumnWriter(ref mut cw) => {
                                if let Field::TimestampMillis(i) = col.1 {
                                    cw.write_batch(&[from_i64(i.clone() as i64)], None, None)
                                        .unwrap();
                                }
                            }
                            ColumnWriter::FloatColumnWriter(ref mut cw) => {
                                if let Field::Float(f) = col.1 {
                                    cw.write_batch(&[f.to_owned()], None, None).unwrap();
                                }
                            }
                            ColumnWriter::DoubleColumnWriter(ref mut cw) => {
                                if let Field::Double(f) = col.1 {
                                    cw.write_batch(&[f.to_owned()], None, None).unwrap();
                                }
                            }
                            ColumnWriter::ByteArrayColumnWriter(ref mut cw) => {
                                if let Field::Bytes(b) = col.1 {
                                    cw.write_batch(&[b.to_owned()], None, None).unwrap();
                                }
                            }
                            ColumnWriter::FixedLenByteArrayColumnWriter(ref mut cw) => {
                                println!("FixedLenByteArray col = {:#?}", col);

                                // if let Field::FixedLenBytes(b) = col.1 {
                                //     cw.write_batch(&[b.to_owned()], None, None).unwrap();
                                // }
                            }
                            _ => {
                                log::error!("unhandled column type for {}:\n{:#?}", col.0, col.1);
                            }
                        }
                        col_writer.close().unwrap();
                    }
                }
                row_group_writer.close().unwrap();
            }
            writer.close().unwrap();
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
