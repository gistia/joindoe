use crate::config::{Config, Table};
use crate::db::Db;
use crate::transform::apply_transformations;
use s3::bucket::Bucket;
use s3::creds::Credentials;
use std::time::Instant;
use tempfile::NamedTempFile;
use tokio_postgres::Error;

pub async fn collect(config: &Config) -> Result<(), Error> {
    let source = &config.source;
    let db = Db::new(&source.connection_uri).await;
    log::debug!("Connecting to source database");

    for table_def in &source.tables {
        let table = &table_def.name;
        log::debug!("Started processing table {}", table);

        let now = Instant::now();
        if let Some(count) = &table_def.generate {
            log::debug!("Generating new table {} with {} rows", table, count);

            generate_csv(&config, &table_def, count).await;
        } else {
            let count = db.count(table).await.unwrap();
            log::debug!("Processing table {} with {} rows", table, count);

            if let Some(from) = &table_def.from {
                let db = Db::new(&config.destination.connection_uri).await;
                log::debug!("Connecting to target database");

                db.unload(&from, &config.store.bucket, table).await.unwrap();
            } else {
                db.unload_table(&table, &table_def.limit, &config.store.bucket)
                    .await
                    .unwrap();
            }
        }
        let elapsed = now.elapsed();

        log::info!(
            "Finished processing {} in {}.{:02}s",
            table,
            elapsed.as_secs(),
            elapsed.subsec_micros()
        );
    }

    Ok(())
}

async fn generate_csv(config: &Config, table_def: &Table, count: &usize) {
    let store = &config.store;

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

    let transform_obj = &table_def.transform;
    let table = &table_def.name;

    let mut transformations = &vec![];
    if let Some(v) = transform_obj {
        transformations = v;
    }

    let file = NamedTempFile::new().unwrap();
    let path = file.path();
    let mut writer = csv::Writer::from_writer(file.reopen().unwrap());
    // FIXME: better error handling when no transformation was given, it's required in this case
    if let Some(transform) = &table_def.transform {
        let columns: Vec<String> = transform.iter().map(|t| t.column.clone()).collect();

        writer.write_record(&columns).unwrap();

        for n in 0..*count {
            let len = columns.len();
            let data: Vec<&str> = vec![""; len];
            let row = apply_transformations(n, transformations, data, columns.clone());
            // println!("   {:?}", row);
            writer.write_record(row).unwrap();
        }

        writer.flush().unwrap();
        bucket
            .put_object(&format!("in/{}_000", table), &std::fs::read(path).unwrap())
            .await
            .unwrap();
    }
}
