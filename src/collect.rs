use crate::config::Config;
use crate::db::Db;
use std::time::Instant;
use tokio_postgres::Error;

pub async fn collect(config: &Config) -> Result<(), Error> {
    let source = &config.source;
    let db = Db::new(&source.connection_uri).await;
    log::debug!(
        "Connecting to source: postgres://*****@{}",
        db.sanitized_uri()
    );

    println!("source = {:#?}", source);
    for table_obj in &source.tables {
        let table = &table_obj.name;
        log::debug!("Started processing table {}", table);

        let count = db.count(table).await.unwrap();

        log::debug!("Processing table {} with {} rows", table, count);

        let now = Instant::now();
        db.unload(&table, &config.store.bucket).await.unwrap();
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
