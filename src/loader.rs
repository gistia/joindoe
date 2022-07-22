use crate::config::Config;
use crate::db::Db;
use std::time::Instant;

pub async fn load(config: &Config) {
    let src_def = &config.source;
    let source = Db::new(&src_def.connection_uri).await;
    let destination = Db::new(&config.destination.connection_uri).await;

    log::debug!(
        "Connected to destination: postgres://*****@{}",
        destination.sanitized_uri()
    );

    for table in &src_def.tables {
        let now = Instant::now();
        let columns = source.columns(&table.name).await.unwrap();

        let sql = format!(
            r#"
                COPY {}({}) FROM 's3://{}/out/{}.csv'
                CREDENTIALS 'aws_access_key_id={};aws_secret_access_key={}'
                CSV BLANKSASNULL;
            "#,
            table.name,
            columns.join(", "),
            config.store.bucket,
            table.name,
            config.store.aws_access_key_id,
            config.store.aws_secret_access_key,
        );

        destination
            .exec(format!("TRUNCATE TABLE {}", table.name).as_str())
            .await
            .unwrap();
        destination.exec(&sql).await.unwrap();

        let elapsed = now.elapsed();
        log::info!(
            "Finished loading {} in {}.{:02}s",
            table.name,
            elapsed.as_secs(),
            elapsed.subsec_micros()
        );
    }
}