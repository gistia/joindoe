use crate::config::{Config, Transformation};
use crate::db;
use s3::creds::Credentials;
use s3::{bucket::Bucket, serde_types::Object};
use std::io::BufReader;
use std::time::Instant;
use tempfile::NamedTempFile;
use tokio_postgres::Error;

pub async fn transform(config: &Config) -> Result<(), Error> {
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

    for table_obj in tables {
        let transform_obj = &table_obj.transform;
        let table = &table_obj.name;

        let mut transform = &vec![];
        if let Some(v) = transform_obj {
            transform = v;
        }

        let columns = db::Db::new(&config.source.connection_uri)
            .await
            .columns(&table)
            .await?;
        let now = Instant::now();

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

            let file = NamedTempFile::new().unwrap();
            let path = file.path();
            let mut writer = csv::Writer::from_writer(file.reopen().unwrap());

            for result in reader.records() {
                let record = result.unwrap();
                let data = record.iter().collect::<Vec<&str>>();
                let res = apply_transformations(&transform, data, columns.clone());
                writer.write_record(res).unwrap();
            }

            writer.flush().unwrap();

            bucket
                .put_object(&format!("out/{}.csv", table), &std::fs::read(path).unwrap())
                .await
                .unwrap();
        }

        let elapsed = now.elapsed();
        log::info!(
            "Finished collecting {} in {}.{:02}s",
            table,
            elapsed.as_secs(),
            elapsed.subsec_micros()
        );
    }

    Ok(())
}

fn apply_transformations(
    transformations: &Vec<Transformation>,
    data: Vec<&str>,
    columns: Vec<String>,
) -> Vec<String> {
    let mut trdata = vec![];

    for (i, column) in columns.iter().enumerate() {
        let value = data[i];

        let transformation = transformations.iter().find(|t| t.column == *column);
        if transformation.is_none() {
            trdata.push(value.to_string());
            continue;
        }

        let transformation = transformation.unwrap();
        let transformer = transformation.transformer.transformer();
        let trvalue = transformer.transform(value);

        trdata.push(trvalue.clone());
    }

    trdata
}

#[cfg(test)]
mod tests {
    use crate::config::TransformerType;

    use super::*;

    #[test]
    fn test_apply_transformations() {
        let transformations = vec![Transformation {
            column: "identifier".to_string(),
            transformer: TransformerType::Reverse,
        }];
        println!("{:?}", transformations);
        let data = apply_transformations(
            &transformations,
            vec!["1184643769", "Martin", "Moore"],
            vec![
                "identifier".to_owned(),
                "first".to_owned(),
                "last".to_owned(),
            ],
        );
        assert_eq!(data, vec!["9673464811", "Martin", "Moore"]);
    }
}
