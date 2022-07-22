use crate::config::Config;
use crate::db;
use s3::creds::Credentials;
use s3::{bucket::Bucket, serde_types::Object};
use serde_yaml::{Mapping, Value};
use std::io::BufReader;
use std::time::Instant;
use tempfile::NamedTempFile;
use tokio_postgres::Error;
use unicode_segmentation::UnicodeSegmentation;

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
        let table = &table_obj.name;
        let columns = db::Db::new(&config.source.connection_uri)
            .await
            .columns(table)
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

        let transform = table_obj.transform.clone();

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
    transformations: &Mapping,
    data: Vec<&str>,
    columns: Vec<String>,
) -> Vec<String> {
    let mut trdata = vec![];
    for (i, column) in columns.iter().enumerate() {
        let value = data[i];
        let transform = transformations
            .get(&Value::String(column.to_owned()))
            .unwrap()
            .as_str()
            .unwrap();
        println!("{} {}", transform, value);

        let tvalue = match transform {
            "reverse" => value.graphemes(true).rev().collect::<String>(),
            "first-name" => "First".to_owned(),
            "last-name" => "Last".to_owned(),
            t => unimplemented!(
                "Unimplemented transformation of type {} for column {}",
                t,
                column
            ),
        };

        trdata.push(tvalue.clone());
    }

    trdata
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_yaml::Value;

    fn make_transformations(maps: Vec<(&str, &str)>) -> Mapping {
        let mut m = Mapping::new();

        for (k, v) in maps {
            m.insert(Value::String(k.to_string()), Value::String(v.to_string()));
        }

        m
    }

    #[test]
    fn test_apply_transformations() {
        let transformations = make_transformations(vec![
            ("first", "first-name"),
            ("last", "last-name"),
            ("identifier", "reverse"),
        ]);
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
        assert_eq!(data, vec!["9673464811", "Moore", "Martin"]);
    }
}
