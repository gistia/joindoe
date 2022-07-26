use std::collections::HashMap;

use crate::config::{Config, TaskType};
use crate::db::Db;
use handlebars::Handlebars;
use printpdf::*;
use s3::bucket::Bucket;
use s3::creds::Credentials;
use std::fs::File;
use std::io::BufWriter;
use tempfile::NamedTempFile;
use tokio_postgres::Error;

pub async fn run(config: &Config) -> Result<(), Error> {
    if let Some(tasks) = &config.postprocess {
        for task_def in tasks {
            log::info!("Running postprocess task - {}", task_def.name);
            log::debug!("{:#?}", task_def.task);

            match &task_def.task {
                TaskType::Sql(sql_config) => {
                    let db = Db::new(&sql_config.connection_uri).await;
                    log::debug!("Running SQL: {}", sql_config.sql);
                    let results = db.exec(&sql_config.sql).await.unwrap();
                    log::debug!("{} affected records", results);
                }
                TaskType::Pdf(pdf_config) => {
                    let credentials = Credentials::new(
                        Some(&pdf_config.aws_access_key_id.clone()),
                        Some(&pdf_config.aws_secret_access_key.clone()),
                        None,
                        None,
                        None,
                    )
                    .unwrap();

                    let region = "us-east-1".parse().unwrap();
                    let bucket = Bucket::new(&pdf_config.bucket, region, credentials).unwrap();
                    let results = bucket.list("".to_string(), None).await.unwrap();
                    let contents = results
                        .iter()
                        .map(|r| r.contents.clone())
                        .flatten()
                        .collect::<Vec<_>>();
                    let existing_pdfs = contents.iter().map(|o| o.key.clone()).collect::<Vec<_>>();

                    let client = Db::new(&config.destination.connection_uri).await;
                    let mut handlebars = Handlebars::new();
                    handlebars
                        .register_template_string("file_name", &pdf_config.file_name)
                        .unwrap();
                    handlebars
                        .register_template_string("contents", &pdf_config.contents)
                        .unwrap();

                    let rows = client.query(&pdf_config.from).await?;
                    let mut i = 0;
                    let count = rows.len();
                    log::info!("Generating and uploading {} PDFs...", count);
                    for row in rows {
                        let mut map = HashMap::new();
                        i += 1;

                        for col in row.columns() {
                            let value: String = row.get(col.name());
                            map.insert(col.name(), value);
                        }

                        let file_name = handlebars.render("file_name", &map).unwrap();
                        if existing_pdfs.contains(&file_name) {
                            log::debug!("Skipping PDF {}/{} - {}...", i, count, file_name);
                            continue;
                        }

                        log::trace!("Generating PDF - {} with {:#?}", file_name, &map);

                        let (doc, page1, layer1) =
                            PdfDocument::new("Something", Mm(216.0), Mm(280.0), "Layer 1");
                        let current_layer = doc.get_page(page1).get_layer(layer1);

                        let text = handlebars.render("contents", &map).unwrap();
                        let font = doc
                            .add_external_font(File::open(&pdf_config.font).unwrap())
                            .unwrap();
                        current_layer.use_text(text, 12.0, Mm(10.0), Mm(270.0), &font);

                        let file = NamedTempFile::new().unwrap();
                        let path = file.path();
                        doc.save(&mut BufWriter::new(File::create(&path).unwrap()))
                            .unwrap();

                        log::debug!("Uploading PDF {}/{} - {}...", i, count, file_name);
                        bucket
                            .put_object(&file_name, &std::fs::read(path).unwrap())
                            .await
                            .unwrap();
                    }
                }
            }
        }
    }

    Ok(())
}
