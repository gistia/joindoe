use std::collections::HashMap;

use crate::config::{Config, TaskType};
use crate::db::Db;
use handlebars::Handlebars;
use tokio_postgres::Error;

pub async fn run(config: &Config) -> Result<(), Error> {
    if let Some(tasks) = &config.postprocess {
        for task_def in tasks {
            log::info!("Running postprocess task - {}", task_def.name);
            log::debug!("{:#?}", task_def.task);

            match &task_def.task {
                TaskType::Pdf(pdf_config) => {
                    let client = Db::new(&config.destination.connection_uri).await;
                    let mut handlebars = Handlebars::new();
                    handlebars
                        .register_template_string("file_name", &pdf_config.file_name)
                        .unwrap();

                    let rows = client.query(&pdf_config.from).await?;
                    for row in rows {
                        let mut map = HashMap::new();

                        for col in row.columns() {
                            let value: String = row.get(col.name());
                            map.insert(col.name(), value);
                        }

                        let file_name = handlebars.render("file_name", &map).unwrap();
                        println!("{}", file_name);
                    }
                }
            }
        }
    }

    Ok(())
}
