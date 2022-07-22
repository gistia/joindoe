use std::env;
use tokio_postgres::{Client, Error, NoTls};

pub struct Db {
    pub uri: String,
    pub client: Client,
}

impl Db {
    pub async fn new(uri: &str) -> Self {
        let (client, connection) = tokio_postgres::connect(uri, NoTls).await.unwrap();

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });

        Db {
            uri: uri.to_owned(),
            client,
        }
    }

    pub fn sanitized_uri(&self) -> String {
        format!(
            "postgres://*****:*****@{}",
            self.uri.split("@").collect::<Vec<_>>()[1]
        )
    }

    pub async fn count(&self, table: &str) -> Result<i64, Error> {
        let count = self
            .client
            .query_one(&format!("SELECT COUNT(*) FROM {}", table), &[])
            .await?;
        Ok(count.get(0))
    }

    pub async fn columns(&self, table: &str) -> Result<Vec<String>, Error> {
        let columns = self
            .client
            .query(
                "SELECT column_name FROM information_schema.columns WHERE table_name = $1",
                &[&table],
            )
            .await?;
        Ok(columns.iter().map(|row| row.get(0)).collect())
    }

    pub async fn unload(&self, table: &str, to_bucket: &str) -> Result<u64, Error> {
        let columns = self.columns(table).await?;
        let sql = &format!(
            r#"
                UNLOAD ('SELECT {} FROM {}') TO 's3://{}/in/{}_'
                CREDENTIALS 'aws_access_key_id={};aws_secret_access_key={}'
                CSV HEADER ALLOWOVERWRITE PARALLEL OFF;
            "#,
            columns.join(", "),
            table,
            to_bucket,
            table,
            // FIXME replace with taking a config as param
            env::var("AWS_ACCESS_KEY_ID").unwrap(),
            env::var("AWS_SECRET_ACCESS_KEY").unwrap()
        );

        Ok(self.client.execute(sql, &[]).await?)
    }

    pub async fn exec(&self, sql: &str) -> Result<u64, Error> {
        self.client.execute(sql, &[]).await
    }
}
