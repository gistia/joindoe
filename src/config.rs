use std::{env, fs};

use serde::{Deserialize, Serialize};

use crate::transformer::{
    FirstNameTransformer, LastNameTransformer, ReverseTransformer, Transformer,
};

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Config {
    pub source: Source,
    pub store: Store,
    pub destination: Destination,
    pub postprocess: Option<Vec<PostProcessTask>>,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[serde(tag = "task", content = "properties")]
pub enum TaskType {
    Pdf(PdfConfig),
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct PdfConfig {
    pub bucket: String,
    pub from: String,
    pub file_name: String,
    pub aws_access_key_id: String,
    pub aws_secret_access_key: String,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct PostProcessTask {
    pub name: String,
    #[serde(flatten)]
    pub task: TaskType,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Transformation {
    pub column: String,
    #[serde(flatten)]
    pub transformer: TransformerType,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Table {
    pub name: String,
    pub transform: Option<Vec<Transformation>>,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Source {
    pub connection_uri: String,
    pub tables: Vec<Table>,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Store {
    pub bucket: String,
    pub aws_access_key_id: String,
    pub aws_secret_access_key: String,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Destination {
    pub connection_uri: String,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
#[serde(rename_all = "kebab-case")]
#[serde(tag = "transformer")]
pub enum TransformerType {
    Reverse,
    FirstName,
    LastName,
}

impl TransformerType {
    pub fn transformer(&self) -> Box<dyn Transformer> {
        match self {
            TransformerType::Reverse => Box::new(ReverseTransformer::default()),
            TransformerType::FirstName => Box::new(FirstNameTransformer::default()),
            TransformerType::LastName => Box::new(LastNameTransformer::default()),
        }
    }
}

fn replace_env_vars(s: &str) -> String {
    let mut res = s.to_owned();
    let mut vars: Vec<(String, String)> = env::vars().into_iter().collect();
    vars.sort_by(|a, b| b.0.len().cmp(&a.0.len()));

    for (name, val) in vars {
        res = res.replace(&format!("${}", name), &val).to_owned();
    }
    res.to_owned()
}

impl Config {
    pub fn new(file: &str) -> Self {
        let config_str = replace_env_vars(&fs::read_to_string(file).unwrap());
        Self::new_from_str(&config_str)
    }

    pub fn new_from_str(s: &str) -> Self {
        serde_yaml::from_str(s).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config() {
        let str = indoc::indoc! {r#"
            source:
                connection_uri: $DATABASE_URL
                tables:
                    - name: providers
                      transform:
                        - column: identifier
                          transformer: reverse
                        - column: first_name
                          transformer: first-name
                        - column: last_name
                          transformer: last-name
                    - name: insurances
                    - name: locations
                    - name: test_definitions
                    - name: orders
                      transform:
                        - column: identifier
                          transformer: reverse
                    - name: orders_tests

            store:
                bucket: nw-data-transfer
                aws_access_key_id: $AWS_ACCESS_KEY_ID
                aws_secret_access_key: $AWS_SECRET_ACCESS_KEY

            destination:
                connection_uri: $TEST_DATABASE_URL

            postprocess:
                - name: Generate results PDF
                  task: pdf
                  properties:
                    bucket: nw-pdf
                    from: |
                      SELECT
                          o.identifier, ot.test_code, p.first_name, p.last_name, p.date_of_birth
                      FROM
                          "orders" o
                          JOIN "orders_tests" ot ON ot.order_id = o.id
                          JOIN "patients" p ON p.id = o.patient_id
                    file_name: "{{ identifier }}_{{ test_code }}.pdf"
                    aws_access_key_id: $AWS_ACCESS_KEY_ID
                    aws_secret_access_key: $AWS_SECRET_ACCESS_KEY
        "#};

        let config = Config::new_from_str(str);
        assert_eq!(config.postprocess.unwrap()[0].name, "Generate results PDF");
    }

    #[test]
    fn test_env_vars() {
        env::set_var("TEST_ENV_VAR", "small");
        env::set_var("TEST_ENV_VAR_1", "large");
        let res = replace_env_vars("env=$TEST_ENV_VAR_1,database=$TEST_ENV_VAR");
        assert_eq!(res, "env=large,database=small");
    }
}
