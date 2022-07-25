use std::{env, fs};

use serde::{Deserialize, Serialize};

use crate::transformer::{
    DateTransformer, EmailTransformer, FirstNameTransformer, FromTransformer, LastNameTransformer,
    NullTransformer, RegexTransformer, ReverseTransformer, SequenceTransformer, Transformer,
};

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Config {
    pub source: Source,
    pub store: Store,
    pub destination: Destination,
    pub postprocess: Option<Vec<PostProcessTask>>,
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
    pub generate: Option<usize>,
    pub from: Option<String>,
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
pub struct RegexOptions {
    pub format: String,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct FromOptions {
    pub column: String,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct DateOptions {
    pub format: String,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
#[serde(rename_all = "kebab-case")]
#[serde(tag = "transformer", content = "properties")]
pub enum TransformerType {
    Null,
    Sequence,
    Reverse,
    Regex(RegexOptions),
    FirstName,
    LastName,
    Email,
    From(FromOptions),
    Date(DateOptions),
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[serde(tag = "task", content = "properties")]
pub enum TaskType {
    Pdf(PdfConfig),
}

impl TransformerType {
    pub fn transformer(&self) -> Box<dyn Transformer> {
        match self {
            TransformerType::Null => Box::new(NullTransformer::default()),
            TransformerType::Reverse => Box::new(ReverseTransformer::default()),
            TransformerType::FirstName => Box::new(FirstNameTransformer::default()),
            TransformerType::LastName => Box::new(LastNameTransformer::default()),
            TransformerType::Sequence => Box::new(SequenceTransformer::default()),
            TransformerType::Email => Box::new(EmailTransformer::default()),
            TransformerType::Regex(options) => Box::new(RegexTransformer::new(&options.format)),
            TransformerType::From(options) => Box::new(FromTransformer::new(&options.column)),
            TransformerType::Date(options) => Box::new(DateTransformer::new(&options.format)),
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
    - name: patient_master_record
      generate: 100
      transform:
        - column: id
          transformer: sequence
        - column: external_id
          transformer: regex
          properties:
            format: /[0-9]{12}/
        - column: internal_id
          transformer: regex
          properties:
            format: /[0-9]{12}/
        - column: first_name
          transformer: first-name
        - column: last_name
          transformer: last-name
        - column: ssn
          transformer: regex
          properties:
            format: /[0-9]{3}-[0-9]{2}-[0-9]{4}/
        - column: date_of_birth
          transformer: date
          properties:
            format: '%Y-%m-%d'
        - column: email
          transformer: email
    - name: patient
      from: SELECT id, external_id, internal_id, first_name, last_name, ssn, date_of_birth, email FROM patient_master_record
      transform:
        - column: id
          transformer: sequence
        - column: external_id
          transformer: from
          properties:
            column: id
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
  connection_uri: $EXP_DATABASE_URL

postprocess:
  - name: Generate results PDF
    task: pdf
    properties:
      from: |
        SELECT
          o.identifier, ot.test_code, p.first_name, p.last_name, p.date_of_birth::text
        FROM
          "orders" o
          JOIN "orders_tests" ot ON ot.order_id = o.id
          JOIN "patients" p ON p.id = o.patient_id
      bucket: nw-pdf
      file_name: ""
      aws_access_key_id: $AWS_ACCESS_KEY_ID
      aws_secret_access_key: $AWS_SECRET_ACCESS_KEY
        "#};

        let config = Config::new_from_str(str);
        println!("{:#?}", config);
        // assert_eq!(config.postprocess.unwrap()[0].name, "Generate results PDF");
    }

    #[test]
    fn test_env_vars() {
        env::set_var("TEST_ENV_VAR", "small");
        env::set_var("TEST_ENV_VAR_1", "large");
        let res = replace_env_vars("env=$TEST_ENV_VAR_1,database=$TEST_ENV_VAR");
        assert_eq!(res, "env=large,database=small");
    }
}
