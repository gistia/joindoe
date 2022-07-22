use std::{env, fs};

use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Source {
    pub connection_uri: String,
    pub tables: Vec<String>,
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

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Config {
    pub source: Source,
    pub store: Store,
    pub destination: Destination,
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
        serde_yaml::from_str(&config_str).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config() {
        Config::new("config.yml");
    }

    #[test]
    fn test_env_vars() {
        env::set_var("TEST_ENV_VAR", "small");
        env::set_var("TEST_ENV_VAR_1", "large");
        let res = replace_env_vars("env=$TEST_ENV_VAR_1,database=$TEST_ENV_VAR");
        assert_eq!(res, "env=large,database=small");
    }
}
