use rand::Rng;
use rand_regex::Regex;

use super::{TransformationContext, Transformer};

pub struct RegexTransformer {
    generator: Regex,
}

impl RegexTransformer {
    pub fn new(format: &str) -> Self {
        let generator = rand_regex::Regex::compile(format, 1).unwrap();
        RegexTransformer { generator }
    }
}

impl Transformer for RegexTransformer {
    fn id(&self) -> &str {
        "regex"
    }

    fn description(&self) -> &str {
        "Random string generated from a regular expression"
    }

    fn transform(&self, _: &TransformationContext) -> String {
        rand::thread_rng()
            .sample_iter::<String, _>(&self.generator)
            .take(1)
            .next()
            .unwrap()
    }
}
