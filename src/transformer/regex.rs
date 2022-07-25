use rand::{Rng, SeedableRng};
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
        "Replaces the content of the field with a random string generated from a regular expression"
    }

    fn transform(&self, _: &TransformationContext) -> String {
        let mut rng = rand_xorshift::XorShiftRng::from_seed(*b"The initial seed");
        let samples = (&mut rng)
            .sample_iter(&self.generator)
            .take(1)
            .collect::<Vec<String>>();
        samples[0].clone()
    }
}
