use super::{TransformationContext, Transformer};
use rand::Rng;

pub struct RandomValueTransformer {
    values: Vec<String>,
}

impl RandomValueTransformer {
    pub fn new(values: &Vec<String>) -> Self {
        RandomValueTransformer {
            values: values.clone(),
        }
    }
}

impl Transformer for RandomValueTransformer {
    fn id(&self) -> &str {
        "random-values"
    }

    fn description(&self) -> &str {
        "Random value picked from a list"
    }

    fn transform(&self, _: &TransformationContext) -> String {
        let mut rng = rand::thread_rng();
        let random_value = rng.gen_range(0..self.values.len());
        self.values[random_value].clone()
    }
}
