use super::{TransformationContext, Transformer};
use rand::Rng;

pub struct RandomTransformer {
    range_start: usize,
    range_end: usize,
}

impl RandomTransformer {
    pub fn new(range_start: &usize, range_end: &usize) -> Self {
        RandomTransformer {
            range_start: *range_start,
            range_end: *range_end,
        }
    }
}

impl Transformer for RandomTransformer {
    fn id(&self) -> &str {
        "random"
    }

    fn description(&self) -> &str {
        "Replaces the content of the field with a random value defined by a range"
    }

    fn transform(&self, _: &TransformationContext) -> String {
        let mut rng = rand::thread_rng();
        let random_value = rng.gen_range(self.range_start..self.range_end);
        random_value.to_string()
    }
}
