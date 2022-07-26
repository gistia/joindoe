use super::{TransformationContext, Transformer};
use fake::{faker::address::en::StreetName, Fake};
use rand::Rng;

pub struct StreetTransformer {}

impl StreetTransformer {
    pub fn default() -> Self {
        StreetTransformer {}
    }
}

impl Transformer for StreetTransformer {
    fn id(&self) -> &str {
        "street"
    }

    fn description(&self) -> &str {
        "Replaces the content of the field with a random street address"
    }

    fn transform(&self, _: &TransformationContext) -> String {
        let street: String = StreetName().fake();
        let mut rng = rand::thread_rng();
        let random_num = rng.gen_range(20..50000);

        format!("{} {}", random_num, street)
    }
}
