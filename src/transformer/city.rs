use super::{TransformationContext, Transformer};
use fake::{faker::address::en::CityName, Fake};

pub struct CityTransformer {}

impl CityTransformer {
    pub fn default() -> Self {
        CityTransformer {}
    }
}

impl Transformer for CityTransformer {
    fn id(&self) -> &str {
        "city"
    }

    fn description(&self) -> &str {
        "Replaces the content of the field with a random city name"
    }

    fn transform(&self, _: &TransformationContext) -> String {
        CityName().fake()
    }
}
