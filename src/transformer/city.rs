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
        "Random city name"
    }

    fn transform(&self, _: &TransformationContext) -> String {
        CityName().fake()
    }
}
