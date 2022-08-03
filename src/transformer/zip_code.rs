use super::{TransformationContext, Transformer};
use fake::{faker::address::en::ZipCode, Fake};

pub struct ZipCodeTransformer {}

impl ZipCodeTransformer {
    pub fn default() -> Self {
        ZipCodeTransformer {}
    }
}

impl Transformer for ZipCodeTransformer {
    fn id(&self) -> &str {
        "zip-code"
    }

    fn description(&self) -> &str {
        "Random zipcode abbreviation"
    }

    fn transform(&self, _: &TransformationContext) -> String {
        ZipCode().fake()
    }
}
