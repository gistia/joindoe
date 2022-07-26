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
        "Replaces the content of the field with a random zipcode abbreviation"
    }

    fn transform(&self, _: &TransformationContext) -> String {
        ZipCode().fake()
    }
}
