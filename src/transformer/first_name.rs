use fake::faker::name::raw::FirstName;
use fake::locales::EN;
use fake::Fake;

use super::{TransformationContext, Transformer};

pub struct FirstNameTransformer {}

impl FirstNameTransformer {
    pub fn default() -> Self {
        FirstNameTransformer {}
    }
}

impl Transformer for FirstNameTransformer {
    fn id(&self) -> &str {
        "first-name"
    }

    fn description(&self) -> &str {
        "Replaces the content of the field with a random first name"
    }

    fn transform(&self, _: &TransformationContext) -> String {
        FirstName(EN).fake()
    }
}
