use fake::faker::name::raw::LastName;
use fake::locales::EN;
use fake::Fake;

use super::{TransformationContext, Transformer};

pub struct LastNameTransformer {}

impl LastNameTransformer {
    pub fn default() -> Self {
        LastNameTransformer {}
    }
}

impl Transformer for LastNameTransformer {
    fn id(&self) -> &str {
        "last-name"
    }

    fn description(&self) -> &str {
        "Random last name"
    }

    fn transform(&self, _: &TransformationContext) -> String {
        LastName(EN).fake()
    }
}
