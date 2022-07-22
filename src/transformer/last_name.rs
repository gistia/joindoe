use fake::faker::name::raw::LastName;
use fake::locales::EN;
use fake::Fake;

use super::Transformer;

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
        "Replaces the content of the field with a random last name"
    }

    fn transform(&self, _: &str) -> String {
        LastName(EN).fake()
    }
}
