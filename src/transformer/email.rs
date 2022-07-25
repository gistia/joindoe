use fake::{faker::internet::en::FreeEmail, Fake};

use super::{TransformationContext, Transformer};

pub struct EmailTransformer {}

impl EmailTransformer {
    pub fn default() -> Self {
        EmailTransformer {}
    }
}

impl Transformer for EmailTransformer {
    fn id(&self) -> &str {
        "email"
    }

    fn description(&self) -> &str {
        "Replaces the content of the field with a random email address"
    }

    fn transform(&self, _: &TransformationContext) -> String {
        FreeEmail().fake()
    }
}
