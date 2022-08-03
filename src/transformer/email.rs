use super::{TransformationContext, Transformer};
use fake::{faker::internet::en::FreeEmail, Fake};

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
        "Random email address"
    }

    fn transform(&self, _: &TransformationContext) -> String {
        FreeEmail().fake()
    }
}
