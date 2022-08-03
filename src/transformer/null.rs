use super::{TransformationContext, Transformer};

pub struct NullTransformer {}

impl NullTransformer {
    pub fn default() -> Self {
        NullTransformer {}
    }
}

impl Transformer for NullTransformer {
    fn id(&self) -> &str {
        "null"
    }

    fn description(&self) -> &str {
        "Null value"
    }

    fn transform(&self, _: &TransformationContext) -> String {
        "".to_string()
    }
}
