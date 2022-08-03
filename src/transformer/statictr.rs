use super::{TransformationContext, Transformer};

pub struct StaticTransformer {
    value: String,
}

impl StaticTransformer {
    pub fn new(value: &str) -> Self {
        StaticTransformer {
            value: value.to_string(),
        }
    }
}

impl Transformer for StaticTransformer {
    fn id(&self) -> &str {
        "static"
    }

    fn description(&self) -> &str {
        "Fixed value for all records"
    }

    fn transform(&self, _: &TransformationContext) -> String {
        self.value.clone()
    }
}
