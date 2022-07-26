use super::{TransformationContext, Transformer};

pub struct SequenceTransformer {}

impl SequenceTransformer {
    pub fn default() -> Self {
        SequenceTransformer {}
    }
}

impl Transformer for SequenceTransformer {
    fn id(&self) -> &str {
        "sequence"
    }

    fn description(&self) -> &str {
        "Replaces the content of the field with a sequential value"
    }

    fn transform(&self, ctx: &TransformationContext) -> String {
        format!("{}", ctx.index + 1)
    }
}
