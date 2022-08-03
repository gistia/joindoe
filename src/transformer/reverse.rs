use super::{TransformationContext, Transformer};
use unicode_segmentation::UnicodeSegmentation;

pub struct ReverseTransformer {}

impl ReverseTransformer {
    pub fn default() -> Self {
        ReverseTransformer {}
    }
}

impl Transformer for ReverseTransformer {
    fn id(&self) -> &str {
        "reverse"
    }

    fn description(&self) -> &str {
        "Reverses the contents of the field"
    }

    fn transform(&self, ctx: &TransformationContext) -> String {
        ctx.value.graphemes(true).rev().collect::<String>()
    }
}
