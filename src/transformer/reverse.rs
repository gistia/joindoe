use super::Transformer;
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
        "Reverse the contents of the field"
    }

    fn transform(&self, input: &str) -> String {
        input.graphemes(true).rev().collect::<String>()
    }
}
