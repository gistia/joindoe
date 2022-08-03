use super::{TransformationContext, Transformer};
use fake::{faker::address::en::StateAbbr, Fake};

pub struct StateTransformer {}

impl StateTransformer {
    pub fn default() -> Self {
        StateTransformer {}
    }
}

impl Transformer for StateTransformer {
    fn id(&self) -> &str {
        "state"
    }

    fn description(&self) -> &str {
        "Random state abbreviation"
    }

    fn transform(&self, _: &TransformationContext) -> String {
        StateAbbr().fake()
    }
}
