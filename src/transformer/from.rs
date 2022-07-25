use super::{TransformationContext, Transformer};

pub struct FromTransformer {
    column: String,
}

impl FromTransformer {
    pub fn new(column: &str) -> Self {
        FromTransformer {
            column: column.to_string(),
        }
    }
}

impl Transformer for FromTransformer {
    fn id(&self) -> &str {
        "from"
    }

    fn description(&self) -> &str {
        "Replaces the content of the field using another column as its source"
    }

    fn transform(&self, ctx: &TransformationContext) -> String {
        let index = ctx.columns.iter().position(|c| *c == self.column).unwrap();
        ctx.row.get(index).unwrap().to_string()
    }
}
