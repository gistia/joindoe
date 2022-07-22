mod reverse;

pub use self::reverse::ReverseTransformer;

pub trait Transformer {
    fn id(&self) -> &str;
    fn description(&self) -> &str;
    fn transform(&self, data: &str) -> String;
}
