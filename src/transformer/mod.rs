mod first_name;
mod last_name;
mod reverse;

pub use self::first_name::FirstNameTransformer;
pub use self::last_name::LastNameTransformer;
pub use self::reverse::ReverseTransformer;

pub trait Transformer {
    fn id(&self) -> &str;
    fn description(&self) -> &str;
    fn transform(&self, data: &str) -> String;
}
