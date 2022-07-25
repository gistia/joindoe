mod date;
mod email;
mod first_name;
mod from;
mod last_name;
mod null;
mod regex;
mod reverse;
mod sequence;

pub use self::date::DateTransformer;
pub use self::email::EmailTransformer;
pub use self::first_name::FirstNameTransformer;
pub use self::from::FromTransformer;
pub use self::last_name::LastNameTransformer;
pub use self::null::NullTransformer;
pub use self::regex::RegexTransformer;
pub use self::reverse::ReverseTransformer;
pub use self::sequence::SequenceTransformer;

pub trait Transformer {
    fn id(&self) -> &str;
    fn description(&self) -> &str;
    fn transform(&self, ctx: &TransformationContext) -> String;
}

pub struct TransformationContext<'a> {
    pub index: usize,
    pub row: Vec<&'a str>,
    pub columns: Vec<String>,
    pub value: &'a str,
}
