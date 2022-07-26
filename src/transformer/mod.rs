mod city;
mod date;
mod email;
mod first_name;
mod from;
mod last_name;
mod null;
mod random;
mod random_value;
mod regex;
mod reverse;
mod sequence;
mod state;
mod statictr;
mod street;
mod zip_code;

pub use self::city::CityTransformer;
pub use self::date::DateTransformer;
pub use self::email::EmailTransformer;
pub use self::first_name::FirstNameTransformer;
pub use self::from::FromTransformer;
pub use self::last_name::LastNameTransformer;
pub use self::null::NullTransformer;
pub use self::random::RandomTransformer;
pub use self::random_value::RandomValueTransformer;
pub use self::regex::RegexTransformer;
pub use self::reverse::ReverseTransformer;
pub use self::sequence::SequenceTransformer;
pub use self::state::StateTransformer;
pub use self::statictr::StaticTransformer;
pub use self::street::StreetTransformer;
pub use self::zip_code::ZipCodeTransformer;

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
