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

pub fn transformers_info() -> Vec<(String, String)> {
    let all: Vec<Box<dyn Transformer>> = vec![
        Box::new(NullTransformer::default()),
        Box::new(ReverseTransformer::default()),
        Box::new(FirstNameTransformer::default()),
        Box::new(LastNameTransformer::default()),
        Box::new(SequenceTransformer::default()),
        Box::new(EmailTransformer::default()),
        Box::new(StreetTransformer::default()),
        Box::new(CityTransformer::default()),
        Box::new(StateTransformer::default()),
        Box::new(ZipCodeTransformer::default()),
        Box::new(RegexTransformer::new("[a]")),
        Box::new(FromTransformer::new("source")),
        Box::new(DateTransformer::new("%Y-%m-%d")),
        Box::new(RandomTransformer::new(&1, &100)),
        Box::new(RandomValueTransformer::new(&vec![
            "value1".to_owned(),
            "value2".to_owned(),
            "value3".to_owned(),
        ])),
        Box::new(StaticTransformer::new("static")),
    ];
    let mut res: Vec<(String, String)> = all
        .iter()
        .map(|t| (t.id().to_string(), t.description().to_string()))
        .collect();
    res.sort_by(|a, b| a.0.cmp(&b.0));
    res
}

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
