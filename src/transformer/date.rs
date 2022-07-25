use chrono::{DateTime, Duration, Utc};
use fake::{faker::chrono::en::DateTimeBetween, Fake};

use super::{TransformationContext, Transformer};

pub struct DateTransformer {
    format: String,
}

impl DateTransformer {
    pub fn new(format: &str) -> Self {
        DateTransformer {
            format: format.to_string(),
        }
    }
}

impl Transformer for DateTransformer {
    fn id(&self) -> &str {
        "date"
    }

    fn description(&self) -> &str {
        "Replaces the content of the random formatted date/time"
    }

    fn transform(&self, _: &TransformationContext) -> String {
        let range_start = Utc::now() - Duration::days(90 * 365);
        let range_end = Utc::now() - Duration::days(5 * 365);
        let date: DateTime<Utc> = DateTimeBetween(range_start, range_end).fake();

        date.format(&self.format).to_string()
    }
}
