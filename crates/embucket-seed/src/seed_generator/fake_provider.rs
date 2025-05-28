use crate::seed_models::ColumnType;
use chrono::NaiveDate;
use fake::faker::{lorem::en::Word, name::raw::Name};
use fake::{Fake, Faker, locales::EN};

pub struct FakeProvider;

impl FakeProvider {
    #[must_use]
    pub fn person_name() -> String {
        Name(EN).fake()
    }

    // entity_name expects idx to gauarntee uniqueness in its domain
    #[must_use]
    pub fn entity_name(idx: usize) -> String {
        // format!("_{}", uuid::Uuid::new_v4().to_string().replace("-", "_"))

        let one: String = Word().fake();
        let two: String = Word().fake();
        format!("{one}_{two}_{idx}")
    }

    fn _value_by_type(column_type: &ColumnType) -> String {
        match column_type {
            ColumnType::String | ColumnType::Varchar => Name(EN).fake(),
            ColumnType::Int | ColumnType::Number => format!("{}", Faker.fake::<i32>()),
            ColumnType::Real => format!("{:.2}", Faker.fake::<f32>()),
            ColumnType::Boolean => format!("{}", Faker.fake::<bool>()),
            ColumnType::Date => format!("{}", Faker.fake::<NaiveDate>()),
            _ => String::new(),
            // ColumnType::Timestamp,
            // Variant,
            //Object,
            // Array,
        }
    }
}
