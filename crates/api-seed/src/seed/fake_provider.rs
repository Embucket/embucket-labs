use api_structs::volumes::VolumeType;
use fake::faker::{lorem::en::Word, name::raw::Name};
use fake::{Fake, Faker, locales::EN};
use chrono::NaiveDate;
use crate::seed::ColumnType;

pub struct FakeProvider;

impl FakeProvider {
    pub fn person_name() -> String {
        Name(EN).fake()
    }

    pub fn entity_name() -> String {
        let one: String = Word().fake();
        let two: String = Word().fake();
        format!("{}_{}", one.to_lowercase(), two.to_lowercase())
    }

    fn value_by_type(column_type: ColumnType) -> String {
        match column_type {
            ColumnType::Int
            | ColumnType::Number => format!("{}", Faker.fake::<i32>()),
            ColumnType::Real => format!("{:.2}", Faker.fake::<f32>()),
            ColumnType::Varchar => Name(EN).fake(),
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
