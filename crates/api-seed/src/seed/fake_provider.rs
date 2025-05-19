use api_structs::volumes::VolumeType;
use fake::faker::{lorem::en::Word, name::raw::Name};
use fake::{Fake, locales::EN};

pub struct FakeProvider;

impl FakeProvider {
    pub fn volume_type() -> VolumeType {
        VolumeType::Memory // no random volume type
    }
    pub fn person_name() -> String {
        Name(EN).fake()
    }
    pub fn entity_name() -> String {
        let one: String = Word().fake();
        let two: String = Word().fake();
        format!("{}_{}", one.to_lowercase(), two.to_lowercase())
    }
}
