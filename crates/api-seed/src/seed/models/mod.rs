pub mod column;
pub mod database;
pub mod schema;
pub mod table;
pub mod volume;

pub use column::*;
pub use database::*;
pub use schema::*;
pub use table::*;
pub use volume::*;

use serde::{Deserialize, Serialize};
use std::marker::PhantomData;

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct VolumesRoot {
    // every volume added explicitely, no volume items auto-generated
    pub volumes: Vec<VolumeGenerator>,
}

impl VolumesRoot {
    #[must_use]
    pub fn generate(&self) -> Vec<Volume> {
        self.volumes
            .iter()
            .enumerate()
            .map(|(i, v)| v.generate(i))
            .collect()
    }
}

pub trait Generator<T> {
    // create entity, item index is just for reference
    fn generate(&self, index: usize) -> T;
}

#[derive(Debug, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct WithCount<T, G>
where
    G: Generator<T>,
{
    count: usize,
    template: G,
    #[serde(skip)]
    _marker: PhantomData<T>,
}

impl<T, G> WithCount<T, G>
where
    G: Generator<T>,
{
    #[must_use]
    pub const fn new(count: usize, template: G) -> Self {
        Self {
            count,
            template,
            _marker: PhantomData,
        }
    }

    // create items for template, item index is just for reference
    pub fn vec_with_count(&self, _index: usize) -> Vec<T> {
        // call generate n times
        (0..self.count).map(|i| self.template.generate(i)).collect()
    }
}
