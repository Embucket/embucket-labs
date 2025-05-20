pub mod volume;
pub mod schema;
pub mod database;
pub mod table;
pub mod column;

pub use volume::*;
pub use schema::*;
pub use database::*;
pub use table::*;
pub use column::*;

use serde::{Deserialize, Serialize};
use std::marker::PhantomData;

#[derive(Debug, Serialize, Deserialize)]
pub struct VolumesRoot {
    pub volumes: Vec<VolumeGenerator>,
}

impl VolumesRoot {
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

#[derive(Debug, Serialize, Deserialize, Default)]
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
    pub fn new(count: usize, template: G) -> Self {
        Self {
            count,
            template,
            _marker: PhantomData,
        }
    }

    // create items for template, item index is just for reference
    pub fn vec_with_count(&self, index: usize) -> Vec<T> {
        // call generate n times
        (0..self.count)
            .map(|i| self.template.generate(i))
            .collect()
    }
}
