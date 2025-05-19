pub mod volume;
pub mod schema;
pub mod database;
pub mod table;


pub use volume::*;
pub use schema::*;
pub use database::*;
pub use table::*;


use serde::{Deserialize, Serialize};
use std::marker::PhantomData;


///// Traits

pub trait Generator<T> {
    fn generate(&self, index: usize) -> T;
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct WithCount<T, G>
where
    G: Generator<T>,
{
    count: usize,
    generator: G,
    #[serde(skip)]
    _marker: PhantomData<T>,
}

impl<T, G> WithCount<T, G>
where
    G: Generator<T>,
{
    pub fn new(count: usize, generator: G) -> Self {
        Self {
            count,
            generator,
            _marker: PhantomData,
        }
    }

    pub fn generate(&self) -> Vec<T> {
        (0..self.count)
            .map(|i| self.generator.generate(i))
            .collect()
    }
}

///// Seed Root

#[derive(Debug, Serialize, Deserialize)]
pub struct SeedRoot {
    pub volumes: Vec<VolumeSeed>,
}
