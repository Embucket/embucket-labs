use std::sync::Arc;
use snafu::ResultExt;
use super::error::*;
use crate::seed::{read_super_template, Volume};


pub enum SeedVariant {
    Minimal,
    Typical,
    Extreme,
    Insane,
}

#[derive(Default)]
pub struct SeedDatabase {
    pub seed_data: Option<Volume>,
}

impl SeedDatabase {
    pub fn try_load_seed(&mut self, _seed_variant: SeedVariant) -> SeedResult<()> {
        let raw_seed_data = read_super_template().context(LoadSeedSnafu)?;
        self.seed_data = Some(raw_seed_data.materialize());
        Ok(())
    }
}

pub trait SeedApi {
    fn create_volumes(&self) -> SeedResult<()>;
    fn create_databases(&self) -> SeedResult<()>;
    fn create_schemas(&self) -> SeedResult<()>;
    fn create_tables(&self) -> SeedResult<()>;
    fn populate_data(&self) -> SeedResult<()>;
}

impl SeedApi for SeedDatabase {
    fn create_volumes(&self) -> SeedResult<()> {
        // self.metastore.create_volume(name, volume)
        // self.seed_data.volume_name
        Ok(())
    }

    fn create_databases(&self) -> SeedResult<()> {
        // self.metastore.create_database(name, database)
        Ok(())
    }

    fn create_schemas(&self) -> SeedResult<()> {
        Ok(())
    }

    fn create_tables(&self) -> SeedResult<()> {
        Ok(())
    }

    fn populate_data(&self) -> SeedResult<()> {
        Ok(())
    }
}
