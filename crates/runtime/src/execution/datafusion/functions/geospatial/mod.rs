pub mod accessors;
pub mod constructors;
pub mod measurement;

pub mod data_types;
pub mod error;

use datafusion::prelude::SessionContext;

pub fn register_udfs(ctx: &SessionContext) {
    constructors::register_udfs(ctx);
    accessors::register_udfs(ctx);
    measurement::register_udfs(ctx);
}
