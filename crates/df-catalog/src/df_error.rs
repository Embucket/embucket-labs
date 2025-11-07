use error_stack_trace;
use snafu::Location;
use snafu::prelude::*;

// This errors list created from inlined errors texts

#[derive(Snafu)]
#[snafu(visibility(pub(crate)))]
#[error_stack_trace::debug]
pub enum DFExternalError {
    #[snafu(display("Object store not found for url {url}"))]
    ObjectStoreNotFound {
        url: String,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Ordinal position param overflow: {error}"))]
    OrdinalPositionParamOverflow {
        #[snafu(source)]
        error: std::num::TryFromIntError,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("rid param doesn't fit in u8"))]
    RidParamDoesntFitInU8 {
        #[snafu(source)]
        error: std::num::TryFromIntError,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Catalog '{name}' not found in catalog list"))]
    CatalogNotFound {
        name: String,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Cannot resolve view reference '{reference}'"))]
    CannotResolveViewReference {
        reference: String,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Failed to downcast Session to SessionState"))]
    SessionDowncast {
        #[snafu(implicit)]
        location: Location,
    },
}

impl From<DFExternalError> for datafusion_common::DataFusionError {
    fn from(value: DFExternalError) -> Self {
        Self::External(Box::new(value))
    }
}
