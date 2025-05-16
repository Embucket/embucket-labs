use datafusion::logical_expr::{Signature, Volatility};
use datafusion_expr::AggregateUDFImpl;
use crate::aggregate::boolor_agg::BoolOrAggUDAF;

#[derive(Debug, Clone)]
pub struct ArrayUniqueAggUDAF {
    signature: Signature,
}

impl Default for ArrayUniqueAggUDAF {
    fn default() -> Self {
        Self::new()
    }
}

impl ArrayUniqueAggUDAF {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for ArrayUniqueAggUDAF {}