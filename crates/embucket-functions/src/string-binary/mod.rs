use datafusion::logical_expr::{Coercion, TypeSignatureClass};
use datafusion_common::types::logical_string;
use datafusion_expr::ScalarUDF;
use datafusion_expr::registry::FunctionRegistry;
use std::sync::Arc;

pub mod errors;
pub mod hex_decode_binary;
pub mod hex_decode_string;
pub mod hex_encode;
pub mod insert;
pub mod jarowinkler_similarity;
pub mod length;
pub mod lower;
pub mod parse_ip;
pub mod randstr;
pub mod replace;
pub mod rtrimmed_length;
pub mod sha2;
pub mod split;
pub mod strtok;
pub mod substr;

use crate::string_binary::hex_decode_binary::HexDecodeBinaryFunc;
use crate::string_binary::hex_decode_string::HexDecodeStringFunc;
pub use errors::Error;

pub fn register_udfs(registry: &mut dyn FunctionRegistry) -> datafusion_common::Result<()> {
    let functions: Vec<Arc<ScalarUDF>> = vec![
        Arc::new(ScalarUDF::from(HexDecodeStringFunc::new(false))),
        Arc::new(ScalarUDF::from(HexDecodeStringFunc::new(true))),
        Arc::new(ScalarUDF::from(HexDecodeBinaryFunc::new(false))),
        Arc::new(ScalarUDF::from(HexDecodeBinaryFunc::new(true))),
        hex_encode::get_udf(),
        insert::get_udf(),
        jarowinkler_similarity::get_udf(),
        length::get_udf(),
        lower::get_udf(),
        parse_ip::get_udf(),
        randstr::get_udf(),
        rtrimmed_length::get_udf(),
        sha2::get_udf(),
        split::get_udf(),
        strtok::get_udf(),
        substr::get_udf(),
        replace::get_udf(),
    ];

    for func in functions {
        registry.register_udf(func)?;
    }

    Ok(())
}

fn logical_str() -> Coercion {
    Coercion::new_exact(TypeSignatureClass::Native(logical_string()))
}
