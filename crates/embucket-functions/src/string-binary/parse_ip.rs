use crate::macros::make_udf_function;
use datafusion::arrow::array::{Array, StringArray, StringBuilder};
use datafusion::arrow::datatypes::DataType;
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::{
    Coercion, ColumnarValue, Signature, TypeSignature, TypeSignatureClass, Volatility,
};
use datafusion_common::ScalarValue;
use datafusion_common::cast::as_generic_string_array;
use datafusion_common::types::{
    NativeType, logical_float16, logical_float32, logical_float64, logical_string,
};
use datafusion_expr::{ScalarFunctionArgs, ScalarUDFImpl};
use ipnet::IpNet;
use serde_json::json;
use std::any::Any;
use std::sync::Arc;

use crate::string_binary::errors::{
    ArrayLengthMismatchSnafu, NonConstantArgumentSnafu, ParseIpFailedSnafu,
};

/// `PARSE_IP` SQL function
///
/// Parses an IPv4 or IPv6 string with optional netmask and returns a JSON
/// object describing the address and its network range.
///
/// Syntax: `PARSE_IP`(<`ip_expr`>, <type> [, <permissive>])
///
/// Arguments:
/// - `ip_expr`: IPv4 or IPv6 address optionally containing a prefix
///   length (e.g. `192.168.0.1/24`).
/// - `type`: Type of parsing to perform. Currently only `'INET'` is
///   supported.
/// - `permissive`: Optional flag controlling error handling. When set to 1,
///   parse errors return an object with an `error` field instead of failing.
///
/// Example: SELECT `PARSE_IP`('192.168.242.188/24', 'INET');
///
/// Returns:
/// - A JSON string with address components and network range.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ParseIpFunc {
    signature: Signature,
}

impl Default for ParseIpFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl ParseIpFunc {
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::String(2),
                    TypeSignature::Coercible(vec![
                        Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                        Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                        // TODO: Change to TypeSignatureClass::Numeric after it's implemented
                        Coercion::new_implicit(
                            TypeSignatureClass::Native(logical_float64()),
                            vec![
                                TypeSignatureClass::Integer,
                                TypeSignatureClass::Native(logical_float16()),
                                TypeSignatureClass::Native(logical_float32()),
                            ],
                            NativeType::Float64,
                        ),
                    ]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for ParseIpFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "parse_ip"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let ScalarFunctionArgs {
            args, number_rows, ..
        } = args;

        let ip_arg = args[0].clone().into_array(number_rows)?;
        let type_arg = args[1].clone().into_array(number_rows)?;

        let ip_array: &StringArray = as_generic_string_array(&ip_arg)?;
        let type_array: &StringArray = as_generic_string_array(&type_arg)?;

        let len = ip_array.len();

        let perm: bool = if args.len() > 2 {
            match &args[2] {
                ColumnarValue::Array(_) => NonConstantArgumentSnafu {
                    function_name: "PARSE_IP".to_string(),
                    position: 3usize,
                }
                .fail()?,
                ColumnarValue::Scalar(v) => {
                    let v = v.cast_to(&DataType::Float64)?;
                    let f = if let ScalarValue::Float64(Some(x)) = v {
                        x
                    } else {
                        0.0
                    };
                    f.round() != 0.0
                }
            }
        } else {
            false
        };

        if ip_array.len() != type_array.len() {
            ArrayLengthMismatchSnafu.fail()?;
        }

        let mut builder = StringBuilder::with_capacity(len, len * 32);

        for i in 0..len {
            if ip_array.is_null(i) || type_array.is_null(i) {
                builder.append_null();
                continue;
            }
            let typ = type_array.value(i).to_uppercase();
            if typ != "INET" {
                builder.append_null();
                continue;
            }
            let ip_str = ip_array.value(i);
            match ip_str.parse::<IpNet>() {
                Ok(parsed) => match parsed {
                    IpNet::V4(net) => {
                        let json = ipv4_json(net);
                        builder.append_value(json);
                    }
                    IpNet::V6(net) => {
                        let json = ipv6_json(net);
                        builder.append_value(json);
                    }
                },
                Err(e) => {
                    if perm {
                        let json_err = json!({ "error": e.to_string() }).to_string();
                        builder.append_value(json_err);
                    } else {
                        ParseIpFailedSnafu {
                            reason: e.to_string(),
                        }
                        .fail()?;
                    }
                }
            }
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

make_udf_function!(ParseIpFunc);

fn ipv4_json(net: ipnet::Ipv4Net) -> String {
    let host: std::net::Ipv4Addr = net.addr();
    let prefix = net.prefix_len();
    let host_u32: u32 = host.into();
    let start_u32: u32 = net.network().into();
    let end_u32: u32 = net.broadcast().into();
    format!(
        "{{\"family\":4,\"host\":\"{host}\",\"ip_fields\":[{host_u32},0,0,0],\"ip_type\":\"inet\",\"ipv4\":{host_u32},\"ipv4_range_end\":{end_u32},\"ipv4_range_start\":{start_u32},\"netmask_prefix_length\":{prefix},\"snowflake$type\":\"ip_address\"}}",
    )
}

#[allow(clippy::as_conversions)]
fn ipv6_json(net: ipnet::Ipv6Net) -> String {
    let host: std::net::Ipv6Addr = net.addr();
    let prefix = net.prefix_len();
    let host_u128: u128 = host.into();
    let start_u128: u128 = net.network().into();
    let end_u128: u128 = net.broadcast().into();
    let fields = [
        ((host_u128 >> 96) & 0xffff_ffff) as u32,
        ((host_u128 >> 64) & 0xffff_ffff) as u32,
        ((host_u128 >> 32) & 0xffff_ffff) as u32,
        (host_u128 & 0xffff_ffff) as u32,
    ];
    format!(
        "{{\"family\":6,\"hex_ipv6\":\"{host_u128:032X}\",\"hex_ipv6_range_end\":\"{end_u128:032X}\",\"hex_ipv6_range_start\":\"{start_u128:032X}\",\"host\":\"{host}\",\"ip_fields\":[{},{},{},{}],\"ip_type\":\"inet\",\"netmask_prefix_length\":{prefix},\"snowflake$type\":\"ip_address\"}}",
        fields[0], fields[1], fields[2], fields[3]
    )
}
