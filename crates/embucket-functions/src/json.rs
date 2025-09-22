#![allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
use crate::arrow_error;
use crate::json::PathToken::{Index, Key};
use base64::engine::Engine;
use datafusion::arrow::array::AsArray;
use datafusion::arrow::array::{
    Array, ArrayRef, BooleanArray, NullArray, PrimitiveArray, StringViewArray,
};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{
    ArrowNativeType, DataType, Date32Type, Date64Type, Decimal256Type, DurationMicrosecondType,
    DurationMillisecondType, DurationNanosecondType, DurationSecondType, Float16Type, Float32Type,
    Float64Type, Int8Type, Int16Type, Int32Type, Int64Type, IntervalDayTimeType,
    IntervalMonthDayNanoType, IntervalUnit, IntervalYearMonthType, Time32MillisecondType,
    Time32SecondType, Time64MicrosecondType, Time64NanosecondType, TimeUnit,
    TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
    TimestampSecondType, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
};
use datafusion::arrow::error::ArrowError;
use datafusion_common::ScalarValue;
use datafusion_expr::ColumnarValue;
use serde_json::{Map, Number, Value as JsonValue, Value};
use std::sync::Arc;

/// Encodes a Boolean Arrow array into a JSON array
pub fn encode_boolean_array(array: ArrayRef) -> Result<JsonValue, ArrowError> {
    let array = array
        .as_ref()
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| ArrowError::InvalidArgumentError("Expected boolean array".into()))?;
    let mut values = Vec::with_capacity(array.len());

    for i in 0..array.len() {
        values.push(if array.is_null(i) {
            JsonValue::Null
        } else {
            JsonValue::Bool(array.value(i))
        });
    }
    Ok(JsonValue::Array(values))
}

/// Encodes a signed Primitive Arrow array into a JSON array
pub fn encode_signed_array<T>(array: ArrayRef) -> Result<JsonValue, ArrowError>
where
    T: datafusion::arrow::datatypes::ArrowPrimitiveType,
    T::Native: Into<i64>,
{
    let array = array
        .as_ref()
        .as_any()
        .downcast_ref::<PrimitiveArray<T>>()
        .ok_or_else(|| ArrowError::InvalidArgumentError("Expected primitive array".into()))?;
    let mut values = Vec::with_capacity(array.len());

    for i in 0..array.len() {
        values.push(if array.is_null(i) {
            JsonValue::Null
        } else {
            JsonValue::Number(Number::from(array.value(i).into()))
        });
    }
    Ok(JsonValue::Array(values))
}

/// Encodes an unsigned Primitive Arrow array into a JSON array
pub fn encode_unsigned_array<T>(array: ArrayRef) -> Result<JsonValue, ArrowError>
where
    T: datafusion::arrow::datatypes::ArrowPrimitiveType,
    T::Native: Into<u64>,
{
    let array = array.as_primitive::<T>();
    let mut values = Vec::with_capacity(array.len());

    for i in 0..array.len() {
        values.push(if array.is_null(i) {
            JsonValue::Null
        } else {
            JsonValue::Number(Number::from(array.value(i).into()))
        });
    }
    Ok(JsonValue::Array(values))
}

/// Encodes a Float16 Arrow array into a JSON array
pub fn encode_float16_array(array: ArrayRef) -> Result<JsonValue, ArrowError> {
    let array = array.as_primitive::<Float16Type>();
    let mut values = Vec::with_capacity(array.len());

    for i in 0..array.len() {
        values.push(if array.is_null(i) {
            JsonValue::Null
        } else {
            let v = f64::from(array.value(i));
            JsonValue::Number(if v.fract() == 0.0 {
                Number::from(array.value(i).to_i64().ok_or_else(|| {
                    ArrowError::InvalidArgumentError("Invalid float value".into())
                })?)
            } else {
                Number::from_f64(v)
                    .ok_or_else(|| ArrowError::InvalidArgumentError("Invalid float value".into()))?
            })
        });
    }
    Ok(JsonValue::Array(values))
}

/// Encodes a Float32 Arrow array into a JSON array
#[allow(clippy::as_conversions)]
pub fn encode_float32_array(array: ArrayRef) -> Result<JsonValue, ArrowError> {
    let array = array.as_primitive::<Float32Type>();
    let mut values = Vec::with_capacity(array.len());

    for i in 0..array.len() {
        values.push(if array.is_null(i) {
            JsonValue::Null
        } else {
            JsonValue::Number(if array.value(i).fract() == 0.0 {
                Number::from(array.value(i).to_i64().ok_or_else(|| {
                    ArrowError::InvalidArgumentError("Invalid float value".into())
                })?)
            } else {
                Number::from_f64(f64::from(array.value(i)))
                    .ok_or_else(|| ArrowError::InvalidArgumentError("Invalid float value".into()))?
            })
        });
    }
    Ok(JsonValue::Array(values))
}

/// Encodes a Float64 Arrow array into a JSON array
pub fn encode_float64_array(array: ArrayRef) -> Result<JsonValue, ArrowError> {
    let array = array.as_primitive::<Float64Type>();
    let mut values = Vec::with_capacity(array.len());

    for i in 0..array.len() {
        values.push(if array.is_null(i) {
            JsonValue::Null
        } else {
            JsonValue::Number(
                Number::from_f64(array.value(i)).ok_or_else(|| {
                    ArrowError::InvalidArgumentError("Invalid float value".into())
                })?,
            )
        });
    }
    Ok(JsonValue::Array(values))
}

/// Encodes a Utf8 Arrow array into a JSON array
pub fn encode_utf8_array(array: ArrayRef) -> Result<JsonValue, ArrowError> {
    let array = array.as_string::<i32>();
    let mut values = Vec::with_capacity(array.len());

    for i in 0..array.len() {
        values.push(if array.is_null(i) {
            JsonValue::Null
        } else {
            JsonValue::String(array.value(i).to_string())
        });
    }
    Ok(JsonValue::Array(values))
}

// Encodes a LargeUtf8 Arrow array into a JSON array
pub fn encode_large_utf8_array(array: ArrayRef) -> Result<JsonValue, ArrowError> {
    let array = array.as_string::<i64>();
    let mut values = Vec::with_capacity(array.len());

    for i in 0..array.len() {
        values.push(if array.is_null(i) {
            JsonValue::Null
        } else {
            JsonValue::String(array.value(i).to_string())
        });
    }
    Ok(JsonValue::Array(values))
}

/// Encodes a Binary Arrow array into a JSON array
pub fn encode_binary_array(array: ArrayRef) -> Result<JsonValue, ArrowError> {
    let array = array.as_binary::<i32>();
    let mut values = Vec::with_capacity(array.len());

    for i in 0..array.len() {
        values.push(if array.is_null(i) {
            JsonValue::Null
        } else {
            JsonValue::String(base64::engine::general_purpose::STANDARD.encode(array.value(i)))
        });
    }
    Ok(JsonValue::Array(values))
}

// Encodes a LargeBinary Arrow array into a JSON array
pub fn encode_large_binary_array(array: ArrayRef) -> Result<JsonValue, ArrowError> {
    let array = array.as_binary::<i64>();
    let mut values = Vec::with_capacity(array.len());

    for i in 0..array.len() {
        values.push(if array.is_null(i) {
            JsonValue::Null
        } else {
            JsonValue::String(base64::engine::general_purpose::STANDARD.encode(array.value(i)))
        });
    }
    Ok(JsonValue::Array(values))
}

// Encodes a Dictionary Arrow array into a JSON array
pub fn encode_dictionary_array<K>(array: ArrayRef) -> Result<JsonValue, ArrowError>
where
    K: datafusion::arrow::datatypes::ArrowDictionaryKeyType,
{
    let array = array.as_dictionary::<K>();
    let keys = array.keys().clone();
    let values = array.values().clone();

    // Encode dictionary keys and values
    let key_values = encode_primitive_array(Arc::new(keys))?;
    let value_values = encode_array(Arc::new(values))?;

    // Build dictionary structure
    let mut result = Map::new();
    result.insert("keys".to_string(), key_values);
    result.insert("values".to_string(), value_values);
    Ok(JsonValue::Object(result))
}

/// Encodes a Timestamp Arrow array into a JSON array
pub fn encode_timestamp_array(array: ArrayRef) -> Result<JsonValue, ArrowError> {
    let data_type = array.data_type();
    let DataType::Timestamp(unit, _) = data_type else {
        return Err(ArrowError::InvalidArgumentError(
            "Expected timestamp array".into(),
        ));
    };

    let mut values = Vec::with_capacity(array.len());

    match unit {
        TimeUnit::Second => {
            let array = array.as_primitive::<TimestampSecondType>();
            for i in 0..array.len() {
                values.push(if array.is_null(i) {
                    JsonValue::Null
                } else {
                    JsonValue::Number(Number::from(array.value(i)))
                });
            }
        }
        TimeUnit::Millisecond => {
            let array = array.as_primitive::<TimestampMillisecondType>();
            for i in 0..array.len() {
                values.push(if array.is_null(i) {
                    JsonValue::Null
                } else {
                    JsonValue::Number(Number::from(array.value(i)))
                });
            }
        }
        TimeUnit::Microsecond => {
            let array = array.as_primitive::<TimestampMicrosecondType>();
            for i in 0..array.len() {
                values.push(if array.is_null(i) {
                    JsonValue::Null
                } else {
                    JsonValue::Number(Number::from(array.value(i)))
                });
            }
        }
        TimeUnit::Nanosecond => {
            let array = array.as_primitive::<TimestampNanosecondType>();
            for i in 0..array.len() {
                values.push(if array.is_null(i) {
                    JsonValue::Null
                } else {
                    JsonValue::Number(Number::from(array.value(i)))
                });
            }
        }
    }
    Ok(JsonValue::Array(values))
}

/// Encodes a Date32 Arrow array into a JSON array
pub fn encode_date32_array(array: ArrayRef) -> Result<JsonValue, ArrowError> {
    let array = array.as_primitive::<Date32Type>();
    let mut values = Vec::with_capacity(array.len());

    for i in 0..array.len() {
        values.push(if array.is_null(i) {
            JsonValue::Null
        } else {
            JsonValue::Number(Number::from(i64::from(array.value(i))))
        });
    }
    Ok(JsonValue::Array(values))
}

/// Encodes a Date64 Arrow array into a JSON array
pub fn encode_date64_array(array: ArrayRef) -> Result<JsonValue, ArrowError> {
    let array = array.as_primitive::<Date64Type>();
    let mut values = Vec::with_capacity(array.len());

    for i in 0..array.len() {
        values.push(if array.is_null(i) {
            JsonValue::Null
        } else {
            JsonValue::Number(Number::from(array.value(i)))
        });
    }
    Ok(JsonValue::Array(values))
}

/// Encodes a Time32 Arrow array into a JSON array
pub fn encode_time32_array(array: ArrayRef) -> Result<JsonValue, ArrowError> {
    let mut values = Vec::with_capacity(array.len());

    match array.data_type() {
        DataType::Time32(unit) => match unit {
            TimeUnit::Second => {
                let array = array.as_primitive::<Time32SecondType>();
                for i in 0..array.len() {
                    values.push(if array.is_null(i) {
                        JsonValue::Null
                    } else {
                        JsonValue::Number(Number::from(i64::from(array.value(i))))
                    });
                }
            }
            TimeUnit::Millisecond => {
                let array = array.as_primitive::<Time32MillisecondType>();
                for i in 0..array.len() {
                    values.push(if array.is_null(i) {
                        JsonValue::Null
                    } else {
                        JsonValue::Number(Number::from(i64::from(array.value(i))))
                    });
                }
            }
            _ => {
                arrow_error::ArraysSupportSecondAndMillisecondUnitsSnafu {
                    data_type: "Time32",
                    unit: *unit,
                }
                .fail()?;
            }
        },
        _ => {
            arrow_error::ExpectedArrayOfTypeSnafu {
                data_type: "Time32",
                actual_type: array.data_type().clone(),
            }
            .fail()?;
        }
    }
    Ok(JsonValue::Array(values))
}

/// Encodes a Time64 Arrow array into a JSON array
pub fn encode_time64_array(array: ArrayRef) -> Result<JsonValue, ArrowError> {
    let mut values = Vec::with_capacity(array.len());

    match array.data_type() {
        DataType::Time64(unit) => match unit {
            TimeUnit::Microsecond => {
                let array = array.as_primitive::<Time64MicrosecondType>();
                for i in 0..array.len() {
                    values.push(if array.is_null(i) {
                        JsonValue::Null
                    } else {
                        JsonValue::Number(Number::from(array.value(i)))
                    });
                }
            }
            TimeUnit::Nanosecond => {
                let array = array.as_primitive::<Time64NanosecondType>();
                for i in 0..array.len() {
                    values.push(if array.is_null(i) {
                        JsonValue::Null
                    } else {
                        JsonValue::Number(Number::from(array.value(i)))
                    });
                }
            }
            _ => {
                arrow_error::ArraysSupportSecondAndMillisecondUnitsSnafu {
                    data_type: "Time64",
                    unit: *unit,
                }
                .fail()?;
            }
        },
        _ => {
            arrow_error::ExpectedArrayOfTypeSnafu {
                data_type: "Time64",
                actual_type: array.data_type().clone(),
            }
            .fail()?;
        }
    }
    Ok(JsonValue::Array(values))
}

/// Encodes a Duration Arrow array into a JSON array
pub fn encode_duration_array(array: ArrayRef) -> Result<JsonValue, ArrowError> {
    let mut values = Vec::with_capacity(array.len());

    match array.data_type() {
        DataType::Duration(unit) => match unit {
            TimeUnit::Second => {
                let array = array.as_primitive::<DurationSecondType>();
                for i in 0..array.len() {
                    values.push(if array.is_null(i) {
                        JsonValue::Null
                    } else {
                        JsonValue::Number(Number::from(array.value(i)))
                    });
                }
            }
            TimeUnit::Millisecond => {
                let array = array.as_primitive::<DurationMillisecondType>();
                for i in 0..array.len() {
                    values.push(if array.is_null(i) {
                        JsonValue::Null
                    } else {
                        JsonValue::Number(Number::from(array.value(i)))
                    });
                }
            }
            TimeUnit::Microsecond => {
                let array = array.as_primitive::<DurationMicrosecondType>();
                for i in 0..array.len() {
                    values.push(if array.is_null(i) {
                        JsonValue::Null
                    } else {
                        JsonValue::Number(Number::from(array.value(i)))
                    });
                }
            }
            TimeUnit::Nanosecond => {
                let array = array.as_primitive::<DurationNanosecondType>();
                for i in 0..array.len() {
                    values.push(if array.is_null(i) {
                        JsonValue::Null
                    } else {
                        JsonValue::Number(Number::from(array.value(i)))
                    });
                }
            }
        },
        _ => {
            arrow_error::ExpectedArrayOfTypeSnafu {
                data_type: "Duration",
                actual_type: array.data_type().clone(),
            }
            .fail()?;
        }
    }
    Ok(JsonValue::Array(values))
}

/// Encodes a primitive Arrow array into a JSON array by dispatching to the appropriate encoder function
pub fn encode_primitive_array(array: ArrayRef) -> Result<JsonValue, ArrowError> {
    match array.data_type() {
        DataType::Int8 => encode_signed_array::<Int8Type>(array),
        DataType::Int16 => encode_signed_array::<Int16Type>(array),
        DataType::Int32 => encode_signed_array::<Int32Type>(array),
        DataType::Int64 => encode_signed_array::<Int64Type>(array),
        DataType::UInt8 => encode_unsigned_array::<UInt8Type>(array),
        DataType::UInt16 => encode_unsigned_array::<UInt16Type>(array),
        DataType::UInt32 => encode_unsigned_array::<UInt32Type>(array),
        DataType::UInt64 => encode_unsigned_array::<UInt64Type>(array),
        DataType::Float16 => encode_float16_array(array),
        DataType::Float32 => encode_float32_array(array),
        DataType::Float64 => encode_float64_array(array),
        DataType::Timestamp(_, _) => encode_timestamp_array(array),
        DataType::Date32 => encode_date32_array(array),
        DataType::Date64 => encode_date64_array(array),
        DataType::Time32(_) => encode_time32_array(array),
        DataType::Time64(_) => encode_time64_array(array),
        DataType::Duration(_) => encode_duration_array(array),
        _ => arrow_error::UnsupportedPrimitiveTypeSnafu {
            data_type: array.data_type().clone(),
        }
        .fail()?,
    }
}

/// Encodes an Interval Arrow array into a JSON array
pub fn encode_interval_array(array: ArrayRef) -> Result<JsonValue, ArrowError> {
    let mut values = Vec::with_capacity(array.len());

    match array.data_type() {
        DataType::Interval(unit) => match unit {
            IntervalUnit::YearMonth => {
                let array = array.as_primitive::<IntervalYearMonthType>();
                for i in 0..array.len() {
                    values.push(if array.is_null(i) {
                        JsonValue::Null
                    } else {
                        let value = array.value(i);
                        let mut obj = Map::new();
                        obj.insert(
                            "type".to_string(),
                            JsonValue::String("interval".to_string()),
                        );
                        obj.insert(
                            "unit".to_string(),
                            JsonValue::String("YearMonth".to_string()),
                        );
                        obj.insert("value".to_string(), JsonValue::String(value.to_string()));
                        JsonValue::Object(obj)
                    });
                }
            }
            IntervalUnit::DayTime => {
                let array = array.as_primitive::<IntervalDayTimeType>();
                for i in 0..array.len() {
                    values.push(if array.is_null(i) {
                        JsonValue::Null
                    } else {
                        let value = array.value(i);
                        let mut obj = Map::new();
                        obj.insert(
                            "type".to_string(),
                            JsonValue::String("interval".to_string()),
                        );
                        obj.insert("unit".to_string(), JsonValue::String("DayTime".to_string()));
                        obj.insert(
                            "days".to_string(),
                            JsonValue::String(value.days.to_string()),
                        );
                        obj.insert(
                            "milliseconds".to_string(),
                            JsonValue::String(value.milliseconds.to_string()),
                        );
                        JsonValue::Object(obj)
                    });
                }
            }
            IntervalUnit::MonthDayNano => {
                let array = array.as_primitive::<IntervalMonthDayNanoType>();
                for i in 0..array.len() {
                    values.push(if array.is_null(i) {
                        JsonValue::Null
                    } else {
                        let value = array.value(i);
                        let mut obj = Map::new();
                        obj.insert(
                            "type".to_string(),
                            JsonValue::String("interval".to_string()),
                        );
                        obj.insert(
                            "unit".to_string(),
                            JsonValue::String("MonthDayNano".to_string()),
                        );
                        obj.insert(
                            "months".to_string(),
                            JsonValue::String(value.months.to_string()),
                        );
                        obj.insert(
                            "days".to_string(),
                            JsonValue::String(value.days.to_string()),
                        );
                        obj.insert(
                            "nanoseconds".to_string(),
                            JsonValue::String(value.nanoseconds.to_string()),
                        );
                        JsonValue::Object(obj)
                    });
                }
            }
        },
        _ => {
            arrow_error::ExpectedArrayOfTypeSnafu {
                data_type: "Interval",
                actual_type: array.data_type().clone(),
            }
            .fail()?;
        }
    }
    Ok(JsonValue::Array(values))
}

// Encodes a List Arrow array into a JSON array
pub fn encode_list_array(array: ArrayRef) -> Result<JsonValue, ArrowError> {
    let array = array.as_list::<i32>();
    let mut values = Vec::with_capacity(array.len());

    for i in 0..array.len() {
        values.push(if array.is_null(i) {
            JsonValue::Null
        } else {
            let list = array.value(i);
            encode_array(Arc::new(list))?
        });
    }
    Ok(JsonValue::Array(values))
}

// Encodes a ListView Arrow array into a JSON array
pub fn encode_list_view_array(array: ArrayRef) -> Result<JsonValue, ArrowError> {
    let array = array.as_list_view::<i32>();
    let mut values = Vec::with_capacity(array.len());

    for i in 0..array.len() {
        values.push(if array.is_null(i) {
            JsonValue::Null
        } else {
            let list = array.value(i);
            encode_array(Arc::new(list))?
        });
    }
    Ok(JsonValue::Array(values))
}

// Encodes a FixedSizeList Arrow array into a JSON array
pub fn encode_fixed_size_list_array(array: ArrayRef) -> Result<JsonValue, ArrowError> {
    let array = array.as_fixed_size_list();
    let mut values = Vec::with_capacity(array.len());

    for i in 0..array.len() {
        values.push(if array.is_null(i) {
            JsonValue::Null
        } else {
            let list = array.value(i);
            encode_array(Arc::new(list))?
        });
    }
    Ok(JsonValue::Array(values))
}

// Encodes a LargeList Arrow array into a JSON array
pub fn encode_large_list_array(array: ArrayRef) -> Result<JsonValue, ArrowError> {
    let array = array.as_list::<i64>();
    let mut values = Vec::with_capacity(array.len());

    for i in 0..array.len() {
        values.push(if array.is_null(i) {
            JsonValue::Null
        } else {
            let list = array.value(i);
            encode_array(Arc::new(list))?
        });
    }
    Ok(JsonValue::Array(values))
}

// Encodes a Struct Arrow array into a JSON array
pub fn encode_struct_array(array: ArrayRef) -> Result<JsonValue, ArrowError> {
    let array = array.as_struct();
    let mut values = Vec::with_capacity(array.len());

    let encoded_arrays = array
        .columns()
        .iter()
        .map(|column| encode_array(column.clone()))
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .zip(array.fields())
        .collect::<Vec<_>>();

    for i in 0..array.len() {
        values.push(if array.is_null(i) {
            JsonValue::Null
        } else {
            let mut struct_value = Map::new();

            #[allow(clippy::unwrap_used)]
            for (encoded_array, field) in &encoded_arrays {
                let encoded_value = encoded_array.get(i).unwrap();
                struct_value.insert(field.name().to_string(), encoded_value.clone());
            }
            JsonValue::Object(struct_value)
        });
    }
    Ok(JsonValue::Array(values))
}

/// Encodes a Decimal128 Arrow array into a JSON array
pub fn encode_decimal128_array(array: ArrayRef) -> Result<JsonValue, ArrowError> {
    // JSON parser can only handle numeric values as floats or integers.
    // Therefore, we convert Decimal128 to Float64 to ensure compatibility
    // with the JSON encoding logic.
    let array = cast(&array, &DataType::Float64)?;
    encode_float64_array(array)
}

/// Encodes a Decimal256 Arrow array into a JSON array
pub fn encode_decimal256_array(array: ArrayRef) -> Result<JsonValue, ArrowError> {
    let array = array.as_primitive::<Decimal256Type>();
    let mut values = Vec::with_capacity(array.len());

    for i in 0..array.len() {
        values.push(if array.is_null(i) {
            JsonValue::Null
        } else {
            let v = array.value(i);
            let (low, high) = v.to_parts();
            JsonValue::Array(vec![
                JsonValue::String(low.to_string()),
                JsonValue::String(high.to_string()),
            ])
        });
    }
    Ok(JsonValue::Array(values))
}

// Encodes a FixedSizeBinary Arrow array into a JSON array
pub fn encode_fixed_size_binary_array(array: ArrayRef) -> Result<JsonValue, ArrowError> {
    let array = array.as_fixed_size_binary();
    let mut values = Vec::with_capacity(array.len());

    for i in 0..array.len() {
        values.push(if array.is_null(i) {
            JsonValue::Null
        } else {
            JsonValue::String(base64::engine::general_purpose::STANDARD.encode(array.value(i)))
        });
    }
    Ok(JsonValue::Array(values))
}

// Encodes a BinaryView Arrow array into a JSON array
pub fn encode_binary_view_array(array: ArrayRef) -> Result<JsonValue, ArrowError> {
    let array = array.as_binary_view();
    let mut values = Vec::with_capacity(array.len());

    for i in 0..array.len() {
        values.push(if array.is_null(i) {
            JsonValue::Null
        } else {
            JsonValue::String(base64::engine::general_purpose::STANDARD.encode(array.value(i)))
        });
    }
    Ok(JsonValue::Array(values))
}

// Encodes a Utf8View Arrow array into a JSON array
pub fn encode_utf8_view_array(array: ArrayRef) -> Result<JsonValue, ArrowError> {
    let array = array
        .as_ref()
        .as_any()
        .downcast_ref::<StringViewArray>()
        .ok_or_else(|| ArrowError::InvalidArgumentError("Expected utf8 view array".into()))?;
    let mut values = Vec::with_capacity(array.len());

    for i in 0..array.len() {
        values.push(if array.is_null(i) {
            JsonValue::Null
        } else {
            JsonValue::String(array.value(i).to_string())
        });
    }
    Ok(JsonValue::Array(values))
}

// Encodes a RunEndEncoded Arrow array into a JSON array
pub fn encode_run_end_encoded_array(array: ArrayRef) -> Result<JsonValue, ArrowError> {
    let mut values = Vec::with_capacity(array.len());

    for i in 0..array.len() {
        values.push(if array.is_null(i) {
            JsonValue::Null
        } else {
            let mut encoded_value = Map::new();
            encoded_value.insert(
                "value".to_string(),
                encode_array(Arc::new(array.slice(i, 1)))?,
            );
            JsonValue::Object(encoded_value)
        });
    }
    Ok(JsonValue::Array(values))
}

// Encodes a Map Arrow array into a JSON array
pub fn encode_map_array(array: ArrayRef) -> Result<JsonValue, ArrowError> {
    let mut values = Vec::with_capacity(array.len());

    for i in 0..array.len() {
        values.push(if array.is_null(i) {
            JsonValue::Null
        } else {
            let map = array.slice(i, 1);
            let mut map_value = Vec::new();
            let mut entry = Map::new();
            entry.insert("entries".to_string(), encode_array(Arc::new(map))?);
            map_value.push(JsonValue::Object(entry));
            JsonValue::Array(map_value)
        });
    }
    Ok(JsonValue::Array(values))
}

// Encodes a Union Arrow array into a JSON array
pub fn encode_union_array(array: ArrayRef) -> Result<JsonValue, ArrowError> {
    let array = array.as_union();
    let mut values = Vec::with_capacity(array.len());

    for i in 0..array.len() {
        values.push(if array.is_null(i) {
            JsonValue::Null
        } else {
            let mut union_value = Map::new();
            let type_id = array.type_id(i);
            union_value.insert(
                "type_id".to_string(),
                JsonValue::Number(Number::from(type_id)),
            );
            union_value.insert(
                "value".to_string(),
                encode_array(Arc::new(array.child(type_id).slice(i, 1)))?,
            );
            JsonValue::Object(union_value)
        });
    }
    Ok(JsonValue::Array(values))
}

/// Encodes a Null Arrow array into a JSON array
pub fn encode_null_array(array: ArrayRef) -> Result<JsonValue, ArrowError> {
    let array = array
        .as_ref()
        .as_any()
        .downcast_ref::<NullArray>()
        .ok_or_else(|| ArrowError::InvalidArgumentError("Expected null array".into()))?;
    let mut values = Vec::with_capacity(array.len());

    for _ in 0..array.len() {
        values.push(JsonValue::Null);
    }
    Ok(JsonValue::Array(values))
}

/// Encodes any Arrow array into a JSON array based on its data type
pub fn encode_array(array: ArrayRef) -> Result<JsonValue, ArrowError> {
    match array.as_ref().data_type() {
        DataType::Null => encode_null_array(array),
        DataType::Boolean => encode_boolean_array(array),
        DataType::Int8 => encode_signed_array::<Int8Type>(array),
        DataType::Int16 => encode_signed_array::<Int16Type>(array),
        DataType::Int32 => encode_signed_array::<Int32Type>(array),
        DataType::Int64 => encode_signed_array::<Int64Type>(array),
        DataType::UInt8 => encode_unsigned_array::<UInt8Type>(array),
        DataType::UInt16 => encode_unsigned_array::<UInt16Type>(array),
        DataType::UInt32 => encode_unsigned_array::<UInt32Type>(array),
        DataType::UInt64 => encode_unsigned_array::<UInt64Type>(array),
        DataType::Float16 => encode_float16_array(array),
        DataType::Float32 => encode_float32_array(array),
        DataType::Float64 => encode_float64_array(array),
        DataType::Utf8 => encode_utf8_array(array),
        DataType::LargeUtf8 => encode_large_utf8_array(array),
        DataType::Binary => encode_binary_array(array),
        DataType::LargeBinary => encode_large_binary_array(array),
        DataType::Dictionary(key_type, _) => match key_type.as_ref() {
            DataType::Int8 => encode_dictionary_array::<Int8Type>(array),
            DataType::Int16 => encode_dictionary_array::<Int16Type>(array),
            DataType::Int32 => encode_dictionary_array::<Int32Type>(array),
            DataType::Int64 => encode_dictionary_array::<Int64Type>(array),
            DataType::UInt8 => encode_dictionary_array::<UInt8Type>(array),
            DataType::UInt16 => encode_dictionary_array::<UInt16Type>(array),
            DataType::UInt32 => encode_dictionary_array::<UInt32Type>(array),
            DataType::UInt64 => encode_dictionary_array::<UInt64Type>(array),
            _ => Err(ArrowError::InvalidArgumentError(
                "Unsupported dictionary key type".into(),
            )),
        },
        DataType::Timestamp(_, _) => encode_timestamp_array(array),
        DataType::Date32 => encode_date32_array(array),
        DataType::Date64 => encode_date64_array(array),
        DataType::Time32(_) => encode_time32_array(array),
        DataType::Time64(_) => encode_time64_array(array),
        DataType::Duration(_) => encode_duration_array(array),
        DataType::Interval(_) => encode_interval_array(array),
        DataType::List(_) => encode_list_array(array),
        DataType::ListView(_) => encode_list_view_array(array),
        DataType::FixedSizeList(_, _) => encode_fixed_size_list_array(array),
        DataType::LargeList(_) | DataType::LargeListView(_) => encode_large_list_array(array),
        DataType::Struct(_) => encode_struct_array(array),
        DataType::Decimal32(_, _) | DataType::Decimal64(_, _) | DataType::Decimal128(_, _) => {
            encode_decimal128_array(array)
        }
        DataType::Decimal256(_, _) => encode_decimal256_array(array),
        DataType::FixedSizeBinary(_) => encode_fixed_size_binary_array(array),
        DataType::BinaryView => encode_binary_view_array(array),
        DataType::Utf8View => encode_utf8_view_array(array),
        DataType::RunEndEncoded(_, _) => encode_run_end_encoded_array(array),
        DataType::Map(_, _) => encode_map_array(array),
        DataType::Union(_, _) => encode_union_array(array),
    }
}

pub fn encode_scalar(scalar: &ScalarValue) -> Result<JsonValue, ArrowError> {
    let scalar_array = scalar.to_array_of_size(1)?;
    let encoded_array = encode_array(Arc::new(scalar_array))?;
    if let JsonValue::Array(mut array) = encoded_array {
        Ok(array.swap_remove(0))
    } else {
        Err(ArrowError::InvalidArgumentError("Expected array".into()))
    }
}

// Encodes a ColumnarValue into a JSON value
#[allow(dead_code)]
pub fn encode_columnar_value(value: &ColumnarValue) -> Result<JsonValue, ArrowError> {
    match value {
        ColumnarValue::Array(array) => encode_array(array.clone()),
        ColumnarValue::Scalar(scalar) => encode_scalar(scalar),
    }
}

#[derive(Debug, Clone)]
pub enum PathToken {
    Key(String),
    Index(usize),
}

pub fn get_json_value<'a>(value: &'a Value, tokens: &[PathToken]) -> Option<&'a Value> {
    let mut current = value;

    for token in tokens {
        match token {
            PathToken::Key(k) => {
                current = current.get(k)?;
            }
            PathToken::Index(i) => {
                current = current.get(i)?;
            }
        }
    }

    Some(current)
}

#[allow(clippy::while_let_on_iterator)]
pub fn tokenize_path(path: &str) -> Option<Vec<PathToken>> {
    let mut tokens = Vec::new();
    let mut chars = path.chars().peekable();

    while let Some(&ch) = chars.peek() {
        match ch {
            '.' | ':' => {
                chars.next();
            }

            '"' => {
                chars.next();
                let mut key = String::new();
                while let Some(&c) = chars.peek() {
                    if c == '"' {
                        chars.next();
                        break;
                    }
                    key.push(c);
                    chars.next();
                }
                tokens.push(Key(key));
            }

            '[' => {
                chars.next(); // '['
                let mut num = String::new();
                while let Some(&c) = chars.peek() {
                    if c == ']' {
                        chars.next(); // ']'
                        break;
                    }
                    num.push(c);
                    chars.next();
                }
                if let Ok(index) = num.parse::<usize>() {
                    tokens.push(Index(index));
                } else {
                    return None;
                }
            }

            _ => {
                let mut key = String::new();
                while let Some(&c) = chars.peek() {
                    if c == '.' || c == ':' || c == '[' {
                        break;
                    }
                    key.push(c);
                    chars.next();
                }
                if !key.is_empty() {
                    tokens.push(Key(key));
                }
            }
        }
    }

    Some(tokens)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use datafusion::arrow::array::*;
    use datafusion::arrow::datatypes::{Field, i256};
    use datafusion::arrow::datatypes::{IntervalDayTime, IntervalMonthDayNano};

    #[test]
    fn test_boolean_array() {
        let array = BooleanArray::from(vec![Some(true), None, Some(false)]);
        let json = encode_array(Arc::new(array)).unwrap();

        let expected = JsonValue::Array(vec![
            JsonValue::Bool(true),
            JsonValue::Null,
            JsonValue::Bool(false),
        ]);
        assert_eq!(json, expected);
    }

    #[test]
    fn test_primitive_signed_arrays() {
        // Int8
        let array = Int8Array::from(vec![Some(1), None, Some(-2)]);
        let json = encode_array(Arc::new(array)).unwrap();
        let expected = JsonValue::Array(vec![
            JsonValue::Number(Number::from(1)),
            JsonValue::Null,
            JsonValue::Number(Number::from(-2)),
        ]);
        assert_eq!(json, expected);

        // Int16
        let array = Int16Array::from(vec![Some(1), None, Some(-2)]);
        let json = encode_array(Arc::new(array)).unwrap();
        assert_eq!(json, expected);

        // Int32
        let array = Int32Array::from(vec![Some(1), None, Some(-2)]);
        let json = encode_array(Arc::new(array)).unwrap();
        assert_eq!(json, expected);

        // Int64
        let array = Int64Array::from(vec![Some(1), None, Some(-2)]);
        let json = encode_array(Arc::new(array)).unwrap();
        assert_eq!(json, expected);
    }

    #[test]
    fn test_primitive_unsigned_arrays() {
        let expected = JsonValue::Array(vec![
            JsonValue::Number(Number::from(1u64)),
            JsonValue::Null,
            JsonValue::Number(Number::from(2u64)),
        ]);

        // UInt8
        let array = UInt8Array::from(vec![Some(1), None, Some(2)]);
        let json = encode_array(Arc::new(array)).unwrap();
        assert_eq!(json, expected);

        // UInt16
        let array = UInt16Array::from(vec![Some(1), None, Some(2)]);
        let json = encode_array(Arc::new(array)).unwrap();
        assert_eq!(json, expected);

        // UInt32
        let array = UInt32Array::from(vec![Some(1), None, Some(2)]);
        let json = encode_array(Arc::new(array)).unwrap();
        assert_eq!(json, expected);

        // UInt64
        let array = UInt64Array::from(vec![Some(1), None, Some(2)]);
        let json = encode_array(Arc::new(array)).unwrap();
        assert_eq!(json, expected);
    }

    #[test]
    fn test_float_arrays() {
        let expected = JsonValue::Array(vec![
            JsonValue::Number(Number::from_f64(1.5).unwrap()),
            JsonValue::Null,
            JsonValue::Number(Number::from_f64(-2.5).unwrap()),
        ]);

        // Float32
        let array = Float32Array::from(vec![Some(1.5), None, Some(-2.5)]);
        let json = encode_array(Arc::new(array)).unwrap();
        assert_eq!(json, expected);

        // Float64
        let array = Float64Array::from(vec![Some(1.5), None, Some(-2.5)]);
        let json = encode_array(Arc::new(array)).unwrap();
        assert_eq!(json, expected);
    }

    #[test]
    fn test_string_arrays() {
        let expected = JsonValue::Array(vec![
            JsonValue::String("hello".to_string()),
            JsonValue::Null,
            JsonValue::String("world".to_string()),
        ]);

        // Utf8
        let array = StringArray::from(vec![Some("hello"), None, Some("world")]);
        let json = encode_array(Arc::new(array)).unwrap();
        assert_eq!(json, expected);

        // LargeUtf8
        let array = LargeStringArray::from(vec![Some("hello"), None, Some("world")]);
        let json = encode_array(Arc::new(array)).unwrap();
        assert_eq!(json, expected);
    }

    #[test]
    fn test_binary_arrays() {
        let data = vec![1u8, 2u8, 3u8];
        let expected = JsonValue::Array(vec![
            JsonValue::String(base64::engine::general_purpose::STANDARD.encode(&data)),
            JsonValue::Null,
            JsonValue::String(base64::engine::general_purpose::STANDARD.encode(&data)),
        ]);

        // Binary
        let array = BinaryArray::from(vec![Some(data.as_slice()), None, Some(data.as_slice())]);
        let json = encode_array(Arc::new(array)).unwrap();
        assert_eq!(json, expected);

        // LargeBinary
        let array =
            LargeBinaryArray::from(vec![Some(data.as_slice()), None, Some(data.as_slice())]);
        let json = encode_array(Arc::new(array)).unwrap();
        assert_eq!(json, expected);
    }

    #[test]
    fn test_list_arrays() {
        let mut builder = ListBuilder::new(Int32Builder::new());

        // First list: [1,2,3]
        builder.values().append_value(1);
        builder.values().append_value(2);
        builder.values().append_value(3);
        builder.append(true);

        // Second list: null
        builder.append(false);

        // Third list: [4,5,6]
        builder.values().append_value(4);
        builder.values().append_value(5);
        builder.values().append_value(6);
        builder.append(true);

        let list_array = builder.finish();

        let json = encode_array(Arc::new(list_array)).unwrap();

        let expected = JsonValue::Array(vec![
            JsonValue::Array(vec![
                JsonValue::Number(Number::from(1)),
                JsonValue::Number(Number::from(2)),
                JsonValue::Number(Number::from(3)),
            ]),
            JsonValue::Null,
            JsonValue::Array(vec![
                JsonValue::Number(Number::from(4)),
                JsonValue::Number(Number::from(5)),
                JsonValue::Number(Number::from(6)),
            ]),
        ]);
        assert_eq!(json, expected);
    }

    #[test]
    #[allow(clippy::as_conversions)]
    fn test_struct_array() {
        let boolean = Arc::new(BooleanArray::from(vec![false, false, true, true]));
        let int = Arc::new(Int32Array::from(vec![42, 28, 19, 31]));

        let struct_array = StructArray::from(vec![
            (
                Arc::new(Field::new("b", DataType::Boolean, false)),
                boolean as ArrayRef,
            ),
            (
                Arc::new(Field::new("c", DataType::Int32, false)),
                int as ArrayRef,
            ),
        ]);

        let json = encode_array(Arc::new(struct_array)).unwrap();

        let mut obj1 = Map::new();
        obj1.insert("b".to_string(), JsonValue::Bool(false));
        obj1.insert("c".to_string(), JsonValue::Number(Number::from(42)));

        let mut obj2 = Map::new();
        obj2.insert("b".to_string(), JsonValue::Bool(false));
        obj2.insert("c".to_string(), JsonValue::Number(Number::from(28)));

        let mut obj3 = Map::new();
        obj3.insert("b".to_string(), JsonValue::Bool(true));
        obj3.insert("c".to_string(), JsonValue::Number(Number::from(19)));

        let mut obj4 = Map::new();
        obj4.insert("b".to_string(), JsonValue::Bool(true));
        obj4.insert("c".to_string(), JsonValue::Number(Number::from(31)));

        let expected = JsonValue::Array(vec![
            JsonValue::Object(obj1),
            JsonValue::Object(obj2),
            JsonValue::Object(obj3),
            JsonValue::Object(obj4),
        ]);
        assert_eq!(json, expected);
    }

    #[test]
    fn test_null_array() {
        let array = NullArray::new(3);
        let json = encode_array(Arc::new(array)).unwrap();

        let expected = JsonValue::Array(vec![JsonValue::Null, JsonValue::Null, JsonValue::Null]);
        assert_eq!(json, expected);
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn test_interval_arrays() {
        // Test YearMonth interval
        let array = IntervalYearMonthArray::from(vec![Some(12), None, Some(-24)]);
        let json = encode_array(Arc::new(array)).unwrap();

        let mut obj1 = Map::new();
        obj1.insert(
            "type".to_string(),
            JsonValue::String("interval".to_string()),
        );
        obj1.insert(
            "unit".to_string(),
            JsonValue::String("YearMonth".to_string()),
        );
        obj1.insert("value".to_string(), JsonValue::String("12".to_string()));

        let mut obj2 = Map::new();
        obj2.insert(
            "type".to_string(),
            JsonValue::String("interval".to_string()),
        );
        obj2.insert(
            "unit".to_string(),
            JsonValue::String("YearMonth".to_string()),
        );
        obj2.insert("value".to_string(), JsonValue::String("0".to_string()));

        let mut obj3 = Map::new();
        obj3.insert(
            "type".to_string(),
            JsonValue::String("interval".to_string()),
        );
        obj3.insert(
            "unit".to_string(),
            JsonValue::String("YearMonth".to_string()),
        );
        obj3.insert("value".to_string(), JsonValue::String("-24".to_string()));

        let expected = JsonValue::Array(vec![
            JsonValue::Object(obj1),
            JsonValue::Null,
            JsonValue::Object(obj3),
        ]);
        assert_eq!(json, expected);

        // Test DayTime interval
        let array = IntervalDayTimeArray::from(vec![
            Some(IntervalDayTime::new(1, 0)),
            None,
            Some(IntervalDayTime::new(-2, 0)),
        ]);
        let json = encode_array(Arc::new(array)).unwrap();

        let mut obj1 = Map::new();
        obj1.insert(
            "type".to_string(),
            JsonValue::String("interval".to_string()),
        );
        obj1.insert("unit".to_string(), JsonValue::String("DayTime".to_string()));
        obj1.insert("days".to_string(), JsonValue::String("1".to_string()));
        obj1.insert(
            "milliseconds".to_string(),
            JsonValue::String("0".to_string()),
        );

        let mut obj3 = Map::new();
        obj3.insert(
            "type".to_string(),
            JsonValue::String("interval".to_string()),
        );
        obj3.insert("unit".to_string(), JsonValue::String("DayTime".to_string()));
        obj3.insert("days".to_string(), JsonValue::String("-2".to_string()));
        obj3.insert(
            "milliseconds".to_string(),
            JsonValue::String("0".to_string()),
        );

        let expected = JsonValue::Array(vec![
            JsonValue::Object(obj1),
            JsonValue::Null,
            JsonValue::Object(obj3),
        ]);
        assert_eq!(json, expected);

        // Test MonthDayNano interval
        let array = IntervalMonthDayNanoArray::from(vec![
            Some(IntervalMonthDayNano::new(1, 0, 0)),
            None,
            Some(IntervalMonthDayNano::new(-2, 0, 0)),
        ]);
        let json = encode_array(Arc::new(array)).unwrap();

        let mut obj1 = Map::new();
        obj1.insert(
            "type".to_string(),
            JsonValue::String("interval".to_string()),
        );
        obj1.insert(
            "unit".to_string(),
            JsonValue::String("MonthDayNano".to_string()),
        );
        obj1.insert("months".to_string(), JsonValue::String("1".to_string()));
        obj1.insert("days".to_string(), JsonValue::String("0".to_string()));
        obj1.insert(
            "nanoseconds".to_string(),
            JsonValue::String("0".to_string()),
        );

        let mut obj3 = Map::new();
        obj3.insert(
            "type".to_string(),
            JsonValue::String("interval".to_string()),
        );
        obj3.insert(
            "unit".to_string(),
            JsonValue::String("MonthDayNano".to_string()),
        );
        obj3.insert("months".to_string(), JsonValue::String("-2".to_string()));
        obj3.insert("days".to_string(), JsonValue::String("0".to_string()));
        obj3.insert(
            "nanoseconds".to_string(),
            JsonValue::String("0".to_string()),
        );

        let expected = JsonValue::Array(vec![
            JsonValue::Object(obj1),
            JsonValue::Null,
            JsonValue::Object(obj3),
        ]);
        assert_eq!(json, expected);
    }

    #[test]
    fn test_decimal256_array() {
        let array = Decimal256Array::from(vec![Some(i256::from(1)), None, Some(i256::from(-2))]);
        let json = encode_array(Arc::new(array)).unwrap();

        let expected = JsonValue::Array(vec![
            JsonValue::Array(vec![
                JsonValue::String("1".to_string()),
                JsonValue::String("0".to_string()),
            ]),
            JsonValue::Null,
            JsonValue::Array(vec![
                JsonValue::String("340282366920938463463374607431768211454".to_string()),
                JsonValue::String("-1".to_string()),
            ]),
        ]);
        assert_eq!(json, expected);
    }
}
