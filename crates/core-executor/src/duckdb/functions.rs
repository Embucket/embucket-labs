use crate::error::{self as ex_error, Result as CoreResult};
use arrow_schema::{DataType, Field, FieldRef, Schema};
use datafusion::arrow::array::Array;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, TypeSignature};
use datafusion::sql::parser::Statement as DFStatement;
use datafusion_common::config::ConfigOptions;
use datafusion_common::internal_datafusion_err;
use datafusion_expr::{ScalarUDF, ScalarUDFImpl};
use duckdb::Connection;
use duckdb::core::DataChunkHandle;
use duckdb::vscalar::{
    ArrowFunctionSignature, ArrowScalarParams, ScalarFunctionSignature, VArrowScalar, VScalar,
};
use duckdb::vtab::arrow::{WritableVector, data_chunk_to_arrow, write_arrow_array_to_vector};
use duckdb::vtab::to_duckdb_logical_type;
use embucket_functions::conditional::{
    booland, boolor, boolxor, equal_null, iff, nullifzero, zeroifnull,
};
use embucket_functions::crypto::md5;
use embucket_functions::datetime::date_part_extract::Interval;
use embucket_functions::datetime::{
    add_months, date_add, date_diff, date_from_parts, date_part_extract, dayname, last_day,
    monthname, next_day, previous_day, time_from_parts, timestamp_from_parts,
};
use embucket_functions::numeric::div0;
use embucket_functions::regexp::{
    regexp_instr, regexp_like, regexp_replace, regexp_substr, regexp_substr_all,
};
use embucket_functions::session::{current_database, current_schema, current_version};
use embucket_functions::string_binary::{
    hex_decode_binary, hex_decode_string, hex_encode, insert, jarowinkler_similarity as js, length,
    lower, parse_ip, randstr, replace, rtrimmed_length, sha2, split, strtok, substr,
};
use embucket_functions::system::{cancel_query, typeof_func};
use snafu::ResultExt;
use sqlparser::ast::{Expr, Ident, ObjectName, Statement, VisitMut, VisitorMut};
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::hash::BuildHasher;
use std::ops::ControlFlow;
use std::{error::Error, sync::Arc};
use strum::IntoEnumIterator;

/// Generic adapter between DF and `DuckDB`
#[derive(Debug, Clone)]
pub struct DfUdfWrapper<T: ScalarUDFImpl + Default>(std::marker::PhantomData<T>);
pub struct SafeVScalar<T: VArrowScalar>(std::marker::PhantomData<T>);

impl<T: VArrowScalar> VScalar for SafeVScalar<T> {
    type State = T::State;

    unsafe fn invoke(
        info: &Self::State,
        input: &mut DataChunkHandle,
        out: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn Error>> {
        let rb = if input.num_columns() == 0 {
            let row_count = input.len().max(1);
            let schema = Arc::new(Schema::empty());
            RecordBatch::try_new_with_options(
                schema,
                vec![],
                &duckdb::arrow::record_batch::RecordBatchOptions::new()
                    .with_row_count(Some(row_count)),
            )?
        } else {
            data_chunk_to_arrow(input)?
        };
        let array = T::invoke(info, rb)?;
        write_arrow_array_to_vector(&array, out)
    }

    #[allow(clippy::expect_used)]
    fn signatures() -> Vec<ScalarFunctionSignature> {
        T::signatures()
            .into_iter()
            .map(|sig: ArrowFunctionSignature| {
                let params = match sig.parameters {
                    Some(ArrowScalarParams::Exact(param_types)) => param_types
                        .into_iter()
                        .map(|dt| {
                            to_duckdb_logical_type(&dt).expect("failed to convert parameter type")
                        })
                        .collect(),
                    Some(ArrowScalarParams::Variadic(param_types)) => {
                        let converted = to_duckdb_logical_type(&param_types)
                            .expect("failed to convert variadic type");
                        vec![converted]
                    }
                    _ => vec![],
                };

                let ret_type = to_duckdb_logical_type(&sig.return_type)
                    .expect("failed to convert return type");

                ScalarFunctionSignature::exact(params, ret_type)
            })
            .collect()
    }
}

/// Stores a specific `ScalarUDF` instance for `invoke()`
#[derive(Clone)]
pub struct UdfState {
    udf: Arc<ScalarUDF>,
}

impl UdfState {
    #[must_use]
    pub const fn new(udf: Arc<ScalarUDF>) -> Self {
        Self { udf }
    }

    #[must_use]
    pub fn udf(&self) -> Arc<ScalarUDF> {
        self.udf.clone()
    }
}

impl Default for UdfState {
    fn default() -> Self {
        // dummy placeholder (will be overridden by register_with_state)
        let fake = length::get_udf();
        Self::new(fake)
    }
}

impl<T: ScalarUDFImpl + Default> VArrowScalar for DfUdfWrapper<T> {
    type State = UdfState;

    fn invoke(state: &Self::State, input: RecordBatch) -> Result<Arc<dyn Array>, Box<dyn Error>> {
        let num_rows = input.num_rows();
        let schema = input.schema();
        let func = state.udf();
        let args: Vec<ColumnarValue> = input
            .columns()
            .iter()
            .map(|col| ColumnarValue::Array(col.clone()))
            .collect();

        let arg_fields: Vec<FieldRef> = schema
            .fields()
            .iter()
            .map(|f| Arc::new(Field::new(f.name(), f.data_type().clone(), f.is_nullable())))
            .collect();

        let input_types: Vec<DataType> = arg_fields.iter().map(|f| f.data_type().clone()).collect();

        let return_field = Arc::new(Field::new(
            func.name(),
            func.return_type(&input_types)?,
            true,
        ));

        let args_struct = ScalarFunctionArgs {
            args,
            arg_fields,
            number_rows: num_rows,
            return_field,
            config_options: Arc::new(ConfigOptions::default()),
        };

        match func.invoke_with_args(args_struct)? {
            ColumnarValue::Array(arr) => Ok(arr),
            ColumnarValue::Scalar(scalar) => {
                let array = scalar.to_array_of_size(num_rows)?;
                Ok(array)
            }
        }
    }

    fn signatures() -> Vec<ArrowFunctionSignature> {
        let func = T::default();
        let sig = func.signature();
        expand_signature(&func, &sig.type_signature)
    }
}

pub fn register_all_udfs<S>(
    conn: &Connection,
    statement: &mut DFStatement,
    udfs: &HashMap<String, Arc<ScalarUDF>, S>,
) -> CoreResult<()>
where
    S: BuildHasher + Clone,
{
    let mut succeeded: HashSet<String, S> = HashSet::with_hasher(udfs.hasher().clone());

    // String binary
    register_duckdb_udf::<hex_decode_binary::HexDecodeBinaryFunc, S>(conn, udfs, &mut succeeded)?;
    register_duckdb_udf_try::<hex_decode_binary::HexDecodeBinaryFunc, S>(
        conn,
        udfs,
        &mut succeeded,
    )?;
    register_duckdb_udf::<hex_decode_string::HexDecodeStringFunc, S>(conn, udfs, &mut succeeded)?;
    register_duckdb_udf_try::<hex_decode_string::HexDecodeStringFunc, S>(
        conn,
        udfs,
        &mut succeeded,
    )?;
    register_duckdb_udf::<hex_encode::HexEncodeFunc, S>(conn, udfs, &mut succeeded)?;
    register_duckdb_udf::<insert::Insert, S>(conn, udfs, &mut succeeded)?;
    register_duckdb_udf::<js::JarowinklerSimilarityFunc, S>(conn, udfs, &mut succeeded)?;
    register_duckdb_udf::<length::LengthFunc, S>(conn, udfs, &mut succeeded)?;
    register_duckdb_udf::<lower::LowerFunc, S>(conn, udfs, &mut succeeded)?;
    register_duckdb_udf::<parse_ip::ParseIpFunc, S>(conn, udfs, &mut succeeded)?;
    register_duckdb_udf::<randstr::RandStrFunc, S>(conn, udfs, &mut succeeded)?;
    register_duckdb_udf::<rtrimmed_length::RTrimmedLengthFunc, S>(conn, udfs, &mut succeeded)?;
    register_duckdb_udf::<sha2::Sha2Func, S>(conn, udfs, &mut succeeded)?;
    register_duckdb_udf::<split::SplitFunc, S>(conn, udfs, &mut succeeded)?;
    register_duckdb_udf::<strtok::StrtokFunc, S>(conn, udfs, &mut succeeded)?;
    register_duckdb_udf::<substr::SubstrFunc, S>(conn, udfs, &mut succeeded)?;
    register_duckdb_udf::<replace::ReplaceFunc, S>(conn, udfs, &mut succeeded)?;

    // Regexp
    register_duckdb_udf::<regexp_instr::RegexpInstrFunc, S>(conn, udfs, &mut succeeded)?;
    register_duckdb_udf::<regexp_like::RegexpLikeFunc, S>(conn, udfs, &mut succeeded)?;
    register_duckdb_udf::<regexp_replace::RegexpReplaceFunc, S>(conn, udfs, &mut succeeded)?;
    register_duckdb_udf::<regexp_substr::RegexpSubstrFunc, S>(conn, udfs, &mut succeeded)?;
    register_duckdb_udf::<regexp_substr_all::RegexpSubstrAllFunc, S>(conn, udfs, &mut succeeded)?;

    // Conditional
    register_duckdb_udf::<booland::BoolAndFunc, S>(conn, udfs, &mut succeeded)?;
    register_duckdb_udf::<boolor::BoolOrFunc, S>(conn, udfs, &mut succeeded)?;
    register_duckdb_udf::<boolxor::BoolXorFunc, S>(conn, udfs, &mut succeeded)?;
    register_duckdb_udf::<equal_null::EqualNullFunc, S>(conn, udfs, &mut succeeded)?;
    register_duckdb_udf::<iff::IffFunc, S>(conn, udfs, &mut succeeded)?;
    register_duckdb_udf::<nullifzero::NullIfZeroFunc, S>(conn, udfs, &mut succeeded)?;
    register_duckdb_udf::<zeroifnull::ZeroIfNullFunc, S>(conn, udfs, &mut succeeded)?;

    // Crypto
    register_duckdb_udf::<md5::Md5Func, S>(conn, udfs, &mut succeeded)?;

    // Datetime
    register_duckdb_udf::<add_months::AddMonthsFunc, S>(conn, udfs, &mut succeeded)?;
    register_duckdb_udf::<date_add::DateAddFunc, S>(conn, udfs, &mut succeeded)?;
    register_duckdb_udf::<date_diff::DateDiffFunc, S>(conn, udfs, &mut succeeded)?;
    register_duckdb_udf::<date_from_parts::DateFromPartsFunc, S>(conn, udfs, &mut succeeded)?;
    register_duckdb_udf::<dayname::DayNameFunc, S>(conn, udfs, &mut succeeded)?;
    register_duckdb_udf::<last_day::LastDayFunc, S>(conn, udfs, &mut succeeded)?;
    register_duckdb_udf::<monthname::MonthNameFunc, S>(conn, udfs, &mut succeeded)?;
    register_duckdb_udf::<next_day::NextDayFunc, S>(conn, udfs, &mut succeeded)?;
    register_duckdb_udf::<previous_day::PreviousDayFunc, S>(conn, udfs, &mut succeeded)?;
    register_duckdb_udf::<time_from_parts::TimeFromPartsFunc, S>(conn, udfs, &mut succeeded)?;
    register_duckdb_udf::<timestamp_from_parts::TimestampFromPartsFunc, S>(
        conn,
        udfs,
        &mut succeeded,
    )?;
    for interval in Interval::iter() {
        register_duckdb_udf_internal::<date_part_extract::DatePartExtractFunc, S>(
            conn,
            udfs,
            &interval.to_string(),
            &mut succeeded,
        )?;
    }

    // Numeric
    register_duckdb_udf_internal::<div0::Div0Func, S>(conn, udfs, "div0null", &mut succeeded)?;
    register_duckdb_udf::<div0::Div0Func, S>(conn, udfs, &mut succeeded)?;

    // System
    register_duckdb_udf::<cancel_query::SystemCancelQuery, S>(conn, udfs, &mut succeeded)?;
    register_duckdb_udf::<typeof_func::SystemTypeofFunc, S>(conn, udfs, &mut succeeded)?;

    // Session
    register_duckdb_udf::<current_database::CurrentDatabase, S>(conn, udfs, &mut succeeded)?;
    register_duckdb_udf::<current_schema::CurrentSchema, S>(conn, udfs, &mut succeeded)?;
    register_duckdb_udf::<current_version::CurrentVersion, S>(conn, udfs, &mut succeeded)?;

    // Rewrite statement functions with duckdb_ prefix
    if let DFStatement::Statement(stmt) = statement {
        visit(stmt, &succeeded);
    }
    Ok(())
}

/// Registers a normal (non-try_) UDF in `DuckDB`.
pub fn register_duckdb_udf<T, S>(
    conn: &Connection,
    udfs: &HashMap<String, Arc<ScalarUDF>, S>,
    succeeded: &mut HashSet<String, S>,
) -> CoreResult<()>
where
    T: ScalarUDFImpl + Default + 'static,
    S: BuildHasher + Clone,
{
    let name = T::default().name().to_string();
    register_duckdb_udf_internal::<T, S>(conn, udfs, &name, succeeded)
}

/// Registers a “try_” variant of a UDF in `DuckDB`.
pub fn register_duckdb_udf_try<T, S>(
    conn: &Connection,
    udfs: &HashMap<String, Arc<ScalarUDF>, S>,
    succeeded: &mut HashSet<String, S>,
) -> CoreResult<()>
where
    T: ScalarUDFImpl + Default + 'static,
    S: BuildHasher + Clone,
{
    let name = format!("try_{}", T::default().name());
    register_duckdb_udf_internal::<T, S>(conn, udfs, &name, succeeded)
}

/// Shared internal logic for both normal and try_ function registration.
/// We register all functions with duckdb_ prefix
fn register_duckdb_udf_internal<T, S>(
    conn: &Connection,
    udfs: &HashMap<String, Arc<ScalarUDF>, S>,
    name: &str,
    succeeded: &mut HashSet<String, S>,
) -> CoreResult<()>
where
    T: ScalarUDFImpl + Default + 'static,
    S: BuildHasher + Clone,
{
    let func = udfs
        .get(name)
        .ok_or_else(|| internal_datafusion_err!("Unable to find expected '{name}' function"))
        .context(ex_error::DataFusionSnafu)?;

    let state = UdfState::new(func.clone());
    let duckdb_name = &format!("duckdb_{}", func.name());
    match conn
        .register_scalar_function_with_state::<SafeVScalar<DfUdfWrapper<T>>>(duckdb_name, &state)
    {
        Ok(()) => {
            succeeded.insert(name.to_string());
        }
        Err(_) => {
            tracing::error!("Failed to register {duckdb_name} func in duckdb");
        }
    }
    Ok(())
}

fn expand_signature<T: ScalarUDFImpl>(
    func: &T,
    sig: &TypeSignature,
) -> Vec<ArrowFunctionSignature> {
    // DataFusion already knows all valid argument type combinations for this signature
    let example_sigs = match sig {
        TypeSignature::Any(arg_count) => {
            let types = vec![DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View];
            types
                .into_iter()
                .map(|dt| vec![dt; *arg_count])
                .collect::<Vec<_>>()
        }
        TypeSignature::VariadicAny => {
            let ret = func
                .return_type(&[DataType::Utf8])
                .unwrap_or(DataType::Utf8);
            return vec![ArrowFunctionSignature::exact(vec![DataType::Utf8], ret)];
        }
        TypeSignature::Nullary => {
            return vec![ArrowFunctionSignature::exact(vec![], DataType::Utf8)];
        }
        _ => sig.get_example_types(),
    };

    // Build a DuckDB signature for each valid argument combination
    example_sigs
        .into_iter()
        .map(|types| {
            let ret = func.return_type(&types).unwrap_or(DataType::Utf8);
            ArrowFunctionSignature::exact(types, ret)
        })
        .collect()
}

/// Rewrites function names to `duckdb_*` equivalents if they were registered in `DuckDB`.
#[derive(Debug)]
pub struct DuckdbFunctionsRewriter<'a, S: BuildHasher> {
    pub duckdb_funcs: &'a HashSet<String, S>,
}

impl<S: BuildHasher> VisitorMut for DuckdbFunctionsRewriter<'_, S> {
    type Break = ();

    fn post_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        if let Expr::Function(func) = expr {
            let func_name_string = func.name.to_string().to_lowercase();

            if self.duckdb_funcs.contains(&func_name_string) {
                let new_name = format!("duckdb_{func_name_string}");
                func.name = ObjectName::from(vec![Ident::new(new_name)]);
            }
        }
        ControlFlow::Continue(())
    }
}

pub fn visit<S>(stmt: &mut Statement, duckdb_funcs: &HashSet<String, S>)
where
    S: BuildHasher,
{
    let _ = stmt.visit(&mut DuckdbFunctionsRewriter { duckdb_funcs });
}
