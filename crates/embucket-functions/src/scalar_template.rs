use std::any::Any;
use std::sync::LazyLock;
use arrow::datatypes::DataType;
use datafusion_common::{plan_err, Result};
use datafusion_expr::{
    ColumnarValue, Documentation, Expr, ScalarFunctionArgs, ScalarUDFImpl, 
    Signature, Volatility, ReturnTypeArgs, ReturnInfo
};
use datafusion_expr::scalar_doc_sections::DOC_SECTION_OTHER; // Change to appropriate section
use datafusion_expr_common::interval_arithmetic::Interval;
use datafusion_expr::sort_properties::{ExprProperties, SortProperties};
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyInfo};

/// Template for implementing a Scalar UDF in Embucket
/// 
/// INSTRUCTIONS FOR IMPLEMENTATION:
/// 1. Replace `YourFunctionName` with your actual function name (PascalCase)
/// 2. Replace `your_function_name` with your function name in snake_case
/// 3. Fill in the signature with appropriate input/output types
/// 4. Implement the core logic in `invoke_with_args`
/// 5. Add/remove optional methods based on your needs (see comments below)
/// 6. Add doc-style comments to the function
/// 7. Remove all instruction comments
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct YourFunctionName {
    signature: Signature,
}

impl YourFunctionName {
    pub fn new() -> Self {
        Self {
            // REQUIRED: Define function signature
            // Common patterns:
            // 1. Signature::any(vec![
            //     Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
            //     Coercion::new_exact(TypeSignatureClass::Native(logical_date())),
            // ])
            // 2.. Signature::uniform(arg_count, vec![DataType::Utf8], Volatility::Immutable)
            // 3.Signature::one_of(vec![
            //     TypeSignature::Exact(vec![DataType::Int64]),
            //     TypeSignature::Exact(vec![DataType::Float64]),
            // ], Volatility::Immutable) 
            // Usage of logical types is preferred over DataType::Utf8, DataType::Int64, etc.
            signature: Signature::uniform(1, logical_string(), Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for YourFunctionName {
    // ========================================
    // REQUIRED METHODS - MUST IMPLEMENT ALL
    // ========================================

    /// REQUIRED: Always implement this exactly as shown
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// REQUIRED: Return the function name (should match SQL usage)
    fn name(&self) -> &str {
        "your_function_name" // Replace with your function name
    }

    /// REQUIRED: Return the function signature
    fn signature(&self) -> &Signature {
        &self.signature
    }

    /// REQUIRED: Define return type based on input types
    /// DELETE this method if you implement `return_type_from_args` instead
    /// IMPLEMENT this for simple cases where return type only depends on input types
    /// IF THIS METHOD RETURNS AN ERROR USE `exec_err!` MACRO.
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        // Example validation:
        if arg_types.len() != 1 {
            return exec_err!("your_function_name requires exactly 1 argument, got {}", arg_types.len());
        }
        
        // Example type checking:
        match arg_types[0] {
            DataType::Utf8 | DataType::LargeUtf8 => Ok(DataType::Utf8),
            _ => exec_err!("your_function_name requires string input, got {:?}", arg_types[0]),
        }
    }

    /// REQUIRED: Core function implementation
    /// This is where your main logic goes
    /// Add tracing for a functions that can be heavy on computation
    #[tracing::instrument(level = "trace", skip(self, args))]

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        // TODO: Implement your function logic here
        // 
        // Common patterns:
        // 1. For simple functions, use ColumnarValue::values_to_arrays()
        // 2. For performance, handle scalar inputs specially
        // 3. Return ColumnarValue::Array() or ColumnarValue::Scalar()
        
        unimplemented!("Implement your function logic here")
        
        // Example structure:
        // let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        // computation logic goes here or
        // let result_array = your_computation_logic(&arrays)?;
        // Ok(ColumnarValue::Array(result_array))
    }

    /// Function simplification/rewrite rules
    /// IMPLEMENT if your function has special optimization rules. E.G. Evaluate result at compile time.
    /// DELETE if no special optimizations needed 
    /// 
    /// This method is used to simplify the function.
    /// 
    /// Example:
    /// 
    /*
    fn simplify(&self, args: Vec<Expr>, info: &dyn SimplifyInfo) -> Result<ExprSimplifyResult> {
        // Example: simplify constant expressions
        // if args.iter().all(|arg| matches!(arg, Expr::Literal(_))) {
        //     // Evaluate at compile time
        //     let result = self.invoke_with_args(...)?;
        //     return Ok(ExprSimplifyResult::Simplified(Expr::Literal(result.into_scalar()?)));
        // }
        Ok(ExprSimplifyResult::Original(args))
    }
    */

    // ========================================
    // COMMONLY USED OPTIONAL METHODS
    // ========================================

    /// OPTIONAL: Use instead of `return_type` for advanced cases
    /// IMPLEMENT if you need to:
    /// - Control nullability of return type
    /// - Return type depends on argument VALUES (not just types)
    /// - Handle complex type inference
    /// DELETE if you implement `return_type` above
    /*
    fn return_type_from_args(&self, args: ReturnTypeArgs) -> Result<ReturnInfo> {
        // Example: return non-nullable result
        let return_type = self.return_type(args.arg_types)?;
        Ok(ReturnInfo::new_non_nullable(return_type))
        
        // Example: type depends on argument value
        // if let Some(scalar_val) = args.scalar_arguments[1] {
        //     match scalar_val {
        //         ScalarValue::Utf8(Some(type_name)) => {
        //             let data_type = parse_type_name(type_name)?;
        //             Ok(ReturnInfo::new_nullable(data_type))
        //         },
        //         _ => plan_err!("Type argument must be a string"),
        //     }
        // } else {
        //     plan_err!("Type argument must be a constant")
        // }
    }
    */

    /// OPTIONAL: Provide alternative names for the function
    /// IMPLEMENT if your function should be callable by multiple names
    /// DELETE if function only has one name
    /*
    fn aliases(&self) -> &[String] {
        // Example: function can be called as "substr" or "substring"
        &["your_alias1".to_string(), "your_alias2".to_string()]
    }
    */

    /// OPTIONAL: Custom display name for query plans
    /// IMPLEMENT if you want custom formatting in EXPLAIN plans
    /// DELETE to use default formatting
    /*
    fn display_name(&self, args: &[Expr]) -> Result<String> {
        // Example: show function with custom formatting
        Ok(format!("CUSTOM_{}({})", self.name().to_uppercase(), 
                  args.iter().map(|e| e.to_string()).collect::<Vec<_>>().join(", ")))
    }
    */

    // ========================================
    // OPTIONAL METHODS
    // ========================================

    /// Whether function short-circuits evaluation
    /// IMPLEMENT if your function may not evaluate all arguments (like AND/OR)
    /// DELETE for normal functions (most cases)
    /*
    fn short_circuits(&self) -> bool {
        // Return true if some arguments might not be evaluated
        // This prevents certain optimizations like common subexpression elimination
        false
    }
    */

    /// Interval arithmetic for bounds checking
    /// IMPLEMENT if your function works with numeric ranges
    /// DELETE for most non-numeric functions
    /*
    fn evaluate_bounds(&self, inputs: &[&Interval]) -> Result<Interval> {
        // Example for ABS function:
        // let input = inputs[0];
        // if input.lower().is_sign_positive() {
        //     Ok(*input) // All positive, no change
        // } else if input.upper().is_sign_negative() {
        //     Ok(Interval::new(-input.upper(), -input.lower())) // All negative, flip
        // } else {
        //     Ok(Interval::new(0, input.upper().abs().max(input.lower().abs()))) // Mixed signs
        // }
        Interval::make_unbounded(&DataType::Null)
    }
    */

    /// Constraint propagation for optimization
    /// IMPLEMENT if your function can propagate constraints backwards
    /// DELETE for most functions
    /*
    fn propagate_constraints(&self, interval: &Interval, inputs: &[&Interval]) -> Result<Option<Vec<Interval>>> {
        // Example: if ABS(x) is in [4,5], then x could be in [-5,-4] or [4,5]
        Ok(Some(vec![]))
    }
    */

    /// Sort order preservation
    /// IMPLEMENT if your function preserves or affects sort order
    /// DELETE for most functions
    /*
    fn output_ordering(&self, inputs: &[ExprProperties]) -> Result<SortProperties> {
        // Example: ABS preserves order for positive inputs
        // if inputs[0].sort_properties.is_ordered() && is_positive_interval(inputs[0]) {
        //     Ok(inputs[0].sort_properties)
        // } else {
        //     Ok(SortProperties::Unordered)
        // }
        Ok(SortProperties::Unordered)
    }
    */

    /// Lexicographical ordering preservation
    /// IMPLEMENT if your function preserves lexicographical order
    /// DELETE for most functions
    /*
    fn preserves_lex_ordering(&self, inputs: &[ExprProperties]) -> Result<bool> {
        // Return true if function preserves the order of inputs
        // Example: UPPER/LOWER preserve string ordering
        Ok(false)
    }
    */

    /// ADVANCED: Custom type coercion
    /// IMPLEMENT only if you use TypeSignature::UserDefined in signature
    /// DELETE for most functions (use standard type signatures instead)
    /*
    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        // Example: coerce all numeric types to Float64
        // Ok(arg_types.iter().map(|_| DataType::Float64).collect())
        not_impl_err!("Function {} does not implement coerce_types", self.name())
    }
    */

    /// ADVANCED: Custom equality comparison
    /// IMPLEMENT only if you need special equality logic
    /// DELETE to use default (compares name and signature)
    /*
    fn equals(&self, other: &dyn ScalarUDFImpl) -> bool {
        // Custom equality logic if needed
        if let Some(other) = other.as_any().downcast_ref::<YourFunctionName>() {
            // Add custom comparison logic here
            true
        } else {
            false
        }
    }
    */

    /// ADVANCED: Custom hash implementation
    /// IMPLEMENT only if you implement custom `equals`
    /// DELETE to use default (hashes name and signature)
    /*
    fn hash_value(&self) -> u64 {
        use std::hash::{DefaultHasher, Hash, Hasher};
        let mut hasher = DefaultHasher::new();
        self.name().hash(&mut hasher);
        self.signature().hash(&mut hasher);
        // Add custom fields to hash if needed
        hasher.finish()
    }
    */
}

/// OPTIONAL: Implement Default if it makes sense for your function
impl Default for YourFunctionName {
    fn default() -> Self {
        Self::new()
    }
}

/// Add snapshot tests for your function to the tests module.
/// The folder name inside tests should be the same as the function folder.
/// Specify the snapshot path in the test_query macro.
/// 
/// Example:
/// test_query!(
///     your_function_name,
///     "SELECT your_function_name(column_name)",
///     "CREATE TABLE your_table (column_name type_name)", // optional
///     snapshot_path = "your_function_name"
/// );
/// 
/// Optionally, Add tests for your function in your function file.
/// 
/// Example:
/// #[cfg(test)]
/// mod tests {
///     use super::*;
///     use datafusion_expr::ScalarUDF;
///     use datafusion_expr::col;
///     #[test]
///     fn test_your_function_name() {
///         // Test basic function creation
///         let udf = YourFunctionName::new();
///         assert_eq!(udf.name(), "your_function_name");
///         
///         // Test ScalarUDF creation
///         let scalar_udf = ScalarUDF::from(udf);
///         let _expr = scalar_udf.call(vec![col("test_column")]);
///         
///         // TODO: Add more specific tests for your function
///     }
/// }
/// 

