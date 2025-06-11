# Embucket Functions Implementation Guide

## Quick Decision Matrix

### ✅ ALWAYS REQUIRED
- `as_any()` - Always implement exactly as shown in template
- `name()` - Function name as it appears in SQL
- `signature()` - Input types and volatility. Preferably using logical types
- `invoke_with_args()` - Core function logic
- `return_type()` OR `return_type_from_args()` - Choose one
- **Registration** - Function must be registered and added to tracking

### 🔄 CHOOSE ONE
| Method | When to Use | When NOT to Use |
|--------|-------------|-----------------|
| `return_type()` | Simple cases where return type only depends on input types | Complex cases with nullability control or value-dependent types |
| `return_type_from_args()` | Need nullability control, type depends on argument values (like CAST) | Simple type mapping |

### 📚 COMMONLY NEEDED
| Method | Implement When | Skip When |
|--------|----------------|-----------|
| `aliases()` | Function has multiple names (e.g., `substr`/`substring`) | Function has only one name |
| `display_name()` | Want custom formatting in query plans | Default formatting is fine |
| `simplify()` | Function has constant-folding or rewrite rules | No special optimizations |

### 🚀 PERFORMANCE & OPTIMIZATION
| Method | Implement When | Skip When |
|--------|----------------|-----------|
| `short_circuits()` | Function may not evaluate all args (like AND/OR) | All arguments always evaluated |
| `evaluate_bounds()` | Numeric function with interval arithmetic | Non-numeric functions |
| `propagate_constraints()` | Function can push constraints to children | No constraint propagation |
| `output_ordering()` | Function affects sort order | Order doesn't matter |
| `preserves_lex_ordering()` | Function preserves string/lexical order | No ordering preservation |

### 🔧 ADVANCED CASES
| Method | Implement When | Skip When |
|--------|----------------|-----------|
| `coerce_types()` | Using `TypeSignature::UserDefined` | Using standard type signatures |
| `equals()` | Custom equality logic needed | Default comparison is sufficient |
| `hash_value()` | Implemented custom `equals()` | Using default equality |

## Common Signature Patterns

```rust
// Approach using logical types (PREFERRED)
// Single string argument
Signature::uniform(1, logical_string(), Volatility::Immutable)

// Multiple type coercions
Signature::one_of(vec![
    Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
    Coercion::new_exact(TypeSignatureClass::Native(logical_date())),
])

// Legacy patterns (still supported)
// Multiple possible signatures
Signature::one_of(vec![
    TypeSignature::Exact(vec![DataType::Int64]),
    TypeSignature::Exact(vec![DataType::Float64]),
], Volatility::Immutable)

// Variable number of args
Signature::variadic_equal(DataType::Float64, Volatility::Immutable)

// Any type accepted
Signature::any(1, Volatility::Immutable)
```

## Logical Types

Use logical types instead of raw DataTypes for better type handling:

```rust
// PREFERRED: Use logical types
logical_string()   // Instead of DataType::Utf8
logical_int()      // Instead of DataType::Int64
logical_float()    // Instead of DataType::Float64
logical_date()     // For date types
logical_boolean()  // For boolean types
```

## Volatility Guide

- `Immutable` - Same inputs always produce same outputs (most functions)
- `Stable` - Same within a single query (e.g., `NOW()`)
- `Volatile` - Can change between calls (e.g., `RANDOM()`)

## Function Categories & Examples

### String Functions
- **Required**: `invoke_with_args`, `return_type` (usually Utf8)
- **Common**: `aliases`, `simplify`
- **Consider**: `preserves_lex_ordering` for case conversion

### Math Functions  
- **Required**: `invoke_with_args`, `return_type` (numeric)
- **Common**: `evaluate_bounds`, `simplify`
- **Consider**: Interval arithmetic for optimization

### Conditional Functions
- **Required**: `invoke_with_args`, `return_type_from_args` (for nullability)
- **Common**: `short_circuits` (true for IF/CASE), `simplify`
- **Consider**: Constraint propagation

### Type Conversion
- **Required**: `invoke_with_args`, `return_type_from_args` (value-dependent)
- **Common**: `coerce_types` if using UserDefined signature, `simplify`
- **Consider**: Custom type validation

## Registration Process

After implementing the function, it **MUST** be registered properly:

### 1. Register the function in Code
Add the function to the appropriate function registry module. The easiest way is to register it through lib.rs with updating `register_udfs` function.

### 2. Add to Tracking (REQUIRED)
Add the function name to `implemented_functions.csv`:

### 3. Regenerate Unimplemented Functions with Documentation (RECOMMENDED)
```bash
python generate_unimplemented_snowflake_functions.py
cargo fmt
```

### 4. Alternative (NOT RECOMMENDED)
For simplicity or when using AI agents, the function from `generated_snowflake_functions.rs` can be manually deleted, but this approach is **not recommended** in general or if multiple functions are implemented simultaneously.

## Testing Strategy

### Primary Testing: Snapshot Tests (RECOMMENDED)
Use the `test_query!` macro for integration testing:

```rust
test_query!(
    function_name,
    "SELECT function_name('test_input')",
    "CREATE TABLE test_table (col_name string)", // setup queries optional
    snapshot_path = "function_name"
);
```

The snapshot test folder should match the function name.

### Optional: Unit Tests
Unit tests can be added directly in the function file:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    // Unit test implementation
}
```

## Testing Checklist

- [ ] Basic function creation and name
- [ ] ScalarUDF creation and expression building  
- [ ] Null input handling
- [ ] Type validation
- [ ] Edge cases (empty strings, zero, etc.)
- [ ] Scalar vs Array input handling
- [ ] Test with real table not only scalar
- [ ] **Snapshot tests** using `test_query!` macro
- [ ] Function registration verification

## Error Handling Guidelines

### In `return_type()` method:
```rust
fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
    if arg_types.len() != 1 {
        return exec_err!("function requires exactly 1 argument, got {}", arg_types.len());
    }
    // Use exec_err! for all return_type errors
}
```

### In `invoke_with_args()` method:
```rust
fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
    // Use exec_err! for runtime execution errors
    // Handle nulls appropriately (usually null in → null out)
    // Try using arrow compute methods (datafusion::arrow::compute) where possible.
    // For exmaple: datafusion::arrow::compute::cast for casting, datafusion::arrow::compute::kernels::cmp::eq for equality checks, etc.
}
```

## Common Pitfalls

1. **Performance**: Handle scalar inputs efficiently, don't always convert to arrays
2. **Nulls**: Properly handle null propagation in the logic
3. **Types**: Validate input types in `return_type()` or `invoke_with_args()`
4. **Memory**: Use appropriate Arrow array builders for large results
5. **Errors**: Use `exec_err!()` for return_type errors, handle planning vs execution errors appropriately
6. **Registration**: Forgetting to register the function or update tracking files
7. **Logical Types**: Use logical types instead of raw DataTypes in signatures
8. **Simplify**: Most functions can be simplified at least if arguments is constants.