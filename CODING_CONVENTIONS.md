# Coding Conventions used in Rust codebase of Embucket project

## Design Conventions
* Define errors with display messages in dedicated `errors.rs` files. Avoid inlining error texts outside of those files
* Define `Error` enum and `Result<T>` types per crate, with public visibility
* Implement `IntoResponse` trait for top-level error for API crates
* Errors appeared in logs and tracing spans/events should also bring an error stack trace

## Error Handling Conventions
These conventions establish consistent practices for defining and handling errors in Rust crates using the [Snafu](https://docs.rs/snafu) error library, and `error_stack_trace::debug` proc macro enabling error stack trace.

### Error Construction and Propagation
* Derive from `Snafu` when defining error enums
* Use proc macro `error_stack_trace::debug` to enable error stack traces
* Restrict generated Snafu selectors' visibility to crate level: `#[snafu(visibility(pub(crate)))]`
* Avoid constructing errors manually except in rare edge cases (e.g. boxed, non-Snafu, or external errors). Always document the reason when doing so.
* Use Snafu’s helpers doing implicit error conversions under the hood:
  * `.context(...)`, also supports chaining
  * `.build()`
  * `.fail()`
  * `.into_error()`
* Do implicit error conversions when propagate errors with `?` operator
* These are preferred over `.map_err(...)` which is generally considered an anti-pattern due to loss of context and less ergonomic traceability

### Defining Errors
Define error enum with `#[derive(Snafu)]` and `#[error_stack_trace::debug]`:
```
#[derive(Snafu)]
#[error_stack_trace::debug]
pub enum Error {...}
```
### Non-Snafu foreign errors
When nesting a non-Snafu foreign error - rename its field from `source` to `error`, and add `#[snafu(source)]` for Snafu context as shown below:
```
#[snafu(source)]
error: ObjectStoreError,
```
### Boxing Large Errors
When `clippy::result_large_err` is triggered, you may box the error variant. Put error into `Box<>` and add `From` trait implementation using:
```
#[snafu(source(from(S3tablesError, Box::new)))]
source: Box<S3tablesError>,
```
Note: With Snafu, most use cases do not require manual boxing or unboxing. Let Snafu manage implicit conversions where possible.
### Transparent Errors
Define transparent error variant to re-use context and display message from the underlying error. No context selectors are generated for transparent errors, as error has no own context. And thus no `.context(...)` call can be used, but it still supports implicit `.into()` conversion, when used with `?`:
```
#[snafu(transparent)]
error: SomeError,
```
