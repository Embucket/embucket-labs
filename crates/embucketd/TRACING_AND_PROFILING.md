# Tracing and profiling

## Tracing
Embucket uses `tracing::instrument` for instrumenting the code for tracing. It's can be used in both dev and production environments. For development use `info`, `debug` or `trace` level; for production `info` level is recommended.

### Logging
Logging is the basic way to observe debug and tracing events.
Usually `RUST_LOG=debug` is just enough. For tracing use `RUST_LOG=trace`

### Open-Telemetry OLTP collector
Calls that have been instrumeneted with tracing are used here as a source of tracing events. Then OpenTelemetry SDK is used for sending tracing spans via OpenTelemetry protocol to the *OpenTelemetry OLTP* collector that is listening on port `4317` by default. In case if no events collector is running it shouldn't give any substantial overhead.

Run OpenTelemetry OLTP events collector:
```
docker run -p 4318:4318 otel/opentelemetry-collector:latest
```

For visualizing & analyzing collected events at least *one of* the following dashboards can be used:


* [Jaeger](https://www.jaegertracing.io/download/) dashboard accessible at [localhost:16686](http://localhost:16686)
  ```
  # Run docker container with Jaeger UI
  docker run -p 16686:16686 -p 4317:4317 -e COLLECTOR_OTLP_ENABLED=true jaegertracing/all-in-one:latest
  ```
* [Aspire](https://learn.microsoft.com/en-us/dotnet/aspire/fundamentals/dashboard/overview?tabs=bash) dashboard accessible at [localhost:18888](http://localhost:18888). Please find the link with auth token in the output of the container as it requires authentication by default.
  ```
  # Run Aspire dashboard:
  docker run --rm -it -p 18888:18888 -p 4317:18889 --name aspire-dashboard mcr.microsoft.com/dotnet/aspire-dashboard:latest
  ```

### tokio-console
For those who found tokio-console useful for tracing - Embucket supports `tokio-console` connection. `RUST_LOG=trace` should be used when running Embucket. To start using tokio-console:
```
# Install
cargo install --locked tokio-console

# Run 
tokio-console
```

### Run Embucket in tracing mode
Debug log level and tracing levels are controlled via `RUST_LOG` environment variable.

To run embucket with debug logs and enabled trace logs:
```
RUST_LOG=debug,api_ui=trace,api_sessions=trace,api_snowflake_rest=trace,api_iceberg_rest=trace,core_executor=trace,core_utils=trace,core_history=trace,core_metastore=trace target/debug/embucketd --jwt-secret=test --backend=memory '--cors-allow-origin=http://localhost:8080' --cors-enabled=true
```

## Profiling
In case if you need to profile `embucketd` executable, you can use [Samply](https://github.com/mstange/samply/).
*Samply* is just one of the ways to profile and added here as an experiment. This solution works out of the box on macOS, Linux, and Windows.

To start profiling, prepend `samply record` to the `embucketd` command invocation. Do actions that need to be profiled and right after you stop profiling it will open a profile report in the browser.

```
# install Samply
cargo install --locked samply

# Profile debug build
cargo build && samply record RUST_LOG=debug target/debug/embucketd --jwt-secret=test --backend=memory '--cors-allow-origin=http://localhost:8080' --cors-enabled=true

```
