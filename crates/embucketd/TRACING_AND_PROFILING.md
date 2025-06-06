# Tracing and profiling

## Tracing
Embucket uses `tracing::instrument` for instrumenting a code for tracing. It's can be used in both dev and production environments. For development use `info`, `debug` or `trace` level; for production `info` level is recommended.

### Logging
Logging is the basic way to observe debug and tracing events.
Usually `RUST_LOG=debug` is just enough. For tracing use `RUST_LOG=trace`

### Open-Telemetry with Jaeger v2
Embucket's calls that have been instrumeneted with tracing are used as a source of tracing events. Then OpenTelemetry SDK used for sending tracing spans via OpenTelemetry protocol via the port `4317` to the *OpenTelemetry OLTP* collector, running inside the Jaeger v2 container. In case if no Jaeger is running it shouldn't give any substantial overhead on *Embucket*. Jaeger dashboard

For collect => visualize => analyze collected events  dashboards can be used:

* [Jaeger v2](https://www.jaegertracing.io/download/) dashboard accessible at [localhost:16686](http://localhost:16686)
  ```
  # Run docker container with Jaeger UI v2
  docker run --rm --name jaeger -p 16686:16686 -p 4317:4317 -p 4318:4318 -p 5778:5778 -p 9411:9411 jaegertracing/jaeger:2.6.0
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
