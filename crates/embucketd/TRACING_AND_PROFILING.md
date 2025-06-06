# Tracing and profiling

## Tracing
Embucket uses `tracing::instrument` for instrumenting a code for tracing. It's can be used in both dev and production environments. For development use `info`, `debug` or `trace` level; for production `info` level is recommended.

### Logging
Logging is the basic way to observe debug and tracing events.
Usually `RUST_LOG=debug` is just enough. For tracing use `RUST_LOG=trace`

### Open-Telemetry with Jaeger v2
Instrumented calls in Embucket produce tracing events and spans by using the OpenTelemetry SDK. These events are then sent via OTLP (OpenTelemetry Protocol) to port 4317, where OpenTelemetry Collector is listening.
It starts collecting data when you run the Docker container, which also serves a [Jaeger](https://www.jaegertracing.io/download/) dashboard at [http://localhost:16686/](http://localhost:16686)

```bash
# Run docker container with Jaeger UI v2
docker run --rm --name jaeger -p 16686:16686 -p 4317:4317 -p 4318:4318 -p 5778:5778 -p 9411:9411 jaegertracing/jaeger:2.6.0
```

### Run Embucket in tracing mode
Use `RUST_LOG` environment variable to define log levels, and `--tracing-level` argument to enable tracing with Jaeger.
Default log level is `info`, and default tracing level is `info` too.

```
target/debug/embucketd --jwt-secret=test --backend=memory '--cors-allow-origin=http://localhost:8080' --cors-enabled=true --tracing-level=trace
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
