## Tracing
Our tracing support uses tracing with logs and also sending traces to the OpenTelemetry OLTP collector on port 4317, using OpenTelemetry SDK. To analyze collected events you can use jaeger or aspire dashboards, we were tested.
Also a minimal overhead is expected in case if no OpenTelemetry OLTP collector is running.

### Howto Tracing
Tracing require tracing events to be enabled. This can be done by providing `RUST_LOG` environment variable with `trace` level, or `debug` level for less events.

Run embucket with enabled trace logs:
```
RUST_LOG=debug,api_ui=trace,api_sessions=trace,api_snowflake_rest=trace,api_iceberg_rest=trace,core_executor=trace,core_utils=trace,core_history=trace,core_metastore=trace target/debug/embucketd --jwt-secret=test --backend=memory '--cors-allow-origin=http://localhost:8080' --cors-enabled=true
```

Run telemetry otlp events collector:
```
docker run -p 4318:4318 otel/opentelemetry-collector:latest
```

Run jaeger dashboard, and open in browser `http://localhost:16686/`:
```
docker run -p 16686:16686 -p 4317:4317 -e COLLECTOR_OTLP_ENABLED=true jaegertracing/all-in-one:latest
```

Or run aspire dashboard instead of jaeger:
```
docker run --rm -it -p 18888:18888 -p 4317:18889 --name aspire-dashboard mcr.microsoft.com/dotnet/aspire-dashboard:latest
```