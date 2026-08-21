# LLM Usage Tracker

A MuleSoft Flex Gateway custom policy (PDK 1.9.2) that observes SSE responses from
OpenAI-compatible LLM endpoints, extracts token usage from the final data chunk, and sends it to
a Mule API or Anypoint MQ relay — with zero buffering of the response stream.

## What it does

- Forwards the SSE response to the client **byte-for-byte** with no buffering
- Detects the chunk containing `usage.total_tokens` and emits a structured log line
- After the stream ends, POSTs usage data to a configurable notification endpoint (Mule API / Anypoint MQ relay)
- Optionally injects `stream_options: {"include_usage": true}` into the request body

## Project structure

```
llm-usage-tracker-definition/   Policy schema (gcl.yaml) — scaffold with PDK CLI
llm-usage-tracker-flex/         Rust implementation
  src/lib.rs                     Policy logic
  src/tests.rs                   Unit tests (pdk-unit, no Docker needed)
  playground/                    Local Docker test environment
    config/api.yaml              Gateway config for local testing
    mock-sse-server.js           Node.js mock that emits 14 chunks + usage + [DONE]
```

## Getting started

See [llm-usage-tracker-guide.md](llm-usage-tracker-guide.md) for the full step-by-step guide
covering prerequisites, scaffolding, implementation, unit tests, and local playground testing.

### Quick start

```bash
# 1. Scaffold (requires Anypoint CLI v4 + PDK plugin)
cd llm-usage-tracker-definition && make release-local && cd ..
cd llm-usage-tracker-flex && make setup && make build-asset-files

# 2. Unit tests (no Docker)
cargo test --lib tests

# 3. Local playground (requires Docker + registration.yaml)
make run
```

## Configuration

| Property | Type | Default | Description |
|---|---|---|---|
| `metricName` | string | `llm.usage.total_tokens` | Label in the `[usageLog]` log line |
| `injectIncludeUsage` | boolean | `false` | Inject `stream_options.include_usage=true` into requests |
| `notificationUrl` | string (service) | — | Mule API or MQ relay endpoint to POST usage to (optional) |

## Notification payload

```json
{"promptTokens": 10, "completionTokens": 14, "totalTokens": 24}
```

Sent via HTTP POST after the full SSE stream (including `[DONE]`) is forwarded to the client.

## Anypoint MQ

Point `notificationUrl` at a thin Mule integration API that handles MQ auth and message
wrapping — this keeps MQ credentials out of the gateway policy.
