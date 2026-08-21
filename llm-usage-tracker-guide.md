# LLM Usage Tracker — Implementation & Testing Guide

PDK 1.9.2 · Flex/Omni Gateway · SSE streaming passthrough · Anypoint MQ / Mule API notification

---

## Background

This policy observes SSE (Server-Sent Events) responses from an OpenAI-compatible LLM endpoint
(e.g. Azure OpenAI) as they stream through the gateway. It extracts `usage.total_tokens` from the
final data chunk and emits a structured log line, then POSTs the usage data to a Mule API or
Anypoint MQ relay — **after** all chunks are forwarded to the client, with zero buffering of the
response stream.

### Key design decisions

| Decision | Reason |
|---|---|
| `into_body_stream_state()` on response | Only non-buffering hook for SSE. `into_body_state()` collapses the full stream before delivering, breaking the streaming contract. |
| Chunks forwarded automatically | Streaming state is read-only; proxy-wasm forwards each chunk as `stream.next()` yields it. No `set_body` needed or possible. |
| `Vec<u8>` parsing buffer | Holds at most one partial SSE event (~a few hundred bytes). Bytes inside it are **already forwarded** to the client — this is an observation buffer, not a response buffer. |
| HTTP call after stream ends | `stream.next()` returning `None` means all chunks including `[DONE]` are forwarded. The MQ/Mule POST happens only then, adding its round-trip after the client has the complete response. |
| Fail-open on HTTP call | A telemetry sidechannel must never block or error the response. |

---

## Phase 1 — Prerequisites

```bash
# Verify Anypoint CLI + PDK plugin
npx anypoint-cli-v4@latest --version
npx anypoint-cli-v4@latest plugins

# If the PDK plugin is missing:
npx anypoint-cli-v4@latest plugins:install anypoint-pdk-plugin
# If the OLD plugin is installed first, remove it:
npx anypoint-cli-v4@latest plugins:uninstall anypoint-cli-pdk-plugin

# Verify Rust + wasm target (must be rustup, NOT Homebrew Rust)
rustup target list --installed | grep wasm32-wasip1
# If missing:
rustup target add wasm32-wasip1

# Verify Docker is running
docker info > /dev/null 2>&1 && echo "Docker OK"
```

---

## Phase 2 — Scaffold the project

```bash
mkdir -p policies/llm-usage-tracker
cd policies/llm-usage-tracker

# Scaffold the definition project
npx anypoint-cli-v4@latest pdk policy-project create \
  --name llm-usage-tracker \
  --project-mode definition \
  --category "Quality of Service" \
  --description "Observes SSE LLM responses, extracts token usage, and sends it downstream."

# Scaffold the implementation project
npx anypoint-cli-v4@latest pdk policy-project create \
  --name llm-usage-tracker \
  --project-mode implementation
```

This creates:

```
llm-usage-tracker/
  llm-usage-tracker-definition/   ← schema + exchange.json
  llm-usage-tracker-flex/         ← Rust implementation
```

---

## Phase 3 — Define the schema

Replace the contents of `llm-usage-tracker-definition/gcl.yaml`:

```yaml
---
apiVersion: gateway.mulesoft.com/v1alpha1
kind: Extension
metadata:
  labels:
    title: LLM Usage Tracker
    description: Observes SSE LLM responses, extracts token usage, emits a log line and POSTs to a notification endpoint.
    category: Quality of Service
    metadata/interfaceScope: api
spec:
  extends:
    - name: extension-definition
      namespace: default
  properties:
    metricName:
      type: string
      default: "llm.usage.total_tokens"
      description: Label used in the structured [usageLog] log line.
    injectIncludeUsage:
      type: boolean
      default: false
      description: When true, injects stream_options.include_usage=true into the request body.
    notificationUrl:
      type: string
      format: service
      description: HTTP endpoint to POST usage data to after the stream ends (Mule API or Anypoint MQ relay). Optional.
```

Build the definition locally (no Exchange publish needed yet):

```bash
cd llm-usage-tracker-definition
make release-local
cd ..
```

---

## Phase 4 — Wire up the implementation

```bash
cd llm-usage-tracker-flex
make setup              # installs cargo-anypoint
make build-asset-files  # generates src/generated/config.rs from gcl.yaml
```

Verify `src/generated/config.rs` contains a `Config` struct with `metric_name`,
`inject_include_usage`, and `notification_url` fields.

---

## Phase 5 — Replace `Cargo.toml`

```toml
[package]
name = "llm_usage_tracker"
version = "1.0.0"
edition = "2018"

[package.metadata.anypoint]
group_id = "<your-org-group-id>"
definition_asset_id = { name = "llm-usage-tracker", version = "1.0.0-DEV" }
implementation_asset_id = "llm-usage-tracker-flex"

[lib]
crate-type = ["cdylib"]

[dependencies]
anyhow = "1.0"
futures = "0.3"
pdk = { version = "1.9.2" }
serde = { version = "1.0", features = ["derive"] }
serde_json = { version = "1.0", default-features = false, features = ["alloc"] }

[dev-dependencies]
pdk-unit = { version = "1.9.2", features = ["experimental"] }

[profile.release]
lto = true
opt-level = 'z'
strip = "debuginfo"
```

---

## Phase 6 — Write `src/lib.rs`

Replace the scaffolded `src/lib.rs` entirely:

```rust
mod generated;

#[cfg(test)]
mod tests;

use futures::StreamExt;
use serde::Deserialize;
use serde_json::Value;

use pdk::hl::*;
use pdk::logger;

use crate::generated::config::Config;

const DEFAULT_METRIC_NAME: &'static str = "llm.usage.total_tokens";
const CONTENT_LENGTH_HEADER: &'static str = "content-length";
const CONTENT_TYPE_HEADER: &'static str = "content-type";
const SSE_CONTENT_TYPE: &'static str = "text/event-stream";

// ── Serde types ──────────────────────────────────────────────────────────────

#[derive(Deserialize)]
struct UsageChunk {
    usage: Option<UsageData>,
}

#[derive(Deserialize, Clone)]
struct UsageData {
    prompt_tokens: Option<u64>,
    completion_tokens: Option<u64>,
    total_tokens: Option<u64>,
}

// ── Request filter (optional include_usage injection) ────────────────────────

async fn request_filter(request_state: RequestState, config: &Config) -> Flow<()> {
    if !config.inject_include_usage.unwrap_or(false) {
        return Flow::Continue(());
    }

    let headers_state = request_state.into_headers_state().await;
    headers_state.handler().remove_header(CONTENT_LENGTH_HEADER);

    let body_state = headers_state.into_body_state().await;
    let body = body_state.handler().body();
    if body.is_empty() {
        return Flow::Continue(());
    }

    let mut json: Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => {
            logger::debug!("Non-JSON request body, skipping include_usage injection: {e}");
            return Flow::Continue(());
        }
    };

    if let Some(obj) = json.as_object_mut() {
        let so = obj
            .entry("stream_options")
            .or_insert_with(|| serde_json::json!({}));
        if let Some(so_obj) = so.as_object_mut() {
            so_obj.insert("include_usage".to_string(), Value::Bool(true));
        }
    }

    match serde_json::to_vec(&json) {
        Ok(new_body) => {
            if let Err(e) = body_state.handler().set_body(&new_body) {
                logger::warn!("Failed to write modified request body: {e:?}");
            }
        }
        Err(e) => logger::warn!("Failed to serialize modified request body: {e}"),
    }

    Flow::Continue(())
}

// ── Response filter (streaming passthrough + usage detection) ────────────────

async fn response_filter(
    response_state: ResponseState,
    _request_data: RequestData<()>,
    config: &Config,
    client: &HttpClient,
) -> Flow<()> {
    let headers_state = response_state.into_headers_state().await;

    let is_sse = headers_state
        .handler()
        .header(CONTENT_TYPE_HEADER)
        .map(|v| v.to_ascii_lowercase().starts_with(SSE_CONTENT_TYPE))
        .unwrap_or(false);

    // Transition to streaming state regardless — read-only, zero-copy passthrough.
    let body_stream_state = headers_state.into_body_stream_state().await;

    if !is_sse {
        return Flow::Continue(());
    }

    let mut stream = body_stream_state.stream();
    let mut buf: Vec<u8> = Vec::new();
    let mut detected: Option<UsageData> = None;

    // Chunks are forwarded to the client as stream.next() yields them.
    // buf holds at most one partial SSE event — bytes already forwarded, kept for parsing only.
    while let Some(chunk) = stream.next().await {
        buf.extend_from_slice(chunk.bytes());
        while let Some((payload, consumed)) = take_one_sse_event(&buf) {
            if detected.is_none() {
                match serde_json::from_str::<UsageChunk>(&payload) {
                    Ok(c) => detected = c.usage,
                    Err(_) => logger::debug!("skipping non-JSON SSE payload"),
                }
            }
            buf.drain(..consumed);
        }
    }
    // All chunks (including [DONE]) are now forwarded. Only now do we make the outbound call.

    if let Some(usage) = detected {
        emit_usage_log(&usage, config);
        notify_usage(client, config, &usage).await;
    }

    Flow::Continue(())
}

fn emit_usage_log(usage: &UsageData, config: &Config) {
    let metric = config.metric_name.as_deref().unwrap_or(DEFAULT_METRIC_NAME);
    logger::info!(
        "[usageLog] metric={} prompt_tokens={} completion_tokens={} total_tokens={}",
        metric,
        usage.prompt_tokens.unwrap_or(0),
        usage.completion_tokens.unwrap_or(0),
        usage.total_tokens.unwrap_or(0),
    );
}

async fn notify_usage(client: &HttpClient, config: &Config, usage: &UsageData) {
    let url = match &config.notification_url {
        Some(u) if !u.uri().authority().is_empty() => u,
        _ => return,
    };

    let payload = serde_json::json!({
        "promptTokens":     usage.prompt_tokens.unwrap_or(0),
        "completionTokens": usage.completion_tokens.unwrap_or(0),
        "totalTokens":      usage.total_tokens.unwrap_or(0),
    });

    let body = match serde_json::to_vec(&payload) {
        Ok(b) => b,
        Err(e) => {
            logger::warn!("Failed to serialize usage payload: {e}");
            return;
        }
    };

    match client
        .request(url)
        .headers(vec![("content-type", "application/json")])
        .body(&body)
        .post()
        .await
    {
        Ok(_) => logger::debug!("Usage notification sent"),
        Err(e) => logger::warn!("Usage notification failed (fail-open): {e}"),
    }
}

// ── SSE parsing helpers ──────────────────────────────────────────────────────

pub(crate) fn take_one_sse_event(buf: &[u8]) -> Option<(String, usize)> {
    let (boundary_start, boundary_len) = find_event_boundary(buf)?;
    let event_str = std::str::from_utf8(&buf[..boundary_start]).ok()?;
    let mut data_lines: Vec<&str> = Vec::new();
    for line in event_str.split('\n') {
        let line = line.trim_end_matches('\r');
        if let Some(payload) = line.strip_prefix("data:") {
            data_lines.push(payload.strip_prefix(' ').unwrap_or(payload));
        }
    }
    Some((data_lines.join("\n"), boundary_start + boundary_len))
}

fn find_event_boundary(buf: &[u8]) -> Option<(usize, usize)> {
    let mut i = 0;
    while i + 1 < buf.len() {
        if buf[i] == b'\n' && buf[i + 1] == b'\n' {
            return Some((i, 2));
        }
        if i + 3 < buf.len()
            && buf[i] == b'\r' && buf[i + 1] == b'\n'
            && buf[i + 2] == b'\r' && buf[i + 3] == b'\n'
        {
            return Some((i, 4));
        }
        i += 1;
    }
    None
}

// ── Entrypoints ──────────────────────────────────────────────────────────────

#[pdk::hl::entrypoint_flex]
fn init(abi: &dyn pdk::flex_abi::api::FlexAbi) -> anyhow::Result<()> {
    let config: Config = serde_json::from_slice(abi.get_configuration()).map_err(|e| {
        anyhow::anyhow!(
            "Failed to parse configuration '{}'. Cause: {e}",
            String::from_utf8_lossy(abi.get_configuration()),
        )
    })?;

    if let Some(url) = &config.notification_url {
        if !url.uri().authority().is_empty() {
            abi.service_create(url.clone())?;
        }
    }

    abi.setup()?;
    Ok(())
}

#[entrypoint]
async fn configure(
    launcher: Launcher,
    Configuration(bytes): Configuration,
    client: HttpClient,
) -> anyhow::Result<()> {
    let config: Config = serde_json::from_slice(&bytes).map_err(|e| {
        anyhow::anyhow!(
            "Failed to parse configuration '{}'. Cause: {e}",
            String::from_utf8_lossy(&bytes),
        )
    })?;

    let filter = on_request(|rs| request_filter(rs, &config))
        .on_response(|rs, rd| response_filter(rs, rd, &config, &client));

    launcher.launch(filter).await?;
    Ok(())
}
```

---

## Phase 7 — Write `src/tests.rs`

Create `src/tests.rs`:

```rust
mod tests {
    use super::*;
    use pdk_unit::{UnitHttpRequest, UnitHttpResponse, UnitHttpMessage, UnitTestBuilder};

    fn cfg(inject: bool) -> String {
        format!(r#"{{"metricName":"test.tokens","injectIncludeUsage":{inject}}}"#)
    }

    fn sse_stream() -> Vec<u8> {
        let mut s = String::new();
        for _ in 0..14 {
            s.push_str(
                "data: {\"choices\":[{\"delta\":{\"content\":\"x\"}}]}\n\n",
            );
        }
        s.push_str(concat!(
            "data: {\"choices\":[],\"usage\":{",
            "\"prompt_tokens\":10,",
            "\"completion_tokens\":14,",
            "\"total_tokens\":24}}\n\n",
        ));
        s.push_str("data: [DONE]\n\n");
        s.into_bytes()
    }

    // ── SSE parsing unit tests ────────────────────────────────────────────────

    mod sse_helpers {
        use super::super::*;

        #[test]
        fn test_lf_boundary() {
            let (payload, consumed) = take_one_sse_event(b"data: hello\n\n").unwrap();
            assert_eq!(payload, "hello");
            assert_eq!(consumed, 13);
        }

        #[test]
        fn test_crlf_boundary() {
            let (payload, _) = take_one_sse_event(b"data: hello\r\n\r\n").unwrap();
            assert_eq!(payload, "hello");
        }

        #[test]
        fn test_partial_returns_none() {
            assert!(take_one_sse_event(b"data: hel").is_none());
        }

        #[test]
        fn test_done_sentinel_returned_as_payload() {
            let (payload, _) = take_one_sse_event(b"data: [DONE]\n\n").unwrap();
            assert_eq!(payload, "[DONE]");
        }

        #[test]
        fn test_no_space_after_colon() {
            let (payload, _) = take_one_sse_event(b"data:nospace\n\n").unwrap();
            assert_eq!(payload, "nospace");
        }

        #[test]
        fn test_usage_chunk_parses_correctly() {
            let json = r#"{"choices":[],"usage":{"prompt_tokens":10,"completion_tokens":14,"total_tokens":24}}"#;
            let chunk: UsageChunk = serde_json::from_str(json).unwrap();
            let u = chunk.usage.unwrap();
            assert_eq!(u.total_tokens, Some(24));
            assert_eq!(u.prompt_tokens, Some(10));
            assert_eq!(u.completion_tokens, Some(14));
        }

        #[test]
        fn test_done_is_not_valid_json() {
            assert!(serde_json::from_str::<UsageChunk>("[DONE]").is_err());
        }
    }

    // ── Response filter integration tests ─────────────────────────────────────

    mod response {
        use super::*;

        #[test]
        fn test_full_stream_forwarded_unchanged() {
            let body = sse_stream();
            let mut t = UnitTestBuilder::default()
                .with_config(&cfg(false))
                .with_backend(
                    UnitHttpResponse::new(200)
                        .with_header("content-type", "text/event-stream")
                        .with_body(body.clone()),
                )
                .with_entrypoint(crate::configure);

            let resp = t.request(UnitHttpRequest::post().with_path("/v1/chat/completions"));
            assert_eq!(resp.status_code(), 200);
            assert_eq!(resp.body(), body.as_slice());
        }

        #[test]
        fn test_accumulation_across_small_chunks() {
            // Forces SSE event boundaries to straddle chunk boundaries.
            let mut t = UnitTestBuilder::default()
                .with_config(&cfg(false))
                .with_backend(
                    UnitHttpResponse::new(200)
                        .with_header("content-type", "text/event-stream")
                        .with_body(sse_stream()),
                )
                .with_entrypoint(crate::configure);

            t.set_chunk_size(32);

            let resp = t.request(UnitHttpRequest::post().with_path("/v1/chat/completions"));
            assert_eq!(resp.status_code(), 200);
            assert_eq!(resp.body(), sse_stream().as_slice());
        }

        #[test]
        fn test_non_sse_response_passes_through() {
            let body = br#"{"answer":42}"#;
            let mut t = UnitTestBuilder::default()
                .with_config(&cfg(false))
                .with_backend(
                    UnitHttpResponse::new(200)
                        .with_header("content-type", "application/json")
                        .with_body(body.to_vec()),
                )
                .with_entrypoint(crate::configure);

            let resp = t.request(UnitHttpRequest::get().with_path("/v1/models"));
            assert_eq!(resp.status_code(), 200);
            assert_eq!(resp.body(), body);
        }
    }

    // ── Request injection tests ───────────────────────────────────────────────

    mod injection {
        use super::*;
        use pdk_unit::TraceBackend;
        use std::rc::Rc;

        fn trace() -> Rc<TraceBackend> {
            Rc::new(TraceBackend::new(UnitHttpResponse::new(200)))
        }

        #[test]
        fn test_include_usage_injected() {
            let backend = trace();
            let mut t = UnitTestBuilder::default()
                .with_config(&cfg(true))
                .with_backend(Rc::clone(&backend))
                .with_entrypoint(crate::configure);

            t.request(
                UnitHttpRequest::post()
                    .with_path("/v1/chat/completions")
                    .with_body(r#"{"model":"gpt-4","stream":true}"#),
            );

            let req = backend.next().unwrap();
            let body: serde_json::Value = serde_json::from_slice(req.body()).unwrap();
            assert_eq!(body["stream_options"]["include_usage"], true);
        }

        #[test]
        fn test_injection_disabled_body_unchanged() {
            let backend = trace();
            let original = r#"{"model":"gpt-4","stream":true}"#;
            let mut t = UnitTestBuilder::default()
                .with_config(&cfg(false))
                .with_backend(Rc::clone(&backend))
                .with_entrypoint(crate::configure);

            t.request(
                UnitHttpRequest::post()
                    .with_path("/v1/chat/completions")
                    .with_body(original),
            );

            let req = backend.next().unwrap();
            let body: serde_json::Value = serde_json::from_slice(req.body()).unwrap();
            assert!(body.get("stream_options").is_none());
        }

        #[test]
        fn test_existing_stream_options_preserved() {
            let backend = trace();
            let mut t = UnitTestBuilder::default()
                .with_config(&cfg(true))
                .with_backend(Rc::clone(&backend))
                .with_entrypoint(crate::configure);

            t.request(
                UnitHttpRequest::post()
                    .with_path("/v1/chat/completions")
                    .with_body(r#"{"stream_options":{"other":true}}"#),
            );

            let req = backend.next().unwrap();
            let body: serde_json::Value = serde_json::from_slice(req.body()).unwrap();
            assert_eq!(body["stream_options"]["include_usage"], true);
            assert_eq!(body["stream_options"]["other"], true);
        }
    }
}
```

---

## Phase 8 — Run unit tests

```bash
# From llm-usage-tracker-flex/
cargo test --lib tests

# Run a specific group
cargo test --lib tests::sse_helpers
cargo test --lib tests::response
cargo test --lib tests::injection
```

All tests should pass before proceeding. These run in milliseconds with no Docker.

---

## Phase 9 — Compile the WASM binary

```bash
make build
```

---

## Phase 10 — Configure the playground

**`playground/config/api.yaml`**

```yaml
---
apiVersion: gateway.mulesoft.com/v1alpha1
kind: ApiInstance
metadata:
  name: ingress-http
spec:
  address: http://0.0.0.0:8081
  services:
    upstream:
      address: "http://backend"
      routes:
        - config:
            destinationPath: /
  policies:
    - policyRef:
        name: llm-usage-tracker
        namespace: default
      config:
        metricName: "azure.openai.tokens"
        injectIncludeUsage: true
        # notificationUrl: "http://mule-api:8082/usage"  # uncomment when ready
```

Copy a `registration.yaml` from another policy in the repo:

```bash
cp ../../<another-policy>/<another-policy>-flex/playground/config/registration.yaml \
   playground/config/registration.yaml
```

---

## Phase 11 — Run the mock SSE server

Add a `mock-sse-server.js` to the playground directory:

```js
// playground/mock-sse-server.js
const http = require('http');

http.createServer((req, res) => {
  res.writeHead(200, {
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive',
  });

  let i = 0;
  const interval = setInterval(() => {
    if (i < 14) {
      res.write(`data: {"choices":[{"delta":{"content":"x"}}]}\n\n`);
      i++;
    } else if (i === 14) {
      res.write(
        `data: {"choices":[],"usage":{"prompt_tokens":10,"completion_tokens":14,"total_tokens":24}}\n\n`
      );
      i++;
    } else {
      res.write(`data: [DONE]\n\n`);
      clearInterval(interval);
      res.end();
    }
  }, 500);
}).listen(80, () => console.log('Mock SSE server on :80'));
```

Add the `backend` service to `playground/docker-compose.yaml`:

```yaml
  backend:
    image: node:20-alpine
    volumes:
      - ./mock-sse-server.js:/app/server.js
    command: node /app/server.js
    networks:
      - gateway
```

---

## Phase 12 — Start the playground

```bash
make run
# Wait for: "Omni Gateway is running"
```

---

## Phase 13 — Assert (i): chunks arrive incrementally

In a second terminal:

```bash
curl -N -s http://localhost:8081/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{"model":"gpt-4","stream":true}' \
  | while IFS= read -r line; do
      echo "$(date '+%H:%M:%S.%3N')  $line"
    done
```

Expected — one line every ~500ms:

```
10:23:01.042  data: {"choices":[{"delta":{"content":"x"}}]}
10:23:01.543  data: {"choices":[{"delta":{"content":"x"}}]}
...  (14 content chunks)
10:23:08.062  data: {"choices":[],"usage":{"prompt_tokens":10,"completion_tokens":14,"total_tokens":24}}
10:23:08.063  data: [DONE]
```

If all lines appear simultaneously, something upstream is buffering. Check that the mock server
flushes after each `res.write()` and that no intermediate proxy is gzip-buffering.

---

## Phase 14 — Assert (ii): total_tokens == 24

While `make run` is active, in another terminal:

```bash
docker compose -f ./playground/docker-compose.yaml logs local-flex 2>&1 \
  | grep '\[usageLog\]'
```

Expected:

```
[usageLog] metric=azure.openai.tokens prompt_tokens=10 completion_tokens=14 total_tokens=24
```

---

## Phase 15 — Test the notification endpoint (Mule API / Anypoint MQ)

1. Uncomment `notificationUrl` in `playground/config/api.yaml` and point it at your Mule API.
2. Restart: `Ctrl+C`, then `make run`.
3. Send a request (same curl as Phase 13).
4. Verify the Mule API received:

```json
{"promptTokens": 10, "completionTokens": 14, "totalTokens": 24}
```

The POST arrives **after** `[DONE]` is forwarded. Confirm by comparing the Mule log timestamp
against the curl output timestamps from Phase 13.

> **Anypoint MQ note:** The MQ REST API requires `Authorization: Bearer <token>` and a specific
> message envelope. Point `notificationUrl` at a thin Mule integration API that handles MQ auth
> and wrapping, rather than at the MQ REST API directly. This keeps MQ credentials out of the
> gateway policy.

---

## Phase 16 — Integration tests (Docker, optional)

```bash
# registration.yaml must also exist in tests/config/
cp playground/config/registration.yaml tests/config/registration.yaml

make test
```

Logs for failing tests land at:

```
target/pdk-test/requests/<test-name>/local-flex.log
```

---

## Quick reference

```bash
# One-time setup
make setup

# After every gcl.yaml change
make build-asset-files

# Fast feedback loop — no Docker
cargo test --lib tests

# Full WASM build
make build

# Local playground
make run

# Integration tests — requires Docker
make test

# Stop playground
docker compose -f ./playground/docker-compose.yaml down
```

---

## Streaming timing guarantee

```
chunk 1  → forwarded to client   ← stream.next() yields chunk 1
chunk 2  → forwarded to client   ← stream.next() yields chunk 2
...
chunk 15 (usage) → forwarded     ← detected = Some(UsageData { total_tokens: 24, ... })
[DONE]   → forwarded to client   ← stream.next() yields [DONE]
stream.next() → None             ← all chunks forwarded; stream is done
POST to Mule API / Anypoint MQ   ← happens here, after the client has the full response
```
