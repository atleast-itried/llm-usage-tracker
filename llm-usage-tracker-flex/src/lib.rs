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
