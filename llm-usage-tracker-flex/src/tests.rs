mod tests {
    use pdk_unit::{UnitHttpMessage, UnitHttpRequest, UnitHttpResponse, UnitTestBuilder};

    fn cfg(inject: bool) -> String {
        format!(r#"{{"metricName":"test.tokens","injectIncludeUsage":{inject}}}"#)
    }

    fn sse_stream() -> Vec<u8> {
        let mut s = String::new();
        for _ in 0..14 {
            s.push_str("data: {\"choices\":[{\"delta\":{\"content\":\"x\"}}]}\n\n");
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
        use crate::{take_one_sse_event, UsageChunk};

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

        fn trace() -> Rc<TraceBackend<UnitHttpResponse>> {
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
