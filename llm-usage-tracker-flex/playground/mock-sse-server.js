// Mock SSE server — emits 14 content chunks at 500ms intervals, then the
// usage chunk, then [DONE]. Matches the spec in the implementation guide.
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
}).listen(80, () => console.log('Mock SSE server listening on :80'));
