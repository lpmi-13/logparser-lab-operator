package notifier

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	ctrl "sigs.k8s.io/controller-runtime"
)

const notifierFaviconDataURL = "data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHZpZXdCb3g9IjAgMCA2NCA2NCI+CiAgPGcgZmlsbD0ibm9uZSIgZmlsbC1ydWxlPSJldmVub2RkIj4KICAgIDxyZWN0IHg9IjIwIiB5PSIxOCIgd2lkdGg9IjM0IiBoZWlnaHQ9IjI4IiByeD0iMTQiIGZpbGw9IiM4QjVBMkIiIHN0cm9rZT0iIzVBMzQxNiIgc3Ryb2tlLXdpZHRoPSIzIi8+CiAgICA8cGF0aCBkPSJNMjggMjNjMyAxIDUgMSA4IDBtLTggN2M0IDEgOCAxIDEyIDBtLTEyIDhjNSAxIDEwIDEgMTYgMCIgc3Ryb2tlPSIjNkQ0MzIwIiBzdHJva2UtbGluZWNhcD0icm91bmQiIHN0cm9rZS13aWR0aD0iMyIgb3BhY2l0eT0iLjkiLz4KICAgIDxlbGxpcHNlIGN4PSIyMiIgY3k9IjMyIiByeD0iMTQiIHJ5PSIxNCIgZmlsbD0iI0UxQjA2QSIgc3Ryb2tlPSIjNUEzNDE2IiBzdHJva2Utd2lkdGg9IjMiLz4KICAgIDxlbGxpcHNlIGN4PSIyMiIgY3k9IjMyIiByeD0iOSIgcnk9IjkiIGZpbGw9IiNGMEM5ODciIHN0cm9rZT0iIzdDNEQyNSIgc3Ryb2tlLXdpZHRoPSIyIi8+CiAgICA8ZWxsaXBzZSBjeD0iMjIiIGN5PSIzMiIgcng9IjQuNSIgcnk9IjQuNSIgZmlsbD0iI0UxQjA2QSIgc3Ryb2tlPSIjOUQ2NTMwIiBzdHJva2Utd2lkdGg9IjIiLz4KICAgIDxjaXJjbGUgY3g9IjQxIiBjeT0iMzEiIHI9IjMuMiIgZmlsbD0iIzZENDMyMCIgb3BhY2l0eT0iLjg1Ii8+CiAgICA8cGF0aCBkPSJNNDggMThjNCAyIDggNyA4IDE0cy00IDEyLTggMTQiIHN0cm9rZT0iI0E0NkIzNyIgc3Ryb2tlLWxpbmVjYXA9InJvdW5kIiBzdHJva2Utd2lkdGg9IjIuNiIvPgogIDwvZz4KPC9zdmc+"

// Server implements manager.Runnable to serve SSE notifications.
type Server struct {
	notifier *Notifier
	port     int
	server   *http.Server
}

// NewServer creates a new SSE notification server.
func NewServer(notifier *Notifier, port int) *Server {
	return &Server{
		notifier: notifier,
		port:     port,
	}
}

func setCommonHeaders(w http.ResponseWriter) {
	headers := w.Header()
	headers.Set("Cache-Control", "no-store, max-age=0")
	headers.Set("Pragma", "no-cache")
}

func setDocumentHeaders(w http.ResponseWriter) {
	setCommonHeaders(w)
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
}

func setEventStreamHeaders(w http.ResponseWriter) {
	setCommonHeaders(w)
	headers := w.Header()
	headers.Set("Content-Type", "text/event-stream")
	headers.Set("Connection", "keep-alive")
}

// Start implements manager.Runnable.
func (s *Server) Start(ctx context.Context) error {
	logger := ctrl.Log.WithName("notifier-server")

	mux := http.NewServeMux()
	mux.HandleFunc("/", s.handleRoot)
	mux.HandleFunc("/events", s.handleEvents)

	s.server = &http.Server{
		Addr:              fmt.Sprintf(":%d", s.port),
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}

	logger.Info("Starting notification server", "port", s.port, "url", fmt.Sprintf("http://localhost:%d", s.port))

	errCh := make(chan error, 1)
	go func() {
		if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
	}()

	select {
	case <-ctx.Done():
		logger.Info("Shutting down notification server")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return s.server.Shutdown(shutdownCtx)
	case err := <-errCh:
		return err
	}
}

func (s *Server) handleRoot(w http.ResponseWriter, _ *http.Request) {
	html := `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Log Parser Lab</title>
  <link rel="icon" type="image/svg+xml" href="` + notifierFaviconDataURL + `">
  <style>
    :root {
      --bg: #f5efe4;
      --panel: rgba(255, 251, 244, 0.9);
      --ink: #1f2a1f;
      --muted: #566357;
      --accent: #1f6f50;
      --accent-2: #c55d3d;
      --border: rgba(31, 42, 31, 0.12);
      --shadow: 0 18px 40px rgba(31, 42, 31, 0.12);
    }

    * { box-sizing: border-box; }
    body {
      margin: 0;
      min-height: 100vh;
      font-family: "IBM Plex Mono", "Fira Code", monospace;
      color: var(--ink);
      background:
        radial-gradient(circle at top left, rgba(197, 93, 61, 0.18), transparent 28rem),
        radial-gradient(circle at bottom right, rgba(31, 111, 80, 0.18), transparent 30rem),
        linear-gradient(135deg, #f5efe4 0%, #efe4d4 100%);
      padding: 24px;
    }

    .shell {
      max-width: 1100px;
      margin: 0 auto;
      background: var(--panel);
      backdrop-filter: blur(8px);
      border: 1px solid var(--border);
      border-radius: 24px;
      box-shadow: var(--shadow);
      overflow: hidden;
    }

    .hero {
      padding: 28px 28px 16px;
      border-bottom: 1px solid var(--border);
      background:
        linear-gradient(120deg, rgba(31, 111, 80, 0.08), rgba(197, 93, 61, 0.08)),
        repeating-linear-gradient(90deg, transparent, transparent 18px, rgba(31, 42, 31, 0.03) 18px, rgba(31, 42, 31, 0.03) 19px);
    }

    .hero h1 {
      margin: 0 0 10px;
      font-size: clamp(1.8rem, 2vw + 1rem, 2.8rem);
      line-height: 1;
      letter-spacing: 0.04em;
      text-transform: uppercase;
    }

    .hero p {
      margin: 0;
      color: var(--muted);
      max-width: 70ch;
    }

    .statusbar {
      display: flex;
      gap: 12px;
      flex-wrap: wrap;
      padding: 18px 28px 0;
    }

    .pill {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      border-radius: 999px;
      padding: 8px 14px;
      background: rgba(31, 111, 80, 0.08);
      color: var(--muted);
      border: 1px solid rgba(31, 111, 80, 0.16);
    }

    .dot {
      width: 10px;
      height: 10px;
      border-radius: 999px;
      background: var(--accent-2);
      box-shadow: 0 0 0 5px rgba(197, 93, 61, 0.12);
    }

    .dot.live {
      background: var(--accent);
      box-shadow: 0 0 0 5px rgba(31, 111, 80, 0.12);
    }

    #feed {
      padding: 24px 28px 28px;
      display: grid;
      gap: 14px;
    }

    .entry {
      border: 1px solid var(--border);
      border-left: 8px solid var(--accent);
      border-radius: 18px;
      background: rgba(255, 255, 255, 0.68);
      padding: 16px 18px;
      animation: slide-in 180ms ease-out;
    }

    .entry[data-kind="attempt"] {
      border-left-color: #9a7c24;
    }

    .entry[data-kind="completed"] {
      border-left-color: var(--accent);
    }

    .entry[data-kind="error"] {
      border-left-color: #9b2c2c;
    }

    .meta {
      display: flex;
      gap: 12px;
      flex-wrap: wrap;
      margin-bottom: 10px;
      color: var(--muted);
      font-size: 0.9rem;
    }

    .message {
      display: grid;
      gap: 12px;
      line-height: 1.5;
    }

    .message-copy {
      white-space: pre-wrap;
    }

    .message-code {
      margin: 0;
      padding: 12px 14px;
      border-radius: 14px;
      border: 1px solid rgba(75, 92, 82, 0.18);
      background: rgba(86, 99, 87, 0.12);
      color: var(--ink);
      white-space: pre-wrap;
      overflow-wrap: anywhere;
    }

    .message-code code {
      font-family: "IBM Plex Mono", "Fira Code", monospace;
      font-size: 0.98rem;
    }

    .summary {
      margin-bottom: 12px;
      padding: 12px 14px;
      border-radius: 14px;
      background: rgba(31, 111, 80, 0.08);
      border: 1px solid rgba(31, 111, 80, 0.14);
    }

    .summary-label {
      margin-bottom: 6px;
      color: var(--ink);
      font-size: 0.75rem;
      font-weight: 800;
      letter-spacing: 0.08em;
      text-transform: uppercase;
    }

    .summary-text {
      display: grid;
      gap: 6px;
      line-height: 1.5;
    }

    .summary-line {
      display: block;
    }

    .summary-key {
      font-weight: 700;
    }

    @keyframes slide-in {
      from { opacity: 0; transform: translateY(10px); }
      to { opacity: 1; transform: translateY(0); }
    }

    @media (max-width: 700px) {
      body { padding: 12px; }
      .hero, .statusbar, #feed { padding-left: 16px; padding-right: 16px; }
    }
  </style>
</head>
<body>
  <main class="shell">
    <section class="hero">
      <h1>Log Parser Lab</h1>
    </section>
    <section class="statusbar">
      <div class="pill"><span id="indicator" class="dot"></span><span id="status">Connecting to event stream...</span></div>
      <div class="pill">Refresh-safe SSE feed</div>
      <div class="pill">Newest event shown first</div>
    </section>
    <section id="feed"></section>
  </main>
  <script>
    const statusNode = document.getElementById('status');
    const indicator = document.getElementById('indicator');
    const feed = document.getElementById('feed');
    const eventSource = new EventSource('/events');
    const originalTitle = document.title;
    let flashing = null;

    function firstLine(value) {
      return (value || '').split('\n').find((line) => line.trim().length > 0) || '';
    }

    function startFlash() {
      if (flashing) return;
      let alerting = true;
      flashing = setInterval(() => {
        document.title = alerting ? '[new] Log Parser Lab' : originalTitle;
        alerting = !alerting;
      }, 1000);
    }

    function stopFlash() {
      if (!flashing) return;
      clearInterval(flashing);
      flashing = null;
      document.title = originalTitle;
    }

    document.addEventListener('visibilitychange', () => {
      if (!document.hidden) stopFlash();
    });

    if ('Notification' in window && Notification.permission === 'default') {
      Notification.requestPermission();
    }

    eventSource.onopen = () => {
      indicator.classList.add('live');
      statusNode.textContent = 'Connected';
    };

    function formatKind(kind) {
      switch (kind) {
        case 'setup': return 'Preparing';
        case 'ready': return 'Ready';
        case 'attempt': return 'Checked answer';
        case 'completed': return 'Completed';
        case 'reset': return 'Resetting';
        case 'error': return 'Error';
        default:
          return kind ? kind.charAt(0).toUpperCase() + kind.slice(1) : '';
      }
    }

    function renderInstructionSummary(summary) {
      const fragment = document.createDocumentFragment();
      const lines = (summary || '').split('\n').map((line) => line.trim()).filter(Boolean);

      for (const line of lines) {
        const row = document.createElement('div');
        row.className = 'summary-line';

        if (line.startsWith('Output: ')) {
          const key = document.createElement('strong');
          key.className = 'summary-key';
          key.textContent = 'Output:';
          row.appendChild(key);
          row.appendChild(document.createTextNode(' ' + line.slice('Output: '.length)));
        } else if (line.startsWith('Suggested tools: ')) {
          const key = document.createElement('strong');
          key.className = 'summary-key';
          key.textContent = 'Suggested Tools:';
          row.appendChild(key);
          row.appendChild(document.createTextNode(' ' + line.slice('Suggested tools: '.length)));
        } else {
          row.textContent = line;
        }

        fragment.appendChild(row);
      }

      return fragment;
    }

    function renderMessageContent(message) {
      const fragment = document.createDocumentFragment();
      const lines = (message || '').split('\n');
      const fence = String.fromCharCode(96).repeat(3);
      let prose = [];
      let code = [];
      let inCode = false;

      function flushProse() {
        const text = prose.join('\n').trim();
        prose = [];
        if (!text) return;

        const block = document.createElement('div');
        block.className = 'message-copy';
        block.textContent = text;
        fragment.appendChild(block);
      }

      function flushCode() {
        const text = code.join('\n').trim();
        code = [];
        if (!text) return;

        const pre = document.createElement('pre');
        pre.className = 'message-code';

        const codeNode = document.createElement('code');
        codeNode.textContent = text;
        pre.appendChild(codeNode);
        fragment.appendChild(pre);
      }

      for (const line of lines) {
        if (line.startsWith(fence)) {
          if (inCode) {
            flushCode();
          } else {
            flushProse();
          }
          inCode = !inCode;
          continue;
        }

        if (inCode) {
          code.push(line);
        } else {
          prose.push(line);
        }
      }

      if (inCode) {
        flushCode();
      } else {
        flushProse();
      }

      return fragment;
    }

    eventSource.onmessage = (event) => {
      const payload = JSON.parse(event.data);
      const article = document.createElement('article');
      article.className = 'entry';
      article.dataset.kind = payload.kind || 'info';

      const meta = document.createElement('div');
      meta.className = 'meta';
      meta.textContent = [new Date().toLocaleTimeString(), formatKind(payload.kind)].filter(Boolean).join('  •  ');

      const message = document.createElement('div');
      message.className = 'message';
      message.appendChild(renderMessageContent(payload.message));

      article.appendChild(meta);
      if (payload.instructionSummary) {
        const summary = document.createElement('div');
        summary.className = 'summary';

        const label = document.createElement('div');
        label.className = 'summary-label';
        label.textContent = 'Activity summary';

        const text = document.createElement('div');
        text.className = 'summary-text';
        text.appendChild(renderInstructionSummary(payload.instructionSummary));

        summary.appendChild(label);
        summary.appendChild(text);
        article.appendChild(summary);
      }
      article.appendChild(message);
      feed.prepend(article);

      while (feed.children.length > 50) {
        feed.removeChild(feed.lastChild);
      }

      if (document.hidden) {
        startFlash();
        if ('Notification' in window && Notification.permission === 'granted') {
          const notificationTitle = formatKind(payload.kind) ? 'Log Parser Lab: ' + formatKind(payload.kind) : 'Log Parser Lab';
          const notificationBody = [firstLine(payload.message), payload.instructionSummary].filter(Boolean).join('\n');
          const n = new Notification(notificationTitle, { body: notificationBody || payload.message });
          n.onclick = () => {
            window.focus();
            n.close();
          };
        }
      }
    };

    eventSource.onerror = () => {
      indicator.classList.remove('live');
      statusNode.textContent = 'Disconnected, retrying...';
    };
  </script>
</body>
</html>`

	setDocumentHeaders(w)
	_, _ = w.Write([]byte(html))
}

func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	setEventStreamHeaders(w)

	eventCh, cleanup := s.notifier.Subscribe()
	defer cleanup()

	keepAlive := time.NewTicker(15 * time.Second)
	defer keepAlive.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case <-keepAlive.C:
			_, _ = fmt.Fprint(w, ": keep-alive\n\n")
			flusher.Flush()
		case event, ok := <-eventCh:
			if !ok {
				return
			}

			payload, err := json.Marshal(event)
			if err != nil {
				ctrl.Log.WithName("notifier-server").Error(err, "marshal notification event")
				continue
			}

			_, _ = fmt.Fprintf(w, "data: %s\n\n", payload)
			flusher.Flush()
		}
	}
}
