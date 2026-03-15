package notifier

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	ctrl "sigs.k8s.io/controller-runtime"
)

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
      white-space: pre-wrap;
      line-height: 1.5;
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
      color: var(--muted);
      font-size: 0.75rem;
      letter-spacing: 0.08em;
      text-transform: uppercase;
    }

    .summary-text {
      line-height: 1.5;
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
      <p>Watch for new activities, successful submissions, and automatic resets while you solve each prompt with standard Linux text-processing tools.</p>
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

    eventSource.onmessage = (event) => {
      const payload = JSON.parse(event.data);
      const article = document.createElement('article');
      article.className = 'entry';
      article.dataset.kind = payload.kind || 'info';

      const meta = document.createElement('div');
      meta.className = 'meta';
      meta.textContent = [new Date().toLocaleTimeString(), payload.lab, payload.kind, payload.activityId].filter(Boolean).join('  •  ');

      const message = document.createElement('div');
      message.className = 'message';
      message.textContent = payload.message;

      article.appendChild(meta);
      if (payload.instructionSummary) {
        const summary = document.createElement('div');
        summary.className = 'summary';

        const label = document.createElement('div');
        label.className = 'summary-label';
        label.textContent = 'Activity summary';

        const text = document.createElement('div');
        text.className = 'summary-text';
        text.textContent = payload.instructionSummary;

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
          const notificationTitle = payload.activityId ? 'Log Parser Lab: ' + payload.activityId : 'Log Parser Lab';
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

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write([]byte(html))
}

func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

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
