package notifier

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestHandleEventsStreamsJSONOnly(t *testing.T) {
	n := New()
	s := NewServer(n, 8888)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	req := httptest.NewRequest("GET", "/events", nil).WithContext(ctx)
	rec := httptest.NewRecorder()

	done := make(chan struct{})
	go func() {
		s.handleEvents(rec, req)
		close(done)
	}()

	// Wait for subscription registration.
	deadline := time.Now().Add(500 * time.Millisecond)
	for {
		n.mu.RLock()
		count := len(n.subscribers)
		n.mu.RUnlock()
		if count == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("subscriber was not registered in time")
		}
		time.Sleep(10 * time.Millisecond)
	}

	n.SendEvent("lab1", Event{
		Message:            "hello",
		Kind:               "ready",
		ChallengeID:        "log-lab-1",
		InstructionSummary: "Question: summarize the activity instructions.",
	})

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("handleEvents did not exit after context cancellation")
	}

	body := rec.Body.String()
	if strings.Contains(body, "Connected to notification stream") {
		t.Fatal("unexpected legacy plain-text connection message in SSE stream")
	}

	lines := strings.Split(body, "\n")
	var payload string
	for _, line := range lines {
		if strings.HasPrefix(line, "data: ") {
			payload = strings.TrimPrefix(line, "data: ")
			break
		}
	}
	if payload == "" {
		t.Fatal("expected at least one data line in SSE response")
	}

	var evt Event
	if err := json.Unmarshal([]byte(payload), &evt); err != nil {
		t.Fatalf("failed to parse SSE payload as JSON: %v", err)
	}
	if evt.Message != "hello" {
		t.Fatalf("expected message %q, got %q", "hello", evt.Message)
	}
	if evt.ChallengeID != "log-lab-1" {
		t.Fatalf("expected challenge ID %q, got %q", "log-lab-1", evt.ChallengeID)
	}
	if evt.InstructionSummary != "Question: summarize the activity instructions." {
		t.Fatalf("expected instruction summary to be preserved, got %q", evt.InstructionSummary)
	}
}

func TestHandleRootIncludesInstructionSummaryUI(t *testing.T) {
	s := NewServer(New(), 8888)
	req := httptest.NewRequest("GET", "/", nil)
	rec := httptest.NewRecorder()

	s.handleRoot(rec, req)

	body := rec.Body.String()
	if !strings.Contains(body, "payload.instructionSummary") {
		t.Fatal("expected root page to render activity instruction summaries")
	}
	if !strings.Contains(body, "Activity summary") {
		t.Fatal("expected root page to label the instruction summary section")
	}
}
