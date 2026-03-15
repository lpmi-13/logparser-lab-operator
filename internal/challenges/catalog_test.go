package challenges

import (
	"os"
	"strings"
	"testing"
)

func TestScenarioInstructionSummary(t *testing.T) {
	scenario := Scenario{
		Question:        "How many unique client IP addresses requested paths under /api/v1/ during the 14:00-14:59 hour in apache_access.log?",
		OutputFormat:    "Write only the number.",
		SuggestedTools:  []string{"grep", "awk", "sort", "uniq", "wc"},
		InstructionHint: "Only the current round log file exists.",
	}

	got := scenario.InstructionSummary()
	want := "How many unique client IP addresses requested paths under /api/v1/ during the 14:00-14:59 hour in apache_access.log? Output: Write only the number. Suggested tools: grep, awk, sort, uniq, wc Only the current round log file exists."

	if got != want {
		t.Fatalf("expected %q, got %q", want, got)
	}
}

func TestPrepareDeterministic(t *testing.T) {
	activity, ok := Lookup("apache-top-ip")
	if !ok {
		t.Fatal("expected apache-top-ip to exist")
	}

	first, err := Prepare(activity, 42)
	if err != nil {
		t.Fatalf("Prepare returned error: %v", err)
	}
	second, err := Prepare(activity, 42)
	if err != nil {
		t.Fatalf("Prepare returned error: %v", err)
	}

	if first.Question != second.Question || first.ExpectedAnswer != second.ExpectedAnswer || first.LogName != second.LogName {
		t.Fatal("same seed should produce the same scenario metadata")
	}
	if strings.Join(first.Lines, "\n") != strings.Join(second.Lines, "\n") {
		t.Fatal("same seed should produce the same log lines")
	}
}

func TestPrepareAllActivitiesProduceLargeSingleLog(t *testing.T) {
	for _, activity := range All() {
		scenario, err := Prepare(activity, 20260314)
		if err != nil {
			t.Fatalf("Prepare(%s) returned error: %v", activity.ID, err)
		}
		if scenario.LogName == "" {
			t.Fatalf("Prepare(%s) returned an empty log name", activity.ID)
		}
		if scenario.Question == "" || scenario.OutputFormat == "" || scenario.ExpectedAnswer == "" {
			t.Fatalf("Prepare(%s) returned incomplete scenario metadata", activity.ID)
		}
		if len(scenario.Lines) != DefaultScenarioLineCount {
			t.Fatalf("Prepare(%s) generated %d lines, want %d", activity.ID, len(scenario.Lines), DefaultScenarioLineCount)
		}
	}
}

func TestScenarioWriteLog(t *testing.T) {
	activity, ok := Lookup("syslog-top-error-service")
	if !ok {
		t.Fatal("expected syslog-top-error-service to exist")
	}
	scenario, err := Prepare(activity, 7)
	if err != nil {
		t.Fatalf("Prepare returned error: %v", err)
	}

	logPath, err := scenario.WriteLog(t.TempDir())
	if err != nil {
		t.Fatalf("WriteLog returned error: %v", err)
	}
	content, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("ReadFile returned error: %v", err)
	}
	if got := strings.Count(string(content), "\n"); got != DefaultScenarioLineCount {
		t.Fatalf("expected %d newline-delimited log lines, got %d", DefaultScenarioLineCount, got)
	}
}
