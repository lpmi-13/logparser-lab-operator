package challenges

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
)

const DefaultScenarioLineCount = 5000

// Activity describes one learner exercise.
type Activity struct {
	ID             string
	Title          string
	SuggestedTools []string
}

// Scenario is a generated round with a single active log file.
type Scenario struct {
	ActivityID      string
	Title           string
	LogName         string
	Question        string
	OutputFormat    string
	SuggestedTools  []string
	ExpectedAnswer  string
	InstructionHint string
	Lines           []string
}

// InstructionSummary returns a compact learner-facing summary for notifications.
func (s Scenario) InstructionSummary() string {
	parts := []string{s.Question}
	if s.OutputFormat != "" {
		parts = append(parts, "Output: "+s.OutputFormat)
	}
	if len(s.SuggestedTools) > 0 {
		parts = append(parts, "Suggested tools: "+strings.Join(s.SuggestedTools, ", "))
	}
	if s.InstructionHint != "" {
		parts = append(parts, s.InstructionHint)
	}
	return strings.Join(parts, "\n")
}

// WriteLog writes the generated scenario log file into the managed logs directory.
func (s Scenario) WriteLog(logsDir string) (string, error) {
	if err := os.MkdirAll(logsDir, 0o755); err != nil {
		return "", err
	}
	logPath := filepath.Join(logsDir, s.LogName)
	content := strings.Join(s.Lines, "\n")
	if content != "" {
		content += "\n"
	}
	if err := os.WriteFile(logPath, []byte(content), 0o644); err != nil {
		return "", err
	}
	return logPath, nil
}

var catalog = []Activity{
	{
		ID:             "apache-unique-ips",
		Title:          "Count Unique IPs",
		SuggestedTools: []string{"grep", "awk", "sort", "uniq", "wc"},
	},
	{
		ID:             "apache-top-ip",
		Title:          "Find The Busiest IP",
		SuggestedTools: []string{"grep", "awk", "sort", "uniq", "head"},
	},
	{
		ID:             "apache-404-unique-ips",
		Title:          "Count Error Sources",
		SuggestedTools: []string{"grep", "awk", "sort", "uniq", "wc"},
	},
	{
		ID:    "apache-top-downloads",
		Title: "Rank Download Paths",
		SuggestedTools: []string{
			"grep", "awk", "sort", "uniq", "head", "sed",
		},
	},
	{
		ID:             "apache-top-user-agent",
		Title:          "Find The Most Common User Agent",
		SuggestedTools: []string{"grep", "awk", "sort", "uniq", "head"},
	},
	{
		ID:             "nginx-success-rate",
		Title:          "Calculate Success Rate",
		SuggestedTools: []string{"grep", "awk"},
	},
	{
		ID:             "nginx-top-bytes-ip",
		Title:          "Sum Bytes By IP",
		SuggestedTools: []string{"grep", "awk", "sort", "head"},
	},
	{
		ID:             "ssh-top-failed-user",
		Title:          "Find The Most Targeted Username",
		SuggestedTools: []string{"grep", "awk", "sed", "sort", "uniq", "head"},
	},
	{
		ID:             "ssh-distinct-failed-ips",
		Title:          "Count Failed SSH Sources",
		SuggestedTools: []string{"grep", "awk", "sort", "uniq", "wc"},
	},
	{
		ID:             "ssh-top-success-user",
		Title:          "Find The Most Successful SSH User",
		SuggestedTools: []string{"grep", "awk", "sort", "uniq", "head"},
	},
	{
		ID:             "syslog-sudo-auth-failures",
		Title:          "Count Sudo Authentication Failures",
		SuggestedTools: []string{"grep", "wc"},
	},
	{
		ID:             "syslog-top-error-service",
		Title:          "Find The Noisiest Error Source",
		SuggestedTools: []string{"grep", "awk", "sed", "sort", "uniq", "head"},
	},
}

// All returns a copy of the activity catalog.
func All() []Activity {
	out := make([]Activity, len(catalog))
	copy(out, catalog)
	return out
}

// Lookup returns an activity by ID.
func Lookup(id string) (Activity, bool) {
	for _, activity := range catalog {
		if activity.ID == id {
			return activity, true
		}
	}
	return Activity{}, false
}

// Select chooses either a fixed activity or a random unseen activity.
func Select(rng *rand.Rand, completed []string, requested string) (Activity, []string, error) {
	if requested != "" && requested != "random" {
		activity, ok := Lookup(requested)
		if !ok {
			return Activity{}, completed, fmt.Errorf("unknown activity %q", requested)
		}
		return activity, completed, nil
	}

	completedSet := make(map[string]struct{}, len(completed))
	for _, id := range completed {
		completedSet[id] = struct{}{}
	}

	remaining := make([]Activity, 0, len(catalog))
	for _, activity := range catalog {
		if _, seen := completedSet[activity.ID]; !seen {
			remaining = append(remaining, activity)
		}
	}

	if len(remaining) == 0 {
		completed = nil
		remaining = append(remaining, catalog...)
	}

	chosen := remaining[rng.Intn(len(remaining))]
	nextCompleted := append(append([]string(nil), completed...), chosen.ID)
	return chosen, nextCompleted, nil
}

// NormalizeAnswer trims blank lines and normalizes repeated whitespace per line.
func NormalizeAnswer(raw string) string {
	raw = strings.ReplaceAll(raw, "\r\n", "\n")
	lines := strings.Split(raw, "\n")
	normalized := make([]string, 0, len(lines))

	for _, line := range lines {
		fields := strings.Fields(strings.TrimSpace(line))
		if len(fields) == 0 {
			continue
		}
		normalized = append(normalized, strings.Join(fields, " "))
	}

	return strings.Join(normalized, "\n")
}
