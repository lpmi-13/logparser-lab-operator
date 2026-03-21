package controller

import (
	"strings"
	"testing"

	"github.com/lpmi-13/logparser-lab-operator/internal/challenges"
)

func TestRenderChallengeTextShowsOnlyExecutionDetails(t *testing.T) {
	reconciler := &LogParserLabReconciler{}
	scenario := challenges.Scenario{
		Title:          "Count 4XX Sources",
		Question:       "How many unique client IPs received a 429 response for paths under /admin/ during the 15:00-15:59 hour in apache_access.log?",
		OutputFormat:   "Write only the number.",
		SuggestedTools: []string{"grep", "awk", "sort", "uniq", "wc"},
	}

	got := reconciler.renderChallengeText("/tmp/log-lab/answer.txt", "/tmp/logs/apache_access.log", scenario)

	checks := []struct {
		needle string
		want   bool
	}{
		{needle: "Round 1: Count 4XX Sources", want: false},
		{needle: "Log file: /tmp/logs/apache_access.log", want: true},
		{needle: "Answer file: /tmp/log-lab/answer.txt", want: true},
		{needle: "Only the current round log exists in the managed logs directory.", want: false},
		{needle: "Run your pipeline directly on the VM filesystem and redirect stdout to the answer file:", want: true},
		{needle: "How many unique client IPs received a 429 response", want: false},
		{needle: "Output format:", want: false},
		{needle: "Suggested tools:", want: false},
	}

	for _, check := range checks {
		has := strings.Contains(got, check.needle)
		if has != check.want {
			t.Fatalf("renderChallengeText() contains %q = %t, want %t\nfull text:\n%s", check.needle, has, check.want, got)
		}
	}
}
