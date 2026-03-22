package challenges

import (
	"math/rand"
	"regexp"
	"strings"
	"testing"
	"time"
)

func TestAuthScenarioUsesISO8601TimestampsAndRuntimeHostname(t *testing.T) {
	activity, ok := Lookup("ssh-top-failed-user")
	if !ok {
		t.Fatal("expected ssh-top-failed-user to exist")
	}

	scenario, err := Prepare(activity, 7)
	if err != nil {
		t.Fatalf("Prepare returned error: %v", err)
	}

	linePattern := regexp.MustCompile(
		`^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}[+-]\d{2}:\d{2} ` +
			regexp.QuoteMeta(scenarioHostname()) +
			` [A-Za-z0-9_.-]+\[\d+\]: .+`,
	)

	for i := 0; i < 25; i++ {
		if !linePattern.MatchString(scenario.Lines[i]) {
			t.Fatalf("line %d did not match expected auth.log format: %q", i, scenario.Lines[i])
		}
	}
}

func TestFailedAuthAttemptRecordsIncludeInvalidUserSequence(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	attemptTime := time.Date(2026, time.March, 12, 0, 4, 48, 123456000, time.UTC)
	records := failedAuthAttemptRecords(attemptTime, "jenkins", "192.0.2.37", 23930, true, "password", 1123, rng)

	if len(records) != 4 {
		t.Fatalf("expected 4 records, got %d", len(records))
	}

	if got := records[2].Message; got != "Invalid user jenkins from 192.0.2.37 port 23930" {
		t.Fatalf("unexpected invalid-user line: %q", got)
	}

	if got := records[3].Message; got != "Failed password for invalid user jenkins from 192.0.2.37 port 23930 ssh2" {
		t.Fatalf("unexpected failed-password line: %q", got)
	}
}

func TestAcceptedSSHEventIncludesPamAndSessionLifecycle(t *testing.T) {
	rng := rand.New(rand.NewSource(2))
	event := acceptedSSHEvent(newAuthTimeline(scenarioDay(rng)), 12, "alice", "203.0.113.170", false, rng)
	messages := make([]string, 0, len(event.Records))
	for _, record := range event.Records {
		messages = append(messages, record.Message)
	}
	joined := strings.Join(messages, "\n")

	for _, pattern := range []string{
		"Connection from 203.0.113.170",
		"Client protocol version 2.0; client software version ",
		"Accepted ",
		"pam_unix(sshd:session): session opened for user alice by (uid=0)",
		"New session ",
		"pam_unix(sshd:session): session closed for user alice",
	} {
		if !strings.Contains(joined, pattern) {
			t.Fatalf("expected accepted session output to contain %q, got %q", pattern, joined)
		}
	}
}

func TestServiceAccountAcceptedSSHEventUsesPublickey(t *testing.T) {
	rng := rand.New(rand.NewSource(3))
	event := acceptedSSHEvent(newAuthTimeline(scenarioDay(rng)), 9, "deploy", "198.51.100.24", false, rng)

	for _, record := range event.Records {
		if strings.HasPrefix(record.Message, "Accepted ") && !strings.HasPrefix(record.Message, "Accepted publickey ") {
			t.Fatalf("expected service account success to use publickey, got %q", record.Message)
		}
	}
}

func TestRandomAuthEventKeepsSuccessAttemptsRare(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	timeline := newAuthTimeline(scenarioDay(rng))
	successes := 0
	failures := 0

	for range 400 {
		event := randomAuthEvent(timeline, rng)
		successes += event.SuccessCount
		failures += event.FailureCount
	}

	ratio := float64(successes) / float64(successes+failures)
	if ratio < 0.05 || ratio > 0.15 {
		t.Fatalf("expected auth success ratio to stay between 5%% and 15%%, got %.3f", ratio)
	}
}

func TestSSHTopFailedUserQuestionClarifiesInvalidUserCounting(t *testing.T) {
	activity, ok := Lookup("ssh-top-failed-user")
	if !ok {
		t.Fatal("expected ssh-top-failed-user to exist")
	}

	scenario, err := Prepare(activity, 7)
	if err != nil {
		t.Fatalf("Prepare returned error: %v", err)
	}

	for _, fragment := range []string{
		`Only consider these valid usernames: alice, bob, carol, dana, monitor, ops, admin, deploy, backup, svc-ci, root.`,
		`Do not count the literal word "invalid"`,
		`ignore standalone "Invalid user ..." lines.`,
	} {
		if !strings.Contains(scenario.Question, fragment) {
			t.Fatalf("expected question to contain %q, got %q", fragment, scenario.Question)
		}
	}

	if containsString(authInvalidUsers, scenario.ExpectedAnswer) {
		t.Fatalf("expected top failed-user answer to be a valid username, got %q", scenario.ExpectedAnswer)
	}
}
