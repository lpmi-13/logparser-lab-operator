package challenges

import (
	"math/rand"
	"regexp"
	"strings"
	"testing"
)

func TestSyslogScenarioUsesISO8601TimestampsAndRuntimeHostname(t *testing.T) {
	activity, ok := Lookup("syslog-top-error-service")
	if !ok {
		t.Fatal("expected syslog-top-error-service to exist")
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
			t.Fatalf("line %d did not match expected syslog format: %q", i, scenario.Lines[i])
		}
	}
}

func TestRandomSyslogRecordIncludesSystemPrograms(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	timeline := newSyslogTimeline(scenarioDay(rng), rng)
	seen := make(map[string]bool)

	for range 4000 {
		record := randomSyslogRecord(timeline, rng)
		seen[record.Program] = true
	}

	for _, program := range []string{"kernel", "dhclient", "CRON", "systemd"} {
		if !seen[program] {
			t.Fatalf("expected to see %s in generated syslog records", program)
		}
	}
}

func TestRandomSyslogRecordKeepsAmbientErrorsRare(t *testing.T) {
	rng := rand.New(rand.NewSource(99))
	timeline := newSyslogTimeline(scenarioDay(rng), rng)
	errorLines := 0
	const total = 4000

	for range total {
		record := randomSyslogRecord(timeline, rng)
		if strings.Contains(strings.ToLower(record.Message), "error") {
			errorLines++
		}
	}

	if errorLines == 0 {
		t.Fatal("expected some error lines to remain present")
	}

	if ratio := float64(errorLines) / float64(total); ratio > 0.08 {
		t.Fatalf("expected ambient error ratio to stay low, got %.3f", ratio)
	}
}
