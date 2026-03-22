package challenges

import (
	"math/rand"
	"net/netip"
	"regexp"
	"strings"
	"testing"
	"time"
)

func TestNginxScenarioUsesExtendedAccessFormat(t *testing.T) {
	activity, ok := Lookup("nginx-success-rate")
	if !ok {
		t.Fatal("expected nginx-success-rate to exist")
	}

	scenario, err := Prepare(activity, 7)
	if err != nil {
		t.Fatalf("Prepare returned error: %v", err)
	}

	linePattern := regexp.MustCompile(`^[0-9A-Fa-f:.]+ - - \[\d{2}/[A-Za-z]{3}/\d{4}:\d{2}:\d{2}:\d{2} [+-]\d{4}\] "(GET|POST|PUT|PATCH|DELETE|OPTIONS|HEAD) [^"]+ HTTP/(1\.1|2\.0|3)" \d{3} \d+ "[^"]*" "[^"]*" \d+ \d+\.\d{3} (http|https) "[^"]*" \S+ \S+ \S+$`)

	for i := 0; i < 25; i++ {
		if !linePattern.MatchString(scenario.Lines[i]) {
			t.Fatalf("line %d did not match expected nginx access format: %q", i, scenario.Lines[i])
		}
	}

	joined := strings.Join(scenario.Lines, "\n")
	if !regexp.MustCompile(`"[0-9A-Fa-f:.]+, [0-9A-Fa-f:.]+"`).MatchString(joined) {
		t.Fatal("expected at least one nginx line to include an X-Forwarded-For chain")
	}
	if !regexp.MustCompile(`"[^"]*" 10\.\d+\.\d+\.\d+:\d+`).MatchString(joined) {
		t.Fatal("expected nginx lines to include upstream fields")
	}
}

func TestRandomNginxRecordDistributionsLookRealistic(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	day := time.Date(2026, time.March, 15, 0, 0, 0, 0, time.UTC)

	var privateIPv4, publicIPv4, ipv6 int
	var getCount, postCount, unsafeCount, headOptionsCount int
	var status2xx, status3xx, status4xx, status5xx int
	var http11, http20, http3 int

	for range 4000 {
		record := randomNginxRecord(day, rng)
		addr, err := netip.ParseAddr(record.IP)
		if err != nil {
			t.Fatalf("ParseAddr(%q) returned error: %v", record.IP, err)
		}

		switch {
		case addr.Is6():
			ipv6++
		case addr.IsPrivate():
			privateIPv4++
		default:
			publicIPv4++
		}

		switch record.Method {
		case "GET":
			getCount++
		case "POST":
			postCount++
		case "PUT", "PATCH", "DELETE":
			unsafeCount++
		case "HEAD", "OPTIONS":
			headOptionsCount++
		}

		switch record.Status / 100 {
		case 2:
			status2xx++
		case 3:
			status3xx++
		case 4:
			status4xx++
		case 5:
			status5xx++
		}

		switch record.Protocol {
		case "HTTP/1.1":
			http11++
		case "HTTP/2.0":
			http20++
		case "HTTP/3":
			http3++
		default:
			t.Fatalf("unexpected protocol %q", record.Protocol)
		}
	}

	total := float64(4000)

	assertRatioBetween(t, float64(privateIPv4)/total, 0.30, 0.40, "private IPv4 ratio")
	assertRatioBetween(t, float64(publicIPv4)/total, 0.50, 0.60, "public IPv4 ratio")
	assertRatioBetween(t, float64(ipv6)/total, 0.10, 0.20, "IPv6 ratio")
	assertRatioBetween(t, float64(getCount)/total, 0.60, 0.70, "GET ratio")
	assertRatioBetween(t, float64(postCount)/total, 0.20, 0.25, "POST ratio")
	assertRatioBetween(t, float64(unsafeCount)/total, 0.00, 0.10, "PUT/PATCH/DELETE ratio")
	assertRatioBetween(t, float64(headOptionsCount)/total, 0.01, 0.03, "HEAD/OPTIONS ratio")
	assertRatioBetween(t, float64(status2xx)/total, 0.65, 0.75, "2xx ratio")
	assertRatioBetween(t, float64(status3xx)/total, 0.15, 0.20, "3xx ratio")
	assertRatioBetween(t, float64(status4xx)/total, 0.05, 0.10, "4xx ratio")
	assertRatioBetween(t, float64(status5xx)/total, 0.00, 0.02, "5xx ratio")
	assertRatioBetween(t, float64(http11)/total, 0.70, 0.80, "HTTP/1.1 ratio")
	assertRatioBetween(t, float64(http20)/total, 0.20, 0.30, "HTTP/2.0 ratio")
	assertRatioBetween(t, float64(http3)/total, 0.00, 0.05, "HTTP/3 ratio")
}

func TestNginxTopExtensionScenarioCountsExtensionsOnly(t *testing.T) {
	activity, ok := Lookup("nginx-top-extension")
	if !ok {
		t.Fatal("expected nginx-top-extension to exist")
	}

	scenario, err := Prepare(activity, 7)
	if err != nil {
		t.Fatalf("Prepare returned error: %v", err)
	}

	if !strings.Contains(scenario.Question, "Ignore query strings and paths without an extension.") {
		t.Fatalf("expected extension question to explain parsing rule, got %q", scenario.Question)
	}
	if strings.Contains(scenario.ExpectedAnswer, ".") {
		t.Fatalf("expected extension answer without leading dot, got %q", scenario.ExpectedAnswer)
	}
}

func assertRatioBetween(t *testing.T, got, low, high float64, name string) {
	t.Helper()
	if got < low || got > high {
		t.Fatalf("expected %s to be between %.3f and %.3f, got %.3f", name, low, high, got)
	}
}
