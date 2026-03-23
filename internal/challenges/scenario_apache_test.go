package challenges

import (
	"math/rand"
	"net/netip"
	"regexp"
	"strings"
	"testing"
	"time"
)

func TestApacheScenarioUsesExtendedVHostAccessFormat(t *testing.T) {
	activity, ok := Lookup("apache-unique-ips")
	if !ok {
		t.Fatal("expected apache-unique-ips to exist")
	}

	scenario, err := Prepare(activity, 7)
	if err != nil {
		t.Fatalf("Prepare returned error: %v", err)
	}

	linePattern := regexp.MustCompile(`^[a-z0-9.-]+ [0-9A-Fa-f:.]+ - (\-|[A-Za-z0-9._-]+) \[\d{2}/[A-Za-z]{3}/\d{4}:\d{2}:\d{2}:\d{2} [+-]\d{4}\] "(GET|POST|PUT|PATCH|DELETE|OPTIONS|HEAD) [^"]+ HTTP/(1\.1|2\.0)" \d{3} \d+ "[^"]*" "[^"]*" "Apache/2\.4\.58 \(Ubuntu\)" \d+ "[^"]*" (\-|TLSv1\.[23]) (\-|[A-Z0-9_-]+)$`)

	for i := 0; i < 25; i++ {
		if !linePattern.MatchString(scenario.Lines[i]) {
			t.Fatalf("line %d did not match expected apache access format: %q", i, scenario.Lines[i])
		}
	}
}

func TestRandomApacheRecordDistributionsLookApacheLike(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	day := time.Date(2026, time.March, 15, 0, 0, 0, 0, time.UTC)

	var publicIPv4, privateIPv4, ipv6 int
	var getCount, postCount, unsafeCount, headOptionsCount int
	var remoteUserCount, sslCount int
	var scannerCount, cgiCount, dirIndexCount, legacyToolCount int

	for range 4000 {
		record := randomAccessRecord(day, rng)
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

		if record.RemoteUser != "-" {
			remoteUserCount++
		}
		if record.SSLProtocol != "-" {
			sslCount++
		}

		switch apachePathCategory(record.Path) {
		case "scanner":
			scannerCount++
		case "cgi":
			cgiCount++
		case "download-index":
			dirIndexCount++
		}

		if strings.Contains(record.UserAgent, "MSIE") || strings.Contains(record.UserAgent, "Trident/") ||
			strings.HasPrefix(record.UserAgent, "curl/") || strings.HasPrefix(record.UserAgent, "Wget/") {
			legacyToolCount++
		}
	}

	total := float64(4000)

	assertRatioBetween(t, float64(publicIPv4)/total, 0.55, 0.75, "public IPv4 ratio")
	assertRatioBetween(t, float64(privateIPv4)/total, 0.08, 0.22, "private IPv4 ratio")
	assertRatioBetween(t, float64(ipv6)/total, 0.10, 0.25, "IPv6 ratio")
	assertRatioBetween(t, float64(getCount)/total, 0.65, 0.85, "GET ratio")
	assertRatioBetween(t, float64(postCount)/total, 0.08, 0.20, "POST ratio")
	assertRatioBetween(t, float64(unsafeCount)/total, 0.00, 0.08, "PUT/PATCH/DELETE ratio")
	assertRatioBetween(t, float64(headOptionsCount)/total, 0.05, 0.15, "HEAD/OPTIONS ratio")
	assertRatioBetween(t, float64(remoteUserCount)/total, 0.05, 0.18, "authenticated user ratio")
	assertRatioBetween(t, float64(sslCount)/total, 0.55, 0.85, "SSL ratio")
	assertRatioBetween(t, float64(scannerCount)/total, 0.03, 0.08, "scanner path ratio")
	assertRatioBetween(t, float64(cgiCount)/total, 0.04, 0.10, "CGI path ratio")
	assertRatioBetween(t, float64(dirIndexCount)/total, 0.04, 0.09, "directory index ratio")
	assertRatioBetween(t, float64(legacyToolCount)/total, 0.20, 0.55, "legacy or CLI user-agent ratio")
}
