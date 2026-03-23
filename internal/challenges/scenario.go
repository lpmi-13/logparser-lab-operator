package challenges

import (
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	accessPrefixes = []string{
		"/api/v1/",
		"/downloads/",
		"/reports/",
		"/billing/",
		"/admin/",
		"/assets/",
		"/exports/",
		"/app/",
	}
	httpMethods             = []string{"GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "HEAD"}
	httpStatuses            = []int{200, 201, 204, 301, 302, 304, 400, 401, 403, 404, 409, 429, 500, 502, 503}
	apacheBrowserUserAgents = []string{
		"Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:136.0) Gecko/20100101 Firefox/136.0",
		"Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
		"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
		"Mozilla/5.0 (iPhone; CPU iPhone OS 17_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Mobile/15E148 Safari/604.1",
	}
	apacheLegacyUserAgents = []string{
		"Mozilla/5.0 (Windows NT 6.1; Trident/7.0; rv:11.0) like Gecko",
		"Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.0; Trident/4.0)",
		"Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:102.0) Gecko/20100101 Firefox/102.0",
		"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/49.0.2623.112 Safari/537.36",
	}
	apacheCLIUserAgents = []string{
		"curl/8.7.1",
		"curl/7.68.0",
		"Wget/1.21.4 (linux-gnu)",
		"python-requests/2.31.0",
		"Go-http-client/1.1",
	}
	apacheBotUserAgents = []string{
		"Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
		"Mozilla/5.0 (compatible; bingbot/2.0; +http://www.bing.com/bingbot.htm)",
	}
	apacheScannerUserAgents = []string{
		"Nikto/2.5.0",
		"Nmap Scripting Engine",
		"Mozilla/5.0 zgrab/0.x",
	}
	apacheTopUserAgentCandidates = []string{
		"Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:136.0) Gecko/20100101 Firefox/136.0",
		"Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
		"Mozilla/5.0 (Windows NT 6.1; Trident/7.0; rv:11.0) like Gecko",
		"Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.0; Trident/4.0)",
		"curl/8.7.1",
		"curl/7.68.0",
		"Wget/1.21.4 (linux-gnu)",
		"python-requests/2.31.0",
	}
	apacheInternalReferrers = []string{
		"-",
		"https://portal.example.com/dashboard",
		"https://portal.example.com/reports",
		"https://portal.example.com/downloads",
		"https://docs.example.org/legacy",
	}
	apacheSearchReferrers = []string{
		"https://www.google.com/search?q=apache+directory+listing",
		"https://search.example.net/?q=legacy+apache+cgi",
		"https://www.bing.com/search?q=apache+mod_ssl+ubuntu",
	}
	apacheSuspiciousReferrers = []string{
		"http://traffic.example-spam.invalid/hit",
		"http://adult-links.example-bad.invalid/gallery",
		"http://bonus-seo.example-bad.invalid/offer",
	}
	apacheCGIPaths = []string{
		"/cgi-bin/status.cgi",
		"/cgi-bin/printenv.pl",
		"/cgi-bin/report.pl",
		"/cgi-bin/healthcheck.cgi",
	}
	apacheScannerPaths = []string{
		"/.htaccess",
		"/.htpasswd",
		"/.htgroups",
	}
	apachePagePaths = []string{
		"/",
		"/index.html",
		"/manual/",
		"/manual/en/mod/mod_ssl.html",
		"/server-status?auto",
	}
	apacheKeepAliveValues = []string{
		"timeout=5, max=100",
		"timeout=5, max=75",
		"timeout=5, max=50",
		"timeout=15, max=100",
	}
	apacheRemoteUsers = []string{
		"alice", "bob", "carol", "dana", "ops", "admin", "deploy", "backup", "svc-ci",
	}
	apacheTLS13Ciphers = []string{
		"TLS_AES_256_GCM_SHA384",
		"TLS_CHACHA20_POLY1305_SHA256",
		"TLS_AES_128_GCM_SHA256",
	}
	apacheTLS12Ciphers = []string{
		"ECDHE-RSA-AES128-GCM-SHA256",
		"ECDHE-RSA-AES256-GCM-SHA384",
		"ECDHE-ECDSA-AES128-GCM-SHA256",
	}
	downloadLeaves = map[string][]string{
		"/downloads/": {
			"report-q1.csv", "guide.pdf", "toolkit.tar.gz", "dataset.json", "release-notes.txt", "schema.yaml",
		},
		"/exports/": {
			"customers-2026-03.csv", "billing-archive.parquet", "audit-2026-03.ndjson", "events.dump.gz", "ops-summary.txt", "inventory-2026-03.csv",
		},
		"/reports/": {
			"uptime-2026-03.html", "latency-weekly.csv", "security-review.pdf", "capacity.xlsx", "alerts.html", "compliance-2026-03.pdf",
		},
	}
	apiLeaves = []string{
		"users", "orders", "sessions", "health", "metrics", "invoices", "roles", "tokens", "alerts",
	}
	authUsers = []string{
		"alice", "bob", "carol", "dana", "monitor", "ops", "admin",
	}
	authServiceUsers = []string{"deploy", "backup", "svc-ci"}
	authInvalidUsers = []string{"jenkins", "ubuntu", "oracle", "postgres", "git", "test", "ftp", "www-data"}
	authMFAUsers     = []string{"alice", "admin", "ops"}
	authSSHClients   = []string{
		"OpenSSH_8.9p1 Ubuntu-3ubuntu0.10",
		"OpenSSH_9.6p1 Debian-3",
		"paramiko_3.4.0",
		"libssh-0.10.6",
		"PuTTY_Release_0.81",
	}
	authBotSubnets = [][3]int{
		{198, 51, 100},
		{203, 0, 113},
		{192, 0, 2},
	}
	authHostIP     = "10.0.2.15"
	syslogUsers    = []string{"alice", "bob", "deploy", "ops", "ubuntu", "analyst"}
	syslogServices = []string{"nginx", "postgres", "app-worker", "haproxy", "redis", "backup-agent"}
	syslogPrograms = []string{
		"nginx", "nginx", "nginx",
		"postgres", "postgres",
		"app-worker", "app-worker", "app-worker",
		"haproxy", "haproxy",
		"redis", "redis",
		"backup-agent", "backup-agent",
		"sudo",
		"systemd", "systemd", "systemd",
		"kernel", "kernel",
		"dhclient",
		"CRON", "CRON",
	}
	scenarioHostOnce sync.Once
	scenarioHost     string
)

var (
	nginxAssetPaths = []string{
		"/assets/app.css",
		"/assets/app.js",
		"/assets/runtime.js",
		"/assets/vendor.js",
		"/assets/logo.svg",
		"/assets/hero.webp",
		"/assets/favicon.ico",
	}
	nginxDownloadPaths = []string{
		"/downloads/toolkit.tar.gz",
		"/downloads/dataset.json",
		"/downloads/guide.pdf",
		"/exports/customers-2026-03.csv",
		"/exports/billing-archive.parquet",
		"/exports/events.dump.gz",
		"/reports/compliance-2026-03.pdf",
		"/reports/latency-weekly.csv",
	}
	nginxAPIV1Paths = []string{
		"/api/v1/health",
		"/api/v1/orders",
		"/api/v1/users",
		"/api/v1/invoices",
		"/api/v1/products",
		"/api/v1/sessions",
	}
	nginxAPIV2Paths = []string{
		"/api/v2/orders",
		"/api/v2/users",
		"/api/v2/invoices",
		"/api/v2/products",
	}
	nginxBillingPaths = []string{
		"/billing/invoices",
		"/billing/payments",
		"/billing/statements",
		"/billing/customers",
		"/billing/aging",
	}
	nginxAppPaths = []string{
		"/app/dashboard",
		"/app/projects",
		"/app/settings",
		"/app/activity",
		"/app/usage",
	}
	nginxProductPaths = []string{
		"/products/featured",
		"/products/search",
		"/products/category/observability",
		"/products/category/security",
		"/products/category/infrastructure",
	}
	nginxAdminPaths = []string{
		"/admin/users",
		"/admin/roles",
		"/admin/audit",
		"/admin/feature-flags",
	}
	nginxScannerPaths = []string{
		"/wp-admin/install.php",
		"/phpmyadmin/index.php",
		"/.env",
		"/cgi-bin/luci",
		"/boaform/admin/formLogin",
	}
	nginxLandingPaths = []string{
		"/",
		"/pricing",
		"/docs/getting-started",
		"/status",
	}
	nginxBrowserUserAgents = []string{
		"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
		"Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
		"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
	}
	nginxLegacyUserAgents = []string{
		"Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:98.0) Gecko/20100101 Firefox/98.0",
		"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.114 Safari/537.36",
		"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7; rv:97.0) Gecko/20100101 Firefox/97.0",
	}
	nginxMobileUserAgents = []string{
		"Mozilla/5.0 (iPhone; CPU iPhone OS 17_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Mobile/15E148 Safari/604.1",
		"Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.6312.99 Mobile Safari/537.36",
		"Mozilla/5.0 (Linux; Android 13; SAMSUNG SM-S918B) AppleWebKit/537.36 (KHTML, like Gecko) SamsungBrowser/25.0 Chrome/121.0.0.0 Mobile Safari/537.36",
	}
	nginxBotUserAgents = []string{
		"Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
		"Mozilla/5.0 (compatible; bingbot/2.0; +http://www.bing.com/bingbot.htm)",
	}
	nginxScannerUserAgents = []string{
		"Nikto/2.5.0",
		"Nmap Scripting Engine",
		"Mozilla/5.0 zgrab/0.x",
	}
	nginxAPIClientUserAgents = []string{
		"curl/8.7.1",
		"python-requests/2.32.3",
		"Go-http-client/2.0",
		"invoice-sync/1.14.2",
		"partner-api-client/3.8.1",
	}
	nginxInternalReferrers = []string{
		"https://portal.example.com/dashboard",
		"https://portal.example.com/reports",
		"https://app.example.com/",
		"https://portal.example.com/billing",
	}
	nginxExternalReferrers = []string{
		"https://www.google.com/search?q=example+products",
		"https://www.google.com/search?q=portal+example+docs",
		"https://www.bing.com/search?q=example+monitoring",
		"https://www.linkedin.com/posts/examplecorp_release-notes",
	}
	nginxSocialReferrers = []string{
		"https://t.co/example",
		"https://www.reddit.com/r/devops/comments/example",
	}
	nginxLoadBalancerIPs = []string{
		"10.26.237.69",
		"10.26.237.70",
		"172.16.14.23",
	}
)

type accessRecord struct {
	Timestamp            time.Time
	VHost                string
	IP                   string
	RemoteUser           string
	Method               string
	Path                 string
	Status               int
	Bytes                int
	Referrer             string
	UserAgent            string
	ServerHeader         string
	RequestDurationMicro int
	KeepAlive            string
	SSLProtocol          string
	SSLCipher            string
	Protocol             string
	RequestLength        int
	RequestTime          float64
	Scheme               string
	ForwardedFor         string
	UpstreamAddr         string
	UpstreamResponseTime string
	UpstreamStatus       string
}

type authRecord struct {
	Timestamp time.Time
	Host      string
	Program   string
	PID       int
	Message   string
}

type authTimeline struct {
	day time.Time
}

type authEvent struct {
	Records      []authRecord
	AttemptHour  int
	User         string
	FailureCount int
	SuccessCount int
	SourceIPs    []string
}

type syslogRecord struct {
	Timestamp time.Time
	Host      string
	Program   string
	PID       int
	Message   string
}

type syslogTimeline struct {
	day            time.Time
	minuteClusters [24][]int
}

type nginxTimeline struct {
	day time.Time
}

// Prepare generates a deterministic round scenario from the activity template and seed.
func Prepare(activity Activity, seed int64) (Scenario, error) {
	switch activity.ID {
	case "apache-unique-ips":
		return prepareApacheUniqueIPs(activity, seed)
	case "apache-top-ip":
		return prepareApacheTopIP(activity, seed)
	case "apache-404-unique-ips":
		return prepareApacheStatusUniqueIPs(activity, seed)
	case "apache-top-downloads":
		return prepareApacheTopDownloads(activity, seed)
	case "apache-top-user-agent":
		return prepareApacheTopUserAgent(activity, seed)
	case "nginx-success-rate":
		return prepareNginxSuccessRate(activity, seed)
	case "nginx-top-bytes-ip":
		return prepareNginxTopBytesIP(activity, seed)
	case "nginx-top-extension":
		return prepareNginxTopExtension(activity, seed)
	case "ssh-top-failed-user":
		return prepareSSHTopFailedUser(activity, seed)
	case "ssh-distinct-failed-ips":
		return prepareSSHDistinctFailedIPs(activity, seed)
	case "ssh-top-success-user":
		return prepareSSHTopSuccessUser(activity, seed)
	case "syslog-sudo-auth-failures":
		return prepareSyslogSudoFailures(activity, seed)
	case "syslog-top-error-service":
		return prepareSyslogTopErrorService(activity, seed)
	default:
		return Scenario{}, fmt.Errorf("unknown activity %q", activity.ID)
	}
}

func prepareApacheUniqueIPs(activity Activity, seed int64) (Scenario, error) {
	rng := rand.New(rand.NewSource(seed))
	day := scenarioDay(rng)
	hour := rng.Intn(24)
	prefix := pickOne(rng, []string{"/api/v1/", "/downloads/", "/reports/", "/billing/", "/admin/"})
	uniqueCount := 20 + rng.Intn(45)
	relevantIPs := uniquePublicIPs(rng, uniqueCount)
	records := make([]accessRecord, 0, DefaultScenarioLineCount)

	for _, ip := range relevantIPs {
		repeats := 1 + rng.Intn(4)
		for range repeats {
			path := randomPathUnderPrefix(prefix, rng)
			records = append(records, buildApacheRecord(
				withinHour(day, hour, rng),
				ip,
				apacheMethodForPath(path, rng),
				path,
				pickOne(rng, []int{200, 201, 204, 302}),
				rng,
			))
		}
	}

	for len(records) < uniqueCount*5 {
		path := randomPathUnderPrefix(prefix, rng)
		records = append(records, buildApacheRecord(
			withinHour(day, hour, rng),
			pickOne(rng, relevantIPs),
			apacheMethodForPath(path, rng),
			path,
			pickOne(rng, []int{200, 200, 201, 204, 304}),
			rng,
		))
	}

	for len(records) < DefaultScenarioLineCount {
		record := randomAccessRecord(day, rng)
		if record.Timestamp.Hour() == hour && strings.HasPrefix(record.Path, prefix) {
			continue
		}
		records = append(records, record)
	}

	return newAccessScenario(activity, "apache_access.log",
		fmt.Sprintf("How many unique client IP addresses requested paths under %s during the %s hour in apache_access.log?", prefix, hourWindow(hour)),
		"Write only the number.",
		strconv.Itoa(uniqueCount),
		records,
	), nil
}

func prepareApacheTopIP(activity Activity, seed int64) (Scenario, error) {
	rng := rand.New(rand.NewSource(seed))
	day := scenarioDay(rng)
	hour := rng.Intn(24)
	status := pickOne(rng, []int{200, 301, 404, 500, 503})
	targetIPs := uniquePublicIPs(rng, 7)
	targetIP := targetIPs[0]
	records := make([]accessRecord, 0, DefaultScenarioLineCount)
	targetCount := 130 + rng.Intn(40)
	counts := []int{targetCount, targetCount - 17, targetCount - 31, targetCount - 44, targetCount - 58, targetCount - 71, targetCount - 84}

	for i, ip := range targetIPs {
		for range counts[i] {
			path := randomPathUnderPrefix(pickOne(rng, accessPrefixes), rng)
			records = append(records, buildApacheRecord(
				withinHour(day, hour, rng),
				ip,
				apacheMethodForPath(path, rng),
				path,
				status,
				rng,
			))
		}
	}

	for len(records) < DefaultScenarioLineCount {
		record := randomAccessRecord(day, rng)
		if record.Timestamp.Hour() == hour && record.Status == status {
			continue
		}
		records = append(records, record)
	}

	return newAccessScenario(activity, "apache_access.log",
		fmt.Sprintf("Which client IP generated the most %d responses during the %s hour in apache_access.log?", status, hourWindow(hour)),
		"Write only the IP address.",
		targetIP,
		records,
	), nil
}

func prepareApacheStatusUniqueIPs(activity Activity, seed int64) (Scenario, error) {
	rng := rand.New(rand.NewSource(seed))
	day := scenarioDay(rng)
	hour := rng.Intn(24)
	status := pickOne(rng, []int{401, 403, 404, 429, 500})
	prefix := pickOne(rng, []string{"/admin/", "/billing/", "/reports/", "/api/v1/"})
	uniqueCount := 6 + rng.Intn(18)
	relevantIPs := uniquePublicIPs(rng, uniqueCount)
	records := make([]accessRecord, 0, DefaultScenarioLineCount)

	for _, ip := range relevantIPs {
		for range 1 + rng.Intn(5) {
			path := randomPathUnderPrefix(prefix, rng)
			records = append(records, buildApacheRecord(
				withinHour(day, hour, rng),
				ip,
				apacheMethodForPath(path, rng),
				path,
				status,
				rng,
			))
		}
	}

	for len(records) < uniqueCount*6 {
		path := randomPathUnderPrefix(prefix, rng)
		records = append(records, buildApacheRecord(
			withinHour(day, hour, rng),
			pickOne(rng, relevantIPs),
			apacheMethodForPath(path, rng),
			path,
			status,
			rng,
		))
	}

	for len(records) < DefaultScenarioLineCount {
		record := randomAccessRecord(day, rng)
		if record.Timestamp.Hour() == hour && record.Status == status && strings.HasPrefix(record.Path, prefix) {
			continue
		}
		records = append(records, record)
	}

	scenario := newAccessScenario(activity, "apache_access.log",
		fmt.Sprintf("How many unique client IPs received a %d response for paths under %s during the %s hour in apache_access.log?", status, prefix, hourWindow(hour)),
		"Write only the number.",
		strconv.Itoa(uniqueCount),
		records,
	)
	scenario.Title = apacheStatusSourceTitle(activity.Title, status)
	return scenario, nil
}

func apacheStatusSourceTitle(defaultTitle string, status int) string {
	switch status / 100 {
	case 4:
		return "Count 4XX Sources"
	case 5:
		return "Count 5XX Sources"
	default:
		return defaultTitle
	}
}

func prepareApacheTopDownloads(activity Activity, seed int64) (Scenario, error) {
	rng := rand.New(rand.NewSource(seed))
	day := scenarioDay(rng)
	hour := rng.Intn(24)
	prefix := pickOne(rng, []string{"/downloads/", "/exports/", "/reports/"})
	pathPool := append([]string(nil), downloadLeaves[prefix]...)
	if len(pathPool) < 6 {
		return Scenario{}, fmt.Errorf("path pool for %s is too small", prefix)
	}
	shuffleStrings(rng, pathPool)
	topPaths := []string{
		prefix + pathPool[0],
		prefix + pathPool[1],
		prefix + pathPool[2],
	}
	otherPaths := []string{
		prefix + pathPool[3],
		prefix + pathPool[4],
		prefix + pathPool[5],
	}
	counts := []int{116 + rng.Intn(20), 88 + rng.Intn(15), 63 + rng.Intn(12), 34 + rng.Intn(8), 21 + rng.Intn(6), 12 + rng.Intn(4)}
	paths := append(append([]string(nil), topPaths...), otherPaths...)
	records := make([]accessRecord, 0, DefaultScenarioLineCount)
	ipPool := uniquePublicIPs(rng, 24)

	for i, path := range paths {
		for range counts[i] {
			records = append(records, buildApacheRecord(
				withinHour(day, hour, rng),
				pickOne(rng, ipPool),
				"GET",
				path,
				pickOne(rng, []int{200, 200, 206, 304}),
				rng,
			))
		}
	}

	for len(records) < DefaultScenarioLineCount {
		record := randomAccessRecord(day, rng)
		if record.Timestamp.Hour() == hour && strings.HasPrefix(record.Path, prefix) {
			continue
		}
		records = append(records, record)
	}

	answer := strings.Join([]string{
		fmt.Sprintf("%d %s", counts[0], topPaths[0]),
		fmt.Sprintf("%d %s", counts[1], topPaths[1]),
		fmt.Sprintf("%d %s", counts[2], topPaths[2]),
	}, "\n")

	return newAccessScenario(activity, "apache_access.log",
		fmt.Sprintf("List the top 3 requested paths under %s during the %s hour in apache_access.log. Sort by count descending.", prefix, hourWindow(hour)),
		"Write exactly three lines as COUNT PATH with single spaces.",
		answer,
		records,
	), nil
}

func prepareApacheTopUserAgent(activity Activity, seed int64) (Scenario, error) {
	rng := rand.New(rand.NewSource(seed))
	day := scenarioDay(rng)
	hour := rng.Intn(24)
	method := pickOne(rng, []string{"GET", "POST"})
	prefix := pickOne(rng, []string{"/api/v1/", "/app/", "/billing/", "/reports/"})
	uas := append([]string(nil), apacheTopUserAgentCandidates...)
	shuffleStrings(rng, uas)
	targetUA := uas[0]
	counts := []int{104 + rng.Intn(18), 83 + rng.Intn(12), 61 + rng.Intn(10), 44 + rng.Intn(8)}
	records := make([]accessRecord, 0, DefaultScenarioLineCount)
	ipPool := uniquePublicIPs(rng, 18)

	for i := range counts {
		for range counts[i] {
			path := randomPathUnderPrefix(prefix, rng)
			record := buildApacheRecord(
				withinHour(day, hour, rng),
				pickOne(rng, ipPool),
				method,
				path,
				pickOne(rng, []int{200, 200, 201, 304}),
				rng,
			)
			record.UserAgent = uas[i]
			record.Referrer = apacheReferrer(apachePathCategory(path), path, record.UserAgent, rng)
			record.SSLProtocol, record.SSLCipher = apacheSSLDetails(apachePathCategory(path), path, record.VHost, record.UserAgent, rng)
			record.Protocol = apacheProtocol(record.SSLProtocol != "-", record.UserAgent, rng)
			record.KeepAlive = apacheKeepAlive(record.Method, record.Protocol, rng)
			records = append(records, record)
		}
	}

	for len(records) < DefaultScenarioLineCount {
		record := randomAccessRecord(day, rng)
		if record.Timestamp.Hour() == hour && record.Method == method && strings.HasPrefix(record.Path, prefix) {
			continue
		}
		records = append(records, record)
	}

	return newAccessScenario(activity, "apache_access.log",
		fmt.Sprintf("Which user agent made the most %s requests to paths under %s during the %s hour in apache_access.log?", method, prefix, hourWindow(hour)),
		"Write the exact user agent string.",
		targetUA,
		records,
	), nil
}

func prepareNginxSuccessRate(activity Activity, seed int64) (Scenario, error) {
	rng := rand.New(rand.NewSource(seed))
	day := scenarioDay(rng)
	hour := rng.Intn(24)
	timeline := newNginxTimeline(day)
	prefix := pickOne(rng, []string{"/api/v1/", "/api/v2/", "/app/", "/assets/", "/billing/"})
	total := 220 + rng.Intn(180)
	successRatio := 0.68 + float64(rng.Intn(7))/100
	successCount := int(float64(total) * successRatio)
	records := make([]accessRecord, 0, DefaultScenarioLineCount)
	ipPool := uniqueNginxClientIPs(rng, 40)

	for i := 0; i < successCount; i++ {
		path := randomNginxPathUnderPrefix(prefix, rng)
		method := nginxMethodForPath(path, rng)
		status := nginxSuccessStatus(path, method, rng)
		records = append(records, buildNginxRecord(
			timeline.withinHour(hour, rng),
			pickOne(rng, ipPool),
			method,
			path,
			status,
			rng.Intn(4) == 0,
			rng,
		))
	}
	for i := successCount; i < total; i++ {
		path := randomNginxPathUnderPrefix(prefix, rng)
		method := nginxMethodForPath(path, rng)
		status := nginxNonSuccessStatus(path, method, rng)
		records = append(records, buildNginxRecord(
			timeline.withinHour(hour, rng),
			pickOne(rng, ipPool),
			method,
			path,
			status,
			rng.Intn(3) == 0,
			rng,
		))
	}
	for len(records) < DefaultScenarioLineCount {
		record := randomNginxRecord(day, rng)
		if record.Timestamp.Hour() == hour && strings.HasPrefix(record.Path, prefix) {
			continue
		}
		records = append(records, record)
	}

	answer := fmt.Sprintf("%.2f%%", float64(successCount)/float64(total)*100)
	return newNginxScenario(activity, "nginx_access.log",
		fmt.Sprintf("What percentage of requests to paths under %s during the %s hour in nginx_access.log returned a 2xx status?", prefix, hourWindow(hour)),
		"Write the percentage with two decimal places followed by %.",
		answer,
		records,
	), nil
}

func prepareNginxTopBytesIP(activity Activity, seed int64) (Scenario, error) {
	rng := rand.New(rand.NewSource(seed))
	day := scenarioDay(rng)
	hour := rng.Intn(24)
	timeline := newNginxTimeline(day)
	method := pickOne(rng, []string{"GET", "POST"})
	ips := uniqueNginxClientIPs(rng, 6)
	targetIP := ips[0]
	records := make([]accessRecord, 0, DefaultScenarioLineCount)
	totals := make(map[string]int, len(ips))

	for i, ip := range ips {
		lineCount := 14 + (len(ips)-i)*5 + rng.Intn(3)
		for range lineCount {
			path := nginxHighBytesPath(method, i == 0, rng)
			status := nginxTopBytesStatus(path, method, rng)
			record := buildNginxRecord(
				timeline.withinHour(hour, rng),
				ip,
				method,
				path,
				status,
				false,
				rng,
			)
			totals[ip] += record.Bytes
			records = append(records, record)
		}
	}

	for len(records) < DefaultScenarioLineCount {
		record := randomNginxRecord(day, rng)
		if record.Timestamp.Hour() == hour && record.Method == method {
			continue
		}
		records = append(records, record)
	}

	return newNginxScenario(activity, "nginx_access.log",
		fmt.Sprintf("Which client IP transferred the most total bytes for %s requests during the %s hour in nginx_access.log?", method, hourWindow(hour)),
		"Write IP BYTES with a single space.",
		fmt.Sprintf("%s %d", targetIP, totals[targetIP]),
		records,
	), nil
}

func prepareNginxTopExtension(activity Activity, seed int64) (Scenario, error) {
	rng := rand.New(rand.NewSource(seed))
	day := scenarioDay(rng)
	hour := rng.Intn(24)
	timeline := newNginxTimeline(day)
	extensions := []string{"js", "css", "pdf", "png", "json"}
	shuffleStrings(rng, extensions)
	targetExt := extensions[0]
	counts := []int{88 + rng.Intn(16), 69 + rng.Intn(12), 51 + rng.Intn(10), 34 + rng.Intn(8), 21 + rng.Intn(6)}
	records := make([]accessRecord, 0, DefaultScenarioLineCount)
	ipPool := uniqueNginxClientIPs(rng, 30)

	for i, ext := range extensions {
		for range counts[i] {
			path := nginxPathForExtension(ext, rng)
			record := buildNginxRecord(
				timeline.withinHour(hour, rng),
				pickOne(rng, ipPool),
				"GET",
				path,
				pickOne(rng, []int{200, 200, 200, 206}),
				rng.Intn(5) == 0,
				rng,
			)
			records = append(records, record)
		}
	}

	for len(records) < DefaultScenarioLineCount {
		record := randomNginxRecord(day, rng)
		if record.Timestamp.Hour() == hour && record.Method == "GET" && record.Status/100 == 2 && nginxPathExtension(record.Path) != "" {
			continue
		}
		records = append(records, record)
	}

	return newNginxScenario(activity, "nginx_access.log",
		fmt.Sprintf("Which file extension had the most 2xx GET requests during the %s hour in nginx_access.log? Ignore query strings and paths without an extension.", hourWindow(hour)),
		"Write only the extension without the leading dot.",
		targetExt,
		records,
	), nil
}

func prepareSSHTopFailedUser(activity Activity, seed int64) (Scenario, error) {
	rng := rand.New(rand.NewSource(seed))
	day := scenarioDay(rng)
	hour := rng.Intn(24)
	timeline := newAuthTimeline(day)
	validUsers := authValidUsers()
	shuffledValidUsers := append([]string(nil), validUsers...)
	shuffleStrings(rng, shuffledValidUsers)
	targetUser := shuffledValidUsers[0]
	otherUsers := append([]string(nil), shuffledValidUsers[1:]...)
	otherUsers = append(otherUsers, authInvalidUsers...)
	shuffleStrings(rng, otherUsers)
	counts := []int{42 + rng.Intn(10), 31 + rng.Intn(8), 22 + rng.Intn(6), 15 + rng.Intn(4), 9 + rng.Intn(3)}
	records := make([]authRecord, 0, DefaultScenarioLineCount)

	for i := range counts {
		user := targetUser
		if i > 0 {
			user = otherUsers[i-1]
		}
		event := authFailureBursts(timeline, hour, user, counts[i], rng)
		records = append(records, event.Records...)
	}
	for len(records) < DefaultScenarioLineCount {
		event := randomAuthEvent(timeline, rng)
		if event.AttemptHour == hour && event.FailureCount > 0 {
			continue
		}
		if len(records)+len(event.Records) > DefaultScenarioLineCount {
			records = append(records, randomAuthNoiseRecord(timeline, rng))
			continue
		}
		records = append(records, event.Records...)
	}

	return newAuthScenario(activity, "auth.log",
		fmt.Sprintf("Which valid username had the most failed SSH authentication attempts during the %s hour in auth.log? Only consider these valid usernames: %s. Count failed password and failed publickey lines. Do not count the literal word \"invalid\", and ignore standalone \"Invalid user ...\" lines.", hourWindow(hour), strings.Join(validUsers, ", ")),
		"Write only the username.",
		targetUser,
		records,
	), nil
}

func prepareSSHDistinctFailedIPs(activity Activity, seed int64) (Scenario, error) {
	rng := rand.New(rand.NewSource(seed))
	day := scenarioDay(rng)
	hour := rng.Intn(24)
	timeline := newAuthTimeline(day)
	targetUser := pickOne(rng, authUsers)
	sourceCount := 7 + rng.Intn(18)
	sources := uniqueAuthSourceIPs(rng, sourceCount)
	records := make([]authRecord, 0, DefaultScenarioLineCount)

	event := authDistinctFailedIPsEvent(timeline, hour, targetUser, sources, rng)
	records = append(records, event.Records...)

	for len(records) < DefaultScenarioLineCount {
		event := randomAuthEvent(timeline, rng)
		if event.AttemptHour == hour && event.FailureCount > 0 && event.User == targetUser {
			continue
		}
		if len(records)+len(event.Records) > DefaultScenarioLineCount {
			records = append(records, randomAuthNoiseRecord(timeline, rng))
			continue
		}
		records = append(records, event.Records...)
	}

	return newAuthScenario(activity, "auth.log",
		fmt.Sprintf("How many distinct source IPs caused failed SSH authentication attempts for user %s during the %s hour in auth.log? Count failed password and failed publickey lines only.", targetUser, hourWindow(hour)),
		"Write only the number.",
		strconv.Itoa(sourceCount),
		records,
	), nil
}

func prepareSSHTopSuccessUser(activity Activity, seed int64) (Scenario, error) {
	rng := rand.New(rand.NewSource(seed))
	day := scenarioDay(rng)
	hour := rng.Intn(24)
	timeline := newAuthTimeline(day)
	users := append([]string(nil), authUsers...)
	shuffleStrings(rng, users)
	targetUser := users[0]
	counts := []int{14 + rng.Intn(4), 10 + rng.Intn(3), 7 + rng.Intn(2), 4 + rng.Intn(2)}
	records := make([]authRecord, 0, DefaultScenarioLineCount)

	for i := range counts {
		event := authSuccessSessions(timeline, hour, users[i], counts[i], false, rng)
		records = append(records, event.Records...)
	}
	for len(records) < DefaultScenarioLineCount {
		event := randomAuthEvent(timeline, rng)
		if event.AttemptHour == hour && event.SuccessCount > 0 {
			continue
		}
		if len(records)+len(event.Records) > DefaultScenarioLineCount {
			records = append(records, randomAuthNoiseRecord(timeline, rng))
			continue
		}
		records = append(records, event.Records...)
	}

	return newAuthScenario(activity, "auth.log",
		fmt.Sprintf("Which username has the most successful SSH logins during the %s hour in auth.log? Count only Accepted lines, not PAM or systemd session messages.", hourWindow(hour)),
		"Write only the username.",
		targetUser,
		records,
	), nil
}

func prepareSyslogSudoFailures(activity Activity, seed int64) (Scenario, error) {
	rng := rand.New(rand.NewSource(seed))
	day := scenarioDay(rng)
	hour := rng.Intn(24)
	timeline := newSyslogTimeline(day, rng)
	targetUser := pickOne(rng, syslogUsers)
	failures := 8 + rng.Intn(18)
	records := make([]syslogRecord, 0, DefaultScenarioLineCount)

	for range failures {
		records = append(records, syslogRecord{
			Timestamp: timeline.timestampInHour(hour, rng),
			Host:      scenarioHostname(),
			Program:   "sudo",
			PID:       syslogPID("sudo", rng),
			Message: fmt.Sprintf(
				"pam_unix(sudo:auth): authentication failure; logname=%s uid=1000 euid=0 tty=/dev/pts/%d ruser=%s rhost= user=root",
				targetUser,
				rng.Intn(4),
				targetUser,
			),
		})
	}
	for len(records) < DefaultScenarioLineCount {
		record := randomSyslogRecord(timeline, rng)
		if record.Timestamp.Hour() == hour && record.Program == "sudo" &&
			strings.Contains(strings.ToLower(record.Message), "authentication failure") &&
			strings.Contains(record.Message, "ruser="+targetUser) {
			continue
		}
		records = append(records, record)
	}

	return newSyslogScenario(activity, "syslog.log",
		fmt.Sprintf("How many sudo authentication failures for user %s appear during the %s hour in syslog.log?", targetUser, hourWindow(hour)),
		"Write only the number.",
		strconv.Itoa(failures),
		records,
	), nil
}

func prepareSyslogTopErrorService(activity Activity, seed int64) (Scenario, error) {
	rng := rand.New(rand.NewSource(seed))
	day := scenarioDay(rng)
	hour := rng.Intn(24)
	timeline := newSyslogTimeline(day, rng)
	services := append([]string(nil), syslogServices...)
	shuffleStrings(rng, services)
	targetService := services[0]
	counts := []int{32 + rng.Intn(7), 21 + rng.Intn(5), 12 + rng.Intn(4), 7 + rng.Intn(3)}
	records := make([]syslogRecord, 0, DefaultScenarioLineCount)

	for i := range counts {
		for range counts[i] {
			records = append(records, errorSyslogRecord(timeline, hour, services[i], rng))
		}
	}
	for len(records) < DefaultScenarioLineCount {
		record := randomSyslogRecord(timeline, rng)
		if record.Timestamp.Hour() == hour && strings.Contains(strings.ToLower(record.Message), "error") {
			continue
		}
		records = append(records, record)
	}

	return newSyslogScenario(activity, "syslog.log",
		fmt.Sprintf("Which service or process name produced the most lines containing the word error during the %s hour in syslog.log? Match case-insensitively.", hourWindow(hour)),
		"Write only the service or process name.",
		targetService,
		records,
	), nil
}

func newAccessScenario(activity Activity, logName, question, outputFormat, expectedAnswer string, records []accessRecord) Scenario {
	sort.Slice(records, func(i, j int) bool {
		return records[i].Timestamp.Before(records[j].Timestamp)
	})
	lines := make([]string, len(records))
	for i, record := range records {
		lines[i] = fmt.Sprintf(
			`%s %s - %s [%s] "%s %s %s" %d %d "%s" "%s" "%s" %d "%s" %s %s`,
			defaultString(record.VHost, apacheDefaultVHost(record.Path)),
			record.IP,
			defaultString(record.RemoteUser, "-"),
			record.Timestamp.Format("02/Jan/2006:15:04:05 -0700"),
			record.Method,
			record.Path,
			defaultString(record.Protocol, "HTTP/1.1"),
			record.Status,
			record.Bytes,
			defaultString(record.Referrer, "-"),
			defaultString(record.UserAgent, "-"),
			defaultString(record.ServerHeader, "Apache/2.4.58 (Ubuntu)"),
			record.RequestDurationMicro,
			defaultString(record.KeepAlive, "-"),
			defaultString(record.SSLProtocol, "-"),
			defaultString(record.SSLCipher, "-"),
		)
	}
	return Scenario{
		ActivityID:     activity.ID,
		Title:          activity.Title,
		LogName:        logName,
		Question:       question,
		OutputFormat:   outputFormat,
		SuggestedTools: append([]string(nil), activity.SuggestedTools...),
		ExpectedAnswer: expectedAnswer,
		Lines:          lines,
	}
}

func newNginxScenario(activity Activity, logName, question, outputFormat, expectedAnswer string, records []accessRecord) Scenario {
	sort.Slice(records, func(i, j int) bool {
		return records[i].Timestamp.Before(records[j].Timestamp)
	})
	lines := make([]string, len(records))
	for i, record := range records {
		lines[i] = fmt.Sprintf(
			`%s - - [%s] "%s %s %s" %d %d "%s" "%s" %d %.3f %s "%s" %s %s %s`,
			record.IP,
			record.Timestamp.Format("02/Jan/2006:15:04:05 -0700"),
			record.Method,
			record.Path,
			defaultString(record.Protocol, "HTTP/1.1"),
			record.Status,
			record.Bytes,
			defaultString(record.Referrer, "-"),
			defaultString(record.UserAgent, "-"),
			record.RequestLength,
			record.RequestTime,
			defaultString(record.Scheme, "https"),
			defaultString(record.ForwardedFor, "-"),
			defaultString(record.UpstreamAddr, "-"),
			defaultString(record.UpstreamResponseTime, "-"),
			defaultString(record.UpstreamStatus, "-"),
		)
	}
	return Scenario{
		ActivityID:     activity.ID,
		Title:          activity.Title,
		LogName:        logName,
		Question:       question,
		OutputFormat:   outputFormat,
		SuggestedTools: append([]string(nil), activity.SuggestedTools...),
		ExpectedAnswer: expectedAnswer,
		Lines:          lines,
	}
}

func newAuthScenario(activity Activity, logName, question, outputFormat, expectedAnswer string, records []authRecord) Scenario {
	sort.Slice(records, func(i, j int) bool {
		return records[i].Timestamp.Before(records[j].Timestamp)
	})
	lines := make([]string, len(records))
	for i, record := range records {
		lines[i] = fmt.Sprintf(
			"%s %s %s[%d]: %s",
			record.Timestamp.Format("2006-01-02T15:04:05.000000-07:00"),
			record.Host,
			record.Program,
			record.PID,
			record.Message,
		)
	}
	return Scenario{
		ActivityID:     activity.ID,
		Title:          activity.Title,
		LogName:        logName,
		Question:       question,
		OutputFormat:   outputFormat,
		SuggestedTools: append([]string(nil), activity.SuggestedTools...),
		ExpectedAnswer: expectedAnswer,
		Lines:          lines,
	}
}

func newSyslogScenario(activity Activity, logName, question, outputFormat, expectedAnswer string, records []syslogRecord) Scenario {
	sort.Slice(records, func(i, j int) bool {
		return records[i].Timestamp.Before(records[j].Timestamp)
	})
	lines := make([]string, len(records))
	for i, record := range records {
		lines[i] = fmt.Sprintf(
			"%s %s %s[%d]: %s",
			record.Timestamp.Format("2006-01-02T15:04:05.000000-07:00"),
			record.Host,
			record.Program,
			record.PID,
			record.Message,
		)
	}
	return Scenario{
		ActivityID:     activity.ID,
		Title:          activity.Title,
		LogName:        logName,
		Question:       question,
		OutputFormat:   outputFormat,
		SuggestedTools: append([]string(nil), activity.SuggestedTools...),
		ExpectedAnswer: expectedAnswer,
		Lines:          lines,
	}
}

func randomAccessRecord(day time.Time, rng *rand.Rand) accessRecord {
	path := randomApachePath(rng)
	method := apacheMethodForPath(path, rng)
	return buildApacheRecord(
		randomTimestamp(day, rng),
		randomApacheClientIPForPath(path, rng),
		method,
		path,
		randomApacheStatus(path, method, rng),
		rng,
	)
}

func buildApacheRecord(timestamp time.Time, ip, method, path string, status int, rng *rand.Rand) accessRecord {
	category := apachePathCategory(path)
	vhost := apacheDefaultVHost(path)
	userAgent := apacheUserAgent(category, method, rng)
	referrer := apacheReferrer(category, path, userAgent, rng)
	sslProtocol, sslCipher := apacheSSLDetails(category, path, vhost, userAgent, rng)
	protocol := apacheProtocol(sslProtocol != "-", userAgent, rng)

	return accessRecord{
		Timestamp:            timestamp,
		VHost:                vhost,
		IP:                   ip,
		RemoteUser:           apacheRemoteUser(category, path, method, status, rng),
		Method:               method,
		Path:                 path,
		Status:               status,
		Bytes:                apacheBodyBytes(path, status, method),
		Referrer:             referrer,
		UserAgent:            userAgent,
		ServerHeader:         "Apache/2.4.58 (Ubuntu)",
		RequestDurationMicro: apacheRequestDurationMicros(category, path, status, method, rng),
		KeepAlive:            apacheKeepAlive(method, protocol, rng),
		SSLProtocol:          sslProtocol,
		SSLCipher:            sslCipher,
		Protocol:             protocol,
	}
}

func randomApachePath(rng *rand.Rand) string {
	switch roll := rng.Intn(100); {
	case roll < 14:
		return randomPathUnderPrefix("/assets/", rng)
	case roll < 29:
		return randomPathUnderPrefix("/api/v1/", rng)
	case roll < 38:
		return randomPathUnderPrefix("/app/", rng)
	case roll < 46:
		return randomPathUnderPrefix("/billing/", rng)
	case roll < 54:
		return randomPathUnderPrefix("/admin/", rng)
	case roll < 63:
		return randomPathUnderPrefix("/downloads/", rng)
	case roll < 69:
		return randomPathUnderPrefix("/reports/", rng)
	case roll < 74:
		return "/downloads/"
	case roll < 80:
		return pickOne(rng, apacheCGIPaths)
	case roll < 85:
		return pickOne(rng, apacheScannerPaths)
	default:
		return pickOne(rng, apachePagePaths)
	}
}

func apachePathCategory(path string) string {
	basePath := accessPathWithoutQuery(path)
	switch {
	case basePath == "/downloads/" || basePath == "/reports/" || basePath == "/exports/":
		return "download-index"
	case strings.HasPrefix(basePath, "/downloads/"), strings.HasPrefix(basePath, "/reports/"), strings.HasPrefix(basePath, "/exports/"):
		return "download"
	case strings.HasPrefix(basePath, "/assets/"), strings.HasPrefix(basePath, "/icons/"):
		return "asset"
	case strings.HasPrefix(basePath, "/api/"), basePath == "/server-status":
		return "api"
	case strings.HasPrefix(basePath, "/billing/"):
		return "billing"
	case strings.HasPrefix(basePath, "/admin/"):
		return "admin"
	case strings.HasPrefix(basePath, "/app/"):
		return "app"
	case strings.HasPrefix(basePath, "/cgi-bin/"), strings.HasSuffix(basePath, ".cgi"), strings.HasSuffix(basePath, ".pl"):
		return "cgi"
	case basePath == "/.htaccess", basePath == "/.htpasswd", basePath == "/.htgroups":
		return "scanner"
	default:
		return "page"
	}
}

func apacheDefaultVHost(path string) string {
	basePath := accessPathWithoutQuery(path)
	switch {
	case strings.HasPrefix(basePath, "/api/"), basePath == "/server-status":
		return "api.example.com"
	case strings.HasPrefix(basePath, "/billing/"), strings.HasPrefix(basePath, "/admin/"):
		return "secure.example.com"
	case strings.HasPrefix(basePath, "/downloads/"), basePath == "/downloads/":
		return "downloads.example.com"
	case strings.HasPrefix(basePath, "/reports/"), strings.HasPrefix(basePath, "/exports/"):
		return "portal.example.com"
	case strings.HasPrefix(basePath, "/cgi-bin/"), strings.HasPrefix(basePath, "/manual/"):
		return "legacy.example.com"
	case strings.HasPrefix(basePath, "/app/"):
		return "app.example.com"
	default:
		return "www.example.com"
	}
}

func apacheMethodForPath(path string, rng *rand.Rand) string {
	switch apachePathCategory(path) {
	case "asset":
		return pickWeightedString(rng, weightedStrings{{"GET", 93}, {"HEAD", 6}, {"OPTIONS", 1}})
	case "download":
		return pickWeightedString(rng, weightedStrings{{"GET", 90}, {"HEAD", 7}, {"OPTIONS", 3}})
	case "download-index":
		return pickWeightedString(rng, weightedStrings{{"GET", 87}, {"HEAD", 8}, {"OPTIONS", 5}})
	case "api":
		return pickWeightedString(rng, weightedStrings{{"GET", 48}, {"POST", 33}, {"PUT", 5}, {"PATCH", 2}, {"DELETE", 1}, {"OPTIONS", 7}, {"HEAD", 4}})
	case "billing":
		return pickWeightedString(rng, weightedStrings{{"GET", 57}, {"POST", 24}, {"PUT", 4}, {"PATCH", 2}, {"DELETE", 1}, {"OPTIONS", 7}, {"HEAD", 5}})
	case "admin":
		return pickWeightedString(rng, weightedStrings{{"GET", 60}, {"POST", 18}, {"PATCH", 2}, {"DELETE", 1}, {"OPTIONS", 9}, {"HEAD", 10}})
	case "cgi":
		return pickWeightedString(rng, weightedStrings{{"GET", 80}, {"POST", 10}, {"OPTIONS", 4}, {"HEAD", 6}})
	case "scanner":
		return pickWeightedString(rng, weightedStrings{{"GET", 90}, {"HEAD", 6}, {"OPTIONS", 4}})
	default:
		return pickWeightedString(rng, weightedStrings{{"GET", 77}, {"POST", 11}, {"OPTIONS", 4}, {"HEAD", 8}})
	}
}

func randomApacheStatus(path, method string, rng *rand.Rand) int {
	basePath := accessPathWithoutQuery(path)
	switch apachePathCategory(path) {
	case "asset":
		return pickWeightedInt(rng, weightedInts{{200, 72}, {304, 18}, {404, 6}, {403, 3}, {500, 1}})
	case "download":
		if method == "OPTIONS" {
			return 204
		}
		return pickWeightedInt(rng, weightedInts{{200, 72}, {206, 10}, {304, 8}, {302, 3}, {404, 5}, {403, 2}})
	case "download-index":
		if method == "OPTIONS" {
			return 204
		}
		return pickWeightedInt(rng, weightedInts{{200, 81}, {304, 7}, {403, 8}, {404, 4}})
	case "api":
		if method == "OPTIONS" {
			return pickWeightedInt(rng, weightedInts{{204, 86}, {200, 14}})
		}
		if method == "HEAD" {
			return pickWeightedInt(rng, weightedInts{{200, 74}, {304, 10}, {404, 8}, {429, 5}, {500, 3}})
		}
		if method == "POST" {
			return pickWeightedInt(rng, weightedInts{{200, 58}, {201, 20}, {400, 5}, {401, 3}, {403, 3}, {404, 4}, {409, 2}, {429, 3}, {500, 1}, {503, 1}})
		}
		if method == "PUT" || method == "PATCH" || method == "DELETE" {
			return pickWeightedInt(rng, weightedInts{{200, 54}, {204, 18}, {400, 5}, {401, 3}, {403, 4}, {404, 5}, {409, 2}, {429, 4}, {500, 3}, {503, 2}})
		}
		return pickWeightedInt(rng, weightedInts{{200, 70}, {204, 5}, {304, 6}, {400, 2}, {401, 2}, {403, 2}, {404, 6}, {429, 3}, {500, 2}, {503, 2}})
	case "billing":
		if method == "OPTIONS" {
			return 204
		}
		return pickWeightedInt(rng, weightedInts{{200, 61}, {201, 7}, {302, 10}, {304, 5}, {401, 4}, {403, 4}, {404, 4}, {429, 2}, {500, 2}, {503, 1}})
	case "admin":
		if method == "OPTIONS" {
			return 204
		}
		return pickWeightedInt(rng, weightedInts{{200, 54}, {302, 18}, {401, 8}, {403, 12}, {404, 4}, {429, 2}, {500, 1}, {503, 1}})
	case "cgi":
		if method == "OPTIONS" {
			return 204
		}
		return pickWeightedInt(rng, weightedInts{{200, 69}, {304, 5}, {403, 6}, {404, 8}, {500, 12}})
	case "scanner":
		if basePath == "/.htaccess" || basePath == "/.htpasswd" {
			return pickWeightedInt(rng, weightedInts{{403, 83}, {404, 12}, {400, 5}})
		}
		return pickWeightedInt(rng, weightedInts{{403, 62}, {404, 26}, {400, 8}, {500, 4}})
	default:
		if method == "OPTIONS" {
			return 204
		}
		return pickWeightedInt(rng, weightedInts{{200, 67}, {301, 10}, {302, 8}, {304, 8}, {403, 2}, {404, 3}, {500, 2}})
	}
}

func apacheUserAgent(category, method string, rng *rand.Rand) string {
	switch category {
	case "scanner":
		if rng.Intn(100) < 65 {
			return pickOne(rng, apacheScannerUserAgents)
		}
		return pickOne(rng, apacheLegacyUserAgents)
	case "cgi":
		switch roll := rng.Intn(100); {
		case roll < 42:
			return pickOne(rng, apacheCLIUserAgents)
		case roll < 70:
			return pickOne(rng, apacheLegacyUserAgents)
		case roll < 88:
			return pickOne(rng, apacheBrowserUserAgents)
		default:
			return pickOne(rng, apacheBotUserAgents)
		}
	case "api":
		switch roll := rng.Intn(100); {
		case roll < 28:
			return pickOne(rng, apacheBrowserUserAgents)
		case roll < 54:
			return pickOne(rng, apacheCLIUserAgents)
		case roll < 82:
			return pickOne(rng, apacheLegacyUserAgents)
		default:
			return pickOne(rng, apacheBotUserAgents)
		}
	default:
		switch roll := rng.Intn(100); {
		case roll < 30:
			return pickOne(rng, apacheBrowserUserAgents)
		case roll < 58:
			return pickOne(rng, apacheLegacyUserAgents)
		case roll < 82:
			return pickOne(rng, apacheCLIUserAgents)
		default:
			return pickOne(rng, apacheBotUserAgents)
		}
	}
}

func apacheReferrer(category, path, userAgent string, rng *rand.Rand) string {
	if category == "scanner" || strings.Contains(userAgent, "bot") {
		return "-"
	}
	if strings.HasPrefix(userAgent, "curl/") || strings.HasPrefix(userAgent, "Wget/") || strings.HasPrefix(userAgent, "Go-http-client") || strings.HasPrefix(userAgent, "python-requests") {
		switch roll := rng.Intn(100); {
		case roll < 76:
			return "-"
		case roll < 92:
			return pickOne(rng, apacheInternalReferrers)
		default:
			return pickOne(rng, apacheSuspiciousReferrers)
		}
	}

	switch category {
	case "download", "download-index", "cgi":
		switch roll := rng.Intn(100); {
		case roll < 24:
			return "-"
		case roll < 58:
			return pickOne(rng, apacheInternalReferrers)
		case roll < 78:
			return pickOne(rng, apacheSearchReferrers)
		default:
			return pickOne(rng, apacheSuspiciousReferrers)
		}
	case "admin", "billing", "api", "app":
		switch roll := rng.Intn(100); {
		case roll < 34:
			return "-"
		case roll < 82:
			return pickOne(rng, apacheInternalReferrers)
		case roll < 92:
			return pickOne(rng, apacheSearchReferrers)
		default:
			return pickOne(rng, apacheSuspiciousReferrers)
		}
	default:
		switch roll := rng.Intn(100); {
		case roll < 28:
			return "-"
		case roll < 56:
			return pickOne(rng, apacheSearchReferrers)
		case roll < 78:
			return pickOne(rng, apacheInternalReferrers)
		default:
			return pickOne(rng, apacheSuspiciousReferrers)
		}
	}
}

func apacheRemoteUser(category, path, method string, status int, rng *rand.Rand) string {
	switch category {
	case "admin", "billing":
		if status == 401 || status == 403 {
			return "-"
		}
		if rng.Intn(100) < 78 {
			return pickOne(rng, apacheRemoteUsers)
		}
	case "api":
		if method != "GET" && method != "HEAD" && status != 401 && status != 403 && status < 500 && rng.Intn(100) < 18 {
			return pickOne(rng, []string{"deploy", "backup", "svc-ci"})
		}
	case "app":
		if status/100 == 2 && rng.Intn(100) < 14 {
			return pickOne(rng, apacheRemoteUsers[:6])
		}
	case "download":
		if strings.HasPrefix(accessPathWithoutQuery(path), "/reports/") && status/100 == 2 && rng.Intn(100) < 12 {
			return pickOne(rng, apacheRemoteUsers[:6])
		}
	}
	return "-"
}

func apacheSSLDetails(category, path, vhost, userAgent string, rng *rand.Rand) (string, string) {
	secureChance := 68
	switch category {
	case "admin", "billing":
		secureChance = 98
	case "api":
		secureChance = 92
	case "download", "download-index", "app":
		secureChance = 84
	case "cgi":
		secureChance = 38
	case "scanner":
		secureChance = 28
	}
	if vhost == "downloads.example.com" || vhost == "portal.example.com" {
		secureChance = maxInt(secureChance, 82)
	}
	if strings.Contains(userAgent, "MSIE 8.0") {
		secureChance -= 24
	} else if strings.Contains(userAgent, "Trident/7.0") || strings.Contains(userAgent, "Firefox/102") || strings.Contains(userAgent, "Chrome/49") {
		secureChance -= 8
	}
	if rng.Intn(100) >= clampInt(secureChance, 0, 100) {
		return "-", "-"
	}

	if strings.Contains(userAgent, "MSIE 8.0") || strings.Contains(userAgent, "Trident/7.0") || strings.Contains(userAgent, "Firefox/102") || strings.Contains(userAgent, "Chrome/49") || category == "cgi" {
		if rng.Intn(100) < 72 {
			return "TLSv1.2", pickOne(rng, apacheTLS12Ciphers)
		}
		return "TLSv1.3", pickOne(rng, apacheTLS13Ciphers)
	}
	if rng.Intn(100) < 62 {
		return "TLSv1.3", pickOne(rng, apacheTLS13Ciphers)
	}
	return "TLSv1.2", pickOne(rng, apacheTLS12Ciphers)
}

func apacheProtocol(secure bool, userAgent string, rng *rand.Rand) string {
	if !secure {
		return "HTTP/1.1"
	}
	if strings.Contains(userAgent, "MSIE 8.0") || strings.Contains(userAgent, "Trident/7.0") || strings.Contains(userAgent, "Firefox/102") || strings.Contains(userAgent, "Chrome/49") {
		return "HTTP/1.1"
	}
	if strings.HasPrefix(userAgent, "curl/8.") && rng.Intn(100) < 18 {
		return "HTTP/2.0"
	}
	if strings.HasPrefix(userAgent, "Mozilla/5.0") && rng.Intn(100) < 24 {
		return "HTTP/2.0"
	}
	return "HTTP/1.1"
}

func apacheKeepAlive(method, protocol string, rng *rand.Rand) string {
	if protocol == "HTTP/2.0" {
		return "-"
	}
	if method == "HEAD" || method == "OPTIONS" {
		return "timeout=5, max=100"
	}
	return pickOne(rng, apacheKeepAliveValues)
}

func apacheBodyBytes(path string, status int, method string) int {
	if status == 204 || status == 304 || method == "HEAD" {
		return 0
	}

	basePath := accessPathWithoutQuery(path)
	if status >= 400 {
		switch apachePathCategory(path) {
		case "scanner":
			return stableRangeForString(basePath, 180, 420)
		case "api":
			return stableRangeForString(basePath, 180, 2600)
		case "admin", "billing":
			return stableRangeForString(basePath, 220, 4200)
		case "cgi":
			return stableRangeForString(basePath, 260, 1800)
		default:
			return stableRangeForString(basePath, 160, 2400)
		}
	}

	switch apachePathCategory(path) {
	case "asset":
		return stableRangeForString(basePath, 800, 2_000_000)
	case "download":
		return stableRangeForString(basePath, 50_000, 25_000_000)
	case "download-index":
		return stableRangeForString(basePath, 12_000, 90_000)
	case "api":
		if method == "OPTIONS" {
			return 0
		}
		return stableRangeForString(basePath, 180, 65_000)
	case "billing":
		return stableRangeForString(basePath, 700, 70_000)
	case "admin", "app", "page":
		return stableRangeForString(basePath, 4_000, 120_000)
	case "cgi":
		return stableRangeForString(basePath, 1_200, 9_000)
	default:
		return stableRangeForString(basePath, 512, 25_000)
	}
}

func apacheRequestDurationMicros(category, path string, status int, method string, rng *rand.Rand) int {
	minUs, maxUs := 2_000, 180_000
	switch category {
	case "asset":
		minUs, maxUs = 1_000, 120_000
	case "download":
		minUs, maxUs = 35_000, 3_200_000
	case "download-index":
		minUs, maxUs = 12_000, 280_000
	case "api":
		minUs, maxUs = 2_000, 450_000
	case "billing", "admin":
		minUs, maxUs = 6_000, 620_000
	case "cgi":
		minUs, maxUs = 8_000, 740_000
	case "scanner":
		minUs, maxUs = 500, 48_000
	case "app", "page":
		minUs, maxUs = 4_000, 220_000
	}
	if status >= 500 {
		maxUs += 350_000
	}
	if method == "HEAD" || method == "OPTIONS" {
		maxUs = maxInt(minUs+1, maxUs/2)
	}
	base := stableRangeForString(method+":"+accessPathWithoutQuery(path), minUs, maxUs)
	jitter := maxInt((maxUs-minUs)/10, 1)
	return clampInt(base+(rng.Intn(jitter)-rng.Intn(jitter)), minUs, maxUs)
}

func randomApacheClientIPForPath(path string, rng *rand.Rand) string {
	switch apachePathCategory(path) {
	case "admin", "billing", "api":
		if rng.Intn(100) < 18 {
			return randomPrivateIP(rng)
		}
	case "scanner", "cgi":
		if rng.Intn(100) < 76 {
			return randomPublicIP(rng)
		}
		return randomApacheIPv6(rng)
	}
	return randomApacheClientIP(rng)
}

func randomApacheClientIP(rng *rand.Rand) string {
	switch roll := rng.Intn(100); {
	case roll < 67:
		return randomPublicIP(rng)
	case roll < 82:
		return randomPrivateIP(rng)
	default:
		return randomApacheIPv6(rng)
	}
}

func randomApacheIPv6(rng *rand.Rand) string {
	return fmt.Sprintf(
		"2001:db8:%x:%x::%x",
		0x10+rng.Intn(0xef),
		1+rng.Intn(0xfff),
		1+rng.Intn(0xffff),
	)
}

func randomNginxRecord(day time.Time, rng *rand.Rand) accessRecord {
	timeline := newNginxTimeline(day)
	path := randomNginxPath(rng)
	method := nginxMethodForPath(path, rng)
	status := randomNginxStatus(path, method, rng)
	return buildNginxRecord(
		timeline.randomTimestamp(rng),
		randomNginxClientIPForPath(path, rng),
		method,
		path,
		status,
		rng.Intn(4) == 0,
		rng,
	)
}

func newNginxTimeline(day time.Time) nginxTimeline {
	return nginxTimeline{day: day}
}

func (t nginxTimeline) randomTimestamp(rng *rand.Rand) time.Time {
	return t.withinHour(weightedNginxHour(rng), rng)
}

func (t nginxTimeline) withinHour(hour int, rng *rand.Rand) time.Time {
	minuteAnchors := []int{0, 1, 2, 14, 15, 16, 29, 30, 31, 44, 45, 46, 58}
	secondAnchors := []int{0, 0, 1, 1, 2, 3, 15, 16, 30, 30, 31, 45, 45, 46, 58}
	minute := clampInt(pickOne(rng, minuteAnchors)+(rng.Intn(3)-1), 0, 59)
	second := clampInt(pickOne(rng, secondAnchors)+(rng.Intn(2)), 0, 59)
	return t.day.Add(
		time.Duration(hour)*time.Hour +
			time.Duration(minute)*time.Minute +
			time.Duration(second)*time.Second,
	)
}

func weightedNginxHour(rng *rand.Rand) int {
	switch roll := rng.Intn(100); {
	case roll < 5:
		return rng.Intn(6)
	case roll < 15:
		return 6 + rng.Intn(2)
	case roll < 65:
		return 8 + rng.Intn(10)
	case roll < 90:
		return 18 + rng.Intn(4)
	default:
		return 22 + rng.Intn(2)
	}
}

func buildNginxRecord(timestamp time.Time, ip, method, path string, status int, proxied bool, rng *rand.Rand) accessRecord {
	category := nginxPathCategory(path)
	userAgent := nginxUserAgent(category, method, rng)
	referer := nginxReferrer(category, path, userAgent, rng)
	scheme := nginxScheme(category, userAgent, rng)
	protocol := nginxProtocol(userAgent, scheme, rng)
	bodyBytes := nginxBodyBytes(path, status, method)
	requestLength := nginxRequestLength(path, method)
	upstreamAddr, upstreamStatus := nginxUpstream(path, status)
	requestTime, upstreamResponseTime := nginxTimings(category, status, bodyBytes, upstreamAddr != "-", rng)
	forwardedFor := "-"
	if proxied {
		forwardedFor = fmt.Sprintf("%s, %s", pickOne(rng, nginxLoadBalancerIPs), ip)
	}
	return accessRecord{
		Timestamp:            timestamp,
		IP:                   ip,
		Method:               method,
		Path:                 path,
		Status:               status,
		Bytes:                bodyBytes,
		Referrer:             referer,
		UserAgent:            userAgent,
		Protocol:             protocol,
		RequestLength:        requestLength,
		RequestTime:          requestTime,
		Scheme:               scheme,
		ForwardedFor:         forwardedFor,
		UpstreamAddr:         upstreamAddr,
		UpstreamResponseTime: upstreamResponseTime,
		UpstreamStatus:       upstreamStatus,
	}
}

func randomNginxPath(rng *rand.Rand) string {
	switch roll := rng.Intn(100); {
	case roll < 18:
		return randomNginxPathUnderPrefix("/assets/", rng)
	case roll < 38:
		return randomNginxPathUnderPrefix("/api/v1/", rng)
	case roll < 48:
		return randomNginxPathUnderPrefix("/api/v2/", rng)
	case roll < 60:
		return randomNginxPathUnderPrefix("/app/", rng)
	case roll < 74:
		return randomNginxPathUnderPrefix("/products/", rng)
	case roll < 85:
		return randomNginxPathUnderPrefix("/billing/", rng)
	case roll < 91:
		return pickOne(rng, nginxDownloadPaths)
	case roll < 94:
		return "/healthz"
	case roll < 95:
		return pickOne(rng, nginxAdminPaths)
	case roll < 97:
		return pickOne(rng, nginxScannerPaths)
	default:
		return pickOne(rng, nginxLandingPaths)
	}
}

func randomNginxPathUnderPrefix(prefix string, rng *rand.Rand) string {
	switch prefix {
	case "/assets/":
		return pickOne(rng, nginxAssetPaths)
	case "/api/v1/":
		return maybePaginateAPIPath(pickOne(rng, nginxAPIV1Paths), rng)
	case "/api/v2/":
		return maybePaginateAPIPath(pickOne(rng, nginxAPIV2Paths), rng)
	case "/billing/":
		return maybePaginateListPath(pickOne(rng, nginxBillingPaths), rng)
	case "/app/":
		return maybePaginateListPath(pickOne(rng, nginxAppPaths), rng)
	case "/products/":
		return maybeProductPath(rng)
	case "/reports/", "/downloads/", "/exports/":
		candidates := make([]string, 0, len(nginxDownloadPaths))
		for _, path := range nginxDownloadPaths {
			if strings.HasPrefix(path, prefix) {
				candidates = append(candidates, path)
			}
		}
		if len(candidates) > 0 {
			return pickOne(rng, candidates)
		}
		return pickOne(rng, nginxDownloadPaths)
	default:
		return randomPathUnderPrefix(prefix, rng)
	}
}

func maybePaginateAPIPath(path string, rng *rand.Rand) string {
	if strings.HasSuffix(path, "/health") || strings.HasSuffix(path, "/sessions") {
		return path
	}
	if rng.Intn(100) < 55 {
		return fmt.Sprintf("%s?page=%d&limit=%d", path, 1+rng.Intn(5), pickOne(rng, []int{20, 50, 100}))
	}
	return path
}

func maybePaginateListPath(path string, rng *rand.Rand) string {
	if rng.Intn(100) < 35 {
		return fmt.Sprintf("%s?page=%d&limit=%d", path, 1+rng.Intn(4), pickOne(rng, []int{20, 50}))
	}
	return path
}

func maybeProductPath(rng *rand.Rand) string {
	path := pickOne(rng, nginxProductPaths)
	switch {
	case strings.Contains(path, "/search"):
		return fmt.Sprintf("%s?q=%s&page=%d&limit=20", path, pickOne(rng, []string{"logs", "monitoring", "security", "billing"}), 1+rng.Intn(4))
	case strings.Contains(path, "/category/"):
		if rng.Intn(100) < 60 {
			return fmt.Sprintf("%s?page=%d&limit=20", path, 1+rng.Intn(5))
		}
	}
	return path
}

func nginxPathCategory(path string) string {
	basePath := accessPathWithoutQuery(path)
	switch {
	case strings.HasPrefix(basePath, "/assets/"):
		return "asset"
	case strings.HasPrefix(basePath, "/api/"):
		return "api"
	case strings.HasPrefix(basePath, "/billing/"):
		return "billing"
	case strings.HasPrefix(basePath, "/app/"):
		return "app"
	case strings.HasPrefix(basePath, "/products/"):
		return "product"
	case strings.HasPrefix(basePath, "/downloads/"), strings.HasPrefix(basePath, "/reports/"), strings.HasPrefix(basePath, "/exports/"):
		return "download"
	case strings.HasPrefix(basePath, "/admin/"):
		return "admin"
	case basePath == "/healthz" || strings.HasSuffix(basePath, "/health"):
		return "health"
	case strings.HasPrefix(basePath, "/wp-admin"), strings.HasPrefix(basePath, "/phpmyadmin"), strings.HasPrefix(basePath, "/cgi-bin/"), basePath == "/.env", strings.HasPrefix(basePath, "/boaform/"):
		return "scanner"
	default:
		return "page"
	}
}

func nginxMethodForPath(path string, rng *rand.Rand) string {
	switch nginxPathCategory(path) {
	case "asset":
		return pickWeightedString(rng, weightedStrings{{"GET", 99}, {"HEAD", 1}})
	case "download":
		return pickWeightedString(rng, weightedStrings{{"GET", 99}, {"HEAD", 1}})
	case "api":
		return pickWeightedString(rng, weightedStrings{{"GET", 49}, {"POST", 41}, {"PUT", 6}, {"PATCH", 2}, {"DELETE", 1}, {"OPTIONS", 1}})
	case "billing":
		return pickWeightedString(rng, weightedStrings{{"GET", 46}, {"POST", 42}, {"PUT", 6}, {"PATCH", 3}, {"DELETE", 2}, {"OPTIONS", 1}})
	case "admin":
		return pickWeightedString(rng, weightedStrings{{"GET", 54}, {"POST", 38}, {"PATCH", 5}, {"DELETE", 2}, {"OPTIONS", 1}})
	case "health":
		return pickWeightedString(rng, weightedStrings{{"GET", 99}, {"HEAD", 1}})
	case "scanner":
		return pickWeightedString(rng, weightedStrings{{"GET", 98}, {"HEAD", 1}, {"OPTIONS", 1}})
	default:
		return pickWeightedString(rng, weightedStrings{{"GET", 71}, {"POST", 26}, {"OPTIONS", 1}, {"HEAD", 2}})
	}
}

func randomNginxStatus(path, method string, rng *rand.Rand) int {
	switch nginxPathCategory(path) {
	case "asset":
		return pickWeightedInt(rng, weightedInts{{200, 76}, {304, 20}, {404, 3}, {403, 1}})
	case "download":
		return pickWeightedInt(rng, weightedInts{{200, 76}, {206, 6}, {302, 11}, {304, 4}, {404, 2}, {403, 1}})
	case "api":
		switch method {
		case "POST":
			return pickWeightedInt(rng, weightedInts{{200, 61}, {201, 18}, {400, 4}, {401, 3}, {403, 3}, {404, 4}, {409, 2}, {429, 2}, {500, 1}, {502, 1}, {503, 1}})
		case "DELETE", "PATCH":
			return pickWeightedInt(rng, weightedInts{{200, 57}, {204, 21}, {400, 4}, {401, 3}, {403, 3}, {404, 4}, {409, 2}, {429, 2}, {500, 2}, {502, 1}, {503, 1}})
		default:
			return pickWeightedInt(rng, weightedInts{{200, 70}, {304, 6}, {400, 3}, {401, 2}, {403, 2}, {404, 4}, {429, 2}, {500, 1}, {502, 1}, {503, 1}, {204, 8}})
		}
	case "billing":
		return pickWeightedInt(rng, weightedInts{{200, 66}, {201, 10}, {302, 10}, {304, 4}, {400, 2}, {401, 2}, {403, 2}, {404, 2}, {429, 1}, {500, 1}})
	case "admin":
		return pickWeightedInt(rng, weightedInts{{200, 52}, {302, 24}, {401, 7}, {403, 12}, {404, 3}, {500, 1}, {503, 1}})
	case "scanner":
		return pickWeightedInt(rng, weightedInts{{404, 67}, {403, 18}, {400, 5}, {301, 8}, {200, 1}, {500, 1}})
	case "health":
		return pickWeightedInt(rng, weightedInts{{200, 98}, {502, 1}, {503, 1}})
	default:
		return pickWeightedInt(rng, weightedInts{{200, 67}, {301, 9}, {302, 10}, {304, 9}, {400, 1}, {403, 1}, {404, 2}, {500, 1}})
	}
}

func nginxSuccessStatus(path, method string, rng *rand.Rand) int {
	switch nginxPathCategory(path) {
	case "download":
		return pickWeightedInt(rng, weightedInts{{200, 88}, {206, 12}})
	case "api":
		switch method {
		case "POST":
			return pickWeightedInt(rng, weightedInts{{200, 55}, {201, 45}})
		case "DELETE", "PATCH":
			return pickWeightedInt(rng, weightedInts{{200, 48}, {204, 52}})
		default:
			return pickWeightedInt(rng, weightedInts{{200, 94}, {204, 6}})
		}
	default:
		return pickWeightedInt(rng, weightedInts{{200, 92}, {201, 4}, {204, 4}})
	}
}

func nginxNonSuccessStatus(path, method string, rng *rand.Rand) int {
	switch nginxPathCategory(path) {
	case "asset":
		return pickWeightedInt(rng, weightedInts{{304, 52}, {404, 30}, {403, 12}, {500, 6}})
	case "api", "billing":
		if method == "POST" {
			return pickWeightedInt(rng, weightedInts{{400, 20}, {401, 16}, {403, 16}, {404, 18}, {409, 8}, {429, 12}, {500, 5}, {502, 3}, {503, 2}})
		}
		return pickWeightedInt(rng, weightedInts{{302, 8}, {304, 8}, {400, 12}, {401, 12}, {403, 16}, {404, 20}, {429, 12}, {500, 6}, {502, 4}, {503, 2}})
	case "admin":
		return pickWeightedInt(rng, weightedInts{{302, 24}, {401, 18}, {403, 40}, {404, 10}, {500, 5}, {503, 3}})
	default:
		return pickWeightedInt(rng, weightedInts{{301, 28}, {302, 28}, {304, 16}, {403, 8}, {404, 14}, {500, 4}, {503, 2}})
	}
}

func nginxTopBytesStatus(path, method string, rng *rand.Rand) int {
	switch nginxPathCategory(path) {
	case "download":
		return pickWeightedInt(rng, weightedInts{{200, 80}, {206, 20}})
	default:
		return nginxSuccessStatus(path, method, rng)
	}
}

func nginxHighBytesPath(method string, preferLarge bool, rng *rand.Rand) string {
	if method == "POST" {
		if preferLarge {
			return pickOne(rng, []string{
				"/api/v1/invoices?page=1&limit=100",
				"/api/v1/orders?page=1&limit=100",
				"/billing/invoices?page=1&limit=100",
				"/api/v2/products?page=1&limit=100",
			})
		}
		return pickOne(rng, []string{
			"/api/v1/orders?page=2&limit=20",
			"/billing/payments",
			"/api/v2/invoices?page=2&limit=20",
			"/api/v1/users?page=3&limit=20",
		})
	}
	if preferLarge {
		return pickOne(rng, []string{
			"/downloads/toolkit.tar.gz",
			"/exports/billing-archive.parquet",
			"/reports/compliance-2026-03.pdf",
			"/assets/vendor.js",
		})
	}
	return pickOne(rng, []string{
		"/assets/app.js",
		"/assets/app.css",
		"/downloads/dataset.json",
		"/reports/latency-weekly.csv",
	})
}

func nginxPathForExtension(ext string, rng *rand.Rand) string {
	switch ext {
	case "js":
		return pickOne(rng, []string{"/assets/app.js", "/assets/runtime.js", "/assets/vendor.js"})
	case "css":
		return "/assets/app.css"
	case "pdf":
		return pickOne(rng, []string{"/downloads/guide.pdf", "/reports/compliance-2026-03.pdf"})
	case "png":
		return "/assets/team-photo.png"
	case "json":
		return "/downloads/dataset.json"
	default:
		return pickOne(rng, nginxDownloadPaths)
	}
}

func nginxPathExtension(path string) string {
	basePath := accessPathWithoutQuery(path)
	slash := strings.LastIndex(basePath, "/")
	dot := strings.LastIndex(basePath, ".")
	if dot == -1 || dot < slash {
		return ""
	}
	return basePath[dot+1:]
}

func accessPathWithoutQuery(path string) string {
	if idx := strings.Index(path, "?"); idx >= 0 {
		return path[:idx]
	}
	return path
}

func nginxUserAgent(category, method string, rng *rand.Rand) string {
	switch category {
	case "scanner":
		return pickOne(rng, nginxScannerUserAgents)
	case "api":
		switch roll := rng.Intn(100); {
		case roll < 34:
			return pickOne(rng, nginxBrowserUserAgents)
		case roll < 50:
			return pickOne(rng, nginxMobileUserAgents)
		case roll < 74:
			return pickOne(rng, nginxAPIClientUserAgents)
		case roll < 88:
			return pickOne(rng, nginxLegacyUserAgents)
		default:
			return pickOne(rng, nginxBotUserAgents)
		}
	case "health":
		return pickOne(rng, nginxAPIClientUserAgents)
	default:
		switch roll := rng.Intn(100); {
		case roll < 46:
			return pickOne(rng, nginxBrowserUserAgents)
		case roll < 63:
			return pickOne(rng, nginxLegacyUserAgents)
		case roll < 83:
			return pickOne(rng, nginxMobileUserAgents)
		case roll < 95:
			return pickOne(rng, nginxAPIClientUserAgents)
		default:
			return pickOne(rng, nginxBotUserAgents)
		}
	}
}

func nginxReferrer(category, path, userAgent string, rng *rand.Rand) string {
	if category == "scanner" || strings.Contains(userAgent, "bot") {
		return "-"
	}
	switch category {
	case "asset", "app", "billing", "admin", "api":
		switch roll := rng.Intn(100); {
		case roll < 38:
			return "-"
		case roll < 82:
			return pickOne(rng, nginxInternalReferrers)
		default:
			return pickOne(rng, nginxExternalReferrers)
		}
	case "download":
		switch roll := rng.Intn(100); {
		case roll < 35:
			return "-"
		case roll < 70:
			return pickOne(rng, nginxInternalReferrers)
		case roll < 90:
			return pickOne(rng, nginxExternalReferrers)
		default:
			return pickOne(rng, nginxSocialReferrers)
		}
	default:
		switch roll := rng.Intn(100); {
		case roll < 36:
			return "-"
		case roll < 66:
			return pickOne(rng, nginxExternalReferrers)
		case roll < 88:
			return pickOne(rng, nginxInternalReferrers)
		default:
			return pickOne(rng, nginxSocialReferrers)
		}
	}
}

func nginxScheme(category, userAgent string, rng *rand.Rand) string {
	if category == "scanner" && rng.Intn(100) < 35 {
		return "http"
	}
	if strings.Contains(userAgent, "Nmap") || strings.Contains(userAgent, "Nikto") {
		if rng.Intn(100) < 45 {
			return "http"
		}
	}
	if rng.Intn(100) < 92 {
		return "https"
	}
	return "http"
}

func nginxProtocol(userAgent, scheme string, rng *rand.Rand) string {
	if scheme == "https" {
		switch roll := rng.Intn(100); {
		case roll < 3 && (strings.Contains(userAgent, "Chrome/12") || strings.Contains(userAgent, "Safari/604") || strings.Contains(userAgent, "Chrome/123") || strings.Contains(userAgent, "Chrome/124")):
			return "HTTP/3"
		case roll < 28:
			return "HTTP/2.0"
		default:
			return "HTTP/1.1"
		}
	}
	if rng.Intn(100) < 6 {
		return "HTTP/2.0"
	}
	return "HTTP/1.1"
}

func nginxBodyBytes(path string, status int, method string) int {
	if status == 204 || status == 304 || method == "HEAD" {
		return 0
	}
	if status >= 400 {
		return stableRangeForString(fmt.Sprintf("%d:%s", status, accessPathWithoutQuery(path)), 156, 2400)
	}

	basePath := accessPathWithoutQuery(path)
	switch nginxPathCategory(path) {
	case "asset":
		return stableRangeForString(basePath, 1024, 5_000_000)
	case "download":
		return stableRangeForString(basePath, 100_000, 50_000_000)
	case "api":
		return stableRangeForString(basePath, 200, 50_000)
	case "billing":
		return stableRangeForString(basePath, 600, 40_000)
	case "product", "app", "page", "admin":
		return stableRangeForString(basePath, 8_000, 120_000)
	case "health":
		return stableRangeForString(basePath, 120, 512)
	default:
		return stableRangeForString(basePath, 512, 20_000)
	}
}

func nginxRequestLength(path, method string) int {
	basePath := accessPathWithoutQuery(path)
	switch method {
	case "POST":
		return stableRangeForString(method+":"+basePath, 700, 3_800)
	case "PUT", "PATCH":
		return stableRangeForString(method+":"+basePath, 900, 4_600)
	case "DELETE":
		return stableRangeForString(method+":"+basePath, 260, 900)
	default:
		return stableRangeForString(method+":"+basePath, 180, 1_400)
	}
}

func nginxUpstream(path string, status int) (string, string) {
	if status/100 == 3 || nginxPathCategory(path) == "asset" || nginxPathCategory(path) == "download" || nginxPathCategory(path) == "health" || nginxPathCategory(path) == "scanner" {
		return "-", "-"
	}
	switch nginxPathCategory(path) {
	case "api":
		return fmt.Sprintf("10.2.1.%d:8080", 10+stableRangeForString(accessPathWithoutQuery(path), 1, 9)), strconv.Itoa(status)
	case "billing":
		return fmt.Sprintf("10.2.2.%d:9000", 20+stableRangeForString(accessPathWithoutQuery(path), 1, 6)), strconv.Itoa(status)
	case "admin":
		return fmt.Sprintf("10.2.3.%d:8443", 30+stableRangeForString(accessPathWithoutQuery(path), 1, 4)), strconv.Itoa(status)
	default:
		return fmt.Sprintf("10.2.4.%d:3000", 40+stableRangeForString(accessPathWithoutQuery(path), 1, 6)), strconv.Itoa(status)
	}
}

func nginxTimings(category string, status, bytes int, hasUpstream bool, rng *rand.Rand) (float64, string) {
	var minMs, maxMs int
	switch category {
	case "asset":
		minMs, maxMs = 3, 240
	case "download":
		minMs, maxMs = 140, 4600
	case "api":
		minMs, maxMs = 12, 620
	case "billing", "admin":
		minMs, maxMs = 18, 760
	case "health":
		minMs, maxMs = 2, 45
	case "scanner":
		minMs, maxMs = 4, 120
	default:
		minMs, maxMs = 8, 420
	}
	if status >= 500 {
		maxMs += 500
	}
	if bytes > 5_000_000 {
		maxMs += 2200
	}
	requestMs := minMs + rng.Intn(maxInt(maxMs-minMs+1, 1))
	requestTime := float64(requestMs) / 1000
	if !hasUpstream {
		return requestTime, "-"
	}
	upstreamMs := maxInt(requestMs-(4+rng.Intn(30)), 1)
	return requestTime, fmt.Sprintf("%.3f", float64(upstreamMs)/1000)
}

func randomNginxClientIPForPath(path string, rng *rand.Rand) string {
	switch nginxPathCategory(path) {
	case "admin":
		return randomNginxPrivateIP(rng)
	case "scanner":
		if rng.Intn(100) < 80 {
			return randomNginxPublicIPv4(rng)
		}
		return randomNginxIPv6(rng)
	default:
		return randomNginxClientIP(rng)
	}
}

func randomNginxClientIP(rng *rand.Rand) string {
	switch roll := rng.Intn(100); {
	case roll < 35:
		return randomNginxPrivateIP(rng)
	case roll < 88:
		return randomNginxPublicIPv4(rng)
	default:
		return randomNginxIPv6(rng)
	}
}

func uniqueNginxClientIPs(rng *rand.Rand, count int) []string {
	out := make([]string, 0, count)
	seen := make(map[string]struct{}, count)
	for len(out) < count {
		ip := randomNginxClientIP(rng)
		if _, ok := seen[ip]; ok {
			continue
		}
		seen[ip] = struct{}{}
		out = append(out, ip)
	}
	return out
}

func randomNginxPrivateIP(rng *rand.Rand) string {
	switch rng.Intn(3) {
	case 0:
		return fmt.Sprintf("10.%d.%d.%d", 20+rng.Intn(8), rng.Intn(255), 1+rng.Intn(253))
	case 1:
		return fmt.Sprintf("172.%d.%d.%d", 16+rng.Intn(12), rng.Intn(255), 1+rng.Intn(253))
	default:
		return fmt.Sprintf("192.168.%d.%d", rng.Intn(255), 1+rng.Intn(253))
	}
}

func randomNginxPublicIPv4(rng *rand.Rand) string {
	blocks := [][3]int{
		{198, 51, 100},
		{198, 51, 100},
		{203, 0, 113},
		{203, 0, 113},
		{192, 0, 2},
	}
	block := pickOne(rng, blocks)
	return fmt.Sprintf("%d.%d.%d.%d", block[0], block[1], block[2], 1+rng.Intn(253))
}

func randomNginxIPv6(rng *rand.Rand) string {
	prefixes := [][2]uint16{
		{0x2001, 0xdb8},
		{0x2001, 0xdb8},
		{0x2001, 0xdb9},
	}
	prefix := pickOne(rng, prefixes)
	return fmt.Sprintf("%x:%x:%x:%x::%x", prefix[0], prefix[1], 0x10+rng.Intn(0xef), 1+rng.Intn(0xfff), 1+rng.Intn(0xfff))
}

func randomAuthEvent(timeline authTimeline, rng *rand.Rand) authEvent {
	if rng.Intn(100) < 65 {
		hour := randomAuthFailureHour(rng)
		switch roll := rng.Intn(100); {
		case roll < 35:
			return authFailureBursts(timeline, hour, pickOne(rng, authInvalidUsers), randomAuthFailureBurstSize(rng), rng)
		case roll < 68:
			return authFailureBursts(timeline, hour, pickOne(rng, authUsers), randomAuthFailureBurstSize(rng), rng)
		case roll < 88:
			return authFailureBursts(timeline, hour, pickOne(rng, authServiceUsers), 2+rng.Intn(5), rng)
		default:
			return authFailureBursts(timeline, hour, "root", 2+rng.Intn(6), rng)
		}
	}

	user := pickOne(rng, authUsers)
	if rng.Intn(5) == 0 {
		user = pickOne(rng, authServiceUsers)
	}
	return authSuccessSessions(timeline, randomAuthSuccessHour(rng), user, 1, true, rng)
}

func randomSyslogRecord(timeline syslogTimeline, rng *rand.Rand) syslogRecord {
	if rng.Intn(45) == 0 {
		return errorSyslogRecord(timeline, -1, pickOne(rng, syslogServices), rng)
	}
	program := pickOne(rng, syslogPrograms)
	return syslogRecord{
		Timestamp: timeline.randomTimestamp(rng),
		Host:      scenarioHostname(),
		Program:   program,
		PID:       syslogPID(program, rng),
		Message:   nonErrorSyslogMessage(program, rng),
	}
}

func newAuthTimeline(day time.Time) authTimeline {
	return authTimeline{day: day}
}

func authFailureBursts(timeline authTimeline, hour int, user string, attempts int, rng *rand.Rand) authEvent {
	invalid := containsString(authInvalidUsers, user)
	method := authFailureMethod(user)
	sourceIPs := authBurstIPs(rng, attempts)
	event := authEvent{
		AttemptHour:  hour,
		User:         user,
		FailureCount: attempts,
		SourceIPs:    append([]string(nil), sourceIPs...),
	}

	burstCount := 1
	if attempts >= 18 {
		burstCount = 2
	}
	if attempts >= 36 {
		burstCount = 3
	}

	for _, burstAttempts := range splitTotal(attempts, burstCount, 1, rng) {
		event.Records = append(event.Records, failedAuthBurstRecords(timeline, hour, user, sourceIPs, burstAttempts, invalid, method, rng)...)
	}

	return event
}

func authDistinctFailedIPsEvent(timeline authTimeline, hour int, user string, sources []string, rng *rand.Rand) authEvent {
	event := authEvent{
		AttemptHour: hour,
		User:        user,
		SourceIPs:   append([]string(nil), sources...),
	}

	for _, ip := range sources {
		attempts := 1 + rng.Intn(4)
		event.FailureCount += attempts
		event.Records = append(event.Records, failedAuthBurstRecords(timeline, hour, user, []string{ip}, attempts, false, authFailureMethod(user), rng)...)
	}

	return event
}

func authSuccessSessions(timeline authTimeline, hour int, user string, count int, allowRecovered bool, rng *rand.Rand) authEvent {
	event := authEvent{
		AttemptHour:  hour,
		User:         user,
		SuccessCount: count,
	}

	successIPs := authBurstIPs(rng, maxInt(1, minInt(count, 3)))
	event.SourceIPs = append(event.SourceIPs, successIPs...)

	for i := 0; i < count; i++ {
		recovered := allowRecovered && !containsString(authServiceUsers, user) && rng.Intn(3) == 0
		session := acceptedSSHEvent(timeline, hour, user, successIPs[i%len(successIPs)], recovered, rng)
		event.FailureCount += session.FailureCount
		event.Records = append(event.Records, session.Records...)
	}

	return event
}

func failedAuthBurstRecords(timeline authTimeline, hour int, user string, sourceIPs []string, attempts int, invalid bool, method string, rng *rand.Rand) []authRecord {
	records := make([]authRecord, 0, attempts*4)
	attemptTime := timeline.burstStart(hour, attempts, rng)

	for i := 0; i < attempts; i++ {
		if i > 0 {
			attemptTime = attemptTime.Add(time.Duration(1+rng.Intn(3)) * time.Second)
		}
		ip := sourceIPs[minInt(len(sourceIPs)-1, (i/5)%len(sourceIPs))]
		port := randomClientPort(rng)
		pid := authPID("sshd", rng)
		records = append(records, failedAuthAttemptRecords(attemptTime, user, ip, port, invalid, method, pid, rng)...)
		if rng.Intn(7) == 0 {
			records = append(records, authPreauthNoiseRecord(attemptTime.Add(250*time.Millisecond), user, ip, port, invalid, pid, rng))
		}
	}

	return records
}

func failedAuthAttemptRecords(attemptTime time.Time, user, ip string, port int, invalid bool, method string, pid int, rng *rand.Rand) []authRecord {
	connectionTime := attemptTime.Add(-time.Second - time.Duration(100+rng.Intn(350))*time.Millisecond)
	bannerTime := connectionTime.Add(time.Duration(120+rng.Intn(220)) * time.Millisecond)
	records := []authRecord{
		newAuthRecord(connectionTime, "sshd", pid, fmt.Sprintf("Connection from %s port %d on %s port 22", ip, port, authHostIP)),
		newAuthRecord(bannerTime, "sshd", pid, fmt.Sprintf("Client protocol version 2.0; client software version %s", pickOne(rng, authSSHClients))),
	}

	switch {
	case invalid:
		invalidTime := attemptTime.Add(-time.Duration(50+rng.Intn(150)) * time.Millisecond)
		records = append(records,
			newAuthRecord(invalidTime, "sshd", pid, fmt.Sprintf("Invalid user %s from %s port %d", user, ip, port)),
			newAuthRecord(attemptTime, "sshd", pid, fmt.Sprintf("Failed password for invalid user %s from %s port %d ssh2", user, ip, port)),
		)
	case method == "publickey":
		records = append(records, newAuthRecord(attemptTime, "sshd", pid, fmt.Sprintf("Failed publickey for %s from %s port %d ssh2", user, ip, port)))
	default:
		records = append(records, newAuthRecord(attemptTime, "sshd", pid, fmt.Sprintf("Failed password for %s from %s port %d ssh2", user, ip, port)))
	}

	return records
}

func acceptedSSHEvent(timeline authTimeline, hour int, user, ip string, recovered bool, rng *rand.Rand) authEvent {
	acceptedTime := timeline.isolatedStart(hour, rng)
	records := make([]authRecord, 0, 8)
	failureCount := 0

	if recovered {
		failCount := 1 + rng.Intn(2)
		failureCount = failCount
		failTime := acceptedTime.Add(-time.Duration(2*failCount+1+rng.Intn(2)) * time.Second)
		for i := 0; i < failCount; i++ {
			pid := authPID("sshd", rng)
			port := randomClientPort(rng)
			records = append(records, failedAuthAttemptRecords(failTime, user, ip, port, false, "password", pid, rng)...)
			failTime = failTime.Add(time.Duration(1+rng.Intn(2)) * time.Second)
		}
	}

	pid := authPID("sshd", rng)
	logindPID := authPID("systemd-logind", rng)
	port := randomClientPort(rng)
	method := authSuccessMethod(user, rng)
	connectionTime := acceptedTime.Add(-time.Second - time.Duration(100+rng.Intn(300))*time.Millisecond)
	bannerTime := connectionTime.Add(time.Duration(120+rng.Intn(220)) * time.Millisecond)
	openTime := acceptedTime.Add(time.Duration(60+rng.Intn(220)) * time.Millisecond)
	sessionTime := openTime.Add(time.Duration(40+rng.Intn(180)) * time.Millisecond)
	closeTime := acceptedTime.Add(time.Duration(2+rng.Intn(14))*time.Minute + time.Duration(rng.Intn(40))*time.Second)
	dayEnd := timeline.day.Add(24*time.Hour - time.Microsecond)
	if closeTime.After(dayEnd) {
		closeTime = dayEnd
	}

	records = append(records,
		newAuthRecord(connectionTime, "sshd", pid, fmt.Sprintf("Connection from %s port %d on %s port 22", ip, port, authHostIP)),
		newAuthRecord(bannerTime, "sshd", pid, fmt.Sprintf("Client protocol version 2.0; client software version %s", pickOne(rng, authSSHClients))),
		newAuthRecord(acceptedTime, "sshd", pid, fmt.Sprintf("Accepted %s for %s from %s port %d ssh2", method, user, ip, port)),
		newAuthRecord(openTime, "sshd", pid, fmt.Sprintf("pam_unix(sshd:session): session opened for user %s by (uid=0)", user)),
		newAuthRecord(sessionTime, "systemd-logind", logindPID, fmt.Sprintf("New session %d of user %s", 10+rng.Intn(500), user)),
		newAuthRecord(closeTime, "sshd", pid, fmt.Sprintf("pam_unix(sshd:session): session closed for user %s", user)),
	)

	return authEvent{
		Records:      records,
		AttemptHour:  hour,
		User:         user,
		FailureCount: failureCount,
		SuccessCount: 1,
		SourceIPs:    []string{ip},
	}
}

func authPreauthNoiseRecord(timestamp time.Time, user, ip string, port int, invalid bool, pid int, rng *rand.Rand) authRecord {
	switch rng.Intn(3) {
	case 0:
		return newAuthRecord(timestamp, "sshd", pid, fmt.Sprintf("Did not receive identification string from %s port %d", ip, port))
	case 1:
		if invalid {
			return newAuthRecord(timestamp, "sshd", pid, fmt.Sprintf("Connection closed by invalid user %s %s port %d [preauth]", user, ip, port))
		}
		return newAuthRecord(timestamp, "sshd", pid, fmt.Sprintf("Connection closed by authenticating user %s %s port %d [preauth]", user, ip, port))
	default:
		return newAuthRecord(timestamp, "sshd", pid, fmt.Sprintf("Connection closed by %s port %d [preauth]", ip, port))
	}
}

func randomAuthNoiseRecord(timeline authTimeline, rng *rand.Rand) authRecord {
	hour := randomAuthFailureHour(rng)
	ip := randomAuthIPFromSubnet(pickOne(rng, authBotSubnets), rng)
	port := randomClientPort(rng)
	timestamp := timeline.burstStart(hour, 2, rng).Add(time.Duration(rng.Intn(6)) * time.Second)
	return authPreauthNoiseRecord(timestamp, pickOne(rng, authInvalidUsers), ip, port, rng.Intn(2) == 0, authPID("sshd", rng), rng)
}

func errorSyslogRecord(timeline syslogTimeline, hour int, service string, rng *rand.Rand) syslogRecord {
	timestamp := timeline.randomTimestamp(rng)
	if hour >= 0 {
		timestamp = timeline.timestampInHour(hour, rng)
	}
	return syslogRecord{
		Timestamp: timestamp,
		Host:      scenarioHostname(),
		Program:   service,
		PID:       syslogPID(service, rng),
		Message:   pickOne(rng, errorMessages(service)),
	}
}

func nonErrorSyslogMessage(service string, rng *rand.Rand) string {
	switch service {
	case "nginx":
		return pickOne(rng, []string{
			"signal process started",
			"gracefully shutting down worker process",
			"client 10.10.4.22 closed keepalive connection",
		})
	case "postgres":
		return pickOne(rng, []string{
			`checkpoint complete: wrote 1842 buffers (11.2%); 0 WAL file(s) added, 0 removed, 1 recycled; write=73.421 s, sync=0.018 s, total=73.463 s`,
			`connection authorized: user=app database=lab application_name=api`,
			`automatic vacuum of table "public.jobs": index scans: 1`,
		})
	case "app-worker":
		return pickOne(rng, []string{
			"drained 14 jobs from queue invoices",
			"processed batch 8821 in 183ms",
			"lease renewed for worker shard 3",
		})
	case "haproxy":
		return pickOne(rng, []string{
			"Proxy api started.",
			"Server api/api-2 is UP, reason: Layer7 check passed, code: 200, check duration: 4ms.",
			"Reloading HAProxy",
		})
	case "sudo":
		return pickOne(rng, []string{
			"pam_unix(sudo:session): session opened for user root by alice(uid=1000)",
			"pam_unix(sudo:session): session closed for user root",
		})
	case "redis":
		return pickOne(rng, []string{
			"Background saving terminated with success",
			"DB loaded from disk: 0.421 seconds",
			"Ready to accept connections",
		})
	case "backup-agent":
		return pickOne(rng, []string{
			"Completed snapshot for volume /var/lib/postgresql/data in 18.4s",
			"Pruned 3 expired recovery points for policy nightly-db",
			"Verified manifest for policy nightly-db",
		})
	case "systemd":
		return pickOne(rng, []string{
			"Started Session 248 of user ubuntu.",
			"Starting Cleanup of Temporary Directories...",
			"Stopped User Manager for UID 1000.",
		})
	case "kernel":
		return pickOne(rng, []string{
			"eth0: Link is Up - 1000Mbps/Full - flow control rx/tx",
			"EXT4-fs (sda1): mounted filesystem with ordered data mode. Opts: (null)",
			"nf_conntrack: default automatic helper assignment has been turned off",
		})
	case "dhclient":
		return pickOne(rng, []string{
			"DHCPREQUEST on eth0 to 10.0.2.2 port 67",
			"DHCPACK of 10.0.2.15 from 10.0.2.2",
			"bound to 10.0.2.15 -- renewal in 34827 seconds",
		})
	case "CRON":
		return pickOne(rng, []string{
			"(root) CMD (/usr/lib/apt/apt.systemd.daily)",
			"(backup) CMD (/usr/local/bin/backup-agent run --incremental)",
			"(www-data) CMD (test -x /usr/bin/certbot && certbot -q renew)",
		})
	default:
		return pickOne(rng, []string{
			"completed scheduled maintenance task",
			"state transition completed",
			"health check passed",
		})
	}
}

func errorMessages(service string) []string {
	switch service {
	case "nginx":
		return []string{
			`[error] 1268#1268: *4921 upstream timed out (110: Connection timed out) while reading response header from upstream, client: 10.10.4.12, server: _, request: "GET /api/v1/orders HTTP/1.1", upstream: "http://10.20.1.12:8080/api/v1/orders", host: "portal.example.com"`,
			`[error] 1244#1244: *311 open() "/var/cache/nginx/proxy_temp/7/00/0000000007" failed (13: Permission denied) while reading upstream`,
			`[error] 1289#1289: *885 connect() failed (111: Connection refused) while connecting to upstream, client: 10.10.4.22, server: _, request: "POST /billing/invoices HTTP/1.1", upstream: "http://10.20.1.44:9000/billing/invoices", host: "portal.example.com"`,
		}
	case "postgres":
		return []string{
			`ERROR:  deadlock detected`,
			`ERROR:  could not serialize access due to concurrent update`,
			`ERROR:  could not extend file "base/16384/2619": No space left on device`,
		}
	case "app-worker":
		return []string{
			"job error: failed to publish batch 8821: context deadline exceeded",
			"stream error: checkpoint commit for shard 3 failed: leader not available",
			"worker error: invoice export retry budget exhausted for customer 1842",
		}
	case "haproxy":
		return []string{
			"[ALERT]    (1942) : parsing [/etc/haproxy/haproxy.cfg:42] : error detected while parsing a 'server' line.",
			"backend api/api-3 connection error during SSL handshake",
			"server payments/payment-2 error limit reached, server disabled",
		}
	case "redis":
		return []string{
			"Error accepting a client connection: Connection reset by peer",
			"Error writing to the AOF buffer: No space left on device",
			"Short read or OOM loading DB. Unrecoverable error, aborting now.",
		}
	case "backup-agent":
		return []string{
			"job error: upload of chunk 004217 for snapshot nightly-db failed: context deadline exceeded",
			"repository error: manifest verification failed for snapshot nightly-db",
			"storage error: unable to flush pack file to object store: broken pipe",
		}
	default:
		return []string{
			"error: unexpected service failure",
		}
	}
}

func scenarioDay(rng *rand.Rand) time.Time {
	return time.Date(2026, time.March, 10+rng.Intn(10), 0, 0, 0, 0, time.UTC)
}

func (t authTimeline) burstStart(hour, attempts int, rng *rand.Rand) time.Time {
	minutes := []int{
		4 + rng.Intn(5),
		18 + rng.Intn(6),
		35 + rng.Intn(8),
	}
	if attempts <= 24 {
		minutes = append(minutes, 50+rng.Intn(5))
	}

	minute := pickOne(rng, minutes)
	second := rng.Intn(10)
	microsecond := rng.Intn(1_000_000)
	return t.day.Add(
		time.Duration(hour)*time.Hour +
			time.Duration(minute)*time.Minute +
			time.Duration(second)*time.Second +
			time.Duration(microsecond)*time.Microsecond,
	)
}

func (t authTimeline) isolatedStart(hour int, rng *rand.Rand) time.Time {
	minute := pickOne(rng, []int{
		1 + rng.Intn(3),
		14 + rng.Intn(3),
		29 + rng.Intn(4),
		45 + rng.Intn(4),
	})
	second := 10 + rng.Intn(40)
	microsecond := rng.Intn(1_000_000)
	return t.day.Add(
		time.Duration(hour)*time.Hour +
			time.Duration(minute)*time.Minute +
			time.Duration(second)*time.Second +
			time.Duration(microsecond)*time.Microsecond,
	)
}

func randomAuthFailureHour(rng *rand.Rand) int {
	switch roll := rng.Intn(100); {
	case roll < 24:
		return rng.Intn(6)
	case roll < 42:
		return 6 + rng.Intn(4)
	case roll < 68:
		return 10 + rng.Intn(8)
	default:
		return 18 + rng.Intn(6)
	}
}

func randomAuthSuccessHour(rng *rand.Rand) int {
	switch roll := rng.Intn(100); {
	case roll < 12:
		return 6 + rng.Intn(3)
	case roll < 72:
		return 9 + rng.Intn(8)
	default:
		return 17 + rng.Intn(5)
	}
}

func randomAuthFailureBurstSize(rng *rand.Rand) int {
	if rng.Intn(8) == 0 {
		return 20 + rng.Intn(31)
	}
	return 2 + rng.Intn(5)
}

func authFailureMethod(user string) string {
	if containsString(authServiceUsers, user) {
		return "publickey"
	}
	return "password"
}

func authSuccessMethod(user string, rng *rand.Rand) string {
	if containsString(authServiceUsers, user) {
		return "publickey"
	}
	if containsString(authMFAUsers, user) && rng.Intn(5) == 0 {
		return "keyboard-interactive/pam"
	}
	if rng.Intn(4) == 0 {
		return "publickey"
	}
	return "password"
}

func authBurstIPs(rng *rand.Rand, attempts int) []string {
	subnet := pickOne(rng, authBotSubnets)
	count := 1
	if attempts >= 6 {
		count = 2
	}
	if attempts >= 18 {
		count = 3
	}
	return uniqueAuthIPsFromSubnets(rng, count, [][3]int{subnet})
}

func uniqueAuthSourceIPs(rng *rand.Rand, count int) []string {
	subnets := make([][3]int, 0, minInt(3, maxInt(1, count/6)))
	for len(subnets) < cap(subnets) {
		subnet := pickOne(rng, authBotSubnets)
		if containsSubnet(subnets, subnet) {
			continue
		}
		subnets = append(subnets, subnet)
	}
	if len(subnets) == 0 {
		subnets = append(subnets, pickOne(rng, authBotSubnets))
	}
	return uniqueAuthIPsFromSubnets(rng, count, subnets)
}

func uniqueAuthIPsFromSubnets(rng *rand.Rand, count int, subnets [][3]int) []string {
	out := make([]string, 0, count)
	seen := make(map[string]struct{}, count)
	for len(out) < count {
		ip := randomAuthIPFromSubnet(pickOne(rng, subnets), rng)
		if _, ok := seen[ip]; ok {
			continue
		}
		seen[ip] = struct{}{}
		out = append(out, ip)
	}
	return out
}

func randomAuthIPFromSubnet(subnet [3]int, rng *rand.Rand) string {
	return fmt.Sprintf("%d.%d.%d.%d", subnet[0], subnet[1], subnet[2], 1+rng.Intn(253))
}

func randomClientPort(rng *rand.Rand) int {
	return 20000 + rng.Intn(40000)
}

func newAuthRecord(timestamp time.Time, program string, pid int, message string) authRecord {
	return authRecord{
		Timestamp: timestamp,
		Host:      scenarioHostname(),
		Program:   program,
		PID:       pid,
		Message:   message,
	}
}

func newSyslogTimeline(day time.Time, rng *rand.Rand) syslogTimeline {
	timeline := syslogTimeline{day: day}
	for hour := 0; hour < 24; hour++ {
		clusterCount := 1 + rng.Intn(3)
		centers := make([]int, 0, clusterCount)
		minute := rng.Intn(12)
		for i := 0; i < clusterCount; i++ {
			minute += 8 + rng.Intn(18)
			if minute > 57 {
				break
			}
			centers = append(centers, minute)
		}
		if len(centers) == 0 {
			centers = append(centers, 10+rng.Intn(40))
		}
		timeline.minuteClusters[hour] = centers
	}
	return timeline
}

func (t syslogTimeline) randomTimestamp(rng *rand.Rand) time.Time {
	return t.timestampInHour(weightedSyslogHour(rng), rng)
}

func (t syslogTimeline) timestampInHour(hour int, rng *rand.Rand) time.Time {
	center := pickOne(rng, t.minuteClusters[hour])
	minute := clampInt(center+(rng.Intn(7)-rng.Intn(7)), 0, 59)
	if rng.Intn(12) == 0 {
		minute = clampInt(minute+(rng.Intn(18)-rng.Intn(18)), 0, 59)
	}
	second := clampInt(rng.Intn(60)+(rng.Intn(11)-rng.Intn(11)), 0, 59)
	microsecond := rng.Intn(1_000_000)
	return t.day.Add(
		time.Duration(hour)*time.Hour +
			time.Duration(minute)*time.Minute +
			time.Duration(second)*time.Second +
			time.Duration(microsecond)*time.Microsecond,
	)
}

func randomTimestamp(day time.Time, rng *rand.Rand) time.Time {
	return day.Add(
		time.Duration(rng.Intn(24))*time.Hour +
			time.Duration(rng.Intn(60))*time.Minute +
			time.Duration(rng.Intn(60))*time.Second,
	)
}

func withinHour(day time.Time, hour int, rng *rand.Rand) time.Time {
	return day.Add(
		time.Duration(hour)*time.Hour +
			time.Duration(rng.Intn(60))*time.Minute +
			time.Duration(rng.Intn(60))*time.Second,
	)
}

func hourWindow(hour int) string {
	return fmt.Sprintf("%02d:00-%02d:59", hour, hour)
}

func weightedSyslogHour(rng *rand.Rand) int {
	switch roll := rng.Intn(100); {
	case roll < 10:
		return rng.Intn(6)
	case roll < 25:
		return 6 + rng.Intn(4)
	case roll < 78:
		return 10 + rng.Intn(8)
	case roll < 92:
		return 18 + rng.Intn(4)
	default:
		return 22 + rng.Intn(2)
	}
}

func randomPathUnderPrefix(prefix string, rng *rand.Rand) string {
	switch prefix {
	case "/downloads/", "/exports/", "/reports/":
		return prefix + pickOne(rng, downloadLeaves[prefix])
	case "/api/v1/":
		return prefix + pickOne(rng, apiLeaves)
	case "/billing/":
		return prefix + pickOne(rng, []string{"invoices", "statements", "aging", "customers", "balances"})
	case "/admin/":
		return prefix + pickOne(rng, []string{"users", "roles", "audit", "feature-flags", "integrations"})
	case "/assets/":
		return prefix + pickOne(rng, []string{"app.css", "app.js", "logo.svg", "runtime.js", "vendor.js"})
	case "/app/":
		return prefix + pickOne(rng, []string{"dashboard", "projects", "settings", "activity", "usage"})
	default:
		return prefix + "index"
	}
}

func uniquePublicIPs(rng *rand.Rand, count int) []string {
	out := make([]string, 0, count)
	seen := make(map[string]struct{}, count)
	for len(out) < count {
		ip := randomPublicIP(rng)
		if _, ok := seen[ip]; ok {
			continue
		}
		seen[ip] = struct{}{}
		out = append(out, ip)
	}
	return out
}

func uniquePrivateIPs(rng *rand.Rand, count int) []string {
	out := make([]string, 0, count)
	seen := make(map[string]struct{}, count)
	for len(out) < count {
		ip := randomPrivateIP(rng)
		if _, ok := seen[ip]; ok {
			continue
		}
		seen[ip] = struct{}{}
		out = append(out, ip)
	}
	return out
}

func randomPublicIP(rng *rand.Rand) string {
	blocks := [][]int{
		{198, 51, 100},
		{203, 0, 113},
		{192, 0, 2},
	}
	block := pickOne(rng, blocks)
	return fmt.Sprintf("%d.%d.%d.%d", block[0], block[1], block[2], 1+rng.Intn(253))
}

func randomPrivateIP(rng *rand.Rand) string {
	return fmt.Sprintf("10.%d.%d.%d", 10+rng.Intn(20), rng.Intn(255), 1+rng.Intn(253))
}

func splitTotal(total, parts, minChunk int, rng *rand.Rand) []int {
	if parts <= 1 {
		return []int{total}
	}
	out := make([]int, parts)
	for i := range out {
		out[i] = minChunk
	}
	remaining := total - (parts * minChunk)
	for remaining > 0 {
		index := rng.Intn(parts)
		add := 1 + rng.Intn(minInt(remaining, 2048))
		out[index] += add
		remaining -= add
	}
	return out
}

func authValidUsers() []string {
	users := append([]string(nil), authUsers...)
	users = append(users, authServiceUsers...)
	users = append(users, "root")
	return users
}

func shuffleStrings(rng *rand.Rand, values []string) {
	rng.Shuffle(len(values), func(i, j int) {
		values[i], values[j] = values[j], values[i]
	})
}

func scenarioHostname() string {
	scenarioHostOnce.Do(func() {
		host, err := os.Hostname()
		host = strings.TrimSpace(host)
		if err != nil || host == "" {
			scenarioHost = "lab-vm"
			return
		}
		scenarioHost = host
	})
	return scenarioHost
}

func authPID(program string, rng *rand.Rand) int {
	switch program {
	case "systemd-logind":
		return 700 + rng.Intn(800)
	case "sshd":
		return 1000 + rng.Intn(8000)
	default:
		return 1000 + rng.Intn(8000)
	}
}

func syslogPID(program string, rng *rand.Rand) int {
	switch program {
	case "kernel":
		return 0
	case "systemd":
		return 1
	case "dhclient":
		return 500 + rng.Intn(300)
	case "CRON":
		return 1800 + rng.Intn(400)
	case "nginx":
		return 800 + rng.Intn(600)
	case "postgres":
		return 1200 + rng.Intn(1500)
	case "haproxy":
		return 1000 + rng.Intn(500)
	case "redis":
		return 900 + rng.Intn(500)
	case "backup-agent":
		return 2000 + rng.Intn(1500)
	case "app-worker":
		return 3000 + rng.Intn(2500)
	case "sudo":
		return 4000 + rng.Intn(3000)
	default:
		return 1000 + rng.Intn(8000)
	}
}

func pickOne[T any](rng *rand.Rand, values []T) T {
	return values[rng.Intn(len(values))]
}

type weightedString struct {
	Value  string
	Weight int
}

type weightedStrings []weightedString

type weightedInt struct {
	Value  int
	Weight int
}

type weightedInts []weightedInt

func pickWeightedString(rng *rand.Rand, values weightedStrings) string {
	total := 0
	for _, value := range values {
		total += value.Weight
	}
	roll := rng.Intn(total)
	for _, value := range values {
		roll -= value.Weight
		if roll < 0 {
			return value.Value
		}
	}
	return values[len(values)-1].Value
}

func pickWeightedInt(rng *rand.Rand, values weightedInts) int {
	total := 0
	for _, value := range values {
		total += value.Weight
	}
	roll := rng.Intn(total)
	for _, value := range values {
		roll -= value.Weight
		if roll < 0 {
			return value.Value
		}
	}
	return values[len(values)-1].Value
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func containsSubnet(values [][3]int, target [3]int) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func stableRangeForString(value string, low, high int) int {
	if high <= low {
		return low
	}
	span := high - low + 1
	return low + (stringChecksum(value) % span)
}

func stringChecksum(value string) int {
	sum := 0
	for i, r := range value {
		sum += (i + 1) * int(r)
	}
	return sum
}

func defaultString(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}

func clampInt(value, low, high int) int {
	if value < low {
		return low
	}
	if value > high {
		return high
	}
	return value
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
