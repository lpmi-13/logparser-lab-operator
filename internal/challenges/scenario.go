package challenges

import (
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"strings"
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
	httpMethods  = []string{"GET", "POST", "PUT", "PATCH", "DELETE"}
	httpStatuses = []int{200, 201, 204, 301, 302, 304, 400, 401, 403, 404, 409, 429, 500, 502, 503}
	userAgents   = []string{
		"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0 Safari/537.36",
		"Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
		"Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:136.0) Gecko/20100101 Firefox/136.0",
		"curl/8.7.1",
		"python-requests/2.32.3",
		"k6/0.49.0 (https://k6.io/)",
		"Go-http-client/1.1",
	}
	referrers = []string{
		"-",
		"https://portal.example.com/dashboard",
		"https://portal.example.com/reports",
		"https://search.example.net/?q=log+lab",
		"https://docs.example.org/getting-started",
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
		"alice", "bob", "carol", "dana", "deploy", "backup", "monitor", "ops", "ubuntu", "admin", "svc-ci", "jenkins",
	}
	syslogUsers    = []string{"alice", "bob", "deploy", "ops", "ubuntu", "analyst"}
	syslogServices = []string{"nginx", "postgres", "app-worker", "haproxy", "redis", "backup-agent", "cron", "systemd"}
)

type accessRecord struct {
	Timestamp time.Time
	IP        string
	Method    string
	Path      string
	Status    int
	Bytes     int
	Referrer  string
	UserAgent string
}

type authRecord struct {
	Timestamp time.Time
	Host      string
	Program   string
	PID       int
	Message   string
}

type syslogRecord struct {
	Timestamp time.Time
	Host      string
	Program   string
	PID       int
	Message   string
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
			records = append(records, accessRecord{
				Timestamp: withinHour(day, hour, rng),
				IP:        ip,
				Method:    pickOne(rng, []string{"GET", "POST"}),
				Path:      randomPathUnderPrefix(prefix, rng),
				Status:    pickOne(rng, []int{200, 201, 204, 302}),
				Bytes:     200 + rng.Intn(4800),
				Referrer:  pickOne(rng, referrers),
				UserAgent: pickOne(rng, userAgents),
			})
		}
	}

	for len(records) < uniqueCount*5 {
		records = append(records, accessRecord{
			Timestamp: withinHour(day, hour, rng),
			IP:        pickOne(rng, relevantIPs),
			Method:    pickOne(rng, []string{"GET", "POST"}),
			Path:      randomPathUnderPrefix(prefix, rng),
			Status:    pickOne(rng, []int{200, 200, 201, 204, 304}),
			Bytes:     200 + rng.Intn(4800),
			Referrer:  pickOne(rng, referrers),
			UserAgent: pickOne(rng, userAgents),
		})
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
			records = append(records, accessRecord{
				Timestamp: withinHour(day, hour, rng),
				IP:        ip,
				Method:    pickOne(rng, httpMethods),
				Path:      randomPathUnderPrefix(pickOne(rng, accessPrefixes), rng),
				Status:    status,
				Bytes:     250 + rng.Intn(9000),
				Referrer:  pickOne(rng, referrers),
				UserAgent: pickOne(rng, userAgents),
			})
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
			records = append(records, accessRecord{
				Timestamp: withinHour(day, hour, rng),
				IP:        ip,
				Method:    pickOne(rng, []string{"GET", "POST"}),
				Path:      randomPathUnderPrefix(prefix, rng),
				Status:    status,
				Bytes:     0,
				Referrer:  pickOne(rng, referrers),
				UserAgent: pickOne(rng, userAgents),
			})
		}
	}

	for len(records) < uniqueCount*6 {
		records = append(records, accessRecord{
			Timestamp: withinHour(day, hour, rng),
			IP:        pickOne(rng, relevantIPs),
			Method:    pickOne(rng, []string{"GET", "POST"}),
			Path:      randomPathUnderPrefix(prefix, rng),
			Status:    status,
			Bytes:     0,
			Referrer:  pickOne(rng, referrers),
			UserAgent: pickOne(rng, userAgents),
		})
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
			records = append(records, accessRecord{
				Timestamp: withinHour(day, hour, rng),
				IP:        pickOne(rng, ipPool),
				Method:    "GET",
				Path:      path,
				Status:    pickOne(rng, []int{200, 200, 206, 304}),
				Bytes:     700 + rng.Intn(180000),
				Referrer:  pickOne(rng, referrers),
				UserAgent: pickOne(rng, userAgents),
			})
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
	uas := append([]string(nil), userAgents...)
	shuffleStrings(rng, uas)
	targetUA := uas[0]
	counts := []int{104 + rng.Intn(18), 83 + rng.Intn(12), 61 + rng.Intn(10), 44 + rng.Intn(8)}
	records := make([]accessRecord, 0, DefaultScenarioLineCount)
	ipPool := uniquePublicIPs(rng, 18)

	for i := range counts {
		for range counts[i] {
			records = append(records, accessRecord{
				Timestamp: withinHour(day, hour, rng),
				IP:        pickOne(rng, ipPool),
				Method:    method,
				Path:      randomPathUnderPrefix(prefix, rng),
				Status:    pickOne(rng, []int{200, 200, 201, 304}),
				Bytes:     180 + rng.Intn(6400),
				Referrer:  pickOne(rng, referrers),
				UserAgent: uas[i],
			})
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
	prefix := pickOne(rng, []string{"/api/v1/", "/app/", "/assets/", "/billing/"})
	total := 180 + rng.Intn(220)
	successCount := 60 + rng.Intn(total-80)
	records := make([]accessRecord, 0, DefaultScenarioLineCount)
	ipPool := uniquePrivateIPs(rng, 40)

	for i := 0; i < successCount; i++ {
		records = append(records, accessRecord{
			Timestamp: withinHour(day, hour, rng),
			IP:        pickOne(rng, ipPool),
			Method:    pickOne(rng, []string{"GET", "POST"}),
			Path:      randomPathUnderPrefix(prefix, rng),
			Status:    pickOne(rng, []int{200, 201, 204}),
			Bytes:     180 + rng.Intn(15000),
			Referrer:  "-",
			UserAgent: pickOne(rng, userAgents),
		})
	}
	for i := successCount; i < total; i++ {
		records = append(records, accessRecord{
			Timestamp: withinHour(day, hour, rng),
			IP:        pickOne(rng, ipPool),
			Method:    pickOne(rng, []string{"GET", "POST"}),
			Path:      randomPathUnderPrefix(prefix, rng),
			Status:    pickOne(rng, []int{301, 302, 401, 404, 429, 500, 502}),
			Bytes:     80 + rng.Intn(4000),
			Referrer:  "-",
			UserAgent: pickOne(rng, userAgents),
		})
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
	method := pickOne(rng, []string{"GET", "POST"})
	ips := uniquePrivateIPs(rng, 6)
	targetIP := ips[0]
	records := make([]accessRecord, 0, DefaultScenarioLineCount)
	totals := make(map[string]int, len(ips))

	for i, ip := range ips {
		lineCount := 18 + rng.Intn(18)
		targetTotal := 0
		chunks := splitTotal(18000+(len(ips)-i)*3500+rng.Intn(2000), lineCount, 120, rng)
		for _, bytes := range chunks {
			targetTotal += bytes
			records = append(records, accessRecord{
				Timestamp: withinHour(day, hour, rng),
				IP:        ip,
				Method:    method,
				Path:      randomPathUnderPrefix(pickOne(rng, []string{"/api/v1/", "/assets/", "/billing/"}), rng),
				Status:    pickOne(rng, []int{200, 200, 201, 206}),
				Bytes:     bytes,
				Referrer:  "-",
				UserAgent: pickOne(rng, userAgents),
			})
		}
		totals[ip] = targetTotal
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

func prepareSSHTopFailedUser(activity Activity, seed int64) (Scenario, error) {
	rng := rand.New(rand.NewSource(seed))
	day := scenarioDay(rng)
	hour := rng.Intn(24)
	users := append([]string(nil), authUsers...)
	shuffleStrings(rng, users)
	targetUser := users[0]
	counts := []int{92 + rng.Intn(16), 77 + rng.Intn(12), 60 + rng.Intn(10), 42 + rng.Intn(8), 27 + rng.Intn(5)}
	records := make([]authRecord, 0, DefaultScenarioLineCount)
	sourcePool := uniquePublicIPs(rng, 40)

	for i := range counts {
		for range counts[i] {
			records = append(records, failedSSHRecord(day, hour, users[i], pickOne(rng, sourcePool), rng.Intn(2) == 0, rng))
		}
	}
	for len(records) < DefaultScenarioLineCount {
		record := randomAuthRecord(day, rng)
		if record.Timestamp.Hour() == hour && strings.Contains(record.Message, "Failed password") {
			continue
		}
		records = append(records, record)
	}

	return newAuthScenario(activity, "auth.log",
		fmt.Sprintf("Which username had the most failed SSH login attempts during the %s hour in auth.log? Count both regular and invalid users.", hourWindow(hour)),
		"Write only the username.",
		targetUser,
		records,
	), nil
}

func prepareSSHDistinctFailedIPs(activity Activity, seed int64) (Scenario, error) {
	rng := rand.New(rand.NewSource(seed))
	day := scenarioDay(rng)
	hour := rng.Intn(24)
	targetUser := pickOne(rng, authUsers)
	sourceCount := 7 + rng.Intn(18)
	sources := uniquePublicIPs(rng, sourceCount)
	records := make([]authRecord, 0, DefaultScenarioLineCount)

	for _, ip := range sources {
		for range 1 + rng.Intn(5) {
			records = append(records, failedSSHRecord(day, hour, targetUser, ip, rng.Intn(2) == 0, rng))
		}
	}
	for len(records) < DefaultScenarioLineCount {
		record := randomAuthRecord(day, rng)
		if record.Timestamp.Hour() == hour && strings.Contains(record.Message, "Failed password") &&
			(strings.Contains(record.Message, " for "+targetUser+" ") || strings.Contains(record.Message, " invalid user "+targetUser+" ")) {
			continue
		}
		records = append(records, record)
	}

	return newAuthScenario(activity, "auth.log",
		fmt.Sprintf("How many distinct source IPs caused failed SSH logins for user %s during the %s hour in auth.log?", targetUser, hourWindow(hour)),
		"Write only the number.",
		strconv.Itoa(sourceCount),
		records,
	), nil
}

func prepareSSHTopSuccessUser(activity Activity, seed int64) (Scenario, error) {
	rng := rand.New(rand.NewSource(seed))
	day := scenarioDay(rng)
	hour := rng.Intn(24)
	users := append([]string(nil), authUsers...)
	shuffleStrings(rng, users)
	targetUser := users[0]
	counts := []int{61 + rng.Intn(14), 48 + rng.Intn(10), 36 + rng.Intn(8), 23 + rng.Intn(6)}
	records := make([]authRecord, 0, DefaultScenarioLineCount)
	sourcePool := uniquePublicIPs(rng, 30)

	for i := range counts {
		for range counts[i] {
			records = append(records, acceptedSSHRecord(day, hour, users[i], pickOne(rng, sourcePool), rng))
		}
	}
	for len(records) < DefaultScenarioLineCount {
		record := randomAuthRecord(day, rng)
		if record.Timestamp.Hour() == hour && strings.Contains(record.Message, "Accepted ") {
			continue
		}
		records = append(records, record)
	}

	return newAuthScenario(activity, "auth.log",
		fmt.Sprintf("Which username has the most successful SSH logins during the %s hour in auth.log?", hourWindow(hour)),
		"Write only the username.",
		targetUser,
		records,
	), nil
}

func prepareSyslogSudoFailures(activity Activity, seed int64) (Scenario, error) {
	rng := rand.New(rand.NewSource(seed))
	day := scenarioDay(rng)
	hour := rng.Intn(24)
	targetUser := pickOne(rng, syslogUsers)
	failures := 8 + rng.Intn(18)
	records := make([]syslogRecord, 0, DefaultScenarioLineCount)

	for range failures {
		records = append(records, syslogRecord{
			Timestamp: withinHour(day, hour, rng),
			Host:      "lab-vm",
			Program:   "sudo",
			PID:       1000 + rng.Intn(8000),
			Message: fmt.Sprintf(
				"pam_unix(sudo:auth): authentication failure; logname=%s uid=1000 euid=0 tty=/dev/pts/%d ruser=%s rhost= user=root",
				targetUser,
				rng.Intn(4),
				targetUser,
			),
		})
	}
	for len(records) < DefaultScenarioLineCount {
		record := randomSyslogRecord(day, rng)
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
	services := append([]string(nil), syslogServices...)
	shuffleStrings(rng, services)
	targetService := services[0]
	counts := []int{83 + rng.Intn(14), 65 + rng.Intn(10), 44 + rng.Intn(8), 29 + rng.Intn(6)}
	records := make([]syslogRecord, 0, DefaultScenarioLineCount)

	for i := range counts {
		for range counts[i] {
			records = append(records, errorSyslogRecord(day, hour, services[i], rng))
		}
	}
	for len(records) < DefaultScenarioLineCount {
		record := randomSyslogRecord(day, rng)
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
			`%s - - [%s] "%s %s HTTP/1.1" %d %d "%s" "%s"`,
			record.IP,
			record.Timestamp.Format("02/Jan/2006:15:04:05 -0700"),
			record.Method,
			record.Path,
			record.Status,
			record.Bytes,
			record.Referrer,
			record.UserAgent,
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
	return newAccessScenario(activity, logName, question, outputFormat, expectedAnswer, records)
}

func newAuthScenario(activity Activity, logName, question, outputFormat, expectedAnswer string, records []authRecord) Scenario {
	sort.Slice(records, func(i, j int) bool {
		return records[i].Timestamp.Before(records[j].Timestamp)
	})
	lines := make([]string, len(records))
	for i, record := range records {
		lines[i] = fmt.Sprintf(
			"%s %s %s[%d]: %s",
			record.Timestamp.Format("Jan _2 15:04:05"),
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
			record.Timestamp.Format("Jan _2 15:04:05"),
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
	prefix := pickOne(rng, accessPrefixes)
	return accessRecord{
		Timestamp: randomTimestamp(day, rng),
		IP:        randomPublicIP(rng),
		Method:    pickOne(rng, httpMethods),
		Path:      randomPathUnderPrefix(prefix, rng),
		Status:    pickOne(rng, httpStatuses),
		Bytes:     120 + rng.Intn(200000),
		Referrer:  pickOne(rng, referrers),
		UserAgent: pickOne(rng, userAgents),
	}
}

func randomNginxRecord(day time.Time, rng *rand.Rand) accessRecord {
	record := randomAccessRecord(day, rng)
	record.IP = randomPrivateIP(rng)
	return record
}

func randomAuthRecord(day time.Time, rng *rand.Rand) authRecord {
	hour := rng.Intn(24)
	if rng.Intn(2) == 0 {
		return failedSSHRecord(day, hour, pickOne(rng, authUsers), randomPublicIP(rng), rng.Intn(2) == 0, rng)
	}
	return acceptedSSHRecord(day, hour, pickOne(rng, authUsers), randomPublicIP(rng), rng)
}

func randomSyslogRecord(day time.Time, rng *rand.Rand) syslogRecord {
	service := pickOne(rng, syslogServices)
	hour := rng.Intn(24)
	if rng.Intn(6) == 0 {
		return errorSyslogRecord(day, hour, service, rng)
	}
	return syslogRecord{
		Timestamp: withinHour(day, hour, rng),
		Host:      "lab-vm",
		Program:   service,
		PID:       1000 + rng.Intn(8000),
		Message:   nonErrorSyslogMessage(service, rng),
	}
}

func failedSSHRecord(day time.Time, hour int, user, ip string, invalid bool, rng *rand.Rand) authRecord {
	if invalid {
		return authRecord{
			Timestamp: withinHour(day, hour, rng),
			Host:      "lab-vm",
			Program:   "sshd",
			PID:       1000 + rng.Intn(8000),
			Message:   fmt.Sprintf("Failed password for invalid user %s from %s port %d ssh2", user, ip, 20000+rng.Intn(40000)),
		}
	}
	return authRecord{
		Timestamp: withinHour(day, hour, rng),
		Host:      "lab-vm",
		Program:   "sshd",
		PID:       1000 + rng.Intn(8000),
		Message:   fmt.Sprintf("Failed password for %s from %s port %d ssh2", user, ip, 20000+rng.Intn(40000)),
	}
}

func acceptedSSHRecord(day time.Time, hour int, user, ip string, rng *rand.Rand) authRecord {
	method := pickOne(rng, []string{"publickey", "password"})
	return authRecord{
		Timestamp: withinHour(day, hour, rng),
		Host:      "lab-vm",
		Program:   "sshd",
		PID:       1000 + rng.Intn(8000),
		Message:   fmt.Sprintf("Accepted %s for %s from %s port %d ssh2", method, user, ip, 20000+rng.Intn(40000)),
	}
}

func errorSyslogRecord(day time.Time, hour int, service string, rng *rand.Rand) syslogRecord {
	return syslogRecord{
		Timestamp: withinHour(day, hour, rng),
		Host:      "lab-vm",
		Program:   service,
		PID:       1000 + rng.Intn(8000),
		Message:   pickOne(rng, errorMessages(service)),
	}
}

func nonErrorSyslogMessage(service string, rng *rand.Rand) string {
	switch service {
	case "nginx":
		return pickOne(rng, []string{
			"worker process is shutting down",
			"gracefully reloaded configuration",
			"accepted connection from 10.0.2.15",
		})
	case "postgres":
		return pickOne(rng, []string{
			"checkpoint complete",
			"autovacuum launcher started",
			"connection authorized: user=app database=lab",
		})
	case "app-worker":
		return pickOne(rng, []string{
			"job queue depth is now 12",
			"completed batch sync in 183ms",
			"lease renewed for worker shard 3",
		})
	case "sudo":
		return pickOne(rng, []string{
			"pam_unix(sudo:session): session opened for user root by alice(uid=1000)",
			"pam_unix(sudo:session): session closed for user root",
		})
	default:
		return pickOne(rng, []string{
			"completed scheduled maintenance task",
			"rotation finished successfully",
			"state heartbeat updated",
		})
	}
}

func errorMessages(service string) []string {
	switch service {
	case "nginx":
		return []string{
			"error connecting to upstream 10.0.4.21:443",
			"ERROR upstream timed out while reading response header from upstream",
			"Error opening cache file /var/cache/nginx/temp/0003",
		}
	case "postgres":
		return []string{
			"error: could not extend relation base/16384/2619",
			"ERROR: deadlock detected while updating order queue",
			"Error writing temporary statistics file",
		}
	case "app-worker":
		return []string{
			"error processing invoice batch 8821",
			"ERROR could not reach message broker within deadline",
			"Error committing checkpoint for stream processor",
		}
	case "haproxy":
		return []string{
			"error while parsing health-check response from backend api-3",
			"ERROR failed to connect to backend payment-1",
			"Error reloading certificate bundle for frontend public_https",
		}
	case "redis":
		return []string{
			"error loading appendonly.aof: short read",
			"ERROR replica sync aborted by master",
			"Error writing command to client output buffer",
		}
	default:
		return []string{
			"error rotating archive segment",
			"ERROR health check exceeded retry budget",
			"Error flushing state checkpoint to disk",
		}
	}
}

func scenarioDay(rng *rand.Rand) time.Time {
	return time.Date(2026, time.March, 10+rng.Intn(10), 0, 0, 0, 0, time.UTC)
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

func shuffleStrings(rng *rand.Rand, values []string) {
	rng.Shuffle(len(values), func(i, j int) {
		values[i], values[j] = values[j], values[i]
	})
}

func pickOne[T any](rng *rand.Rand, values []T) T {
	return values[rng.Intn(len(values))]
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
