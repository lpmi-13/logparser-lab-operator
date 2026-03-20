# logparser-lab-operator

`logparser-lab-operator` is a learning-focused Kubernetes operator for practicing Linux log parsing with tools like `grep`, `awk`, `sed`, `sort`, `uniq`, `head`, and `wc`.

The operator uses Kubernetes only for the CRD, reconciliation loop, and notifications. Each round it generates one realistic log file, tailored to that round's activity, writes it into the managed logs directory, and deletes the previous round's log immediately. The learner works directly with that single active file.

Each round, the operator picks a random activity template from a catalog of 12 tasks, randomizes the prompt parameters, generates a fresh 5k-line log file, publishes the prompt through the CR status and browser notification UI, and waits for the learner to write the correct answer into an answer file. When the answer matches, the operator removes the current log, resets the answer file, and generates the next activity. The lab can continue indefinitely.

## How It Works

The default local workflow is host-run, just like the reference operator:

1. Start a Kubernetes cluster.
2. Install the CRD and seed a `LogParserLab` resource in namespace `log-lab`.
3. Run the operator on the host.
4. Solve each challenge by running shell pipelines directly against the current round log file and redirecting stdout into `/tmp/logparser-labs/<lab>/answer.txt`.
5. Watch progress in the browser notification feed on `http://localhost:8888`.

The operator tracks answers in host files under `/tmp/logparser-labs` by default.

## Example Flow

Start the lab:

```sh
./scripts/lab.sh up
```

If `8888` is already in use:

```sh
NOTIFICATION_PORT=8890 ./scripts/lab.sh up
```

If you want a different on-disk location for the generated round log:

```sh
LOGS_DIR=/path/to/logs ./scripts/lab.sh up
```

Check the challenge:

```sh
kubectl get logparserlab -n log-lab log-lab -o yaml
```

Read the current round file path:

```sh
kubectl get logparserlab -n log-lab log-lab -o jsonpath='{.status.currentLogPath}{"\n"}'
```

Solve it from your shell:

```sh
CURRENT_LOG="$(kubectl get logparserlab -n log-lab log-lab -o jsonpath='{.status.currentLogPath}')"
cat "${CURRENT_LOG}" | <your pipeline> > /tmp/logparser-labs/log-lab/answer.txt
```

The operator checks the file continuously. If the answer is correct, the current round log is deleted, the next activity is selected automatically, and the answer file is reset.

Clean up local lab state with:

```sh
./scripts/cleanup-k3s-resources.sh
```

This removes discovered `LogParserLab` resources, the lab namespaces, host-side answer directories under `/tmp/logparser-labs`, `.log` files from the managed logs directory, and any prior in-cluster operator namespace. By default it also deletes the `logparserlabs.lab.learning.io` CRD.

If you want to keep the CRD installed for a faster rerun:

```sh
./scripts/cleanup-k3s-resources.sh --keep-crd
```

If you started the lab with a custom `LOGS_DIR` or `ANSWER_ROOT`, pass the same environment variables during cleanup:

```sh
LOGS_DIR=/path/to/logs ANSWER_ROOT=/path/to/answers ./scripts/cleanup-k3s-resources.sh --keep-crd
```

`./scripts/lab.sh reset` and `./scripts/lab.sh down` reuse the same cleanup logic and keep the CRD installed.

## Activity Catalog

The lab includes 12 activity templates across four log families:

- Count unique IPs under a target prefix in a target hour
- Find the busiest Apache client IP for a target status code
- Count unique Apache error sources for a target status and prefix
- Rank the top requested paths under a target download-like prefix
- Find the most common Apache user agent for a target method and prefix
- Calculate a Nginx 2xx success rate for a target prefix and hour
- Sum Nginx bytes by IP for a target method and hour
- Find the username with the most failed SSH logins in a target hour
- Count distinct source IPs behind failed SSH logins for a target username
- Find the user with the most successful SSH logins in a target hour
- Count sudo authentication failures for a target user and hour
- Find the service with the most syslog error lines in a target hour

The operator avoids repeats until it has exhausted the full activity list, then starts a new randomized cycle.

## CRD

The sample resource is:

```yaml
apiVersion: lab.learning.io/v1alpha1
kind: LogParserLab
metadata:
  name: log-lab
  namespace: log-lab
spec:
  activity: random
```

Optional fields:

- `spec.activity`: set a fixed activity ID instead of `random`
- `spec.logsDir`: override the managed directory where the single active log file is written
- `spec.answerFile`: override the answer file path

## Notification UI

The operator runs a small SSE server on `http://localhost:8888`.

You get notifications when:

- the workspace is being prepared
- a new challenge is ready
- a changed answer file is checked but still incorrect
- a challenge is completed
- the answer file is reset

## Data Sources

The current implementation generates realistic synthetic logs locally for every round, so no external dataset is required. If you later want to expand the distributions or field shapes, public corpora can still be used as inspiration.

## Local Development

Requirements:

- Go 1.24+
- `kubectl`
- `jq`
- a reachable Kubernetes cluster

Common commands:

```sh
make manifests generate
make build
make run
go test ./...
```
