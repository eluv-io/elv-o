# CLI reference

```
node o.js <command> [--option=value ...] [--flag]
```

Run `node o.js help` (or no command) to print the built-in usage summary.

## Global options

Several options can also be set through environment variables. Where both are given, the
command-line value wins. Once read, an option's value is exported back to its environment
variable, so child processes (job runners and actions) inherit it.

| Option | Env var | Default | Purpose |
| --- | --- | --- | --- |
| `--private-key` | `PRIVATE_KEY` | — | Key O signs fabric requests with |
| `--o-id` | `O_ID` | — | O instance object ID |
| `--config-url` | `CONFIG_URL` | `https://main.net955305.contentfabric.io/config` | Fabric config URL |
| `--webhook` | `WEBHOOK` | — | Default job-status webhook |
| `--webhook-api-key` | `WEBHOOK_API_KEY` | — | Sent as `X-Elvo-Api-Key` to the webhook |
| `--pid-file-dir` | `PID_FILE_DIR` | `.` | Where `o.pid` / `o-api.pid` are written |
| `--api-port` | `API_PORT` | `8080` | HTTP API port (`0` disables it for `service`) |
| `--heartbeat-ms` | `HEARTBEAT_MS` | — (loop runs every ~500 ms) | Engine loop interval |
| `--verbose` | `VERBOSE=true` | off | Extra console output |

Exit code is `0` on success, non-zero otherwise. Long-running commands (`service`) do not exit.

## Commands that do not need fabric access

### Queues

| Command | Arguments |
| --- | --- |
| `list-queues` | `[--active \| --inactive]` |
| `create-queue` | `--queue-id=<id> [--priority=<n>] [--active] [--name=<name>]`. A lower priority number is processed first, and `0` is the highest priority. |
| `activate` | `--queue-id=<id>` |
| `deactivate` | `--queue-id=<id>` |
| `queued` | `--queue-id=<id> [--limit=<n>]`. Lists queued items as JSON. |
| `next-item` | `--queue-id=<id>[,<id>...] [--pop]`. Shows (and optionally pops) the next item. |
| `queue-item` | `--queue-id=<id> --item='<json>' [--item-id=<ref>] [--priority=<n>]` |

### Monitoring

| Command | Arguments |
| --- | --- |
| `stats` | `[--step] [--action] [--running] [--queue]`. With no flags it shows steps, running workflows and queues. It reads the process table (Linux and macOS). |

### Logs

| Command | Arguments |
| --- | --- |
| `rotate-log` | — |
| `purge-log` | `[--days-kept=<n>]` (default 7) |

## Commands that initialize a fabric client

These read `--private-key`, `--o-id` and `--config-url`.

### Service control

| Command | Description |
| --- | --- |
| `service` / `start` | Runs the engine loop, and the HTTP API unless `--api-port=0`. Writes `o.pid`. |
| `stop` | Renames `o.pid` to `o.pid.stopping` and waits for the engine to exit. |
| `api-service` / `start-api` | Runs only the HTTP API. Writes `o-api.pid`. |
| `stop-api` | Stops the API-only process. |
| `run-job` | `--job-id=<id>`. Runs one job's loop in the foreground. The engine normally spawns this for you. |

### Instance setup

```bash
node o.js instantiate-O --private-key=<admin key> --name=<name> --o-library-id=<ilib…> \
  [--service-url=<external API url>] [--max-running=<n>] [--admin-group=<group address>] \
  [--content-type=<content type id>]
```

Creates the O object and prints its ID.

### Throttles and workflows

| Command | Arguments |
| --- | --- |
| `throttle` | `[--workflow-id=<id>] [--workflow-object-id=<iq…>] [--limit=<n>]`. Without `--limit`, prints the current limit. |
| `refresh` | `--workflow-id=<id> [--workflow-object-id=<iq…>]`. Re-downloads and caches the workflow definition. |
| `workflow-io` | `--workflow-id=<id>`. Prints the workflow's `{inputs, outputs}` as JSON. |

### Jobs

`restart`, `restart-from` and `restart-after` identify the job with `--job-id`, `--job-reference` or
`--job-reference-hex`. You can also pass `--job=<value>` and let the command tell a job ID (`j_…`)
from a reference hex (`0x…`).

| Command | Arguments |
| --- | --- |
| `job-info` | `--job-reference=<ref>`. Prints `job.json`. |
| `job-details` | `--job-reference=<ref> [--step-id=<step>] [--payload \| --log \| --result]`. Without a step, lists the step files. |
| `restart` | `<job> [--renew] [--requeue]`. Restarts the whole job. |
| `restart-from` | `<job> --step-id=<step> [--renew] [--requeue] [--simple]`. Re-runs the step and everything after it. |
| `restart-after` | `<job> --step-id=<step> [--renew] [--requeue]`. Re-runs everything after the step. |
| `reset-step` | `--job-id=<id> --step-id=<step>` |
| `cancel-job` | `--job-id=<id>` |

`--renew` reloads the cached workflow definition before restarting. `--requeue` sends the restart
through a queue instead of performing it immediately.

### API access and secrets

| Command | Arguments |
| --- | --- |
| `authorize` | `--client-address=<0x…> [--authorized-url='["/.*"]']`. Writes `authorized_address/<addr>` on the O object. |
| `get-client-address-authorization-profile` | `--client-address=<0x…>` |
| `make-api-key` | `--client-address=<0x…>`. Generates a legacy ECIES API key. |
| `check-api-key` | `--client-address=<0x…> --api-key=<key>` |
| `encrypt` | `--value=<cleartext>`. Prints `p__:<ciphertext>`. |
| `decrypt` | `--value=p__:<ciphertext>` |

After you `authorize` a new address, call the API's `/refresh_authorizations` endpoint (or restart the
service) so a running service picks up the change.
