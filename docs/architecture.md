# Architecture

## Overview

```
            HTTP API (o-svc)                CLI (o-cmd)
                  │                              │
                  ▼                              ▼
          ┌──────────────┐               ┌──────────────┐
          │   Queues/    │◄──────────────│  queue-item, │
          │ (o-queue)    │               │ create-queue │
          └──────┬───────┘               └──────────────┘
                 │ pop (respecting throttles)
                 ▼
  ┌───────────────────────────────┐
  │ Engine loop (o.js service)    │  every heartbeat:
  │  PopFromQueueAndCreateJobs()  │   1. pop queued items → create jobs
  │  RunJobs()                    │   2. spawn a job runner per running job
  └──────────────┬────────────────┘
                 │ spawn: node o.js run-job --job-id=…
                 ▼
  ┌───────────────────────────────┐
  │ Job runner (one per job)      │  loop every job heartbeat:
  │  executeReadySteps()          │   - launch steps whose prerequisites are met
  │  checkStatusOfOngoingSteps()  │   - poll running steps
  │  checkForCompletion()         │   - test end states, mark job executed
  └──────────────┬────────────────┘
                 │ spawn: node actions/action_<id>.js execute-sync --job-id --step-id
                 ▼
  ┌───────────────────────────────┐
  │ Action process (one per step) │  reads the step payload, runs Execute(),
  │  (o-action subclass)          │  writes outputs and a progress tracker log
  └───────────────────────────────┘
```

Each layer runs as a separate, detached OS process:

| Process | Started by | Lifetime | PID file |
| --- | --- | --- | --- |
| Engine (`service`) | an operator | until `stop` | `<pid-file-dir>/o.pid` |
| API only (`api-service`) | an operator | until `stop-api` | `<pid-file-dir>/o-api.pid` |
| Job runner (`run-job`) | the engine | until the job reaches an end state or is no longer in `Jobs/running` | — |
| Action (`execute-sync`) | the job runner | until `Execute()` returns (or longer for non-continuous actions that are polled) | — |

Running jobs and steps as independent processes means one crashing action cannot take the engine down.
It also means a restarted engine picks jobs back up from disk.

## Content Fabric integration

- On startup, `ElvO.Initialize` builds an `ElvClient` from `--config-url` (default: Eluvio main net),
  using `--private-key` as the signer.
- The **O object** (`--o-id`) holds instance configuration in its metadata:
  - `throttle/<workflow_id>`: per-workflow concurrency limits (see `throttle`).
  - `authorized_address/<address>`: an array of URL regexes each client address may call on the API.
  - `public/name`, `service_url`.
- **Workflow objects** (`iq__…`) store the workflow definition under the `workflow_definition`
  metadata key. Definitions are cached locally in `Workflows/<workflow_id>.json`. Refresh the cache
  with `node o.js refresh`.
- When a job finishes, O reads `throttle/<workflow_id>` from the O object with a
  `User-Agent: o-execution-reporting` header. This records the execution on the fabric side.

## Job lifecycle

1. **Queued.** A job description (`{id, workflow_id, parameters, ...}`) is written as a file into
   `Queues/<queue_id>/`.
2. **Created.** The engine pops items from active queues, highest priority first. It skips items whose
   workflow has reached its throttle limit. For each popped item it creates
   `Jobs/data/<ref_hex>/job.json`, hard-links it into `Jobs/running/<job_id>`, and sends the
   "Job initiated" webhook.
3. **Ongoing.** A job runner process loads every step's state. It launches each step whose
   `prerequisites` are met and polls running steps every `polling_interval` seconds.
4. **Executed.** When an end state's prerequisites are met, the step results are merged into
   `job.json`, the running link is removed, and a link is added under `Jobs/<status>/`. Then the
   "Job executed" webhook is sent.

### Identifiers

- **Job reference**: a caller-supplied unique string (`job_reference`). If none is given, O
  generates `job_<timestamp>`. A reference cannot be re-queued while a job using it is still in progress.
- **Reference hex**: `0x` + keccak256(reference). This names the job's data folder.
- **Job ID**: `j_<start_ms>_<workflow_id>_<ref_hex>`.

### Status codes

Job status (`workflow_execution.status_code`):

| Code | Meaning |
| --- | --- |
| 1 | queued |
| 5 | created (restarted, waiting to resume) |
| 10 | ongoing |
| 99 | failed |
| 100 | complete |
| -1 | exception |
| -5 | canceled |

Step / action execution codes:

| Code | Constant | Meaning |
| --- | --- | --- |
| 0 / 5 | — | not started / initiated |
| 10 | `EXECUTION_ONGOING` | started, still running |
| 99 | `EXECUTION_FAILED` | ran to completion with a negative outcome (e.g. a test returned false) |
| 100 | `EXECUTION_COMPLETE` | success |
| -1 | `EXECUTION_EXCEPTION` | error |
| -10 | `EXECUTION_EXCEPTION_TO_BE_RETRIED` | error, will be relaunched after `retry_delay` |

Prerequisites refer to step states by name: `complete` (100), `failed` (99), `exception` (-1),
`started` (10), `initiated` (5).

## On-disk layout

All paths are relative to the working directory, which is the repository root.

```
Queues/<queue_id>/.meta.json            queue settings {id, name, priority, active}
Queues/<queue_id>/<pppp>__<ts>__<slug>__<workflow_id>   one queued item (JSON)
Queues/<queue_id>/.error/               items that failed to become jobs
Archive/<queue_id>/                     popped items
Workflows/<workflow_id>.json            cached workflow definitions
Jobs/data/<ref_hex>/job.json            job definition and execution summary
Jobs/data/<ref_hex>/steps/<step>.json           step state and outputs
Jobs/data/<ref_hex>/steps/<step>_payload.json   payload handed to the action
Jobs/data/<ref_hex>/steps/<step>_<attempt>.log  progress tracker for each attempt
Jobs/running/<job_id>                   hard link to job.json while the job runs
Jobs/{complete,failed,exception,...}/<job_id>   hard link after execution
Store/                                  persisted action data (see Actions → persistence)
Mutex/                                  named mutex lock files
Logs/o.log, Logs/o-api.log              service logs (rotated daily)
```

Queued item names start with a zero-padded 4-digit priority. Items are processed in name order,
so a lower number means higher priority.

### Housekeeping

The engine runs these automatically:

- **Logs** are rotated (`logger.AutoRotate`). `purge-log` keeps 7 days by default.
- **Executed jobs** older than 30 days (`ElvOJob.DAYS_KEPT`) are purged once a day.
- **Archived queue items** older than 60 days (`ElvOQueue.DAYS_KEPT`) are purged.

## Concurrency control

- **Global and per-workflow throttles** come from O object metadata (`throttle`). `JobCapacity` limits
  how many jobs can be popped on each pass.
- **Mutexes** (`o-mutex.js`): named, file-based locks with hold and wait timeouts. They are available to
  workflows through the `mutex` action and to remote callers through `/acquire_mutex` and `/release_mutex`.
- **Mutex pools** (`o-mutex-pool.js`): lock one free resource out of a named set. An example is
  `crawl` with `use_resource_pool`.

## Webhooks

When a job starts and when it ends, O POSTs JSON to a webhook if one is configured. The webhook URL
is resolved in this order:

1. `parameters[parameters.web_hook_field]`
2. `parameters.web_hook`
3. the `--webhook` / `WEBHOOK` setting of the engine

The API key is resolved the same way (`web_hook_api_key_field`, `web_hook_api_key`, `--webhook-api-key`)
and is sent as the `X-Elvo-Api-Key` header. A value of the form `p__:<…>` is decrypted first.
The body contains `message`, `reference`, `workflow_id`, `job_id`, `root`, `status`, `parameters`,
`steps` (on completion, without outputs) and, when present, `ip_title_id`, `ip_asset_id` and
`delivery_id`.

## Secrets

Values of the form `p__:<ciphertext>` are ECIES-encrypted for the O identity. To produce one:

- run `node o.js encrypt --value=…`, or
- send `e__:<cleartext>` as a job parameter through `/queue_job`. O encrypts it before writing the queue file.

Action inputs declared with `type: "password"` are decrypted just before the action uses them.
