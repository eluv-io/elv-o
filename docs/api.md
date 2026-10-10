# HTTP API

The API listener starts with `node o.js service` (unless `--api-port=0`) or with
`node o.js api-service`. It listens on `--api-port` (default `8080`).

## Conventions

- Every endpoint accepts any HTTP method. Arguments are sent as a JSON body. If the body is empty,
  they are read from the query string instead.
- `Content-Type: application/xml` bodies are passed through as a raw string. This is only
  meaningful for `queue_job`.
- `POST /` with `{"action": "<endpoint>", ...}` is the same as calling `/<endpoint>`.
- `GET /` with no body returns a self-describing list of commands and their arguments.
- Responses are JSON. CORS headers are added when the `Origin` ends in `contentfabric.io` or matches
  the `Host` header.

## Authentication

Every endpoint except `/`, `/heartbeat` and `OPTIONS` requests needs an `api-key` header.

| Key type | Format | How to obtain |
| --- | --- | --- |
| Fabric token | `api_<fabric authorization token>` | The token's `addr` becomes the client address. Its `qspace_id` must match O's content space. |
| Legacy key | ECIES ciphertext | `node o.js make-api-key --client-address=0x…`. You may also send a `client-address` header, which must match the key. |

The client address must appear in the O object's `authorized_address` metadata, and the request
path must match at least one of the regexes listed for that address. Set this up with
`node o.js authorize`. If the check fails, the API returns `401 {"error": "Invalid API Key"}`.

## Endpoints

### Health

| Endpoint | Response |
| --- | --- |
| `/heartbeat` | `{"timestamp": <ms>}`. No authentication needed. |

### Queueing jobs

#### `/queue_job` (legacy form)

```json
{
  "queue_id": "default",
  "priority": 100,
  "job_reference": "my-unique-ref",
  "job_description": {
    "workflow_id": "iq__…",
    "parameters": { "...": "..." }
  }
}
```

Response: `{"path": "<queue file>", "item": {...}, "queue_id": "default", "queued": true}`

#### `/queue_job/<workflow_id>[--<param>]/<queue_id>/<job_reference>` (path form)

The body is the workflow's parameters, either directly or wrapped as `{"job_parameters": {...}}`.
If the body is not JSON (for example XML), it is stored in a single parameter. That parameter is
named after the `--<param>` suffix on the workflow ID, or `__body` if there is no suffix.

```bash
curl -X POST "http://o-host:8080/queue_job/iq__WF/default/ref-123" \
  -H "api-key: $KEY" -d '{"source_url": "s3://…", "title": "Foo"}'
```

Response: `{"queue_id": "default", "job_reference": "ref-123", "queued": true}`

Notes:

- The API returns `400 Job reference not unique` if a job with the same reference is still in
  progress. If the earlier job with that reference has finished, its step files are archived and
  the reference is reused.
- Parameter values of the form `e__:<cleartext>` are encrypted to `p__:<ciphertext>` before they are
  written to disk.

### Job status and history

| Endpoint | Arguments | Returns |
| --- | --- | --- |
| `/job_status` | `job_reference` or `job_id`, `details` (bool) | Status, progress and step information. `{"job_id": null, "status": 0}` if the job is not found. |
| `/list_running_jobs` | — | An array of running jobs' `job.json` |
| `/list_executed_jobs` | `workflow_id`, `group_id`, `from_date`, `to_date`, `limit` | An array of `workflow_execution` records |
| `/get_job_execution_steps` | `job_id` | Per-step execution data |
| `/list_queued_items` | `queue_ids` (array), `limit` | Queued items |
| `/get_queued_job_details` | `queue_id`, `path` | One queued item (`204` if not found) |

### Job control

| Endpoint | Arguments | Notes |
| --- | --- | --- |
| `/cancel_job` | `job_id` or `job_reference`, **or** `queue_id` + `path` | Cancels a running job, or removes a queued item |
| `/restart_job` | `job_id` or `job_reference`, `step_id` | Same as `restart-from` |
| `/clear_job_reference` | `job_reference` | Deletes the job's data so the reference can be reused |

### Queues

| Endpoint | Arguments |
| --- | --- |
| `/create_jobs_queue` | `queue_id`, `queue_name`, `queue_priority` (default 100), `queue_active` (default true) |
| `/activate_jobs_queue` | `queue_id` |
| `/deactivate_jobs_queue` | `queue_id` |

### Workflows and actions

| Endpoint | Arguments | Returns |
| --- | --- | --- |
| `/workflow_io` | `workflow_id`, `workflow_object_id`, `force` | `{inputs, outputs}` for a workflow. Used by the `subworkflow` and `remote_execution` actions. |
| `/get_throttles` | `workflow_id`, `force` | Throttle limits |
| `/action` | `action`, `parameters`, `force` | With no `action`, lists every action's spec. Otherwise returns one action's spec, or its I/O for the given `parameters`. |
| `/execute_action` | `action`, `parameters`, `inputs`, `synchronous` (default true) | Runs an action synchronously on the O host and returns its result. Asynchronous mode is not implemented. |
| `/refresh_authorizations` | — | Reloads `authorized_address` from the O object |

### Mutexes and files

These endpoints let other O instances or external tools coordinate with this host.

| Endpoint | Arguments | Returns |
| --- | --- | --- |
| `/acquire_mutex` | `name`, `immortal`, `hold_timeout`, `wait_timeout` | `{"mutex": ...}`, or `408` on timeout. Blocks until the lock is acquired or the wait times out. |
| `/release_mutex` | `mutex` | `{"released": true}`, or `404` |
| `/file_info` | `file_path` | `{exists, is_file, is_directory, size, created_time_ms, modified_time_ms, accessed_time_ms}`. Used by `distributed_execution` / `LOCATE_FILE`. |

Restrict `/execute_action` and `/file_info` to trusted clients through the `authorized_address`
URL regexes. Both endpoints run code on, or read files from, the O host.
