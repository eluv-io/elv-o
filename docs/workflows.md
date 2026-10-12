# Workflows

A workflow is a graph of **steps**. Each step runs one [action](actions.md). Steps start when their
**prerequisites** are met, receive **inputs** from workflow parameters, constants or other steps'
outputs, and produce **outputs**. The workflow ends when one of its **end states** is reached.

## Where definitions live

A workflow definition is stored in the `workflow_definition` metadata of a Content Fabric object.
The object's ID (`iq__…`) is the `workflow_id` that queued jobs refer to.

O caches definitions in `Workflows/<workflow_id>.json`. Each job also keeps a frozen copy of the
definition in its own `job.json`, so editing a workflow does not affect jobs that are already running.
After editing a definition on the fabric, run:

```bash
node o.js refresh --workflow-id=iq__…
```

To make a running job pick up the new definition, restart it with `--renew`.

## Structure

```json
{
  "name": "Ingest title",
  "heartbeat": 1,
  "parameters": {
    "source_path": { "type": "string", "required": true },
    "title":       { "type": "string", "required": false, "default": "Untitled" }
  },
  "steps": {
    "probe": {
      "action": { "action": "ffprobe" },
      "configuration": {
        "parameters": { "aws_s3_inputs": false },
        "inputs": {
          "input_file_path": { "class": "parameter", "location": "source_path" }
        }
      }
    },
    "check_bitrate": {
      "action": { "action": "test" },
      "prerequisites": { "probe": "complete" },
      "retries": { "exception": { "max": 3, "delay": 30 } },
      "configuration": {
        "parameters": {
          "code": "(inputs.bit_rate > 1000000)",
          "variables": { "bit_rate": { "type": "numeric" } }
        },
        "inputs": {
          "bit_rate": { "class": "output", "step_id": "probe", "location": "results/format/bit_rate" }
        }
      }
    }
  },
  "states": {
    "complete": { "prerequisites": { "check_bitrate": "complete" } },
    "failed":   { "prerequisites": { "check_bitrate": "failed" } }
  }
}
```

### Top-level fields

| Field | Description |
| --- | --- |
| `name` | Display name. Job names are built as `<name> - <job_reference>`. |
| `parameters` | The workflow's inputs: `{name: {type, required, default}}`. |
| `steps` | `{step_id: step definition}` |
| `states` | Optional end states (see below). |
| `heartbeat` | Seconds between iterations of the job runner loop (default 1). |

## Step definition

| Field | Description |
| --- | --- |
| `action.action` | Action ID. Runs `actions/action_<id>.js`. |
| `action.type` | Action folder. Defaults to `actions`; `node-js` also maps to `actions`. |
| `configuration.parameters` | Static action parameters. These choose the action's mode and shape its inputs and outputs. See each action's `Parameters()`. |
| `configuration.inputs` | `{input_name: input source}` (see below) |
| `prerequisites` | A condition that must be true before the step starts (see below). If omitted, the step starts immediately. |
| `retries.exception.max` | Maximum number of attempts when the action ends in an exception (default 1) |
| `retries.exception.delay` | Seconds to wait before a retry (default 15) |
| `polling_interval` | Overrides the action's default status polling interval, in seconds |
| `idle_timeout` | Overrides the action's idle timeout, in seconds |

### Input sources

| `class` | Fields | Value |
| --- | --- | --- |
| `parameter` | `location` | The workflow parameter of that name, or its `default`. A required parameter with no value puts the job into exception. `location: "."` passes all parameters as one object. |
| `output` | `step_id`, `location` | An output of an earlier step. `location` is a `/`-separated path into that step's `outputs`. |
| `status` | `step_id`, `location` | A field of an earlier step's state record, such as `status_code`. |
| `constant` | `value` | A literal value |
| `info` | `location` | `timestamp` (job start in ms), `job_id` or `job_ref` |

Any input may also have a `preprocessing` field. This is a JavaScript snippet, `eval`-ed with `input`
(the resolved value) and `inputs` (all resolved inputs) in scope. Assign to `input` to change the value:

```json
"file_name": { "class": "parameter", "location": "source_path",
               "preprocessing": "input = input.split('/').pop();" }
```

**Clustered values.** If a resolved input is an object `{"cluster_type": "random", "cluster_values": [...]}`,
one of the values is picked at random and passed to the action. `round-robin` is accepted, but it
currently behaves like `random`.

### Dynamic parameters

A step parameter of the form `"%%<json template>%%"` is expanded inside the action. Each `%name%` in
the template is replaced by the input `name`, encoded according to the type that the action's `IOs()`
declares for it. The result is then parsed as JSON. The `slack` action uses this for its `text`
parameter, for example.

## Prerequisites and end-state conditions

A simple condition maps step IDs to the state each step must be in. All entries must match:

```json
{ "probe": "complete", "download": "complete" }
```

The valid states are `complete`, `failed`, `exception`, `started` and `initiated`.

A composite condition uses `"."` for a list of sub-conditions and `"#"` for how many of them must be
true. If `"#"` is missing or `0`, all of them must be true:

```json
{ "#": 1, ".": [ { "a": "complete" }, { "b": "complete" } ] }
```

Any other keys in the same object must match as well.

## End states

`states` maps an end-state name to `{prerequisites, job_status_code}`. If `job_status_code` is
omitted, it is derived from the name: `complete` → 100, `failed` → 99, `exception` → -1,
`canceled` → -5. End states are checked after every runner iteration, and the first one whose
prerequisites are met ends the job.

O fills in defaults:

- If no states are defined, `complete` requires every step to be `complete`.
- `failed` is also reached when any step that no condition mentions as `failed` fails.
- `exception` is also reached when any step that no condition mentions as `exception` ends in an
  exception.

To handle a step's failure yourself (for example with a cleanup or notification step), reference the
failure in another step's prerequisites. For example, `"notify": {"prerequisites": {"encode": "failed"}}`
stops `encode` from failing the whole job automatically.

## Retries

When an action ends in an exception and has attempts left, its step is marked
`EXECUTION_EXCEPTION_TO_BE_RETRIED` (-10). After `retries.exception.delay` seconds, the job runner
launches it again. Each attempt writes its own `<step>_<attempt>.log` tracker.

For more control over retries, such as waiting for another step to reach a status or retrying only
on certain statuses, use the `retryer` action.

## Composition patterns

| Need | Use |
| --- | --- |
| Run one step over each element of an array | `fan_out` action. Set `action_configuration` to the step template and `fan_out_variable` to the input to vary. The `<var>_array` input supplies the values. `serialize` runs the copies one after another, and `execute_all` continues past failures. Outputs: `statuses`, `fan_out_values` and `<output>_map` for each entry in `indexed_outputs`. Fanned-out steps are named `<step>_<index>`. |
| Call another workflow on the same O | `subworkflow` action (`workflow_id`, `synchronous`). Its inputs and outputs mirror the sub-workflow's. |
| Call a workflow on another O instance | `remote_execution` action (`server_address`, `api_key` inputs) |
| Wait until a time or interval | `schedule` action (`TIMED` with `ToD`/`DoW`, or `PERIODIC` with `delay_*`) |
| Serialize access to a shared resource | `mutex` action (`ACQUIRE` / `RELEASE`) |
| Branch on a condition | `test` action. It returns `complete` or `failed`. Downstream steps put their prerequisites on either outcome. |

## Job parameters with special meaning

| Parameter | Effect |
| --- | --- |
| `web_hook` / `web_hook_field` | The webhook URL for job notifications, or the name of the parameter that holds it |
| `web_hook_api_key` / `web_hook_api_key_field` | The webhook API key (may be `p__:` encrypted), or the name of the parameter that holds it |
| `ip_title_id`, `ip_asset_id`, `delivery_id` (and their `*_field` variants) | Copied into webhook payloads |
| `job_reference` | Used as the job reference if the request does not include one |
