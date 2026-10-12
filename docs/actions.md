# Actions

An action is a Node.js module in `actions/action_<id>.js` that subclasses `ElvOAction` (`o-action.js`).
Each workflow step runs one action, in its own process.

## How an action runs

1. The job runner resolves the step's inputs and builds a **payload**:
   `{action, action_type, parameters, inputs, references: {job_id, step_id, workflow_id, root, group_reference}, max_attempts, retry_delay, idle_timeout, polling_interval}`.
2. `ElvOAction.Launch` validates the inputs against the action's `IOs()`, saves the payload to
   `steps/<step>_payload.json`, and spawns:
   `node actions/action_<id>.js execute-sync --job-id=… --step-id=…`
3. The child process reads the payload, decrypts `password` inputs, converts `numeric` and `date`
   inputs, then calls `Execute(inputs, outputs)`.
4. The value `Execute` returns is the execution code. Outputs and the final status are written to
   `steps/<step>.json`.
5. The job runner polls `checkStatus()` every `PollingInterval()` seconds. For **non-continuous**
   actions (`IsContinuous()` returns `false`), polling calls `MonitorExecution(pid, outputs)`, so a step
   can stay "ongoing" after its process has exited. `fan_out`, `subworkflow` and `schedule` work this way.

## Writing an action

```js
const ElvOAction = require("../o-action").ElvOAction;

class ElvOActionHello extends ElvOAction {

    ActionId() {
        return "hello";             // must match the file name: action_hello.js
    };

    Description() {
        return "Builds a greeting";
    };

    // Static configuration, set in the step's configuration.parameters
    Parameters() {
        return {parameters: {
            action: {type: "string", required: true, values: ["GREET", "SHOUT"]}
        }};
    };

    // Inputs and outputs can depend on the parameters
    IOs(parameters) {
        let inputs = {name: {type: "string", required: true}};
        let outputs = {greeting: {type: "string"}};
        return {inputs, outputs};
    };

    PollingInterval() {
        return 5;                   // seconds between status checks
    };

    async Execute(inputs, outputs) {
        try {
            let text = "Hello " + inputs.name;
            if (this.Payload.parameters.action == "SHOUT") {
                text = text.toUpperCase();
            }
            outputs.greeting = text;
            this.ReportProgress("Greeting built", text);
            return ElvOAction.EXECUTION_COMPLETE;
        } catch (err) {
            this.Error("Could not build greeting", err);
            return ElvOAction.EXECUTION_EXCEPTION;
        }
    };

    static VERSION = "0.0.1";
}

if (ElvOAction.executeCommandLine(ElvOActionHello)) {
    ElvOAction.Run(ElvOActionHello);
} else {
    module.exports = ElvOActionHello;
}
```

The final `if` block is required. It makes the file runnable as a process, and also loadable as a
module so the engine can read its spec.

### Overridable methods

| Method | Default | Purpose |
| --- | --- | --- |
| `ActionId()` | `"generic"` | Must match the file name |
| `Description()` | `""` | Shown in `/action` listings |
| `Parameters()` | `{parameters: {}}` | Static parameter spec |
| `IOs(parameters)` | `{inputs: {}, outputs: {}}` | Input and output spec |
| `Execute(inputs, outputs)` | returns 100 | The work. Fill in `outputs` and return an execution code. |
| `IsContinuous()` | `true` | `false` if the step should be monitored after `Execute` returns |
| `MonitorExecution(pid, outputs)` | — | Status check for non-continuous actions. Return 10 while ongoing. |
| `PollingInterval()` | `5` | Seconds between status checks |
| `IdleTimeout()` | `null` | Seconds without progress before the step is considered stuck |
| `MaxMemory()` | `null` | MB. Sets `--max-old-space-size` for the child process. |

### Execution codes

Return one of `ElvOAction.EXECUTION_COMPLETE` (100), `EXECUTION_FAILED` (99),
`EXECUTION_EXCEPTION` (-1) or `EXECUTION_ONGOING` (10, only for non-continuous actions).
A continuous action that returns a non-final code is treated as an exception.

### Input and output types

`string`, `numeric`, `boolean`, `object`, `array`, `date`, `datetime`, `time`, `file`, `password`.

- `numeric` inputs given as strings like `"30000/1001"` are evaluated into numbers.
- `date` inputs become `Date` objects.
- `password` inputs of the form `p__:<…>` are decrypted with O's key.
- For `numeric`, `date` and `password` inputs, the raw value is kept in `inputs["<name>.original"]`.

### Logging and progress

- `this.ReportProgress(message, details)` writes a progress line to the step's tracker log. The latest
  progress line is shown by `job_status` and `job-details --log`.
- `this.Info`, `this.Debug` and `this.Error(label, err)` write both to `Logs/o.log` and to the tracker.

### Fabric access

`this.Client` is an initialized `ElvClient` signed with O's key. `ElvOFabricClient` (`o-fabric.js`) adds
helpers such as `getMetadata`, `getWriteToken`, `FinalizeContentObject`, `getLibraryId` and `safeExec`
(which wraps calls with timeouts). Many actions also accept optional `private_key` / `config_url` inputs
to act as a different identity.

### Persistence

`this.persistData(data, scope)` and `this.getPersistedData(scope)` store JSON under `Store/`.
The scope sets how widely the data is shared:

| Scope | Shared across |
| --- | --- |
| `none` | this job's step only |
| `workflow-step` | the same step in every job of the workflow |
| `workflow` | every step of every job of the workflow |
| `group` (default) | jobs with the same `group_reference`, per step |
| `action` | every use of this action |
| `absolute` | everything |

Set the scope per step with the `persistence_scope` parameter.

## Running an action standalone

Every action file is also a CLI:

```bash
# Print the parameter spec
node actions/action_test.js specs

# Print the input/output spec for some parameters
node actions/action_test.js specs --parameters='{"code":"inputs.x > 1","variables":{"x":{"type":"numeric"}}}'

# Execute outside a workflow (polls until done; payload may be @file.json)
node actions/action_test.js execute --private-key=$PRIVATE_KEY \
  --parameters='{"code":"inputs.x > 1","variables":{"x":{"type":"numeric"}}}' \
  --inputs='{"x": 2}' --result --log
```

Standalone runs use the job ID `-` and write `/tmp/<step>.json` (result), `/tmp/<step>.log`
(tracker) and `/tmp/<step>_payload.json`. The `/execute_action` API endpoint runs actions the same way.

## Bundled actions

Most actions take an `action` parameter that selects a mode. Use `node actions/action_<id>.js specs`,
or the `/action` API endpoint, to see the exact inputs and outputs for each mode.

### Flow control and orchestration

| Action | Modes / key parameters | Purpose |
| --- | --- | --- |
| `test` | `code`, `variables` | Evaluates a JavaScript expression over the inputs. Returns `complete` if it is truthy, `failed` otherwise. |
| `fan_out` | `action_configuration`, `fan_out_variable`, `serialize`, `execute_all`, `indexed_outputs` | Runs a step template once for each element of an array |
| `subworkflow` | `workflow_id`, `synchronous` | Queues a workflow on this O and optionally waits for it |
| `remote_execution` | `workflow_id`, `synchronous` | Queues a workflow on another O through its API |
| `schedule` | `type`: `TIMED` / `PERIODIC`, `ongoing` | Waits for a time of day or day of week, or for a delay |
| `retryer` | `WAIT_FOR_STATUS` | Waits for a step to reach a status and retries on the given statuses |
| `mutex` | `ACQUIRE`, `RELEASE` | Named lock around a section of a workflow |
| `distributed_execution` | `LOCATE_FILE` | Finds which O host has a file (via `/file_info`) |
| `fabric_picker` | `PICK`, `STATUS`, `WAIT` | Picks a fabric node from a picker service |
| `id_generator` | `GENERATE_ID`, `QUICK_SUM` | Generates IDs or checksums |
| `simulate` | `KILL_ENGINE`, `KILL_STEP`, `DIE`, `END` | Fault injection for testing |
| `slack` | `text`, `blocks`, `variables` | Posts to a Slack (or generic) webhook |

### Content objects and libraries

| Action | Modes |
| --- | --- |
| `manage_object` | `CREATE`, `DELETE`, `DELETE_MULTIPLE`, `DELETE_BY_IP_TITLE_ID`, `GET_OWNER`, `FINALIZE`, `SET_GROUP_PERMISSIONS`, `LIST_VERSIONS`, `CHECK_LINKS`, `COPY_CAPS`, `LIST_PARTS`, `SET_PERMISSION`, `OPEN_WRITE_TOKEN` |
| `handle_metadata` | `READ`, `READ_MULTIPLE`, `SET`, `SET_MULTIPLE`, `LINK`, `DELETE`, `SIGN`, `LIST_PUBLIC`, `LIST_PUBLIC_ACROSS_LIBRARIES` |
| `handle_write_token` | `GET_WRITE_TOKEN`, `FINALIZE_CONTENT_OBJECT`, `DELETE_WRITE_TOKEN` |
| `handle_tags` | `CREATE`, `ADD`, `QUERY`, `WRITE_TO_FABRIC`, `LIST_ALL_TRACKS` (tagstore) |
| `manage_library` | `CREATE`, `DELETE`, `LIST_ITEMS`, `SET_TENANT`, `INDEX_LIBRARY` |
| `manage_file` | `UPLOAD`, `DOWNLOAD`, `DOWNLOAD_PART`, `DELETE`, `SED_TRANSFORM`, `JSON_PARSE`, `JSON_STRINGIFY`, `WRITE_TO_LOCAL_FILE`, `LOCAL_DELETE`, `LOCAL_MOVE`, `LOCAL_FILES_EXIST`, `CLEAR_FILE_JOBS` |
| `manage_site` | `ADD`, `DELETE`, `SHAKE` |
| `fabric_indexer_query` | `LIST_UPDATED_OBJECTS`, `LIST_UPDATED_VERSIONS`, `LIST_OBJECTS` |
| `crawl` | Crawls a site into a search index |

### Access control

| Action | Modes |
| --- | --- |
| `manage_permissions` | `ADD`, `PURGE_EXPIRED`, `ADD_FROM_JSON_PROFILE` |
| `change_permissions` | `GRANT`, `RESCIND` |
| `manage_group` | `CREATE`, `LIST_MEMBERS` |
| `manage_user` | `CREATE`, `TOPOFF` |

### Media processing

| Action | Modes / key parameters |
| --- | --- |
| `master_init` | Initializes a production master |
| `create_production_master` | `aws_s3`, `use_s3_signed_url`, `do_not_finalize` |
| `manage_production_master` | `MASTER_INIT`, `PROBE_SOURCES`, `PROBE_ALL_FILES`, `CREATE`, `CACHE_AWS_SOURCES` |
| `create_mezzanine` | Creates or updates a mezzanine (`aws_s3`, `add_downloadable_offering`, `modify_existing_mezzanine`, …) |
| `manage_mezzanine` | `CLIP`, `FINALIZE`, `CHECK_PLAYABLE`, offering, stream and rung management, `REGEN_DRM_KEYS`, `SET_HDR10_DATA`, … |
| `create_ladder` | `CREATE`, `ADD_HIGH_BITRATE_RUNGS` |
| `create_spritesheet` | Generates thumbnail sprite sheets |
| `manage_watermark` | `SET`, `DELETE`, `SET_WITH_DOWNLOADABLE` × `TEXT` / `IMAGE` / `HTML` / `COMPOSITE` |
| `manage_captions` | `ADD`, `CONVERT_TO_VTT`, `TRANSLATE`, `CLEAR`, `OFFSET`, `FIX_CAPTIONS_OFFSET`, `NORMALIZE_IMSC_TIMECODES`, `CLEAN_UP`, `DOWNLOAD` |
| `variant_edit_stream` | `EDIT`, `ADD`, `REMOVE` |
| `ffprobe` | Runs ffprobe on a local or S3 file |
| `ffmpeg` | Runs ffmpeg with templated command-line options |
| `dolby_utility` | Dolby Atmos and Dolby Vision (2D/3D) encoding and preparation |
| `mvhevc_encoder` | `ENCODE`, `REENCODE`, `COMBINE`, `PACKAGE` (MV-HEVC / spatial video) |
| `kickoff_detector` | Detects the kickoff time in a match video through an external service |
| `epcr_variants`, `urc_variants` | Customer-specific variant pipelines (probing, conforming, QC, Opta match lookup) |

### Transfers and files

| Action | Modes |
| --- | --- |
| `aws_s3_operation` | `LIST`, `DOWNLOAD_FILE`, `UPLOAD_FILE(S)`, `CREATE_DOWNLOAD_LINK`, `CREATE_REMOTE_FILE`, Glacier retrieval and status, `SEND_BACK_TO_GLACIER` |
| `https_transfer` | `UPLOAD`, `DOWNLOAD`, `DOWNLOAD_FILE` |
| `scp_transfer` | `UPLOAD`, `DOWNLOAD`, `DOWNLOAD_FILE` (password or key authentication) |
| `local_file_watcher` | `FOLDER_WATCH`, `ASPERA_MANIFEST_WATCH`. Can be used as a workflow trigger. |
