# elv-o

**O** is Eluvio's workflow orchestration engine for the Content Fabric. It takes job requests from
file-based priority queues, runs them as multi-step workflows, and runs each step as a separate
Node.js *action* process (ingest, transcoding, mezzanine creation, metadata, captions, transfers and so on).

Workflow definitions live in Content Fabric objects. Job state, queues, logs and step results
are stored on the local filesystem of the host that runs O.

## Documentation

| Document | Contents |
| --- | --- |
| [Architecture](docs/architecture.md) | Components, process model, job lifecycle, status codes, on-disk layout |
| [CLI reference](docs/cli.md) | Every `node o.js <command>`, with its arguments and environment variables |
| [HTTP API](docs/api.md) | API service endpoints, authentication and examples |
| [Workflows](docs/workflows.md) | The workflow definition format: parameters, steps, inputs, prerequisites, end states and retries |
| [Actions](docs/actions.md) | Writing a new action, running one standalone, and a catalog of the bundled actions |

## Requirements

- Node.js 18 or later (the code uses optional chaining, static class fields and the global `fetch`).
- A private key for the identity O runs as, plus an O object on the Content Fabric.
- Linux or macOS. The `stats` command parses `ps` output, so it only supports those two platforms.

## Quick start

```bash
npm install
```

Create an O instance object. Do this once, with an admin key:

```bash
node o.js instantiate-O --private-key=$ADMIN_KEY --name="My O" --o-library-id=ilib... --service-url=https://o.example.com
```

Create and activate a queue:

```bash
node o.js create-queue --queue-id=default --priority=100 --active --name="Default queue"
```

Start the engine (it also serves the HTTP API on port 8080 unless `--api-port=0` is passed):

```bash
PRIVATE_KEY=0x... O_ID=iq__... node o.js service
```

Queue a job over the API:

```bash
curl -X POST http://localhost:8080/queue_job/iq__WORKFLOW/default/my-job-ref \
  -H "api-key: $API_KEY" -H "Content-Type: application/json" \
  -d '{"some_parameter": "value"}'
```

Check on it:

```bash
node o.js job-info --job-reference=my-job-ref
```

Stop the engine:

```bash
node o.js stop
```

## Repository layout

```
o.js               CLI entry point (delegates to o-cmd.js)
o-cmd.js           CLI command dispatcher
o-svc.js           Engine service loop and HTTP API listener
o-core.js          Workflow engine: step scheduling, input resolution, completion detection
o-job.js           Job persistence, restart/cancel, archive purge, webhook notifications
o-queue.js         File-based priority queues
o-action.js        Base class and runtime for actions
o-fabric.js        Content Fabric client helpers (elv-client-js wrapper)
o-mutex.js         File-based named mutexes
o-mutex-pool.js    File-based resource pools
o-logger.js        File logger with rotation and purge
o-process.js       Argument/env parsing and process helpers
o-install.js       Fabric setup helper script
actions/           Bundled actions (one action_<id>.js file per action)
```

## License

Apache 2.0. See [LICENSE](LICENSE).
