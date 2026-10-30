#!/usr/bin/env node

const ElvOAction = require("../o-action").ElvOAction;
const { spawnSync, spawn } = require("child_process");
const path = require("path");
const fs = require("fs");

/**
 * External Game Events Action
 * 
 * Subprocess wrapper for external_game_events CLI tools.
 * Supports three operations: LOOKUP, PUBLISH (dual-mode), DELETE
 * 
 * External module location configured via parameter "external_game_events_path"
 * or environment variable EXTERNAL_GAME_EVENTS_PATH, defaulting to /home/o/external_game_events
 */
class ElvOActionExternalGameEvents extends ElvOAction {

  ActionId() {
    return "external_game_events";
  }

  /**
   * Workflow design-time parameters (static)
   */
  Parameters() {
    return {
      parameters: {
        // Operation selector - determines which CLI script to invoke
        operation: {
          type: "string",
          required: true,
          values: ["LOOKUP", "PUBLISH", "DELETE"],
          description: "Operation to perform"
        },

        // Path to translator JavaScript file (e.g., '/home/o/external_game_events/translators/epcr_rugby.js')
        translator_path: {
          type: "string",
          required: true,
          description: "Absolute path to translator file"
        },

        // Path to config JSON file with all credentials (PRIVATE_KEY, API keys, etc.)
        config_path: {
          type: "string",
          required: true,
          description: "Path to config JSON file containing PRIVATE_KEY and provider API credentials"
        },

        // Optional: override default external_game_events installation path
        external_game_events_path: {
          type: "string",
          required: false,
          description: "Base directory for external_game_events module (default: /home/o/external_game_events)"
        },

        // PUBLISH-only: execution mode control
        run_once: {
          type: "boolean",
          required: false,
          default: false,
          description: "If true, run single processing cycle (PUBLISH only). If false, run continuously until match ends"
        },

        // PUBLISH-only: trigger Content Fabric finalization when match is over
        write_to_cf: {
          type: "boolean",
          required: false,
          default: false,
          description: "If true, write finalized tags to Content Fabric when match ends (PUBLISH only)"
        },

        // Timeout for synchronous operations (LOOKUP, DELETE, PUBLISH run_once)
        timeout_secs: {
          type: "number",
          required: false,
          default: 600,
          description: "Timeout in seconds for subprocess execution"
        }
      }
    };
  }

  /**
   * Polling interval for periodic status checks (async mode)
   */
  PollingInterval() {
    return 10; // Check every 10 seconds during async monitoring
  }

  /**
   * Dynamic inputs/outputs schema based on operation
   */
  IOs(parameters) {
    const operation = parameters.operation;

    const externalGameEventsPath = this.getExternalGameEventsPath(parameters);

    // Validate translator file exists
    if (!fs.existsSync(parameters.translator_path)) {
      throw new Error(`Translator file not found: ${parameters.translator_path}`);
    }

    // Operation selector
    if (operation === "LOOKUP") {
      return {
        inputs: {
          date_from: {
            type: "string",
            required: true,
            description: "Start date in YYYY-MM-DD format"
          },
          date_to: {
            type: "string",
            required: false,
            description: "End date (exclusive) in YYYY-MM-DD format. If omitted, search only date_from"
          }
        },
        outputs: {
          success: { type: "boolean" },
          operation: { type: "string" },
          games: { type: "array", description: "Array of game objects from lookup" },
          game_count: { type: "number", description: "Number of games found" },
          error: { type: "string", required: false }
        }
      };

    } else if (operation === "PUBLISH") {
      return {
        inputs: {
          // Required inputs for publish pipeline
          content_id: {
            type: "string",
            required: true,
            description: "Content Fabric object QID for the video"
          },
          game_id: {
            type: "string",
            required: true,
            description: "Game ID from data provider (e.g., '987654' for EPCR)"
          },
          video_game_start: {
            type: "string",
            required: true,
            description: "Video timestamp where game starts in H:MM:SS:FF|HH:MM:SS:FF format or decimal seconds"
          },

          // Optional inputs with defaults (PENDING: not used by periodic_events_to_tagstore.js yet)
          frame_rate: {
            type: "string",
            required: false,
            default: "50",
            description: "[PENDING - NOT USED] Frame rate as string (e.g., '50'). Currently hardcoded in external_game_events"
          },

          // Optional inputs with defaults
          poll_interval_secs: {
            type: "number",
            required: false,
            default: 60,
            description: "[USED FOR ASYNC MODE ONLY] Polling interval in seconds for periodic processing cycles. Ignored in sync mode"
          },

          // Flags to pass to external script
          dry_run: {
            type: "boolean",
            required: false,
            default: false,
            description: "If true, generate tags but don't upload to Tagstore or Content Fabric"
          },
          force: {
            type: "boolean",
            required: false,
            default: false,
            description: "If true, bypass lock file (allows concurrent runs on same content_id)"
          },
          fresh_start: {
            type: "boolean",
            required: false,
            default: true,
            description: "If true, ignore existing state file and start from scratch (useful for re-processing)"
          }
        },
        outputs: {
          success: { type: "boolean" },
          operation: { type: "string" },
          status: { 
            type: "string",
            description: 'Execution status: "completed" (sync/once mode) or "ongoing"/"failed" (async mode)'
          },
          cycle_info: {
            type: "string",
            required: false,
            description: "Summary of last processing cycle from script logs"
          },
          error: { type: "string", required: false }
        }
      };

    } else if (operation === "DELETE") {
      return {
        inputs: {
          content_id: {
            type: "string",
            required: true,
            description: "Content Fabric object QID whose Tagstore tracks to delete"
          },
          dry_run: {
            type: "boolean",
            required: false,
            default: false,
            description: "If true, show what would be deleted but don't actually delete from Tagstore"
          },
          force: {
            type: "boolean",
            required: false,
            default: false,
            description: "If true, bypass lock file"
          }
        },
        outputs: {
          success: { type: "boolean" },
          operation: { type: "string" },
          tracks_deleted: {
            type: "array",
            description: "Array of deleted track names (empty if dry_run or no matches)"
          },
          error: { type: "string", required: false }
        }
      };
    }

    throw new Error(`Unknown operation: ${operation}`);
  }

  /**
   * Main execution logic - dispatches to operation-specific handler
   */
  async Execute(inputs, outputs) {
    const params = this.Payload.parameters;
    const operation = params.operation;

    this.Debug(`Executing action_external_game_events with operation=${operation}`);
    this.Debug(`Translator: ${params.translator_path}`);
    this.Debug(`Config: ${params.config_path}`);

    // Resolve external_game_events base directory from parameter or env var
    const externalGameEventsPath = this.getExternalGameEventsPath(params);
    
    if (!this.directoryExists(externalGameEventsPath)) {
      outputs.error = `external_game_events module not found at ${externalGameEventsPath}`;
      outputs.success = false;
      return ElvOAction.EXECUTION_FAILED;  // Not retryable - configuration issue
    }

    this.Debug(`external_game_events location: ${externalGameEventsPath}`);
    this.Payload.externalGameEventsPath = externalGameEventsPath;  // Persist for MonitorExecution

    try {
      // Route to operation-specific execution
      if (operation === "LOOKUP") {
        return await this.executeLookup(inputs, outputs, externalGameEventsPath);
      } else if (operation === "PUBLISH") {
        // Check mode: sync vs async
        const runOnce = params.run_once || inputs.run_once || false;
        
        this.Debug(`PUBLISH mode: ${runOnce ? 'run-once (sync)' : 'periodic (async)'} `);

        if (runOnce) {
          return await this.executePublishRunOnce(inputs, outputs, externalGameEventsPath);
        } else {
          return await this.executePublishAsync(inputs, outputs, externalGameEventsPath);
        }
      } else if (operation === "DELETE") {
        return await this.executeDelete(inputs, outputs, externalGameEventsPath);
      }

    } catch (err) {
      // Log full error stack for debugging
      this.Error(`Exception caught in Execute: ${err.message}`);
      this.Error(`Stack trace: ${err.stack || 'No stack'}`);      
      if (err.name) this.Debug(`Error name: ${err.name}`);
      if (err.code) this.Debug(`Error code: ${err.code}`);
      if (err.syscall) this.Debug(`Syscall: ${err.syscall}`);
      if (err.path) this.Debug(`Path involved: ${err.path}`);

      outputs.error = `Exception during execution: ${err.message}`;
      outputs.success = false;
      
      // Network errors should be retryable
      if (this.isNetworkError(err)) {
        return ElvOAction.EXECUTION_EXCEPTION;
      }

      return ElvOAction.EXECUTION_FAILED;
    }

    throw new Error(`Unreachable: no handler for operation=${operation}`);
  }

  /**
   * LOOKUP Operation: Query games by date range
   */
  async executeLookup(inputs, outputs, externalGameEventsPath) {
    const scriptPath = path.join(externalGameEventsPath, "lookupGames.js");

    if (!this.fileExists(scriptPath)) {
      throw new Error(`Script not found: ${scriptPath}`);
    }

    this.Debug(`Running LOOKUP with script: ${scriptPath}`);

    // Build CLI arguments: CONFIG_FILE TRANSLATOR_PATH [date1 [date2]]
    const args = [
      this.Payload.parameters.config_path,
      this.Payload.parameters.translator_path
    ];

    if (inputs.date_from) {
      args.push(inputs.date_from);
    }
    if (inputs.date_to) {
      args.push(inputs.date_to);
    }

    // Execute synchronously - lookup is fast (< 30 seconds typically)
    const result = this.spawnScriptSync(scriptPath, args, this.Payload.parameters.timeout_secs || 600);

    outputs.operation = "LOOKUP";

    if (result.error) {
      throw result.error;  // Exception thrown during spawn (not network error)
    }

    if (result.status === 0) {
      // Parse JSON output from lookupGames.js (it does console.log(JSON.stringify(...)))
      try {
        const parsed = JSON.parse(result.stdout.trim());
        
        if (Array.isArray(parsed)) {
          outputs.games = parsed;
          outputs.game_count = parsed.length;
        } else if (parsed && Array.isArray(parsed.games)) {
          // Alternative format if script is updated
          outputs.games = parsed.games;
          outputs.game_count = parsed.game_count || parsed.games.length;
        }

        this.Debug(`LOOKUP completed: found ${outputs.game_count} games`);
        outputs.success = true;
        return ElvOAction.EXECUTION_COMPLETE;

      } catch (parseErr) {
        // Non-JSON output - report success but no games parsed
        this.Debug(`Could not parse JSON from lookup stdout: ${parseErr.message}`);
        this.Debug(`Output was: ${result.stdout.substring(0, 200)}`);
        
        outputs.success = true;
        outputs.game_count = 0;
        outputs.games = [];
        return ElvOAction.EXECUTION_COMPLETE;
      }

    } else {
      // Script failed - check if it's a network error (retryable)
      outputs.success = false;
      
      if (this.isNetworkErrorFromStderr(result.stderr)) {
        return ElvOAction.EXECUTION_EXCEPTION;  // Retry for transient network issues
      }

      throw new Error(`lookupGames.js exited with code ${result.status}: ${result.stderr}`);
    }
  }

  /**
   * DELETE Operation: Remove Tagstore tracks
   */
  async executeDelete(inputs, outputs, externalGameEventsPath) {
    const scriptPath = path.join(externalGameEventsPath, "deleteTracksFromTagstore.js");

    if (!this.fileExists(scriptPath)) {
      throw new Error(`Script not found: ${scriptPath}`);
    }

    this.Debug(`Running DELETE with script: ${scriptPath}`);

    // Build CLI arguments: CONFIG_FILE TRANSLATOR_PATH QID [--dry-run] [--force]
    const args = [
      this.Payload.parameters.config_path,
      this.Payload.parameters.translator_path,
      inputs.content_id
    ];

    // Add flags
    if (inputs.dry_run === true) {
      args.push("--dry-run");
    }
    if (inputs.force === true) {
      args.push("--force");
    }

    // Execute synchronously - delete is fast (< 120 seconds typically)
    const result = this.spawnScriptSync(scriptPath, args, this.Payload.parameters.timeout_secs || 120);

    outputs.operation = "DELETE";

    if (result.error) {
      throw result.error;
    }

    if (result.status === 0) {
      // Delete operation successful - parse output for deleted tracks info
      this.Debug(`DELETE stdout: ${result.stdout ? result.stdout.substring(0, 200) : '(empty)'}`);
      
      // Current delete script outputs summary text, not JSON
      // We could implement parsing later if structured output is added to external_game_events
      
      outputs.success = true;
      outputs.tracks_deleted = [];  // Empty array as we don't parse track names yet
      
      return ElvOAction.EXECUTION_COMPLETE;

    } else {
      outputs.success = false;
      
      if (this.isNetworkErrorFromStderr(result.stderr)) {
        return ElvOAction.EXECUTION_EXCEPTION;
      }

      throw new Error(`deleteTracksFromTagstore.js exited with code ${result.status}: ${result.stderr}`);
    }
  }

  /**
   * PUBLISH Operation (Run-Once Mode): Single cycle execution
   */
  async executePublishRunOnce(inputs, outputs, externalGameEventsPath) {
    const scriptPath = path.join(externalGameEventsPath, "scripts", "periodic_events_to_tagstore.js");

    if (!this.fileExists(scriptPath)) {
      throw new Error(`Script not found: ${scriptPath}`);
    }

    this.Debug(`Running PUBLISH (run-once) with script: ${scriptPath}`);

    // PRE-SPAWN DEBUGGING - Log all inputs before processing
    this.Debug(`PRE-SPAWN CHECKS:`);
    this.Debug(`  inputs.content_id = '${inputs.content_id}' (type: ${typeof inputs.content_id})`);
    this.Debug(`  inputs.game_id = '${inputs.game_id}' (type: ${typeof inputs.game_id})`);
    this.Debug(`  inputs.video_game_start = '${inputs.video_game_start}' (type: ${typeof inputs.video_game_start})`);
    this.Debug(`  translator_path = '${this.Payload.parameters.translator_path}'`);
    this.Debug(`  config_path = '${this.Payload.parameters.config_path}'`);

    // Validate required inputs exist before attempting toString()
    if (!inputs.content_id) {
      throw new Error("inputs.content_id is missing or null/undefined");
    }
    if (!inputs.game_id) {
      throw new Error("inputs.game_id is missing or null/undefined");
    }
    if (inputs.video_game_start === undefined || inputs.video_game_start === null) {
      throw new Error("inputs.video_game_start is missing or null/undefined");
    }

    // Build CLI arguments per periodic_events_to_tagstore.js contract:
    // [content_id, game_id, relative_starttime, translator_path, period_seconds?] [--once] [--dry-run] [--write-to-cf] [--force]
    const args = [
      String(inputs.content_id),
      String(inputs.game_id),        // Convert number to string for CLI arg
      String(inputs.video_game_start),  // Convert number (decimal seconds) to string
      this.Payload.parameters.translator_path
    ];

    // Period seconds - default 60, but unused in --once mode anyway
    const pollInterval = inputs.poll_interval_secs || this.Payload.inputs.poll_interval_secs || 60;
    args.push(pollInterval.toString());

    // Flags (MUST use --once for synchronous mode)
    args.push("--once");

    if (inputs.dry_run === true) {
      args.push("--dry-run");
    }
    
    if (this.Payload.parameters.write_to_cf || inputs.write_to_cf) {
      args.push("--write-to-cf");
    }
    
    if (inputs.force === true) {
      args.push("--force");
    }
    
    if (inputs.fresh_start === true) {
      args.push("--fresh-start");
    }

    // Set CONFIG_PATH environment variable (required by script)
    const env = {
      ...process.env,
      CONFIG_PATH: this.Payload.parameters.config_path
    };

    // Execute synchronously with extended timeout (publish can take 5-10 minutes per cycle)
    const timeout = this.Payload.parameters.timeout_secs || 1200;  // Default 20 minutes
    const result = this.spawnScriptSync(scriptPath, args, timeout, env, true);

    outputs.operation = "PUBLISH";

    if (result.error) {
      throw result.error;
    }

    if (result.status === 0) {
      // Extract useful info from logs
      const logOutput = result.stdout || "";
      const lastCycleInfo = this.extractLastCycleInfo(logOutput);
      
      outputs.success = true;
      outputs.status = "completed";
      outputs.cycle_info = lastCycleInfo;

      this.Debug(`PUBLISH run-once completed successfully (${lastCycleInfo})`);
      return ElvOAction.EXECUTION_COMPLETE;

    } else {
      outputs.success = false;
      
      if (this.isNetworkErrorFromStderr(result.stderr)) {
        return ElvOAction.EXECUTION_EXCEPTION;  // Retry for transient issues
      }

      throw new Error(`periodic_events_to_tagstore.js exited with code ${result.status}: ${result.stderr}`);
    }
  }

  /**
   * PUBLISH Operation (Async/Periodic Mode): Spawn background process and monitor
   */
  async executePublishAsync(inputs, outputs, externalGameEventsPath) {
    const scriptPath = path.join(externalGameEventsPath, "scripts", "periodic_events_to_tagstore.js");

    if (!this.fileExists(scriptPath)) {
      throw new Error(`Script not found: ${scriptPath}`);
    }

    this.Debug(`Starting PUBLISH async with script: ${scriptPath}`);

    // Build CLI arguments (same as sync, but WITHOUT --once flag)
    const args = [
      String(inputs.content_id),
      String(inputs.game_id),
      String(inputs.video_game_start),
      this.Payload.parameters.translator_path,
      (inputs.poll_interval_secs || 60).toString()
    ];

    // Skip --once for periodic mode
    if (inputs.dry_run === true) {
      args.push("--dry-run");
    }
    
    const runOnce = this.Payload.parameters.run_once || inputs.run_once;
    if (!runOnce && (this.Payload.parameters.write_to_cf || inputs.write_to_cf)) {
      args.push("--write-to-cf");
    }
    
    if (inputs.force === true) {
      args.push("--force");
    }
    
    if (inputs.fresh_start === true) {
      args.push("--fresh-start");
    }

    // Set CONFIG_PATH env var
    const env = {
      ...process.env,
      CONFIG_PATH: this.Payload.parameters.config_path
    };

    // PRE-SPAWN DEBUGGING - Log all inputs before processing
    this.Debug(`PRE-SPAWN CHECKS (ASYNC):`);
    this.Debug(`  inputs.content_id = '${inputs.content_id}' (type: ${typeof inputs.content_id})`);
    this.Debug(`  inputs.game_id = '${inputs.game_id}' (type: ${typeof inputs.game_id})`);
    this.Debug(`  inputs.video_game_start = '${inputs.video_game_start}' (type: ${typeof inputs.video_game_start})`);

    // Validate required inputs exist before spawning
    if (!inputs.content_id) {
      throw new Error("inputs.content_id is missing or null/undefined");
    }
    if (!inputs.game_id) {
      throw new Error("inputs.game_id is missing or null/undefined");
    }
    if (inputs.video_game_start === undefined || inputs.video_game_start === null) {
      throw new Error("inputs.video_game_start is missing or null/undefined");
    }

    this.Debug(`Spawning async process with args: ${args.join(" ")}`);

    // Ensure script is executable
    try {
      fs.chmodSync(scriptPath, 0o755);
    } catch (err) {
      this.Debug(`Chmod warning (may be ignored): ${err.message}`);
    }

    // Spawn background process with pipe for stderr capture
    const child = spawn("node", [scriptPath, ...args], {
      cwd: externalGameEventsPath,
      env: env,
      stdio: ["ignore", "pipe", "pipe"],  // Don't inherit stdout/stderr from elv-o
      detached: false  // Process stays attached to parent for monitoring
    });

    this.Debug(`Background process spawned with PID: ${child.pid}`);

    // Store PID in Payload for MonitorExecution()
    const pid = child.pid.toString();
    if (pid) {
      this.Payload.pid = pid;
    } else {
      throw new Error("Failed to get PID from spawned process");
    }

    // Capture and store log output for monitoring
    let stdoutLog = "";
    let stderrLog = "";

    child.stdout.on("data", (data) => {
      const chunk = data.toString();
      stdoutLog += chunk;
      
      // Log significant milestones without flooding elv-o logs
      if (/=== Cycle \d+ @/.test(chunk)) {
        this.Debug(`PUBLISH cycle started: ${chunk.trim()}`);
      } else if (/Match is over:/i.test(chunk)) {
        this.Debug("Match has ended - process will finalize and exit");
      }      
    });

    child.stderr.on("data", (data) => {
      stderrLog += data.toString();
    });

    // Store logs temporarily for MonitorExecution to check
    const logDir = path.join("/tmp", "elv_o_game_events");
    if (!fs.existsSync(logDir)) {
      fs.mkdirSync(logDir, { recursive: true });
    }
    
    const contentId = inputs.content_id;
    const stdoutFile = path.join(logDir, `${contentId}.stdout.log`);
    const stderrFile = path.join(logDir, `${contentId}.stderr.log`);
    
    fs.writeFileSync(stdoutFile, stdoutLog);
    fs.writeFileSync(stderrFile, stderrLog);

    // Continue logging to files in background
    child.stdout.on("data", (data) => {
      fs.appendFileSync(stdoutFile, data.toString());
    });
    
    child.stderr.on("data", (data) => {
      fs.appendFileSync(stderrFile, data.toString());
    });

    // When process exits, child emits 'exit' event
    child.on("exit", (code, signal) => {
      this.Debug(`Background process exited: code=${code}, signal=${signal}`);
    });

    outputs.operation = "PUBLISH";
    
    // Initial status message
    outputs.cycle_info = `Process started with PID ${pid}. Monitoring via log files at ${stdoutFile}`;

    this.Debug(`Async execution initiated. MonitorExecution will track progress.`);
    return ElvOAction.EXECUTION_ONGOING;  // Signal that execution is ongoing, will check status next turn
  }

  /**
   * Monitor background process status for async PUBLISH mode
   */
  async MonitorExecution(pid, outputs) {
    this.Debug(`MonitorExecution called with PID: ${pid}`);

    // Retrieve logs from temp directory
    const contentId = this.Payload.inputs.content_id;
    if (!contentId) {
      throw new Error("MonitorExecution cannot find content_id in Payload inputs");
    }

    const logDir = path.join("/tmp", "elv_o_game_events");
    const stdoutFile = path.join(logDir, `${contentId}.stdout.log`);
    const stderrFile = path.join(logDir, `${contentId}.stderr.log`);

    if (!fs.existsSync(stdoutFile)) {
      // Logs don't exist yet - should not happen in normal operation
      this.Debug(`Log file not found: ${stdoutFile}`);
      return ElvOAction.EXECUTION_ONGOING;  // Continue monitoring
    }

    // Check if process is still running (ElvOAction helper from o-process.js)
    const pidNum = parseInt(pid, 10);
    const isRunning = ElvOAction.PidRunning(pidNum);

    if (!isRunning) {
      // Process has ended - check exit status and logs for completion reason
      this.Debug("Background process has stopped. Checking completion status...");

      const stdoutContent = fs.readFileSync(stdoutFile, "utf8");
      const stderrContent = fs.readFileSync(stderrFile, "utf8");

      outputs.operation = "PUBLISH";
      
      // Check if match ended successfully
      if (stdoutContent.includes("Match is over:")) {
        // Successful completion - process exited normally
        this.Debug("Process completed: match ended.");
        
        const lastCycleInfo = this.extractLastCycleInfo(stdoutContent);
        outputs.success = true;
        outputs.status = "completed";
        outputs.cycle_info = lastCycleInfo;

        return ElvOAction.EXECUTION_COMPLETE;  // Normal successful end
        
      } else if (this.isNetworkErrorFromStderr(stderrContent)) {
        // Network/transient error caused exit
        this.Debug("Process failed due to network error. Will retry.");
        
        outputs.success = false;
        outputs.status = "failed";
        outputs.error = `Network error detected: ${stderrContent.substring(0, 200)}`;

        return ElvOAction.EXECUTION_EXCEPTION;  // Retry for transient issues
        
      } else {
        // Unknown failure - likely bug or missing dependency
        outputs.success = false;
        outputs.status = "failed";
        outputs.error = `Process exited unexpectedly: ${stderrContent.substring(0, 500)}`;

        return ElvOAction.EXECUTION_FAILED;  // Not retryable
        
      }

    } else {
      // Process still running - report ongoing but extract progress info
      this.Debug(`Process still running (PID: ${pid}).`);

      const stdoutContent = fs.readFileSync(stdoutFile, "utf8");
      const lastCycleInfo = this.extractLastCycleInfo(stdoutContent);
      
      outputs.operation = "PUBLISH";
      outputs.cycle_info = lastCycleInfo;

      return ElvOAction.EXECUTION_ONGOING;  // Continue monitoring next poll
    }
  }

  /**
   * Helper: Resolve external_game_events path from parameter or env var
   */
  getExternalGameEventsPath(params) {
    return process.env.EXTERNAL_GAME_EVENTS_PATH || 
           params.external_game_events_path || 
           "/home/o/external_game_events";
  }

  /**
   * Helper: Check if file exists (with permission to read)
   */
  fileExists(filePath) {
    try {
      fs.accessSync(filePath, fs.constants.R_OK);
      return true;
    } catch (err) {
      return false;
    }
  }

  /**
   * Helper: Check if directory exists
   */
  directoryExists(dirPath) {
    try {
      const stats = fs.statSync(dirPath);
      return stats.isDirectory();
    } catch (err) {
      return false;
    }
  }

  /**
   * Helper: Spawn script synchronously with stderr/stdout capture
   */
  spawnScriptSync(scriptPath, args, timeoutSecs, customEnv = null, inheritStdio = false) {
    // Use externalGameEventsPath as cwd so relative paths in scripts resolve correctly
    // e.g., './state/...' → /home/o/external_game_events/state/ (not scripts/state/)
    const externalGameEventsPath = this.Payload.externalGameEventsPath || "/home/o/external_game_events";
    const options = {
      cwd: externalGameEventsPath,
      env: customEnv || process.env,
      encoding: "utf8",
      timeout: timeoutSecs * 1000
    };

    // Always use pipe for stdout/stderr so we can capture and parse output
    options.stdio = ["ignore", "pipe", "pipe"];

    this.Debug(`SPAWNSYNC DEBUG:`);
    this.Debug(`  command: node ${scriptPath}`);
    this.Debug(`  args: ${args.join(" ")}`);
    this.Debug(`  cwd: ${options.cwd}`);
    this.Debug(`  timeout: ${options.timeout}ms`);
    this.Debug(`  CONFIG_PATH: ${customEnv?.CONFIG_PATH || process.env.CONFIG_PATH}`);

    const result = spawnSync("node", [scriptPath, ...args], options);

    // Log results immediately for debugging
    if (result.error) {
      this.Error(`SPAWNSYNC ERROR: ${result.error.message}`);
      if (result.error.code) this.Error(`  error.code: ${result.error.code}`);
      if (result.error.syscall) this.Error(`  error.syscall: ${result.error.syscall}`);
      if (result.error.path) this.Error(`  error.path: ${result.error.path}`);
    } else {
      this.Debug(`SPAWNSYNC returned status: ${result.status}`);
      if (result.stdout && result.stdout.length > 0) {
        const preview = result.stdout.substring(0, 500).replace(/\n/g, "\\n");
        this.Debug(`stdout (preview): ${preview}`);
      }
      if (result.stderr && result.stderr.length > 0) {
        const preview = result.stderr.substring(0, 500).replace(/\n/g, "\\n");
        this.Error(`stderr (preview): ${preview}`);
      }
    }

    return result;
  }

  /**
   * Helper: Check if error is network-related (retryable)
   */
  isNetworkError(err) {
    const msg = String(err.message || "").toLowerCase();
    
    const networkPatterns = [
      /econnrefused|econnreset|etimedout|enotfound/,
      /network.?is.?unreachable/,
      /connection.?timed.?out/,
      /socket.?hang.?up/,
      /50[234]|bad.?gateway|service.?temporarily.?unavailable/
    ];

    return networkPatterns.some(pattern => pattern.test(msg));
  }

  /**
   * Helper: Check if error is in stderr output (for spawnSync results)
   */
  isNetworkErrorFromStderr(stderr) {
    return this.isNetworkError({ message: stderr || "" });
  }

  /**
   * Helper: Extract meaningful progress info from script logs
   */
  extractLastCycleInfo(logContent) {
    const lines = logContent.split("\n");
    
    // Look for cycle completion messages
    for (let i = lines.length - 1; i >= 0; i--) {
      const line = lines[i].trim();
      
      if (/cycle completed successfully/i.test(line)) {
        return "Latest cycle completed successfully";
      } else if (/\[\d+\] games? found/i.test(line)) {
        // Try to parse game count from log
        const match = line.match(/(\d+)\s+games?\s*(?:found|returned)?/i);
        if (match) {
          return `${match[1]} games processed`;
        }
      } else if (/error|\bfailed\b/i.test(line)) {
        // Return last error message excerpt
        return `Last activity: ${line.substring(0, 150)}...`;
      }
    }

    return "Processing in progress";
  }
}

// Export and handle command-line execution if this file is invoked directly
if (ElvOAction.executeCommandLine(ElvOActionExternalGameEvents)) {
  ElvOAction.Run(ElvOActionExternalGameEvents);
} else {
  module.exports = ElvOActionExternalGameEvents;
}
