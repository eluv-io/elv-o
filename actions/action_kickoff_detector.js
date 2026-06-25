const ElvOAction = require("../o-action").ElvOAction;
const ElvOFabricClient = require("../o-fabric");
const fs = require("fs");
const { setGlobalDispatcher, Agent } = require("undici");

setGlobalDispatcher(
  new Agent({
    headersTimeout: 1200000,
    bodyTimeout: 0
  })
);

/**
 * Kickoff Detector Action
 * This action detects the kickoff point in a video by calling an external Python service.
 * The external service runs in a container, details here: https://github.com/eluv-io/elv-andrea/tree/main/kickoff_detector
 * 
 * It supports multiple config input methods and handles token generation for secure access.
 * 
 * Parameters:
 * - content_id (string): The ID of the object to analyze (required)
 * - config_path (string): Optional path to a config file inside the container
 * - config (object): Optional config JSON structure provided directly  
 * 
 * Here is an example of config structure:
 * {
 *  "roi": [846, 923, 1072, 970],
 * 
 *  "priority_windows": [
 *    [300, 600],
 *    [1500, 2400],
 *    [4500, 5400]
 *  ],
 * 
 *  "fallback_windows": [
 *    [1200, 2400],
 *    [4200, 5400]
 *  ],
 * 
 *  "offset_in_match": 20,
 *  "sampling_distance": 60,
 * 
 *  "ocr_threshold": 54,
 *  "easyocr_langs": ["en"],
 *  "ocr_preprocess_scale": 1.3,
 * 
 *  "fps_override": 50,
 *  "logging_level": "INFO",
 * 
 *  "max_validation_attempts": 6,
 *  "validation_tolerance": 2,
 *  "nan_shift_seconds": 60,
 * 
 *  "fine_scan_max_jump_seconds": 2,
 *  "fine_scan_probe_offset_seconds": 1,
 * 
 *  "time_regex": "\\b(\\d{2}):(\\d{2})\\b"
 * }
 */
class ElvOActionKickoffDetector extends ElvOAction {

  ActionId() {
    return "kickoff_detector";
  }

  // ------------------------------------------------------------
  // PARAMETERS
  // ------------------------------------------------------------
  Parameters() {
    return {
      parameters: {
        content_id: { type: "string", required: false },
        config_url: { type: "string", required: false },
        // Optional: path to config file inside container
        config_path: { type: "string", required: false },

        // Optional: config JSON structure provided directly
        config: { type: "object", required: false }
      }
    };
  }

  IdleTimeout() {
    return 1200; // 20 minutes
  };

  // ------------------------------------------------------------
  // INPUTS / OUTPUTS
  // ------------------------------------------------------------
  IOs(parameters) {
    return {
      inputs: {
        // Object ID to analyze (can also be provided as a parameter)
        content_id: { type: "string", required: false },
        // Content Fabric config URL (if different from the one in the client)
        config_url: { type: "string", required: false },
        // Optional config runtime override
        config: { type: "object", required: false },
        // Optional: runtime path to config file
        config_path: { type: "string", required: false },
        // PRIVATE_KEY it's not recommended to provide private keys, but in case it's needed for token generation, it can be provided as an input
        private_key: {type: "password", required: false},
      },
      outputs: {
        success: { type: "boolean" },
        kickoff_timecode: { type: "number", required: false },
        kickoff_seconds: { type: "number", required: false },
        error: { type: "string", required: false }
      }
    };
  }

  // ------------------------------------------------------------
  // CONFIG RESOLUTION LOGIC
  // ------------------------------------------------------------
  resolveConfig() {
    const params = this.Payload.parameters;
    const inputs = this.Payload.inputs;

    // Priority 1 — runtime input
    if (inputs.config) {
      return JSON.stringify(inputs.config);
    }

    // Priority 2 — config provided in parameters
    if (params.config) {
      return JSON.stringify(params.config);
    }

    // Priority 3 — config_path provided in runtime inputs
    if (inputs.config_path) {
      const fileContent = fs.readFileSync(inputs.config_path, "utf8");
      return JSON.stringify(JSON.parse(fileContent));
    }

    // Priority 4 — config_path provided
    if (params.config_path) {
      const fileContent = fs.readFileSync(params.config_path, "utf8");
      return JSON.stringify(JSON.parse(fileContent));
    }

    // Nothing provided
    throw new Error("No config or config_path provided");
  }

  // ------------------------------------------------------------
  // PYTHON SERVICE CALL
  // ------------------------------------------------------------
  async detectKickoff(url, config) {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 1200000);

    try {
      const response = await fetch("http://localhost:8000/detect_kickoff", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ url, config }),
        signal: controller.signal
      });

      clearTimeout(timeout);
      return await response.json();

    } catch (err) {
      clearTimeout(timeout);
      throw err;
    }
  }

  // ------------------------------------------------------------
  // TOKEN GENERATION
  // ------------------------------------------------------------
  async generateEditorToken(client, content_id, subject) {
    const wallet = client.GenerateWallet();
    let privateKey;
    if (process.env.PRIVATE_KEY && process.env.PRIVATE_KEY !== "undefined") {
      privateKey = process.env.PRIVATE_KEY;
    } else if (this.Payload.inputs.private_key) {
      privateKey = this.Payload.inputs.private_key;
    } else {
      throw new Error("No private key provided for token generation");
    } 
    
    const signer = wallet.AddAccount({
      privateKey: privateKey
    });

    client.SetSigner({ signer });

    this.Debug("Generating signed token for content_id:", content_id, "with subject:", subject);    
    var tok = await client.CreateSignedToken({
      objectId: content_id,
      subject,
      duration: 10 * 60 * 1000   // 1 month
      //policyId,
      //context,
    });

    return tok;
  }

  // ------------------------------------------------------------
  // MAIN EXECUTION
  // ------------------------------------------------------------
  async Execute(inputs, outputs) {
    const content_id = this.Payload.parameters.content_id != null ? this.Payload.parameters.content_id : inputs.content_id;
    this.Debug(`Starting kickoff detection for content_id: ${content_id}`);
    let client;
    if (!inputs.private_key && !inputs.config_url) {
      client = this.Client;
    } else {
      let privateKey = inputs.private_key || this.getPrivateKey();
      let configUrl = inputs.config_url || this.Client.configUrl;
      client = await ElvOFabricClient.InitializeClient(configUrl, privateKey)
    }

    if (!content_id) {
      outputs.success = false;
      outputs.error = "Missing content_id";
      return ElvOAction.EXECUTION_FAILED;
    }

    // 1. Resolve config
    let config;
    try {
      config = this.resolveConfig();
      this.ReportProgress("Config loaded");
    } catch (err) {
      outputs.success = false;
      outputs.error = err.message;
      return ElvOAction.EXECUTION_FAILED;
    }

    try {
      // 2. Generate signed token
      this.ReportProgress("Generating signed token");
      const token = await this.generateEditorToken(client, content_id, "wsc");

      // 3. Build URLs
      const base = `https://main.net955305.contentfabric.io/t/${token}/q/${content_id}/rep/playout/default`;
      const urlAES = `${base}/hls-aes128/playlist.m3u8?ignore_trimming=false`;
      const urlClear = `${base}/hls-clear/playlist.m3u8?ignore_trimming=false`;

      // 4. Fetch entry_point_rat
      let libraryId = await this.getLibraryId(content_id, client);
      
      let entryPointRatSeconds = 0;
      let frame_rate = 50; // Default frame rate
      let entry_point_rat = null;
      try {
        const metadata_struct = await client.ContentObjectMetadata({
          objectId: content_id,
          metadataSubtree: "offerings/default",
          libraryId: libraryId
        });
        entry_point_rat = metadata_struct['entry_point_rat'];
        this.Debug("Metadata retrieved:", entry_point_rat);
      
        frame_rate = parseInt(metadata_struct["media_struct"]["streams"]["video"]["rate"])  || 50; // Default to 50 if not available

        if (entry_point_rat) {
          // metadata is a string like "79545/50" — evaluate as a rational number
          const parts = entry_point_rat.split("/");
          if (parts.length === 2) {
            const numerator = parseFloat(parts[0]);
            const denominator = parseFloat(parts[1]);
            if (!isNaN(numerator) && !isNaN(denominator) && denominator !== 0) {
              entryPointRatSeconds = numerator / denominator;
              this.Debug(`entry_point_rat parsed: ${numerator}/${denominator} = ${entryPointRatSeconds}s`);
            } else {
              this.Error("Invalid entry_point_rat format (bad numbers):", entry_point_rat);
            }
          } else {
            this.Error("Unexpected entry_point_rat format (expected 'N/D'):", entry_point_rat);
          }
        }
      } catch (err) {
        this.Error("Failed to retrieve metadata:", err);
      }


      // 5. Try AES128 first
      this.ReportProgress("Calling Python kickoff detector (AES128)");
      let result = await this.detectKickoff(urlAES, config);

      if (!result.success) {
        this.ReportProgress("AES128 failed, trying CLEAR");
        result = await this.detectKickoff(urlClear, config);
      }

      if (!result.success) {
        outputs.success = false;
        outputs.error = result.error;
        this.Error("Kickoff detection failed", result.error);
        return ElvOAction.EXECUTION_FAILED;
      }

      // 6. Adjust kickoff_seconds
      const adjustedKickoffSeconds = result.kickoff_seconds + entryPointRatSeconds;

      // Adjust kickoff_timecode: convert seconds offset to HH:MM:SS:FF timecode and add
      // Assumes kickoff_timecode is "HH:MM:SS:FF" and entry_point_rat is in seconds
      const adjustedKickoffTimecode = this.addSecondsToTimecode(result.kickoff_timecode, entryPointRatSeconds,frame_rate);



      // 7. Success
      outputs.success = true;
      outputs.kickoff_timecode = adjustedKickoffTimecode;
      outputs.kickoff_seconds = adjustedKickoffSeconds;

      this.ReportProgress(
        `Kickoff detected at ${adjustedKickoffTimecode} (${adjustedKickoffSeconds}s) [offset by ${entryPointRatSeconds}s]`
      );

      return ElvOAction.EXECUTION_COMPLETE;

    } catch (err) {
      outputs.success = false;
      outputs.error = err.message || String(err);
      this.Error("Kickoff detection exception", err);
      return ElvOAction.EXECUTION_EXCEPTION;
    }
  }

  addSecondsToTimecode(timecode, secondsToAdd, frameRate = 25) {
    // Parse "HH:MM:SS:FF"
    const parts = timecode.split(":");
    if (parts.length !== 4) throw new Error(`Unexpected timecode format: ${timecode}`);
  
    const [hh, mm, ss, ff] = parts.map(Number);
  
    // Convert timecode to total frames
    const totalFrames =
      ((hh * 3600 + mm * 60 + ss) * frameRate + ff) +
      Math.round(secondsToAdd * frameRate);
  
    // Convert back
    const frames = totalFrames % frameRate;
    const totalSeconds = Math.floor(totalFrames / frameRate);
    const seconds = totalSeconds % 60;
    const totalMinutes = Math.floor(totalSeconds / 60);
    const minutes = totalMinutes % 60;
    const hours = Math.floor(totalMinutes / 60);
  
    const pad = (n, len = 2) => String(n).padStart(len, "0");
    return `${pad(hours)}:${pad(minutes)}:${pad(seconds)}:${pad(frames)}`;
  }

  static VERSION = "0.0.3";
  static REVISION_HISTORY = {
    "0.0.1": "Initial kickoff detector action",
    "0.0.2": "Added config + config_path resolution logic",
    "0.0.3": "Increased idle timeout to 20 minutes"
  };
}

if (ElvOAction.executeCommandLine(ElvOActionKickoffDetector)) {
  ElvOAction.Run(ElvOActionKickoffDetector);
} else {
  module.exports = ElvOActionKickoffDetector;
}

