const ElvOAction = require("../o-action").ElvOAction
const ElvOFabricClient = require("../o-fabric")
const { execSync } = require('child_process')
const fs = require("fs")
const path = require("path")
const { isArray } = require("util")

const STORE_PLAY_DATA_FOLDER = "/home/o/elv-o/cricket_play_data"

class ElvOActionCricketAustraliaVariants extends ElvOAction {

  ActionId() {
    return "cricket_australia_variants";
  };

  Parameters() {
    return {
      parameters: {
        action: {
          type: "string", required: true,
          values: ["STORE_PLAY_DATA", "CONVERT_PLAY_DATA", "CACHE_ARCHIVE_VARIANT"]
        }
      }
    };
  };

  IOs(parameters) {
    let inputs = {
      private_key: { type: "password", required: false },
      config_url: { type: "string", required: false },
    };
    let outputs = {};
    if (parameters.action == "STORE_PLAY_DATA") {
      // This is used to expose a webhook to receive play data
      inputs.web_hooks = { type: "object", required: true };
      inputs.store_play_data_folder = { type: "string", required: false, default: STORE_PLAY_DATA_FOLDER };
      outputs.event_file_path = "string";
      outputs.transalted_event_file_path = "string";
    }
    if (parameters.action == "CONVERT_PLAY_DATA") {
      // This is used to convert a stored play data file into eluvio tags
      inputs.event_file_path = { type: "string", required: true };
      inputs.store_play_data_folder = { type: "string", required: false, default: STORE_PLAY_DATA_FOLDER };
      outputs.event_file_path = "string";
      outputs.transalted_event_file_paths = "object";
    }

    if (parameters.action == "CACHE_ARCHIVE_VARIANT") {
      inputs.production_master_object_id = { type: "string", required: true };
      inputs.write_token = { type: "string", required: true };
      inputs.config_url = { type: "string", required: false };
      inputs.private_key = { type: "password", required: false };
      outputs.variant = "object";
    }
    return { inputs, outputs };
  };

  async Execute(inputs, outputs) {
    if (this.Payload.parameters.action == "CACHE_ARCHIVE_VARIANT") {
      return await this.executeCacheArchiveVariant({ inputs, outputs })
    }
    if (this.Payload.parameters.action == "STORE_PLAY_DATA") {
      return await this.executeStorePlayData({ inputs, outputs })
    }
    if (this.Payload.parameters.action == "CONVERT_PLAY_DATA") {
      return await this.executeConvertPlayData({ inputs, outputs })
    }
    throw Error("Action not supported: " + this.Payload.parameters.action);
  };


  static CHALLENGE_LOOKUP = {
    CoupeRegionale: "crl"
  };

  async executeStorePlayData({ inputs, outputs }) {
    let payload = inputs.web_hooks || inputs;
    const eventFilePath = path.join(inputs.store_play_data_folder, "event-" + payload.event?.providerMatchId + ".json")
    this.reportProgress("event file path", eventFilePath);
    let events;
    if (fs.existsSync(eventFilePath)) {
      this.reportProgress("adding event to ", eventFilePath);
      events = JSON.parse(fs.readFileSync(eventFilePath));
      if (!JSON.stringify(events).includes(JSON.stringify(payload))) {      
        events.push(payload); 
      }
    } else {
      this.reportProgress("new event at ", eventFilePath);
      events = [payload];
    }
    fs.writeFileSync(eventFilePath, JSON.stringify(events, null, 2));
    this.reportProgress("Saving to ", eventFilePath);
    outputs.event_file_path = eventFilePath;

    if (payload && payload.event?.type != "scoreUpdate") {
      outputs.transalted_event_file_path = this.processCricketPayload(payload, inputs.store_play_data_folder)
    }

    return ElvOAction.EXECUTION_COMPLETE;
  }

  async executeConvertPlayData({ inputs, outputs }) {
    const eventFilePath = inputs.event_file_path
    this.reportProgress("event file path", eventFilePath);
    let event_file_content = require(eventFilePath)
    const payload = event_file_content.web_hooks || event_file_content;
    outputs.event_file_path = eventFilePath;
    outputs.transalted_event_file_path = this.convertCricketPayload(payload, inputs.store_play_data_folder)
    this.reportProgress("Converted to ", outputs.transalted_event_file_path);
    return ElvOAction.EXECUTION_COMPLETE;
  }

  async executeCacheArchiveVariant({ inputs, outputs }) {
    this.reportProgress("executing executeCacheArchiveVariant");
    let client = await this.initializeActionClient();
    let libraryId = await this.getLibraryId(inputs.production_master_object_id, client);
    let sources = await this.getMetadata({
      client, objectId: inputs.production_master_object_id, libraryId,
      writeToken: inputs.write_token, metadataSubtree: "production_master/sources"
    })
    this.reportProgress("Retrieved sources from Write-token", sources);
    let variant = {
      streams: {
        video: null,
        audio: null
      }
    }
    for (let filename in sources) {
      this.reportProgress("Scanning source ", filename);
      let streams = sources[filename].streams;
      let streamIndex = 0;
      for (let stream of streams) {
        if (stream.type == "StreamVideo") {
          if (variant.streams.video) {
            this.reportProgress("Warning a supplemental video was found in ", { filename, streamIndex });
          } else {
            variant.streams.video = {
              default_for_media_type: true, label: "", language: "en", mapping_info: "",
              sources: [{ files_api_path: filename, stream_index: streamIndex }]
            };
          }
        }
        if (stream.type == "StreamAudio") {
          if (stream.channels == 2) {
            if (variant.streams.audio) {
              this.reportProgress("Warning a supplemental stereo was found in ", { filename, streamIndex });
            } else {
              variant.streams.audio = {
                default_for_media_type: true, label: "English(Stereo)", language: "en", mapping_info: "",
                sources: [{ files_api_path: filename, stream_index: streamIndex }]
              };
            }
          }
          if (stream.channels == 1) {
            if (variant.streams.audio) {
              if (variant.streams.audio.label == "English(Stereo)") {
                this.reportProgress("Warning a supplemental stereo was found in ", { filename, streamIndex });
              } else {
                variant.streams.audio.label = "English(Stereo)";
                variant.streams.audio.mapping_info = "2MONO_1STEREO";
                variant.streams.sources.push({ files_api_path: filename, stream_index: streamIndex })
              }
            } else {
              variant.streams.stereo = {
                default_for_media_type: true, label: "English(Mono)", language: "en", mapping_info: "",
                sources: [{ files_api_path: filename, stream_index: streamIndex }]
              };
            }
          }
          if (stream.channels > 2) {
            this.reportProgress("A complex audio was found in ", { filename, streamIndex, audioChannels: stream.channels });
          }
        }
        streamIndex++;
      }
      outputs.variant = variant;
      await client.ReplaceMetadata({
        client,
        objectId: inputs.production_master_object_id,
        libraryId,
        writeToken: inputs.write_token,
        metadata: variant,
        metadataSubtree: "production_master/variants/default"
      });
      return ElvOAction.EXECUTION_COMPLETE;
    }
  };


  /**
   * Andrea - this is not used yet, we need to find a way to retrieve the start time from either a file
   * or the live object
   * 
   */
  load_video_start_time_from_file(filename) {
    if (fs.existsSync(filename)) {
      try {
        const fileContent = fs.readFileSync(filename, 'utf-8')
        const parsed = JSON.parse(fileContent)
        return parsed.video_start_absolute_timestamp
      } catch (err) {
        console.error(`Failed to parse JSON from ${filename}:`, err.message)
        return null
      }
    }
    return null
  }

  /**
 * Extract the providerMatchId from a cricket payload.
 * Throws if invalid.
 */
  extractProviderMatchId(payload) {
    if (payload?.type === 'cricket' && typeof payload?.event?.providerMatchId === 'string') {
      return payload.event.providerMatchId;
    }
    if (payload && Array.isArray(payload)) {
      for (const item of payload) {
        if (item?.type === 'cricket' && typeof item?.event?.providerMatchId === 'string') {
          return item.event.providerMatchId;
        }
      }
    }
    throw new Error('Invalid or missing providerMatchId in the payload');
  }

  /**
   * There are only 4 innings in cricket.
   */
  stringifyInning(inningsNumber, capitalize = false) {
    const arr = capitalize
      ? ['First', 'Second', 'Third', 'Fourth']
      : ['first', 'second', 'third', 'fourth'];

    return arr[inningsNumber - 1] || (capitalize ? 'Unknown' : 'unknown');
  }

  /**
   * Compute output file name for a given match and innings.
   */
  getOutputFileName(baseFolder, providerMatchId, inningsNumber) {
    return path.join(
      baseFolder,
      `translated-${providerMatchId}-${this.stringifyInning(inningsNumber)}-inning.json`
    );
  }

  /**
   * Load existing tags from file if it exists.
   * Returns an array of tags.
   */
  loadTagsFromFile(filename) {
    const existingTags = [];

    if (!fs.existsSync(filename)) {
      return existingTags;
    }

    try {
      const fileContent = fs.readFileSync(filename, 'utf-8');
      const parsed = JSON.parse(fileContent);

      if (
        parsed &&
        parsed.metadata_tags &&
        typeof parsed.metadata_tags === 'object'
      ) {
        const inningsKey = Object.keys(parsed.metadata_tags)[0];
        const inningsObj = parsed.metadata_tags[inningsKey];

        if (inningsObj && Array.isArray(inningsObj.tags)) {
          existingTags.push(...inningsObj.tags);
        }
      } else {
        throw new Error(`Invalid prologue format in file: ${filename}`);
      }
    } catch (err) {
      throw new Error(
        `Failed to load or parse file "${filename}": ${err.message}`
      );
    }

    return existingTags;
  }

  /**
   * Save tags to file, wrapped in the metadata format.
   */
  saveTagsToFile(filename, inningsNumber, tags) {
    const inningsKey = `game_events_all__${this.stringifyInning(
      inningsNumber
    )}_innings`;

    const outputJson = {
      version: 1,
      video_level_tags: {},
      metadata_tags: {
        [inningsKey]: {
          label: `Event - ALL: ${this.stringifyInning(
            inningsNumber,
            true
          )} Innings`,
          tags,
        },
      },
    };

    try {
      fs.writeFileSync(filename, JSON.stringify(outputJson, null, 2), 'utf-8');
    } catch (err) {
      console.error(`Failed to write to ${filename}:`, err.message);
    }
  }

  /**
   * Transform a single cricket event into your tag format and append to file.
   * - No relative timestamps.
   * - Dedup based on full JSON.
   *
   * Returns the transformed object, or null if it was a duplicate.
   */
  transformCricketEvent(event, providerMatchId, baseFolder = __dirname) {
    
    const transformed = this.executeConvertPlayDataEvent(event);

    if (transformed === null) {
      // skip (e.g. scoreUpdate)
      return null;
    }

    const outputFileName = this.getOutputFileName(
      baseFolder,
      providerMatchId,
      event.inningsNumber
    );

    const existingTags = this.loadTagsFromFile(outputFileName);

    const newEventStr = JSON.stringify(transformed);
    const existingStrings = existingTags.map((e) => JSON.stringify(e));

    if (existingStrings.includes(newEventStr)) {
      // duplicate – do nothing
      return outputFileName;
    }

    const updatedTags = [...existingTags, transformed];
    this.saveTagsToFile(outputFileName, event.inningsNumber, updatedTags);

    return outputFileName;
  }

  /**
   * Transform a list of cricket events into eluvio tag format and save them to one file per innning.
   * - No relative timestamps.
   * - Dedup based on full JSON.
   *
   * Returns the list of output file paths.
   */
  transformCricketEvents(events, providerMatchId, baseFolder = __dirname) {
    const transformedTags = {};

    for (let event of events) {
      if (event.type == "cricket") {
        event = event.event;
      }
      const transformed = this.executeConvertPlayDataEvent(event);
      if (transformed !== null) {
        const inningsNumber = event.inningsNumber;
        if (!transformedTags[inningsNumber]) {
          transformedTags[inningsNumber] = [];
        }
        if (!transformedTags[inningsNumber].some(tag => JSON.stringify(tag) === JSON.stringify(transformed))) {
          transformedTags[inningsNumber].push(transformed);
        }
      }        
    }


    let outputFileNames = [];

    for (let inningsNumber in transformedTags) {
      const outputFileName = this.getOutputFileName(
        baseFolder,
        providerMatchId,
        inningsNumber
      );
      this.saveTagsToFile(outputFileName, inningsNumber, transformedTags[inningsNumber]);
      outputFileNames.push(outputFileName);
    }
    

    return outputFileNames;
  }

  /**
   * Generates an eluvio formatted event tag from a single cricket event
   * 
   */
  executeConvertPlayDataEvent(event) {

    if (event.type == "scoreUpdate") {
      // skip score updates
      return null;
    }

    const {
      type,
      ballTimestamp = null,
      inningsNumber,
      overNumber,
      ballNumber,
      battingTeamName = 'Unknown Team',
      bowlingTeamName = 'Unknown Team',
    } = event;

    let description = '';
    let eventType = '';

    const eventId =
      String(event.providerMatchId || providerMatchId) +
      String(event.ballTimestamp || '');

    switch (type) {
      case 'appeal':
        description = `Appeal - ${event.appealType} appeal by [${event.fielderName || 'Unknown'}] <${bowlingTeamName}>`;
        eventType = 'Appeal';
        break;

      case 'halfCentury':
        description = `Half-century - [${event.batterName || 'Batter'}] <${battingTeamName}> reaches ${event.batterRunsTotal} runs.`;
        eventType = 'Half-century';
        break;

      case 'century':
        description = `Century -  [${event.batterName || 'Batter'}] <${battingTeamName}> reaches ${event.batterRunsTotal} runs.`;
        eventType = 'Century';
        break;

      case 'dismissal':
        description = `Dismissal - ${event.dismissalType} by [${event.fielderName || 'Unknown'}] <${bowlingTeamName}>`;
        eventType = 'Dismissal';
        break;

      case 'droppedCatch':
        description = `Dropped-Catch - [${event.fielderName || 'Unknown'}] <${bowlingTeamName}> at ${event.fieldPosition || 'Unknown Position'}`;
        eventType = 'Dropped-Catch';
        break;

      case 'boundary':
        description = `Boundary - [${event.batterName || 'Batter'}] <${battingTeamName}> hits a ${event.batterRunsScored} with a ${event.shotType || 'Unknown'} shot in ${event.fieldPosition || 'Unknown Position'}`;
        eventType = 'Boundary';
        break;

      case 'firstBallInnings':
        description = `First-Ball Inning ${event.inningsNumber}`;
        eventType = 'First-Ball Inning';
        break;

      default:
        description = `Unknown event type: ${type}`;
        eventType = 'Unknown';
    }

    const transformed = {
      // keep absolute timestamps only, as requested
      absolute_start_timestamp: ballTimestamp,
      absolute_end_timestamp: ballTimestamp,
      text: [
        `${eventType}: ${description}`,
        `I:${inningsNumber || '-'} O:${overNumber || '-'} Ball_Number:${ballNumber || '-'}`,
        `id: ${eventId}`,
      ],
    };

    return transformed;
  }

  /**
   * Process a full payload in one go.
   *  - Extract providerMatchId
   *  - Transform & append event
   */
  processCricketPayload(payload, baseFolder = __dirname) {
    const providerMatchId = this.extractProviderMatchId(payload);
    const event = payload.event;
    return this.transformCricketEvent(event, providerMatchId, baseFolder);
  }

  /**
   * Converts a cricket payload file to the eluvio metadata format and returns the list of output file paths.
   */
  convertCricketPayload(payload, baseFolder = __dirname) {
    // payload is expected to be an array of events
    const providerMatchId = this.extractProviderMatchId(payload[0]);
    return this.transformCricketEvents(payload, providerMatchId, baseFolder);
  }

  static REVISION_HISTORY = {
    "0.0.1": "ADM - Initial release - simplified from URC",
    "0.0.2": "Add actions for ingest from archive",
    "0.0.2a": "restore event posting code from ad-hoc version",
    "0.0.3": "Simplified event translation code"

  }

  static VERSION = "0.0.3"
}

if (ElvOAction.executeCommandLine(ElvOActionCricketAustraliaVariants)) {
  ElvOAction.Run(ElvOActionCricketAustraliaVariants)
} else {
  module.exports = ElvOActionCricketAustraliaVariants
}
