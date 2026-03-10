const { ElvOAction } = require("./ElvOAction");
const { ElvOFabricClient } = require("./ElvOFabricClient");

class ElvOActionHandleWriteToken extends ElvOAction {

  static VERSION = "0.0.1";
  static REVISION_HISTORY = {
    "0.0.1": "Initial release"
  };

  ActionId() {
    return "handle_write_token";
  }

  Parameters() {
    return {
      parameters: {
        action: {
          type: "string",
          required: true,
          values: ["GET_WRITE_TOKEN", "FINALIZE_CONTENT_OBJECT"]
        },
        commit_message: { type: "string", required: false },
        max_attempts: { type: "number", required: false },
        timeout: { type: "number", required: false }
      }
    };
  }

  IOs(parameters) {
    const action = parameters.action;

    let inputs = {
      content_id: { type: "string", required: false },
      content_hash: { type: "string", required: false },
      library_id: { type: "string", required: false },
      private_key: { type: "password", required: false },
      config_url: { type: "string", required: false }
    };

    let outputs = {
      object_id: { type: "string" },
      library_id: { type: "string" }
    };

    if (action === "GET_WRITE_TOKEN") {
      inputs.force_update = { type: "boolean", required: false };
      outputs.write_token = { type: "string" };
      outputs.node_url = { type: "string" };
    }

    if (action === "FINALIZE_CONTENT_OBJECT") {
      inputs.write_token = { type: "string", required: true };
      // Apparently we don't need to specify the node url when finalizing the object
      // inputs.node_url = { type: "string", required: false };
      inputs.commit_message = { type: "string", required: false },
      outputs.modified_object_version_hash = { type: "string" };
    }

    return { inputs, outputs };
  }

  // ============================================================
  // EXECUTE DISPATCHER — canonical Elv‑O pattern
  // ============================================================
  async Execute(inputs, outputs) {
    const action = this.Payload.parameters.action;

    const client = await this.initializeClient(inputs);

    // Resolve objectId
    let objectId = this.resolveObjectId(inputs, client);
    if (!objectId) {
      outputs.error = "Missing content_id or content_hash";
      return ElvOAction.EXECUTION_FAILED;
    }

    // Resolve libraryId
    let libraryId = inputs.library_id;
    if (!libraryId) {
      libraryId = await this.getLibraryId(objectId, client);
      inputs.library_id = libraryId;
    }

    outputs.object_id = objectId;
    outputs.library_id = libraryId;


    if (action === "GET_WRITE_TOKEN") {
      return await this.executeGetWriteToken(client, libraryId, objectId, inputs, outputs);
    }

    if (action === "FINALIZE_CONTENT_OBJECT") {
      return await this.executeFinalizeContentObject(client, libraryId, objectId, inputs, outputs);
    }

    this.ReportProgress("Unknown command " + action);
    outputs.error = "Unknown action: " + action;
    return ElvOAction.EXECUTION_EXCEPTION;
  }

  // ============================================================
  // INTERNAL HELPERS
  // ============================================================

  async initializeClient(inputs) {
    if (!inputs.private_key && !inputs.config_url) {
      return this.Client;
    }

    return await ElvOFabricClient.InitializeClient(
      inputs.config_url || this.Client.configUrl,
      inputs.private_key || this.getPrivateKey()
    );
  }

  resolveObjectId(inputs, client) {
    if (inputs.content_hash) {
      return client.utils.DecodeVersionHash(inputs.content_hash).objectId;
    }
    if (inputs.content_id) {
      return inputs.content_id;
    }
    return null;
  }

  // ============================================================
  // GET_WRITE_TOKEN
  // ============================================================
  async executeGetWriteToken(client, libraryId, objectId, inputs, outputs) {
    try {

      // Build edit params
      let editParams = {
        libraryId,
        objectId,
        client,
        force: inputs.force_update
      };

      // Request write token
      const response = await client.getWriteTokenSpecs(editParams, 600000);
      outputs.write_token = response.write_token;
      outputs.node_url = response.nodeUrl;

      return ElvOAction.EXECUTION_COMPLETE;

    } catch (err) {
      outputs.error = err.message || String(err);
      return ElvOAction.EXECUTION_FAILED;
    }
  }

  // ============================================================
  // FINALIZE_CONTENT_OBJECT
  // ============================================================
  async executeFinalizeContentObject(client, libraryId, objectId, inputs, outputs) {
    try {

      if (!inputs.write_token) {
        outputs.error = "Missing write_token for FINALIZE_CONTENT_OBJECT";
        return ElvOAction.EXECUTION_FAILED;
      }

      const commit_message = inputs.commit_message || this.Payload.parameters.commit_message;

      const finalizeParams = {
        libraryId,
        objectId,
        writeToken: inputs.write_token,
        commitMessage: commit_message,
        maxAttempts: this.Payload.parameters.max_attempts || 10,
        timeout: this.Payload.parameters.timeout,
        client
      };

      const response = await this.FinalizeContentObject(finalizeParams);
      outputs.modified_object_version_hash = response.hash;

      return ElvOAction.EXECUTION_COMPLETE;

    } catch (err) {
      outputs.error = err.message || String(err);
      return ElvOAction.EXECUTION_FAILED;
    }
  }
}

if (ElvOAction.executeCommandLine(ElvOActionHandleWriteToken)) {
  ElvOAction.Run(ElvOActionHandleWriteToken);
} else {
  module.exports = ElvOActionHandleWriteToken;
}
