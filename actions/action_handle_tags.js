const ElvOAction = require("../o-action").ElvOAction;
const ElvOFabricClient = require("../o-fabric");
const https = require("https");
const querystring = require("querystring");

class ElvOActionHandleTags extends ElvOAction {

  static VERSION = "0.0.1";
  static REVISION_HISTORY = {
    "0.0.1": "Initial release"
  };

  ActionId() {
    return "handle_tags";
  }

  Parameters() {
    return {
      parameters: {
        action: {
          type: "string",
          required: true,
          values: ["CREATE", "ADD", "QUERY", "WRITE_TO_FABRIC", "LIST_ALL_TRACKS"]
        },
        timeout_ms: {
          type: "number",
          required: false,
          default: 10000
        }
      }
    };
  }

  IOs(parameters) {
    const action = parameters.action;

    const inputs = {
      content_id: { type: "string", required: true },
      auth_token: { type: "string", required: false },
      private_key: { type: "password", required: false },
      config_url: { type: "string", required: false }
    };

    const outputs = {};

    if (action === "CREATE") {
      inputs.track = { type: "string", required: true };
      inputs.label = { type: "string", required: false };
      inputs.color = { type: "string", required: false };
      inputs.description = { type: "string", required: false };

      outputs.message = { type: "string" };
      outputs.track_id = { type: "string" };
      outputs.track = { type: "string" };
    }

    if (action === "ADD") {
      inputs.author = { type: "string", required: true };
      inputs.track = { type: "string", required: true };
      inputs.tags = { type: "array", required: true };

      outputs.batch_id = { type: "string" };
    }

    if (action === "QUERY") {
      inputs.track = { type: "string", required: false };
      inputs.author = { type: "string", required: false };
      inputs.start_time_gte = { type: "number", required: false };
      inputs.end_time_lte = { type: "number", required: false };
      inputs.limit = { type: "number", required: false };
      inputs.start = { type: "number", required: false };

      outputs.tags = { type: "array" };
      outputs.meta = { type: "object" };
    }

    if (action === "WRITE_TO_FABRIC") {
      inputs.write_token = { type: "string", required: true };
      outputs.status = { type: "string" };
    }

    if (action === "LIST_ALL_TRACKS") {
      outputs.tracks = { type: "array" };
    }

    return { inputs, outputs };
  }

  async Execute(inputs, outputs) {
    const action = this.Payload.parameters.action;

    if (action === "CREATE") {
      return await this.executeCreateTrack(inputs, outputs);
    }

    if (action === "ADD") {
      return await this.executeAddTags(inputs, outputs);
    }

    if (action === "QUERY") {
      return await this.executeQueryTags(inputs, outputs);
    }

    if (action === "WRITE_TO_FABRIC") {
      return await this.executeWriteToFabric(inputs, outputs);
    }

    if (action === "LIST_ALL_TRACKS") {
      return await this.executeListAllTracks(inputs, outputs);
    }

    this.ReportProgress("Unknown command " + action);
    outputs.error = "Unknown action: " + action;
    return ElvOAction.EXECUTION_EXCEPTION;
  }

  async getAuthToken(inputs, qid) {
    if (inputs.auth_token) {
      this.Info("Using provided auth token");
        return inputs.auth_token;
    }

    this.Info("Generating backend auth token", { qid });

    let client;
    if (!inputs.private_key && !inputs.config_url) {
      client = this.Client;
    } else {
      client = await ElvOFabricClient.InitializeClient(
        inputs.config_url || this.Client.configUrl,
        inputs.private_key || this.getPrivateKey()
      );
    }

    if (client.CreateSignedToken) {
      const tok = await client.CreateSignedToken({
        objectId: qid,
        subject: "play-by-play-tagger",
        duration: 24 * 60 * 60 * 1000,
        grantType: "create"
      });
      return tok;
    }

    if (client.authClient && client.authClient.MakeAccessToken) {
      const tok = await client.authClient.MakeAccessToken({ subject: qid });
      return tok;
    }

    throw new Error("Unable to generate backend auth token");
  }

  httpRequest(method, path, body, token, timeoutMs) {
    const options = {
      hostname: "ai.contentfabric.io",
      port: 443,
      path: `/tagstore${path}`,
      method,
      headers: {
        "Content-Type": "application/json"
      },
      timeout: timeoutMs
    };

    if (token) {
      if (token.startsWith("Bearer ")) {
        options.headers["Authorization"] = `${token}`;        
      } else {
        options.headers["Authorization"] = `Bearer ${token}`;
      }
    }

    this.Debug("Tagstore request", { method, path: options.path });

    return new Promise((resolve, reject) => {
      const req = https.request(options, res => {
        let data = "";
        res.on("data", chunk => (data += chunk));
        res.on("end", () => {
          resolve({ statusCode: res.statusCode, body: data });
        });
      });

      req.on("error", reject);
      req.on("timeout", () => req.destroy(new Error("Request timed out")));

      if (body) req.write(JSON.stringify(body));
      req.end();
    });
  }

  //
  // CREATE TRACK
  //
  async executeCreateTrack(inputs, outputs) {
    const qid = inputs.content_id;
    const track = inputs.track;
    const timeoutMs = this.Payload.parameters.timeout_ms || 10000;

    this.Info("Starting CREATE track", { qid, track });

    try {
      const token = await this.getAuthToken(inputs, qid);

      const body = {};
      if (inputs.label !== undefined) body.label = inputs.label;
      if (inputs.color !== undefined) body.color = inputs.color;
      if (inputs.description !== undefined) body.description = inputs.description;

      const path = `/${encodeURIComponent(qid)}/tracks/${encodeURIComponent(track)}`;
      const response = await this.httpRequest("POST", path, body, token, timeoutMs);

      if (response.statusCode === 201) {
        let parsed = {};
        if (response.body) {
          try { parsed = JSON.parse(response.body); } catch {}
        }

        outputs.message = parsed.message;
        outputs.track_id = parsed.track_id;
        outputs.track = track;

        this.Info("CREATE track succeeded", { qid, track });
        return ElvOAction.EXECUTION_COMPLETE;
      }

      let errorMsg = `HTTP ${response.statusCode}`;
      if (response.body) {
        try { errorMsg = JSON.parse(response.body).message || response.body; } catch { errorMsg = response.body; }
      }

      this.Debug("CREATE track failed", { statusCode: response.statusCode });
      outputs.error = errorMsg;
      return ElvOAction.EXECUTION_FAILED;

    } catch (err) {
      this.Error("CREATE track exception", err);
      outputs.error = err.message || String(err);
      return ElvOAction.EXECUTION_FAILED;
    }
  }

  //
  // ADD TAGS
  //
  async executeAddTags(inputs, outputs) {
    const qid = inputs.content_id;
    const timeoutMs = this.Payload.parameters.timeout_ms || 10000;

    this.Info("Starting ADD tags", { qid, track: inputs.track });

    try {
      const token = await this.getAuthToken(inputs, qid);

      const body = {
        author: inputs.author,
        track: inputs.track,
        tags: inputs.tags
      };

      const path = `/${encodeURIComponent(qid)}/tags`;
      const response = await this.httpRequest("POST", path, body, token, timeoutMs);

      if (response.statusCode === 200) {
        let parsed = {};
        if (response.body) {
          try { parsed = JSON.parse(response.body); } catch {}
        }

        outputs.batch_id = parsed.batch_id;

        this.Info("ADD tags succeeded", { qid, batch_id: parsed.batch_id });
        return ElvOAction.EXECUTION_COMPLETE;
      }

      let errorMsg = `HTTP ${response.statusCode}`;
      if (response.body) {
        try { errorMsg = JSON.parse(response.body).message || response.body; } catch { errorMsg = response.body; }
      }

      this.Debug("ADD tags failed", { statusCode: response.statusCode });
      outputs.error = errorMsg;
      return ElvOAction.EXECUTION_FAILED;

    } catch (err) {
      this.Error("ADD tags exception", err);
      outputs.error = err.message || String(err);
      return ElvOAction.EXECUTION_FAILED;
    }
  }

  //
  // QUERY TAGS
  //
  async executeQueryTags(inputs, outputs) {
    const qid = inputs.content_id;
    const timeoutMs = this.Payload.parameters.timeout_ms || 10000;

    this.Info("Starting QUERY tags", { qid });

    try {
      const token = await this.getAuthToken(inputs, qid);

      const explicitLimit = inputs.limit;
      const explicitStart = inputs.start;
      const defaultLimit = 500;

      const allTags = [];
      let finalMeta = null;

      // Single-shot mode
      if (explicitLimit !== undefined || explicitStart !== undefined) {
        const query = {};
        if (inputs.track !== undefined) query.track = inputs.track;
        if (inputs.author !== undefined) query.author = inputs.author;
        if (inputs.start_time_gte !== undefined) query.start_time_gte = inputs.start_time_gte;
        if (inputs.end_time_lte !== undefined) query.end_time_lte = inputs.end_time_lte;
        if (explicitLimit !== undefined) query.limit = explicitLimit;
        if (explicitStart !== undefined) query.start = explicitStart;

        const qs = querystring.stringify(query);
        const path = `/${encodeURIComponent(qid)}/tags${qs ? "?" + qs : ""}`;

        const response = await this.httpRequest("GET", path, null, token, timeoutMs);

        if (response.statusCode === 200) {
          let parsed = {};
          if (response.body) parsed = JSON.parse(response.body);

          outputs.tags = parsed.tags || [];
          outputs.meta = parsed.meta || {};

          this.Info("QUERY tags succeeded (single-shot)", {
            qid,
            count: (parsed.tags || []).length
          });
          return ElvOAction.EXECUTION_COMPLETE;
        }

        let errorMsg = `HTTP ${response.statusCode}`;
        if (response.body) {
          try { errorMsg = JSON.parse(response.body).message || response.body; } catch { errorMsg = response.body; }
        }

        this.Debug("QUERY tags failed (single-shot)", { statusCode: response.statusCode });
        outputs.error = errorMsg;
        return ElvOAction.EXECUTION_FAILED;
      }

      // Auto-pagination
      let start = 0;
      const limit = defaultLimit;

      while (true) {
        const query = { start, limit };
        if (inputs.track !== undefined) query.track = inputs.track;
        if (inputs.author !== undefined) query.author = inputs.author;
        if (inputs.start_time_gte !== undefined) query.start_time_gte = inputs.start_time_gte;
        if (inputs.end_time_lte !== undefined) query.end_time_lte = inputs.end_time_lte;

        const qs = querystring.stringify(query);
        const path = `/${encodeURIComponent(qid)}/tags${qs ? "?" + qs : ""}`;

        const response = await this.httpRequest("GET", path, null, token, timeoutMs);

        if (response.statusCode !== 200) {
          let errorMsg = `HTTP ${response.statusCode}`;
          if (response.body) {
            try { errorMsg = JSON.parse(response.body).message || response.body; } catch { errorMsg = response.body; }
          }

          this.Debug("QUERY tags failed (paged)", { statusCode: response.statusCode });
          outputs.error = errorMsg;
          return ElvOAction.EXECUTION_FAILED;
        }

        let parsed = {};
        if (response.body) parsed = JSON.parse(response.body);

        const tags = parsed.tags || [];
        const meta = parsed.meta || {};

        allTags.push(...tags);
        finalMeta = meta;

        if (!meta.count || meta.count < limit) break;

        start += limit;
      }

      outputs.tags = allTags;
      outputs.meta = finalMeta || {
        total: allTags.length,
        count: allTags.length,
        start: 0,
        limit: defaultLimit
      };

      this.Info("QUERY tags succeeded (paged)", {
        qid,
        total: allTags.length
      });

      return ElvOAction.EXECUTION_COMPLETE;

    } catch (err) {
      this.Error("QUERY tags exception", err);
      outputs.error = err.message || String(err);
      return ElvOAction.EXECUTION_FAILED;
    }
  }

  //
  // WRITE_TO_FABRIC
  //
  async executeWriteToFabric(inputs, outputs) {
    const qid = inputs.content_id;
    const writeToken = inputs.write_token;
    const timeoutMs = this.Payload.parameters.timeout_ms || 10000;

    this.Info("Starting WRITE_TO_FABRIC", { qid });

    try {
      const token = await this.getAuthToken(inputs, qid);

      const query = { write_token: writeToken };
      const qs = querystring.stringify(query);
      const path = `/${encodeURIComponent(qid)}/write${qs ? "?" + qs : ""}`;

      const response = await this.httpRequest("POST", path, null, token, timeoutMs);

      if (response.statusCode === 200) {
        outputs.status = "success";
        this.Info("WRITE_TO_FABRIC succeeded", { qid });
        return ElvOAction.EXECUTION_COMPLETE;
      }

      let errorMsg = `HTTP ${response.statusCode}`;
      if (response.body) {
        try { errorMsg = JSON.parse(response.body).message || response.body; } catch { errorMsg = response.body; }
      }

      this.Debug("WRITE_TO_FABRIC failed", { statusCode: response.statusCode });
      outputs.error = errorMsg;
      return ElvOAction.EXECUTION_FAILED;

    } catch (err) {
      this.Error("WRITE_TO_FABRIC exception", err);
      outputs.error = err.message || String(err);
      return ElvOAction.EXECUTION_FAILED;
    }
  }

  //
  // LIST_ALL_TRACKS
  //
  async executeListAllTracks(inputs, outputs) {
    const qid = inputs.content_id;
    const timeoutMs = this.Payload.parameters.timeout_ms || 10000;

    this.Info("Starting LIST_ALL_TRACKS", { qid });

    try {
      const token = await this.getAuthToken(inputs, qid);

      const path = `/${encodeURIComponent(qid)}/tracks`;
      const response = await this.httpRequest("GET", path, null, token, timeoutMs);

      if (response.statusCode === 200) {
        let parsed = {};
        if (response.body) {
          try { parsed = JSON.parse(response.body); } catch {}
        }

        outputs.tracks = parsed.tracks || [];

        this.Info("LIST_ALL_TRACKS succeeded", {
          qid,
          count: outputs.tracks.length
        });

        return ElvOAction.EXECUTION_COMPLETE;
      }

      let errorMsg = `HTTP ${response.statusCode}`;
      if (response.body) {
        try { errorMsg = JSON.parse(response.body).message || response.body; } catch { errorMsg = response.body; }
      }

      this.Debug("LIST_ALL_TRACKS failed", { statusCode: response.statusCode });
      outputs.error = errorMsg;
      return ElvOAction.EXECUTION_FAILED;

    } catch (err) {
      this.Error("LIST_ALL_TRACKS exception", err);
      outputs.error = err.message || String(err);
      return ElvOAction.EXECUTION_FAILED;
    }
  }
}

if (ElvOAction.executeCommandLine(ElvOActionHandleTags)) {
  ElvOAction.Run(ElvOActionHandleTags);
} else {
  module.exports = ElvOActionHandleTags;
}
