const ElvOAction = require("../o-action").ElvOAction;
//const ElvOFabricClient = require("../o-fabric");
const fetch = require('node-fetch');

class ElvOActionSlack extends ElvOAction  {

  ActionId() {
    return "slack";
  };

  Parameters() {
    return {
      "parameters": {
        loosen_certificate: {type: "boolean", required: false, default: false},
        is_slack_webhook: {type: "boolean", required: false, default: true},
        text: {type: "string", required: false, default: "%%\"%text%\"%%"},
        blocks: {type: "string", required: false, default: null},
        variables: {
          type: "object", required: false, default: {text: {type: "string", required: false, default: "-"}}
        },
        headers: {type: "array", required: false, default:[]},
        success_field: {type: "string", required: false, default: "message"} ,
        success_value: {type: "string", required: false, default: "success"}
      }
    }
  };

  IOs(parameters) {
    let inputs =  parameters.variables || {}; //all variables are to be explicitly defined for now
    if (parameters.headers) {
      for (let header of parameters.headers) {
        inputs[header] = {type: "password", required: true}; //password allows string, but would decrypt if needed (for API key typically)
      }
    }
    inputs.web_hook = {type: "password", required:true};
    let outputs =  {text: {type:"string"}, blocks:  {type:"string"}, result:{type:"string"}};
    return {inputs: inputs, outputs: outputs}
  };


  async Execute(handle, outputs) {
    let inputs = this.Payload.inputs;
    let text = this.Payload.parameters.text;
    let blocks = this.Payload.parameters.blocks;
    this.ReportProgress("Submitting message to web-hook");
    let headers = null;
    if (this.Payload.parameters.loosen_certificate) {
      process.env['NODE_TLS_REJECT_UNAUTHORIZED'] = 0
    }
    if (this.Payload.parameters.headers && this.Payload.parameters.headers.length != 0) {
      headers = {};
      for (let header of this.Payload.parameters.headers) {
        headers[header] = inputs[header];
      }
    }
    let body = this.Payload.parameters.is_slack_webhook ? JSON.stringify({text, blocks}) : text
    let rawResult = await fetch(inputs.web_hook, {
      method: "POST",
      body,
      headers
    });
    let result = await rawResult.text();
    outputs.result = result;
    this.ReportProgress("Response received from web-hook call", result);
    if  (this.Payload.parameters.is_slack_webhook && result != "ok") {
      this.Error("Unexpected response, 'ok' was expected ", result);
      this.ReportProgress("Unexpected response, 'ok' was expected");
      return ElvOAction.EXECUTION_EXCEPTION;
    } else {
      let parsedResult;
      try {
        parsedResult = JSON.parse(result);
      } catch(err) {
        this.ReportProgress("Result received is not valid JSON");
      }
      outputs.text = text;
      outputs.blocks = blocks;
      if (parsedResult && parsedResult.message) {
        if ((parsedResult[this.Payload.parameters.success_field] == this.Payload.parameters.success_value) || (parsedResult.message == "Eluvio event received.") ) {
          return ElvOAction.EXECUTION_COMPLETE;
        } else {
          this.ReportProgress("Unexpected message", parsedResult.message);
          return ElvOAction.EXECUTION_EXCEPTION;
        }
      }
      return ElvOAction.EXECUTION_COMPLETE;     
    }

  };

  static REVISION_HISTORY = {
    "0.0.1": "Initial release",
    "0.0.2": "Adds ability to set custom headers",
    "0.0.3": "changes non-slack processing to avoid double escaping",
    "0.0.4": "Adds option to loosen the certificate handshake",
    "0.0.5": "Adds web-hook specific parsing logic that should not be here",
    "0.0.6": "Adds parameterization of success",
    "0.0.7": "2026-06-05 - Types header fields as password to allow encrypted API keys"
  };
  static VERSION = "0.0.7";

}

if (ElvOAction.executeCommandLine(ElvOActionSlack)) {
  ElvOAction.Run(ElvOActionSlack);
} else {
  module.exports=ElvOActionSlack;
}


