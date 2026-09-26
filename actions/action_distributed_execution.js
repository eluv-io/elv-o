const ElvOAction = require("../o-action").ElvOAction;
const fetch = require("node-fetch");

class ElvOActionDistributedExecution extends ElvOAction {

    ActionId() {
        return "distributed_execution";
    };

    Parameters() {
        return {
            parameters: {
                action: {
                    type: "string",
                    values: ["LOCATE_FILE"],
                    required: true
                }
            }
        };
    };

    IOs(parameters) {
        let inputs = {};
        let outputs = {};
        if (parameters.action == "LOCATE_FILE") {
            inputs.file_path = {type: "string", required: true};
            inputs.server_addresses = {type: "array", required: true};
            inputs.api_key = {type: "password", required: true};
            inputs.file_type = {type: "string", required: false, default: "ANY", values: ["SIMPLE_FILE", "DIRECTORY", "ANY"]};
            inputs.returns = {type: "string", required: false, default: "LAST_MODIFIED", values: ["FIRST_FOUND", "LAST_MODIFIED", "FIRST_CREATED"]};
            outputs.server_address = {type: "string"};
            outputs.found_stats = {type: "object"};
        }
        return {inputs, outputs};
    };

    async Execute(inputs, outputs) {
        try {
            if (this.Payload.parameters.action == "LOCATE_FILE") {
                return await this.executeLocateFile(inputs, outputs);
            }
        } catch(err) {
            this.Error("action_distributed_execution failed", err);
            return ElvOAction.EXECUTION_EXCEPTION;
        }
    };

    async executeLocateFile(inputs, outputs) {
        let candidates = {};
        for (let serverAddress of inputs.server_addresses) {
            try {
                this.ReportProgress("Checking server", serverAddress);
                let url = serverAddress + "/file_info";
                this.reportProgress("command", "curl -X POST " + url + " -H \"Content-Type: application/json\" -H \"api-key: " + inputs.api_key +"\" -d '{\"file_path\": \""+ inputs.file_path+"\"}'");
                 let response = await fetch(url, {
                    method: "POST",
                    headers: {"Content-Type": "application/json", "api-key": inputs.api_key},
                    body: JSON.stringify({file_path: inputs.file_path})
                });
                let result = await response.json();
                //console.log("result", result);
                if (result.exists) {
                    if ((inputs.file_type == "SIMPLE_FILE") && !result.is_file) continue;
                    if ((inputs.file_type == "DIRECTORY") && !result.is_directory) continue;                   
                    this.ReportProgress("File found on server " + serverAddress, result);
                    if (inputs.returns == "FIRST_FOUND") {
                        outputs.server_address = serverAddress;
                        outputs.found_stats = result;
                        return ElvOAction.EXECUTION_COMPLETE;
                    } 
                    candidates[serverAddress] = result;
                }
            } catch(err) {
                //console.log("Error checking on server "+ serverAddress, err);
                this.ReportProgress("Error checking on server "+ serverAddress, err);
            }
        }
        let foundOn = Object.keys(candidates); 
        if (foundOn.length == 0) {
        this.ReportProgress("File not found on any server", inputs.file_path);
        return ElvOAction.EXECUTION_FAILED;
        }
        let best = null;
        for (let server of foundOn){
            let candidate = candidates[server];
            if (inputs.returns == "LAST_MODIFIED") {
                if ((best == null) || (best.modified_time_ms < candidate.modified_time_ms)) {
                    best = candidate;
                    outputs.server_address = server;
                    continue;
                }
            }
            if (inputs.returns == "FIRST_CREATED") {
                if ((best == null) || (best.created_time_ms > candidate.created_time_ms)) {
                    best = candidate;
                    outputs.server_address = server;
                    continue;
                }
            }
        }
        outputs.found_stats = best;
        return ElvOAction.EXECUTION_COMPLETE;
    };

     static REVISION_HISTORY = {
        "0.0.1": "2026-08-07 - ML - First tracked version, with fixed API-key type"
    };
    static VERSION = "0.0.1";

};

if (require.main === module) {
    ElvOAction.Run(ElvOActionDistributedExecution);
} else {
    module.exports = ElvOActionDistributedExecution;
}
