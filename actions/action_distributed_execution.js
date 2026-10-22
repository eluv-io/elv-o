const ElvOAction = require("../o-action").ElvOAction;
const fetch = require("node-fetch");
const fs = require("fs");
const path = require("path");
const glob = require("glob");

class ElvOActionDistributedExecution extends ElvOAction {
    
    ActionId() {
        return "distributed_execution";
    };
    
    IsContinuous() {
        return false;
    };
    
    Parameters() {
        return {
            parameters: {
                action: {required: true,  values: ["LOCATE_FILE", "CHECK_LOCAL_CAPACITY", "WAIT_FOR_LOCAL_CAPACITY", "CHECK_REMOTE_QUEUES", "CANCEL_REMOTE_JOB"]}
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
        if (parameters.action == "WAIT_FOR_LOCAL_CAPACITY") {
            inputs.workflow_id = {type: "string", required: false};
            inputs.workflow_ids = {type: "array", required: false};
            outputs.run_status = {type: "object"};
        }
        if (parameters.action == "CHECK_LOCAL_CAPACITY") {
            inputs.workflow_id = {type: "string", required: false};
            inputs.workflow_ids = {type: "array", required: false};
            outputs.run_status = {type: "object"};
        }
        if (parameters.action == "CHECK_REMOTE_QUEUES") {
            inputs.workflow_id = {type: "string", required: false};
            inputs.workflow_ids = {type: "array", required: false};
            inputs.server_addresses = {type: "array", required: true};
            inputs.api_key = {type: "password", required: true};
            inputs.queues = {type: "string", required: false, default: ["system"]};
            inputs.pop = {type: "boolean", required: false, default: false};
            outputs.queued_jobs = {type: "object"};
            outputs.popped_job_details = {type: "object"};
        }
        if (parameters.action == "CANCEL_REMOTE_JOB") {
            inputs.server_address = {type: "string", required: true};
            inputs.job_id = {type: "string", required: true};
            inputs.api_key = {type: "password", required: true};
            outputs.canceled = {type: "boolean"};
            outputs.job_details = {type: "object"};
        }
        return {inputs, outputs};
    };
    
    async Execute(inputs, outputs) {
        try {
            if (this.Payload.parameters.action == "LOCATE_FILE") {
                return await this.executeLocateFile(inputs, outputs);
            }
            if (this.Payload.parameters.action == "CHECK_LOCAL_CAPACITY") {
                return await this.executeCheckLocalCapacity(inputs, outputs);
            }
            if (this.Payload.parameters.action == "WAIT_FOR_LOCAL_CAPACITY") {
                return await this.executeWaitForLocalCapacity(inputs, outputs);
            }
            if (this.Payload.parameters.action == "CHECK_REMOTE_QUEUES") {
                return await this.executeCheckRemoteQueues(inputs, outputs);
            }
            if (this.Payload.parameters.action == "CANCEL_REMOTE_JOB") {
                return await this.executeCancelRemoteJob(inputs, outputs);
            }
        } catch(err) {
            this.Error("action_distributed_execution failed", err);
            return ElvOAction.EXECUTION_EXCEPTION;
        }
    };
    
    async MonitorExecution(pid, outputs) {   
        try {    
            if (ElvOAction.PidRunning(pid)) {
                return ElvOAction.EXECUTION_ONGOING;
            }
            
            if (this.Payload.parameters.action == "WAIT_FOR_LOCAL_CAPACITY") {
                let result = await this.executeCheckLocalCapacity(this.Payload.inputs, outputs);
                if (result == ElvOAction.EXECUTION_FAILED) return ElvOAction.EXECUTION_ONGOING;
                return result;
            }
        } catch(err) {
            this.reportProgress("Error monitoring execution", err);
            return ElvOAction.EXECUTION_EXCEPTION;
        }
    }
    
    async executeCheckRemoteQueues(inputs, outputs) {
        if (inputs.workflow_id) inputs.workflow_ids = [inputs.workflow_id];
        outputs.items_found = []
        for (let serverAddress of inputs.server_addresses) {
            try {
                this.ReportProgress("Checking server", serverAddress);
                let url = serverAddress + "/list_queued_items";
                this.reportProgress("command", "curl -X POST " + url + " -H \"Content-Type: application/json\" -H \"api-key: " + inputs.api_key +"\" -d '{\"queue_ids\": \"["+ inputs.queues.join("\",\"")+"\"]}'");
                let response = await fetch(url, {
                    method: "POST",
                    headers: {"Content-Type": "application/json", "api-key": inputs.api_key},
                    body: JSON.stringify({queue_ids: inputs.queues})
                });
                let result = await response.json();
                for (let entry of result) {
                    for (let queueId in entry) {
                        this.reportProgress("Result for "+ queueId);
                        for (let item of entry[queueId]) {
                            if (!inputs.workflow_ids || inputs.workflow_ids.includes(item.workflow_id)) {
                                item.server = serverAddress;
                                outputs.items_found.push(item);
                                if (inputs.pop && !outputs.popped_job_details) {
                                    let popUrl = serverAddress + "/cancel_job";
                                    this.reportProgress("command", "curl -X POST " + popUrl + " -H \"Content-Type: application/json\" -H \"api-key: " + inputs.api_key +"\" -d '{\"queue_id\": \""+ inputs.queue_id+"\", \"path\": \""+ inputs.path+"\"}'");
                                    let response = await fetch(popUrl, {
                                        method: "POST",
                                        headers: {"Content-Type": "application/json", "api-key": inputs.api_key},
                                        body: JSON.stringify({queue_id: item.queue_id, path: item.path})
                                    });
                                    let popResult = await response.json();
                                    if (popResult.canceled == true) {
                                        item.definition = popResult.job_details;
                                        outputs.popped_job_details = item;
                                    }                                
                                }
                            }
                        }
                    }
                }
            } catch(err) {
                this.reportProgress("Error checking server "+serverAddress, err);
            }
        }
        if (outputs.items_found.length != 0) {
            return ElvOAction.EXECUTION_COMPLETE
        }
        return ElvOAction.EXECUTION_FAILED;
    };
    
    
    // Same /cancel_job call CHECK_REMOTE_QUEUES makes inline when pop:true, exposed as
    // its own action so a job found earlier (e.g. by a prior CHECK_REMOTE_QUEUES peek)
    // can be explicitly canceled later, once it's actually safe to do so.
    async executeCancelRemoteJob(inputs, outputs) {
        let url = inputs.server_address + "/cancel_job";
        this.reportProgress("command", "curl -X POST " + url + " -H \"Content-Type: application/json\" -H \"api-key: " + inputs.api_key +"\" -d '{\"job_id\": \""+ inputs.job_id+"\"}'");
        let response = await fetch(url, {
            method: "POST",
            headers: {"Content-Type": "application/json", "api-key": inputs.api_key},
            body: JSON.stringify({job_id: inputs.job_id})
        });
        let result = await response.json();
        outputs.canceled = result.canceled == true;
        outputs.job_details = result.job_details;
        if (!outputs.canceled) {
            this.reportProgress("Remote job was not canceled", result);
            return ElvOAction.EXECUTION_FAILED;
        }
        return ElvOAction.EXECUTION_COMPLETE;
    };

    async executeWaitForLocalCapacity(inputs, outputs) {
        return ElvOAction.EXECUTION_ONGOING;
    };
    
    
    
    async executeCheckLocalCapacity(inputs, outputs) {
        if (inputs.workflow_id) inputs.workflow_ids = [inputs.workflow_id];
        let throttles = JSON.parse(fs.readFileSync(path.join(process.cwd(), "Workflows/throttles.json")));
        let global = throttles["global"];
        delete throttles["global"];
        this.reportProgress("Global limit", global);
        if (!inputs.workflow_ids) {            
            inputs.workflow_ids = Object.keys(throttles);
        }
        outputs.run_status = {};
        let allRunning = glob.sync(path.join(process.cwd(), "Jobs/running", "j_*"));
        let overallCapacity = Math.max(global - allRunning.length, 0);
        this.reportProgress("Overall capacity",overallCapacity);
        let totalCapacity = 0;
        for (let workflowId of inputs.workflow_ids) {
            let runningFound = glob.sync(path.join(process.cwd(), "Jobs/running", "j_*" + workflowId + "_0x*"));
            let rawCapacity = Math.max(throttles[workflowId].limit - runningFound.length, 0);
            let netCapacity = Math.max(Math.min(throttles[workflowId].limit, overallCapacity) - runningFound.length, 0);
            outputs.run_status[workflowId] = netCapacity
            totalCapacity += netCapacity;
        }
        if (totalCapacity == 0) {
            return ElvOAction.EXECUTION_FAILED;
        }
        return ElvOAction.EXECUTION_COMPLETE;
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
        "0.0.1": "2026-08-07 - ML - First tracked version, with fixed API-key type",
        "0.0.2": "Adds CANCEL_REMOTE_JOB to explicitly cancel a remote queued job found earlier"
    };
    static VERSION = "0.0.2";
    
};

if (require.main === module) {
    ElvOAction.Run(ElvOActionDistributedExecution);
} else {
    module.exports = ElvOActionDistributedExecution;
}
