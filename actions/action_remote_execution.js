const ElvOAction = require("../o-action").ElvOAction;
const ElvOJob = require("../o-job.js");
const ElvO = require("../o-core.js");
const ElvOQueue = require("../o-queue");
const { execFileSync } = require('child_process');
const fs = require("fs");


class ElvOActionRemoteExecution extends ElvOAction  {
    
    ActionId() {
        return "remote_execution";
    };
    
    IsContinuous() {
        return false; //indicates that the execution stays within a single PID
    };
    
    Parameters() {
        return {
            parameters: {
                workflow_id: {type: "string", required: true},
                synchronous: {type: "boolean", required: false, default: true},
                ignore_subworkflow_outputs: {type: "boolean", required: false, default: false} 
            }
        };
    };
    
    IOs(parameters) {
        let io = this.retrieveIOs();
        if  (!io) {
            let host = this.Payload.inputs.server_address;
            let apiKey = this.Payload.inputs.api_key;
            let stdout = execFileSync("curl", [
                "-sS", "-k", "-d", JSON.stringify({workflow_id: parameters.workflow_id}),
                "-X", "POST", host + "/workflow_io",
                "-H", "api-key:" + apiKey
            ], {encoding: "utf8"});
            console.log("stdout", stdout);
            io = JSON.parse(stdout);
            io.inputs.queue_id = {type: "string", required: false, default: "system"};
            io.inputs.priority = {type: "numeric", required: false, default: 100};
            io.inputs.api_key =  {type: "password", required: true};
            io.inputs.server_address =  {type: "string", required: true};
            if (!parameters.synchronous) {
                io.outputs = {queue_path: "string"};
            } else {
                io.inputs.match_subworkflow_status = {type: "boolean", required: false, default: false};
            }
            this.markIOs(io);
        }
        io.inputs.job_reference = {type: "string", required: false, default: null}; //Need to figure out what happened when resubmitting an existing reference
        if (parameters.ignore_subworkflow_outputs) {
            return {inputs: io.inputs, outputs: {}};
        }
        return io;
    };
    
    
    PollingInterval() {
        return 5; //temporary 
        //return 60; //poll every minutes
    };
    
    async Execute(inputs, outputs) {
        let queueId = this.Payload.inputs.queue_id;
        let priority = this.Payload.inputs.priority || 100;
        let parentJobId = this.Payload.references && this.Payload.references.job_id;
        let prefix = (this.Payload.parameters.synchronous) ? "r-s-subworkflow" : "r-a-subworkflow";
        let itemId = this.Payload.inputs.job_reference || (prefix + "__" + parentJobId +"--"+ (this.Payload.references.step_id || (new Date()).getTime()));
        delete this.Payload.inputs.job_reference;
        this.markSubworkflowRef(itemId);
        /*let jobDescription = await this.getMetadata({
        objectId: this.Payload.parameters.workflow_id,
        metadataSubtree: "workflow_definition"
        });
        jobDescription.id = itemId;
        */
        let jobDescription = {
            id: itemId,
            workflow_object_id: this.Payload.parameters.workflow_object_id,
            workflow_id: this.Payload.parameters.workflow_id,
            root: this.Payload.references?.root,
            parent: {job_id: parentJobId, step_id: this.Payload.references.step_id, synchronous: this.Payload.parameters.synchronous},
            parameters: this.Payload.inputs
        };
        let url = inputs.server_address + "/queue_job/" + this.Payload.parameters.workflow_id + "/" + (this.Payload.inputs.queue_id || "system");
        process.env['NODE_TLS_REJECT_UNAUTHORIZED'] = 0;
        let response = await fetch(url, {
            method: "POST",
            headers: {"Content-Type": "application/json", "api-key": inputs.api_key},
            body: JSON.stringify({job_parameters: inputs})
        });
        let submission = await response.json();
        /*
        {
        queue_id: 'system',
        job_reference: 'job_1788441505028',
        queued: true
        }
        */
        console.log("submission", submission);      
        this.ReportProgress("Executing "+ this.Payload.parameters.synchronous ? "synchronous" : "asynchronous", submission);
        
        let reference = submission.job_reference;
        if (submission.queued) {
            if (this.Payload.parameters.synchronous) {
                this.ReportProgress("launched synchronous", reference);
                this.markSubworkflowRef(submission);
                return ElvOAction.EXECUTION_ONGOING;
            } else {
                this.ReportProgress("launched asynchronous");
                return ElvOAction.EXECUTION_COMPLETE;
            }
        } else {
            this.ReportProgress("Failed to queue remote execution job");
            return ElvOAction.EXECUTION_EXCEPTION;
        }
    };
    
    
    async MonitorExecution(pid, outputs) {
        let inputs = this.Payload.inputs;
        if (ElvOAction.PidRunning(pid)) {
            return ElvOAction.EXECUTION_ONGOING;
        }
        let subworkflowRef = this.retrieveSubworkflowRef();
        if (!subworkflowRef?.job_reference) { //if not marked yet, pid should still be running
            this.ReportProgress("Failed to retrieve sub-workflow reference");
            return ElvOAction.EXECUTION_EXCEPTION;
        }
        
        let url = inputs.server_address + "/job_status";
        process.env['NODE_TLS_REJECT_UNAUTHORIZED'] = 0;
        let response = await fetch(url, {
            method: "POST",
            headers: {"Content-Type": "application/json", "api-key": inputs.api_key},
            body: JSON.stringify({job_reference: subworkflowRef.job_reference})
        });
        let jobStatusData =  await response.json();
        console.log("jobStatusData", jobStatusData);
        if (jobStatusData.status_code == ElvOAction.EXECUTION_ONGOING) {
            this.reportProgress("Remote execution ongoing", jobStatusData);
            return ElvOAction.EXECUTION_ONGOING;
        }
        
        if ([100, 99, -1].includes(jobStatusData.status_code)) {
            response = await fetch(url, {
                method: "POST",
                headers: {"Content-Type": "application/json", "api-key": inputs.api_key},
                body: JSON.stringify({job_reference: subworkflowRef.job_reference, details: true})
            });
            let jobDetails =  await response.json();
            outputs.job_id = jobDetails.job_id;
            for (let step in jobDetails.status_details.steps) {
                outputs[step] = jobDetails.status_details.steps[step].outputs;
            }
            return jobStatusData.status_code 
        }
        console.log("Unexpected status, maybe we should err", jobStatusData.status_code);
        this.ReportProgress("Unexpected status, maybe we should err", jobStatusData.status_code);
        return ElvOAction.EXECUTION_ONGOING;
    };
    
    
    markSubworkflowRef(subworkflowRef) {
        this.trackProgress(ElvOActionRemoteExecution.TRACKER_REF, "Sub-workflow reference", subworkflowRef);
    };
    
    markSubworkflowJobId(subworkflowJobId) {
        let jobId = this.Tracker && this.Tracker[ElvOActionRemoteExecution.TRACKER_ID];
        if (!jobId) {
            this.trackProgress(ElvOActionRemoteExecution.TRACKER_ID, "Sub-workflow job-id", subworkflowJobId);
        }
    };
    
    retrieveSubworkflowRef() {
        let info = this.Tracker && this.Tracker[ElvOActionRemoteExecution.TRACKER_REF];
        return info && info.details;
    };
    
    
    markIOs(io) {
        this.trackProgress(ElvOActionRemoteExecution.TRACKER_IO, "IO", io);
    };
    
    retrieveIOs() {
        let info = this.Tracker && this.Tracker[ElvOActionRemoteExecution.TRACKER_IO];
        return info && info.details;
    };
    
    static TRACKER_REF = 63;
    static TRACKER_IO = 64;
    static QUEUED_REF = 66;
    static TRACKER_ID = 67;
    
    
    static REVISION_HISTORY = {
        "0.0.1": "Initial release",        
    };
    static VERSION = "0.0.1";
    
}


if (ElvOAction.executeCommandLine(ElvOActionRemoteExecution)) {
    ElvOAction.Run(ElvOActionRemoteExecution);
} else {
    module.exports=ElvOActionRemoteExecution;
}
