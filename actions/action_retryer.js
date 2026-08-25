
const ElvOAction = require("../o-action").ElvOAction;
const ElvOJob = require("../o-job");


class ElvOActionRetryer extends ElvOAction  {

    ActionId() {
        return "retryer";
    };

    IsContinuous() {
        return false;
    };

    Parameters() {
        return {
            parameters: {
                action: {type: "string", required: true, values: ["WAIT_FOR_STATUS"]}
            }
        };
    };

    IOs(parameters) {

        let inputs = {};
        let outputs = {};
        if (parameters.action == "WAIT_FOR_STATUS") {
            inputs.job_id = {type: "string", required: false, default: null};
            inputs.step_id = {type: "string", required: true};
            inputs.wait_for_status = {type: "string", required: false, default: "complete"};
            inputs.retry_on_statuses = {type: "array", required: true};
            inputs.max_retries =  {type: "numeric", required: false, default: 0};
            inputs.retry_delay = {type: "numeric", required: false, default: 60};
        }
        return {inputs, outputs};
    };

    async Execute(handle, outputs) {        
        try {
            let action = this.Payload.parameters.action;
            if (action == "WAIT_FOR_STATUS") {
                return await this.executeWaitForStatus(this.Payload.inputs, outputs);
            }
            this.Error("Unknown action", action);
            return ElvOAction.EXECUTION_EXCEPTION;
        } catch(err) {
            this.Error("Execute error", err);
            return ElvOAction.EXECUTION_EXCEPTION;
        }
    };

        async MonitorExecution(pid, outputs) {         
            let inputs = this.Payload.inputs; 
            if (this.Payload.parameters.action == "WAIT_FOR_STATUS") {
                return await this.executeWaitForStatus(inputs, outputs);
            }
            throw "Unsupported action "+ this.Payload.parameters.action;
        }

    async executeWaitForStatus(inputs, outputs) {
        let jobId = inputs.job_id || this.Payload.references.job_id;
        let info = ElvOJob.GetStepInfoSync(jobId, inputs.step_id);
        let statusFound = (ElvOAction.STATUS_LABEL[info.status_code] || "").toLowerCase();
        if (statusFound == inputs.wait_for_status) {
            return ElvOAction.EXECUTION_COMPLETE;
        }
        if (inputs.retry_on_statuses.includes(statusFound)) {
            let count = 0;
            if (inputs.max_retries != 0) {
                let infoTracker = this.Tracker[ElvOActionRetryer.RETRIES_COUNT];
                count = infoTracker?.details?.count || 0;
                if (count > inputs.max_retries) {
                    return ElvOAction.EXECUTION_FAILED;
                }
            }
            let now = (new Date()).toISOString();
            let endTime = info.end_time && (new Date(info.end_time))
            if (!endTime || ((new Date(endTime.getTime() + inputs.retry_delay * 1000)) <= new Date())){
                this.reportProgress("Initiating retry for step " + inputs.step_id, inputs.retry_delay);
                let jobRefHex = jobId.match(/j_([0-9]+)_(.*)_(0x.*)/)[3];
                ElvOJob.ArchiveStepFiles({jobRefHex}, inputs.step_id);
            }    
            this.trackProgress(ElvOActionRetryer.RETRIES_COUNT, "Retries", {count: count+1} );
        }
        return ElvOAction.EXECUTION_ONGOING;
    };

    
    PollingInterval() {
        if (this.Payload) {
            return Math.min(this.Payload.inputs.retry_delay / 2, 3600);
        } else {
            return 60; //poll every minutes
        }
    };
    

    IdleTimeout() {
        if (this.Payload) {
            return this.Payload.inputs.retry_delay * 2;
        }
        return 0; //no maximum
    };
    
    static RETRIES_COUNT = 1001;


    static REVISION_HISTORY = {
        "0.0.1": "Initial release",
        "0.0.2": "2026-08-10 - Fixes typo in job id retrieval",
        "0.0.3": "2026-08-19 - Polling and timeout are pegged to retry_delay"
    };
    static VERSION = "0.0.3";
}


if (ElvOAction.executeCommandLine(ElvOActionRetryer)) {
    ElvOAction.Run(ElvOActionRetryer);
} else {
    module.exports = ElvOActionRetryer;
}
