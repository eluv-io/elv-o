const ElvOAction = require("../o-action").ElvOAction;
const ElvOJob = require("../o-job");

class ElvOActionFanOut extends ElvOAction  {
    
    ActionId() {
        return "fan_out";
    };

    IsContinuous() {
        return false;
    };

    
    Parameters() {
        return {parameters: {
            action_configuration: {type: "object", required:true}, 
            action_step_id: {type: "string", required: false, default: null},
            serialize: {type: "boolean", required:false, default: true},
            fan_out_variable: {type: "string", required: true},
            indexed_outputs: {type: "array",  required: false, default: []},
            execute_all: {type: "boolean", required: false, default: true}
        }};
    };
    
    IOs(parameters) {
        let inputs = {}; // parameters.action_configuration.action_configuration.inputs;
        inputs[parameters.fan_out_variable + "_array"] = {type: "array",  required: true};
        //delete inputs[parameters.fan_out_variable];
        let outputs = {statuses: {type: "object"}, fan_out_values: {type: "object"}};
        for (let indexedOutput of parameters.indexed_outputs) {
            outputs[indexedOutput + "_map"] = {type: "object"};
        }
        return {inputs, outputs};
    };
    
    async Execute(handle, outputs) {
        let jobId = this.Payload.references.job_id;
        let stepId = this.Payload.references.step_id;
        let jobInfo = ElvOJob.GetJobInfoSync({jobId});
        let workflowDefinition = jobInfo.workflow_definition;
        let inputs = this.Payload.inputs;
        let parameters = this.Payload.parameters;
        let actionStepId = this.Payload.parameters.action_step_id || stepId;
        let fanoutSize = inputs[parameters.fan_out_variable + "_array"].length;
        if (fanoutSize == 0) {
            this.Info("Empty fan-out");
            outputs.statuses = {};
            outputs.fan_out_values = {};
            for (let indexedOutput of parameters.indexed_outputs) {
                outputs[indexedOutput + "_map"] = {};
            }
            return ElvOAction.EXECUTION_COMPLETE;
        }
        let stepIds = [];
        for (let index = 0; index < fanoutSize; index++) {
            let stepId = actionStepId + "_" + index;
            stepIds.push(stepId);
            let stepDefinition = JSON.parse(JSON.stringify(parameters.action_configuration));
            stepDefinition.configuration.inputs[parameters.fan_out_variable] = {class: "constant", value: inputs[parameters.fan_out_variable + "_array"][index]};
            stepDefinition.prerequisites = {};
            stepDefinition.fanned_out = true;
            if  ((index > 0) && parameters.serialize) {
                if (!parameters.execute_all) {
                    stepDefinition.prerequisites[actionStepId + "_" + (index - 1)]  = "complete";
                } else {
                    stepDefinition.prerequisites["#"]= 1;
                    stepDefinition.prerequisites["."] = [{},{},{}];
                    stepDefinition.prerequisites["."][0][actionStepId + "_" + (index - 1)] = "complete";
                    stepDefinition.prerequisites["."][1][actionStepId + "_" + (index - 1)] = "failed";
                    stepDefinition.prerequisites["."][2][actionStepId + "_" + (index - 1)] = "exception";
                }
            }
            workflowDefinition.steps[stepId] = stepDefinition;
        }
        this.markStepIds(stepIds);
        ElvOJob.updateJobInfoSync(jobId, {workflow_definition: workflowDefinition}, false);
        return ElvOAction.EXECUTION_ONGOING;
    };
    
    async MonitorExecution(pid, outputs) {   
        try {    
            let jobId = this.Payload.references.job_id;
            if (ElvOAction.PidRunning(pid)) {
                return ElvOAction.EXECUTION_ONGOING;
            }
            let stepIds = this.retrieveStepIds();
            if (!stepIds) { // if stepIds are not found yet, it means the set-up is not complete yet
                return ElvOAction.EXECUTION_ONGOING;
            }
            let allDone = true;
            let stepsInfo = {};
            for (let stepId of stepIds) {
                let stepInfo = ElvOJob.GetStepInfoSync(jobId, stepId, true);
                if (!stepInfo) {
                    this.reportProgress("Step "+ stepId +" not started");
                    allDone = false;
                } else {
                    if (!([-1,100,99].includes(stepInfo.status_code))) {
                        this.reportProgress("Step "+ stepId +" still in progress");
                        allDone = false;
                    }
                    stepsInfo[stepId] = stepInfo;
                }
            } 
            if (!allDone) {
                return ElvOAction.EXECUTION_ONGOING;
            }
            let parameters = this.Payload.parameters;
            let inputs = this.Payload.inputs;
            let fanoutValues = inputs[parameters.fan_out_variable + "_array"];
            let status = ElvOAction.EXECUTION_COMPLETE;
            outputs.statuses = {};
            outputs.fan_out_values = {};
            for (let index=0; index <stepIds.length; index++) {
                let stepId = stepIds[index];
                let fanoutValue = fanoutValues[index];
                let stepInfo = stepsInfo[stepId];
                outputs.statuses[stepId] = stepInfo.status_code;
                if (stepInfo.status_code < status) {
                    status = stepInfo.status_code;
                }
                outputs.fan_out_values[stepId] = fanoutValue;
                for (let indexedOutput of parameters.indexed_outputs) {
                    if (!outputs[indexedOutput + "_map"]) {
                        outputs[indexedOutput + "_map"] = {};
                    }
                    outputs[indexedOutput + "_map"][stepId] = stepInfo.outputs[indexedOutput];
                }
            }
            return status;
        } catch(err) {
            this.Error("Could not check fan out status", err);
            return ElvOAction.EXECUTION_EXCEPTION;
        }
    };

    markStepIds(stepIds) {
        this.trackProgress(ElvOActionFanOut.STEP_IDS, "step ids", stepIds);
    };


    retrieveStepIds() {
        let info = this.Tracker && this.Tracker[ElvOActionFanOut.STEP_IDS];
        return info = info && info.details;
    };

    static STEP_IDS = 64;

    static VERSION = "0.0.2";
    static REVISION_HISTORY = {
        "0.0.1": "Initial release",
        "0.0.2": "Fixes compatibility issue with o-job"
    };
}
  
  
if (ElvOAction.executeCommandLine(ElvOActionFanOut)) {
    ElvOAction.Run(ElvOActionFanOut);
} else {
    module.exports=ElvOActionFanOut;
}