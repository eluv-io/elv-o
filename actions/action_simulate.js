const ElvOAction = require("../o-action").ElvOAction;
const ElvOFabricClient = require("../o-fabric");
const { execSync } = require('child_process');
const Path = require('path');
const fs = require('fs');
const ElvOJob = require("../o-job.js");

class ElvOActionSimulate extends ElvOAction  {

    ActionId() {
        return "simulate";
    };

    Parameters() {
        return {parameters: {action: {type: "string", required: true,  values:["KILL_ENGINE","KILL_STEP", "DIE", "END"]}}};
    };

    IOs(parameters) {
        let inputs = {};
        let outputs = {};
        inputs.reporting_frequency = {type: "numeric", required: false, default: 30};
        outputs.attempts = {type: "numeric"};
        if (parameters.action == "KILL_ENGINE") {
            inputs.delay = {type: "numeric", required: false, default: 0};
            inputs.reporting_frequency = {type: "numeric", required: false, default: 30};
            outputs.pid_killed = {type: "numeric"};
        }
        if (parameters.action == "END") {
            inputs.delay = {type: "numeric", required: false, default: 0};
            inputs.exit_status_code = {type: "numeric", required: false, default: 100};
        }
        if (parameters.action == "DIE") {
            inputs.delay = {type: "numeric", required: false, default: 0};
            inputs.process_exit_code = {type: "numeric", required: false, default: 1};
        }
        if (parameters.action == "KILL_STEP") {
            inputs.delay = {type: "numeric", required: false, default: 0};
            inputs.step_id = {type: "string", required: true};
            outputs.pid_killed = {type: "numeric"};
        }
        return {inputs, outputs};
    };

    static async reportAlive(action, frequency) {
        if (action) {
            ElvOActionSimulate.reportAliveAction = action;
            ElvOActionSimulate.reportAliveFrequency = frequency;
        } else {
            action = ElvOActionSimulate.reportAliveAction;
            frequency = ElvOActionSimulate.reportAliveFrequency;
        }
        if (ElvOActionSimulate.running && frequency) {
            action.ReportProgress("still alive");
            setTimeout(ElvOActionSimulate.reportAlive, frequency * 1000);
        }
    };

    async Execute(handle, outputs) {
        ElvOActionSimulate.running = true;
        try {
            ElvOActionSimulate.reportAlive(this, this.Payload.inputs.reporting_frequency);
            this.getRetries(outputs);
            let result;
            switch(this.Payload.parameters.action) {
                case "KILL_ENGINE":
                    result = await this.executeKillEngine(handle, outputs);
                    break;
                case "KILL_STEP":
                    result =  await this.executeKillStep(handle, outputs);
                    break;
                case "DIE":
                    result =  await this.executeDie(handle, outputs);
                    break;
                case "END":
                    result =  await this.executeEnd(handle, outputs);
                    break;
                default:
                    throw "Unsupported command " + this.Payload.parameters;
            }
            ElvOActionSimulate.running = false;
            return result;
        } catch(err) {
            this.Error("Execution error", err);
            this.reportProgress("Execution error", err);
            ElvOActionSimulate.running = false;
        }
    };



    async executeEnd(handle, outputs) {
        let delay = this.Payload.inputs.delay;
        if (delay) {
            this.ReportProgress("Initiated wait for " + delay + " sec");
            await this.sleep(delay * 1000)
        }
        let exitStatusCode = this.Payload.inputs.exit_status_code;
        this.ReportProgress("Exiting with status " + exitStatusCode);
        return exitStatusCode;
    };

    async executeDie(handle, outputs) {
        let delay = this.Payload.inputs.delay;
        if (delay) {
            this.ReportProgress("Initiated wait for " + delay + " sec");
            await this.sleep(delay * 1000)
        }
        let exitCode = this.Payload.inputs.process_exit_code;
        this.ReportProgress("Exiting process with status " + exitCode);
        process.exit(exitCode);
    };

    async executeKillEngine(handle, outputs) {
        let delay = this.Payload.inputs.delay;
        if (delay) {
            this.ReportProgress("Initiated wait for " + delay + " sec");
            await this.sleep(delay * 1000)
        }
        let pidFilePath = "./o.pid";
        let data = JSON.parse(fs.readFileSync(pidFilePath, "utf8"));
        if (ElvOAction.PidRunning(data.pid)) {
            this.ReportProgress("Engine pid "+ data.pid + " found to be running");
            ElvOAction.Kill(data.pid);
            outputs.pid_killed = data.pid;
            this.ReportProgress("Killed engine pid "+ data.pid);
            return 100;
        } else {
            this.ReportProgress("Engine pid "+ data.pid + " not running");
            return 99;
        }
    };

    async executeKillStep(handle, outputs) {
        let stepId = this.Payload.inputs.step_id;
        let delay = this.Payload.inputs.delay;
        if (delay) {
            this.ReportProgress("Initiated wait for " + delay + " sec");
            await this.sleep(delay * 1000)
        }
        let data = ElvOJob.GetStepInfoSync(this.Payload.references.job_id, stepId);
        if (ElvOAction.PidRunning(data.pid)) {
            this.ReportProgress("Step " + stepId + " pid "+ data.pid + " found to be running");
            ElvOAction.Kill(data.pid);
            outputs.pid_killed = data.pid;
            this.ReportProgress("Killed step pid "+ data.pid);
            return 100;
        } else {
            this.ReportProgress("Step " + stepId + " pid " + data.pid + " not running");
            return 99;
        }
    };


    getRetries(outputs) {
        let data = ElvOJob.GetStepInfoSync(this.Payload.references.job_id, this.Payload.references.step_id);
        outputs.attempts = data.attempts +1;
    };


    static VERSION = "0.0.1";
    static REVISION_HISTORY = {
        "0.0.1": "Initial release"
    };

}


if (ElvOAction.executeCommandLine(ElvOActionSimulate)) {
    ElvOAction.Run(ElvOActionSimulate);
} else {
    module.exports=ElvOActionSimulate;
}
