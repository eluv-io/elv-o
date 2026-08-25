const ElvOAction = require("../o-action").ElvOAction;
const ElvOFabricClient = require("../o-fabric");


class ElvOActionFabricPicker extends ElvOAction  {

    Parameters() {
        return {
            parameters: { 
                action: {type:"string", required: true, values: ["PICK", "STATUS", "WAIT"]}
            }
        };
    };

    IOs(parameters) {
        let inputs={};
        let outputs={};
        
        if (parameters.action == "PICK") {
            inputs.picker_url = {type: "string", required: false, default: "http://qcheck.eluvio:9009/lros/next"}
            outputs.node = "string";
            outputs.config_url = "string";
        }
        if (parameters.action == "WAIT") {
            inputs.picker_url = {type: "string", required: false, default: "http://qcheck.eluvio:9009/lros/next"}
            outputs.node = "string";
            outputs.config_url = "string";
        }
        if (parameters.action == "STATUS") {
            inputs.picker_url = {type: "string", required: false, default: "http://qcheck.eluvio:9009/lros"}
            outputs.status = "object"
        }      
        return {inputs, outputs}
    };

    ActionId() {
        return "fabric_picker";
    };

    IsContinuous() {
        return false;
    };

    PollingInterval() {
        return 60; //poll every minute
    };

    async Execute(inputs, outputs) {
        let parameters = this.Payload.parameters
        if (parameters.action == "STATUS") {
            return await this.executeGetStatus(inputs, outputs);
        }
        if (parameters.action == "PICK") {
            return await this.executePick(inputs, outputs);
        }
        if (parameters.action == "WAIT") {
            inputs.wait = true;
            return await this.executePick(inputs, outputs);
        }
        throw "Unsupported action: "+parameters.action;
    };

    async MonitorExecution(pid, outputs) {   
        try {    
            if (ElvOAction.PidRunning(pid)) {
                return ElvOAction.EXECUTION_ONGOING;
            }            
            if (this.Payload.parameters.action == "WAIT") {
                return this.executePick({wait: true, picker_url: this.Payload.inputs.picker_url}, outputs);
            } 
            throw "Process not running";
        } catch(error) {
            this.Error("Monitoring failed", error)
            return ElvOAction.EXECUTION_EXCEPTION;
        }
    };

    async executeGetStatus(inputs, outputs) {
        let url = inputs.picker_url;
        let options = {};
        let result = await ElvOFabricClient.fetchJSON(url, options);
        if (result?.Nodes) {
            outputs.status = result;
            return ElvOAction.EXECUTION_COMPLETE;
        }
        this.ReportProgress("Call did not return correctly formatted result", result);
        return ElvOAction.EXECUTION_EXCEPTION;
    };

    async executePick(inputs, outputs) {
        let url = inputs.picker_url;
        let options = {};
        let result = await ElvOFabricClient.fetchJSON(url, options);
        console.log("result", result);
        if (result?.url) {
            outputs.node = result.url;
            outputs.config_url = result.url + "/config?self&qspace=main";
            return ElvOAction.EXECUTION_COMPLETE;
        }
        if (result?.reason) {
            this.ReportProgress("Call did not return correctly valid result", result.reason);
            if (!inputs.wait) {
                return ElvOAction.EXECUTION_FAILED;
            } else {
                return ElvOAction.EXECUTION_ONGOING;
            }
        }
        this.ReportProgress("Call did not return correctly formatted result", result);
        return ElvOAction.EXECUTION_EXCEPTION;
    };


    static VERSION = "0.0.1";
    static REVISION_HISTORY = {
        "0.0.1": "2026-01-27 - Initial release"
    };
}


if (ElvOAction.executeCommandLine(ElvOActionFabricPicker)) {
    ElvOAction.Run(ElvOActionFabricPicker);
} else {
    module.exports=ElvOActionFabricPicker;
}




