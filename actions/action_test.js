const ElvOAction = require("../o-action").ElvOAction;



class ElvOActionTest extends ElvOAction  {

    Parameters() {
        return {"parameters": { "code": {"type":"string"}, "outputs_variables": {"type":"object", "required": false}, "variables": {"type":"object", "required": false}}};
    };

    IOs(parameters) {
        let inputs = parameters.variables || {};
        let outputs = parameters.outputs_variables || {};
        outputs["result"] = {"type": "boolean"};
        return {inputs: inputs, outputs: outputs}
    };

    ActionId() {
        return "test";
    };



    async Execute(handle, outputs) {
        let inputs = this.Payload.inputs;
        let evalCodeBlock = this.Payload.parameters.code;
        //this.Debug("evalCodeBlock for "+ handle, evalCodeBlock);
        //this.Debug("inputs for "+ handle, inputs);
        let statusCode;
        try {
            outputs.result = (eval(evalCodeBlock) && true) || false;
            statusCode = (outputs.result) ? ElvOAction.EXECUTION_COMPLETE : ElvOAction.EXECUTION_FAILED;
            this.ReportProgress("Test completed", outputs.result);
        } catch(err) {
            statusCode = ElvOAction.EXECUTION_EXCEPTION;
            this.Error("Could not execute test code block", err);
            outputs.result = null;
        }
        return statusCode;
    };
    static VERSION = "0.0.3";
    static REVISION_HISTORY = {
        "0.0.1": "Initial release",
        "0.0.2": "Remove reference  to old report methods",
        "0.0.3": "Adds progress report"
    };
}


if (ElvOAction.executeCommandLine(ElvOActionTest)) {
    ElvOAction.Run(ElvOActionTest);
} else {
    module.exports=ElvOActionTest;
}





/*
node actions/action_test.js specs  --private-key=0xDaKey --verbose
Command specs
{
  parameters: { code: { type: 'string' }, outputs_variables: { type: 'object' } },
  variables: { type: 'object', required: false }
}

node actions/action_test.js specs  --private-key=0xb0001cad3e9c749f8fa7265f058f57e4b21054eda4c94801448efb7d4daa5b77 --verbose --payload='{"parameters":{"code":"(inputs.bit_rate == \"9457073\")"}, "variables": {"bit_rate":{"type":"string"}}}'
Command specs
{
  inputs: { bit_rate: { type: 'string' } },
  outputs: { result: { type: 'boolean' } }
}


node actions/action_test.js execute  --private-key=0x5268c39903567717ec5ac9e381d6a9e4bf63e3fbf3cbf62831301da2e187e60d   --verbose --payload='{"parameters":{"code":"(inputs.bit_rate == \"9457073\")"}, "variables": {"bit_rate":{"type":"string"}}, "references":{"step_id":"Test-bitrate"}, "inputs":{"bit_rate":"9457071"},  "outputs_fabric_location": ""}'
Command execute
{ handle: 1611968998071 }

node actions/action_test.js check-status --handle=1611968998071 --private-key=0x5268c39903567717ec5ac9e381d6a9e4bf63e3fbf3cbf62831301da2e187e60d --verbose
Command check-status
{
  status: { state: 'failed', progress_message: 'Test failed' },
  handle: '1611968998071',
  execution_node: ''
}


node actions/action_test.js execute  --private-key=0x5268c39903567717ec5ac9e381d6a9e4bf63e3fbf3cbf62831301da2e187e60d   --verbose --payload='{"parameters":{"code":"(inputs.bit_rate == \"9457073\")"}, "variables": {"bit_rate":{"type":"string"}}, "references":{"step_id":"Test-bitrate"}, "inputs":{"bit_rate":"9457073"},  "outputs_fabric_location": ""}'
Command execute
{ handle: 1611969099162 }

node actions/action_test.js check-status --handle=1611969099162 --private-key=0x5268c39903567717ec5ac9e381d6a9e4bf63e3fbf3cbf62831301da2e187e60d --verbose
Command check-status
{
  status: { state: 'complete', progress_message: 'Test pass' },
  handle: '1611969099162',
  execution_node: ''
}





 */