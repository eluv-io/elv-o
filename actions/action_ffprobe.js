const ElvOAction = require("../o-action").ElvOAction;

const { execSync } = require('child_process');



class ElvOActionFfprobe extends ElvOAction  {
    
    Parameters() {
        return {
            parameters: {
                aws_s3_inputs: {type: "boolean"}, 
                command_line_options: {type:"string", required: false, default:""}, 
                variables: {type:"object", required: false}
            }
        };
    };
    
    IOs(parameters) {
        let inputs =  parameters.variables || {};
        inputs.input_file_path = {type: "file", required: true};
        if (parameters.aws_s3_inputs) {
            inputs.cloud_access_key_id = {type: "string", required:true};
            inputs.cloud_secret_access_key = {type: "string", required:true};
            inputs.cloud_bucket = {type: "string", required:false};
            inputs.cloud_region = {type: "file", required:true};
        }
        return { inputs : inputs, 
            outputs: {
                results:  {type: "object", format:"json"}, 
                stderr: {type: "string"}, 
                execution_code: {type:"numeric"}
            }
        };
    };
    
    ActionId() {
        return "ffprobe";
    };
    
    ProgressMessage(tracker) {
        if (tracker[100]) {
            return "Execution " + tracker[80].state;
        }
        if (tracker[99]) {
            return "Attempting to update execution status";
        }
        if (tracker[90]) {
            return "Execution outputs saved ";
        }
        if (tracker[89]) {
            return "Attempting to process execution outputs";
        }
        if (tracker[80]) {
            return "Wrapping up execution";
        }
        if (tracker[15]) {
            return "Executing " + tracker[15].details;
        }
        if (tracker[10]) {
            return "Execution started";
        }
        return "No tracking information yet";
    };

    expandInputFilePath(rawPath){
        if (!this.Payload.parameters.aws_s3_inputs) {
            return rawPath;
        } else {
            if (rawPath.match(/^s3:/)) {
                return execSync("AWS_ACCESS_KEY_ID=" + this.Payload.inputs.cloud_access_key_id +
                    "  AWS_SECRET_ACCESS_KEY=" + this.Payload.inputs.cloud_secret_access_key +
                    "  aws s3 presign --region=" + this.Payload.inputs.cloud_region + " \"" + rawPath + "\" --expires 41600").toString().replace(/\n$/, "");
            } else {
                return rawPath;
            }
        }
    };
    
    async Execute(handle, outputs) {
        let inputFilePath = this.expandInputFilePath(this.Payload.inputs.input_file_path); //legacy support removed (replace acquire file) .replace(/^temp:\/\//,"")
        let commandLineOptions = this.Payload.parameters.command_line_options;
        let commandLine = "ffprobe   -v quiet -show_format -show_streams -print_format json "+ commandLineOptions+ " \"" + inputFilePath+ "\"";
        this.trackProgress(15,"Command line prepared",commandLine);
        try {
            outputs.results = JSON.parse(execSync(commandLine).toString());
            outputs.execution_code = 0;
            outputs.stderr = "";
            if (!outputs.results || (Object.keys(outputs.results).length ==0)) {
                this.ReportProgress("Probing returned no output");
                return ElvOAction.EXECUTION_EXCEPTION;
            } 
        } catch(error) {
            //stdout = error.stdout.toString();
            outputs.stderr = error.stderr.toString();
            outputs.execution_code =error.status;
            this.Error("FProbe execution error", error);
            return ElvOAction.EXECUTION_EXCEPTION
        }
        return  ElvOAction.EXECUTION_COMPLETE;
    };
    static VERSION = "0.0.4";
    static REVISION_HISTORY = {
        "0.0.1": "Initial release",
        "0.0.2": "Use standard dynamic variable expansion",
        "0.0.3": "Identifies runtime exception",
        "0.0.4": "Adds support for probing a media file store on aws s3"
    };
}


if (ElvOAction.executeCommandLine(ElvOActionFfprobe)) {
    ElvOAction.Run(ElvOActionFfprobe);
} else {
    module.exports=ElvOActionFfprobe;
}

/*

node actions/action_ffprobe.js specs  --private-key=0xprivate --verbose
Command specs
{ parameters: { command_line_options: { type: 'string' } } }


node actions/action_ffprobe.js specs --private-key=0xprivate --verbose --payload='{"parameters" : {"command_line_options": "-b %BABA%",  "variables":{"BABA":  {type: "string",required:"false","default":"ZOB"}}}}'
Command specs
{
    inputs: {
        BABA: { type: 'string', required: 'false', default: 'ZOB' },
        input_file: { type: 'file', required: 'true' }
    },
    outputs: {
        results: { type: 'object', format: 'json' },
        stderr: { type: 'string' },
        execution_code: { type: 'numeric' }
    }
}


node actions/action_ffprobe.js execute --private-key=0xprivate --verbose --payload='{"parameters" : {"command_line_options": "-b %BABA%"},  "variables":{"BABA":  {type: "string",required:"false","default":"ZOB"}},"inputs":{"input_file":"temp:///Users/marc-olivier/Downloads/57808325821__0ABFB146-3886-4E40-9B36-10428CCFC2E5.MOV","BABA":"bite"}, "references":{"step_id":"probe_simul_1"}, "outputs_fabric_location": ""}'
Command execute
{ handle: 1611801130236 }


node actions/action_ffprobe.js check-status --handle=1611801130236 --private-key=0xprivate --verbose
Command check-status
{
    status: { state: 'complete', progress_message: 'Probing complete' },
    handle: '1611801130236',
    execution_node: ''
}

*/