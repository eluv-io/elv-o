const ElvOAction = require("../o-action").ElvOAction;
const Path = require('path');
const { execSync, spawn } = require('child_process');


class ElvOActionFfmpeg extends ElvOAction  {
    
    Parameters() {
        return {"parameters": {"aws_s3_inputs": {type: "boolean"}, "command_line_options": {type:"string", required: true}, "number_of_input_files":{type: "numeric", required: false}, "variables": {type:"object", required: false}}};
    };
    
    IOs(parameters) {
        let inputs = this.parseDynamicVariables(parameters.command_line_options, parameters.variables);
        if (parameters.number_of_input_files &&  (parameters.number_of_input_files != 1)) {
            for (let i=1; i <= parameters.number_of_input_files; i++) {
                inputs["input_file_path_"+i] = {type: "file", required:"true"};
            }
        } else {
            if (parameters.number_of_input_files != 0) {
                inputs["input_file_path"] = {type: "file", required:"true"};
            }
        }
        if (parameters.aws_s3_inputs) {
            inputs.cloud_access_key_id = {type: "string", required:true};
            inputs.cloud_secret_access_key = {type: "password", required:true};
            inputs.cloud_bucket = {type: "string", required:false};
            inputs.cloud_region = {type: "file", required:true};
        }
        inputs["output_file_path"] = {type: "file", required:"true"};
        return { inputs : inputs, outputs: {"stderr": {type: "string"}, "execution_code": {type:"numeric"}, "output_file_path": {type: "string"}} };
    };
    
    ActionId() {
        return "ffmpeg";
    };
    
    expandInputFilePath(rawPath){
        if (!this.Payload.parameters.aws_s3_inputs) {
            return "\""+rawPath +"\"";
        } else {
            let  s3Path = (!rawPath.match(/^s3:/)) ? ("s3://" + Path.join(this.Payload.inputs.cloud_bucket, rawPath)) : rawPath;
            let linkCmd = "AWS_ACCESS_KEY_ID=" + this.Payload.inputs.cloud_access_key_id 
            + "  AWS_SECRET_ACCESS_KEY=" + this.Payload.inputs.cloud_secret_access_key 
            + "  aws s3 presign --region=" + this.Payload.inputs.cloud_region + " \"" + s3Path + "\" --expires 41600";
            let signedLink = execSync(linkCmd).toString().replace(/\n$/, "") ;
            return "\""+signedLink +"\"";
        }
    };
    
    async Execute(handle, outputs) {
        let outputFilePath = this.Payload.inputs.output_file_path;
        //this.Info("command_line_options: " + this.Payload.parameters.command_line_options);
        let commandLineOptions = await this.expandDynamicVariables(this.Payload.inputs, JSON.stringify(this.Payload.parameters.command_line_options), this.Payload.parameters.variables);
        //this.Info("expanded command_line_options: " + commandLineOptions);
        let fileInputs = [];
        let inputFileNum = this.Payload.parameters.hasOwnProperty("number_of_input_files") ?  this.Payload.parameters.number_of_input_files : 1;
        if (inputFileNum == 1) {
            fileInputs.push("-i");
            fileInputs.push(this.expandInputFilePath(this.Payload.inputs.input_file_path));
        } else {
            for (let i=1; i <= inputFileNum; i++) {
                fileInputs.push("-i");
                fileInputs.push(this.expandInputFilePath(this.Payload.inputs["input_file_path_"+i]));
            }
        }
        /*
        let components = commandLineOptions.split(/ /);
        let enclosed = false;
        let composite;
        for (let c=0; c < components.length; c++) {
            let component  = components[c];
            if (!enclosed){
                let matcher=component.match(/^"(.*)/);
                if (!matcher) {
                    if (component) {
                        args.push(component);
                    }
                } else {
                    let rematch = component.match(/^"(.*)"$/);
                    if (rematch) {
                        args.push(rematch[1]);
                    } else {
                        enclosed = true;
                        composite = matcher[1];
                    }
                }
            } else {
                let matcher = component.match(/^(.*)"$/);
                if (!matcher) {
                    composite = composite + " " + component
                } else {
                    composite = composite + " " + matcher[1];
                    enclosed = false;
                    args.push(composite);
                    composite = null;
                }
            }
        }
        args.push(outputFilePath);
        */
        let args = ["-c", "ffmpeg " + fileInputs.join(" ") +" "+ commandLineOptions + " \"" + outputFilePath + "\""];
        
        this.ReportProgress("Command line prepared");
        this.reportProgress("Command line args", args);
        let tracker = this;
        let lastReported = null;
        try {
            var outsideResolve;
            var outsideReject;
            var commandExecuted = new Promise(function(resolve, reject) {
                outsideResolve = resolve;
                outsideReject = reject;
            });
            
            var proc = spawn("sh",  args);
            
            proc.stdout.on('data', function(data) {
                tracker.ReportProgress("Stdout " + data);
            });
            
            proc.stderr.setEncoding("utf8")
            proc.stderr.on('data', function(data) {
                let now = new Date().getTime();
                if (!lastReported || (lastReported + 5000 <  now)) {
                    tracker.ReportProgress("Transcoding " + data.trim());
                    lastReported = now;
                }
            });
            
            proc.on('close', function(executionCode) {
                outsideResolve(executionCode);
                tracker.ReportProgress("Command executed");
            });
            
            outputs.execution_code = await commandExecuted;
            if (outputs.execution_code == 0) {
                this.ReportProgress("Transcoding complete");
                outputs.output_file_path = outputFilePath;
            } else {
                throw Error("Transcoding returned exec code: " +  outputs.execution_code)
            }
        } catch(error) {
            this.Error("Execution failed", error)
            return ElvOAction.EXECUTION_EXCEPTION
        }
        return 100;
    };
    static VERSION = "0.0.3";
    static REVISION_HISTORY = {
        "0.0.1": "Initial release",
        "0.0.2": "Uses sh in spawn to allows piped commands in the command line options",
        "0.0.3": "Handles s3 partial paths"
    };
}


if (ElvOAction.executeCommandLine(ElvOActionFfmpeg)) {
    ElvOAction.Run(ElvOActionFfmpeg);
} else {
    module.exports=ElvOActionFfmpeg;
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