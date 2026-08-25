const ElvOAction = require("../o-action").ElvOAction;
const Path = require('path');
const { execSync, spawn } = require('child_process');


class ElvOActionFfmpeg extends ElvOAction  {
    
    Parameters() {
        return {
            parameters: {
                aws_s3_inputs: {type: "boolean"}, 
                command_line_options: {type:"string", required: false, default: null}, 
                decoder_options: {type:"string", required: false, default: null}, //ffmpeg <decoder option> -i inputs <command_line_options>
                number_of_input_files: {type: "numeric", required: false}, 
                variable_number_of_input_files: {type: "boolean", required: false, default: false},
                variable_number_of_input_files: {type: "boolean", required: false, default: false},
                variables: {type:"object", required: false}
            }
        };
    };
    
    IOs(parameters) {
        let inputs;
        if (parameters.command_line_options) {     
             inputs = this.parseDynamicVariables(parameters.command_line_options, parameters.variables);
        } else {
            inputs = {command_line_options: "string"};
        }
        if (parameters.decoder_options) {     
             inputs = this.parseDynamicVariables(parameters.decoder_options, parameters.variables);
        } else {
            inputs = {decoder_options: "string"};
        }
        if (!parameters.variable_number_of_input_files) {
            if (parameters.number_of_input_files &&  (parameters.number_of_input_files != 1)) {
                for (let i=1; i <= parameters.number_of_input_files; i++) {
                    inputs["input_file_path_"+i] = {type: "file", required: true};
                }
            } else {
                if (parameters.number_of_input_files != 0) {
                    inputs.input_file_path = {type: "file", required: true};
                }
            }
        } else {
            inputs.input_file_paths = {type: "array", required: true};
        }
        if (parameters.aws_s3_inputs) {
            inputs.cloud_access_key_id = {type: "string", required:true};
            inputs.cloud_secret_access_key = {type: "password", required:true};
            inputs.cloud_bucket = {type: "string", required:false};
            inputs.cloud_region = {type: "file", required:true};
        }
        inputs.output_file_path = {type: "file", required: true};
        inputs.execution_priority = {type: "numeric", required: false, default: null};
        let outputs = {
            stderr: {type: "string"}, 
            execution_code: {type:"numeric"}, 
            output_file_path: {type: "string"}
        };
        return { inputs, outputs };
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
    
    async Execute(inputs, outputs) {
        let outputFilePath = this.Payload.inputs.output_file_path;
        let commandLineOptions = inputs.command_line_options || await this.expandDynamicVariables(this.Payload.inputs, JSON.stringify(this.Payload.parameters.command_line_options), this.Payload.parameters.variables);
        this.reportProgress("command_line_options: " + this.Payload.parameters.command_line_options, commandLineOptions);
        let decoderOptions = inputs.decoder_options || await this.expandDynamicVariables(this.Payload.inputs, JSON.stringify(this.Payload.parameters.decoder_options), this.Payload.parameters.variables);
        this.reportProgress("decoder_options: " + this.Payload.parameters.decoder_options, decoderOptions);
        let fileInputs = [];
        let inputFileNum;
        if (!this.Payload.parameters.variable_number_of_input_files) {
            inputFileNum = this.Payload.parameters.hasOwnProperty("number_of_input_files") ?  this.Payload.parameters.number_of_input_files : 1;
        } else {
            inputFileNum = inputs.input_file_paths.length;
        }
        if (inputFileNum == 1) {
            fileInputs.push("-i");
            fileInputs.push(this.expandInputFilePath(inputs.input_file_path || inputs.input_file_paths[0]));
        } else {
            for (let i=1; i <= inputFileNum; i++) {
                fileInputs.push("-i");
                fileInputs.push(this.expandInputFilePath(inputs["input_file_path_"+i] || inputs.input_file_paths[i -1]));
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
        let args;
        let exec =  "ffmpeg " + ((!decoderOptions) ? "" : (decoderOptions + " "))
        if (inputs.execution_priority == null) {
            args = ["-c", exec + fileInputs.join(" ") +" "+ commandLineOptions + " \"" + outputFilePath + "\""];
        } else  {            
            args = ["-c", "nice -n "+inputs.execution_priority+ " "+ exec + fileInputs.join(" ") +" "+ commandLineOptions + " \"" + outputFilePath + "\""];
        }
        
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
    static REVISION_HISTORY = {
        "0.0.1": "Initial release",
        "0.0.2": "Uses sh in spawn to allows piped commands in the command line options",
        "0.0.3": "Handles s3 partial paths",
        "0.0.4": "2026-02-04 - ML - Adds support for custom execution priority (using nice)",
        "0.0.5": "2026-04-29 - ML - Adds option to execute with variable number of files" ,
        "0.0.6": "2026-08-20 - ML - Adds option to add decoder flags to the command line - decoder flags precede the -i <input>"
    };
    static VERSION = "0.0.6a";
}


if (ElvOAction.executeCommandLine(ElvOActionFfmpeg)) {
    ElvOAction.Run(ElvOActionFfmpeg);
} else {
    module.exports=ElvOActionFfmpeg;
}