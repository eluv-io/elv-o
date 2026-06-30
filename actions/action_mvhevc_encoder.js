const ElvOAction = require("../o-action").ElvOAction;
const Path = require('path');
const { execSync, spawn } = require('child_process');


class ElvOActionMvhevcEncoder extends ElvOAction  {
    
    Parameters() {
        return {
            parameters: {
                encode_executable_path:{type:"string", required: false, default: "qfab-mvhevc/bin/mvhevc_encoder"},                
                encode_command_line_options: {type:"string", required: false, default: "-keyint %double_frame_rate% -bframes 0  -bitrate %bit_rate_kbs% -w %width% -h %height%"},
                package_executable_path:{type:"string", required: false, default: "qfab-mvhevc/bin/mvhevc"},
                package_command_line_options: {type:"string", required: false, default: "add -fps %frame_rate% -spatial "},
                variables: {type:"object", required: false, default: {"bit_rate_kbs": "string", "width": "numeric", "height": "numeric", "double_frame_rate": "numeric", "frame_rate": "numeric"}},               
                action: {type: "string", required: true, values: ["COMBINE","REENCODE", "ENCODE", "PACKAGE"]}
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
        if (parameters.action == "COMBINE") {
            delete inputs.double_frame_rate;
            inputs.bit_rate = inputs.bit_rate_kbs;
            delete inputs.bit_rate_kbs
            inputs.left_eye_file_path = {type: "string", required:true};
            inputs.right_eye_file_path = {type: "string", required:true};
            inputs.output_file_path = {type: "file", required: true};
            inputs.execution_priority = {type: "numeric", required: false, default: null};
        }

        if (parameters.action == "PACKAGE") {
            delete inputs.double_frame_rate;
            delete inputs.width;
            delete inputs.height;
            inputs.input_file_path = {type: "file", required: true};
            inputs.output_file_path = {type: "file", required: true};
        }
        
        let outputs = {
            stderr: {type: "string"},
            execution_code: {type:"numeric"},
            output_file_path: {type: "string"}
        };
        return { inputs, outputs };
    };
    
    ActionId() {
        return "mvhevc_encoder";
    };
    
    
    
    async Execute(inputs, outputs) {
        if (this.Payload.parameters.action == "COMBINE") {
            let result = await this.executeCombineEncode(inputs, outputs);
            if (result == ElvOAction.EXECUTION_COMPLETE) {
                return await this.executeCombinePackage(inputs, outputs);
            }
        }
        if (this.Payload.parameters.action == "PACKAGE") {
            return await this.executeCombinePackage(inputs, outputs);
        }
        throw "Unsupported action "+this.Payload.parameters.action;
    }
    
    async executeCombineEncode(inputs, outputs) {
        if (inputs.frame_rate && !inputs.double_frame_rate) {
            inputs.double_frame_rate = Math.ceil(inputs.frame_rate) * 2;
            this.reportProgress("Setting up keyint based on framerate of "+ inputs.frame_rate, inputs.double_frame_rate);
        }
        if (inputs.bit_rate && !inputs.bit_rate_kbs) {
            inputs.bit_rate_kbs = Math.ceil(inputs.bit_rate /1000);
            this.reportProgress("Setting up bit rate to (kbs)", inputs.bit_rate_kbs);
        }
        console.log("inputs", inputs);
        let commandLineOptions = inputs.encode_command_line_options || await this.expandDynamicVariables(this.Payload.inputs, JSON.stringify(this.Payload.parameters.encode_command_line_options), this.Payload.parameters.variables);
        
        let exe = this.Payload.parameters.encode_executable_path;
        let outputFilePath = inputs.output_file_path.replace(/\.[^.]*$/,"")+ ".hevc";
        this.reportProgress("Setting encoding file path to ", outputFilePath);
        //let args = ["-c", exe,  commandLineOptions + " \""+inputs.left_eye_file_path + "\" \""+inputs.right_eye_file_path+ "\"  \"" + outputFilePath +"\""];
        //let args = ["-c", exe,  commandLineOptions , inputs.left_eye_file_path , inputs.right_eye_file_path, outputFilePath ];
        let args = ["-c", exe + " " + commandLineOptions +" " + inputs.left_eye_file_path + " " + inputs.right_eye_file_path +" "+outputFilePath ];
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
                    tracker.ReportProgress("Encoding " + data.trim());
                    lastReported = now;
                }
            });
            
            proc.on('close', function(executionCode) {
                outsideResolve(executionCode);
                tracker.ReportProgress("Command executed");
            });
            
            outputs.execution_code = await commandExecuted;
            if (outputs.execution_code == 0) {
                this.ReportProgress("Encoding complete");
                outputs.output_file_path = outputFilePath;
            } else {
                throw Error("Encoding returned exec code: " +  outputs.execution_code)
            }
        } catch(error) {
            this.Error("Execution failed", error)
            return ElvOAction.EXECUTION_EXCEPTION
        }
        return 100;
    };

    async executeCombinePackage(inputs, outputs) {

        let commandLineOptions = inputs.package_command_line_options || await this.expandDynamicVariables(this.Payload.inputs, JSON.stringify(this.Payload.parameters.package_command_line_options), {"frame_rate": "numeric"});
        
        let inputFilePath = inputs.input_file_path || (inputs.output_file_path.replace(/\.[^.]*$/,"") + ".hevc");
        let exe = this.Payload.parameters.package_executable_path;
        //let args = ["-c", exe,  commandLineOptions, inputFilePath, inputs.output_file_path];
        let args = ["-c", exe + "  "+ commandLineOptions +" "+inputFilePath +" "+ inputs.output_file_path];
        
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
                    tracker.ReportProgress("Packaging " + data.trim());
                    lastReported = now;
                }
            });
            
            proc.on('close', function(executionCode) {
                outsideResolve(executionCode);
                tracker.ReportProgress("Command executed");
            });
            
            outputs.execution_code = await commandExecuted;
            if (outputs.execution_code == 0) {
                this.ReportProgress("Packaging complete");
                outputs.output_file_path = inputs.output_file_path;
            } else {
                throw Error("Packaging returned exec code: " +  outputs.execution_code)
            }
        } catch(error) {
            this.Error("Execution failed", error)
            return ElvOAction.EXECUTION_EXCEPTION
        }
        return 100;
    };




    static REVISION_HISTORY = {
        "0.0.1": "Initial release",
        "0.0.2": "2026-05-11 - with encode and package"
    };
    static VERSION = "0.0.2";
}


if (ElvOAction.executeCommandLine(ElvOActionMvhevcEncoder)) {
    ElvOAction.Run(ElvOActionMvhevcEncoder);
} else {
    module.exports=ElvOActionMvhevcEncoder;
}
