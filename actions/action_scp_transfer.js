
const ElvOAction = require("../o-action").ElvOAction;
const ElvOFabricClient = require("../o-fabric");
const fs = require("fs");
const mime = require("mime-types");
const Path = require('path');
const ScpClient = require('node-scp').Client;



class ElvOActionScpTransfer extends ElvOAction  {
    
    ActionId() {
        return "scp_transfer";
    };
    
    Parameters() {
        return {
            "parameters": {
                action: {type: "string", values:["UPLOAD","DOWNLOAD"]},
                authentication_type: {type: "string", values:["PASSWORD","KEY"]}
            }
        };
    };
    
    IOs(parameters) {
        let inputs = {
            host: {type: "string", required: true},
            port: {type: "numeric", required: false, default: 22},
            user_name: {type: "string", required:false}
        }
        if (parameters.authentication_type == "PASSWORD") {
            inputs.password = {type: "password", required: true};
        } else {
            inputs.key_file_path = {type: "string", required: true};
            inputs.key_file_passphrase = {type: "password", required: false, default: null};
        }
        let outputs =  {}
        if (parameters.action == "UPLOAD") {
            inputs.local_files_path = {type: "array", required:true};
            inputs.target_folder = {type: "string", required:true};
            inputs.target_flattening_base = {type:"string", require: false, default:null}; //null indicates flattening to basename, "" indicates no flattening, "/tmp/" would indicate "/tmp/ala/la.txt"->"ala/la.txt"
            outputs.uploaded_files = "array";
        }
        
        if (parameters.action == "DOWNLOAD") {
            throw "Not implemented yet"
        }
        
        return {inputs: inputs, outputs: outputs}
    };
    
    
    flatten(sourceFilePath, targetFlatteningBase) {
        if ((typeof targetFlatteningBase) == "undefined") {
            targetFlatteningBase = this.Payload.inputs.target_flattening_base;
        }
        sourceFilePath = sourceFilePath.replace("s3://","");
        if (targetFlatteningBase == null) {
            return  Path.basename(sourceFilePath);
        }
        targetFlatteningBase = targetFlatteningBase.replace("s3://","");
        return sourceFilePath.replace(targetFlatteningBase,"");
    };
    
    
    
    
    async executeScpUpload(inputs, outputs, client) {
        outputs.uploaded_files = {};
        let uploads=0
        for (let filePath of inputs.local_files_path) {
            try {
                this.ReportProgress("Processing file upload for " + filePath);
                let targetFilePath = Path.join(inputs.target_folder, Path.basename(filePath));
                await client.uploadFile(filePath, targetFilePath);
                outputs.uploaded_files[filePath] = targetFilePath;
                uploads++
            } catch(err) {
                this.Error("Failed to upload "+ filePath, err);
            }
        }
        client.close(); 
        if (uploads == inputs.local_files_path.length) {
            this.ReportProgress("Uploads complete");
            return  ElvOAction.EXECUTION_COMPLETE;
        } else {
            this.ReportProgress("Some Uploads did complete");
            return  ElvOAction.EXECUTION_EXCEPTION;
        }
        
    };
    
    
    
    
    
    async Execute(handle, outputs) {
        let client;
        if (this.Payload.parameters.authentication_type == "PASSWORD") {
            client = await ScpClient({
                host: this.Payload.inputs.host,
                port: this.Payload.inputs.port,
                username: this.Payload.inputs.user_name,
                password: this.Payload.inputs.password
            });
        } else {
            client = await ScpClient({
                host: this.Payload.inputs.host,
                port: this.Payload.inputs.port,
                username: this.Payload.inputs.user_name,
                privateKey: fs.readFileSync(this.Payload.inputs.key_file_path)//,
                //passphrase: this.Payload.inputs.key_file_passphrase
            });
        }
        
        
        try {
            if (this.Payload.parameters.action == "UPLOAD") {
                return await this.executeScpUpload(this.Payload.inputs, outputs, client);
            }
            if (this.Payload.parameters.action == "DOWNLOAD") {
                return await this.executeFabricDownload(this.Payload.inputs, outputs, client);
            }
            throw "Unsupported action: " + this.Payload.parameters.action;
        } catch(err) {
            this.Error("Could not process " + this.Payload.parameters.action , err);
            return ElvOAction.EXECUTION_EXCEPTION;
        }
    };
    
    
    static VERSION = "0.0.1";
    static REVISION_HISTORY = {
        "0.0.1": "Initial release with only upload"
    };
}

if (ElvOAction.executeCommandLine(ElvOActionScpTransfer)) {
    ElvOAction.Run(ElvOActionScpTransfer);
} else {
    module.exports=ElvOActionScpTransfer;
}
