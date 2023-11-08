const ElvOAction = require("../o-action").ElvOAction;
//const ElvOJob = require("../o-job");
const { execSync, spawn, spawnSync } = require('child_process');
const path = require("path");
const fs = require("fs");


class ElvAwsS3Operation extends ElvOAction  {
    
    ActionId() {
        return "aws_s3_operation";
    };
    
    IsContinuous() {
        return false;
    };
    
    Parameters() {
        return {
            parameters: {
                action: {
                    type:"string", 
                    required: true, 
                    values:[
                        "DOWNLOAD_FILE", "INITIATE_GLACIER_RETRIEVAL", "MASS_INITIATE_GLACIER_RETRIEVAL", 
                        "GLACIER_RETRIEVAL", "MASS_GLACIER_RETRIEVAL", "GLACIER_RETRIEVAL_STATUS",
                        "UPLOAD_FILE", "CREATE_REMOTE_FILE"
                    ]
                }               
            }
        };
    };
    
    PollingInterval() {
        return 3600; //poll every hour
    };
    
    IOs(parameters) {
        let inputs = {}; 
        let outputs = {};
        if (parameters.action == "UPLOAD_FILE") {
            inputs.s3_file_path = {type: "string", required: true};
            inputs.cloud_region = {type: "string", required: true};   
            inputs.cloud_access_key_id = {type: "string", required: true};
            inputs.cloud_secret_access_key = {type: "password", required: true};
            inputs.cloud_bucket = {type: "string", required: false, default: null};
            inputs.local_path = {type: "string", required: true};
            outputs.remote_file_path = {type: "string"};
        }
        if (parameters.action == "CREATE_REMOTE_FILE") {
            inputs.file_content = {type: "string", required: true};
            inputs.s3_file_path = {type: "string", required: true};
            inputs.cloud_region = {type: "string", required: true};   
            inputs.cloud_access_key_id = {type: "string", required: true};
            inputs.cloud_secret_access_key = {type: "password", required: true};
            inputs.cloud_bucket = {type: "string", required: false, default: null};
            outputs.remote_file_path = {type: "string"};
            outputs.file_size = {type: "string"};
        }
        if (parameters.action == "DOWNLOAD_FILE") {
            inputs.s3_file_path = {type: "string", required: true};
            inputs.cloud_region = {type: "string", required: true};   
            inputs.cloud_access_key_id = {type: "string", required: true};
            inputs.cloud_secret_access_key = {type: "password", required: true};
            inputs.cloud_bucket = {type: "string", required: false, default: null};
            inputs.local_path = {type: "string", required: true};
            outputs.local_file_path = {type: "string"};
            outputs.file_size = {type: "string"};
        }
        if (parameters.action == "INITIATE_GLACIER_RETRIEVAL") {
            inputs.s3_file_path = {type: "string", required: true};
            inputs.cloud_region = {type: "string", required: true};   
            inputs.cloud_access_key_id = {type: "string", required: true};
            inputs.cloud_secret_access_key = {type: "password", required: true};
            inputs.cloud_bucket = {type: "string", required: false, default: null};   
            inputs.restore_tier = {type: "string", required: false, default: "Bulk",  values:  ["Bulk", "Standard", "Expedited"]};    
            inputs.restore_for_days = {type: "numeric", required: true};    
            outputs.ongoing_request = {type: "boolean"};
            outputs.expiry_date = {type: "date"};
            outputs.storage_class = {type: "string"};
        }
        if (parameters.action == "MASS_INITIATE_GLACIER_RETRIEVAL") {
            inputs.s3_file_paths = {type: "array", required: true};
            inputs.cloud_region = {type: "string", required: true};   
            inputs.cloud_access_key_id = {type: "string", required: true};
            inputs.cloud_secret_access_key = {type: "password", required: true};
            inputs.cloud_bucket = {type: "string", required: false, default: null};     
            inputs.restore_for_days = {type: "numeric", required: true};    
            inputs.restore_tier = {type: "string", required: false, default: "Bulk",  values:  ["Bulk", "Standard", "Expedited"]};     
            outputs.ongoing_requests = {type: "object"};
            outputs.expiry_dates = {type: "object"};
            outputs.storage_classes = {type: "object"};
        }
        if (parameters.action == "GLACIER_RETRIEVAL") {
            inputs.s3_file_path = {type: "string", required: true};
            inputs.cloud_region = {type: "string", required: true};   
            inputs.cloud_access_key_id = {type: "string", required: true};
            inputs.cloud_secret_access_key = {type: "password", required: true};
            inputs.cloud_bucket = {type: "string", required: false, default: null};     
            inputs.restore_for_days = {type: "numeric", required: true};    
            inputs.restore_tier = {type: "string", required: false, default: "Bulk",  values:  ["Bulk", "Standard", "Expedited"]}; 
            outputs.ongoing_request = {type: "boolean"};
            outputs.expiry_date = {type: "date"};
            outputs.storage_class = {type: "string"};
        }
        if (parameters.action == "MASS_GLACIER_RETRIEVAL") {
            inputs.s3_file_paths = {type: "array", required: true};
            inputs.cloud_region = {type: "string", required: true};   
            inputs.cloud_access_key_id = {type: "string", required: true};
            inputs.cloud_secret_access_key = {type: "password", required: true};
            inputs.cloud_bucket = {type: "string", required: false, default: null};     
            inputs.restore_for_days = {type: "numeric", required: true};    
            inputs.restore_tier = {type: "string", required: false, default: "Bulk",  values:  ["Bulk", "Standard", "Expedited"]}; 
            outputs.ongoing_requests = {type: "object"};
            outputs.expiry_dates = {type: "object"};
            outputs.storage_classes = {type: "object"};
        }
        if (parameters.action == "GLACIER_RETRIEVAL_STATUS") {
            inputs.s3_file_path = {type: "string", required: true};
            inputs.cloud_region = {type: "string", required: true};   
            inputs.cloud_access_key_id = {type: "string", required: true};
            inputs.cloud_secret_access_key = {type: "password", required: true};
            inputs.cloud_bucket = {type: "string", required: false, default: null};         
            outputs.ongoing_request = {type: "boolean"};
            outputs.expiry_date = {type: "date"};
            outputs.storage_class = {type: "string"};
        }
        return {inputs, outputs};
    };
    
    async Execute(inputs, outputs) {
        if (this.Payload.parameters.action == "CREATE_REMOTE_FILE") {
            return await this.executeCreateRemoteFile(inputs, outputs);
        }
        if (this.Payload.parameters.action == "UPLOAD_FILE") {
            return await this.executeUploadFile(inputs, outputs);
        }
        if (this.Payload.parameters.action == "DOWNLOAD_FILE") {
            return await this.executeDownloadFile(inputs, outputs);
        }
        if (this.Payload.parameters.action == "INITIATE_GLACIER_RETRIEVAL") {
            return await this.executeInitiateGlacierRetrieval(inputs, outputs);
        }
        if (this.Payload.parameters.action == "MASS_INITIATE_GLACIER_RETRIEVAL") {
            return await this.executeMassInitiateGlacierRetrieval(inputs, outputs);
        }
        if (this.Payload.parameters.action == "GLACIER_RETRIEVAL") {
            return await this.executeGlacierRetrieval(inputs, outputs);
        }
        if (this.Payload.parameters.action == "MASS_GLACIER_RETRIEVAL") {
            return await this.executeMassGlacierRetrieval(inputs, outputs);
        }
        if (this.Payload.parameters.action == "GLACIER_RETRIEVAL_STATUS") {
            return await this.executeGlacierRetrievalStatus(inputs, outputs);
        }
        this.ReportProgress("Unknown command " + this.Payload.parameters.action)
        return ElvOAction.EXECUTION_EXCEPTION;
    };

    async executeCreateRemoteFile(inputs, outputs) {
        let tmpFile = "/tmp/"+ this.Payload.references.job_id + "__" + this.Payload.references.step_id;
        fs.writeFileSync(tmpFile, inputs.file_content);
        let stats = fs.statSync(tmpFile);
        outputs.file_size = stats.size;
        inputs.local_path = tmpFile;
        let uploadStatus = ElvOAction.EXECUTION_EXCEPTION;
        try {
             uploadStatus = await this.executeUploadFile(inputs, outputs);
        } catch(err) {
            this.ReportProgress("Failed to upload created file");
        }
        if (fs.existsSync(tmpFile)) {
            fs.unlinkSync(tmpFile);
        }
        return uploadStatus;
    };

    async executeUploadFile(inputs, outputs) {
        let s3Path =  (inputs.s3_file_path.match(/^s3:\/\//)) ? inputs.s3_file_path : ("s3://" + path.join(inputs.cloud_bucket, inputs.s3_file_path));
        let args = ["s3", "cp", inputs.local_path, s3Path];
        let cloudCredentials = {
            AWS_ACCESS_KEY_ID: inputs.cloud_access_key_id,
            AWS_SECRET_ACCESS_KEY: inputs.cloud_secret_access_key,
            AWS_DEFAULT_REGION :inputs.cloud_region
        };
        try {
            var outsideResolve;
            var outsideReject;
            var commandExecuted = new Promise(function(resolve, reject) {
                outsideResolve = resolve;
                outsideReject = reject;
            });
            this.reportProgress("aws args", args);
            var proc = spawn("/usr/local/bin/aws",  args, {env: cloudCredentials});
            
            let tracker = this;
            let lastReported;
            proc.stderr.on('data', function(data) {
                tracker.ReportProgress("Stderr " + data);
            });
            
            proc.stdout.setEncoding("utf8");
            proc.stdout.on('data', function(data) {
                let now = new Date().getTime();
                if (!lastReported || (lastReported + 5000 <  now)) {
                    tracker.ReportProgress(data.trim());
                    lastReported = now;
                }
                let matcher = data.trim().match(/upload: (.*) to (.*)/);
                if (matcher) {
                    outputs.remote_file_path = matcher[2];
                }
            });
            
            proc.on('close', function(executionCode) {
                outsideResolve(executionCode);
                tracker.ReportProgress("Command executed");
            });
            
            outputs.execution_code = await commandExecuted;
            if (outputs.execution_code == 0) {
                this.ReportProgress("Upload complete");
                return ElvOAction.EXECUTION_COMPLETE;
            } else {
                this.ReportProgress("Upload failed", outputs.execution_code);
                return ElvOAction.EXECUTION_EXCEPTION;
            }
        } catch(error) {
            this.Error("Execution failed", error)
            return ElvOAction.EXECUTION_EXCEPTION;
        }
        
    };

    async executeDownloadFile(inputs, outputs) {
        let s3Path =  (inputs.s3_file_path.match(/^s3:\/\//)) ? inputs.s3_file_path : ("s3://" + path.join(inputs.cloud_bucket, inputs.s3_file_path));
        let args = ["s3", "cp", s3Path, inputs.local_path];
        let cloudCredentials = {
            AWS_ACCESS_KEY_ID: inputs.cloud_access_key_id,
            AWS_SECRET_ACCESS_KEY: inputs.cloud_secret_access_key,
            AWS_DEFAULT_REGION :inputs.cloud_region
        };
        try {
            var outsideResolve;
            var outsideReject;
            var commandExecuted = new Promise(function(resolve, reject) {
                outsideResolve = resolve;
                outsideReject = reject;
            });
            this.reportProgress("aws args", args);
            var proc = spawn("/usr/local/bin/aws",  args, {env: cloudCredentials});
            
            let tracker = this;
            let lastReported;
            proc.stderr.on('data', function(data) {
                tracker.ReportProgress("Stderr " + data);
            });
            
            proc.stdout.setEncoding("utf8");
            proc.stdout.on('data', function(data) {
                let now = new Date().getTime();
                if (!lastReported || (lastReported + 5000 <  now)) {
                    tracker.ReportProgress(data.trim());
                    lastReported = now;
                }
                let matcher = data.trim().match(/download: (.*) to (.*)/);
                if (matcher) {
                    outputs.local_file_path = matcher[2];
                }
            });
            
            proc.on('close', function(executionCode) {
                outsideResolve(executionCode);
                tracker.ReportProgress("Command executed");
            });
            
            outputs.execution_code = await commandExecuted;
            if (outputs.execution_code == 0) {
                this.ReportProgress("Download complete");
                try {
                    let stats = fs.statSync(outputs.local_file_path);
                    outputs.file_size = stats.size;
                } catch(errSize) {
                    this.Error("Could not get file size for " + outputs.local_file_path, errSize);
                }
                return ElvOAction.EXECUTION_COMPLETE;
            } else {
                this.ReportProgress("Download failed", outputs.execution_code);
                return ElvOAction.EXECUTION_EXCEPTION;
            }
        } catch(error) {
            this.Error("Execution failed", error)
            return ElvOAction.EXECUTION_EXCEPTION;
        }
        
    };
    
    async executeInitiateGlacierRetrieval(inputs, outputs) {
        let tier = inputs.restore_tier || "Bulk";
        if (!inputs.cloud_bucket ) {
            let matcher = inputs.s3_file_path.match(/^s3:\/\/([^\/]+)\//);
            if (matcher) {
                inputs.cloud_bucket = matcher[1];
            } else {
                throw new Error("Bucket not specified");
            }
        }
        let s3key =  (!inputs.s3_file_path.match(/^s3:\/\//)) ? inputs.s3_file_path : inputs.s3_file_path.replace(/^s3:\/\//,"").replace(inputs.cloud_bucket+"/", "");
        let restoreRequest = {"Days": inputs.restore_for_days, "GlacierJobParameters":{"Tier":tier}};
        let args = ["s3api", "restore-object", "--bucket", inputs.cloud_bucket, "--key", s3key, "--restore-request", JSON.stringify(restoreRequest)];
        let cloudCredentials = {
            AWS_ACCESS_KEY_ID: inputs.cloud_access_key_id,
            AWS_SECRET_ACCESS_KEY: inputs.cloud_secret_access_key,
            AWS_DEFAULT_REGION :inputs.cloud_region
        };
        try {
            this.reportProgress("aws args", args);
            var proc = spawnSync("/usr/local/bin/aws",  args, {env: cloudCredentials});
            this.reportProgress("aws exec-status", proc.status);
            //this.Debug("stdout", proc.stdout.toString());
            //this.Debug("stderr", proc.stderr.toString());
            //this.Debug("error", proc.error);
            if (proc.status == 254) { //RestoreAlreadyInProgress
                this.ReportProgress("Restore already in progress", inputs.s3_file_path);
                outputs.ongoing_request = true;
                return ElvOAction.EXECUTION_FAILED;
            }
            let retrievalStatus = await this.executeGlacierRetrievalStatus(inputs, outputs);
            if (retrievalStatus == ElvOAction.EXECUTION_COMPLETE) { //item already available
                return ElvOAction.EXECUTION_FAILED;
            }
            if (outputs.ongoing_request) {
                return ElvOAction.EXECUTION_COMPLETE;
            }
            return ElvOAction.EXECUTION_EXCEPTION;
        } catch(error) {
            this.Error("Execution failed", error)
            return ElvOAction.EXECUTION_EXCEPTION;
        }
        
    };
    
    
    async massInitiateGlacierRetrieval(inputs, outputs) {
        let files = inputs.s3_file_paths;
        let execCodes = {};
        outputs.ongoing_requests = {};
        outputs.expiry_dates = {};
        outputs.storage_classes = {};
        for (let file of files) {
            let fileInputs = {
                s3_file_path: file,
                cloud_region: inputs.cloud_region,   
                cloud_access_key_id: inputs.cloud_access_key_id,
                cloud_secret_access_key: inputs.cloud_secret_access_key,
                cloud_bucket: inputs.cloud_bucket,     
                restore_for_days: inputs.restore_for_days
            };
            let fileOutputs = {};
            let execCode = await this.executeInitiateGlacierRetrieval(fileInputs, fileOutputs);
            execCodes[file] = execCode;
            outputs.ongoing_requests[file] = fileOutputs.ongoing_request;
            outputs.expiry_dates[file] = fileOutputs.expiry_date;
            outputs.storage_classes[file] = fileOutputs.storage_class;
        }
        return execCodes;
    };
    
    async executeMassInitiateGlacierRetrieval(inputs, outputs) {
        let execCodes = await this.massInitiateGlacierRetrieval(inputs, outputs)
        let codes = Object.values(execCodes);              
        if (codes.includes(ElvOAction.EXECUTION_EXCEPTION)) {
            this.ReportProgress("At least one item one exception occured while retrieving from Glacier");
            return ElvOAction.EXECUTION_EXCEPTION;
        }
        if (codes.includes(ElvOAction.EXECUTION_COMPLETE)) {
            this.ReportProgress("At least one item is still being retrieved from Glacier");
            return ElvOAction.EXECUTION_COMPLETE;
        }
        this.ReportProgress("All items already retrieved from Glacier");
        return ElvOAction.EXECUTION_FAILED;        
    };
    
    
    
    async executeGlacierRetrieval(inputs, outputs) {
        let initiate = await this.executeInitiateGlacierRetrieval(inputs, outputs);
        if  (initiate == ElvOAction.EXECUTION_COMPLETE) {
            this.ReportProgress("Glacier retrieval initiated",inputs.s3_file_path);
            return ElvOAction.EXECUTION_ONGOING;
        } 
        if  (initiate == ElvOAction.EXECUTION_FAILED) {
            if (outputs.ongoing_request) {
                this.ReportProgress("Glacier retrieval already in progress",inputs.s3_file_path);
                return ElvOAction.EXECUTION_ONGOING;
            } else {
                this.ReportProgress("Item already retrieved from Glacier",inputs.s3_file_path);
                return ElvOAction.EXECUTION_FAILED;
            }
        }
    };
    
    async executeMassGlacierRetrieval(inputs, outputs) {
        let execCodes = await this.massInitiateGlacierRetrieval(inputs, outputs);
        for (let file in execCodes) {
            let execCode = execCodes[file];
            if (execCode == ElvOAction.EXECUTION_EXCEPTION) {
                this.ReportProgress("At least one item one exception occured while retrieving from Glacier", file);
                return ElvOAction.EXECUTION_EXCEPTION;
            }
            if (execCode == ElvOAction.EXECUTION_COMPLETE) {
                this.ReportProgress("At least one item is being retrieved from Glacier", file);
                return ElvOAction.EXECUTION_ONGOING;
            }
            if  (execCode == ElvOAction.EXECUTION_FAILED) {
                if (outputs.ongoing_requests[file]) {
                    this.ReportProgress("Glacier retrieval already in progress", file);
                    return ElvOAction.EXECUTION_ONGOING;
                } else {
                    this.ReportProgress("Item already retrieved from Glacier", file);
                }
            }
        }       
        this.ReportProgress("All items already retrieved from Glacier");
        return ElvOAction.EXECUTION_FAILED;        
    };
    
    async MonitorExecution(pid, outputs) {   
        try {    
            if (ElvOAction.PidRunning(pid)) {
                return ElvOAction.EXECUTION_ONGOING;
            }
            
            if (this.Payload.parameters.action == "MASS_GLACIER_RETRIEVAL") {
                let inputs = this.Payload.inputs
                let files = inputs.s3_file_paths;
                let execCodes = {};
                for (let file of files) {
                    let fileInputs = {
                        s3_file_path: file,
                        cloud_region: inputs.cloud_region,   
                        cloud_access_key_id: inputs.cloud_access_key_id,
                        cloud_secret_access_key: inputs.cloud_secret_access_key,
                        cloud_bucket: inputs.cloud_bucket,     
                        restore_for_days: inputs.restore_for_days
                    };
                    let fileOutputs = {};
                    let execCode = await this.executeGlacierRetrievalStatus(fileInputs, fileOutputs);
                    execCodes[file] = execCode;
                }
                let codes = Object.values(execCodes);
                if (codes.includes(ElvOAction.EXECUTION_FAILED)) {
                    this.ReportProgress("At least one item one item retrieval from Glacier is still ongoing");
                    return ElvOAction.EXECUTION_ONGOING;
                }
                if (codes.includes(ElvOAction.EXECUTION_EXCEPTION)) {
                    this.ReportProgress("At least one item one exception occured while retrieving from Glacier");
                    return ElvOAction.EXECUTION_EXCEPTION;
                }
                this.ReportProgress("All files are available");
                return ElvOAction.EXECUTION_COMPLETE;
            } else {
                let statusCheck = await this.executeGlacierRetrievalStatus(this.Payload.inputs, outputs);
                if (statusCheck == ElvOAction.EXECUTION_FAILED) {
                    return ElvOAction.EXECUTION_ONGOING;
                }
                return statusCheck;
            }
        } catch(error) {
            this.Error("Monitoring failed", error)
            return ElvOAction.EXECUTION_EXCEPTION;
        }
    };
    
    
    
    async executeGlacierRetrievalStatus(inputs, outputs) {
        let s3key =  (!inputs.s3_file_path.match(/^s3:\/\//)) ? inputs.s3_file_path : inputs.s3_file_path.replace(/^s3:\/\//,"").replace(inputs.cloud_bucket+"/", "");
        let args = ["s3api", "head-object", "--bucket", inputs.cloud_bucket, "--key", s3key];
        let cloudCredentials = {
            AWS_ACCESS_KEY_ID: inputs.cloud_access_key_id,
            AWS_SECRET_ACCESS_KEY: inputs.cloud_secret_access_key,
            AWS_DEFAULT_REGION :inputs.cloud_region
        };
        try {
            this.reportProgress("aws args", args);
            var proc = spawnSync("/usr/local/bin/aws",  args, {env: cloudCredentials});
            let result;
            try {
                result = JSON.parse(proc.stdout.toString());
            } catch(errJSON) {
                this.Debug("Result received", proc.stdout.toString());
                throw errJSON;
            }
            outputs.storage_class = result.StorageClass;
            if (!outputs.storage_class &&  (result.ContentLength !=  null)) {
                outputs.storage_class = "NOT_ARCHIVED";
                this.ReportProgress("Item does not seeem to be archived");
                return ElvOAction.EXECUTION_COMPLETE;
            } 
            outputs.ongoing_request = (result.Restore && result.Restore.match(/ongoing-request=.true/) && true) || false;
            let matcher = result.Restore && result.Restore.match(/expiry-date=\"([^\"]+)/);
            if (matcher) {
                outputs.expiry_date = new Date(matcher[1]);
            }
            if ((outputs.storage_class == "DEEP_ARCHIVE") && !outputs.ongoing_request && (outputs.expiry_date > new Date())) {
                this.ReportProgress("Item was retrieved from archive");
                return ElvOAction.EXECUTION_COMPLETE;
            } 
            if ((outputs.storage_class == "DEEP_ARCHIVE") && outputs.ongoing_request) {
                this.ReportProgress("Item currently being retrieved from archive");
                return ElvOAction.EXECUTION_FAILED;
            } 
            if ((outputs.storage_class == "DEEP_ARCHIVE") && !result.Restore) {
                this.ReportProgress("Item is archived with no current request to retrieve");
                return ElvOAction.EXECUTION_EXCEPTION;
            } 
            if (outputs.storage_class == "ONEZONE_IA") {
                this.ReportProgress("Item is not archived");
                return ElvOAction.EXECUTION_COMPLETE;
            }
            if (outputs.storage_class == "GLACIER_IR") {
                this.ReportProgress("Restore is not required for downloading the data");
                return ElvOAction.EXECUTION_COMPLETE;
            }
            this.reportProgress("Unknown case", outputs);
            return ElvOAction.EXECUTION_EXCEPTION;
        } catch(error) {
            this.Error("Execution failed", error)
            return ElvOAction.EXECUTION_EXCEPTION;
        }
        
    };
    
    static VERSION = "0.2.0";
    static REVISION_HISTORY = {
        "0.0.1": "Initial release",
        "0.0.2": "Removed exessive logging",
        "0.0.3": "Errs if object being retrieved is not being retrieved",
        "0.0.4": "Adds some MASS operations",
        "0.0.5": "Normalizes logging",
        "0.0.6": "Adds treatment for non-archived objects",
        "0.0.7": "re-uses existing logic in status monitoring",
        "0.0.8": "Adds debugging for invalid JSON returned",
        "0.0.9": "Parameterizes the restore tier, default to bulk",
        "0.1.0": "Ensures bucket is specified one way or another",
        "0.1.1": "Adds support for GLACIER_IR storage_class",
        "0.2.0": "Adds create remote file and upload, also change API to have inputs provided to execute"
    };
}


if (ElvOAction.executeCommandLine(ElvAwsS3Operation)) {
    ElvOAction.Run(ElvAwsS3Operation);
} else {
    module.exports=ElvAwsS3Operation;
}
