
const ElvOAction = require("../o-action").ElvOAction;
const { execSync } = require('child_process');
const ElvOFabricClient = require("../o-fabric");
const fs = require("fs");
const mime = require("mime-types");
const Path = require('path');
const ElvOMutex = require("../o-mutex");



class ElvOActionManageFile extends ElvOAction  {
    
    ActionId() {
        return "manage_file";
    };
    
    Parameters() {
        return {"parameters": {aws_s3: {type: "boolean"}, action: {type: "string", values:["UPLOAD","DOWNLOAD","SED_TRANSFORM"]}, identify_by_version: {type: "boolean", required:false, default: false}}};
    };
    
    IOs(parameters) {
        
        if (parameters.action == "SED_TRANSFORM") {
            return {
                inputs: {
                    file_path: {type: "string", required:true},
                    sed_command: {type: "string", required:true},
                    target: {type: "string", required: true}
                },
                outputs: {target_file_path: "string"}
            };
        }
        
        let inputs = {
            private_key: {type: "password", required:false},
            config_url: {type: "string", required:false}
        }
        let outputs =  {}
        if (parameters.action == "UPLOAD") {
            inputs.files_path = {type: "array", required:true};
            inputs.target_flattening_base = {type:"string", require: false, default:null}; //null indicates flattening to basename, "" indicates no flattening, "/tmp/" would indicate "/tmp/ala/la.txt"->"ala/la.txt"
            inputs.encrypt = {type: "boolean", required: false, default: true};
            inputs.safe_update = {type: "boolean", required: false, default: false};
            if (!parameters.identify_by_version) {
                inputs.target_object_id = {type: "string", required: true};
            } else {
                inputs.target_object_version_hash = {type: "string", required: true};
            }
            if (parameters.aws_s3) {
                inputs.cloud_access_key_id = {type: "string", required:false};
                inputs.cloud_secret_access_key = {type: "password", required:false};
                inputs.cloud_crendentials_path = {type: "file", required:false};
                inputs.cloud_bucket = {type: "string", required:false};
                inputs.cloud_region = {type: "file", required:false};
                inputs.s3_copy = {type: "boolean", required:false, default: true};
            }
            outputs.modified_object_version_hash = "string";
            outputs.uploaded_files = "array";
        }
        
        if (parameters.action == "DOWNLOAD") {
            inputs.files_path = {type: "array", required:true};
            //inputs.target_flattening_base = {type:"string", require: false, default:null}; //null indicates flattening to basename, "" indicates no flattening, "/tmp/" would indicate "/tmp/ala/la.txt"->"ala/la.txt"
            inputs.decrypt = {type: "boolean", required: false, default: true};
            inputs.target = {type: "string", required: true};
            if (!parameters.identify_by_version) {
                inputs.source_object_id = {type: "string", required: true};
            } else {
                inputs.source_object_version_hash = {type: "string", required: true};
            }           
            outputs.target_files_path = "array";
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
    
    s3Path(path, bucket) {
        if (!bucket)  {
            bucket = this.Payload.inputs.cloud_bucket;
        }
        if (!path.match(bucket))  {
            return "s3://" +  Path.join(bucket, path);
        }
        return path;
    };
    
    releaseMutex() {
        if  (this.SetMetadataMutex) {
            ElvOMutex.ReleaseSync(this.SetMetadataMutex); 
            this.ReportProgress("Mutex released");
        }
    };
    
    async acquireMutex(objectId) {
        if  (this.Payload.inputs.safe_update) {
            this.ReportProgress("Reserving mutex");
            this.SetMetadataMutex = await ElvOMutex.WaitForLock({name: objectId, holdTimeout: 120000}); 
            this.ReportProgress("Mutex reserved", this.SetMetadataMutex);
            return this.SetMetadataMutex
        }
        return null;
    };
    
    async executeS3Upload(handle, outputs, client) {
        let inputs = this.Payload.inputs;
        
        let objectId = inputs.target_object_id;
        let versionHash = inputs.target_object_version_hash;
        if (!objectId && versionHash) {
            objectId = client.utils.DecodeVersionHash(versionHash).objectId;
        }
        let libraryId = await this.getLibraryId(objectId, client);
        let encrypted = inputs.encrypt;
        
        let files = inputs.files_path;
        let allFilesInfo = files.map(path => {
            return {
                path: this.flatten(path),
                type: "file",
                mime_type: mime.lookup(path),
                source: this.s3Path(path)
            };
        });
        await  this.acquireMutex(objectId);
        
        let writeToken = await this.getWriteToken({
            libraryId: libraryId,
            objectId: objectId,
            client
        });
        this.ReportProgress("Processing file(s) upload for " + objectId +"/"+ writeToken, allFilesInfo);
        
        let tracker = this;
        this.reportProgress("UploadFilesFromS3", {
            libraryId,
            objectId,
            writeToken,
            fileInfo: allFilesInfo,
            encryption: (!encrypted) ? "none" : "cgck",
            copy: inputs.s3_copy,
            region: inputs.cloud_region || "us-west-2",
            bucket: inputs.cloud_bucket,
            secret: inputs.cloud_secret_access_key,
            accessKey: inputs.cloud_access_key_id});
            
            await client.UploadFilesFromS3({
                libraryId,
                objectId,
                writeToken,
                fileInfo: allFilesInfo,
                encryption: (!encrypted) ? "none" : "cgck",
                copy: inputs.s3_copy,
                region: inputs.cloud_region || "us-west-2",
                bucket: inputs.cloud_bucket,
                secret: inputs.cloud_secret_access_key,
                accessKey: inputs.cloud_access_key_id,
                callback: progress => {   // callback { done: boolean, uploaded: number, total: number, uploadedFiles: number, totalFiles: number, fileStatus: Object }
                    if (progress.done) {
                        tracker.ReportProgress("Upload complete " + progress.uploadedFiles + " of " +progress.totalFiles + " files", progress.uploaded);
                    } else {
                        tracker.ReportProgress("Uploading " + progress.uploadedFiles + " of " +progress.totalFiles + " files", progress.uploaded);
                    }
                }
            });
            
            let msg =  (files.length > 1) ? "Uploaded " + files.length + " files" : "Uploaded file "+ Path.basename(files[0]);
            let response = await this.FinalizeContentObject({
                libraryId: libraryId,
                objectId: objectId,
                writeToken: writeToken,
                commitMessage:  msg,
                client
            });
            
            if (!response.hash) {
                this.ReportProgress("Failed to finalize update", response);
                throw Error("Failed to finalize update");
            }
            outputs.modified_object_version_hash = response.hash;
            outputs.uploaded_files = allFilesInfo.map(function(item){return item.path;});
            this.releaseMutex();
            this.ReportProgress("Upload complete", response.hash);
            return  ElvOAction.EXECUTION_COMPLETE;
            
        };
        
        async executeLocalUpload(handle, outputs, client) {
            let inputs = this.Payload.inputs;
            let objectId = inputs.target_object_id;
            let versionHash = inputs.target_object_version_hash;
            if (!objectId && versionHash) {
                objectId = client.utils.DecodeVersionHash(arg).objectId;
            }
            let libraryId = await this.getLibraryId(objectId, client);
            let encrypted = inputs.encrypt;
            let fileHandles = [];
            let files = inputs.files_path;
            outputs.uploaded_files = [];
            let fileInfo = files.map(path => { //TO_DO: get the files_path from the "file" input using "this.acquireFile"
                const fileDescriptor = fs.openSync(path, "r");
                fileHandles.push(fileDescriptor);
                const size = fs.fstatSync(fileDescriptor).size;
                const mimeType = mime.lookup(path);
                let targetPath = this.flatten(path)
                outputs.uploaded_files.push(targetPath);
                return {
                    path: targetPath,
                    type: "file",
                    mime_type: mimeType,
                    size: size,
                    data: fileDescriptor
                };
            });
            let reporter = this;
            ElvOAction.TrackerPath = this.TrackerPath;
            client.ToggleLogging(true, {log: reporter.Debug, error: reporter.Error});
            await  this.acquireMutex(objectId);
            let writeToken = await this.getWriteToken({
                libraryId: libraryId,
                objectId: objectId,
                client
            });
            this.ReportProgress("Processing file(s) upload for " + objectId, writeToken);
            let tracker = this;
            await client.UploadFiles({
                libraryId,
                objectId,
                writeToken,
                encryption: (!encrypted) ? "none" : "cgck",
                fileInfo,
                callback: progress => {
                    Object.keys(progress).sort().forEach(filename => {
                        const {uploaded, total} = progress[filename];
                        const percentage = total === 0 ? "100.0%" : (100 * uploaded / total).toFixed(1) + "%";
                        
                        //console.log(`${filename}: ${percentage}`);
                        tracker.ReportProgress("Uploading file(s)", `${filename}: ${percentage}`);
                    });
                }
            });
            
            // Close file handles
            fileHandles.forEach(descriptor => fs.closeSync(descriptor));
            let msg =  (files.length > 1) ? "Uploaded " + files.length + " files" : "Uploaded file "+ Path.basename(files[0]);
            let response = await this.FinalizeContentObject({
                libraryId: libraryId,
                objectId: objectId,
                writeToken: writeToken,
                commitMessage:  msg,
                client
            });
            
            if (!response.hash) {
                this.ReportProgress("Failed to finalize update", response);
                throw Error("Failed to finalize update");
            }
            outputs.modified_object_version_hash = response.hash;
            this.ReportProgress("Upload complete", response.hash);
            this.releaseMutex();
            return  ElvOAction.EXECUTION_COMPLETE;
        };
        
        async executeFabricDownload(inputs, outputs, client) {
            let objectId = inputs.source_object_id;
            let versionHash = inputs.source_object_version_hash;
            if (!objectId && versionHash) {
                objectId = client.utils.DecodeVersionHash(versionHash).objectId;
            }
            let libraryId = await this.getLibraryId(objectId, client);
            let tracker = this;
            outputs.target_files_path = [];
            let hasError= false;
            for (let filePath of inputs.files_path) {
                try {
                    this.ReportProgress("Initiating download of "+ filePath);
                    let rawBuffer = await  client.DownloadFile({
                        libraryId,
                        objectId,
                        versionHash,
                        filePath,
                        clientSideDecryption: inputs.decrypt,
                        callback: progress => {   // callback { done: boolean, uploaded: number, total: number, uploadedFiles: number, totalFiles: number, fileStatus: Object }
                            if (progress.done) {
                                tracker.ReportProgress(filePath + " download complete " + progress.bytesFinished );
                            } else {
                                tracker.ReportProgress("Downloading " +filePath +": " + progress.bytesFinished + " of " +progress.bytesTotal);
                            }
                        }
                    }); 
                    let targetPath;
                    if (fs.existsSync(inputs.target)) {
                        if  (fs.statSync(inputs.target).isDirectory()) {
                            targetPath = Path.join(inputs.target, Path.basename(filePath)); //copy into directory
                        } else {
                            targetPath = inputs.target; //overwrite
                        }
                    }  else {
                        targetPath = inputs.target; //create new
                    }
                    this.ReportProgress("Saving to "+ targetPath);
                    fs.writeFileSync(targetPath, Buffer.from(rawBuffer));
                    outputs.target_files_path.push(targetPath);
                } catch(errFile) {
                    this.Error("Could not download "+ filePath, errFile);
                    hasError = true;
                }
            }
            if (hasError) {
                this.ReportProgress("Not all files were downloaded");
                return ElvOAction.EXECUTION_EXCEPTION;
            } else {
                return ElvOAction.EXECUTION_COMPLETE;
            }
        };
        
        //sed -r 's/[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+/XX.XX.XX.XX/g' source.csv > target.csv
        async executeSedTransform(inputs, outputs) {
            let filePath = inputs.file_path;
            let targetPath;
            if (fs.existsSync(inputs.target)) {
                if  (fs.statSync(inputs.target).isDirectory()) {
                    targetPath = Path.join(inputs.target, Path.basename(filePath)); //copy into directory
                } else {
                    targetPath = inputs.target; //overwrite
                }
            }  else {
                targetPath = inputs.target; //create new
            }            
            this.ReportProgress("Target set", targetPath);
            let SedCmd = "sed " + inputs.sed_command.replace(/\\/g,"\\\\") + " \""+filePath+"\" > \""+targetPath+"\"";
            this.reportProgress("Command", SedCmd);
            let result = execSync(SedCmd).toString();
            this.reportProgress("Command executed", result);
            outputs.target_file_path = targetPath;
            return ElvOAction.EXECUTION_COMPLETE;                 
        };
        
        async Execute(handle, outputs) {
            let client;
            if (!this.Payload.inputs.private_key && !this.Payload.inputs.config_url){
                client = this.Client;
            } else {
                let privateKey = this.Payload.inputs.private_key || this.Client.signer.signingKey.privateKey.toString();
                let configUrl = this.Payload.inputs.config_url || this.Client.configUrl;
                client = await ElvOFabricClient.InitializeClient(configUrl, privateKey)
            }
            
            try {
                if (this.Payload.parameters.action == "UPLOAD") {
                    if (!this.Payload.parameters.aws_s3) {
                        return await this.executeLocalUpload(handle, outputs, client);
                    } else {
                        return await this.executeS3Upload(handle, outputs, client);
                    }
                }
                if (this.Payload.parameters.action == "DOWNLOAD") {
                    return await this.executeFabricDownload(this.Payload.inputs, outputs, client);
                }
                if (this.Payload.parameters.action == "SED_TRANSFORM") {
                    return await this.executeSedTransform(this.Payload.inputs, outputs);
                }
                
                throw "Unsupported action: " + this.Payload.parameters.action;
            } catch(err) {
                this.Error("Could not process" + this.Payload.parameters.action + " for " + this.Payload.inputs && (this.Payload.inputs.target_object_id || this.Payload.inputs.target_object_version_hash), err);
                this.releaseMutex();
                return ElvOAction.EXECUTION_EXCEPTION;
            }
        };
        
        
        static VERSION = "0.0.7";
        static REVISION_HISTORY = {
            "0.0.1": "Initial release",
            "0.0.2":"Adds support for uploads from S3",
            "0.0.3": "Private key input is encrypted",
            "0.0.4": "Use reworked finalize method",
            "0.0.5": "Adds flat download option",
            "0.0.6": "Adds option to only keep a reference in case of s3 upload",
            "0.0.7": "Adds support for sed transformation on local files"
        };
    }
    
    if (ElvOAction.executeCommandLine(ElvOActionManageFile)) {
        ElvOAction.Run(ElvOActionManageFile);
    } else {
        module.exports=ElvOActionManageFile;
    }
