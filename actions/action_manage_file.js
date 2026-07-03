
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
        return {
            parameters: {
                aws_s3: {type: "boolean"}, 
                cache_in_write_token: {type: "boolean", required:false, default: false},
                action: {type: "string", values:[
                    "UPLOAD", "DOWNLOAD", "SED_TRANSFORM", "DELETE", "JSON_PARSE", "LOCAL_DELETE", "LOCAL_MOVE", "JSON_STRINGIFY", "WRITE_TO_LOCAL_FILE"
                ]}, 
                identify_by_version: {type: "boolean", required:false, default: false}
            }
        };
    };
    
    IOs(parameters) {
        if (parameters.action == "WRITE_TO_LOCAL_FILE") {
            return {
                inputs: {
                    file_path: {type: "string", required:true},
                    file_content: {type: "string", required:true},
                    file_encoding: {type: "string", required:false, values: ['utf8', 'ascii', 'base64', 'hex']}, //utf8 if null
                    file_mode: {type: "numeric", required:false}, //0o666 (438) if null
                    file_flag: {type: "string", required:false, values: ["a", "w", "ax", "wx"]} 
                    //'a': Open for appending. Creates the file if it doesn't exist.
                    //'ax': Like 'a' but fails if the path exists.
                    //'wx': Like 'w' but fails if the path exists.
                },
                outputs: {
                    file_path: {type: "string"},
                    file_stats: {type: "object"}
                }
            };
        }
        if (parameters.action == "LOCAL_DELETE") {
            return {
                inputs: {
                    file_paths: {type: "array", required:true},
                },
                outputs: {deleted_files: "array"}
            };
        }
        if (parameters.action == "LOCAL_MOVE") {
            return {
                inputs: {
                    file_path: {type: "string", required: true},
                    target_path: {type: "string", required: true}
                },
                outputs: {moved_file_path: {type: "string"}}
            };
        }
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
        if (parameters.action == "JSON_PARSE") {
            return {
                inputs: {
                    file_path: {type: "string", required:true}
                },
                outputs: {value: "object"}
            };
        }
        if (parameters.action == "JSON_STRINGIFY") {
            return {
                inputs: {
                    value: {type: "object", required:true},
                    target_file_path: {type: "string", required:true},
                },
                outputs: {file_path: "string"}
            };
        }
        let inputs = {
            private_key: {type: "password", required:false},
            config_url: {type: "string", required:false}
        }
        let outputs =  {}
        if (parameters.action == "UPLOAD") {
            inputs.files_path = {type: "array", required: true};
            inputs.delete_source_after_completion = {type: "boolean", required:false, default: false};
            inputs.target_flattening_base = {type:"string", required: false, default:null}; //null indicates flattening to basename, "" indicates no flattening, "/tmp/" would indicate "/tmp/ala/la.txt"->"ala/la.txt"
            inputs.encrypt = {type: "boolean", required: false, default: true};
            inputs.safe_update = {type: "boolean", required: false, default: false};
            if (!parameters.identify_by_version) {
                inputs.target_object_id = {type: "string", required: true};
            } else {
                inputs.target_object_version_hash = {type: "string", required: true};
            }
            inputs.write_token = {type: "string", required: false, default: null};
            if (parameters.aws_s3) {
                inputs.cloud_access_key_id = {type: "string", required:false};
                inputs.cloud_secret_access_key = {type: "password", required:false};
                inputs.cloud_crendentials_path = {type: "file", required:false};
                inputs.cloud_bucket = {type: "string", required:false};
                inputs.cloud_region = {type: "file", required:false};
                inputs.s3_copy = {type: "boolean", required:false, default: true};
                inputs.use_s3_signed_url = {type: "boolean", required:false, default: false};
            }
            if (parameters.cache_in_write_token) {
                outputs.write_token = "string";
                outputs.config_url = "string";
            } else {
                outputs.modified_object_version_hash = "string";
            }
            outputs.uploaded_files = "array";
        }
        
        if (parameters.action == "DOWNLOAD") {
            inputs.files_path = {type: "array", required:true};
            //inputs.target_flattening_base = {type:"string", require: false, default:null}; //null indicates flattening to basename, "" indicates no flattening, "/tmp/" would indicate "/tmp/ala/la.txt"->"ala/la.txt"
            inputs.decrypt = {type: "boolean", required: false, default: true};
            inputs.target = {type: "string", required: true};
            if (!parameters.identify_by_version) {
                inputs.source_object_id = {type: "string", required: true};
                inputs.write_token = {type: "string", required: false};
            } else {
                inputs.source_object_version_hash = {type: "string", required: true};
            }           
            outputs.target_files_path = "array";
        }
        if (parameters.action == "DELETE") {
            inputs.files_path = {type: "array", required:true};
            //inputs.target_flattening_base = {type:"string", require: false, default:null}; //null indicates flattening to basename, "" indicates no flattening, "/tmp/" would indicate "/tmp/ala/la.txt"->"ala/la.txt"
            if (!parameters.identify_by_version) {
                inputs.source_object_id = {type: "string", required: true};
                inputs.write_token = {type: "string", required: false};
            } else {
                inputs.source_object_version_hash = {type: "string", required: true};
            }           
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
            this.SetMetadataMutex = await ElvOMutex.WaitForLock({name: objectId, holdTimeout: 3600000}); 
            this.ReportProgress("Mutex reserved", this.SetMetadataMutex);
            return this.SetMetadataMutex
        }
        return null;
    };
    
    async executeS3Upload(inputs, outputs, client) {
        if ((typeof inputs) =="string"){
            inputs = this.Payload.inputs;
        }
        
        let objectId = inputs.target_object_id;
        let versionHash = inputs.target_object_version_hash;
        if (!objectId && versionHash) {
            objectId = client.utils.DecodeVersionHash(versionHash).objectId;
        }
        let libraryId = await this.getLibraryId(objectId, client);
        let encrypted = inputs.encrypt;
        
        let files = inputs.files_path;
        let allFilesInfo;
        let s3SignedUrl = this.Payload.parameters.use_s3_signed_url || this.Payload.inputs.use_s3_signed_url ;
        if (!s3SignedUrl) {
            allFilesInfo = files.map(path => {
                return {
                    path: this.flatten(path),
                    type: "file",
                    mime_type: mime.lookup(path),
                    source: this.s3Path(path)
                };          
            });
        } else {
            console.log("Signed links", files);
            if (files.length != 1) {
                throw Error("Only implents singled signed link upload for now");
            }
            let s3Region,s3Bucket,s3Path;
            let matcher = files[0].match(/^https:\/\/s3\.([^\.]+)\.[^\/]+\/([^\/]+)\/(.*)\?/);
            if (!matcher) {
                matcher = files[0].match(/^https:\/\/([^\.]+)\.s3\.([^\.]+)\.[^\/]+\/(.*)\?/);
                if (matcher) {
                    s3Region = matcher[2];
                    s3Bucket = decodeURIComponent(matcher[1]); //bucket name should not have escaped characters, if it does use decodeURI(matcher[2])
                    s3Path = decodeURIComponent(matcher[3]);
                } else {
                    matcher = files[0].match(/^https:\/\/([^\/]+)\/([^\/]+)\/(.*)\?(.*)/);
                    s3Path = decodeURIComponent(matcher[3]);
                    s3Bucket = decodeURIComponent(matcher[2]);
                    s3Region = this.Payload.inputs["cloud_region"];
                }
            } else {
                s3Region = matcher[1];
                s3Bucket = decodeURIComponent(matcher[2]); //bucket name should not have escaped characters, if it does use decodeURI(matcher[2])
                s3Path = decodeURIComponent(matcher[3]);
            }
            let signedUrl = files[0];
            //let singleFileInfo = {path: fileInfo[i].path, source:fileInfo[i].path /*source: decodeURIComponent(Path.basename(s3Path))*/};//, source: "s3://"+s3Bucket+"/" + s3Path};
            let singleFileInfo = {
                path: decodeURI(Path.basename(files[0].replace(/^.*:\//,"").replace(/\?.*/,""))), 
                type: "file",
                source: decodeURIComponent(Path.basename(s3Path))
            };
            allFilesInfo = [singleFileInfo];
            if (!inputs.cloud_region) {
                inputs.cloud_region = s3Region;
            }
            
            //allFilesInfo = files.map(path => { 
            //    let original = path;                  
            //    return {
            //        path: decodeURI(Path.basename(path.replace(/^.*:\//,"").replace(/\?.*/,""))),
            //        type: "file",
            //        source: original
            //    };
            //});            
        }
        console.log("allFilesInfo", allFilesInfo);
        let writeToken = inputs.write_token;
        await  this.acquireMutex(objectId);     
        if (!writeToken) {   
            writeToken = await this.getWriteToken({
                libraryId: libraryId,
                objectId: objectId,
                client
            });
        }
        this.ReportProgress("Processing file(s) upload for " + objectId +"/"+ writeToken, allFilesInfo);
        
        let tracker = this;
        this.reportProgress("UploadFilesFromS3", {
            libraryId,
            objectId,
            writeToken,
            fileInfo: allFilesInfo,
            encryption: (!encrypted) ? "none" : "cgck",
            copy: inputs.s3_copy,
            region: inputs.cloud_region,
            bucket: inputs.cloud_bucket,
            secret: inputs.cloud_secret_access_key,
            accessKey: inputs.cloud_access_key_id,
            signedUrl: (s3SignedUrl && files[0]) || null
        });
        
        await client.UploadFilesFromS3({
            libraryId,
            objectId,
            writeToken,
            fileInfo: allFilesInfo,
            encryption: (!encrypted) ? "none" : "cgck",
            copy: inputs.s3_copy,
            region: inputs.cloud_region,
            bucket: inputs.cloud_bucket,
            secret: inputs.cloud_secret_access_key,
            accessKey: inputs.cloud_access_key_id,
            signedUrl: (s3SignedUrl && files[0]) || null,
            callback: progress => {   // callback { done: boolean, uploaded: number, total: number, uploadedFiles: number, totalFiles: number, fileStatus: Object }
                if (progress.done) {
                    tracker.ReportProgress("Upload complete " + progress.uploadedFiles + " of " +progress.totalFiles + " files", progress.uploaded);
                } else {
                    tracker.ReportProgress("Uploading " + progress.uploadedFiles + " of " +progress.totalFiles + " files", progress.uploaded);
                }
            }
        });
        
        let msg =  (files.length > 1) ? "Uploaded " + files.length + " files" : "Uploaded file "+ Path.basename(files[0]);
        if (!inputs.write_token && !this.Payload.parameters.cache_in_write_token) {                
            let response = await client.FinalizeContentObject({
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
        } else {
            this.ReportProgress(msg + " to write-token", writeToken);
            if (this.Payload.parameters.cache_in_write_token) {
                outputs.write_token = writeToken;
                if (client.HttpClient.draftURIs[writeToken]) {
                    outputs.node_url = "https://" + client.HttpClient.draftURIs[writeToken].hostname() + "/";
                    outputs.config_url = "https://" + client.HttpClient.draftURIs[writeToken].hostname() + "/config?self&qspace=main";
                }
            }
        }
        outputs.uploaded_files = allFilesInfo.map(function(item){return item.path;});
        this.releaseMutex();
        
        return  ElvOAction.EXECUTION_COMPLETE;
        
    };
    
    async executeLocalUpload(inputs, outputs, client) {
        inputs = this.Payload.inputs;
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
        let fileInfo = files.map(fileSpec => {
            let path, targetPath;
            if ((typeof fileSpec) == "string") {
                path = fileSpec;
                targetPath = this.flatten(path);
            } else {
                path = fileSpec.source;
                targetPath = fileSpec.target;
            }
            const fileDescriptor = fs.openSync(path, "r");
            fileHandles.push(fileDescriptor);
            const size = fs.fstatSync(fileDescriptor).size;
            const mimeType = mime.lookup(path);
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
        let writeToken = inputs.write_token;
        if (!writeToken) {
            await  this.acquireMutex(objectId);
            writeToken = await this.getWriteToken({
                libraryId: libraryId,
                objectId: objectId,
                client
            });
        }
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
        let msg =  (files.length > 1) ? "Uploaded " + files.length + " files" : "Uploaded file "+ Path.basename(files[0].source || files[0]);
        if (!inputs.write_token && !this.Payload.parameters.cache_in_write_token) {
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
        } else {
            this.ReportProgress("Upload to write-token complete", inputs.write_token);
            if (this.Payload.parameters.cache_in_write_token) {
                outputs.write_token = writeToken;
            }
            if (client.HttpClient.draftURIs[writeToken]) {
                outputs.node_url = "https://" + client.HttpClient.draftURIs[writeToken].hostname() + "/";
                outputs.config_url = "https://" + client.HttpClient.draftURIs[writeToken].hostname() + "/config?self&qspace=main";
            }
        }
        
        if (inputs.delete_source_after_completion) {
            for (let file of inputs.files_path) {
                this.ReportProgress("Deleting source after upload", file);
                fs.unlinkSync(file);
            }
        }           
        return  ElvOAction.EXECUTION_COMPLETE;
    };
    
    executeWriteToLocalFile(inputs, outputs) { //WRITE_TO_LOCAL_FILE
        let options = {};
        for (let field in ["encoding", "mode", "flag"]) {
            if (inputs["file_"+field]) options[field] = inputs["file_"+field];
        }
        fs.writeFileSync(inputs.file_path, inputs.file_content, options);
        outputs.file_path = inputs.file_path;
        outputs.file_stats = fs.lstatSync(inputs.file_path);
        return ElvOAction.EXECUTION_COMPLETE;
    }

    async executeFabricDownload(inputs, outputs, client) {
        let objectId = inputs.source_object_id;
        let versionHash = inputs.source_object_version_hash;
        let writeToken = inputs.write_token;
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
                var stream = fs.createWriteStream(targetPath, {flags:'w'});
                console.log("DownloadFile", {
                    libraryId,
                    objectId,
                    versionHash,
                    writeToken,
                    chunked: true,
                    filePath, targetPath});
                    let reporter = this;
                    //client.ToggleLogging(true, {log: reporter.Debug, error: reporter.Error}); 
                    await  client.DownloadFile({
                        libraryId,
                        objectId,
                        versionHash,
                        writeToken,
                        chunked: true,
                        filePath,
                        clientSideDecryption: inputs.decrypt,
                        callback: progress => {   // callback { done: boolean, uploaded: number, total: number, uploadedFiles: number, totalFiles: number, fileStatus: Object }
                            if (progress.done) {
                                tracker.ReportProgress(filePath + " download complete " + progress.bytesFinished );
                                stream.end();
                            } else {
                                tracker.ReportProgress("Downloading " +filePath +": " + progress.bytesFinished + " of " +progress.bytesTotal);
                                stream.write(Buffer.from(progress.chunk));
                            }
                        }
                    });             
                    
                    this.ReportProgress("Saved to "+ targetPath);
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
        
        async executeLocalDelete(inputs, outputs) {
            outputs.deleted_files = [];
            let errors = [];
            for (let filePath of inputs.file_paths) { //Not implementing GLOB-ing now
                try {
                    if (fs.existsSync(filePath)) {
                        this.reportProgress("Deleting", filePath); 
                        fs.rmSync(filePath, { recursive: true, force: true });  
                        outputs.deleted_files.push(filePath);
                    }
                } catch(errFile) {
                    this.Error("Error deleting "+ filePath, errFile);
                    errors.push(filePath);
                }
            }
            if (errors.length != 0) {
                return ElvOAction.EXECUTION_EXCEPTION;
            }
            if (outputs.deleted_files.length == 0) {
                return ElvOAction.EXECUTION_FAILED;
            }
            return ElvOAction.EXECUTION_COMPLETE;
        };
        
        async executeLocalMove(inputs, outputs) { //LOCAL_MOVE
            try {
                let targetPath;
                if (fs.existsSync(inputs.target_path) && fs.lstatSync(inputs.target_path).isDirectory()) {
                    targetPath = Path.join(inputs.target_path, Path.basename(inputs.file_path));
                } else {
                    targetPath = inputs.target_path;
                }
                this.reportProgress("Moving", inputs.file_path + " -> " + targetPath);
                fs.renameSync(inputs.file_path, targetPath);
                outputs.moved_file_path = targetPath;
                return ElvOAction.EXECUTION_COMPLETE;
            } catch(err) {
                this.Error("Error moving " + inputs.file_path, err);
                return ElvOAction.EXECUTION_EXCEPTION;
            }
        };

        async executeFabricDelete(inputs, outputs, client) {
            let objectId = inputs.source_object_id;
            let libraryId = await this.getLibraryId(objectId, client);
            let versionHash = inputs.source_object_version_hash;
            let writeToken = inputs.write_token;
            if (!objectId && versionHash) {
                objectId = client.utils.DecodeVersionHash(versionHash).objectId;
            }
            if (!writeToken) {
                writeToken = await this.getWriteToken({
                    client,
                    libraryId,
                    objectId,
                    versionHash
                });
            }
            let tracker = this;
            outputs.target_files_path = [];
            let hasError= false;
            for (let filePath of inputs.files_path) {
                try {
                    this.ReportProgress("Initiating deletion of "+ filePath);
                    
                    await  client.DeleteFiles({
                        libraryId,
                        objectId,
                        versionHash,
                        writeToken,
                        filePaths: [filePath]
                    });             
                    
                } catch(errFile) {
                    this.Error("Could not delete "+ filePath, errFile);
                    hasError = true;
                }
            }
            if (hasError) {
                this.ReportProgress("Not all files were deleted");
                return ElvOAction.EXECUTION_EXCEPTION;
            } 
            if (!inputs.write_token) {
                let result = await this.FinalizeContentObject({
                    client,
                    libraryId,
                    objectId,
                    versionHash,
                    writeToken,
                    commitMessage: "Completed file deletion"
                });
                if (result && result.hash) {
                    outputs.version_hash = result.hash;
                    return ElvOAction.EXECUTION_COMPLETE;
                }
                this.ReportProgress("Failed to finalize", result);
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
        
        async executeJSONParse(inputs, outputs) {
            let filePath = inputs.file_path;
            let content = fs.readFileSync(filePath);
            this.reportProgress("Content read");
            outputs.value = JSON.parse(content);                  
            this.ReportProgress("Content parsed into JSON");        
            return ElvOAction.EXECUTION_COMPLETE;                 
        };
        
        async executeJSONStringify(inputs, outputs) {
            let filePath = inputs.target_file_path;
            let valueStr = JSON.stringify(inputs.value, null, 2);  
            outputs.file_path = filePath;
            fs.writeFileSync(filePath, valueStr);            
            this.ReportProgress("Content written to file", filePath);        
            return ElvOAction.EXECUTION_COMPLETE;                
        };

        async Execute(inputs, outputs) {
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
                    if (this.Payload.inputs.files_path.length == 0) {
                        this.reportProgress("No files to upload");
                        outputs.uploaded_files = [];
                        return ElvOAction.EXECUTION_COMPLETE;
                    }
                    if (!this.Payload.parameters.aws_s3) {
                        return await this.executeLocalUpload(inputs, outputs, client);
                    } else {
                        console.log("Execute calling executeS3Upload");
                        return await this.executeS3Upload(inputs, outputs, client);
                    }
                }
                if (this.Payload.parameters.action == "WRITE_TO_LOCAL_FILE") {
                     return this.executeWriteToLocalFile(inputs, outputs);
                }
                if (this.Payload.parameters.action == "DOWNLOAD") {
                    return await this.executeFabricDownload(this.Payload.inputs, outputs, client);
                }
                if (this.Payload.parameters.action == "DELETE") {
                    return await this.executeFabricDelete(this.Payload.inputs, outputs, client);
                }
                if (this.Payload.parameters.action == "LOCAL_DELETE") {
                    return await this.executeLocalDelete(this.Payload.inputs, outputs, client);
                }
                if (this.Payload.parameters.action == "LOCAL_MOVE") {
                    return await this.executeLocalMove(this.Payload.inputs, outputs);
                }
                if (this.Payload.parameters.action == "SED_TRANSFORM") {
                    return await this.executeSedTransform(this.Payload.inputs, outputs);
                }
                if (this.Payload.parameters.action == "JSON_PARSE") {
                    return await this.executeJSONParse(this.Payload.inputs, outputs);
                }
                if (this.Payload.parameters.action == "JSON_STRINGIFY") {
                    return await this.executeJSONStringify(this.Payload.inputs, outputs);
                }
                throw "Unsupported action: " + this.Payload.parameters.action;
            } catch(err) {
                this.Error("Could not process" + this.Payload.parameters.action + " for " + this.Payload.inputs && (this.Payload.inputs.target_object_id || this.Payload.inputs.target_object_version_hash), err);
                this.releaseMutex();
                return ElvOAction.EXECUTION_EXCEPTION;
            }
        };
        
        static REVISION_HISTORY = {
            "0.0.1": "Initial release",
            "0.0.2":"Adds support for uploads from S3",
            "0.0.3": "Private key input is encrypted",
            "0.0.4": "Use reworked finalize method",
            "0.0.5": "Adds flat download option",
            "0.0.6": "Adds option to only keep a reference in case of s3 upload",
            "0.0.7": "Adds support for sed transformation on local files",
            "0.0.8": "Adds option to delete source after local upload",
            "0.0.9": "Allows local upload to write-token",
            "0.1.0": "Allows s3 upload to write-token",
            "0.1.1": "Allows to not finalize an upload to cache file into write-token, and allows upload from s3 signed URL",
            "0.1.2": "Adds a bypass to avoid upload marked as error if no files are to be uploaded",
            "0.1.3": "Adds option to delete a file",
            "0.1.4": "Adds JSON parsing option",
            "0.1.5": "Adds cache-in-writetoken option for local upload to match s3 upload functionalities",
            "0.1.6": "Adds a basic local delete without globbing capabilities",
            "0.1.7": "Adds action to stringify object to file",
            "0.1.8": "Adds option to rename files at target in local uploads",
            "0.1.9": "2026-05-08 - Adds LOCAL_MOVE action",
            "0.2.0": "2026-05-12 - Support local move into a folder without specifying full path"
        };
        static VERSION = "0.2.0a"; 
    }
    
    if (ElvOAction.executeCommandLine(ElvOActionManageFile)) {
        ElvOAction.Run(ElvOActionManageFile);
    } else {
        module.exports=ElvOActionManageFile;
    }
