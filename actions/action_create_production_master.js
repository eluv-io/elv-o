const ElvOAction = require("../o-action").ElvOAction;
const ElvOFabricClient = require("../o-fabric");

const mime = require("mime-types");
const fs = require("fs");
const Path = require('path');

class ElvOActionCreateProductionMaster extends ElvOAction  {
    
    Parameters() {
        return { "parameters": { 
            aws_s3: { type: "boolean", required: false, default: false }, 
            finalize_write_token: { type: "boolean", required: false, default: true } 
        } };
    };
    
    IOs(parameters) {
        let inputs = {
            production_masters_type: {type: "string", required: false},
            production_masters_lib: {type: "string", required: false},
            production_master_object_id: {type: "string", required: false, default: null},
            content_admins_group:	{type: "string", required:false, default:null},
            content_accessors_group: {type: "string", required: false, default: null},            
            ip_title_id: {type: "string", required: false},
            title: {type: "string", required: false},
            display_title: {type: "string", required: false},
            encrypt: {type: "boolean", required: false, default: false},
            metadata: {type: "object", required: false},
            name: {type: "string", required: false},
            master_source_file_paths: {type: "array", required: true},//"s3://dabucket-eluvio-dist/BABA1001HL.MXF",
            create_default_offering: {type: "boolean", required: false, default: true},
            private_key: {type: "password", required: false},
            config_url: {type: "string", required: false},
            write_token: {type: "string", required: false}            
        };
        if (parameters.aws_s3) {
            inputs.cloud_access_key_id = {type: "string", required:false};
            inputs.cloud_secret_access_key = {type: "password", required:false};
            inputs.cloud_crendentials_path = {type: "file", required:false};
            inputs.cloud_bucket = {type: "string", required:false};
            inputs.cloud_region = {type: "file", required:false};
            inputs.use_s3_signed_url = {type: "boolean", required:false, default: false};
            inputs.s3_copy = {type: "boolean", required:false, default: false};
        }
        
        let outputs = {
            production_master_object_id: {type: "string"},
            production_master_object_name: {type: "string"},
            production_master_version_hash: {type: "string"},
            errors: {type: "string"},
            warnings: {type: "string"},
            audio_found: {type: "boolean"},
            video_found: {type: "boolean"},
            write_token: {type: "string"},
            config_url: {type: "string"}
        };
        return { inputs : inputs, outputs: outputs };
    };
    
    ActionId() {
        return "create_production_master";
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
        let reporter = this;
        ElvOAction.TrackerPath = this.TrackerPath;
        client.ToggleLogging(true, {log: reporter.Debug, error: reporter.Error});
        let objectId = this.Payload.inputs.production_master_object_id;
        let library = this.Payload.inputs.production_masters_lib;
        let type = this.Payload.inputs.production_masters_type;
        let name = this.Payload.inputs.name;
        let ipTitleId = this.Payload.inputs.ip_title_id;
        let title = this.Payload.inputs.title;
        let displayTitle = this.Payload.inputs.display_title;
        let slug = this.Payload.inputs.slug;
        let metadata = this.Payload.inputs.metadata;
        let files = this.Payload.inputs.master_source_file_paths; 
        let encrypt = this.Payload.inputs.encrypt; //false ?
        let s3Copy = this.Payload.inputs.s3_copy;
        let s3Reference = !s3Copy && this.Payload.parameters.aws_s3;
        let s3SignedUrl = this.Payload.parameters.use_s3_signed_url || (this.Payload.parameters.aws_s3 && !this.Payload.inputs.cloud_access_key_id) ;
        let cloud_access_key_id = this.Payload.inputs.cloud_access_key_id;
        let cloud_secret_access_key = this.Payload.inputs.cloud_secret_access_key;
        let cloud_region = this.Payload.inputs.cloud_region;
        let cloud_bucket = this.Payload.inputs.cloud_bucket;
        let cloud_crendentials_path = this.Payload.inputs.cloud_crendentials_path;
        const input_write_token = this.Payload.inputs.write_token;
        const finalize_write_token = this.Payload.parameters.finalize_write_token
        if (cloud_crendentials_path) {
            let inputFileData = await this.acquireFile(cloud_crendentials_path);
            cloud_crendentials_path = inputFileData.location;
            await inputFileData.acquisition;
        }
        
        if (ipTitleId) {
            ipTitleId = ipTitleId.toString();
        }
        let access;
        if ((s3Reference || s3Copy) && !s3SignedUrl) {
            if (cloud_crendentials_path) {
                access = JSON.parse(fs.readFileSync(cloud_crendentials_path));
            } else {
                if (!cloud_region || !cloud_bucket || !cloud_access_key_id || !cloud_secret_access_key) {
                    this.Error("Missing required S3 environment variables: cloud_region, cloud_bucket, cloud_secret_access_key");
                    return -1
                }
                access = [
                    {
                        path_matchers: [".*"],
                        remote_access: {
                            protocol: "s3",
                            platform: "aws",
                            path: cloud_bucket + "/",
                            storage_endpoint: {
                                region: cloud_region
                            },
                            cloud_credentials: {
                                access_key_id: cloud_access_key_id,
                                secret_access_key: cloud_secret_access_key
                            }
                        }
                    }
                ];
            }
        }
        if (metadata) {
            try {
                if (((typeof metadata) == "string") && metadata.match(/^@/)) {
                    metadata = JSON.parse(fs.readFileSync(metadata.substring(1)));
                }
                if (!metadata.public) {
                    metadata.public = {};
                }
                
                name = name || metadata.public.name || metadata.name;
            } catch(error) {
                this.Error("Error parsing metadata: ", error);
                return -1;
            }
        } else {
            metadata = {public: {asset_metadata: {}}};
        }
        if (!title) {
            title = Path.basename(files[0]).replace(/\?.*/,"");
        }
        metadata.public.asset_metadata = {
            title,
            ...(metadata.public.asset_metadata || {})
        };
        if (ipTitleId) {
            metadata.public.asset_metadata.ip_title_id = ipTitleId;
        }
        if (displayTitle) {
            metadata.public.asset_metadata.displayTitle = displayTitle;
        }
        if (slug) {
            metadata.public.asset_metadata.slug = slug;
        }
        name = name || ((!ipTitleId) ? (title + " MASTER") : (ipTitleId + " - " + title + " MASTER"));
        let fileInfo;
        let fileHandles = [];
        if (access) {
            fileInfo = files.map(path => ({
                path: decodeURI(Path.basename(path)),
                source: path,
            }));
        } else {
            if (!s3SignedUrl) {
                fileInfo = files.map(path => { //TO_DO: get the files_path from the "file" input using "this.acquireFile"
                    const fileDescriptor = fs.openSync(path, "r");
                    fileHandles.push(fileDescriptor);
                    const size = fs.fstatSync(fileDescriptor).size;
                    const mimeType = mime.lookup(path) || "video/mp4";
                    
                    return {
                        path: Path.basename(path),
                        type: "file",
                        mime_type: mimeType,
                        size: size,
                        data: fileDescriptor
                    };
                });
            } else {
                fileInfo = files.map(path => {                   
                    return {
                        path: decodeURI(Path.basename(path.replace(/^.*:\//,"").replace(/\?.*/,""))),
                        type: "file",
                        source: path
                    };
                });
            }
        }
        this.ReportProgress("Creating Production Master");
        
        if (!objectId) {
            const originalType = type;
            if (type.startsWith("iq__")) {
                type = await client.ContentType({typeId: type});
            } else if(type.startsWith("hq__")) {
                type = await client.ContentType({versionHash: type});
            } else {
                type = await client.ContentType({name: type});
            }
            if (!type) {
                this.Error("Error: Unable to find content type ", originalType);
                return -1
            }
            type = type.hash;
        }
        
        try {
            let tracker = this;
            this.Debug("CreateProductionMaster", {
                objectId,
                libraryId: library,
                type,
                name,
                description: "Production Master for " + title,
                metadata,
                fileInfo,
                encrypt,
                access,
                s3SignedUrl,
                copy: s3Copy && !s3Reference,
                privateKey: this.Payload.inputs.private_key || this.Client.signer.signingKey.privateKey.toString(),
                configUrl: this.Payload.inputs.config_url 
            });
            const {errors, warnings, id, write_token,write_token_config_url, hash} = await this.CreateProductionMaster({
                objectId,
                libraryId: library,
                type,
                name,
                description: "Production Master for " + title,
                metadata,
                fileInfo,
                encrypt,
                access,
                s3SignedUrl,
                copy: s3Copy && !s3Reference,
                callback: progress => {
                    if (access) {
                        //console.log(progress);
                        tracker.ReportProgress("Preparing master from S3",progress);
                    } else {
                        //console.log();
                        Object.keys(progress).sort().forEach(filename => {
                            const {uploaded, total} = progress[filename];
                            const percentage = total === 0 ? "100.0%" : (100 * uploaded / total).toFixed(1) + "%";
                            
                            //console.log(`${filename}: ${percentage}`);
                            tracker.ReportProgress("Uploading master files",`${filename}: ${percentage}`);
                        });
                    }
                },
                input_write_token,
                finalize_write_token,
                client
            });
            
            // Close file handles
            fileHandles.forEach(descriptor => fs.closeSync(descriptor));
            //tracker.ReportProgress("Setting visibility to 0");
            //await client.SetVisibility({id, visibility: 0});
            
            outputs.production_master_object_id = id;
            outputs.production_master_version_hash = hash;
            outputs.production_master_object_name = name;
            outputs.write_token = write_token
            outputs.config_url = write_token_config_url
            if (errors.length > 0) {
                outputs.errors = errors.join("\n");
            }
            if (warnings.length) {
                outputs.warnings = warnings.join("\n");
            }

            await this.grantRights(this.Payload.inputs.content_admins_group, client, id, 2);
            await this.grantRights(this.Payload.inputs.content_accessors_group, client, id, 1);

            if (this.Payload.inputs.create_default_offering) {
                // Check if resulting variant has an audio and video stream
                tracker.ReportProgress("Check if resulting variant has an audio and video stream");
                const streams = (await this.getMetadata({
                    libraryId: library,
                    objectId: id,
                    versionHash: hash,
                    metadataSubtree: "production_master/variants/default/streams",
                    writeToken: write_token,
                    node_url: write_token_config_url,
                    client
                }));
                outputs.audio_found = streams && (streams.hasOwnProperty("audio") || streams.hasOwnProperty("stereo"));
                outputs.video_found = streams && streams.hasOwnProperty("video");
                
                if (streams) {
                    if (!outputs.audio_found || !outputs.video_found) {
                        this.ReportProgress("An audio and a video stream must be present");
                        return ElvOAction.EXECUTION_FAILED;
                    } else {
                        this.ReportProgress("Master read with both an audio and a video stream");
                        return ElvOAction.EXECUTION_COMPLETE;
                    }
                } else {
                    this.ReportProgress("No streams found");
                    return ElvOAction.EXECUTION_EXCEPTION;
                }
            } else {
                return ElvOAction.EXECUTION_COMPLETE;
            }
        } catch(error) {
            this.Error("Error encountered during execution", error);
            return  ElvOAction.EXECUTION_EXCEPTION;
        }
    };
    
    
    
    async grantAdminRights(client, objectId) {
        let attempt = 0;
        let groupAddress = this.Payload.inputs.content_admins_group
        if (groupAddress) {
            let objAddress = client.utils.HashToAddress(objectId);
            while (attempt < 5)  {
                attempt ++;
                await this.CallContractMethodAndWait({
                    contractAddress: groupAddress,
                    methodName: "setContentObjectRights",
                    methodArgs: [objAddress, 2, 1], //EDIT rights
                    client
                });
                let hasRights = await client.CallContractMethod({
                    contractAddress: groupAddress,
                    methodName: "checkDirectRights",
                    methodArgs: [1, objAddress, 2]
                });
                if (hasRights) {
                    this.reportProgress("Granted admin rights to group " + groupAddress);        
                    return true;
                } else {
                    this.reportProgress("Failed to grant admin rights to group "+ groupAddress, attempt); 
                    await this.sleep(100);
                }
            }
            throw Error("Could not grant rights to " + groupAddress);       
        } 
        return false;
    };
    async grantRights(groupAddress, client, objectId, rightType /*1:access, 2:edit*/) {
        let attempt = 0;
        if (groupAddress) {
            let objAddress = client.utils.HashToAddress(objectId);
            while (attempt < 5)  {
                attempt ++;
                await this.CallContractMethodAndWait({
                    contractAddress: groupAddress,
                    methodName: "setContentObjectRights",
                    methodArgs: [objAddress, rightType, 1], //EDIT rights
                    client
                });
                let hasRights = await client.CallContractMethod({
                    contractAddress: groupAddress,
                    methodName: "checkDirectRights",
                    methodArgs: [1, objAddress, rightType]
                });
                if (hasRights) {
                    this.reportProgress("Granted admin rights to group " + groupAddress);        
                    return true;
                } else {
                    this.reportProgress("Failed to grant admin rights to group "+ groupAddress, attempt); 
                    await this.sleep(100);
                }
            }
            throw Error("Could not grant rights to " + groupAddress);       
        } 
        return false;
    };
    
    async CreateProductionMaster({
        libraryId,
        objectId,
        type,
        name,
        description,
        metadata={},
        fileInfo,
        encrypt=false,
        access=[],
        copy=false,
        s3SignedUrl=false,
        callback,
        input_write_token,
        finalize_write_token,
        client
    }) {
        if (!client) {
            client = this.Client;
        }
        //client.ValidateLibrary(libraryId);
        let id, write_token, write_token_config_url;
        if (objectId) {
            id = objectId;
            if (!libraryId) {
                libraryId = await this.getLibraryId(objectId, client);
            }
            if (input_write_token){
                write_token = input_write_token
                write_token_config_url = client.configUrl
            }else {
                respObject = await this.editContentObject({
                    objectId,
                    libraryId,
                    client
                });
                write_token = respObject.write_token
                write_token_config_url = respObject.nodeUrl
            }
            this.reportProgress("Re-using existing master object", id);
            this.reportProgress("Write Token", write_token);
        } else {
            let resCreate = await this.CreateContentObject({
                libraryId,
                options: type ? { type } : {},
                client
            });
            id = resCreate.objectId; // in resCreate id and objectId are the same value. Let's use objectId since it's clearer
            // ADM - In this case, since the object gets created, we cannot reuse the write token in input
            if (inputs.write_token){
                this.Info("WriteToken specified in input cannot be used, since the step creates a new content object")
            }
            write_token = resCreate.write_token;
            write_token_config_url = resCreate.nodeUrl
            this.reportProgress("Created object", id);
        }
        // any files specified?
        if (fileInfo) {
            // are they stored in cloud?
            if (!s3SignedUrl) {
                if(access.length > 0) {
                    // S3 Upload
                    const s3prefixRegex = /^s3:\/\/([^/]+)\//i; // for matching and extracting bucket name when full s3:// path is specified
                    // batch the cloud storage files by matching credential set, check each file's source path against credential set path_matchers
                    for(let i = 0; i < fileInfo.length; i++) {
                        const oneFileInfo = fileInfo[i];
                        let matched = false;
                        for(let j = 0; !matched && j < access.length; j++) {
                            let credentialSet = access[j];
                            // strip trailing slash to get bucket name for credential set
                            const credentialSetBucket = credentialSet.remote_access.path.replace(/\/$/, "");
                            const matchers = credentialSet.path_matchers;
                            for(let k = 0; !matched && k < matchers.length; k++) {
                                const matcher = new RegExp(matchers[k]);
                                const fileSourcePath = oneFileInfo.source;
                                if(matcher.test(fileSourcePath)) {
                                    matched = true;
                                    // if full s3 path supplied, check bucket name
                                    const s3prefixMatch = (s3prefixRegex.exec(fileSourcePath));
                                    if(s3prefixMatch) {
                                        const bucketName = s3prefixMatch[1];
                                        if(bucketName !== credentialSetBucket) {
                                            throw Error("Full S3 file path \"" + fileSourcePath + "\" matched to credential set with different bucket name '" + credentialSetBucket + "'");
                                        }
                                    }
                                    if(credentialSet.hasOwnProperty("matched")) {
                                        credentialSet.matched.push(oneFileInfo);
                                    } else {
                                        // first matching file path for this credential set,
                                        // initialize new 'matched' property to 1-element array
                                        credentialSet.matched = [oneFileInfo];
                                    }
                                }
                            }
                        }
                        if(!matched) {
                            throw Error("no credential set found for file path: \"" + filePath + "\"");
                        }
                    }
                    // iterate over credential sets, if any matching files were found, upload them using that credential set
                    for(let i = 0; i < access.length; i++) {
                        const credentialSet = access[i];
                        if(credentialSet.hasOwnProperty("matched") && credentialSet.matched.length > 0) {
                            const region = credentialSet.remote_access.storage_endpoint.region;
                            const bucket = credentialSet.remote_access.path.replace(/\/$/, "");
                            const accessKey = credentialSet.remote_access.cloud_credentials.access_key_id;
                            const secret = credentialSet.remote_access.cloud_credentials.secret_access_key;
                            await client.UploadFilesFromS3({
                                libraryId,
                                objectId: id,
                                writeToken: write_token,
                                fileInfo: credentialSet.matched,
                                region,
                                bucket,
                                accessKey,
                                secret,
                                copy,
                                callback,
                                encryption: encrypt ? "cgck" : "none"
                            });
                        }
                    }
                } else {
                    await client.UploadFiles({
                        libraryId,
                        objectId: id,
                        writeToken: write_token,
                        fileInfo,
                        callback,
                        encryption: encrypt ? "cgck" : "none"
                    });
                }
            } else {
                /*if (!copy) {
                    throw new Error("s3 signed link can only be used as copy")
                }*/
                let assetsUploaded = [];               
                for (let i=0 ; i < fileInfo.length; i++) {
                    let s3Region,s3Bucket,s3Path;
                    let matcher = fileInfo[i].source.match(/^https:\/\/s3\.([^\.]+)\.[^\/]+\/([^\/]+)\/(.*)\?/);
                    if (!matcher) {
                        matcher = fileInfo[i].source.match(/^https:\/\/([^\.]+)\.s3\.([^\.]+)\.[^\/]+\/(.*)\?/);
                        if (matcher) {
                            s3Region = matcher[2];
                            s3Bucket = decodeURIComponent(matcher[1]); //bucket name should not have escaped characters, if it does use decodeURI(matcher[2])
                            s3Path = decodeURIComponent(matcher[3]);
                        } else {
                            matcher = fileInfo[i].source.match(/^https:\/\/([^\/]+)\/([^\/]+)\/(.*)\?(.*)/);
                            s3Path = decodeURIComponent(matcher[3]);
                            s3Bucket = decodeURIComponent(matcher[2]);
                            s3Region = this.Payload.inputs["cloud_region"];
                        }
                    } else {
                        s3Region = matcher[1];
                        s3Bucket = decodeURIComponent(matcher[2]); //bucket name should not have escaped characters, if it does use decodeURI(matcher[2])
                        s3Path = decodeURIComponent(matcher[3]);
                    }
                    let signedUrl = fileInfo[i].source;
                    //let singleFileInfo = {path: fileInfo[i].path, source:fileInfo[i].path /*source: decodeURIComponent(Path.basename(s3Path))*/};//, source: "s3://"+s3Bucket+"/" + s3Path};
                    let singleFileInfo = {path: fileInfo[i].path, source: decodeURIComponent(Path.basename(s3Path))};
                    
                    let assetPath = Path.basename(singleFileInfo.path);
                    
                    if (!copy) {
                        access =  [{
                            path_matchers: [".*"],
                            remote_access: {
                                protocol: "s3",
                                platform: "aws",
                                //path: s3Bucket +"/",
                                storage_endpoint: {
                                    region: s3Region
                                },
                                cloud_credentials: {
                                    signed_url: signedUrl
                                }
                            }
                        }];
                    }
                    
                    try {
                        this.ReportProgress("Uploading file "+ assetPath);
                        let reporter = this;
                        this.reportProgress("UploadFilesFromS3",  {
                            objectId: id, 
                            libraryId, 
                            writeToken: write_token,
                            fileInfo: [singleFileInfo],
                            encryption: encrypt ? "cgck" : "none",
                            copy,
                            region: s3Region,
                            bucket: s3Bucket,
                            signedUrl: signedUrl
                        });
                        await client.UploadFilesFromS3({
                            objectId: id, 
                            libraryId, client,
                            writeToken: write_token,
                            fileInfo: [singleFileInfo],
                            encryption: encrypt ? "cgck" : "none",
                            copy,
                            region: s3Region,
                            //bucket: s3Bucket,
                            signedUrl: signedUrl,
                            callback: function(stats){reporter.ReportProgress("Uploading ... ", stats);}
                        });
                        assetsUploaded.push(assetPath);
                    } catch(errorS3) {
                        this.Error("Could not upload "+ assetPath +" from s3 signed link", errorS3);
                        throw errorS3;                       
                    }
                }
            }
        }
        await this.CreateEncryptionConk({libraryId, objectId: id, writeToken: write_token, createKMSConk: true, client});
        this.ReportProgress("Initiating probing of files");
        this.reportProgress("CallBitcodeMethod", {
            libraryId,
            objectId: id,
            writeToken: write_token,
            nodeUrl: write_token_config_url,
            method: "media/production_master/init",
            body: {
                access
            },
            constant: false
        });
        const { logs, errors, warnings } = await client.CallBitcodeMethod({
            libraryId,
            objectId: id,
            writeToken: write_token,
            nodeUrl: write_token_config_url,
            method: "media/production_master/init",
            body: {
                access
            },
            constant: false
        });
        this.ReportProgress("Completed probing of files");
        
        /*
        //read metadata production_master  section from write_token -  note  required, just for debugging
        let masterSection  = await this.getMetadata({
            client,
            libraryId,
            objectId: id,
            writeToken: write_token,
            metadataSubtree: "production_master"
        });
        this.reportProgress("metadata production_master section from write_token", masterSection);
        */
        
        await client.MergeMetadata({
            libraryId,
            objectId: id,
            writeToken: write_token,
            nodeUrl: write_token_config_url,
            metadata: {
                ...(metadata || {}),
                name,
                description,
                reference: access && !copy,
                public: {
                    ...((metadata || {}).public || {}),
                    name: name || "",
                    description: description || ""
                },
                elv_created_at: new Date().getTime(),
            }
        });
        this.ReportProgress("Merged probing info into metadata");
        if ((finalize_write_token == null) || (finalize_write_token == true)) {
            const finalizeResponse = await this.FinalizeContentObject({
                libraryId,
                objectId: id,
                writeToken: write_token,
                nodeUrl: write_token_config_url,
                commitMessage: (objectId) ? "Repurpose existing master" : "Create master",
                awaitCommitConfirmation: true, //flip to true on 07/04/2025 to avoid race condition when looking for the result of probe
                client
            });
            this.ReportProgress("Finalized production master object", finalizeResponse);
            return {
                errors: errors || [],
                logs: logs || [],
                warnings: warnings || [],
                write_token: write_token,
                write_token_config_url: write_token_config_url,
                ...finalizeResponse
            };

        }
        return {
            errors: errors || [],
            logs: logs || [],
            warnings: warnings || [],
            write_token: write_token,
            write_token_config_url: write_token_config_url,            
        };
    };
    
    
    async CreateProductionMasterMarc({
        libraryId,
        objectId,
        type,
        name,
        description,
        metadata={},
        fileInfo,
        encrypt=false,
        access=[],
        copy=false,
        s3SignedUrl=false,
        callback,
        client
    }) {
        if (!client) {
            client = this.Client;
        }
        //client.ValidateLibrary(libraryId);
        let id, write_token;
        if (objectId) {
            id = objectId;
            if (!libraryId) {
                libraryId = await this.getLibraryId(objectId, client);
            }
            write_token = await this.getWriteToken({
                objectId,
                libraryId,
                client
            });
            this.reportProgress("Re-using existing master object", id);
        } else {
            let resCreate = await this.CreateContentObject({
                libraryId,
                options: type ? { type } : {},
                client
            });
            id = resCreate.id;
            write_token = resCreate.write_token;
            this.reportProgress("Created object", id);
        }
        // any files specified?
        
        if (fileInfo) {
            // are they stored in cloud?
            if (!s3SignedUrl) {
                if(access.length > 0) {
                    // S3 Upload
                    const s3prefixRegex = /^s3:\/\/([^/]+)\//i; // for matching and extracting bucket name when full s3:// path is specified
                    // batch the cloud storage files by matching credential set, check each file's source path against credential set path_matchers
                    for(let i = 0; i < fileInfo.length; i++) {
                        const oneFileInfo = fileInfo[i];
                        let matched = false;
                        for(let j = 0; !matched && j < access.length; j++) {
                            let credentialSet = access[j];
                            // strip trailing slash to get bucket name for credential set
                            const credentialSetBucket = credentialSet.remote_access.path.replace(/\/$/, "");
                            const matchers = credentialSet.path_matchers;
                            for(let k = 0; !matched && k < matchers.length; k++) {
                                const matcher = new RegExp(matchers[k]);
                                const fileSourcePath = oneFileInfo.source;
                                if(matcher.test(fileSourcePath)) {
                                    matched = true;
                                    // if full s3 path supplied, check bucket name
                                    const s3prefixMatch = (s3prefixRegex.exec(fileSourcePath));
                                    if(s3prefixMatch) {
                                        const bucketName = s3prefixMatch[1];
                                        if(bucketName !== credentialSetBucket) {
                                            throw Error("Full S3 file path \"" + fileSourcePath + "\" matched to credential set with different bucket name '" + credentialSetBucket + "'");
                                        }
                                    }
                                    if(credentialSet.hasOwnProperty("matched")) {
                                        credentialSet.matched.push(oneFileInfo);
                                    } else {
                                        // first matching file path for this credential set,
                                        // initialize new 'matched' property to 1-element array
                                        credentialSet.matched = [oneFileInfo];
                                    }
                                }
                            }
                        }
                        if(!matched) {
                            throw Error("no credential set found for file path: \"" + filePath + "\"");
                        }
                    }
                    // iterate over credential sets, if any matching files were found, upload them using that credential set
                    for(let i = 0; i < access.length; i++) {
                        const credentialSet = access[i];
                        if(credentialSet.hasOwnProperty("matched") && credentialSet.matched.length > 0) {
                            const region = credentialSet.remote_access.storage_endpoint.region;
                            const bucket = credentialSet.remote_access.path.replace(/\/$/, "");
                            const accessKey = credentialSet.remote_access.cloud_credentials.access_key_id;
                            const secret = credentialSet.remote_access.cloud_credentials.secret_access_key;
                            await client.UploadFilesFromS3({
                                libraryId,
                                objectId: id,
                                writeToken: write_token,
                                fileInfo: credentialSet.matched,
                                region,
                                bucket,
                                accessKey,
                                secret,
                                copy,
                                callback,
                                encryption: encrypt ? "cgck" : "none"
                            });
                        }
                    }
                    
                    //matched not needed anymore
                    for (let entry of access) {
                        delete entry.matched;
                    }
                } else {
                    await client.UploadFiles({
                        libraryId,
                        objectId: id,
                        writeToken: write_token,
                        fileInfo,
                        callback,
                        encryption: encrypt ? "cgck" : "none"
                    });
                }
            } else {
                //if (!copy) {
                //throw new Error("s3 signed link can only be used as copy")
                //}
                let assetsUploaded = [];               
                for (let i=0 ; i < fileInfo.length; i++) {
                    let s3Region,s3Bucket,s3Path;
                    let matcher = fileInfo[i].source.match(/^https:\/\/s3\.([^\.]+)\.[^\/]+\/([^\/]+)\/(.*)\?/);
                    if (!matcher) {
                        matcher = fileInfo[i].source.match(/^https:\/\/([^\.]+)\.s3\.([^\.]+)\.[^\/]+\/(.*)\?/);
                        s3Region = matcher[2];
                        s3Bucket = matcher[1]; //bucket name should not have escaped characters, if it does use decodeURI(matcher[2])
                        s3Path = decodeURI(matcher[3]);
                    } else {
                        s3Region = matcher[1];
                        s3Bucket = matcher[2]; //bucket name should not have escaped characters, if it does use decodeURI(matcher[2])
                        s3Path = decodeURI(matcher[3]);
                    }
                    let signedUrl = fileInfo[i].source;
                    let singleFileInfo = {"path": fileInfo[i].path, "source": decodeURIComponent(Path.basename(s3Path))};//, source: "s3://"+s3Bucket+"/" + s3Path};
                    let assetPath = Path.basename(singleFileInfo.path);
                    try {
                        this.ReportProgress("Uploading file "+ assetPath);
                        let reporter = this;
                        await client.UploadFilesFromS3({
                            objectId: id, 
                            libraryId, client,
                            writeToken: write_token,
                            fileInfo: [singleFileInfo],
                            encryption: encrypt ? "cgck" : "none",
                            copy,
                            region: s3Region,
                            bucket: s3Bucket,
                            signedUrl: signedUrl,
                            callback: function(stats){reporter.ReportProgress("Uploading ... ", stats);}
                        });
                        assetsUploaded.push(assetPath);
                    } catch(errorS3) {
                        this.Error("Could not upload "+ assetPath +" from s3 signed link", errorS3);
                        throw errorS3;                       
                    }
                }
            }
        }
        await this.CreateEncryptionConk({libraryId, objectId: id, writeToken: write_token, createKMSConk: true, client});
        this.ReportProgress("Initiating probing of files");
        
        if (fileInfo) {
            let probeOutputs = {};
            for (let fileDesc of fileInfo) {
                let fileToProbe = fileDesc.path;
                await this.probeSource({client, objectId: id, libraryId, access, fileToProbe, writeToken: write_token}, probeOutputs)
                await  client.ReplaceMetadata({
                    libraryId: libraryId,
                    objectId: id,
                    metadataSubtree: "production_master/sources/"+fileToProbe,
                    metadata: probeOutputs.probe[fileToProbe],
                    writeToken: write_token, 
                    client
                });
            }
            if (this.Payload.inputs.create_default_offering) {
                await  client.ReplaceMetadata({
                    libraryId: libraryId,
                    objectId: id,
                    metadataSubtree: "production_master/variants/default/streams",
                    metadata: probeOutputs.default_variant_streams,
                    writeToken: write_token, 
                    client
                });
            }
            this.ReportProgress("Completed probing of files");
            await client.MergeMetadata({
                libraryId,
                objectId: id,
                writeToken: write_token,
                metadata: {
                    ...(metadata || {}),
                    name,
                    description,
                    reference: access && !copy,
                    public: {
                        ...((metadata || {}).public || {}),
                        name: name || "",
                        description: description || ""
                    },
                    elv_created_at: new Date().getTime(),
                }
            });
            this.ReportProgress("Merged probing info into metadata");
            const finalizeResponse = await this.FinalizeContentObject({
                libraryId,
                objectId: id,
                writeToken: write_token,
                commitMessage: (objectId) ? "Repurpose existing master" : "Create master",
                awaitCommitConfirmation: false,
                client
            });
            this.ReportProgress("Finalized production master object", finalizeResponse);
            let errors  = [];
            let logs = [];
            let warnings = [];
            for (let file in probeOutputs.probe) {
                errors = errors.concat(probeOutputs.probe_errors[file] || []);
                logs = logs.concat(probeOutputs.probe_logs[file] || []);
                warnings = warnings.concat(probeOutputs.probe_warnings[file] || []);
            }
            return {
                errors: errors || [],
                logs: logs || [],
                warnings: warnings || [],
                ...finalizeResponse
            };
        } else {
            const { logs, errors, warnings } = await client.CallBitcodeMethod({
                libraryId,
                objectId: id,
                writeToken: write_token,
                method: "media/production_master/init",
                body: {
                    access
                },
                constant: false
            });
            this.ReportProgress("Completed probing of files");
            await client.MergeMetadata({
                libraryId,
                objectId: id,
                writeToken: write_token,
                metadata: {
                    ...(metadata || {}),
                    name,
                    description,
                    reference: access && !copy,
                    public: {
                        ...((metadata || {}).public || {}),
                        name: name || "",
                        description: description || ""
                    },
                    elv_created_at: new Date().getTime(),
                }
            });
            this.ReportProgress("Merged probing info into metadata");
            const finalizeResponse = await this.FinalizeContentObject({
                libraryId,
                objectId: id,
                writeToken: write_token,
                commitMessage: (objectId) ? "Repurpose existing master" : "Create master",
                awaitCommitConfirmation: false,
                client
            });
            this.ReportProgress("Finalized production master object", finalizeResponse);
            return {
                errors: errors || [],
                logs: logs || [],
                warnings: warnings || [],
                ...finalizeResponse
            };
        }
    };
    
    async probeSource({client, objectId, libraryId, access, fileToProbe, writeToken}, outputs) {
        try {
            if (!outputs){
                outputs = {};
            }
            if (!outputs.probe_errors){
                outputs.probe_errors = {};
            }
            if (!outputs.probe_warnings){
                outputs.probe_warnings = {};
            }
            if (!outputs.probe_logs){
                outputs.probe_logs = {};
            }
            if (!outputs.probe){
                outputs.probe = {};
            }
            if (!outputs.default_variant_streams){
                outputs.default_variant_streams = {};
            }
            let body = {file_paths: [fileToProbe], access};    
            let reporter = this;
            ElvOAction.TrackerPath = this.TrackerPath;
            client.ToggleLogging(true, {log: reporter.Debug, error: reporter.Error});
            
            let result = await client.CallBitcodeMethod({
                //versionHash: contentHash,
                objectId,
                libraryId,
                writeToken,
                method: "/media/files/probe",
                constant: false,
                body: body
            });
            let probe = result.data[fileToProbe];
            if (probe) {
                outputs.probe[fileToProbe] = probe;
                if (this.Payload.inputs.create_default_offering && probe.streams && (!outputs.default_variant_streams.audio || !outputs.default_variant_streams.video)) {
                    let streamIndex = 0;
                    for (let stream of probe.streams) {
                        if (stream.type == "StreamVideo") {                           
                            if (!outputs.default_variant_streams.video) {
                                outputs.default_variant_streams.video = {
                                    default_for_media_type: false,
                                    label: "",
                                    language: "",
                                    mapping_info:"",
                                    sources: [ {
                                        files_api_path: fileToProbe,
                                        stream_index: streamIndex
                                    } ]
                                }
                                this.reportProgress("Found video stream, using as default",  {files_api_path: fileToProbe, stream_index: streamIndex});
                            } else {
                                continue;
                            }
                        }
                        if ((stream.type == "StreamAudio") && (stream.channels == 2)){  
                            if (!outputs.default_variant_streams.audio) {
                                outputs.default_variant_streams.audio = {
                                    default_for_media_type: false,
                                    label: "",
                                    language: "",
                                    mapping_info:"",
                                    sources: [ {
                                        files_api_path: fileToProbe,
                                        stream_index: streamIndex
                                    } ]
                                }
                                this.reportProgress("Found stereo audio stream, using as default",  {files_api_path: fileToProbe, stream_index: streamIndex});
                            } else {
                                continue;
                            }
                        }
                        streamIndex++;
                    }
                }
                
            }
            outputs.probe_errors[fileToProbe] = result.errors;
            outputs.probe_warnings[fileToProbe] = result.warnings;
            outputs.probe_logs[fileToProbe]= result.logs
            if (result.errors && result.errors.length > 0) {
                this.Error("errors", result.errors);
                throw Error("Errors encountered during probing");
            }
            this.ReportProgress("Probed production master source file", fileToProbe);
            return outputs;
        } catch(err) {
            this.Error("Could not probe production master source file "+fileToProbe, err);
            return null;
        }
    };
    
    
    
    static VERSION = "0.3.0";
    static REVISION_HISTORY = {
        "0.0.1": "Initial release",
        "0.0.2": "Private key input is encrypted",
        "0.0.3": "Uses safe-exec for object creation to avoid nonce issues",
        "0.0.4": "cloud_secret_access_key now accepts encrypted values",
        "0.0.5": "Removes transaction from audio metadata read",
        "0.0.6": "Fix bugs preventing metadata from being provided",
        "0.0.7": "de-escape the special characters in URL paths",
        "0.0.8": "Adds option to provide an admin group for the created master object",
        "0.0.9": "Fixes option to provide an admin group",
        "0.0.10": "Allows the reuse of existing object",
        "0.0.11": "Fixes a typo on path vs. Path",
        "0.1.0": "Allows the reuse of existing object",
        "0.1.1": "Adds verification and retry to grant admin rights option",
        "0.2.0": "Probes files one at a time",
        "0.2.1": "Cleans up matched entries in access after upload",
        "0.2.2": "Allows s3 reference on signed links",
        "0.2.3": "Reverts to original coding",
        "0.2.4": "Fixes support for s3 reference on signed links",
        "0.2.5": "Allows special character in bucket names",
        "0.2.6": "Works around validation error when using deprecated s3 pre-signed URL format",
        "0.2.7": "Bypasses type validation if an existing object was provided",
        "0.2.8": "Adds defaulting of the title",
        "0.2.9": "Adds ability to set accessor rights upon creation",
        "0.3.0": "Improves detection of audio stream in default offering",
        "0.3.1": "Adds write token support"
    };
}

if (ElvOAction.executeCommandLine(ElvOActionCreateProductionMaster)) {
    ElvOAction.Run(ElvOActionCreateProductionMaster);
} else {
    module.exports=ElvOActionCreateProductionMaster;
}