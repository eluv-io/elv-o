const ElvOAction = require("../o-action").ElvOAction;
const ElvOFabricClient = require("../o-fabric");



class ElvOActionCreateSpritesheet extends ElvOAction  {
    
    ActionId() {
        return "create_spritesheet";
    };
    
    Parameters() {
        return {
            parameters: {
                identify_by_version: {type: "boolean", required:false, default: false},
                clear_pending_commit: {type: "boolean", required:false, default: false},
                restore_existing: {type: "boolean", required:false, default: false},
                finalize_write_token: { type: "boolean", required: false, default: true },
                finalize_write_token_on_exceptions: { type: "boolean", required: false, default: true },
            }
        };
    };
    
    IdleTimeout() {
        return 600; //10 minutes
    };
    
    IOs(parameters) {
        let inputs = {
            private_key: {type: "password", required:false},
            config_url: {type: "string", required:false},
            frame_interval: {type: "numeric", required: false},
            time_interval: {type: "numeric", required: false},
            target_thumb_count: {type: "numeric", required: false},
            thumb_height: {type: "numeric", required: false}, // optional - default: 180         
            //"thumb_width": -1              # optional; default: -1 (auto - preserve aspect ratio)
            offering: {type:"string", required: false, default:"default"},
            write_token: {type:"string", required: false}
        };
        if (!parameters.identify_by_version) {
            inputs.mezzanine_object_id = {type: "string", required: true};
        } else {
            inputs.mezzanine_object_version_hash = {type: "string", required: true};
        }
        let outputs =  {modified_object_version_hash: {type:"string"}};
        return {inputs: inputs, outputs: outputs}
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
        inputs = this.Payload.inputs;
        let parameters = this.Payload.parameters;
        let objectId = inputs.mezzanine_object_id;
        let versionHash = inputs.mezzanine_object_version_hash;
        
        try {
            if (!objectId) {
                objectId = client.utils.DecodeVersionHash(versionHash).objectId;
            }
            let libraryId = await this.getLibraryId(objectId, client);

            let writeToken = inputs.write_token
            
            if (this.Payload.parameters.restore_existing) {
            let meta = await this.getMetadata({
                libraryId: libraryId,
                objectId: objectId,
                client: client
            });
            if ((Object.keys(meta.offerings[inputs.offering].frame_sets || {}).length != 0)
            && (Object.keys(meta.offerings[inputs.offering].storyboard_sets || {}).length != 0)) {
                this.reportProgress("A story board is already present,  no need to restore");
                return ElvOAction.EXECUTION_FAILED;
            }

            if (meta.files?.frame_sets) {
                this.reportProgress("Files from storyboard found hanging");
                let versions = await client.ContentObjectVersions({libraryId, objectId});
                let frameSets;
                let storyboardSets;
                for (let version of versions.versions) {
                    let metaVersion = await this.getMetadata({client, libraryId, versionHash: version.hash, metadataSubtree: "offerings/"+inputs.offering});
                    if ((Object.keys(metaVersion.frame_sets || {}).length != 0)
                    && (Object.keys(metaVersion.storyboard_sets || {}).length != 0)) {
                        frameSets = metaVersion.frame_sets;
                        storyboardSets = metaVersion.storyboard_sets
                        break;
                    }
                }
                if (frameSets && storyboardSets) {
                    if (!writeToken) {
                        writeToken = await this.getWriteToken({
                            libraryId: libraryId,
                            objectId: objectId,
                            client: client,
                            force: this.Payload.parameters.clear_pending_commit
                        });
                    }
                    this.reportProgress("write_token", writeToken);
                    await client.ReplaceMetadata({
                        libraryId, objectId, writeToken,
                        metadataSubtree: "offerings/"+inputs.offering + "/frame_sets",
                        metadata: frameSets
                    }); 
                    await client.ReplaceMetadata({
                        libraryId, objectId, writeToken,
                        metadataSubtree: "offerings/"+inputs.offering + "/storyboard_sets",
                        metadata: storyboardSets
                    });
                    let msg = "Restored Spritesheet";
                    if (parameters.finalize_write_token) {
                        let response = await this.FinalizeContentObject({
                            libraryId: libraryId,
                            objectId: objectId,
                            writeToken: writeToken,
                            commitMessage: msg,
                            client
                        });
                    
                        if (response && response.hash) {
                            this.ReportProgress(msg);
                            outputs.modified_object_version_hash = response.hash;
                            return ElvOAction.EXECUTION_COMPLETE;
                        } else {
                            throw "Failed to restore Spritesheet";
                        }
                    } else {
                        return ElvOAction.EXECUTION_COMPLETE;
                    }
                }
            }
            if (!writeToken) {
                writeToken = await this.getWriteToken({
                        libraryId: libraryId,
                        objectId: objectId,
                        client: client,
                        force: this.Payload.parameters.clear_pending_commit
                    });
                }
            }
            
            this.reportProgress("write_token", writeToken);
            
            let reporter = this;
            ElvOAction.TrackerPath = this.TrackerPath;
            client.ToggleLogging(true, {log: reporter.Debug, error: reporter.Error});
            
            let body  = {async: true}; // use new API that launches as LRO
            if (inputs.offering) {
                body.offering_key = inputs.offering;
            }  else {
                inputs.offering = "default";
            }
            if (inputs.frame_interval) {
                body.frame_interval = inputs.frame_interval;
            }
            if (inputs.time_interval) {
                let videoInfo = await  this.getMetadata({
                    libraryId: libraryId,
                    objectId: objectId,
                    client: client,
                    metadataSubtree: "offerings/"+inputs.offering+"/media_struct/streams/video"
                });
                body.frame_interval = Math.floor(eval(videoInfo.rate) * inputs.time_interval);
            }
            if (inputs.target_thumb_count) {
                body.target_thumb_count = inputs.target_thumb_count;
            }
            if (inputs.thumb_height) {
                body.thumb_height = inputs.thumb_height;
            }
            this.reportProgress("Storyboard parameters", body);
            let existingStoryboardSets = await  this.getMetadata({
                libraryId: libraryId,
                objectId: objectId,
                client: client,
                metadataSubtree: "offerings/"+inputs.offering+"/storyboard_sets"
            });
            try {
                let logs = await client.CallBitcodeMethod({
                    "libraryId": libraryId,
                    "objectId": objectId,
                    "writeToken": writeToken,
                    "method": "media/thumbnails/create",
                    "body": body,
                    //header,
                    "constant": false
                });
                //console.log("Bit code result", logs);
                if (logs && logs.logs && logs.logs.length >0){
                    for (let log of logs.logs) {
                        this.reportProgress(log);
                    }
                }
                //{ data: 'tlro1EjbjiAhsrHR8uJwCaPWKDSbXroekgC4JjmzWEjuehn7bdxTvpY8bs' }
                client.ToggleLogging(false);
                let lroHandle =logs && logs.data;
                if (lroHandle) {
                    let lroNode =  client.HttpClient.draftURIs[writeToken].hostname();
                    this.markLROStarted(logs.data ,lroNode);
                    let lastPollProgress = null;
                    while (true) {
                        await this.sleep((this.Payload.polling_interval || this.PollingInterval() || 60) * 1000);
                        logs = await client.CallBitcodeMethod({
                            "libraryId": libraryId,
                            "objectId": objectId,
                            "writeToken": writeToken,
                            "method": "media/thumbnails/status/"+lroHandle,
                            "body": {},
                            //header,
                            "constant": true
                        });
                        let progress = logs && logs.data.custom && logs.data.custom.progress;
                        //console.log("Bit code progress", progress);
                        if ((progress && progress.percentage) != (lastPollProgress && lastPollProgress.percentage)) { //so that idle-timeout is triggered if stalled LRO
                            this.ReportProgress("LRO progress", progress);
                            lastPollProgress = progress;
                        }
                        let state = logs.data && logs.data.state;
                        if (state == "terminated") {
                            let runState = logs.data.custom && logs.data.custom.run_state;
                            if (runState == "finished") {
                                this.ReportProgress("LRO not process completed successfully", logs.data.custom);
                                break;
                            } else {
                                this.ReportProgress("LRO not running but process did not complete", logs.data.custom);
                                throw new Error("LRO not running but process did not complete");
                            }
                        }
                    }
                } else {
                    this.reportProgress("Asynchronous mode does not seem to be supported, processing as synch");
                }
            } catch(errBitCode) {
                if (errBitCode.message != "Gateway Time-out") {
                    throw errBitCode
                }
                //Monitor progress
                let startTime = (new Date()).getTime();
                let  storyboardFound;
                if (!existingStoryboardSets) {
                    existingStoryboardSets = {};
                }
                while (!storyboardFound) {
                    let now = (new Date()).getTime();
                    if  ((now - startTime) > (this.IdleTimeout() * 1000)) {
                        throw Error("Storyboard has not been found after "+ this.IdleTimeout() + " seconds");
                    }
                    this.reportProgress("check for storyboardFound in", (this.Payload.polling_interval || this.PollingInterval() || 60) * 1000);
                    await this.sleep((this.Payload.polling_interval || this.PollingInterval() || 60) * 1000);
                    let storyboardSets = await  this.getMetadata({
                        libraryId: libraryId,
                        objectId: objectId,
                        writeToken,
                        client: client,
                        metadataSubtree: "offerings/"+inputs.offering+"/storyboard_sets"
                    });
                    if (storyboardSets) {
                        for (let storyboardKey in storyboardSets)  {
                            this.reportProgress("New storyboard key found", storyboardKey);
                            let matcher = storyboardKey.match(/default_video_i([0-9]+)/);
                            if  (matcher &&  !existingStoryboardSets[storyboardKey]) {
                                storyboardFound = storyboardKey;
                                break;
                            }
                        }
                        if  (storyboardFound) {
                            this.reportProgress("Story board appears to have been generated", storyboardFound);
                        }
                    }
                }
            }
            let msg = "Added Spritesheet";
            if (parameters.finalize_write_token) {
                let response = await this.FinalizeContentObject({
                    libraryId: libraryId,
                    objectId: objectId,
                    writeToken: writeToken,
                    commitMessage: msg,
                    client
                });
                outputs.modified_object_version_hash = response.hash;
            }
            if (response && response.hash) {
                this.ReportProgress(msg)
            } else {
                throw "Failed to add Spritesheet";
            }
        } catch (errSet) {
            this.Error("Could not add Spritesheet for " + (objectId || versionHash), errSet);
            this.ReportProgress("Could not add Spritesheet");
            // We might want to commit the write token even if there are exceptions since spritesheet are not fatal
            if (parameters.finalize_write_token_on_exceptions) {
                let response = await this.FinalizeContentObject({
                    libraryId: libraryId,
                    objectId: objectId,
                    writeToken: writeToken,
                    commitMessage: msg,
                    client
                });
                outputs.modified_object_version_hash = response.hash;
            }
            return ElvOAction.EXECUTION_EXCEPTION;
        }
        return ElvOAction.EXECUTION_COMPLETE;
        
    };
    
    markLROStarted(lroHandle,lroNode) {
        this.trackProgress(ElvOActionCreateSpritesheet.TRACKER_LRO_STARTED, "Thumbnails creation job started", lroHandle+","+lroNode);
    };
    
    static TRACKER_LRO_STARTED = 65;
    
    static VERSION = "0.0.8";
    static REVISION_HISTORY = {
        "0.0.1": "Initial release",
        "0.0.2": "Adds option to clear pending commit",
        "0.0.3": "Adds parameterization",
        "0.0.4": "Adds protection against gateway timeouts",
        "0.0.5": "Exposes target_thumb_count setting",
        "0.0.6": "Uses idle-timeout to limit duration of search for completed offering in case of gateway error",
        "0.0.7": "Adds asynchronous processing via LRO",
        "0.0.8": "Add write-token support"
    };
    
}

if (ElvOAction.executeCommandLine(ElvOActionCreateSpritesheet)) {
    ElvOAction.Run(ElvOActionCreateSpritesheet);
} else {
    module.exports=ElvOActionCreateSpritesheet;
}