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
                finalize_write_token: { type: "boolean", required: false, default: true },
                finalize_write_token_on_exceptions: { type: "boolean", required: false, default: true },
            }
        };
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

    
    
    
    async Execute(handle, outputs) {
        let client;
        if (!this.Payload.inputs.private_key && !this.Payload.inputs.config_url){
            client = this.Client;
        } else {
            let privateKey = this.Payload.inputs.private_key || this.Client.signer.signingKey.privateKey.toString();
            let configUrl = this.Payload.inputs.config_url || this.Client.configUrl;
            client = await ElvOFabricClient.InitializeClient(configUrl, privateKey)
        }
        let inputs = this.Payload.inputs;
        let parameters = this.Payload.parameters;
        let field = inputs.field;
        let objectId = inputs.mezzanine_object_id;
        let versionHash = inputs.mezzanine_object_version_hash;
        
        try {
            if (!objectId) {
                objectId = client.utils.DecodeVersionHash(versionHash).objectId;
            }
            let libraryId = await this.getLibraryId(objectId, client);

            let writeToken = inputs.write_token
            if (!writeToken) {
                let writeToken = await this.getWriteToken({
                    libraryId: libraryId,
                    objectId: objectId,
                    client: client,
                    force: this.Payload.parameters.clear_pending_commit
                });
            }
            
            this.reportProgress("write_token", writeToken);
            
            let reporter = this;
            ElvOAction.TrackerPath = this.TrackerPath;
            client.ToggleLogging(true, {log: reporter.Debug, error: reporter.Error});
            
            let body  = {} //"body": {"offering_key": "clips","frame_interval": frameInterval},
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
                
                if (logs && logs.logs && logs.logs.length >0){
                    for (log of logs.logs)
                    this.reportProgress(log);
                }
                this.Debug("Bit code result", logs);
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
                    if  ((now - startTime) > 600000) {
                        throw Error("Storyboard has not been found after 10 minutes");
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
    
    
    
    static VERSION = "0.0.6";
    static REVISION_HISTORY = {
        "0.0.1": "Initial release",
        "0.0.2": "Adds option to clear pending commit",
        "0.0.3": "Adds parameterization",
        "0.0.4": "Adds protection against gateway timeouts",
        "0.0.5": "Exposes target_thumb_count setting",
        "0.0.6": "Add write-token support",
    };
    
}

if (ElvOAction.executeCommandLine(ElvOActionCreateSpritesheet)) {
    ElvOAction.Run(ElvOActionCreateSpritesheet);
} else {
    module.exports=ElvOActionCreateSpritesheet;
}