
const ElvOAction = require("../o-action").ElvOAction;
const ElvOFabricClient = require("../o-fabric");
const fs = require('fs');


class ElvOActionManageWatermark extends ElvOAction  {
    
    ActionId() {
        return "manage_watermark";
    };
    
    Parameters() {
        return {
            "parameters": {
                action: {type: "string", required:true, values:["SET","DELETE","SET_WITH_DOWNLOADABLE"]},
                watermark_type: {type: "string", required:true, values:["TEXT","IMAGE", "HTML","COMPOSITE"]},
                identify_by_version: {type: "boolean", required:false, default: false},
                clear_pending_commit: {type: "boolean", required:false, default: false},
                link_to_source: {type: "array", required: false, default: ["media_struct/streams", "playout/streams", "frame_sets", "storyboard_sets"]}
            }
        };
    };
    
    IOs(parameters) {
        let inputs = {
            private_key: {"type": "password", "required":false},
            config_url: {"type": "string", "required":false}
        };
        if (!parameters.identify_by_version) {
            inputs.target_object_id = {type: "string", required: true};
        } else {
            inputs.target_object_version_hash = {type: "string", required: true};
        }
        let outputs =  {};
        if ((parameters.action == "SET") || (parameters.action == "SET_WITH_DOWNLOADABLE")) {
            inputs.target_offering = {type:"string", required: false, default:"default"};
            switch (parameters.watermark_type) {
                case "TEXT": {
                    inputs.text_watermark = {
                        type: "object", 
                        required: true,
                        example: {
                            "font_color": "white@0.2",
                            "font_relative_height": 0.015,
                            "shadow": true,
                            "shadow_color": "black@0.15",
                            "template": "$USERNAME",
                            "x": "(w-tw)/100*85",
                            "y": "(h-th)/100*20"
                        } 
                    };
                    break;
                }
                case "IMAGE": {
                    inputs.image_watermark = {
                        type: "object", 
                        required: true,
                        example: {
                            "height": 0.6,
                            "image": {
                                "/": "/qfab/hq__CXi6RkyNBqMqzaMkidvh2QtHhsMFbmtSJZ6nDMWcwaWpUCntE52yBN6y6jTT4uNMdbEL1xk4VA/files/public/MGM_FullLogo_OneColor_Black.png"
                            },
                            "opacity": 0.4,
                            "x": 0.5,
                            "y": 0.5
                        }
                    };
                    break;
                }
                case "HTML": {
                    inputs.html_watermark = {
                        type: "object", 
                        required: true,
                        example: {
                            "height": 0.95,
                            "image": {
                                ".": {
                                    "container": "hq__MZ9xY6VY75fXJf8qBRMtVnx1dmb6j16kpjJCH2Pm6c22tzFwShZfuPENYcC277sR28zY4gz3Uk"
                                },
                                "/": "./files/amazon.html"
                            },
                            "opacity": 0.75,
                            "x": 0.5,
                            "y": 0.5
                        }
                    };
                    break;
                }
                case "COMPOSITE": {
                    inputs.watermarks = {
                        type: "object", 
                        required: true,
                        example: {
                            "text_watermark": {
                                "font_color": "white@0.2",
                                "font_relative_height": 0.015,
                                "shadow": true,
                                "shadow_color": "black@0.15",
                                "template": "$USERNAME",
                                "x": "(w-tw)/100*85",
                                "y": "(h-th)/100*20"
                            },
                            "image_watermark": {
                                "height": 0.6,
                                "image": {
                                    "/": "/qfab/hq__CXi6RkyNBqMqzaMkidvh2QtHhsMFbmtSJZ6nDMWcwaWpUCntE52yBN6y6jTT4uNMdbEL1xk4VA/files/public/MGM_FullLogo_OneColor_Black.png"
                                },
                                "opacity": 0.4,
                                "x": 0.5,
                                "y": 0.5
                            },
                            "html_watermark": null
                        }  
                    };
                    break;
                }
                default: {
                    this.Error("Not supported yet watermark_type" + parameters.watermark_type);
                    throw  "Not supported yet watermark_type" + parameters.watermark_type;
                }
            }
            inputs.source_offering = {type:"string", required: false, default:null};
            outputs.modified_object_version_hash = {type:"string"};
        }
        if (parameters.action == "SET_WITH_DOWNLOADABLE") {
            inputs.downloadable_offering_suffix = {type: "file", required:false, default:"_downloadable"};
        }
        if (parameters.action == "DELETE") {
            inputs.target_offering = {type:"string", required: false, default:"default"};
            inputs.source_offering = {type:"string", required: false, default:null};
            outputs.modified_object_version_hash = {type:"string"};
        }
        return {inputs: inputs, outputs: outputs}
    };
    
    readTemplate(templateValue) {
        if ((typeof templateValue) == "string") {
            let matcher = templateValue.match(/^@(.*)/);
            if (matcher) {
                this.reportProgress("Reading template from provided path", matcher[1]);
                return JSON.parse(fs.readFileSync(matcher[1]));
            }
        }
        return templateValue;
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
        let field = inputs.field;
        let objectId = inputs.target_object_id;
        let versionHash = inputs.target_object_version_hash;
        
        try {
            if ((this.Payload.parameters.action == "SET") || (this.Payload.parameters.action == "SET_WITH_DOWNLOADABLE")) {
                try {
                    if (!objectId) {
                        objectId = this.Client.utils.DecodeVersionHash(versionHash).objectId;
                    }
                    let libraryId = await this.getLibraryId(objectId, client);
                    this.ReportProgress("Reading source offering", inputs.source_offering || inputs.target_offering);

                    let offerings = await this.getMetadata({
                        objectId: objectId,
                        libraryId: libraryId,
                        versionHash: versionHash,
                        metadataSubtree: "offerings",
                        resolveLinks: false,
                        client: client
                    });
                    let sourceOffering = offerings[inputs.source_offering || inputs.target_offering];
                    if (!sourceOffering) {
                        this.ReportProgress("Inexistant offering", inputs.source_offering || inputs.target_offering);
                        return ElvOAction.EXECUTION_FAILED;
                    }
                    
                    let writeToken = await this.getWriteToken({
                        libraryId: libraryId,
                        objectId: objectId,
                        client: client,
                        force: this.Payload.parameters.clear_pending_commit
                    });
                    this.reportProgress("write_token", writeToken);
                    
                    let linksToSource = this.Payload.parameters.link_to_source || [];
                    let modifiedSource = false;
                    if (inputs.source_offering && (inputs.source_offering != inputs.target_offering) && (linksToSource.length != 0)) {
                        for (let linkedField of linksToSource) {
                            let fieldElements = linkedField.split("/");
                            let element = sourceOffering;
                            for (let fieldElement of fieldElements){
                                if (!element[fieldElement]) {                                    
                                    element[fieldElement] = {};
                                    modifiedSource = true;
                                }
                                element = element[fieldElement];
                            }
                        }     
                        if  (modifiedSource) {
                            await client.ReplaceMetadata({
                                libraryId: libraryId,
                                objectId: objectId,
                                writeToken: writeToken,
                                metadataSubtree: "offerings/"+ inputs.source_offering,
                                metadata: sourceOffering,
                                client
                            });
                        }   
                        for (let linkedField of linksToSource) {
                            let fieldElements = linkedField.split("/");
                            let element = sourceOffering;
                            for (let i=0; i < (fieldElements.length - 1); i++) {
                                let fieldElement = fieldElements[i];
                                element = element[fieldElement];
                            }                           
                            element[fieldElements[fieldElements.length - 1]] = {".":{}, "/": "./meta/offerings/" + inputs.source_offering + "/"+linkedField};
                        }                 
                    }
                    
                    if (!sourceOffering.frame_sets) {
                        sourceOffering.frame_sets = {}; //no matter how we get here the target offering should have that field
                    }
                    if (!sourceOffering.storyboard_sets) {
                        sourceOffering.storyboard_sets = {}; //no matter how we get here the target offering should have that field
                    }
                    
                    switch(this.Payload.parameters.watermark_type) {
                        case "TEXT": {
                            sourceOffering.simple_watermark = this.readTemplate(inputs.text_watermark);
                            break;
                        }
                        case "IMAGE": {
                            sourceOffering.image_watermark = this.readTemplate(inputs.image_watermark);
                            break;
                        }
                        case "HTML": {
                            sourceOffering.html_watermark = this.readTemplate(inputs.html_watermark);
                            break;
                        }
                        case "COMPOSITE": {
                            let watermarks = this.readTemplate(inputs.watermarks);
                            
                            if (watermarks.hasOwnProperty("text_watermark")) {
                                sourceOffering.simple_watermark = watermarks.text_watermark;
                            }
                            if (watermarks.hasOwnProperty("image_watermark")) {
                                sourceOffering.image_watermark = watermarks.image_watermark;
                            }
                            if (watermarks.hasOwnProperty("html_watermark")) {
                                sourceOffering.html_watermark = watermarks.html_watermark;
                            }
                            break;
                        }
                        default: {
                            this.Error("Only TEXT watermark supported for now");
                            throw "Only TEXT watermark supported for now";
                        }
                    }
                    
                    this.ReportProgress("Watermark prepared for " + inputs.target_offering);
                    let msg = "Set watermark for offering  '" + inputs.target_offering + "'";
                    
                    await client.ReplaceMetadata({
                        libraryId: libraryId,
                        objectId: objectId,
                        writeToken: writeToken,
                        metadataSubtree: "offerings/"+ inputs.target_offering,
                        metadata: sourceOffering,
                        client
                    });
                    await client.ReplaceMetadata({
                        libraryId: libraryId,
                        objectId: objectId,
                        writeToken: writeToken,
                        metadataSubtree: "public/asset_metadata/sources/"+ inputs.target_offering,
                        metadata: {"/": "./rep/playout/" + inputs.target_offering +"/options.json"},
                        client
                    });
                    if (this.Payload.parameters.action == "SET_WITH_DOWNLOADABLE") {
                        if (!sourceOffering.drm_optional && inputs.downloadable_offering_suffix) {
                            sourceOffering.drm_optional = true;
                            sourceOffering.playout.playout_formats = {
                                "dash-clear": {drm: null, protocol: {min_buffer_length: 2, type: "ProtoDash"}},
                                "hls-clear": {drm: null, protocol: {min_buffer_length: 2, type: "ProtoHls"}}
                            };
                            let downloadableOffering = inputs.target_offering + inputs.downloadable_offering_suffix
                            this.ReportProgress("Preparing downloadable offering for "+ inputs.target_offering, downloadableOffering);
                            
                            sourceOffering.playout.streams = {".": {}, "/":"./meta/offerings/" + inputs.target_offering + "/playout/streams"};
                            sourceOffering.media_struct.streams = {".":{}, "/":"./meta/offerings/" + inputs.target_offering + "/media_struct/streams"};
                            sourceOffering.frame_sets = {".":{}, "/":"./meta/offerings/" + inputs.target_offering + "/frame_sets"};
                            sourceOffering.storyboard_sets = {".":{}, "/":"./meta/offerings/" + inputs.target_offering + "/storyboard_sets"};
                            
                            
                            await client.ReplaceMetadata({
                                libraryId: libraryId,
                                objectId: objectId,
                                writeToken: writeToken,
                                metadataSubtree: "offerings/" + downloadableOffering,
                                metadata: sourceOffering,
                                client
                            });
                            await client.ReplaceMetadata({
                                libraryId: libraryId,
                                objectId: objectId,
                                writeToken: writeToken,
                                metadataSubtree: "public/asset_metadata/sources/"+ downloadableOffering,
                                metadata: {"/": "./rep/playout/" + downloadableOffering +"/options.json"},
                                client
                            });
                        } else {
                            this.ReportProgress("Offering "+ inputs.target_offering + " is downloadable");
                        }
                    }
                    
                    this.ReportProgress("Finalizing changes");
                    let response = await this.FinalizeContentObject({
                        libraryId: libraryId,
                        objectId: objectId,
                        writeToken: writeToken,
                        commitMessage: msg,
                        client
                    });
                    this.ReportProgress(msg, response.hash);
                    outputs.modified_object_version_hash = response.hash;
                } catch (errSet) {
                    this.Error("Could not set offering watermark for " + (objectId || versionHash), errSet);
                    this.ReportProgress("Could not set offering watermark");
                    return ElvOAction.EXECUTION_EXCEPTION;
                }
                return ElvOAction.EXECUTION_COMPLETE;
            }
            
            throw "Operation not implemented yet " + this.Payload.parameters.action;
        } catch(err) {
            this.Error("Could not process " + this.Payload.parameters.action + " " +this.Payload.parameters.watermark_type  + " watermark for " + (objectId || versionHash), err);
            return ElvOAction.EXECUTION_EXCEPTION;
        }
    };
    
    static VERSION = "0.1.3";
    static REVISION_HISTORY = {
        "0.0.1": "Initial release",
        "0.0.2": "Private key input is encrypted",
        "0.0.3": "Uses reworked finalize method",
        "0.0.4": "Adds intermediate tracking messages",
        "0.0.5": "Adds option to create downloadable offering",
        "0.0.6": "Will err instead of fail if offerings can not be read",
        "0.0.7": "Adds option to clear pending commit",
        "0.0.8": "Adds sources link",
        "0.1.0": "Ensures that downloadable offering is clear",
        "0.1.1": "Allows linking of field instead of copying, uses links in downloadable offering",
        "0.1.2": "Adds support for image and html watermarks",
        "0.1.3": "Fixes bug in template reading"
    };
}


if (ElvOAction.executeCommandLine(ElvOActionManageWatermark)) {
    ElvOAction.Run(ElvOActionManageWatermark);
} else {
    module.exports=ElvOActionManageWatermark;
}
