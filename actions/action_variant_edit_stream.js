const ElvOAction = require("../o-action").ElvOAction;
const ElvOFabricClient = require("../o-fabric");
const ElvOMutex = require("../o-mutex");
const mime = require("mime-types");
const fs = require("fs");
const Path = require('path');

class ElvOActionVariantEditStream extends ElvOAction  {

    Parameters() {
        return {"parameters": {action: {type: "string", values:["EDIT","ADD","REMOVE"]}}};
    };

    IOs(parameters) {
        let inputs = {
            production_master_object_id: {type: "string", required:true},
            variant_key: {type: "string", required:false, description: "Stream within variant to change", default:"default"},
            stream_key: {type: "string", required:true},
            private_key: {type: "password", required:false},
            config_url: {type: "string", required:false}
        };
        if (parameters.action == "ADD") {
            inputs.file = {type: "string", required:false, "description": "File path within object"};
            inputs.label = {type: "string", required:false, description: "Label to display for stream"};
            inputs.language = {type: "string", required:false, default:"en", description: "Language code for stream (some older players may use this as the label)"};
            inputs.stream_indexes = {type: "array", "array_item_type":"numeric", required:true, description: "Index(es) of stream(s) to use from file. (Currently only audio streams can use 2 stream indexes)"};
            inputs.channel_indexes = {type: "array", "array_item_type":"numeric", required:false, description: "Index(es) of channels to use from the selected stream in 2CHANNELS_1STEREO mode"};
            inputs.mapping = {type:"string", required: false, description: "Mapping info for stream", values:["2MONO_1STEREO","2CHANNELS_1STEREO"], default:"2MONO_1STEREO"};
            inputs.is_default = {type: "boolean", required:false, default: false, description: "Stream should be chosen by default"};
            inputs.edit_if_present = {type: "boolean", required:false, default: false, description: "If stream_key is present, it will act as edit"};
            inputs.safe_update = {type: "boolean", required:false, default: false};
        }
        if (parameters.action == "EDIT") {
            inputs.file = {type: "string", required:false, "description": "File path within object"};
            inputs.stream_indexes = {type: "array", array_item_type:"numeric", required:false, default: null};
            inputs.stream_index = {type: "numeric", required:false, default: null};
            inputs.label = {type: "string", required:false, description: "Label to display for stream"};
            inputs.language = {type: "string", required:false, default: null};
            inputs.channel_indexes = {type: "array", array_item_type:"numeric", required:false, default: null};
            inputs.mapping = {type:"string", required: false, default:null};
            inputs.is_default = {type: "boolean", required:false, default: null};
            inputs.safe_update = {type: "boolean", required:false, default: false};
        }
        let outputs = {
            production_master_version_hash: {type: "string"}
        };
        return { inputs : inputs, outputs: outputs };
    };

    ActionId() {
        return "variant_edit_stream";
    };



    validateVariant(metadata, variantKey) {
        // check to make sure we have production_master
        if(!metadata.production_master) {
            this.Error("Key '/production_master' not found in object metadata");
            this.ReportProgress("Key '/production_master' not found in object metadata");
            return false;
        }

        // check to make sure we have variants
        if(!metadata.production_master.variants) {
            this.Error("Key '/production_master/variants' not found in object metadata");
            this.ReportProgress("Key '/production_master/variants' not found in object metadata");
            return false;
        }

        // check for specified variant key
        if(!metadata.production_master.variants.hasOwnProperty(variantKey)) {
            this.Error("Variant '" + variantKey + "' not found in production master metadata");
            this.ReportProgress("Variant '" + variantKey + "' not found in production master metadata");
            return false;
        }
        return true;
    };

    validateStreamSource(metadata, filePath, streamIndex, channelIndex) {
        const sources = metadata.production_master.sources;
        if(!sources.hasOwnProperty(filePath)) {
            const sourceList = Object.keys(sources).join("\n  ");
            this.Error("File not found in Production Master source list", filePath);
            this.ReportProgress("File not found in Production Master source list", filePath);
            return false;
        }

        const sourceStreams = metadata.production_master.sources[filePath].streams;
        if(streamIndex < 0 || streamIndex >= sourceStreams.length) {
            this.Error("streamIndex must be between 0 and " + (sourceStreams.length - 1) + " for file '" + filePath + "'")
            this.ReportProgress("streamIndex must be between 0 and " + (sourceStreams.length - 1) + " for file '" + filePath + "'");
            return false;
        }

        const sourceStream = metadata.production_master.sources[filePath].streams[streamIndex];
        if(sourceStream.type !== "StreamAudio" && sourceStream.type !== "StreamVideo") {
            this.Error("streamIndex " + streamIndex + " in file '" + filePath + "' is of type '" + sourceStream.type + "', currently only StreamAudio and StreamVideo are supported");
            this.ReportProgress("streamIndex " + streamIndex + " in file '" + filePath + "' is of type '" + sourceStream.type + "', currently only StreamAudio and StreamVideo are supported");
            return false;
        }
        if(channelIndex) {
            if(sourceStream.type !== "StreamAudio") {
                this.Error("channelIndex specified for non-audio stream source");
                this.ReportProgress("channelIndex specified for non-audio stream source");
                return false;
            }

            if(channelIndex < 0) {
                this.Error("channelIndex cannot be a negative number");
                this.ReportProgress("channelIndex cannot be a negative number");
                return false;
            }

            if(channelIndex >= sourceStream.channels) {
                this.Error("channelIndex (" + channelIndex + ") exceeds maximum for stream (" + (sourceStream.channels - 1) + ")");
                this.ReportProgress("channelIndex (" + channelIndex + ") exceeds maximum for stream (" + (sourceStream.channels - 1) + ")");
                return false;
            }
        }
        // if(sourceStream.type === "StreamAudio" && sourceStream.channels !== 2) {
        //   this.throwError("streamIndex " + streamIndex + " in file '" + filePath + "' is audio but has " + sourceStream.channels + " channel(s), currently only audio streams with 2 channels are supported");
        // }
        return true;
    }


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


    async executeAdd(handle, outputs, client) {

        const objectId = this.Payload.inputs.production_master_object_id;
        const libraryId = await this.getLibraryId(objectId, client);
        const streamKey = this.Payload.inputs.stream_key;
        const label = this.Payload.inputs.label;
        const language = this.Payload.inputs.language;
        const default_for_media_type = this.Payload.inputs.is_default;
        const variantKey = this.Payload.inputs.variant_key;
        const filePath = this.Payload.inputs.file;
        const streamIndexes = this.Payload.inputs.stream_indexes;
        const channelIndexes = this.Payload.inputs.channel_indexes;
        const mappingInfo = this.Payload.inputs.mapping;
        const editIfPresent = this.Payload.inputs.edit_if_present;

        try {

            if  (streamIndexes.length == 0) {
                this.ReportProgress("No audio stream provided");
                outputs.production_master_version_hash = await  this.getVersionHash({
                    objectId,
                    libraryId,
                    client
                });
                this.reportProgress("No update required, returning object latest hash", outputs.production_master_version_hash);
                return ElvOAction.EXECUTION_FAILED;
            }

            // ===============================================================
            // retrieve metadata from object and validate presence of variant
            // ===============================================================
            await  this.acquireMutex(objectId);
            let metadata = await this.getMetadata({libraryId: libraryId, objectId: objectId, client});
            if (!this.validateVariant(metadata, variantKey)) {
                throw Error("Invalid variant "+ variantKey);
            }

            if(channelIndexes) {
                if(channelIndexes.length > 0 && streamIndexes.length != 1) {
                    this.ReportProgress("Channel indexes can only be used when a single stream is used");
                    throw Error("Invalid channel configuration");
                }
            }

           
            let sources = [];
            for (const streamIndex of streamIndexes) {
                if(channelIndexes) {
                    for(const channelIndex of channelIndexes) {
                        this.validateStreamSource(metadata, filePath, streamIndex, channelIndex);
                        sources.push({
                            channel_index: channelIndex,
                            files_api_path: filePath,
                            stream_index: streamIndex
                        });
                    }
                } else {
                    if (this.validateStreamSource(metadata, filePath, streamIndex)) {
                        sources.push({files_api_path: filePath, stream_index: streamIndex});
                    } else {
                        throw Error("Invalid stream source");
                    }
                }
            }


            this.ReportProgress("Adding stream '" + streamKey + "' to variant '" + variantKey + "'... ");

            // =======================================
            // make sure entry for specified stream does not already exist
            // =======================================
            let variantStreams = metadata.production_master.variants[variantKey].streams;
            if (variantStreams.hasOwnProperty(streamKey)) {
                this.ReportProgress("Stream '" + streamKey + "' is already present in variant '" + variantKey + "'");
                if (!editIfPresent) {
                    this.releaseMutex();
                    return ElvOAction.EXECUTION_FAILED;
                }
            }
            // make our changes
            // merge into object variant metadata
            variantStreams[streamKey] = {
                default_for_media_type,
                label,
                language,
                mapping_info: mappingInfo || "",
                sources
            };
            // write back to object
            let writeToken = await this.getWriteToken({
                libraryId: libraryId,
                objectId: objectId,
                force: true,
                client
            });
            this.ReportProgress("Acquired write-token " + writeToken);
            await this.safeExec("client.ReplaceMetadata", [{
                libraryId: libraryId,
                objectId: objectId,
                writeToken: writeToken,
                metadataSubtree: "production_master/variants",
                metadata: metadata.production_master.variants,
                client
            }]);
            this.ReportProgress("Adding stream " + streamKey + " to variant " + variantKey + "... ");
            let msg = "Added stream '" + streamKey + "' to variant '" + variantKey + "'... ";
            let response = await this.safeExec("client.FinalizeContentObject", [{
                libraryId: libraryId,
                objectId: objectId,
                writeToken: writeToken,
                commitMessage: msg,
                client
            }]);
            this.ReportProgress(msg);
            if (response.hash) {
                outputs.production_master_version_hash = response.hash;
            } else {
                this.Error("Could not finalize change", writeToken);
                this.ReportProgress("Could not finalize change", writeToken);
                throw Error("Could not finalize change");
            }
        } catch (error) {
            this.Error("Could not add Variant", error);
            this.releaseMutex();
            return ElvOAction.EXECUTION_EXCEPTION;
        }
        this.releaseMutex();
        return ElvOAction.EXECUTION_COMPLETE;

    };

    async executeEdit(inputs, outputs, client) {

        const objectId = inputs.production_master_object_id;
        const libraryId = await this.getLibraryId(objectId, client);
        const streamKey = inputs.stream_key;
        const variantKey = inputs.variant_key;
        const filePath = inputs.file;
        let  streamIndexes = inputs.stream_indexes;
        if (!streamIndexes && (inputs.stream_index != null)) {
            streamIndexes = [inputs.stream_index];
        }

        try {
            // ===============================================================
            // retrieve metadata from object and validate presence of variant
            // ===============================================================
            await  this.acquireMutex(objectId);
            let metadata = await this.getMetadata({libraryId: libraryId, objectId: objectId, client});
            if (!this.validateVariant(metadata, variantKey) || !this.validateStreamSource(metadata, filePath, streamIndexes[0])) {
                throw Error("Invalid variant  or stream source");
            }

            this.ReportProgress("Changing source for stream '" + streamKey + "' in variant '" + variantKey + "'... ");

            // =======================================
            // find entry for specified stream
            // =======================================

            let variantStreams = metadata.production_master.variants[variantKey].streams;

            if(!variantStreams.hasOwnProperty(streamKey)) {
                this.Error("Stream '" + streamKey + "' not found in variant '" + variantKey + "'");
                throw Error("Stream '" + streamKey + "' not found in variant '" + variantKey + "'");
            }

            let variantStream = variantStreams[streamKey];
           
            // make our changes
            if  (streamIndexes != null) {
                variantStream.sources = [];
                for (const streamIndex of streamIndexes) {
                    if (inputs.channel_indexes) {
                        for (let channelIndex of inputs.channel_indexes) {
                            if (!this.validateStreamSource(metadata, filePath, streamIndex, channelIndex)){
                                throw this.Error("Invalid  stream source ", {filePath, streamIndex, channelIndex});
                            }
                            variantStream.sources.push({
                                channel_index: channelIndex,
                                files_api_path: filePath,
                                stream_index: streamIndex
                            });
                        }
                    } else {
                        if (!this.validateStreamSource(metadata, filePath, streamIndex)) {
                            throw this.Error("Invalid  stream source ", {filePath, streamIndex});
                        }
                        variantStream.sources.push({files_api_path: filePath, stream_index: streamIndex});
                    }
                }
            }
        

            if (inputs.mapping != null)  {
                this.reportProgress("resetting mapping_info to "+ inputs.mapping);
                variantStream.mapping_info = inputs.mapping;
            }
            if (inputs.label != null)  {
                this.reportProgress("resetting label to "+ inputs.label);
                variantStream.label = inputs.label;
            }
            if (inputs.language != null)  {
                this.reportProgress("resetting language to "+ inputs.language);
                variantStream.language = inputs.language;
            }
            if (inputs.is_default != null)  {
                this.reportProgress("resetting is_default to "+ inputs.is_default);
                variantStream.is_default = inputs.is_default;
            }


            // write back to object
            let writeToken = await this.getWriteToken({
                libraryId: libraryId,
                objectId: objectId,
                force: true,
                client
            });
            this.ReportProgress("Acquired write-token " + writeToken);
            await this.safeExec("client.ReplaceMetadata", [{
                libraryId: libraryId,
                objectId: objectId,
                writeToken: writeToken,
                metadataSubtree: "production_master/variants",
                metadata: metadata.production_master.variants,
                client
            }]);
            this.ReportProgress("Adding stream " + streamKey + " to variant " + variantKey + "... ");
            let msg = "Added stream '" + streamKey + "' to variant '" + variantKey + "'... ";
            let response = await this.FinalizeContentObject({
                libraryId: libraryId,
                objectId: objectId,
                writeToken: writeToken,
                commitMessage: msg,
                client
            });
            this.ReportProgress(msg);

            if (response.hash) {
                outputs.production_master_version_hash = response.hash;
            } else {
                this.Error("Could not finalize change", writeToken);
                this.ReportProgress("Could not finalize change", writeToken);
                throw Error("Could not finalize change");
            }
        } catch (error) {
            this.Error("Could not edit Variant", error);
            this.releaseMutex();
            return ElvOAction.EXECUTION_EXCEPTION;
        }
        this.releaseMutex();
        return ElvOAction.EXECUTION_COMPLETE;
    };

    async executeRemove(handle, outputs, client) {

        const objectId = this.Payload.inputs.production_master_object_id;
        const libraryId = await this.getLibraryId(objectId, client);
        const streamKey = this.Payload.inputs.stream_key;
        const variantKey = this.Payload.inputs.variant_key;

        try {
            this.ReportProgress("Retrieving data for stream '" + streamKey + "' in variant '" + variantKey + "'... ");

            let variants = await this.getMetadata({libraryId: libraryId, objectId: objectId, client, metadataSubtree: "production_master/variants"});
            if (!variants) {
                this.ReportProgress("Could not read variant from "+objectId);
                return ElvOAction.EXECUTION_EXCEPTION;
            }
            if (!variants[variantKey]) {
                this.ReportProgress("Could not read variant '"+ variantKey +"' from "+objectId);
                return ElvOAction.EXECUTION_EXCEPTION;
            }


            let variantStreams = variants[variantKey].streams;

            if(!variantStreams.hasOwnProperty(streamKey)) {
                this.Error("Stream '" + streamKey + "' not found in variant '" + variantKey + "'");
                this.ReportProgress("Stream '" + streamKey + "' not found in variant '" + variantKey + "'");
                return ElvOAction.EXECUTION_FAILED;
            }
            delete variants[variantKey].streams[streamKey];

            // write back to object
            let writeToken = await this.getWriteToken({
                libraryId: libraryId,
                objectId: objectId,
                force: true,
                client
            });
            this.ReportProgress("Acquired write-token " + writeToken);
            await client.ReplaceMetadata({
                libraryId: libraryId,
                objectId: objectId,
                writeToken: writeToken,
                metadataSubtree: "production_master/variants",
                metadata: variants,
            });
            this.ReportProgress("Removing stream " + streamKey + " to variant " + variantKey + "... ");
            let msg = "Removed stream '" + streamKey + "' to variant '" + variantKey + "'... ";
            let response = await this.FinalizeContentObject({
                libraryId: libraryId,
                objectId: objectId,
                writeToken: writeToken,
                commitMessage: msg,
                client
            });
            this.ReportProgress(msg);

            if (response.hash) {
                outputs.production_master_version_hash = response.hash;
            } else {
                this.Error("Could not finalize change", writeToken);
                this.ReportProgress("Could not finalize change", writeToken);
                return ElvOAction.EXECUTION_EXCEPTION;
            }
        } catch (error) {
            this.Error("Could not edit Variant", error);
            return ElvOAction.EXECUTION_EXCEPTION;
        }
        return ElvOAction.EXECUTION_COMPLETE;
    };

    async Execute(handle, outputs) {
        try {

            let client;
            if (!this.Payload.inputs.private_key && !this.Payload.inputs.config_url){
                client = this.Client;
            } else {
                let privateKey = this.Payload.inputs.private_key || this.Client.signer.signingKey.privateKey.toString();
                let configUrl = this.Payload.inputs.config_url || this.Client.configUrl;
                client = await ElvOFabricClient.InitializeClient(configUrl, privateKey)
            }

            if (this.Payload.parameters.action == "REMOVE") {
                return await this.executeRemove(handle, outputs, client);
            }
            if (this.Payload.parameters.action == "ADD") {
                return await this.executeAdd(handle, outputs, client);
            }
            if (this.Payload.parameters.action == "EDIT") {
                return await this.executeEdit(this.Payload.inputs, outputs, client);
            }
        } catch (error) {
            this.Error("Could not execute Variant operation", error);
            return ElvOAction.EXECUTION_EXCEPTION;
        }
    };
    static VERSION = "0.1.1";
    static REVISION_HISTORY = {
        "0.0.1": "Initial release",
        "0.0.2": "Adds support for 2CHANNELS_1STEREO",
        "0.0.3": "Adds support for edit_if_present",
        "0.0.4": "Private key input is encrypted",
        "0.0.5": "Use reworked finalize method",
        "0.0.6": "Adds some progress reporting",
        "0.0.7": "Forces clean-up of pending hash",
        "0.0.8": "Adds support for REMOVE action",
        "0.0.9": "Fails if no audio stream is provided",
        "0.1.0": "Broaden EDIT features",
        "0.1.1": "Adds support for mutex protected edits"
    };
}


if (ElvOAction.executeCommandLine(ElvOActionVariantEditStream)) {
    ElvOAction.Run(ElvOActionVariantEditStream);
} else {
    module.exports=ElvOActionVariantEditStream;
}
