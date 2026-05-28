const ElvOAction = require("../o-action").ElvOAction;
const ElvOFabricClient = require("../o-fabric");
const ElvOMutex = require("../o-mutex");
const fetch = require('node-fetch');
const fs = require('fs');
const Path = require('path');
const { type } = require("os");

class ElvOActionManageMezzanine extends ElvOAction  {
    ActionId() {
        return "manage_mezzanine";
    };
    
    
    Parameters() {
        return {
            parameters: {
                action: {
                    type: "string", 
                    values:[
                        "UNIFY_AUDIO_DRM_KEYS", "CLIP", "ADD_CLEAR_OFFERING", "REMOVE_PLAYOUT_FORMATS_FROM_OFFERING",
                        "COPY_ENTRY_EXIT_POINT_ACCROSS_OFFERINGS",
                        "REMOVE_OFFERING",
                        "REMOVE_STREAM",
                        "LINK_PLAYOUT_BETWEEN_OFFERINGS",
                        "READ_OFFERING",
                        "DOWNLOAD_MEDIA",
                        "FINALIZE",
                        "COPY_STREAMS_BETWEEN_OBJECTS", "COPY_STREAMS_FROM_VERSION",
                        "COPY_RUNGS_BETWEEN_OBJECTS",
                        "COPY_OFFERINGS_AND_COMBINE_ALL_STREAMS",
                        "CLEAN_UP_STREAMS"
                    ], 
                    required: true
                },
                identify_by_version: {type: "boolean", required:false, default: false}
            }
        };
    };
    
    IOs(parameters) {
        let inputs = {
            private_key: {type: "password", "required":false},
            config_url: {type: "string", "required":false}
        };
        let outputs = {};
        if (parameters.action  == "COPY_OFFERINGS_AND_COMBINE_ALL_STREAMS") {
            inputs.copy_offerings_from = {type: "string", required: true}; //we could have it optional if offerings are same in all
            inputs.mezzanine_object_ids =  {type: "array", required: true};
            inputs.offering_keys = {type: "array", required: false};
            outputs.mezzanine_object_version_hash = {type: "object", required: true}; //indexed by objectId
        }
        if (parameters.action  == "CLEAN_UP_STREAMS") {
            inputs.mezzanine_object_id =  {type: "string", required: true};
            inputs.offering =  {type: "string", required: false, default: "default"};
        }
        //input.download.default[\"/\"] = \"/qfab/\" + inputs.clip_mezzanine_version_hash + \"/rep/media_download/default/video_1920x1080@9500000\";" +
        if (parameters.action == "COPY_STREAMS_BETWEEN_OBJECTS"){
            inputs.source_mezzanine_object_version_hash =  {type: "string", required: true};
            inputs.source_mezzanine_object_id =  {type: "string", required: true};
            inputs.target_mezzanine_object_id =  {type: "string", required: true};
            inputs.write_token =  {type: "string", required: false}; //TO DO - Add option to work on a right token and make finalizing optional
            inputs.stream_keys  =  {type: "array", required: false, default: null};
            inputs.offering_key =  {type: "string", required: false};
            inputs.source_offering_key =  {type: "string", required: false, default: null};
            inputs.target_offering_key =  {type: "string", required: false, default: null};
            inputs.finalize =  {type: "boolean", required: false, default: true};
            outputs.version_hash = {type: "string"};
            outputs.streams_imported = {type: "array"};
            outputs.transcode_imported = {type: "array"};
            outputs.write_token = {type:"string", conditional: true};
            outputs.node_url = {type:"string", conditional: true};
            outputs.config_url = {type:"string", conditional: true};
            outputs.commit_message = {type:"string", conditional: true};
        }
        if (parameters.action == "COPY_STREAMS_FROM_VERSION"){
            inputs.source_mezzanine_version_hash =  {type: "string", required: true};
            inputs.target_mezzanine_object_id =  {type: "string", required: true};
            inputs.write_token =  {type: "string", required: false}; //TO DO - Add option to work on a right token and make finalizing optional
            inputs.stream_keys =  {type: "array", required: false, default: null};
            inputs.offering_key =  {type: "string", required: false};
            inputs.source_offering_key =  {type: "string", required: false, default: null};
            inputs.target_offering_key =  {type: "string", required: false, default: null};
            inputs.finalize =  {type: "boolean", required: false, default: true};
            outputs.version_hash = {type: "string"};
            outputs.streams_imported = {type: "array"};
            outputs.transcode_imported = {type: "array"};
            outputs.write_token = {type:"string", conditional: true};
            outputs.node_url = {type:"string", conditional: true};
            outputs.config_url = {type:"string", conditional: true};
            outputs.commit_message = {type:"string", conditional: true};
        }
        if (parameters.action == "COPY_RUNGS_BETWEEN_OBJECTS") {
            inputs.source_mezzanine_object_id =  {type: "string", required: true};
            inputs.target_mezzanine_object_id =  {type: "string", required: true};
            inputs.offering_key =  {type: "string", required: false, default: "default"};
            inputs.rung_keys = {type: "array", required: false, default: null};
            inputs.write_token =  {type: "string", required: true};
            inputs.finalize =  {type: "boolean", required: false, default: true};
            inputs.baba =  {type: "string", required: false, default: "ZOB"};
            outputs.version_hash = {type: "string"};
            outputs.rungs_imported = {type: "array"};
            outputs.transcode_imported = {type: "array"};
            outputs.write_token = {type:"string", conditional: true};
            outputs.node_url = {type:"string", conditional: true};
            outputs.config_url = {type:"string", conditional: true};
            outputs.commit_message = {type:"string", conditional: true};
        }
        
        if (parameters.action == "FINALIZE"){
            inputs.mezzanine_object_id =  {type: "string", required: true};
            inputs.write_token =  {type: "string", required: true};
            inputs.downloadable_suffix =  {type: "string", required: false};
            inputs.offering_key =  {type: "string", required: false, default: "default"};
        }
        if (parameters.action == "DOWNLOAD_MEDIA"){
            if (!parameters.identify_by_version) {
                inputs.mezzanine_object_id =  {type: "string", required: true};
            } else {
                inputs.mezzanine_object_version_hash = {type: "string", required: true};
            }
            inputs.offering = {type: "string", required: false, default: "default"};
            inputs.video_representation = {type: "string", required: false, default: null};
            inputs.video_resolution = {type: "string", required: false, default: null};
            inputs.audio_representation = {type: "string", required: false, default: null};
            inputs.audio_label = {type: "string", required: false, default: null};
            inputs.audio_language_code = {type: "string", required: false, default: null};
            inputs.audio_stream_key = {type: "string", required: false, default: null};
            inputs.target_path = {type: "string", required: false, default: null};
            outputs.target_path = {type: "string"};
            outputs.download_url = {type: "string"};
        }
        if (parameters.action == "READ_OFFERING")  {
            inputs.offering = {type: "string"};
            if (!parameters.identify_by_version) {
                inputs.mezzanine_object_id =  {type: "string", required: true};
            } else {
                inputs.mezzanine_object_version_hash = {type: "string", required: true};
            }
            outputs.value = {type: "string"};
            outputs.video_representation = {type: "string"};
        }
        if ((parameters.action == "LINK_PLAYOUT_BETWEEN_OFFERINGS") || (parameters.action == "COPY_ENTRY_EXIT_POINT_ACCROSS_OFFERINGS")) {
            inputs.safe_update = {type: "boolean", required: false, default: false};
            inputs.source_offering = {type: "string", required: false, default: "default"};
            inputs.target_offering = {type: "string", required: false, default: null};
            if (!parameters.identify_by_version) {
                inputs.mezzanine_object_id =  {type: "string", required: true};
            } else {
                inputs.mezzanine_object_version_hash = {type: "string", required: true};
            }
            outputs.mezzanine_object_version_hash = {type: "string"};
        }
        if (parameters.action == "UNIFY_AUDIO_DRM_KEYS") {
            inputs.safe_update = {type: "boolean", required: false, default: false};
            if (!parameters.identify_by_version) {
                inputs.mezzanine_object_id =  {type: "string", required: true};
            } else {
                inputs.mezzanine_object_version_hash = {type: "string", required: true};
            }
            inputs.offering_key = {type: "string", required: false};
            outputs.mezzanine_object_version_hash = {type: "string"};
        }
        if (parameters.action == "CLIP") {
            inputs.safe_update = {type: "boolean", required: false, default: false};
            if (!parameters.identify_by_version) {
                inputs.mezzanine_object_id =  {type: "string", required: true};
            } else {
                inputs.mezzanine_object_version_hash = {type: "string", required: true};
            }
            inputs.offering = {type: "string", required: false, default: "default"};
            inputs.entry_point_sec = {type: "numeric", required: false, default: null};
            inputs.entry_point_rat = {type: "string", required: false, default: null};
            inputs.exit_point_sec = {type: "numeric", required: false, default: null};
            inputs.exit_point_rat = {type: "string", required: false, default: null};
            outputs.mezzanine_object_version_hash = {type: "string"};
        }
        if (parameters.action == "ADD_CLEAR_OFFERING") {
            inputs.safe_update = {type: "boolean", required: false, default: false};
            if (!parameters.identify_by_version) {
                inputs.mezzanine_object_id =  {type: "string", required: true};
            } else {
                inputs.mezzanine_object_version_hash = {type: "string", required: true};
            }
            inputs.source_offering = {type: "string", required: false, default: "default"};
            inputs.playout_formats = {type: "array", required: false, default: ["dash-clear", "hls-clear"]};
            inputs.offering = {type: "string", required: false, default: "default"};
            outputs.mezzanine_object_version_hash = {type: "string"};
        }
        if (parameters.action == "REMOVE_PLAYOUT_FORMATS_FROM_OFFERING") {
            inputs.safe_update = {type: "boolean", required: false, default: false};
            if (!parameters.identify_by_version) {
                inputs.mezzanine_object_id =  {type: "string", required: true};
            } else {
                inputs.mezzanine_object_version_hash = {type: "string", required: true};
            }
            inputs.offering = {type: "string", required: false, default: "default"};
            inputs.playout_formats = {type: "array", required: true};
            outputs.mezzanine_object_version_hash = {type: "string"};
        }
        if  (parameters.action == "REMOVE_STREAM") {
            inputs.safe_update = {type: "boolean", required: false, default: false};
            if (!parameters.identify_by_version) {
                inputs.mezzanine_object_id =  {type: "string", required: true};
            } else {
                inputs.mezzanine_object_version_hash = {type: "string", required: true};
            }
            inputs.offering = {type: "string", required: false, default: "default"};
            inputs.stream_key = {type: "string", required: true};
            outputs.mezzanine_object_version_hash = {type: "string"};
            outputs.modified_offerings = {type: "array"};
        }
        if  (parameters.action == "REMOVE_OFFERING") {
            inputs.safe_update = {type: "boolean", required: false, default: false};
            if (!parameters.identify_by_version) {
                inputs.mezzanine_object_id =  {type: "string", required: true};
            } else {
                inputs.mezzanine_object_version_hash = {type: "string", required: true};
            }
            inputs.offering = {type: "string", required: false, default: "default"};
            outputs.mezzanine_object_version_hash = {type: "string"};
            outputs.removed_offerings = {type: "array"};
        }        
        return {inputs, outputs};
    };
    
    async Execute(inputs, outputs) {
        try {
            let client;
            let privateKey;
            let configUrl;
            if (!this.Payload.inputs.private_key && !this.Payload.inputs.config_url){
                client = this.Client;
            } else {
                privateKey = this.Payload.inputs.private_key || this.Client.signer.signingKey.privateKey.toString();
                configUrl = this.Payload.inputs.config_url || this.Client.configUrl;
                client = await ElvOFabricClient.InitializeClient(configUrl, privateKey)
            }
            if (this.Payload.parameters.action == "COPY_STREAMS_BETWEEN_OBJECTS") {
                return await this.executeCopyStreamsBetweenObjects(client, inputs, outputs);
            }
            if (this.Payload.parameters.action == "COPY_STREAMS_FROM_VERSION") {
                return await this.executeCopyStreamsFromVersion(client, inputs, outputs);
            }
            if (this.Payload.parameters.action == "COPY_RUNGS_BETWEEN_OBJECTS") {
                return await this.executeCopyRungsBetweenObjects(client, inputs, outputs);
            }
            if (this.Payload.parameters.action == "CLEAN_UP_STREAMS") {
                return await this.executeCleanUpStreams(client, inputs, outputs);
            }
            
            let objectId = this.Payload.inputs.mezzanine_object_id;
            let versionHash = this.Payload.inputs.mezzanine_object_version_hash;
            if (!versionHash) {
                versionHash = await this.getVersionHash({objectId, client});
                this.Debug("Mezzanine object version hash: " + versionHash, objectId);
            } else {
                objectId = client.utils.DecodeVersionHash(versionHash).objectId;
            }
            let libraryId = await this.getLibraryId(objectId, client);
            if (this.Payload.parameters.action == "FINALIZE"){
                let writeToken = inputs.write_token;
                return await this.executeFinalize({objectId, libraryId, writeToken, client, inputs, outputs});
            }
            if (this.Payload.parameters.action == "DOWNLOAD_MEDIA"){
                //input.download.default[\"/\"] = \"/qfab/\" + inputs.clip_mezzanine_version_hash + \"/rep/media_download/default/video_1920x1080@9500000\";" +
                return await this.executeDownloadMedia({objectId, libraryId, versionHash, client, inputs, outputs});
            }
            if (this.Payload.parameters.action == "COPY_ENTRY_EXIT_POINT_ACCROSS_OFFERINGS"){
                return await this.executeCopyEntryExitPointAccrossOfferings({objectId, libraryId, versionHash, client}, outputs);
            }
            if (this.Payload.parameters.action == "LINK_PLAYOUT_BETWEEN_OFFERINGS"){
                return await this.executeLinkPlayoutOfferings({objectId, libraryId, versionHash, client}, outputs);
            }
            if (this.Payload.parameters.action == "UNIFY_AUDIO_DRM_KEYS") {
                return await this.executeUnifyAudioDRMKeys({objectId, libraryId, versionHash, client}, outputs);
            }
            if (this.Payload.parameters.action == "CLIP") {
                return await this.executeClipMezzanine({objectId, libraryId, versionHash, client}, outputs);
            }
            if (this.Payload.parameters.action == "ADD_CLEAR_OFFERING") {
                return await this.addClearOffering({objectId, libraryId, versionHash, client}, outputs);
            }
            if (this.Payload.parameters.action == "REMOVE_PLAYOUT_FORMATS_FROM_OFFERING") {
                return await this.removePlayoutFormatsFromOffering({objectId, libraryId, versionHash, client}, outputs);
            }
            if (this.Payload.parameters.action == "REMOVE_STREAM") {
                return await this.removeStream({objectId, libraryId, versionHash, client}, outputs);
            }
            if (this.Payload.parameters.action == "REMOVE_OFFERING") {
                return await this.removeOffering({objectId, libraryId, versionHash, client}, outputs);
            }
            if (this.Payload.parameters.action == "READ_OFFERING")  {
                return await this.readOffering({objectId, libraryId, versionHash, client}, outputs);
            }
            if (this.Payload.parameters.action == "COPY_OFFERINGS_AND_COMBINE_ALL_STREAMS") {
                return await this.executeCopyOfferingsAndCombineAllStreams(client, inputs, outputs);
            }
        } catch(errExecute) {
            this.releaseMutex();
            this.Error("Execution error", errExecute);
            return ElvOAction.EXECUTION_EXCEPTION;
        }
    };
    
    
    async executeLinkPlayoutOfferings({objectId, libraryId, versionHash, client}, outputs) {
        await this.acquireMutex(objectId);
        let offerings = await this.getMetadata({objectId, libraryId, versionHash, client, metadataSubtree: "offerings", resolve: false});
        
        if (!offerings || (Object.keys(offerings).length == 0)) {
            throw new Error("no offerings found in metadata");
        }
        // loop through offerings
        let sourceOffering = this.Payload.inputs.source_offering;
        let targetOffering = this.Payload.inputs.target_offering;
        if (!offerings[sourceOffering]) {
            throw new Error("Source offering " +sourceOffering + " not found in metadata");
        }
        let  changed = 0;
        for (let offeringKey in offerings) {
            if ((offeringKey == sourceOffering)  || (targetOffering && (targetOffering != offeringKey))){
                continue;
            }
            let offering = offerings[offeringKey];
            this.reportProgress(`Linking offering ${offeringKey}...`);
            let streamLink = "./meta/offerings/" + sourceOffering + "/playout/streams";
            let structLink = "./meta/offerings/" + sourceOffering + "/media_struct/streams";
            if (offering.playout.streams && offering.media_struct.streams && ((offering.playout.streams["/"] != streamLink) || (offering.media_struct.streams["/"] != structLink))) {
                offering.playout.streams = {
                    ".": {},
                    "/": streamLink
                };
                offering.media_struct.streams = {
                    ".": {},
                    "/": structLink
                };
                changed++;
            }
        }
        if (changed == 0) {
            outputs.mezzanine_object_version_hash = versionHash;
            this.ReportProgress("No changes to make");
            this.releaseMutex();
            return ElvOAction.EXECUTION_FAILED;
        }
        let writeToken = await this.getWriteToken({
            libraryId: libraryId,
            objectId: objectId,
            versionHash,
            client: client
        });
        
        await client.ReplaceMetadata({
            objectId,
            libraryId,
            metadataSubtree: "offerings", 
            writeToken,
            metadata: offerings,
            client
        });
        
        let msg = "Linked playout streams to offering " + sourceOffering;
        let response = await this.FinalizeContentObject({
            libraryId: libraryId,
            objectId: objectId,
            writeToken: writeToken,
            commitMessage: msg,
            client
        });
        if (response && response.hash) {
            this.ReportProgress(msg)
        } else {
            throw new Error("Failed to save changes to offerings");
        }
        outputs.mezzanine_object_version_hash = response.hash;
        this.releaseMutex();
        return ElvOAction.EXECUTION_COMPLETE;        
        
    };
    
    findTopVideoRepresentation(offering, {resolution}) {
        let representations = Object.keys(offering.playout.streams.video.representations);
        let index = {};
        let topRate = 0;
        if (resolution) {
            let matcher = resolution.match(/([0-9]+)x([0-9]+)/);
            let w =  parseInt(matcher[1]);
            let h =  parseInt(matcher[2]);
            let p = w * h;
            let first = null;
            for (let representation of representations) {
                let matcher = representation.match(/videovideo_([0-9]+)x([0-9]+)_.*@([0-9]+)/);
                if (!matcher) {
                    continue;
                }
                let bitrate = parseInt(matcher[3]);
                let width = parseInt(matcher[1]);
                let height = parseInt(matcher[2]);
                let pixels = width * height;
                if (pixels < p) {
                    continue;
                }
                if (!first || (first > pixels))  {
                    first = pixels;
                }
                if (!index[pixels] || index[pixels].bitrate < bitrate) {
                    index[pixels] = {bitrate, pixels, representation};
                }
            }
            return index[first].representation;
        }
        for (let representation of representations) {
            let matcher = representation.match(/videovideo_([0-9]+)x([0-9]+)_.*@([0-9]+)/);
            if (!matcher) {
                continue;
            }
            let bitrate = parseInt(matcher[3]);
            let width = parseInt(matcher[1]);
            let height = parseInt(matcher[2]);
            let pixels = width * height;
            if (!index[bitrate] || index[bitrate].pixels < pixels) {
                index[bitrate] = {bitrate, pixels, representation};
            }
            if (bitrate > topRate) {
                topRate = bitrate;
            }
        }
        return index[topRate].representation;
    };
    
    findAudioRepresentation(offering, {label, language, key}) {
        if (label) {
            for (let streamId in offering.media_struct.streams) {
                let stream = offering.media_struct.streams[streamId];
                if ((stream.codec_type == "audio") && (stream.label == label)) {
                    key = streamId;
                    break;
                }
            }
            if (!key) {
                this.ReportProgress("Audio representation label not found", label);
                return null;
            }
        }
        if (language) {
            for (let streamId in offering.media_struct.streams) {
                let stream = offering.media_struct.streams[streamId];
                if ((stream.codec_type == "audio") && (stream.language == language)) {
                    key = streamId;
                    break;
                }
            }
            if (!key) {
                this.ReportProgress("Audio representation language code not found", language);
                return null;
            }
        }
        if (key) {
            for (let streamId in offering.playout.streams) {
                let stream = offering.playout.streams[streamId];
                for (let representationId in stream.representations) {
                    let representation = stream.representations[representationId];
                    if ((representation.type == "RepAudio") && (representation.media_struct_stream_key == key)) {
                        return representationId;
                    }
                }
            }
            this.ReportProgress("Audio representation key not found", key);
            return null;
        }
        
    }
    
    async downloadFile(url, path){
        const res = await fetch(url);
        const fileStream = fs.createWriteStream(path);
        let downloaded = 0;
        let pollingInterval = this.PollingInterval() * 500;
        let lastPoll = 0;
        await new Promise((resolve, reject) => {
            res.body.pipe(fileStream);
            res.body.on("error", reject);
            res.body.on('data', (data) => {
                downloaded += data.length;
                let now = (new Date()).getTime();
                if ((now - lastPoll) >= pollingInterval) {
                    lastPoll = now;
                    this.ReportProgress("Downloading... ", downloaded);
                }
            });
            fileStream.on("finish", resolve);
        });
        console.log("Done downloading " + downloaded + " Bytes to "+path);
    };
    
    async readCaps(client, metadata) {
        let outputs = {};
        //const permission = await this.Permission({objectId: originalObjectId});
        // User CAP
        const userCapKey = `eluv.caps.iusr${client.utils.AddressToHash(client.signer.address)}`;
        outputs.user_cap_key = userCapKey;
        if (metadata[userCapKey]) {
            const userConkKey = await client.Crypto.DecryptCap(metadata[userCapKey], this.getPrivateKey(client) );
            outputs.user_conk_key = userConkKey;      
            return outputs;
        } else {
            this.ReportProgress("No caps found matching key", userCapKey);
            return null;
        }
    };
    
    
    async executeCleanUpStreams(client, inputs, outputs) { 
        let libraryId = await this.getLibraryId(inputs.mezzanine_object_id, client);
        let offering = await this.getMetadata({ client, objectId: inputs.mezzanine_object_id, metadataSubtree: "offerings/"+inputs.offering});
        let changed= false;
        for (let streamKey in offering.playout.streams) {
            if (!offering.media_struct.streams[streamKey]) {
                this.reportProgress("Removing playout stream "+ streamKey);
                delete offering.playout.streams[streamKey];
                changed = true;
            }
        }
        if (!changed) {
            return ElvOAction.EXECUTION_FAILED
        }
        let writeToken = await this.getWriteToken({client, objectId: inputs.mezzanine_object_id, libraryId});
        await client.ReplaceMetadata({writeToken, objectId: inputs.mezzanine_object_id, libraryId, metadataSubtree: "offerings/"+inputs.offering, metadata: offering});
        let result = await this.FinalizeContentObject({writeToken, objectId: inputs.mezzanine_object_id, libraryId, client, commitMessage: "Clean up playout/mediastruct"})
        if (result.hash) {
            outputs.mezzanine_object_version_hash = result.hash;
            return ElvOAction.EXECUTION_COMPLETE;
        }
    }
    
    compareRat(rat1, rat2){
        if (!this.Rats) this.Rats={};
        let num1 = this.Rats[rat1];
        let num2 = this.Rats[rat2]
        if (this.Rats[rat1] == null) {
            if ((typeof rat1) == "number") this.Rats[rat1] = rat1;
            else {
                if (rat1.match(/^[0-9/]+$/)) this.Rats[rat1] = eval(rat1);
                else throw "Not a rat "+rat1;
            }
        }
        if (this.Rats[rat2] == null) {
            if ((typeof rat2) == "number") this.Rats[rat2] = rat2;
            else {
                if (rat2.match(/^[0-9/]+$/)) this.Rats[rat2] = eval(rat2);
                else throw "Not a rat "+rat2;
            }
        }
        return (this.Rats[rat1] > this.Rats[rat2]);
    }
    largerRat(rat1, rat2) {
        return this.compareRat(rat1, rat2) ? rat1 :  rat2
    }
    
    async executeCopyRungsBetweenObjects(client, inputs, outputs) { //COPY_RUNGS_BETWEEN_OBJECTS
        console.log("inputs", inputs);
        if (!inputs.source_offering_key) {
            inputs.source_offering_key = inputs.offering_key;
        } 
        if (!inputs.target_offering_key) {
            inputs.target_offering_key = inputs.offering_key || inputs.source_offering_key;
        }
        let metadata = await this.getMetadata({ client, objectId: inputs.source_mezzanine_object_id});
        let sourceCap = await this.readCaps(client, metadata); 
        
        let offering = metadata.offerings[inputs.source_offering_key];
        let objectId = inputs.target_mezzanine_object_id;
        let libraryId = await this.getLibraryId(objectId, client); 
        let writeToken = inputs.write_token || await this.getWriteToken({client, objectId, libraryId});
        let targetMetadata = await this.getMetadata({client, objectId, libraryId, writeToken});
        
        let targetCap = await this.readCaps(client, targetMetadata);
        
        if (targetCap && (targetCap.user_conk_key.secret_key != sourceCap.user_conk_key.secret_key)) {
            throw "Caps in target object are incompatible with source object. Import caps first"; 
        }    
        if (!targetMetadata.offerings || !targetMetadata.offerings[inputs.target_offering_key] || !targetMetadata.offerings[inputs.target_offering_key].media_struct.streams.video){
            inputs.stream_keys = ["video"];
            this.reportProgress("No existing video, using copying streams instead"); 
            return await this.executeCopyStreamsBetweenObjects(client, inputs, outputs); //Questionable as only designated rung should be copied - TO BE IMPROVED
        }
        let transcodeIds = [];
        if (!inputs.rung_keys) {
            for (let repId in offering.playout.streams.video.representations){
                let representation = offering.playout.streams.video.representations[repId];
                if (representation.transcode_matches_rep) {
                    inputs.rung_keys = [repId]
                }
            }
        }
        for (let repId of inputs.rung_keys) {
            let representation = offering.playout.streams.video.representations[repId];
            transcodeIds.push(representation.transcode_id);
        }
        
        if (!targetCap) {
            sourceCap.user_conk_key.qid = objectId;
            await client.ReplaceMetadata({
                libraryId, objectId, writeToken,
                metadataSubtree: sourceCap.user_cap_key,
                metadata: await client.Crypto.EncryptConk(sourceCap.user_conk_key, client.signer._signingKey().publicKey)
            });
        }
        let durationRat = "0";
        for (let transcodeId of transcodeIds) {
            durationRat = this.largerRat(DurationRat, metadata.transcodes[transcodeId].stream.duration.rat);
            await client.ReplaceMetadata({
                objectId, libraryId, writeToken,
                metadataSubtree: "transcodes/"+transcodeId,
                metadata: metadata.transcodes[transcodeId]
            });
            this.reportProgress("Adding transcode to target object", transcodeId);
        }
        
        let drmKeys = [];
        let streamData = offering.playout.streams.video;
        for (let scheme in (streamData.encryption_schemes || {})){
            let schemeData = streamData.encryption_schemes[scheme];
            if (schemeData?.key_id) {
                drmKeys.push(schemeData.key_id);
            }
        }
        let targetOffering = targetMetadata.offerings[inputs.target_offering_key];
        for (let key of drmKeys) {
            this.ReportProgress("Copying drm key from source", key);
            targetOffering.playout.drm_keys[key] = offering.playout.drm_keys[key];
            targetMetadata.elv.crypt.drm.kids[key] = metadata.elv.crypt.drm.kids[key];
        }
        let representations = offering.playout.streams.video.representations;
        let targetRepresentations = targetMetadata.offerings[inputs.target_offering_key].playout.streams.video.representations;
        for (let repId of inputs.rung_keys) {
            targetRepresentations[repId] = representations[repId];
            if (!targetMetadata.offerings[inputs.target_offering_key].media_struct.streams.video) {
                targetMetadata.offerings[inputs.target_offering_key].media_struct.streams.video = offering.media_struct.streams.video;
            } else {
                if (targetMetadata.offerings[inputs.target_offering_key].media_struct.streams.video.width < representations[repId].width) {
                    targetMetadata.offerings[inputs.target_offering_key].media_struct.streams.video = offering.media_struct.streams.video;
                }
            }
        }
        targetMetadata.offerings[inputs.target_offering_key].media_struct.duration_rat = this.largerRat(durationRat, targetMetadata.offerings[inputs.target_offering_key].media_struct.duration_rat);
        await client.ReplaceMetadata({
            objectId, libraryId, writeToken,
            metadataSubtree: "elv",
            metadata: targetMetadata.elv
        });
        await client.ReplaceMetadata({
            objectId, libraryId, writeToken,
            metadataSubtree: "offerings/"+inputs.target_offering_key,
            metadata: targetOffering 
        });
        this.reportProgress("Replaced offering in target", inputs.offering_key);
        outputs.rungs_imported = inputs.rung_keys;
        outputs.transcode_imported = transcodeIds;
        if (inputs.finalize != false) {
            let result = await this.FinalizeContentObject({
                objectId, libraryId, writeToken, client,
                commitMessage: "Imported rungs from "+inputs.source_mezzanine_object_id
            });
            if (result?.hash) {
                outputs.version_hash = result?.hash;
                return ElvOAction.EXECUTION_COMPLETE;           
            } else {
                return ElvOAction.EXECUTION_EXCEPTION;
            }
        } else {
            outputs.write_token = writeToken;
            if (client.HttpClient.draftURIs[writeToken]) {
                outputs.node_url = "https://" + client.HttpClient.draftURIs[writeToken].hostname() + "/";
                outputs.config_url = "https://" + client.HttpClient.draftURIs[writeToken].hostname() + "/config?self&qspace=main";
                outputs.commit_message = "Imported rungs from " + inputs.source_mezzanine_object_id;
            } else {
                throw new Error("Could not get node for writeToken");
            }
            return ElvOAction.EXECUTION_COMPLETE;  
        }
    }
    
    async executeCopyOfferingsAndCombineAllStreams(client, inputs, outputs) { // COPY_OFFERINGS_AND_COMBINE_ALL_STREAMS
        let meta = {};
        let mergedOfferings = {};
        let streams = {};
        let all_offerings = {};
        let all_streams = {};
        let all_transcodes = {};
        let objectStatus = {};
        outputs.mezzanine_object_version_hash = {};
        meta[inputs.copy_offerings_from] = await this.getMetadata({objectId: inputs.copy_offerings_from, client});
        let sourceCap = await this.readCaps(client, meta[inputs.copy_offerings_from]); 
        let sourceOfferings = meta[inputs.copy_offerings_from].offerings;
        let offeringIds = inputs.offerings || Object.keys(sourceOfferings);
        for (let objectId of inputs.mezzanine_object_ids) {
            meta[objectId] = await this.getMetadata({objectId, client});
            streams[objectId] = {};
            for (let offeringId of offeringIds) {
                if (meta[objectId].offerings && meta[objectId].offerings[offeringId] && meta[objectId].offerings[offeringId].media_struct) {
                    streams[objectId][offeringId] = Object.keys(meta[objectId].offerings[offeringId].media_struct.streams);
                    for (let streamId of streams[objectId][offeringId]) {
                        if (!all_streams[streamId]) all_streams[streamId] = [];
                        all_streams[streamId].push({object_id: objectId, offering: offeringId});
                        for (let repId in meta[objectId].offerings[offeringId].playout.streams[streamId].representations){
                            let representation = meta[objectId].offerings[offeringId].playout.streams[streamId].representations[repId];
                            if (representation.transcode_id){
                                all_transcodes[representation.transcode_id] = meta[objectId].transcodes[representation.transcode_id];
                            }
                        }
                    }
                }
                if (!mergedOfferings[objectId]) mergedOfferings[objectId] = {};
                mergedOfferings[objectId][offeringId] = sourceOfferings[offeringId];
            }
        }
        for (let sourceOffering in sourceOfferings) {
            let offering = sourceOfferings[sourceOffering];
            for (let streamId in all_streams) {
                if (!offering.media_struct.streams[streamId]) {
                    let source = all_streams[streamId][0];
                    this.reportProgress("Adding stream "+ streamId + " to reference offering "+ sourceOffering, {source});
                    offering.media_struct.streams[streamId] = meta[source.object_id].offerings[source.offering].media_struct.streams[streamId];
                    offering.playout.streams[streamId] = meta[source.object_id].offerings[source.offering].playout.streams[streamId];
                    objectStatus[inputs.copy_offerings_from] = true;
                }
            }
        }
        for (let objectId of inputs.mezzanine_object_ids) {
            for (let offeringId in sourceOfferings) {
                let offering = sourceOfferings[offeringId];
                for (let streamId in offering.media_struct.streams) {
                    if (!meta[objectId].offerings[offeringId]) {
                        this.reportProgress("Offering "+ offeringId + " is new to "+ objectId);
                        objectStatus[objectId] = true;
                        break;
                    }
                    let originalStream = meta[objectId].offerings[offeringId].media_struct.streams[streamId];
                    if (!originalStream) {
                        this.reportProgress("Offering "+ offeringId + "in "+ objectId +" is missing stream", streamId);
                        objectStatus[objectId] = true;
                        break;
                    }
                    let stream = offering.media_struct.streams[streamId];
                    if ((originalStream.label != stream.label) || (originalStream.aspect_ratio != stream.aspect_ratio)) {
                        this.reportProgress("Offering "+ offeringId + "in "+ objectId +" has a modified stream", streamId);
                        objectStatus[objectId] = true;
                        break;
                    }
                }
            }
        }
        let allGood = true;
        console.log("PEEK sourceCap", sourceCap);
        
        for (let objectId in objectStatus) {
            try {
                if (!objectStatus[objectId]) {
                    this.reportProgress("Mezzanine "+ objectId + " is unchanged. Skipping update...");
                } else {
                    let targetCap = await this.readCaps(client, meta[objectId]);
                    console.log("PEEK targetCap", targetCap);
                    if (targetCap && (targetCap.user_conk_key.secret_key != sourceCap.user_conk_key.secret_key)) {
                        throw "Caps in target object are incompatible with source object. Import caps first into "+objectId; 
                    }       
                    
                    let libraryId = await this.getLibraryId(objectId, client);
                    let writeToken = await this.getWriteToken({objectId, libraryId, client});
                    if (!targetCap) {
                        sourceCap.user_conk_key.qid = objectId;
                        await client.ReplaceMetadata({
                            libraryId, objectId, writeToken,
                            metadataSubtree: sourceCap.user_cap_key,
                            metadata: await client.Crypto.EncryptConk(sourceCap.user_conk_key, client.signer._signingKey().publicKey)
                        });
                        this.reportProgress("Copying source caps into "+ objectId);
                    }
                    for (let transcodeId in all_transcodes) {
                        if (!meta[objectId].transcodes) meta[objectId].transcodes = {};
                        meta[objectId].transcodes[transcodeId] = all_transcodes[transcodeId];
                    }
                    await client.ReplaceMetadata({
                        objectId, libraryId, writeToken,
                        metadataSubtree: "transcodes",
                        metadata: meta[objectId].transcodes 
                    });
                    for (let offeringId in sourceOfferings) {
                        await client.ReplaceMetadata({
                            objectId, libraryId, writeToken,
                            metadataSubtree: "offerings/"+offeringId,
                            metadata: sourceOfferings[offeringId] 
                        });
                    }
                    let result = await this.FinalizeContentObject({client, objectId, libraryId, writeToken, commitMessage: "Synched with compatible mezz"});
                    if (result?.hash) {
                        outputs.mezzanine_object_version_hash[objectId] = result?.hash
                    } else {
                        allGood = false;
                    }
                }
            } catch (errMezz) {
                this.Error("Could not update "+ objectId, errMezz);
                allGood = false;
            }
        }
        if (allGood) {
            return ElvOAction.EXECUTION_COMPLETE
        }
        return ElvOAction.EXECUTION_EXCEPTION;
        
    }
    
    async executeCopyStreamsBetweenObjects(client, inputs, outputs) {
        let metadata = await this.getMetadata({ client, objectId: inputs.source_mezzanine_object_id, versionHash: inputs.source_mezzanine_object_version_hash});
        if (!inputs.source_offering_key) {
            inputs.source_offering_key = inputs.offering_key || "default";
        } 
        
        let sourceCap = await this.readCaps(client, metadata); 
        
        let offering = metadata.offerings[inputs.source_offering_key];
        let streamKeys = inputs.stream_keys || Object.keys(offering.playout.streams);
        let transcodeIds = [];
        let parts = [];
        let drmKeysPerStream = {};
        for (let streamKey of streamKeys) {
            let streamData = offering.playout.streams[streamKey];
            for (let scheme in (streamData.encryption_schemes || {})){
                let schemeData = streamData.encryption_schemes[scheme];
                if (schemeData?.key_id) {
                    if (!drmKeysPerStream[streamKey]) {
                        drmKeysPerStream[streamKey] = [];
                    }
                    drmKeysPerStream[streamKey].push(schemeData.key_id);
                }
            }
            for (let representationKey in streamData.representations) {
                let representation = streamData.representations[representationKey];
                if ((representation.type == "RepVideo") || (representation.type == "RepAudio")) {
                    if (representation.transcode_id){
                        transcodeIds.push(representation.transcode_id) //assumes no duplicate                       
                    }
                }
                if ((representation.type == "RepCaptions")  || (representation.type == "RepThumbnails")) {
                    let sourcePart = offering.media_struct.streams[streamKey].sources[0].source; //never seen more than one
                    parts.push(sourcePart);
                }
            }
        }
        if (streamKeys.length == 0) {
            this.ReportProgress("No streams to copy");
            return ElvOAction.EXECUTION_FAILED;
        }
        let objectId = inputs.target_mezzanine_object_id;
        let libraryId = await this.getLibraryId(objectId, client); 
        let writeToken = inputs.write_token || await this.getWriteToken({client, objectId, libraryId});
        let targetMetadata = await this.getMetadata({client, objectId, libraryId, writeToken});
        
        let targetCap = await this.readCaps(client, targetMetadata);
        
        if (targetCap && (targetCap.user_conk_key.secret_key != sourceCap.user_conk_key.secret_key)) {
            throw "Caps in target object are incompatible with source object. Import caps first"; 
        }       
        
        if (!targetCap) {
            sourceCap.user_conk_key.qid = objectId;
            await client.ReplaceMetadata({
                libraryId, objectId, writeToken,
                metadataSubtree: sourceCap.user_cap_key,
                metadata: await client.Crypto.EncryptConk(sourceCap.user_conk_key, client.signer._signingKey().publicKey)
            });
        }
        for (let transcodeId of transcodeIds) {
            await client.ReplaceMetadata({
                objectId, libraryId, writeToken,
                metadataSubtree: "transcodes/"+transcodeId,
                metadata: metadata.transcodes[transcodeId]
            });
            this.reportProgress("Adding transcode to target object", transcodeId);
        }
        let targetOfferingKeys;
        if (inputs.target_offering_key) {
            targetOfferingKeys = [inputs.target_offering_key];
        } else {
            if (!targetMetadata.offerings || (Object.keys(targetMetadata.offerings).length == 0)) {
                targetOfferingKeys = [inputs.source_offering_key];
            } else {
                targetOfferingKeys = Object.keys(targetMetadata.offerings);
            }
        }
        for (let targetOfferingKey of targetOfferingKeys) {
            let targetOffering = targetMetadata.offerings && targetMetadata.offerings[targetOfferingKey];
            if (!targetOffering) {
                this.reportProgress("Initializing offering", {source: inputs.source_offering_key, target: targetOfferingKey});
                targetOffering = JSON.parse(JSON.stringify(offering));
                targetOffering.media_struct.streams = {};
                targetOffering.playout.streams = {};
            }
            if (!targetMetadata.elv?.crypt) {
                targetMetadata.elv = metadata.elv;
                this.ReportProgress("Copying elv.crypt from source");
            }
            if (!targetMetadata.elv.crypt.drm) {
                targetMetadata.elv.crypt.drm = {};
            }
            if (!targetMetadata.elv.crypt.drm.kids) {
                targetMetadata.elv.crypt.drm.kids = {};
            }
            if (!targetMetadata.elv.crypt.drm.fps) {
                targetMetadata.elv.crypt.drm.fps = metadata.elv.crypt.drm.fps;
                this.ReportProgress("Copying elv.crypt.drm.fps from source");
            }
            if (!targetOffering.playout.drm_keys) {
                targetOffering.playout.drm_keys = {};
            }
            
            for (let streamKey of streamKeys) {
                //We need to ensure the new stream is not redundant with an existing one
                // for audio, unicity is based on channel_layout and language
                // for captions, unicity is based on language and forced
                // for video, we only keep one track
                for (let targetKey in targetOffering.media_struct.streams) {
                    let targetStream = targetOffering.media_struct.streams[targetKey];
                    let stream = offering.media_struct.streams[streamKey];
                    if (!stream || !targetStream || (stream.codec_type != targetStream.codec_type)) continue;
                    if ((stream.codec_type == "captions") && (stream.language == targetStream.language) && (stream.forced == targetStream.forced)) {
                        this.reportProgress("Removing captions stream "+targetKey +" which is replaced by "+streamKey);
                        delete targetOffering.media_struct.streams[targetKey];
                        delete targetOffering.playout.streams[targetKey];
                        break;
                    }
                    if ((stream.codec_type == "audio") && (stream.language == targetStream.language) && (stream.channel_layout == targetStream.channel_layout)) {
                        this.reportProgress("Removing audio stream "+targetKey +" which is replaced by "+streamKey);
                        delete targetOffering.media_struct.streams[targetKey];
                        delete targetOffering.playout.streams[targetKey];
                        break;
                    }
                }
                
                
                targetOffering.media_struct.streams[streamKey] = offering.media_struct.streams[streamKey];
                targetOffering.playout.streams[streamKey] = offering.playout.streams[streamKey];
                for (let key of (drmKeysPerStream[streamKey] || [])) {
                    this.ReportProgress("Copying drm key from source", key);
                    targetOffering.playout.drm_keys[key] = offering.playout.drm_keys[key];
                    targetMetadata.elv.crypt.drm.kids[key] = metadata.elv.crypt.drm.kids[key];
                }
            }
            
            await client.ReplaceMetadata({
                objectId, libraryId, writeToken,
                metadataSubtree: "offerings/"+targetOfferingKey, // inputs.target_offering_key,
                metadata: targetOffering 
            });
            this.reportProgress("Replaced offering in target", targetOfferingKey);
        }
        await client.ReplaceMetadata({
            objectId, libraryId, writeToken,
            metadataSubtree: "elv",
            metadata: targetMetadata.elv
        });
        
        outputs.streams_imported = streamKeys;
        outputs.transcode_imported = transcodeIds;
        if (inputs.finalize) {
            let result = await this.FinalizeContentObject({
                objectId, libraryId, writeToken, client,
                commitMessage: "Imported streams from "+inputs.source_mezzanine_object_id
            });
            if (result?.hash) {
                outputs.version_hash = result?.hash;
                return ElvOAction.EXECUTION_COMPLETE;           
            } else {
                return ElvOAction.EXECUTION_EXCEPTION;
            }
        } else {
            outputs.write_token = writeToken;
            if (client.HttpClient.draftURIs[writeToken]) {
                outputs.node_url = "https://" + client.HttpClient.draftURIs[writeToken].hostname() + "/";
                outputs.config_url = "https://" + client.HttpClient.draftURIs[writeToken].hostname() + "/config?self&qspace=main";
                outputs.commit_message = "Imported streams from " + inputs.source_mezzanine_object_id;
            } else {
                throw new Error("Could not get node for writeToken");
            }
            return ElvOAction.EXECUTION_COMPLETE;  
        }
        
    }
    async executeCopyStreamsFromVersion(client, inputs, outputs) { //COPY_STREAMS_FROM_VERSION
        let metadata = await this.getMetadata({ client, objectId: inputs.target_mezzanine_object_id, versionHash: inputs.source_mezzanine_version_hash});
        if (!inputs.source_offering_key) {
            inputs.source_offering_key = inputs.offering_key || "default";
        } 
        
        let sourceCap = await this.readCaps(client, metadata); 
        
        let offering = metadata.offerings[inputs.source_offering_key];
        let streamKeys = inputs.stream_keys || Object.keys(offering.playout.streams);
        let transcodeIds = [];
        let parts = [];
        let drmKeysPerStream = {};
        for (let streamKey of streamKeys) {
            let streamData = offering.playout.streams[streamKey];
            for (let scheme in (streamData.encryption_schemes || {})){
                let schemeData = streamData.encryption_schemes[scheme];
                if (schemeData?.key_id) {
                    if (!drmKeysPerStream[streamKey]) {
                        drmKeysPerStream[streamKey] = [];
                    }
                    drmKeysPerStream[streamKey].push(schemeData.key_id);
                }
            }
            for (let representationKey in streamData.representations) {
                let representation = streamData.representations[representationKey];
                if ((representation.type == "RepVideo") || (representation.type == "RepAudio")) {
                    if (representation.transcode_id){
                        transcodeIds.push(representation.transcode_id) //assumes no duplicate                       
                    }
                }
                if ((representation.type == "RepCaptions")  || (representation.type == "RepThumbnails")) {
                    let sourcePart = offering.media_struct.streams[streamKey].sources[0].source; //never seen more than one
                    parts.push(sourcePart);
                }
            }
        }
        if (streamKeys.length == 0) {
            this.ReportProgress("No streams to copy");
            return ElvOAction.EXECUTION_FAILED;
        }
        let objectId = inputs.target_mezzanine_object_id;
        let libraryId = await this.getLibraryId(objectId, client); 
        let writeToken = inputs.write_token || await this.getWriteToken({client, objectId, libraryId});
        let targetMetadata = await this.getMetadata({client, objectId, libraryId, writeToken});
        
        /* CAPS SHOULD BE SAME
        let targetCap = await this.readCaps(client, targetMetadata);
        
        if (targetCap && (targetCap.user_conk_key.secret_key != sourceCap.user_conk_key.secret_key)) {
        throw "Caps in target object are incompatible with source object. Import caps first"; 
        }       
        
        if (!targetCap) {
        sourceCap.user_conk_key.qid = objectId;
        await client.ReplaceMetadata({
        libraryId, objectId, writeToken,
        metadataSubtree: sourceCap.user_cap_key,
        metadata: await client.Crypto.EncryptConk(sourceCap.user_conk_key, client.signer._signingKey().publicKey)
        });
        }
        */
        for (let transcodeId of transcodeIds) {
            await client.ReplaceMetadata({
                objectId, libraryId, writeToken,
                metadataSubtree: "transcodes/"+transcodeId,
                metadata: metadata.transcodes[transcodeId]
            });
            this.reportProgress("Adding transcode to target object", transcodeId);
        }
        let targetOfferingKeys;
        if (inputs.target_offering_key) {
            targetOfferingKeys = [inputs.target_offering_key];
        } else {
            if (!targetMetadata.offerings || (Object.keys(targetMetadata.offerings).length == 0)) {
                targetOfferingKeys = [inputs.source_offering_key];
            } else {
                targetOfferingKeys = Object.keys(targetMetadata.offerings);
            }
        }
        for (let targetOfferingKey of targetOfferingKeys) {
            let targetOffering = targetMetadata.offerings && targetMetadata.offerings[targetOfferingKey];
            if (!targetOffering) {
                this.reportProgress("Initializing offering", {source: inputs.source_offering_key, target: targetOfferingKey});
                targetOffering = JSON.parse(JSON.stringify(offering));
                targetOffering.media_struct.streams = {};
                targetOffering.playout.streams = {};
            }
            if (!targetMetadata.elv?.crypt) {
                targetMetadata.elv = metadata.elv;
                this.ReportProgress("Copying elv.crypt from source");
            }
            if (!targetMetadata.elv.crypt.drm) {
                targetMetadata.elv.crypt.drm = {};
            }
            if (!targetMetadata.elv.crypt.drm.kids) {
                targetMetadata.elv.crypt.drm.kids = {};
            }
            if (!targetMetadata.elv.crypt.drm.fps) {
                targetMetadata.elv.crypt.drm.fps = metadata.elv.crypt.drm.fps;
                this.ReportProgress("Copying elv.crypt.drm.fps from source");
            }
            if (!targetOffering.playout.drm_keys) {
                targetOffering.playout.drm_keys = {};
            }
            
            for (let streamKey of streamKeys) {
                //We need to ensure the new stream is not redundant with an existing one
                // for audio, unicity is based on channel_layout and language
                // for captions, unicity is based on language and forced
                // for video, we only keep one track
                for (let targetKey in targetOffering.media_struct.streams) {
                    let targetStream = targetOffering.media_struct.streams[targetKey];
                    let stream = offering.media_struct.streams[streamKey];
                    if (!stream || !targetStream || (stream.codec_type != targetStream.codec_type)) continue;
                    if ((stream.codec_type == "captions") && (stream.language == targetStream.language) && (stream.forced == targetStream.forced)) {
                        this.reportProgress("Removing captions stream "+targetKey +" which is replaced by "+streamKey);
                        delete targetOffering.media_struct.streams[targetKey];
                        delete targetOffering.playout.streams[targetKey];
                        break;
                    }
                    if ((stream.codec_type == "audio") && (stream.language == targetStream.language) && (stream.channel_layout == targetStream.channel_layout)) {
                        this.reportProgress("Removing audio stream "+targetKey +" which is replaced by "+streamKey);
                        delete targetOffering.media_struct.streams[targetKey];
                        delete targetOffering.playout.streams[targetKey];
                        break;
                    }
                }
                
                
                targetOffering.media_struct.streams[streamKey] = offering.media_struct.streams[streamKey];
                targetOffering.playout.streams[streamKey] = offering.playout.streams[streamKey];
                for (let key of (drmKeysPerStream[streamKey] || [])) {
                    this.ReportProgress("Copying drm key from source", key);
                    targetOffering.playout.drm_keys[key] = offering.playout.drm_keys[key];
                    targetMetadata.elv.crypt.drm.kids[key] = metadata.elv.crypt.drm.kids[key];
                }
            }
            
            await client.ReplaceMetadata({
                objectId, libraryId, writeToken,
                metadataSubtree: "offerings/"+targetOfferingKey, // inputs.target_offering_key,
                metadata: targetOffering 
            });
            this.reportProgress("Replaced offering in target", targetOfferingKey);
        }
        await client.ReplaceMetadata({
            objectId, libraryId, writeToken,
            metadataSubtree: "elv",
            metadata: targetMetadata.elv
        });
        
        outputs.streams_imported = streamKeys;
        outputs.transcode_imported = transcodeIds;
        if (inputs.finalize) {
            let result = await this.FinalizeContentObject({
                objectId, libraryId, writeToken, client,
                commitMessage: "Imported streams from "+inputs.source_mezzanine_version_hash
            });
            if (result?.hash) {
                outputs.version_hash = result?.hash;
                return ElvOAction.EXECUTION_COMPLETE;           
            } else {
                return ElvOAction.EXECUTION_EXCEPTION;
            }
        } else {
            outputs.write_token = writeToken;
            if (client.HttpClient.draftURIs[writeToken]) {
                outputs.node_url = "https://" + client.HttpClient.draftURIs[writeToken].hostname() + "/";
                outputs.config_url = "https://" + client.HttpClient.draftURIs[writeToken].hostname() + "/config?self&qspace=main";
                outputs.commit_message = "Imported streams from " + inputs.source_mezzanine_object_id;
            } else {
                throw new Error("Could not get node for writeToken");
            }
            return ElvOAction.EXECUTION_COMPLETE;  
        }
        
    }
    
    
    async executeFinalize({objectId, libraryId, writeToken, client, inputs, outputs}) {
        
        let editAuthorizationToken = await this.EditAuthorizationToken({libraryId, objectId, client});
        let authorizationTokens = [
            editAuthorizationToken
        ];
        const headers = {
            Authorization: authorizationTokens.map(token => `Bearer ${token}`).join(","),
            ignore_bitrate_limit: (inputs.ignore_bitrate_limit == true)
        };
        let offeringKey = this.Payload.inputs.offering_key || "default";
        let nodeUrl = this.Payload.inputs.config_url.replace(/contentfabric\.io.*/,"contentfabric.io");
        
        let reporter = this;
        ElvOAction.TrackerPath = this.TrackerPath;
        client.ToggleLogging(true, {log: reporter.Debug, error: reporter.Error});
        let bitCodeRes = await client.CallBitcodeMethod({
            objectId,
            libraryId,
            writeToken: writeToken,
            method: "media/abr_mezzanine/offerings/" + offeringKey + "/finalize",
            headers,
            body: {ignore_bitrate_limit: (inputs.ignore_bitrate_limit == true)},
            nodeUrl,
            constant: false
        });
        this.reportProgress("finalize mezzaning bitcode result", bitCodeRes);
        
        const mezzanineMetadata = await this.getMetadata({
            libraryId,
            objectId,
            client,
            writeToken,
            metadataSubtree: "offerings"
        });
        let modifiedOfferings = false;
        
        if (this.unifyAudioDRMKeys(mezzanineMetadata)) {
            modifiedOfferings = true;
        }
        
        if (!mezzanineMetadata[offeringKey].storyboard_sets) {
            modifiedOfferings = true;
            mezzanineMetadata[offeringKey].storyboard_sets = {};
        }
        if (!mezzanineMetadata[offeringKey].frame_sets) {
            modifiedOfferings = true;
            mezzanineMetadata[offeringKey].frame_sets = {};
        }
        if (modifiedOfferings) {
            await client.ReplaceMetadata({
                libraryId,
                objectId: objectId,
                writeToken,
                metadataSubtree: "offerings",
                metadata: mezzanineMetadata
            });
        }
        let downloadableSuffix = this.Payload.inputs.downloadable_suffix;
        if (downloadableSuffix) {
            if (!mezzanineMetadata[offeringKey].drm_optional) {
                let downloadable = mezzanineMetadata[offeringKey];
                downloadable.drm_optional = true;
                downloadable.playout.playout_formats = {
                    "dash-clear": {drm: null, protocol: {min_buffer_length: 2, type: "ProtoDash"}},
                    "hls-clear": {drm: null, protocol: {min_buffer_length: 2, type: "ProtoHls"}}
                };
                downloadable.playout.streams = {".": {}, "/":"./meta/offerings/"+ offeringKey+"/playout/streams"};
                downloadable.media_struct.streams = {".":{}, "/":"./meta/offerings/"+ offeringKey+"/media_struct/streams"};
                downloadable.frame_sets = {".":{}, "/":"./meta/offerings/"+ offeringKey+"/frame_sets"};
                downloadable.storyboard_sets = {".":{}, "/":"./meta/offerings/"+ offeringKey+"/storyboard_sets"};
                await client.ReplaceMetadata({
                    libraryId,
                    objectId: objectId,
                    writeToken,
                    metadataSubtree: "offerings/" + offeringKey + downloadableSuffix,
                    metadata: downloadable
                });
            }
        }
        let tobeMerged = {
            public: {asset_metadata: {playout: {"/": "./rep/playout"}}},
            assets: {},
            video_tags: {},
            offerings: {},
            searchables: {
                asset_metadata: {"/": "./meta/public/asset_metadata"},
                assets: {"/": "./meta/assets"},
                offerings: {"/": "./meta/offerings"},
                video_tags: {"/": "./meta/video_tags"}
            }
        };
        await this.MergeMetadata({
            libraryId,
            objectId: objectId,
            writeToken,
            editAuthorizationToken,
            metadata: tobeMerged,
            client,
            nodeUrl
        });
        
        this.ReportProgress("Finalizing content object", {
            objectId, libraryId,
            writeToken: this.Payload.inputs.write_token,
            commitMessage: "Finalize ABR mezzanine"
        });
        let result = await this.FinalizeContentObject({
            client,
            objectId, libraryId,
            writeToken: this.Payload.inputs.write_token,
            commitMessage: "Finalize ABR mezzanine"
        });
        if (result && result.hash) {
            this.ReportProgress("Finalized content object", result);
            outputs.version_hash = result.hash;
            return ElvOAction.EXECUTION_COMPLETE;
        }
        this.ReportProgress("Failed to finalize content object", result);
        return ElvOAction.EXECUTION_EXCEPTION;
    };
    
    unifyAudioDRMKeys(offerings) {
        if (!offerings || (Object.keys(offerings).length == 0)) {
            throw new Error("no offerings found in metadata");
        }
        // loop through offerings
        let  changed = 0;
        for (let offeringKey in offerings) {
            let offering = offerings[offeringKey];
            this.reportProgress(`Checking offering ${offeringKey}...`);
            
            // loop through playout streams, saving first audio stream's keys
            let keyIds;
            for (let streamKey in offering.playout.streams) {
                let stream = offering.playout.streams[streamKey];
                if (stream.representations && Object.entries(stream.representations)[0][1].type === "RepAudio") {
                    if (keyIds) {
                        this.reportProgress(`Setting keys for stream '${streamKey}'...`);
                        stream.encryption_schemes = keyIds;
                        changed++;
                    } else {
                        if (!stream.encryption_schemes || (Object.keys(stream.encryption_schemes).length == 0)) {
                            throw Error(`Audio stream ${streamKey} has no encryption scheme info`);
                        }
                        this.reportProgress(`Using keys from stream '${streamKey}'...`);
                        keyIds = stream.encryption_schemes;
                    }
                }
            }
        }
        if (changed == 0) {
            this.ReportProgress("No changes to make to audio stream DRM keys");
            return null;
        }          
        return offerings;        
    };
    //input.download.default[\"/\"] = \"/qfab/\" + inputs.clip_mezzanine_version_hash + \"/rep/media_download/default/video_1920x1080@9500000\";" +
    async executeDownloadMedia({objectId, libraryId, versionHash, client, inputs, outputs}) {
        let offering = await this.getMetadata({objectId, libraryId, versionHash, client, metadataSubtree: "offerings/"+inputs.offering, resolve: true});
        let videoRepresentation = inputs.video_representation || this.findTopVideoRepresentation(offering, {resolution: inputs.video_resolution});
        let audioRepresentation = inputs.audio_representation || this.findAudioRepresentation(offering, {label: inputs.audio_label, language: inputs.audio_language_code, key: inputs.audio_stream_key});
        let queryParams = audioRepresentation ? {audio: audioRepresentation} : {};
        
        let url = await client.Rep({
            libraryId,
            objectId,
            versionHash,
            rep: "media_download/"+inputs.offering+"/"+videoRepresentation,
            queryParams,
            channelAuth: true
        });
        outputs.download_url = url;
        
        let targetPath = inputs.target_path;
        if (fs.existsSync(targetPath) && fs.lstatSync(targetPath).isDirectory()) {
            let slug = (await this.getMetadata({objectId, libraryId, versionHash, client, metadataSubtree: "public/asset_metadata/slug"})) || objectId;
            let name = slug + "_" + videoRepresentation.replace(/videovideo/,"") + ( audioRepresentation ? ("_" + audioRepresentation) : "");
            targetPath = Path.join(targetPath, name + ".mp4");
        }
        outputs.target_path = targetPath;
        outputs.audio_representation = audioRepresentation;
        outputs.video_representation = videoRepresentation;
        await this.downloadFile(url, targetPath)
        
        
        return ElvOAction.EXECUTION_COMPLETE;
    };
    
    async executeCopyEntryExitPointAccrossOfferings({objectId, libraryId, versionHash, client}, outputs) {
        await this.acquireMutex(objectId);
        let offerings = await this.getMetadata({objectId, libraryId, versionHash, client, metadataSubtree: "offerings", resolve: false});
        
        if (!offerings || (Object.keys(offerings).length == 0)) {
            throw new Error("no offerings found in metadata");
        }
        // loop through offerings
        let sourceOffering = this.Payload.inputs.source_offering;
        let targetOffering = this.Payload.inputs.target_offering;
        if (!offerings[sourceOffering]) {
            throw new Error("Source offering " +sourceOffering + " not found in metadata");
        }
        let entryPointRat = offerings[sourceOffering].entry_point_rat;
        let exitPointRat = offerings[sourceOffering].exit_point_rat;
        let  changed = 0;
        for (let offeringKey in offerings) {
            if ((offeringKey == sourceOffering)  || (targetOffering && (targetOffering != offeringKey))){
                continue;
            }
            let offering = offerings[offeringKey];
            this.reportProgress(`Setting entry/exit points for offering ${offeringKey}...`);
            if ((offering.entry_point_rat != entryPointRat)  || (offering.exit_point_rat != exitPointRat)){
                offering.entry_point_rat = entryPointRat;
                offering.exit_point_rat = exitPointRat; 
                changed++;
            }
        }
        if (changed == 0) {
            outputs.mezzanine_object_version_hash = versionHash;
            this.ReportProgress("No changes to make");
            this.releaseMutex();
            return ElvOAction.EXECUTION_FAILED;
        }
        let writeToken = await this.getWriteToken({
            libraryId: libraryId,
            objectId: objectId,
            versionHash,
            client: client
        });
        
        await client.ReplaceMetadata({
            objectId,
            libraryId,
            metadataSubtree: "offerings", 
            writeToken,
            metadata: offerings,
            client
        });
        
        let msg = "Copied entry/exit points from " + sourceOffering;
        let response = await this.FinalizeContentObject({
            libraryId: libraryId,
            objectId: objectId,
            writeToken: writeToken,
            commitMessage: msg,
            client
        });
        if (response && response.hash) {
            this.ReportProgress(msg)
        } else {
            throw new Error("Failed to save changes to offerings");
        }
        outputs.mezzanine_object_version_hash = response.hash;
        this.releaseMutex();
        return ElvOAction.EXECUTION_COMPLETE;        
        
    };
    
    async removePlayoutFormatsFromOffering({objectId, libraryId, versionHash, client}, outputs) {
        this.reportProgress("Remove playout formats from offering in "+ objectId);
        await this.acquireMutex(objectId);
        let inputs = this.Payload.inputs;
        let formats = this.Payload.playout_formats;
        let changed = false;
        let sourceOffering = await  this.getMetadata({objectId, libraryId, versionHash, client, metadataSubtree:"offerings/"+inputs.offering});
        for (let format of formats) {
            if (sourceOffering.playout_formats[format]) {
                delete sourceOffering.playout_formats[format];
                changed = true;
                this.reportProgress("Removing playout format " +format + " from offering " + inputs.offering);
            } else {
                this.reportProgress("Playout format " +format + "not present in offering " + inputs.offering);
            }
        }
        
        if (!changed) {
            outputs.mezzanine_object_version_hash = versionHash;
            this.ReportProgress("No changes to make");
            this.releaseMutex();
            return ElvOAction.EXECUTION_FAILED;
        }
        
        let writeToken = await this.getWriteToken({
            libraryId: libraryId,
            objectId: objectId,
            versionHash,
            client: client
        });
        
        await client.ReplaceMetadata({
            objectId,
            libraryId,
            metadataSubtree: "offerings/"+  inputs.offering, 
            writeToken,
            metadata: sourceOffering,
            client
        });
        
        let msg = "Removed some playout formats from offering "+inputs.offering;
        let response = await this.FinalizeContentObject({
            libraryId: libraryId,
            objectId: objectId,
            writeToken: writeToken,
            commitMessage: msg,
            client
        });
        if (response && response.hash) {
            this.ReportProgress(msg)
        } else {
            throw new Error("Failed to remove playout formats");
        }
        outputs.mezzanine_object_version_hash = response.hash;
        this.releaseMutex();
        return ElvOAction.EXECUTION_COMPLETE;   
        
    };
    
    async addClearOffering({objectId, libraryId, versionHash, client}, outputs) {
        this.reportProgress("Add clear offering to "+ objectId);
        await this.acquireMutex(objectId);
        let inputs = this.Payload.inputs;
        let formats = this.Payload.inputs.playout_formats;
        let sourceOffering = await  this.getMetadata({objectId, libraryId, versionHash, client, metadataSubtree:"offerings/"+inputs.source_offering});
        let modifiedSource = false;
        if (!sourceOffering.storyboard_sets) {
            modifiedSource = true;
            sourceOffering.storyboard_sets = {};
        }
        if (!sourceOffering.frame_sets) {
            modifiedSource = true;
            sourceOffering.frame_sets = {};
        }
        let writeToken = await this.getWriteToken({
            libraryId: libraryId,
            objectId: objectId,
            versionHash,
            client: client
        });
        if (modifiedSource) {
            await client.ReplaceMetadata({
                objectId,
                libraryId,
                metadataSubtree: "offerings/"+  inputs.source_offering, 
                writeToken,
                metadata: sourceOffering,
                client
            });
        }
        
        if (inputs.offering != inputs.source_offering) {
            for (let format in sourceOffering.playout.playout_formats) {
                if (!formats.includes(format)) {
                    delete sourceOffering.playout.playout_formats[format];
                }
            }
            sourceOffering.playout.streams = {".": {}, "/":"./meta/offerings/" + inputs.source_offering + "/playout/streams"};
            sourceOffering.media_struct.streams = {".":{}, "/":"./meta/offerings/" + inputs.source_offering + "/media_struct/streams"};
            sourceOffering.frame_sets = {".":{}, "/":"./meta/offerings/" + inputs.source_offering + "/frame_sets"};
            sourceOffering.storyboard_sets = {".":{}, "/":"./meta/offerings/" + inputs.source_offering + "/storyboard_sets"};
        }
        
        this.Debug("modified source", sourceOffering);
        sourceOffering.drm_optional = true;
        for (let format of formats) {
            if (format  == "dash-clear") {
                sourceOffering.playout.playout_formats[format] = {drm: null, protocol: {min_buffer_length: 2, type: "ProtoDash"}};
                continue;
            }
            if (format  == "hls-clear") {
                sourceOffering.playout.playout_formats[format] = {drm: null, protocol: {min_buffer_length: 2, type: "ProtoHls"}};
                continue;
            }
        }
        
        await client.ReplaceMetadata({
            objectId,
            libraryId,
            metadataSubtree: "offerings/"+  inputs.offering, 
            writeToken,
            metadata: sourceOffering,
            client
        });
        
        let msg = "Added clear offering "+ inputs.offering;
        let response = await this.FinalizeContentObject({
            libraryId: libraryId,
            objectId: objectId,
            writeToken: writeToken,
            commitMessage: msg,
            client
        });
        if (response && response.hash) {
            this.ReportProgress(msg)
        } else {
            throw new Error("Failed to add clear offering");
        }
        outputs.mezzanine_object_version_hash = response.hash;
        this.releaseMutex();
        return ElvOAction.EXECUTION_COMPLETE;   
        
    };
    
    isEqualTo = function(a,b) {
        if (a == null) {
            return (b == null);
        }
        if (b == null) {
            return false; //since a==null has already been handled
        }
        if ((typeof a) != (typeof b)) {
            return false;
        }
        if ((typeof a) != "object") {
            return (a == b);
        }
        let aKeys = Object.keys(a);
        if (aKeys.length != Object.keys(b).length) {
            return false;
        }
        for (let i=0; i < aKeys.length; i++) {
            if (!(aKeys[i] in b) || !this.isEqualTo(a[aKeys[i]], b[aKeys[i]])) {
                return false;
            }
        }
        return true;
    };
    
    async executeUnifyAudioDRMKeys({objectId, libraryId, versionHash, client}, outputs) {
        this.reportProgress("Make all audio streams use same DRM keys in object "+ objectId);
        await this.acquireMutex(objectId);
        let offerings;
        if (this.Payload.inputs.offering_key) {
            offerings = {}
            offerings[this.Payload.inputs.offering_key] = await this.getMetadata({objectId, libraryId, versionHash, client, metadataSubtree: "offerings/"+this.Payload.inputs.offering_key});
        } else {
            offerings = await this.getMetadata({objectId, libraryId, versionHash, client, metadataSubtree: "offerings"});
        }
        
        if (!offerings || (Object.keys(offerings).length == 0)) {
            throw new Error("no offerings found in metadata");
        }
        // loop through offerings
        let  changed = 0;
        for (let offeringKey in offerings) {
            let offering = offerings[offeringKey];
            this.reportProgress(`Checking offering ${offeringKey}...`);
            
            // loop through playout streams, saving first audio stream's keys
            let keyIds;
            for (let streamKey in offering.playout.streams) {
                let stream = offering.playout.streams[streamKey];
                if (stream.representations && Object.entries(stream.representations)[0][1].type === "RepAudio") {
                    if (keyIds) {
                        this.reportProgress(`Setting keys for stream '${streamKey}'...`);
                        if (!this.isEqualTo(stream.encryption_schemes, keyIds)) {
                            stream.encryption_schemes = keyIds;
                            changed++;
                        }
                    } else {
                        if (!stream.encryption_schemes || (Object.keys(stream.encryption_schemes).length == 0)) {
                            throw Error(`Audio stream ${streamKey} has no encryption scheme info`);
                        }
                        this.reportProgress(`Using keys from stream '${streamKey}'...`);
                        keyIds = stream.encryption_schemes;
                    }
                }
            }
        }
        if (changed == 0) {
            outputs.mezzanine_object_version_hash = versionHash;
            this.ReportProgress("No changes to make");
            this.releaseMutex();
            return ElvOAction.EXECUTION_FAILED;
        }
        let writeToken = await this.getWriteToken({
            libraryId: libraryId,
            objectId: objectId,
            versionHash,
            client: client
        });
        for (let offeringKey in offerings) {
            await client.ReplaceMetadata({
                objectId,
                libraryId,
                metadataSubtree: "offerings/"+offeringKey, 
                writeToken,
                metadata: offerings[offeringKey],
                client
            });
        }
        
        let msg = "Unified audio streams with single set of DRM keys";
        let response = await this.FinalizeContentObject({
            libraryId: libraryId,
            objectId: objectId,
            writeToken: writeToken,
            commitMessage: msg,
            client
        });
        if (response && response.hash) {
            this.ReportProgress(msg)
        } else {
            throw new Error("Failed to save changes to DRM keys");
        }
        outputs.mezzanine_object_version_hash = response.hash;
        this.releaseMutex();
        return ElvOAction.EXECUTION_COMPLETE;        
    };
    
    ratToFloat(rat) {
        if (!rat) return 0;
        if (Number.isFinite(rat)) {
            return rat;
        }
        if (!rat.match(/^[0-9\/]+$/)) {
            throw new Error("Not a rat: "+ rat);
        }
        return eval(rat)
    };
    
    compareRat(rat1, rat2) {
        let float1 = this.ratToFloat(rat1);
        let float2 = this.ratToFloat(rat2);
        if (float1 < float2) {
            return -1;
        }
        if (float1 > float2) {
            return 1;
        }
        return 0;
    };
    
    async executeClipMezzanine({objectId, libraryId, versionHash, client}, outputs) {
        /*
        inputs.offering = {type: "string", required: false, default: "default"};
        inputs.entry_point_sec = {type: "numeric", required: false, default: null};
        inputs.entry_point_rat = {type: "string", required: false, default: null};
        inputs.exit_point_sec = {type: "numeric", required: false, default: null};
        inputs.exit_point_rat = {type: "string", required: false, default: null};
        outputs.mezzanine_object_version_hash = {type: "string"};
        */
        let inputs = this.Payload.inputs;
        await this.acquireMutex(objectId);
        let matchingOfferingKeys;
        let offerings;
        if (inputs.offering.match(/\*/)) {
            offerings = await this.getMetadata({
                objectId, 
                versionHash, 
                libraryId,
                metadataSubtree: "offerings",
                resolve: false,
                client
            });     
            let pattern = "^" +inputs.offering.replace(/\*/g,".*") + "$";
            matchingOfferingKeys = Object.keys(offerings).filter(function(key) {return key.match(pattern)});
        } else {
            matchingOfferingKeys = [inputs.offering];
            offerings ={};
            offerings[inputs.offering] = await this.getMetadata({
                objectId, 
                versionHash, 
                libraryId,
                metadataSubtree: "offerings/"+inputs.offering,
                resolve: false,
                client
            }); 
        }
        
        if ((matchingOfferingKeys.length == 0) || ((matchingOfferingKeys.length == 1) && !Object.values(offerings)[0])){
            this.ReportProgress("No matching offerings found for "+inputs.offering);
            return ElvOAction.EXECUTION_EXCEPTION;
        }
        
        for (let offeringKey of matchingOfferingKeys) {
            let offering = offerings[offeringKey];
            let framerate = (offering.media_struct.streams && offering.media_struct.streams.video && offering.media_struct.streams.video.rate)
            || await this.getMetadata({
                objectId, 
                versionHash, 
                libraryId,
                metadataSubtree: "offerings/"+offeringKey+"/media_struct/streams/video/rate",
                client
            }); 
            if ((typeof framerate) == "string") {}   
            let matcher = framerate.match(/^([0-9]+)\/([0-9]+)$/) || framerate.match(/^([0-9]+)$/);
            if (!matcher) {
                throw Error("Invalid framerate format '"+ framerate + "'");
            }
            if (!matcher[2]) {
                matcher[2] = 1;
            }
            let changed =  false;
            let durationRat = offering.media_struct.duration_rat  || await this.getMetadata({
                objectId, 
                versionHash, 
                libraryId,
                metadataSubtree: "offerings/"+offeringKey+"/media_struct/duration_rat",
                client
            }); 
            let entryPointRat =  inputs.entry_point_rat;
            if (inputs.entry_point_sec) {
                let frameCount = Math.round(inputs.entry_point_sec * matcher[1] / matcher[2]);
                entryPointRat  = "" + (frameCount * matcher[2]) +"/" + matcher[1];
            } 
            if (this.compareRat(entryPointRat, durationRat) > 0) {
                this.reportProgress("Specified entry point is past end of video", {entryPointRat, durationRat});
                entryPointRat = "0";
            }
            if (entryPointRat &&  (offering.entry_point_rat != entryPointRat)) {
                offering.entry_point_rat = entryPointRat;
                changed = true;
            }
            if (((entryPointRat == "") || (inputs.entry_point_sec == 0)) && offering.entry_point_rat) {
                offering.entry_point_rat = null;
                changed = true;
            }
            let exitPointRat =  inputs.exit_point_rat;
            if (inputs.exit_point_sec) {
                let frameCount = Math.round(inputs.exit_point_sec * matcher[1] / matcher[2]);
                exitPointRat  = "" + (frameCount * matcher[2]) +"/" + matcher[1];
                
            }
            if (this.compareRat(exitPointRat, durationRat) > 0) {
                this.reportProgress("Specified exit point is past end of video", {exitPointRat, durationRat});
                exitPointRat = durationRat;
            }
            if (exitPointRat && (offering.exit_point_rat != exitPointRat)) {
                offering.exit_point_rat = exitPointRat;
                changed = true;
            }
            if (((exitPointRat == "") || (inputs.exit_point_sec == 0)) && offering.exit_point_rat) {
                offering.exit_point_rat = null;
                changed = true;
            }
            if (!changed) {
                outputs.mezzanine_object_version_hash = versionHash;
                this.ReportProgress("No changes to make to offering " + offeringKey);
                delete offerings[offeringKey];
            }
        }
        if (Object.keys(offerings).length == 0) {
            this.releaseMutex();
            this.ReportProgress("No changes to make");
            return ElvOAction.EXECUTION_FAILED;   
        }
        let writeToken = await this.getWriteToken({
            libraryId: libraryId,
            objectId: objectId,
            versionHash,
            client: client
        });
        for (let offeringKey in offerings){
            let offering = offerings[offeringKey];
            this.ReportProgress("Adjust entry/exit points for offering ", offeringKey);
            await client.ReplaceMetadata({
                objectId,
                libraryId,
                metadataSubtree: "offerings/" + offeringKey, 
                writeToken,
                metadata: offering,
                client
            });
        }
        let msg = "Modified entry and/or exit point";
        let response = await this.FinalizeContentObject({
            libraryId: libraryId,
            objectId: objectId,
            writeToken: writeToken,
            commitMessage: msg,
            client
        });
        if (response && response.hash) {
            this.ReportProgress(msg)
        } else {
            throw new Error("Failed to save changes to entry and/or exit point");
        }
        outputs.mezzanine_object_version_hash = response.hash;
        this.releaseMutex();
        return ElvOAction.EXECUTION_COMPLETE;     
    };
    
    
    async executeClipMezzanineSingleOffering({objectId, libraryId, versionHash, client}, outputs) {
        /*
        inputs.offering = {type: "string", required: false, default: "default"};
        inputs.entry_point_sec = {type: "numeric", required: false, default: null};
        inputs.entry_point_rat = {type: "string", required: false, default: null};
        inputs.exit_point_sec = {type: "numeric", required: false, default: null};
        inputs.exit_point_rat = {type: "string", required: false, default: null};
        outputs.mezzanine_object_version_hash = {type: "string"};
        */
        let inputs = this.Payload.inputs;
        await this.acquireMutex(objectId);
        let offering = await this.getMetadata({
            objectId, 
            versionHash, 
            libraryId,
            metadataSubtree: "offerings/"+inputs.offering,
            client
        }); 
        let framerate = offering.media_struct.streams.video.rate;
        let matcher = framerate.match(/^([0-9]+)\/([0-9]+)$/);
        if (!matcher) {
            throw Error("Invalid framerate format '"+ framerate + "'");
        }
        let changed =  false;
        let entryPointRat =  inputs.entry_point_rat;
        if (inputs.entry_point_sec  != null) {
            let frameCount = Math.round(inputs.entry_point_sec * matcher[1] / matcher[2]);
            entryPointRat  = "" + (frameCount * matcher[2]) +"/" + matcher[1];
        } 
        if ((entryPointRat != null) &&  (offering.entry_point_rat != entryPointRat)) {
            offering.entry_point_rat = entryPointRat;
            changed = true;
        }
        if (((entryPointRat == "") || (inputs.entry_point_sec == 0)) && offering.entry_point_rat) {
            offering.entry_point_rat = null;
            changed = true;
        }
        let exitPointRat =  inputs.exit_point_rat;
        if (inputs.exit_point_sec  != null) {
            let frameCount = Math.round(inputs.exit_point_sec * matcher[1] / matcher[2]);
            exitPointRat  = "" + (frameCount * matcher[2]) +"/" + matcher[1];
            
        }
        if ((exitPointRat != null) && (offering.exit_point_rat != exitPointRat)) {
            offering.exit_point_rat = exitPointRat;
            changed = true;
        }
        if (((exitPointRat == "") || (inputs.exit_point_sec == 0)) && offering.exit_point_rat) {
            offering.exit_point_rat = null;
            changed = true;
        }
        if (!changed) {
            outputs.mezzanine_object_version_hash = versionHash;
            this.ReportProgress("No changes to make");
            this.releaseMutex();
            return ElvOAction.EXECUTION_FAILED;
        }
        let writeToken = await this.getWriteToken({
            libraryId: libraryId,
            objectId: objectId,
            versionHash,
            client: client
        });
        
        await client.ReplaceMetadata({
            objectId,
            libraryId,
            metadataSubtree: "offerings/"+inputs.offering, 
            writeToken,
            metadata: offering,
            client
        });
        
        let msg = "Modified entry and/or exit point";
        let response = await this.FinalizeContentObject({
            libraryId: libraryId,
            objectId: objectId,
            writeToken: writeToken,
            commitMessage: msg,
            client
        });
        if (response && response.hash) {
            this.ReportProgress(msg)
        } else {
            throw new Error("Failed to save changes to entry and/or exit point");
        }
        outputs.mezzanine_object_version_hash = response.hash;
        this.releaseMutex();
        return ElvOAction.EXECUTION_COMPLETE;     
    };
    
    async removeStream({objectId, libraryId, versionHash, client}, outputs) {       
        let inputs = this.Payload.inputs;
        await this.acquireMutex(objectId);
        let offerings = await this.getMetadata({
            objectId, 
            versionHash, 
            libraryId,
            metadataSubtree: "offerings",
            resolve: false,
            client
        }); 
        let matchingOfferingKeys;
        if (inputs.offering.match(/\*/)) {
            let pattern = "^" +inputs.offering.replace(/\*/g,".*") + "$";
            matchingOfferingKeys = Object.keys(offerings).filter(function(key) {return key.match(pattern)});
        } else {
            matchingOfferingKeys = (offerings[inputs.offering]) ? [inputs.offering] : [];
        }
        if (matchingOfferingKeys.length == 0) {
            this.ReportProgress("No matching offerings found for "+inputs.offering);
            return ElvOAction.EXECUTION_EXCEPTION;
        }
        
        let changed =  {};
        let transcodeIds = [];
        let streamKey = inputs.stream_key;
        for (let offeringKey of matchingOfferingKeys) {
            let  offering = offerings[offeringKey];
            if  (offering.playout.streams[streamKey]) {
                changed[offeringKey] = true;
                for (let repId in offering.playout.streams[streamKey].representations) {
                    let representation  = offering.playout.streams[streamKey].representations[repId];
                    if ((representation.transcode_id) && !transcodeIds.includes(representation.transcode_id)){
                        transcodeIds.push(representation.transcode_id);
                    }
                }
                delete offering.playout.streams[streamKey];
            }
            if  (offering.media_struct.streams[streamKey]) {
                changed[offeringKey] = true;
                delete offering.media_struct.streams[streamKey];
            }
        }
        let  changedOfferings = Object.keys(changed);
        if (changedOfferings.length == 0) {
            this.ReportProgress("No changes required for "+inputs.offering);
            return ElvOAction.EXECUTION_FAILED;
        }
        if (transcodeIds.length != 0) {
            let transcodes = await this.getMetadata({
                objectId, 
                versionHash, 
                libraryId,
                metadataSubtree: "transcodes",
                resolve: false,
                client
            }); 
            for (let transcodeId of transcode_id) {
                this.reportProgress("Deleting transcode", transcodeId);
                delete transcodes[transcodeId];
            }
        }
        
        let writeToken = await this.getWriteToken({
            libraryId: libraryId,
            objectId: objectId,
            versionHash,
            client: client
        });
        
        await client.ReplaceMetadata({
            objectId,
            libraryId,
            metadataSubtree: "offerings", 
            writeToken,
            metadata: offerings,
            client
        });
        if (transcodeIds.length != 0) {
            await client.ReplaceMetadata({
                objectId,
                libraryId,
                metadataSubtree: "trancodes", 
                writeToken,
                metadata: transcodes,
                client
            });
        }
        
        let msg = "Removed "+ streamKey + " from " + matchingOfferingKeys.join(", ");
        let response = await this.FinalizeContentObject({
            libraryId: libraryId,
            objectId: objectId,
            writeToken: writeToken,
            commitMessage: msg,
            client
        });
        if (response && response.hash) {
            this.ReportProgress(msg)
        } else {
            throw new Error("Failed to remove stream from offering");
        }
        outputs.mezzanine_object_version_hash = response.hash;
        this.releaseMutex();
        return ElvOAction.EXECUTION_COMPLETE;     
    };
    
    async removeOffering({objectId, libraryId, versionHash, client}, outputs) {       
        let inputs = this.Payload.inputs;
        await this.acquireMutex(objectId);
        let offerings = await this.getMetadata({
            objectId, 
            versionHash, 
            libraryId,
            metadataSubtree: "offerings",
            client
        }); 
        let matchingOfferingKeys;
        if (inputs.offering.match(/\*/)) {
            let pattern = "^" +inputs.offering.replace(/\*/g,".*") + "$";
            matchingOfferingKeys = Object.keys(offerings).filter(function(key) {return key.match(pattern)});
        } else {
            matchingOfferingKeys = (offerings[inputs.offering]) ? [inputs.offering] : [];
        }
        if (matchingOfferingKeys.length == 0) {
            this.ReportProgress("No matching offerings found for "+inputs.offering);
            return ElvOAction.EXECUTION_EXCEPTION;
        }
        
        let changed =  {};
        let streamKey = inputs.stream_key;
        for (let offeringKey of matchingOfferingKeys) {
            delete offerings[offeringKey];
            changed[offeringKey] = true;
        }
        let  changedOfferings = Object.keys(changed);
        if (changedOfferings.length == 0) {
            this.ReportProgress("No changes required for removal of "+inputs.offering);
            return ElvOAction.EXECUTION_FAILED;
        }
        
        let writeToken = await this.getWriteToken({
            libraryId: libraryId,
            objectId: objectId,
            versionHash,
            client: client
        });
        
        await client.ReplaceMetadata({
            objectId,
            libraryId,
            metadataSubtree: "offerings", 
            writeToken,
            metadata: offerings,
            client
        });
        
        let msg = "Offering removal of "+ matchingOfferingKeys.join(", ");
        let response = await this.FinalizeContentObject({
            libraryId: libraryId,
            objectId: objectId,
            writeToken: writeToken,
            commitMessage: msg,
            client
        });
        if (response && response.hash) {
            this.ReportProgress(msg)
        } else {
            throw new Error("Failed to remove offering");
        }
        outputs.mezzanine_object_version_hash = response.hash;
        this.releaseMutex();
        return ElvOAction.EXECUTION_COMPLETE;     
    };
    
    async readOffering({objectId, libraryId, versionHash, client}, outputs) {       
        let inputs = this.Payload.inputs;
        let offering = await this.getMetadata({
            objectId, 
            versionHash, 
            libraryId,
            metadataSubtree: "offerings/"+inputs.offering,
            client
        }); 
        if (!offering) {
            return ElvOAction.EXECUTION_FAILED;
        }
        outputs.value = offering;
        for (let key in offering.playout.streams.video.representations) {
            let representation = offering.playout.streams.video.representations[key];
            if (representation.transcode_matches_rep) {
                outputs.video_representation = key;
                break;
            }
        }
        return ElvOAction.EXECUTION_COMPLETE;     
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
    
    static VERSION = "0.2.3"; 
    static REVISION_HISTORY = {
        "0.0.1": "Initial release",
        "0.0.2": "Adds clipping function to modify entry/exit point of mezzanine",
        "0.0.3": "Fixes support for exit_point_sec in clipping",
        "0.0.4": "Fixes calculation of exit/entry point in rat from sec to make it a multiple of frame duration",
        "0.0.5": "Adds option to remove a stream from matching offering (glob allowed)",
        "0.0.6": "Adds option to remove a set of matching offering (glob allowed)",
        "0.0.7": "Adds linking to source when creating a clear offering",
        "0.0.8": "Adds option to clip a set of matching offering (glob allowed), an exit point value of 0 is now a removal, null means don't touch",
        "0.0.9": "Do not resolve links when clipping",
        "0.1.0": "Adds option to link offerings playout streams to a source offering",
        "0.1.1": "Adds option to copy the entry and exit points from a source offering to other offerings",
        "0.1.2": "Avoids committing a new version when entry/exit point are already synched between offerings",
        "0.1.3": "Avoids committing offerings are already linked",
        "0.1.4": "Adds support for reading an offering",
        "0.1.5": "Prevents clipping outside of duration",
        "0.1.6": "Adds option to download a mezzanine as an mp4 file",
        "0.1.7": "Adds action to copy streams between object with compatible encryption",
        "0.1.8": "2025-12-12 - ML - Adds DRM copy to stream copy",
        "0.1.9": "2026-02-17 - ML - Fixes copy of DRM keys to existing media object",
        "0.2.0": "2026-04-05 - ML - Makes finalizing optional when copying streams between objects",
        "0.2.1": "Adds action to copy rungs between object with compatible encryption",
        "0.2.2": "Adds action to combine streams of different mezzanines",
        "0.2.3": "Ensures the duration of an offering is not shorter than imported rungs"
    };
}


if (ElvOAction.executeCommandLine(ElvOActionManageMezzanine)) {
    ElvOAction.Run(ElvOActionManageMezzanine);
} else {
    module.exports=ElvOActionManageMezzanine;
}
