const ElvOAction = require("../o-action").ElvOAction;
const ElvOFabricClient = require("../o-fabric");
const EPCR_metadata = require("./action_epcr_variants_dependencies/epcr_metadata_helper");
const { execSync } = require('child_process');
const fs = require("fs");
const path = require("path");
const target_metadata_folder = "./importer_data/metadata_per_content"
const mez_catalog = path.join(target_metadata_folder,"mez_summary.csv")
const master_catalog = path.join(target_metadata_folder,"master_summary.csv")
const MIN_BIT_RATE_TO_ACCEPT = 12000
const MAX_BIT_RATE_TO_ACCEPT = 28000

class ElvOActionEpcrVariants extends ElvOAction  {
    
    ActionId() {
        return "epcr_variants";
    };
    
    Parameters() {
        return {
            parameters: {
                action: {
                    type: "string", required: true, 
                    values: ["CREATE_VARIANT", "PROBE_SOURCES", "CREATE_VARIANT_COMPONENT", "CONFORM_MASTER",
                    "ADD_COMPONENT", "CONFORM_MASTER_TO_FILE", "CONFORM_MEZZANINE_TO_FILE",
                    "MAKE_THUMBNAIL", "LOOKUP_OBJECT_DATA", "UPDATE_PROGRESS", "QC_MEZZ"]
                }
            }
        };
    };
    
    IOs(parameters) {
        let inputs = {
            private_key: {type: "password", required:false},
            config_url: {type: "string", required:false}
        };
        let outputs = {};
        if (parameters.action == "CREATE_VARIANT") {
            inputs.production_master_object_id = {type: "string", required:true};
            inputs.variant_name = {type: "string", required:false, default: "default"};
            inputs.save_variant = {type: "boolean", required:false, default: true};
            inputs.variant_source_file = {type: "string", required:false, default: null};
            outputs.production_master_version_hash = "string";
            outputs.audios = "array";
            outputs.mezz_command = "string";
            outputs.is_interlaced = "boolean";
            outputs.mezzanine_object_name = "string";
        }
        if (parameters.action  == "ADD_COMPONENT") {
            inputs.production_master_object_id = {type: "string", required:true};
            inputs.asset_type = {type: "string", required:true};
            inputs.asset_source_files = {type: "string", required:true};
            inputs.mezzanine_object_id = {type: "string", required:false};
            inputs.mezzanine_status = {type: "string", required:false};            
            outputs.new_source_files = "array";
            outputs.launch_mezz_creation = "boolean";
            outputs.production_master_version_hash = "string";
        }
        if (parameters.action  == "CONFORM_MASTER_TO_FILE") {
            inputs.production_master_object_id = {type: "string", required:true};
            inputs.asset_type = {type: "string", required:false};
            inputs.asset_source_files = {type: "string", required:false};
            inputs.mezzanine_object_id = {type: "string", required:false};
            inputs.mezzanine_status = {type: "string", required:false};            
            outputs.new_source_files = "array";
            outputs.launch_mezz_creation = "boolean";
            outputs.production_master_version_hash = "string";
            outputs.ip_title_id = "string";
        }
        if (parameters.action  == "CONFORM_MEZZANINE_TO_FILE") {
            inputs.production_master_object_id = {type: "string", required:false};
            inputs.asset_type = {type: "string", required:false};
            inputs.mezzanine_object_id = {type: "string", required:false};
            outputs.mezzanine_version_hash = "string";
            outputs.ip_title_id = "string";
        }
        if (parameters.action == "CONFORM_MASTER") {
            inputs.production_master_object_id = {type: "string", required:true};
            inputs.ip_title_id = {type: "string", required: true};
            inputs.master_type = {type: "string", required: true};
            inputs.game_name = {type: "string", required: true};
            inputs.game_date = {type: "string", required: true};
            inputs.admin_group = {type: "string", required: false};
            outputs.production_master_version_hash = "string";
            outputs.production_master_object_name = "string";
        }
        if (parameters.action == "MAKE_THUMBNAIL") {
            inputs.assets_object_id = {type: "string", required: true};
            inputs.home_team = {type: "string", required: true};
            inputs.away_team = {type: "string", required: true};      
            inputs.round = {type: "string", required: false, default: ""};    
            inputs.html_template_path = {type: "string", required: true};
            inputs.width = {type: "numeric", required: true}; 
            inputs.height = {type: "numeric", required: true}; 
            inputs.target_folder=  {type: "string", required: false, default: "/tmp/"};
            outputs.thumbnail_path = {type: "string", required: true};
        }
        if (parameters.action == "LOOKUP_OBJECT_DATA") {
            inputs.object_id = {type: "string", required: true};
            outputs.metadata = {type: "object", required: true};
        }
        if (parameters.action == "UPDATE_PROGRESS") {
            // master ID, a progress status, a progress message and optionally a write-token
            inputs.object_id = {type: "string", required: false};
            inputs.production_master_object_id = {type: "string", required:false};
            inputs.progress_status = {type: "string", required: true};
            inputs.progress_message = {type: "string", required: true};
            inputs.write_token = {type: "string", required: false};
            inputs.do_persist = {type: "boolean", required: false, default: true};            
        }
        if (parameters.action == "QC_MEZZ") {
            // master ID, mezz ID
            inputs.production_master_object_id = {type: "string", required: true};
            inputs.mezzanine_object_id = {type: "string", required: true};
            outputs.qc_message = {type: "string", required: true};            
        }
        if (parameters.action == "GET_METADATA") {
            // comp_id,date,home_team,away_team,asset_type
            inputs.comp_id = {type: "string", required: true};
            inputs.date = {type: "string", required: true};
            inputs.home_team = {type: "string", required: true};
            inputs.away_team = {type: "string", required: true};
            inputs.asset_type = {type: "string", required: true};
            outputs.public_metadata = {type: "string", required: true};            
        }
        if (parameters.action == "GET_METADATA_FROM_S3_NAME") {
            // s3_url | file_name
            inputs.file_url = {type: "string", required: true};            
            outputs.public_metadata = {type: "string", required: true};
        }
        return {inputs, outputs};
    };
    
    async Execute(inputs, outputs) {
        let client;
        if (!this.Payload.inputs.private_key && !this.Payload.inputs.config_url) {
            client = this.Client;
        } else {
            let privateKey = this.Payload.inputs.private_key || this.getPrivateKey();
            let configUrl = this.Payload.inputs.config_url || this.Client.configUrl;
            client = await ElvOFabricClient.InitializeClient(configUrl, privateKey)
        }
        
        let objectId = this.Payload.inputs.production_master_object_id;
        // We need to check if objectId is not null or undefined
        // if it is, then we need to simply get the library id from the inputs
        let libraryId = null
        if (objectId == null || objectId == undefined) {
            libraryId = this.Payload.inputs.master_library
        } else {
            libraryId = await this.getLibraryId(objectId, client)
        }   
        
        if (this.Payload.parameters.action == "CREATE_VARIANT") {
            return await this.executeCreateVariant({client, objectId, libraryId, inputs, outputs});
        }
        if (this.Payload.parameters.action == "ADD_COMPONENT") {
            return await this.executeAddComponent({client, objectId, libraryId, inputs, outputs});
        }
        if (this.Payload.parameters.action == "CONFORM_MASTER_TO_FILE") {
            return await this.executeConformMasterToFile({client, objectId, libraryId, inputs, outputs});
        }
        if (this.Payload.parameters.action == "CONFORM_MEZZANINE_TO_FILE") {
            return await this.executeConformMezzanineToFile({client, objectId, libraryId, inputs, outputs});
        }
        if (this.Payload.parameters.action == "CONFORM_MASTER") {
            return await this.executeConformMaster({client, objectId, libraryId, inputs, outputs});
        }
        if (this.Payload.parameters.action == "MAKE_THUMBNAIL") {
            return await this.executeMakeThumbnail(inputs, outputs) 
        }
        if (this.Payload.parameters.action == "LOOKUP_OBJECT_DATA") {
            return await this.executeLookupObjectData(inputs,outputs)
        }
        if (this.Payload.parameters.action == "UPDATE_PROGRESS") {
            return await this.executeUpdateProgress(inputs,outputs)
        }
        if (this.Payload.parameters.action == "QC_MEZZ") {
            return await this.executeQcMezz({client, objectId, libraryId, inputs, outputs})
        }        
        if (this.Payload.parameters.action == "GET_METADATA") {
            return await this.executeGetMetadata({client, objectId, libraryId, inputs, outputs})
        }      
        if (this.Payload.parameters.action == "GET_METADATA_FROM_S3_NAME") {
            return await this.executeGetMetadataFromS3Name({client, objectId, libraryId, inputs, outputs})
        }     
        throw Error("Action not supported: "+this.Payload.parameters.action);
    };
    
    async executeMakeThumbnail(inputs, outputs) {
        let client = await this.initializeActionClient();
        let libraryId = await this.getLibraryId(inputs.match_object_id, client);
        let info = {round: inputs.round, resources: {team_home: {name: inputs.home_team}, team_away: {name: inputs.away_team}}};
        // info.resources.team_home.name
        
        let thumbnailPath = this.makeMatchThumbnail({
            assetsObjectId: inputs.assets_object_id, 
            info, 
            htmlPath: inputs.html_template_path, 
            width: inputs.width, height:inputs.height,
            imageLabel: inputs.label,
            targetFolder: inputs.target_folder
        });
        outputs.thumbnail_path = thumbnailPath;
        
        return ElvOAction.EXECUTION_COMPLETE;
    };
    
    normalizeName(name) {
        
        let substitutions = [
            {expression: /ü/g, replacement: "u"}, //turkey
            {expression: / and /g, replacement: "_"}, //bosnia
            {expression: /[^a-zA-Z0-9]+/g, replacement: "_"} //always last
        ]
        for (let substitution of substitutions) {
            name = name.replace(substitution.expression, substitution.replacement);
        }
        
        name = name.replace(/ /g,"_");
        return name;
    }
    
    makeMatchThumbnail({assetsObjectId, info, htmlPath, width,height,imageLabel, targetFolder}){
        let homeTeam = this.normalizeName(info.resources.team_home.name);
        let awayTeam = this.normalizeName(info.resources.team_away.name);
        let round = info.round.replace(/[_ ]/g,"+");
        let root = "https://main.net955305.contentfabric.io/s/main/q/"
        let html = encodeURIComponent(htmlPath);
        if (!imageLabel) {
            imageLabel = "thumbnail";
        }
        let url  = root +assetsObjectId +"/rep/webshot?url="+html +"&url_format=2&width="+ width +"&height=" +height + "&animate_interval=3000&template_map=%7B%22home%22%3A%22"+homeTeam+ "%22%2C%22visitor%22%3A%22"+ awayTeam+"%22%2C%22round%22%3A%22"+round+"%22%7D";
        this.reportProgress("url", url);
        let thumbnail = path.join(targetFolder, "match_"+imageLabel+"_"+homeTeam.toLowerCase()+"_vs_"+awayTeam.toLowerCase()+".png");
        let cmd = "curl -L '"+url+"' --output '"+ thumbnail +"'";
        this.reportProgress("generating", thumbnail);
        console.log("cmd", cmd);
        execSync(cmd);
        //Check if the file exist and verify with imagemagik or fprobe that it is an image
        if (!fs.existsSync(thumbnail)) {
            throw new Error("No files created at target path "+ thumbnail);
        }
        let probeCmd = "identify png:'"+thumbnail+"'";
        try{
            let probe = execSync(probeCmd).toString();
            this.reportProgress("probe", probe);
        } catch(errProbe) {
            console.log("errProbe", errProbe);
            throw new Error("Generated image "+ thumbnail + " seems to have incorrect format", errProbe);           
        }
        return thumbnail;
    };
    
    async  executeAddComponent({client, objectId, libraryId, inputs, outputs}) {
        let meta = await this.getMetadata({client, objectId, libraryId, resolve: false});
        outputs.new_source_files = [];
        let sourceFileNames = [];
        for (let sourceFile of inputs.asset_source_files) {
            let fileName = path.basename(sourceFile);
            sourceFileNames.push(fileName);
            if (!meta.files[fileName]){
                outputs.new_source_files.push(sourceFile);
            }
        }
        if (!meta.public) {
            meta.public = {};
        }
        if (!meta.public.asset_metadata) {
            meta.public.asset_metadata = {};
        }
        if (!meta.public.asset_metadata.info) {
            meta.public.asset_metadata.info = {};
        }
        if (!meta.public.asset_metadata.info.components) {
            meta.public.asset_metadata.info.components = {};
        }
        meta.public.asset_metadata.info.components[inputs.asset_type] = {
            source_files: sourceFileNames,
            mezzanine_object_id: inputs.mezzanine_object_id,
            mezzanine_status: inputs.mezzanine_status
        };
        outputs.launch_mezz_creation = true;
        if (!outputs.new_source_files.length) {
            //check if variant exist to rename it
            for (let variantKey in (meta.production_master?.variants || {})) {
                let variant = meta.production_master.variants[variantKey];
                if (variant.streams.video.sources[0].files_api_path == sourceFileNames[0]) {
                    meta.production_master.variants[inputs.asset_type] = variant;
                    this.reportProgress("Renaming variant "+ variantKey + " to " + inputs.asset_type);
                }
            }
            if (inputs.mezzanine_status == "complete") {
                outputs.launch_mezz_creation = false;
            }
        }
        let writeToken = await this.getWriteToken({
            client, objectId, libraryId,
        });
        await client.ReplaceMetadata({
            objectId, libraryId, writeToken,
            metadata: meta
        });
        let result = await this.FinalizeContentObject({
            objectId, libraryId, writeToken, client,
            commitMessage: "Added component "+ inputs.asset_type
        });
        if (result?.hash) {
            outputs.production_master_version_hash = result.hash;
        } else {
            this.ReportProgress("Failed to finalized master object", result);
            return ElvOAction.EXECUTION_EXCEPTION;
        }
        if (outputs.new_source_files.length) {
            return ElvOAction.EXECUTION_COMPLETE;
        } else {
            return ElvOAction.EXECUTION_FAILED;
        }
    }
    
    
    async  executeConformMasterToFile({client, objectId, libraryId, inputs, outputs}) {        
        let meta = await this.getMetadata({client, objectId, libraryId, resolve: false});
        outputs.new_source_files = [];
        outputs.launch_mezz_creation = !(inputs.mezzanine_status == "complete");
        if (meta.public?.model && meta.public.model.match(/^v[0-9]$/) && !inputs.force) {
            outputs.ip_title_id = meta.public.asset_metadata.ip_title_id;
            return ElvOAction.EXECUTION_FAILED;
        }
        let dataFile = path.join(target_metadata_folder,objectId+".json");
        let newMeta =  (fs.existsSync(dataFile)) ?  JSON.parse(fs.readFileSync(dataFile, 'utf8')) : null;
        if (!newMeta) {
            this.reportProgress("No metadata file found for "+objectId);
            return ElvOAction.EXECUTION_COMPLETE;
        }
        if (!newMeta.public) {
            newMeta.public = {};
        }
        newMeta.public.model = (!inputs.asset_type) ? "v0" : "v1";
        meta.public = newMeta.public;
        if (!meta.public.asset_metadata) {
            meta.public.asset_metadata = {};
        }
        if (!meta.public.asset_metadata.info) {
            meta.public.asset_metadata.info = {};
        }
        if (!meta.public.asset_metadata.info.components) {
            meta.public.asset_metadata.info.components = {};
        }
        let sourceFileNames = [];
        if (inputs.asset_source_files) {
            for (let sourceFile of inputs.asset_source_files) {
                let fileName = path.basename(sourceFile);
                sourceFileNames.push(fileName);
                if (!meta.files[fileName]){
                    outputs.new_source_files.push(sourceFile);
                }
            }
        } else {
            sourceFileNames.push(meta.production_master.variants.default.streams.video.sources[0].files_api_path);
        }
        
        meta.public.asset_metadata.info.components[inputs.asset_type || "default"] = {
            source_files: sourceFileNames,
            mezzanine_object_id: inputs.mezzanine_object_id,
            mezzanine_status: inputs.mezzanine_status
        };
        outputs.launch_mezz_creation = true;
        if (!outputs.new_source_files.length   && inputs.asset_type ) {
            //check if variant exist to rename it
            for (let variantKey in (meta.production_master?.variants || {})) {
                let variant = meta.production_master.variants[variantKey];
                if (variant.streams.video.sources[0].files_api_path == sourceFileNames[0]) {
                    meta.production_master.variants[inputs.asset_type] = variant;
                    this.reportProgress("Renaming variant "+ variantKey + " to " + inputs.asset_type);
                }
            }
            if (inputs.mezzanine_status == "complete") {
                outputs.launch_mezz_creation = false;
            }
        }
        let writeToken = await this.getWriteToken({
            client, objectId, libraryId,
        });
        await client.ReplaceMetadata({
            objectId, libraryId, writeToken,
            metadata: meta.public,
            metadataSubtree: "public"
        });
        if (inputs.asset_type &&  meta.production_master.variants[inputs.asset_type]) {
            await client.ReplaceMetadata({
                objectId, libraryId, writeToken,
                metadata: meta.production_master.variants[inputs.asset_type],
                metadataSubtree: "production_master/variants/" + inputs.asset_type
            });
        }
        let message = (!inputs.asset_type) ? "Normalized to v0" : ("Added component " + inputs.asset_type)
        let result = await this.FinalizeContentObject({
            objectId, libraryId, writeToken, client,
            commitMessage: message
        });
        if (result?.hash) {
            outputs.production_master_version_hash = result.hash;
        } else {
            this.ReportProgress("Failed to finalized master object", result);
            return ElvOAction.EXECUTION_EXCEPTION;
        }
        if (outputs.new_source_files.length) {
            return ElvOAction.EXECUTION_COMPLETE;
        } else {
            return ElvOAction.EXECUTION_FAILED;
        }
    };

    async  executeConformMezzanineToFile({client, inputs, outputs}) {        
        let objectId = inputs.mezzanine_object_id;
        let libraryId = await this.getLibraryId(objectId, client);
        let meta = await this.getMetadata({client, objectId, libraryId, resolve: false, metadataSubtree: "public"});
        outputs.launch_mezz_creation = !(inputs.mezzanine_status == "complete");
        if ((meta.model == "v1") && !inputs.force) {
            outputs.ip_title_id = meta.asset_metadata.ip_title_id;
            return ElvOAction.EXECUTION_FAILED;
        }
        let dataFile = path.join(target_metadata_folder,objectId+".json");
        let newMeta =  (fs.existsSync(dataFile)) ?  JSON.parse(fs.readFileSync(dataFile, 'utf8')) : null;
        // ADM - here we should check if newMeta has a public section, otherwise we skill newMeta
        if (!newMeta || !newMeta.public) {
            
            let masterDataPath = inputs.production_master_object_id && path.join(target_metadata_folder,inputs.production_master_object_id+".json");
            if (masterDataPath && fs.existsSync(masterDataPath)) {
                this.reportProgress("Looking for master metadata "+objectId);
                newMeta = JSON.parse(fs.readFileSync(masterDataPath));
                newMeta.public.name = newMeta.public.name.replace(/MASTER/, "VOD");
                newMeta.public.asset_metadata.title = newMeta.public.asset_metadata.title.replace(/MASTER/, "VOD");
            } else {
                this.reportProgress("No metadata file found for "+objectId);
                return ElvOAction.EXECUTION_COMPLETE;
            }
            this.reportProgress()


            
        }
        if (!newMeta.public.asset_metadata.info.start_time && newMeta.time) {
            newMeta.public.asset_metadata.info.start_time = newMeta.time;
        }

        meta = newMeta.public;        
        if (inputs.asset_type && (!meta.model  || (meta.model == "v0") )) {
            meta.asset_metadata.ip_title_id = meta.asset_metadata.ip_title_id + "_" + inputs.asset_type.replace(/_/g, "-");
            meta.asset_metadata.asset_type = inputs.asset_type;
            meta.name = meta.name.replace(/MATCH/, inputs.asset_type.toUpperCase());
            meta.model = "v1";
        } else {
            meta.model = "v0";
        }
        
        let writeToken = await this.getWriteToken({
            client, objectId, libraryId,
        });
        await client.ReplaceMetadata({
            objectId, libraryId, writeToken,
            metadata: meta,
            metadataSubtree: "public"
        });
        
        let message = (!inputs.asset_type) ? "Normalized to v0" : "Normalized to v1";
        let result = await this.FinalizeContentObject({
            objectId, libraryId, writeToken, client,
            commitMessage: message
        });
        if (result?.hash) {
            outputs.mezzanine_version_hash = result.hash;
        } else {
            this.ReportProgress("Failed to finalized mezzanine object", result);
            return ElvOAction.EXECUTION_EXCEPTION;
        }
        return ElvOAction.EXECUTION_COMPLETE;
        
    };
    
    async  executeConformMaster({client, objectId, libraryId, inputs, outputs}) {
        //MASTER - Match - 2023-12-10 - ech202324-r1-008 - USAP v Emirates Lions
        let name = "MASTER - Match - " + inputs.game_date + " - " + inputs.ip_title_id + " - " + inputs.game_name;
        outputs.production_master_object_name = name;
        if (inputs.admin_group) {
            this.reportProgress("Setting manage permission for " + inputs.admin_group);
            await client.AddContentObjectGroupPermission({
                objectId,
                groupAddress: inputs.admin_group,
                permission: "manage"
            });
            this.ReportProgress("Manage permission set for " + inputs.admin_group);
        }
        
        let writeToken = await this.getWriteToken({
            objectId, libraryId, client,
            options: {type: inputs.master_type}
        });
        await client.ReplaceMetadata({
            objectId, libraryId, writeToken,
            metadataSubtree: "public/name",
            metadata: name
        });
        await client.ReplaceMetadata({
            objectId, libraryId, writeToken,
            metadataSubtree: "public/asset_metadata",
            metadata: {
                ip_title_id: inputs.ip_title_id,
                title: "Match - " + inputs.game_date + " - " + inputs.ip_title_id + " - " + inputs.game_name,
                info :{
                    game_date: inputs.game_date,
                    game_name: inputs.game_name
                }
            }
        });
        let response = await this.FinalizeContentObject({
            objectId, libraryId, writeToken, client,
            commitMessage: "Setting name and type"
        });
        if (response.hash) {
            outputs.production_master_version_hash = response.hash; 
            return ElvOAction.EXECUTION_COMPLETE;
        } else {
            this.ReportProgress("Could not finalize");
            return ElvOAction.EXECUTION_EXCEPTION;
        }
    };
    
    async executeCreateVariant({client, objectId, libraryId, inputs, outputs}) {        
        let meta = await this.getMetadata({objectId, libraryId, client, metadataSubtree: "production_master"});
        if (!inputs.variant_source_file) {
            let probedFiles = meta?.sources ? Object.keys(meta.sources) : [];
            if (probedFiles.length == 0) {
                throw new Error("No probed files found");
            }
            if (probedFiles.length > 1) {
                throw new Error("More than one probed files found, the source has to be specified explicitly");
            }
            inputs.variant_source_file = probedFiles[0];
        }
        let probe = meta.sources[inputs.variant_source_file];
        if (!probe) {
            throw new Error("Speficied file has not been probed");
        }
        let sources = probe.streams ;
        let audios = [];
        let videoIndex = null;
        let index = 0;
        outputs.is_interlaced = false;
        let targetFramerate, targetTimebase;
        for (let source of sources) {
            if (source.type == "StreamAudio") {
                audios.push(index);
            }
            if (source.type == "StreamVideo") {
                videoIndex = index;
                if (["tf", "tt", "tb", "bt"].includes(source.field_order)) {
                    this.reportProgress("Video is interlaced", source.field_order);
                    outputs.is_interlaced = true;
                    /* //Not sure if this still applies
                    if (source.codec_name == "mpeg2video") {
                        this.reportProgress("Interlaced mpeg2video are not supported yet");
                        console.log("Interlaced mpeg2video are not supported yet");
                        throw new Error("Interlaced mpeg2video are not supported yet");
                    }
                    */
                    
                    let framerate = source.frame_rate;
                    let timebase = source.time_base;
                    targetFramerate = parseInt(framerate) * 2;
                    let matcher = timebase.match(/^([0-9]+)\/([0-9]+)$/);
                    if (!matcher) {
                        this.ReportProgress("Invalid timebase", timebase);
                        throw new Error("Invalid timebase");
                    }
                    let isMultiple = (1 / targetFramerate) / eval(timebase);
                    if ((isMultiple > 0) && (parseInt(isMultiple) == isMultiple)) {
                        targetTimebase = timebase;
                    } else {
                        targetTimebase = matcher[1] + "/" + (parseInt(matcher[2]) * 2);
                    }
                }
            }
            index++;
        }
        let variant;
        if (videoIndex == null) {
            throw new Error("Probed file has no videos");
        } else {
            if (!meta.variants) {
                meta.variants = {};
            }
            let videoStream = {
                default_for_media_type: true,
                label: "video",
                language: "en",
                mapping_info: "",
                type: "video",
                sources: [ {
                    files_api_path: inputs.variant_source_file,
                    stream_index: videoIndex
                }]
            };
            if (outputs.is_interlaced) {
                videoStream.deinterlace = "bwdif_field";
                videoStream.target_frame_rate = targetFramerate.toString();
                videoStream.target_timebase = targetTimebase;
            }
            meta.variants[inputs.variant_name] = {
                streams: {
                    video: videoStream            
                }
            }
            variant = meta.variants[inputs.variant_name];
            
        }
        if (audios.length < 1) {
            throw new Error("Probed file has no audios or just a mono track");
        }
        if ((sources[audios[0]].channel_layout  == "stereo")  || (sources[audios[0]].channels == 2)){
            this.ReportProgress("Stereo audio encountered");
            outputs.audios = 1;
            variant.streams.audio = {
                default_for_media_type: true,
                label: "Audio",
                language: "en",
                mapping_info: "",
                sources: [
                    {
                        files_api_path: inputs.variant_source_file,
                        stream_index: audios[0]
                    }
                ]                
            };
        }
        if (audios.length >= 2) {
            this.ReportProgress("Adding Full RTE Mix audio");
            outputs.audios = 1;
            variant.streams.audio_1 = {
                default_for_media_type: true,
                label: "Audio 1",
                language: "en",
                mapping_info: "2MONO_1STEREO",
                sources: [
                    {
                        files_api_path: inputs.variant_source_file,
                        stream_index: audios[0]
                    },
                    { 
                        files_api_path:inputs.variant_source_file,
                        stream_index: audios[1]
                    }
                ]                
            };
        }
        if (audios.length >= 4) {
            this.ReportProgress("Adding international audio");
            outputs.audios = 2;
            variant.streams.audio_2 = {
                default_for_media_type: false,
                label: "Audio 2",
                language: "en-2",
                mapping_info: "2MONO_1STEREO",
                sources: [
                    {
                        files_api_path: inputs.variant_source_file,
                        stream_index: audios[2]
                    },
                    { 
                        files_api_path:inputs.variant_source_file,
                        stream_index: audios[3]
                    }
                ]                
            };
        }
        if (audios.length >= 6) {
            this.ReportProgress("Adding PA/TMO audio");
            outputs.audios = 3;
            variant.streams.audio_3 = {
                default_for_media_type: false,
                label: "Audio 3",
                language: "en-3",
                mapping_info: "2MONO_1STEREO",
                sources: [
                    {
                        files_api_path: inputs.variant_source_file,
                        stream_index: audios[4]
                    },
                    { 
                        files_api_path:inputs.variant_source_file,
                        stream_index: audios[5]
                    }
                ]                
            };
        }
        if (audios.length >= 8) {
            this.ReportProgress("Adding commentary audio");
            outputs.audios = 4;
            variant.streams.audio_4 = {
                default_for_media_type: false,
                label: "Audio 4",
                language: "en-4",
                mapping_info: "2MONO_1STEREO",
                sources: [
                    {
                        files_api_path: inputs.variant_source_file,
                        stream_index: audios[6]
                    },
                    { 
                        files_api_path:inputs.variant_source_file,
                        stream_index: audios[7]
                    }
                ]                
            };
        }
        let response = {hash: null};
        if (inputs.save_variant) {
            let writeToken = await this.getWriteToken({objectId, libraryId, client});
            await client.ReplaceMetadata({
                objectId, libraryId, writeToken,
                metadataSubtree: "production_master/variants/"+inputs.variant_name,
                metadata: variant
            });
            response = await this.FinalizeContentObject({
                objectId, libraryId, writeToken, client,
                commitMessage: "Adding generated variant "+ inputs.variant_name
            });
        }
        if (response.hash || !inputs.save_variant) {
            outputs.production_master_version_hash = response.hash || await this.getVersionHash({objectId, libraryId, client}); 
            /*
            node utilities/MezCreate.js --config-url "https://host-154-14-211-100.contentfabric.io/config?self&qspace=main"  --library-id ilib4FtcGxjMK3rhTedA8MFb9KZoeTsy  --master-hash hq__E9ELyTYqQjFU7EkEsNkKVP2DbQwajiUUGrnSHn4DJXandamCEX91p4kYrhZhnmETKNfkgRJaV5  --mez-type  iq__B2y3ALACpL58jRYMuQhxz1fcDgN  --title "Toyota Cheetahs v Hollywoodbets Sharks" --name "VOD - Match - 2023-12-17 - ech202324-r2-008 - Toyota Cheetahs v Hollywoodbets Sharks"
            */

            /*
            let masterName = await this.getMetadata({objectId, libraryId, client, metadataSubtree: "public/name"});
            let nameParts = masterName.match(/MASTER - Match - ([0-9\-]+) - ([^ ]+) - (.*)/);
            //"VOD - Match - 2023-12-17 - ech202324-r2-008 - Toyota Cheetahs v Hollywoodbets Sharks"
            let mezzName =  "VOD - Match - " + nameParts[1] + " - " + nameParts[2] + " - " + nameParts[3];
            outputs.mezzanine_object_name = mezzName;
            let commandConfigUrl = (outputs.is_interlaced && "https://host-154-14-211-100.contentfabric.io/config?self&qspace=main") ||  "https://main.net955305.contentfabric.io/config";
            outputs.mezz_command = "node utilities/MezCreate.js --config-url \"" + commandConfigUrl + "\"  --library-id ilib4FtcGxjMK3rhTedA8MFb9KZoeTsy  --master-hash " + outputs.production_master_version_hash + " --mez-type  iq__B2y3ALACpL58jRYMuQhxz1fcDgN  --title \"" + nameParts[3] + "\" --name \"" + mezzName + "\"";
            console.log("\n"+outputs.mezz_command+"\n");
            */
            return ElvOAction.EXECUTION_COMPLETE;
        } else {
            this.ReportProgress("Could not finalize");
            return ElvOAction.EXECUTION_EXCEPTION;
        }
    };
    
    /**
    * Retrieves the content information from the local JSON file used to store them and sets them 
    * in the outputs structure.
    * 
    * @returns ElvOAction.EXECUTION_COMPLETE 
    */
    
    async executeLookupObjectData({inputs, outputs}){
        if (!inputs.object_id) {
            throw new Error("Missing object_id inputs");
        }
        let data_file = path.join(target_metadata_folder,inputs.object_id+".json")
        outputs.metadata = JSON.parse(fs.readFileSync(data_file, 'utf8'))
        return ElvOAction.EXECUTION_COMPLETE;
    }
    
    /**
    * Updates the structure holding content information (backed by a JSON file) with 
    * the specified progress report.
    * Inputs contains:
    *  inputs.object_id = {type: "string", required: true};
    *  inputs.progress_status = {type: "string", required: true};
    *  inputs.progress_message = {type: "string", required: true};
    *  inputs.write_token = {type: "string", required: false};
    *  inputs.do_persist = {type: "boolean", required: false, default: true};            
    * 
    * @returns ElvOAction.EXECUTION_COMPLETE or ElvOAction.EXECUTION_EXCEPTION if the JSON file
    * cannot be persisted
    */
    async executeUpdateProgress(inputs, outputs){
        if (!inputs.object_id) {
            inputs.object_id = inputs.production_master_object_id;
        }
        let data_file = path.join(target_metadata_folder,inputs.object_id+".json");
        let metadata_obj =  (fs.existsSync(data_file)) ?  JSON.parse(fs.readFileSync(data_file, 'utf8')) : {};
        
        if (metadata_obj.progress == null){
            metadata_obj.progress = {}
        }        
        metadata_obj.progress_status = inputs.progress_status
        metadata_obj.progress_message = inputs.progress_message
        if (inputs.write_token != null) {
            metadata_obj.write_token = inputs.write_token
        }
        if (inputs.do_persist) {
            let data_file = path.join(target_metadata_folder,inputs.object_id+".json")
            try{
                fs.writeFileSync(data_file,JSON.stringify(metadata_obj))
            }catch(exception){
                console.log("Error writing to target file: " + data_file, exception);
                return ElvOAction.EXECUTION_EXCEPTION
            }            
        }        
        return ElvOAction.EXECUTION_COMPLETE;
    }
    
    /**
    * Check that the video bit rate of the transcoded mezzanine is within ranges
    * Sets outputs.qc_message 
    * @returns ElvOAction.EXECUTION_COMPLETE if bit rate is withing ranges, ElvOAction.EXECUTION_EXCEPTION otherwise
    */
    async executeQcMezz({client, objectId, libraryId, inputs, outputs}){
        let mez_object_id = inputs.mezzanine_object_id
        let meta = await this.getMetadata({objectId: mez_object_id, libraryId, client, metadataSubtree: "offerings/default"})
        // ADM - I assume all metadata are represented as int
        // if not the case then add parseInt
        const bit_rate = meta.media_struct.streams.video.bit_rate
        if (bit_rate < MIN_BIT_RATE_TO_ACCEPT  || bit_rate > MAX_BIT_RATE_TO_ACCEPT) {
            outputs.qc_message = "Mezzanine Bit Rate outside acceptable range: " + bit_rate
            return ElvOAction.EXECUTION_FAILED;
        }
        outputs.qc_message = "Mezzanine Bit Rate " + bit_rate
        return ElvOAction.EXECUTION_COMPLETE;
    }
    
    /**
     * Retreives the public metadata for the specified match
     * It uses the external library epcr_metadata_helper
     */
    async executeGetMetadata({client, objectId, libraryId, inputs, outputs}){
        let match_element = await EPCR_metadata.fetch_and_create_metadata(inputs.comp_id,inputs.date,inputs.home_team,inputs.away_team,inputs.asset_type);
        outputs.public_metadata = match_element.public;
        return ElvOAction.EXECUTION_COMPLETE;
    }
    
    /**
     * Retreives the public metadata for a file stored in the S3 bucket
     * It uses the external library epcr_metadata_helper
     */
    async executeGetMetadataFromS3Name({client, objectId, libraryId, inputs, outputs}){                                          
        let match_element = await EPCR_metadata.fetch_and_create_metadata_from_s3(inputs.file_url)
        outputs.public_metadata = match_element.public;
        return ElvOAction.EXECUTION_COMPLETE;
    }

    
    /**
    * Public Metadata Handling
    * 
    * For EPCR we decided to use a specific structure for public metadata:
    * 
    * MASTER
    * public.name = 2023-12-08 - chp202324-r1-001 - Glasgow Warriors v Northampton Saints - Match - Master
    * public.asset_metadata.asset_type = master
    * public.asset_metadata.display_title = EPCR Champions Cup - 2023-2024 - R1 - Glasgow Warriors v Northampton Saints - Master      
    * public.asset_metadata.info.broadcast_info.broadcaster = TNT
    * public.asset_metadata.info.broadcast_info.quality = QuickTime, Avid DNX 120
    * public.asset_metadata.info.date = 2023-12-08
    * public.asset_metadata.info.match_id = chp202324-r1-001
    * public.asset_metadata.info.start_time = HH:MM:ss
    * public.asset_metadata.info.team_away_code = NOR
    * public.asset_metadata.info.team_away_name = Northampton Saints
    * public.asset_metadata.info.team_home_code = GLA
    * public.asset_metadata.info.team.home_name = Glasgow Warriors
    * public.asset_metadata.info.tournament_id = chp
    * public.asset_metadata.info.torunament_name = EPCR Champions Cup
    * public.asset_metadata.info.tournament_season = 2023-2024
    * public.asset_metadata.info.tournament_stage = Group Stage Round 1
    * public.asset_metadata.info.tournament_stage_short = R1
    * public.asset_metadata.ip_title_id = chp202324-r1-001
    * public.asset_metadata.slug = chp202324-r1-001
    * public.asset_metadata.title = 2023-12-08 - chp202324-r1-001 - Glasgow Warriors v Northampton Saints - Match - Master
    * public.asset_metadata.title_type = primary
    * 
    * 
    * MEZANINE
    * public.name = 2023-12-08 - chp202324-r1-001 - Glasgow Warriors v Northampton Saints - Match - VOD
    * public.asset_metadata.asset_type = Match (Match|Highlights|ISO)
    * public.asset_metadata.display_title = EPCR Champions Cup - 2023-2024 - R1 - Glasgow Warriors v Northampton Saints
    * public.asset_metadata.info.date = 2023-12-08
    * public.asset_metadata.info.match_id = chp202324-r1-001
    * public.asset_metadata.info.start_time = HH:MM:ss
    * public.asset_metadata.info.team_away_code = NOR
    * public.asset_metadata.info.team_away_name = Northampton Saints
    * public.asset_metadata.info.team_home_code = GLA
    * public.asset_metadata.info.team.home_name = Glasgow Warriors
    * public.asset_metadata.info.tournament_id = chp
    * public.asset_metadata.info.torunament_name = EPCR Champions Cup
    * public.asset_metadata.info.tournament_season = 2023-2024
    * public.asset_metadata.info.tournament_stage = Group Stage Round 1
    * public.asset_metadata.info.tournament_stage_short = R1
    * public.asset_metadata.ip_title_id = chp202324-r1-001
    * public.asset_metadata.slug = chp202324-r1-001
    * public.asset_metadata.title = 2023-12-08 - chp202324-r1-001 - Glasgow Warriors v Northampton Saints - Match - VOD
    * public.asset_metadata.title_type = auxiliary
    * 
    * 
    * For every season and every tournament, we store all data into a CSV file (202324-master-tournament_id.csv) except:
    * public.asset_metadata.info.tournament_stage (long name is statically mapped to public.asset_metadata.info.tournament_stage_short)
    * 
    */
    
    static REVISION_HISTORY = {
        "0.0.1": "Initial release - CREATE_VARIANT only",
        "0.0.2": "ADM - Added methods to read data from JSON backing file",
        "0.0.3": "ADM - Added method to validate mezzanine bit rate quality",
        "0.0.4": "ML - minor tweak to UPDATE PROGRESS and CONFORM - changed deinterlacing conf",
        "0.0.5": "ML - renamed the audios",
        "0.0.6": "ML - adds CONFORM_MASTER_TO_FILE",
        "0.0.7": "Improves detection of stereo audio to not only rely on the channel_layout field",
        "0.0.8": "ML - considers the case where master object does not have any public metadata",
        "0.0.9": "ML - reads metadata from Master if none is provided for the mezzanine (case of new mezz)",
        "0.1.0": "ML-ADM - adding ancillarily library and GET_METADATA functions",
        "0.1.1": "ML-ADM - fixinf parse_name output to be a proper JSON and not a string",
        "0.1.2": "ADM - Fixing libraryId reference when objectId is not provided, ignoring metadata file if does not contain a public section"
    };
    static VERSION = "0.1.2";
}

if (ElvOAction.executeCommandLine(ElvOActionEpcrVariants)) {
    ElvOAction.Run(ElvOActionEpcrVariants);
} else {
    module.exports=ElvOActionEpcrVariants;
}
