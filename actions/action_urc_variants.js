const ElvOAction = require("../o-action").ElvOAction
const ElvOFabricClient = require("../o-fabric")
const { execSync } = require('child_process')
const fs = require("fs")
const path = require("path")
const xml2js = require("xml2js")
const https = require("https")
const csv2json = require("csvtojson")

const MIN_BIT_RATE_TO_ACCEPT = 12000
const MAX_BIT_RATE_TO_ACCEPT = 28000

const team_map = new Map([
  ["ASM Clermont Auvergne","CLE"],
  ["Bath Rugby","BAT"],
  ["Benetton Rugby","BEN"],
  ["Bristol Bears","BRS"],
  ["Castres Olympique","CAS"],
  ["DHL Stormers","STO"],
  ["Exeter Chiefs","EXE"],
  ["Glasgow Warriors","GLA"],
  ["Harlequins","HAR"],
  ["Hollywoodbets Sharks","SHA"], // Durban Sharks
  ["Leicester Tigers","LEIC"],
  ["Leinster Rugby","LEIN"],
  ["Munster Rugby","MUN"],
  ["Northampton Saints","NOR"],
  ["Racing 92","R92"],
  ["RC Toulon","TLN"],
  ["Sale Sharks","SAL"],
  ["Saracens","SAR"],
  ["Stade Francais Paris","STA"],
  ["Stade Rochelais","LAR"],
  ["Stade Toulousain","TLS"],
  ["Ulster Rugby","ULS"],
  ["Union Bordeaux-Begles","BOR"],
  ["Vodacom Bulls","BUL"],
  ["Avrion Bayonnais","BAY"],
  ["Black Lion","BLA"],
  ["Cardiff Rugby","CAR"],
  ["Connacht Rugby","CON"],
  ["Dragons RFC","DRA"],
  ["Edinburgh Rugby","EDI"],
  ["Emirates Lions","LIO"],
  ["Gloucester Rugby","GLO"],
  // ["Lyon Olympique Universitaire Rugby (LOU Rugby)","LYN"],
  ["Lyon","LYN"],
  ["Montpellier Herault Rugby","MON"],
  ["Newcastle Falcons","NEW"],
  ["Ospreys","OSP"],
  ["RC Vannes","VAN"],
  ["Scarlets","SCA"],
  ["Section Paloise","PAU"],
  ["Toyota Cheetahs USAP","CHE"],
  ["USAP","PER"],
  ["Zebre Parma","ZEB"],
  ["FC Grenoble Rugby","GRE"],
  ["CA Brive","BRI"],
  ["Enisei-STM","ENI"],  
  ["Worcester Warriors","WOR"],
  ["SCM Rugby Timișoara","TIM"],
  ["Wasps RFC","WAS"],
  ["SU Agen","AGE"],
  ["Krasny Yar","KRA"],
  ["London Irish","LIR"],
  ["Rugby Calvisano","CAL"],
  ["Biarritz Olympique","BIA"],
  ["Romanian Wolves","WOL"],
  ["London Welsh","LWE"],
  ["Rugby Rovigo Delta","ROV"],
  ["Oyonnax Rugby","OYO"] //
])

const similar_name_mapping = new Map([
  // ["Lyon","Lyon Olympique Universitaire Rugby (LOU Rugby)"],
  ["Aviron Bayonnais","Avrion Bayonnais"],
  ["Bath","Bath Rugby"],
  ["Bristol Rugby","Bristol Bears"],
  ["Edinburgh Rugby [Interlaced]","Edinburgh Rugby"],
  ["Durban Sharks","Hollywoodbets Sharks"],
  ["Durban Sharks - Deint01","Hollywoodbets Sharks"],
  ["Durban Sharks - Deint02","Hollywoodbets Sharks"],
  ["Gloucester","Gloucester Rugby"],
  ["Cell C Sharks","Hollywoodbets Sharks"],
  ["Connact Rugby","Connacht Rugby"],
  ["Exeter Rugby","Exeter Chiefs"],
  ["Exter Rugby","Exeter Chiefs"],  
  ["Exter","Exeter Chiefs"],
  ["Exeter","Exeter Chiefs"],    
  ["Clermont","ASM Clermont Auvergne"],
  ["Cardiff Blues","Cardiff Rugby"],
  ["Cardiff Rubgy","Cardiff Rugby"],
  ["Castre Olympique","Castres Olympique"],
  ["Castres","Castres Olympique"],  
  ["Dragons (dirty)","Dragons RFC"],
  ["Glasgow","Glasgow Warriors"],
  ["Leinster","Leinster Rugby"],
  ["Leicester Rugby","Leicester Tigers"],
  ["Leicetser Tigers","Leicester Tigers"],    
  ["Newscatle Falcons","Newcastle Falcons"], 
  ["Racing","Racing 92"],
  ["WASPS","Wasps RFC"],    
  ["Wasps","Wasps RFC"],    
  ["Black Lions","Black Lion"],  
  ["Toyota Cheetahs","Toyota Cheetahs USAP"],
  ["Toyota Cheetahs","Toyota Cheetahs USAP"],  
  ["Sections Paloise","Section Paloise"],
  ["UBB","Union Bordeaux-Begles"],  
  ["Union Bordeaux-Bègles","Union Bordeaux-Begles"],  
  ["Bordeaux-Bègles","Union Bordeaux-Begles"],  
  ["Montpellier Hérault Rugby","Montpellier Herault Rugby"],
  ["Montpellier HR","Montpellier Herault Rugby"],
  ["Stade Rochleais","Stade Rochelais"],
  ["Bristol","Bristol Bears"],
  ["Bristol Bears VoD","Bristol Bears"],
  ["Munster","Munster Rugby"],
  ["Racing92","Racing 92"],
  ["LaRochelle","Stade Rochelais"],
  ["La Rochelle","Stade Rochelais"],
  ["Stade Francais","Stade Francais Paris"],
  ["Stade Français Paris Paris","Stade Francais Paris"],
  ["Sale","Sale Sharks"],
  ["Munster","Munster Rugby"],
  ["Harelquins","Harlequins"],
  ["Ulster","Ulster Rugby"],
  ["Leicester","Leicester Tigers"],
  ["Newcastle","Newcastle Falcons"],
  ["Benetton","Benetton Rugby"],
  ["Connacht","Connacht Rugby"],
  ["Cardiff","Cardiff Rugby"],
  ["Northampton","Northampton Saints"],
  ["Edinburgh","Edinburgh Rugby"],
  ["Biarritz","Biarritz Olympique"],
  ["Zebre","Zebre Parma"],
  ["Zebra Rugby","Zebre Parma"],
  ["Bordeaux","Union Bordeaux-Begles"],
  ["Worcester","Worcester Warriors"],
  ["Union Bordeaux Begles","Union Bordeaux-Begles"],
  ["Union Bordeaux Bègles","Union Bordeaux-Begles"],
  ["Bordeaux Begles","Union Bordeaux-Begles"],
  ["Worcester Warrior","Worcester Warriors"],
  ["- Ospreys","Ospreys"],
  ["The Sharks","Hollywoodbets Sharks"],
  ["NG Dragons","Dragons RFC"],
  ["Dragons","Dragons RFC"],
  ["Brive","CA Brive"]
])

const target_metadata_folder = "/home/o/elv-o/metadata_per_content"

// It stores all json information for every match stored in "./urc_data.json"
// late initialization to avoid hidden exceptions
let opta_metadata = null


class ElvOActionUrcVariants extends ElvOAction  {
    
    ActionId() {
        return "urc_variants";
    };
    
    Parameters() {
        return {
            parameters: {
                action: {
                    type: "string", required: true, 
                    values: ["CREATE_VARIANT", "PROBE_SOURCES", "CREATE_VARIANT_COMPONENT", "CONFORM_MASTER",
                    "ADD_COMPONENT", "CONFORM_MASTER_TO_FILE", "CONFORM_MEZZANINE_TO_FILE",
                    "MAKE_THUMBNAIL", "LOOKUP_OBJECT_DATA", "UPDATE_PROGRESS", "QC_MEZZ", "GET_METADATA_FROM_FILE", "GET_MEDIA_URL"]
                },
                finalize_write_token: {
                    type: "boolean", required: false, 
                    default: true}
            }
        };
    };
    
    IOs(parameters) {
        let inputs = {
            private_key: {type: "password", required:false},
            config_url: {type: "string", required:false},
            write_token: {type: "string", required:false}
            
        };
        let outputs = {
            write_token : "string"
        };
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
            inputs.metadata_file_name = {type: "string", required:true};
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
        if (parameters.action == "GET_METADATA_FROM_FILE") {
            // metadata are provided via an xml side-car file                        
            inputs.metadata_file_path = {type: "string", required: true};
            outputs.public_metadata = {type: "string", required: true};
        }
        if (parameters.action == "GET_MEDIA_URL") {
            // metadata are provided via an xml side-car file
            // master ID or mezz ID
            inputs.metadata_file_path = {type: "string", required: true};            
            outputs.media_link = {type: "string", required: true};        
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
        if (objectId == null || objectId == undefined || this.Payload.inputs.master_library != null) {
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
        if (this.Payload.parameters.action == "GET_METADATA_FROM_FILE") {
            try{
                opta_metadata = JSON.parse(fs.readFileSync(path.join(target_metadata_folder,"urc_data.json")))
            }catch(err){
                this.reportProgress("Missing match archive in " + path.join(target_metadata_folder,"urc_data.json"))
                this.Error("Loading metadata", err)
                throw Error("Missing match archive in " + path.join(target_metadata_folder,"urc_data.json"))
            }
            
            return await this.executeGetMetadataFromFile({client, objectId, libraryId, inputs, outputs})
        }      
        if (this.Payload.parameters.action == "GET_MEDIA_URL") {
            return await this.executeGetMediaUrl({client, objectId, libraryId, inputs, outputs})
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
        this.reportProgress("cmd", cmd);
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
            logger.Error("errProbe", errProbe);
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
        let writeToken = await this.get_write_token(inputs, client, objectId, libraryId)
        outputs.write_token = writeToken
        await client.ReplaceMetadata({
            objectId, libraryId, writeToken,
            metadata: meta
        });
        if (inputs.finalize_write_token){
            let result = await this.FinalizeContentObject({
                objectId, libraryId, writeToken, client,
                commitMessage: "Added component "+ inputs.asset_type
            });
        }
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
    
    
    async get_write_token(inputs, client, objectId, libraryId) {
        let writeToken = inputs.write_token
        if (writeToken == null) {
            writeToken = await this.getWriteToken({
                client, objectId, libraryId,
            })
        }
        return writeToken
    }

    async  executeConformMasterToFile({client, objectId, libraryId, inputs, outputs}) {        
        let meta = await this.getMetadata({client, objectId, libraryId, resolve: false, writeToken: inputs.write_token});
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
        let writeToken = await this.get_write_token(inputs, client, objectId, libraryId)
        outputs.write_token = writeToken

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
        if (inputs.finalize_write_token){
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
        // ADM - here we need to change the logic to parse the xml side car and extract the metadata
        // Name/Title example 2023-12-10 - urc202324-r1-008 - USAP v Emirates Lions - MATCH - VOD

        let meta_file_name = inputs.metadata_file_name

        let dataFile = path.join(target_metadata_folder,meta_file_name)

        let newMeta =  this.extract_metadata(dataFile)
        if (!newMeta) {
            this.reportProgress("No metadata file found for "+objectId);
            return ElvOAction.EXECUTION_COMPLETE;
            this.reportProgress()
        }

        meta = newMeta.public;        
        if (inputs.asset_type && (!meta.model)) {
            meta.model = "v0";
        }
        let writeToken = await this.get_write_token(inputs, client, objectId, libraryId)
        outputs.write_token = writeToken

        await client.ReplaceMetadata({
            objectId, libraryId, writeToken,
            metadata: meta,
            metadataSubtree: "public"
        });
        
        let message = (!inputs.asset_type) ? "Normalized to v0" : "Normalized to v1";

        if (inputs.finalize_write_token){
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
        }
        return ElvOAction.EXECUTION_COMPLETE;
        
    };
    
    async  executeConformMaster({client, objectId, libraryId, inputs, outputs}) {
        //MASTER - Match - 2023-12-10 - ech202324-r1-008 - USAP v Emirates Lions
        // 2023-12-10 - urc202324-r1-008 - USAP v Emirates Lions - MASTER
        let name = "MASTER - Match - " + inputs.game_date + " - " + inputs.ip_title_id + " - " + inputs.game_name;
        outputs.production_master_object_name = name;
        if (inputs.admin_group) {
            this.reportProgress("Setting manage permission for " + inputs.admin_group);
            // ADM - Do we need to handle the write token here ?
            await client.AddContentObjectGroupPermission({
                objectId,
                groupAddress: inputs.admin_group,
                permission: "manage"
            });
            this.ReportProgress("Manage permission set for " + inputs.admin_group);
        }
        let writeToken = await this.get_write_token(inputs, client, objectId, libraryId)
        outputs.write_token = writeToken
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
        if (inputs.finalize_write_token){
            let result = await this.FinalizeContentObject({
                objectId, libraryId, writeToken, client,
                commitMessage: "Setting name and type"
            });
            if (result?.hash) {
                outputs.production_master_version_hash = result.hash;
            } else {
                this.ReportProgress("Failed to finalized object", result);
                return ElvOAction.EXECUTION_EXCEPTION;
            }
        }else{
            return ElvOAction.EXECUTION_COMPLETE;
        }
    };
    
    async executeCreateVariant({client, objectId, libraryId, inputs, outputs}) {
        let writeToken = await this.get_write_token(inputs, client, objectId, libraryId)
        outputs.write_token = writeToken
        let meta = await this.getMetadata({objectId, libraryId, client, writeToken: writeToken, metadataSubtree: "production_master"});
        this.ReportProgress("metadata",meta)
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
            await client.ReplaceMetadata({
                objectId, libraryId, writeToken,
                metadataSubtree: "production_master/variants/"+inputs.variant_name,
                metadata: variant
            });
            if (inputs.finalize_write_token) {
                response = await this.FinalizeContentObject({
                    objectId, libraryId, writeToken, client,
                    commitMessage: "Adding generated variant "+ inputs.variant_name
                });
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
            }
        }
        this.ReportProgress("Execution completed without saving write token");
        return ElvOAction.EXECUTION_COMPLETE;
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
                logger.Error("Error writing to target file: " + data_file, exception);
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
        let meta = await this.getMetadata({objectId: mez_object_id, libraryId, writeToken: inputs.write_token, client, metadataSubtree: "offerings/default"})
        // ADM - I assume all metadata are represented as int
        // if not the case then add parseInt
        const bit_rate = meta.media_struct.streams.video.bit_rate
        if (bit_rate < MIN_BIT_RATE_TO_ACCEPT  || bit_rate > MAX_BIT_RATE_TO_ACCEPT) {
            outputs.qc_message = "Mezzanine Bit Rate outside acceptable range: " + bit_rate
            return ElvOAction.EXECUTION_FAILED
        }
        outputs.qc_message = "Mezzanine Bit Rate " + bit_rate
        return ElvOAction.EXECUTION_COMPLETE
    }

    /**
     * Retreives the public metadata for the match specified in the metadata_file_path
     */
    async executeGetMetadataFromFile({client, objectId, libraryId, inputs, outputs}){
        // ADM - The logic of extracting the metadata from the xml file is implemented in the extract_metadata method
        // Here we need to locate the xml_file and call the extract_metadata method
        // then save the metadata into the objectId
        outputs.metadata = await this.extract_metadata(inputs.metadata_file_path)
        if (!outputs.metadata) {
            this.reportProgress("No metadata file found for "+inputs.metadata_file_path)
            return ElvOAction.EXECUTION_FAILED;
        }
        return ElvOAction.EXECUTION_COMPLETE;  
    }    

    /**
     * Retreives the media link for the specified match
     r
     */
    async executeGetMediaUrl({client, objectId, libraryId, inputs, outputs}){
        // ADM - The logic of extracting the metadata from the xml file is implemented in the extract_metadata method
        // Here we need to locate the xml_file and call the extract_metadata method
        // then save the metadata into the objectId
        outputs.media_link = await this.extract_media_url(inputs.metadata_file_path)        
        if (!outputs.media_link) {
            this.reportProgress("No media link found for "+inputs.metadata_file_path)
            return ElvOAction.EXECUTION_FAILED;
        }
        return ElvOAction.EXECUTION_COMPLETE;  
    }    


    /**
    * Public Metadata Handling
    * 
    * For URC we decided to use a specific structure for public metadata:
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
    async extract_metadata(xml_file) {
        /* EXAMPLE of metadata XML file
        <?xml version="1.0" encoding="utf-8"?>
        <item>
        <title><![CDATA[Leinster Vs. Vodacom Bulls, 14-06-2025, 18:00]]></title>
        <description><![CDATA[*]]></description>
        <guid>7ca0dd29-6b6c-4895-8667-02098f5b3228</guid>
        <link>https://fullgameprodeus2.blob.core.windows.net/fullgames/83806_pro14rugby_281015_b99056ea-0d90-4e2d-9970-66934720f3b2.mp4</link>
        <sourceDoc>
            <Body><![CDATA[Leinster Vs. Vodacom Bulls, 14-06-2025, 18:00]]></Body>
            <Category><![CDATA[Full Match Replays]]></Category>
            <Coach><![CDATA[Kieran Crowley]]></Coach>
            <Fixture_OPTA_ID><![CDATA[281015]]></Fixture_OPTA_ID>
            <Label><![CDATA[Full Match Replays]]></Label>
            <Language><![CDATA[English]]></Language>
            <Official_OPTA_ID><![CDATA[281015]]></Official_OPTA_ID>
            <Player_OPTA_ID><![CDATA[5356,193699,211240,5586,133837,126800,229809,144570,225401,203381,196853,131501,165888,177342,169055,229690,219828,241698,226014,109852,109783,105748,225264,107890,116468,107434,147251,223413,130029,211746,246819,237078,148279,148123,156781,220697,118531,166187,210564,158431,178659,110114,244717,221220,237234,229812,117209]]></Player_OPTA_ID>
            <Round><![CDATA[]]></Round>
            <Season><![CDATA[2025]]></Season>
            <SysEntryEntitlements><![CDATA[1]]></SysEntryEntitlements>
            <Tags><![CDATA[]]></Tags>
            <Team><![CDATA[Leinster,Vodacom Bulls,]]></Team>
            <Title><![CDATA[Leinster Vs. Vodacom Bulls, 14-06-2025, 18:00]]></Title>
            <Video_Duration />
        </sourceDoc>
        <Source>WSC</Source>
        </item>
        */
        // Create a parser instance
        const xml_parser = new xml2js.Parser({ explicitArray: false });
        const metadata = {}
        // Read the XML file
        let xml_data = fs.readFileSync(xml_file, 'utf8')
        this.reportProgress("Extracting metadata from file ",xml_file)
        // Parse the XML data
        xml_parser.parseString(xml_data, (err, result) => {
            if (err) {
                logger.Error('Error parsing XML:', err);
                return;
            } 
            metadata.public = {}
            metadata.link = result.item.link
            metadata.public.asset_metadata = {}
            metadata.public.asset_metadata.info = {}
            metadata.public.asset_metadata.info.tournament_id = "urc"
            metadata.public.asset_metadata.info.tournament_name = "United Rugby Championship"
            metadata.public.asset_metadata.info.opta_id = result.item.sourceDoc.Fixture_OPTA_ID
            // Extract the fields
            if (result.item.sourceDoc.Category == "Full Match Replays") {
                metadata.public.asset_metadata.asset_type = "primary"
                metadata.public.asset_metadata.title_type = "Match"
            } else {
                // ADM - we assume that all other categories are highlights
                metadata.public.asset_metadata.asset_type = "auxiliary"                
                metadata.public.asset_metadata.title_type = "Highlights"
            }


            if (metadata.public.asset_metadata.info.opta_id == null) {
                // Leinster Vs. Vodacom Bulls, 14-06-2025, 18:00

                const title = result.item.sourceDoc.Title
                const parser = new RegExp(/^(.*) Vs\. (.*), ([0-9\-]+), ([0-9:]+)$/).exec(title)
                const full_match_parser_without_date = new RegExp(/^(.*) [V|v]s\. (.*) - Full Game Stream$/).exec(title)
                if (parser) {
                    metadata.public.asset_metadata.info.team_home_name = this.adapt_if_needed(parser[1].trim())
                    metadata.public.asset_metadata.info.team_home_code = team_map.get(metadata.public.asset_metadata.info.team_home_name)
                    metadata.public.asset_metadata.info.team_away_name = this.adapt_if_needed(parser[2].trim())
                    metadata.public.asset_metadata.info.team_away_code = team_map.get(metadata.public.asset_metadata.info.team_away_name)
                } else {
                    // if it's not a march, then it's an highligh
                    // Zebre Parma v Vodacom Bulls | Extended Highlights | Round 3 | URC 2023/24
                    const highlight_parser = new RegExp(/^(.*) v (.*) \| (.*) \| (.*) \| URC (.*)$/).exec(title)
                    if (highlight_parser) {
                        metadata.public.asset_metadata.info.team_home_name = this.adapt_if_needed(parser[1].trim())
                        metadata.public.asset_metadata.info.team_home_code = team_map.get(metadata.public.asset_metadata.info.team_home_name)
                        metadata.public.asset_metadata.info.team_away_name = this.adapt_if_needed(parser[2].trim())
                        metadata.public.asset_metadata.info.team_away_code = team_map.get(metadata.public.asset_metadata.info.team_away_name)                    
                        // Not available for highlights
                        const highlight_date = this.find_date_by_file_name(xml_file)
                        let date_parser = new RegExp(/^([0-9]{2})-([0-9]{2})-([0-9]{4})$/).exec(highlight_date)
                        metadata.public.asset_metadata.info.date = date_parser[3].trim() + "-" + date_parser[2].trim() + "-" + date_parser[1].trim()
                        metadata.public.asset_metadata.info.start_time = parser[4].trim()                

                    } else {
                        this.Error('Could not parse title:', title);
                    return null;
                    }
                }
            }
        })
        let opta_data = null

        if (metadata.public.asset_metadata.info.opta_id) {
            opta_data = opta_metadata[metadata.public.asset_metadata.info.opta_id]
            metadata.public.asset_metadata.info.team_home_name = this.adapt_if_needed(opta_data.home_team.trim())
            metadata.public.asset_metadata.info.team_home_code = team_map.get(metadata.public.asset_metadata.info.team_home_name)
            metadata.public.asset_metadata.info.team_away_name = this.adapt_if_needed(opta_data.away_team.trim())
                metadata.public.asset_metadata.info.team_away_code = team_map.get(metadata.public.asset_metadata.info.team_away_name)
        } else {
            // ADM - Here we need to extract the round and match index from the OPTA data
            const date_parser = new RegExp(/^([0-9]{4})-([0-9]{2})-([0-9]{2})$/).exec(metadata.public.asset_metadata.info.date)
            let year = null
            if (date_parser) {
                year = date_parser[1];
                if (date_parser[2] <= "07") {
                    year = date_parser[1] - 1;                
                }
                metadata.public.asset_metadata.info.tournament_season = year + "-" + (parseInt(year) + 1);
            }
            // If the date is not in the title, year is not known
            // best is to change the call to use opta_id directly

            // we need to retrieve these from the opta feed
            opta_data = await this.get_opta_data(metadata.public.asset_metadata.info.team_home_name, metadata.public.asset_metadata.info.team_away_name, metadata.public.asset_metadata.info.date,year,metadata.public.asset_metadata.info.opta_id)

        }
        metadata.public.asset_metadata.info.tournament_season = opta_data.tournament_season
        metadata.public.asset_metadata.info.date = opta_data.date

        let round = opta_data.round
        let match_index = String(opta_data.index + 1) // ADM - index is zero-based, we need to make it one-based
        match_index.padStart(3, '0')
        metadata.public.asset_metadata.info.time = opta_data.time
        metadata.public.asset_metadata.info.tournament_stage_short = this.find_round_short_name(round)
        metadata.public.asset_metadata.info.tournament_stage = this.find_round_name(metadata.public.asset_metadata.info.tournament_stage_short)
        metadata.public.asset_metadata.info.tournament_name = "United Rugby Championship"
        metadata.public.asset_metadata.info.tournament_id = "urc"

        const slug = metadata.public.asset_metadata.info.tournament_id + metadata.public.asset_metadata.info.tournament_season.replace("-20","") + "-" + round + "-" + match_index;

        metadata.public.description = "United Rugby Championship - " + metadata.public.asset_metadata.info.tournament_season + " - " + round + " - " + metadata.public.asset_metadata.info.team_home_name + " v " + metadata.public.asset_metadata.info.team_away_name
        metadata.public.asset_metadata.slug = slug 
        metadata.public.asset_metadata.ip_title_id = slug 
        metadata.public.asset_metadata.info.match_id = slug            

        if (metadata.public.asset_metadata.title_type != "Match") {
            metadata.public.description += " - " + metadata.public.asset_metadata.title_type
            metadata.public.asset_metadata.slug += " -" + metadata.public.asset_metadata.title_type.toLowerCase();
            metadata.public.asset_metadata.ip_title_id += " -" + metadata.public.asset_metadata.title_type.toLowerCase();                
        }

        metadata.public.name = metadata.public.asset_metadata.info.date + " - " + metadata.public.asset_metadata.info.match_id + " - " + metadata.public.asset_metadata.info.team_home_name + " v " + metadata.public.asset_metadata.info.team_away_name + " - " + metadata.public.asset_metadata.title_type.toUpperCase() + " - VOD"
        metadata.public.asset_metadata.title = metadata.public.name;
        this.reportProgress("Extracted metadata for match", metadata)
        return metadata
    }

    /**
     * Finds the date for a given file_name in the CSV file using csv2json.
     * @param {string} fileName - The file_name to search for.
     * @param {string} csvPath - Path to the CSV file.
     * @returns {Promise<string|null>} - The date if found, otherwise null.
    */
    async find_date_by_file_name(fileName, csvPath = '/home/o/elv-o/metadata/full_list-highlights.csv') {
        const jsonArray = await csv().fromFile(csvPath);
        const result = jsonArray.find(row => row.file_name === fileName);
        return result ? result.date : null;
    }

    async extract_media_url(xml_file) {
        /* EXAMPLE of metadata XML file
        <?xml version="1.0" encoding="utf-8"?>
        <item>
        <title><![CDATA[Leinster Vs. Vodacom Bulls, 14-06-2025, 18:00]]></title>
        <description><![CDATA[*]]></description>
        <guid>7ca0dd29-6b6c-4895-8667-02098f5b3228</guid>
        <link>https://fullgameprodeus2.blob.core.windows.net/fullgames/83806_pro14rugby_281015_b99056ea-0d90-4e2d-9970-66934720f3b2.mp4</link>
        <sourceDoc>
            <Body><![CDATA[Leinster Vs. Vodacom Bulls, 14-06-2025, 18:00]]></Body>
            <Category><![CDATA[Full Match Replays]]></Category>
            <Coach><![CDATA[Kieran Crowley]]></Coach>
            <Fixture_OPTA_ID><![CDATA[281015]]></Fixture_OPTA_ID>
            <Label><![CDATA[Full Match Replays]]></Label>
            <Language><![CDATA[English]]></Language>
            <Official_OPTA_ID><![CDATA[281015]]></Official_OPTA_ID>
            <Player_OPTA_ID><![CDATA[5356,193699,211240,5586,133837,126800,229809,144570,225401,203381,196853,131501,165888,177342,169055,229690,219828,241698,226014,109852,109783,105748,225264,107890,116468,107434,147251,223413,130029,211746,246819,237078,148279,148123,156781,220697,118531,166187,210564,158431,178659,110114,244717,221220,237234,229812,117209]]></Player_OPTA_ID>
            <Round><![CDATA[]]></Round>
            <Season><![CDATA[2025]]></Season>
            <SysEntryEntitlements><![CDATA[1]]></SysEntryEntitlements>
            <Tags><![CDATA[]]></Tags>
            <Team><![CDATA[Leinster,Vodacom Bulls,]]></Team>
            <Title><![CDATA[Leinster Vs. Vodacom Bulls, 14-06-2025, 18:00]]></Title>
            <Video_Duration />
        </sourceDoc>
        <Source>WSC</Source>
        </item>
        */
        // Create a parser instance
        const xml_parser = new xml2js.Parser({ explicitArray: false })
        let link = null

        let xml_data = fs.readFileSync(xml_file, 'utf8')
        
        // Parse the XML data
        xml_parser.parseString(xml_data, (err, result) => {
            if (err) {
                logger.Error('Error parsing XML:', err)
                return;
            } 
            link = result.item.link
        })        

        if (link == null) {
            throw Error("Link not found in XML file")
        }        
        return link
    }

    async get_opta_data(team_home_name, team_away_name, match_date, year,id) {        
        const authetication_header = 'Basic YWFkaWxtdWtodGFyOkFsbXVraHRhcjcm'
        let rows = [];
        if (id != null) {
            this.reportProgress("Querying using opta_id ",id)
            await this.getInfoPromiseForOptaID(rows,id, authetication_header)
            // here we don't have the index
        } else {
            const comp_id = "1068"        
            this.reportProgress("Querying using comp_id " + comp_id + " and year " + year)
            await this.getInfoPromise(rows,comp_id,year, authetication_header)
        }
  
        for (let index = 0; index < rows.length; index++) {
            const match = rows[index];
            if ((match.id == id) ||
                (match.date == match_date && match.home_team == team_home_name && match.away_team == team_away_name)){            
                return match
                
            }
        }
        throw Error("Match not found")
    }

    async getInfoPromise(rows,comp_id,year, authenticationHeader) {      
        let path = `/rugby/v1/match/search?compId=${comp_id}&seasonId=${year}01`  
        let options = {
            hostname: 'api.rugbyviz.com',
            port: 443,
            path: path,
            method: 'GET',
            headers : { "Authorization" : authenticationHeader,
            accept: 'application/json'
            } 
        }

        return new Promise((resolve,reject) => {
            let body = '';

            const req = https.get(options, (res) => {
            
                res.on('data', (d) => {
                    body += d;    
                });
        
                res.on('end', () =>{          
                    JSON.parse(body).forEach( (item, index, full_array) => {
                        let entry = {}
                        // "dateTime": "2025-06-14T16:00:00.000Z",
                        entry.date = item["dateTime"].substring(0,10) // YYYY-MM-DD
                        entry.id = item["id"]
                        entry.home_team = item["homeTeam"]["name"]
                        entry.away_team = item["awayTeam"]["name"]
                        entry.time = item["dateTime"].substring(11,19) // HH:MM:ss
                        entry.tournament_season = item["season"]["name"].replace("/","-20")
                        entry.index = index
                        entry.round = item["title"] // ADM - title is either a number (for rounds) or a string (for QF, SF, F)
                        if ( !isNaN(entry.round) ){
                            entry.round = "R" + entry.round
                        }
                        if (entry.round == "TF") {
                            entry.round = "F"
                        }                
                        rows.push(entry);
                    });        
                resolve(rows);
                })
            })
            
            req.on('error', (e) => {
                logger.Error(e);
                reject(e);
            })
        })
    }

    async getInfoPromiseForOptaID(rows,opta_id, authenticationHeader) {      
        let path = `/rugby/v1/match/${opta_id}`  
        let options = {
            hostname: 'api.rugbyviz.com',
            port: 443,
            path: path,
            method: 'GET',
            headers : { "Authorization" : authenticationHeader,
            accept: 'application/json'
            } 
        }

        return new Promise((resolve,reject) => {
            let body = '';

            const req = https.get(options, (res) => {
            
                res.on('data', (d) => {
                    body += d;    
                });
        
                res.on('end', () =>{          
                    JSON.parse(body).forEach( (item, index, full_array) => {
                        let entry = {}
                        // "dateTime": "2025-06-14T16:00:00.000Z",
                        entry.date = item["dateTime"].substring(0,10) // YYYY-MM-DD
                        entry.id = item["id"]
                        entry.home_team = item["homeTeam"]["name"]
                        entry.away_team = item["awayTeam"]["name"]
                        entry.time = item["dateTime"].substring(11,19) // HH:MM:ss
                        entry.index = index
                        entry.round = item["title"] // ADM - title is either a number (for rounds) or a string (for QF, SF, F)
                        if ( !isNaN(entry.round) ){
                            entry.round = "R" + entry.round
                        }
                        if (entry.round == "TF") {
                            entry.round = "F"
                        }                
                        rows.push(entry);
                    })        
                resolve(rows);
                })
            })
            
            req.on('error', (e) => {
                logger.Error(e);
                reject(e);
            })
        })
    }

    adapt_if_needed(team_name){
        let adapted_name = similar_name_mapping.get(team_name)
        if (adapted_name != null){
            return adapted_name
        } else {
            return team_name
        }
    }  

    find_round_name(round_short_form){
        const regEx = new RegExp(/R(\d)$/)
        if (round_short_form.match(regEx) != null)
            return "Group Stage Round " + round_short_form.match(regEx)[1];
        switch (round_short_form.toUpperCase()) {
            case "R16":
            case "RO16":  
            case "RNULL":
            return "Round of 16";
            case "TF":
            case "F":
            return "Final";
            case "SF":
            return "Semifinals";
            case "QF":
            return "Quarterfinals";      
            default:
               if (round_short_form.match(/R(\d\d)$/) != null)
                    return "Group Stage Round " + round_short_form.match(/R(\d\d)$/)[1];
                else
                    throw new Error("Can't find round long form for " + round_short_form);
        }
    }

    find_round_short_name(original_round) {
        switch(original_round.toUpperCase()) {
            case "TF":
            return "F"
            case "RNULL":
            return "RO16"
        }

        return original_round
    }
    
    static REVISION_HISTORY = {
        "0.0.1": "ADM - Initial release - copy from EPCR",
        "0.0.2": "Adds write token support",
    }

    static VERSION = "0.0.2"
}

if (ElvOAction.executeCommandLine(ElvOActionUrcVariants)) {
    ElvOAction.Run(ElvOActionUrcVariants)
} else {
    module.exports=ElvOActionUrcVariants
}
