const ElvOAction = require("../o-action").ElvOAction;
const ElvOFabricClient = require("../o-fabric");

const mime = require("mime-types");
const fs = require("fs");
const Path = require('path');
const moment = require("moment");
const { threadId } = require("worker_threads");

class ElvOActionCreateMezzanine extends ElvOAction  {
    
    ActionId() {
        return "create_mezzanine";
    };
    
    IsContinuous() {
        return true; //false;
    };
    
    Parameters() {
        return {"parameters": {
            aws_s3: {type: "boolean"},
            identify_by_version: {type: "boolean", required:false, default: true},
            add_downloadable_offering: {type: "boolean", required:false, default: false},
            unified_audio_drm_keys: {type: "boolean", required: false, default: false},
            modify_existing_mezzanine: {type: "boolean", required: false, default: false},
            incomplete_tolerance: {type: "numeric", required: false, default: 100}
        }};
    };
    
    IOs(parameters) {
        let inputs = {
            mezzanines_type: {type: "string", required:false},
            mezzanines_lib: {type: "string", required:false},
            mezzanine_object_id:	{type: "string", required:false},
            content_admins_group:	{type: "string", required:false, default:null},
            abr_profile: {type:"object", required:false},
            variant: {type:"string", required: false, description: "Variant of the mezzanine", default: "default"},
            offering_key: {type:"string", required: false, description: "Offering key for the new mezzanine", default: "default"},
            ip_title_id: {type: "string", required:false},
            title: {type: "string", required:false},
            display_title: {type: "string", required:false},
            slug: {type: "string", required:false},
            asset_type: {type: "string", required:false},
            title_type: {type: "string", required:false},
            metadata: {type: "object", required:false},
            merge_metadata: {type: "boolean", required: false, default: false},
            name: {type: "string", required:false},
            elv_geo: {
                type: "string", required:false,
                values: ["na-west-north", "na-west-south", "na-east", "eu-west", "eu-east", "as-east", "au-east"],
                description: "Geographic region for the fabric nodes"
            },
            private_key: {type: "password", required:false},
            config_url: {type: "string", required:false},
        };
        if (parameters.identify_by_version == false){
            inputs.production_master_object_id = {type: "string", required:true};
        } else {
            inputs.production_master_version_hash = {type: "string", required:true};
        }
        if (parameters.modify_existing_mezzanine) {
            inputs.stream_keys = {type: "array", required:true};
        }
        if (parameters.aws_s3) {
            inputs.cloud_access_key_id = {type: "string", required:false};
            inputs.cloud_secret_access_key = {type: "password", required:false};
            inputs.cloud_crendentials_path = {type: "file", required:false};
            inputs.cloud_bucket = {type: "string", required:false};
            inputs.cloud_region = {type: "string", required:false};
            inputs.signed_url = {type: "string", required:false};
            inputs.signed_urls = {type: "array", required:false};
        }
        if (parameters.add_downloadable_offering){
            inputs.downloadable_offering_suffix = {type: "file", required:false, default:"_downloadable"};
        }
        inputs.entry_point_sec = {type: "numeric", required: false, default: null};
        inputs.entry_point_rat = {type: "string", required: false, default: null};
        inputs.exit_point_sec = {type: "numeric", required: false, default: null};
        inputs.exit_point_rat = {type: "string", required: false, default: null};
        let outputs = {
            mezzanine_object_id: {type: "string"},
            mezzanine_object_version_hash: {type: "string"}
        };
        return { inputs : inputs, outputs: outputs };
    };
    
    
    slugify(str) {
        return (str || "").toLowerCase().replace(/ /g, "-").replace(/[^a-z0-9\-]/g,"");
    };
    
    PollingInterval() {
        return 60; //poll every minutes
    };
    
    async Execute(handle, outputs) {
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
        
        let masterHash = this.Payload.inputs.production_master_version_hash;
        if (!masterHash) {
            masterHash = await this.getVersionHash({objectId: this.Payload.inputs.production_master_object_id, client});
            this.Debug("masterHash: " + masterHash, this.Payload.inputs.production_master_object_id);
        }
        let library = this.Payload.inputs.mezzanines_lib;
        let type = this.Payload.inputs.mezzanines_type;
        let variant =  this.Payload.inputs.variant;
        let offeringKey = this.Payload.inputs.offering_key;
        let existingMezzId = this.Payload.inputs.mezzanine_object_id;
        let name = this.Payload.inputs.name;
        let ipTitleId = this.Payload.inputs.ip_title_id;
        let title = this.Payload.inputs.title;
        let displayTitle = this.Payload.inputs.display_title;
        let slug = this.Payload.inputs.slug;
        let titleType = this.Payload.inputs.title_type;
        let assetType = this.Payload.inputs.asset_type;
        let metadata = this.Payload.inputs.metadata;
        let elvGeo = this.Payload.inputs.elv_geo;
        let abrProfile = this.Payload.inputs.abr_profile;
        let mergeMetadata = this.Payload.inputs.merge_metadata || (!metadata == false);  //unintuitive - when no metadata, the original is all read and replaced
        let cloud_access_key_id = this.Payload.inputs.cloud_access_key_id;
        let cloud_secret_access_key = this.Payload.inputs.cloud_secret_access_key;
        let cloud_region = this.Payload.inputs.cloud_region;
        let cloud_bucket = this.Payload.inputs.cloud_bucket;
        let cloud_crendentials_path = this.Payload.inputs.cloud_crendentials_path;
        let signedUrls = this.Payload.inputs.signed_urls || (this.Payload.inputs.signed_url && [this.Payload.inputs.signed_url]);
        if (cloud_crendentials_path) {
            let inputFileData = await this.acquireFile(cloud_crendentials_path);
            cloud_crendentials_path = inputFileData.location;
            await inputFileData.acquisition;
        }
        if (ipTitleId) {
            ipTitleId = ipTitleId.toString();
        }
        if (!existingMezzId && !title) {
            this.ReportProgress("title is required unless an mezzanine object id is specified");
            return ElvOAction.EXECUTION_EXCEPTION;
        }
        if (elvGeo) {
            this.Info("TODO - Setting region for elv-client not implemented here.")
        }
        let existingMetadata;
        if (existingMezzId) {
            library = await client.ContentObjectLibraryId({objectId: existingMezzId});
            existingMetadata = await this.getMetadata({objectId: existingMezzId, libraryId: library, client: client})
            delete existingMetadata.abr_mezzanine;
        }  
        if (metadata) {
            this.reportProgress("Metadata provided");
            if (((typeof metadata) == "string") && metadata.startsWith("@")) {
                this.reportProgress("Metadata provided as a file");
                try {
                    metadata = fs.readFileSync(metadata.substring(1));
                    metadata = JSON.parse(metadata) || {};
                } catch(errParse) {
                    this.ReportProgress("Error parsing metadata");
                }
            }
            if(!metadata.public) {
                metadata.public = {};
            }                      
        } else {
            this.reportProgress("Metadata not provided");
            if (existingMezzId) {
                metadata = existingMetadata
            } else {
                metadata = {};
            }           
        }
        
        if (!metadata.public) {
            metadata.public = {};
        }
        if (!metadata.public.asset_metadata) {
            metadata.public.asset_metadata = {info:{}};
        }
        if (ipTitleId) {
            metadata.public.asset_metadata.ip_title_id = ipTitleId;                
        } else {
            if (!metadata.public.asset_metadata.ip_title_id &&  !(existingMetadata && existingMetadata.public && existingMetadata.public.asset_metadata && existingMetadata.public.asset_metadata.ip_title_id) ) {
                this.ReportProgress("Existing mez does not have 'ip_title_id' set and ip_title_id argument was not provided");
                return ElvOAction.EXECUTION_EXCEPTION;
            } else {
                if (!metadata.public.asset_metadata.ip_title_id) {
                    metadata.public.asset_metadata.ip_title_id = existingMetadata && existingMetadata.public && existingMetadata.public.asset_metadata && existingMetadata.public.asset_metadata.ip_title_id;
                }
            }
        }
        if (title) {
            metadata.public.asset_metadata.title = title;
        } else {
            if (!metadata.public.asset_metadata.title
                &&  !(existingMetadata && existingMetadata.public && existingMetadata.public.asset_metadata && existingMetadata.public.asset_metadata.title)) {
                    this.ReportProgress("Existing mez does not have 'title' set and title argument was not provided");
                    return ElvOAction.EXECUTION_EXCEPTION;
                } else {
                    if (!metadata.public.asset_metadata.title) {
                        metadata.public.asset_metadata.title = existingMetadata && existingMetadata.public && existingMetadata.public.asset_metadata && existingMetadata.public.asset_metadata.title;
                    }
                }
            }
            if (displayTitle) {
                metadata.public.asset_metadata.display_title = displayTitle;
            } else {
                if (!metadata.public.asset_metadata.display_title) {
                    metadata.public.asset_metadata.display_title = (existingMetadata && existingMetadata.public && existingMetadata.public.asset_metadata && existingMetadata.public.asset_metadata.display_title) ||  metadata.public.asset_metadata.title;
                }
            }
            if (slug) {
                metadata.public.asset_metadata.slug = slug;
            } else {
                if (!metadata.public.asset_metadata.slug) {
                    metadata.public.asset_metadata.slug = (existingMetadata && existingMetadata.public && existingMetadata.public.asset_metadata && existingMetadata.public.asset_metadata.slug) || this.slugify(metadata.public.asset_metadata.display_title || metadata.public.asset_metadata.title);
                }
            }
            if (name) {
                metadata.public.name = name;
            }  else {
                if  (!metadata.public.name) {
                    metadata.public.name = (existingMetadata && existingMetadata.public && existingMetadata.public.name) || metadata.public.asset_metadata.ip_title_id  + " - "+ metadata.public.asset_metadata.title + " MEZ";
                }
            }
            if  (titleType) {
                metadata.public.asset_metadata.title_type = titleType;
            } else {
                if (!metadata.public.asset_metadata.title_type)  {
                    metadata.public.asset_metadata.title_type = existingMetadata && existingMetadata.public && existingMetadata.public.asset_metadata && existingMetadata.public.asset_metadata.title_type;
                }
            }
            if (assetType) {
                metadata.public.asset_metadata.asset_type = assetType;
            } else {
                if (!metadata.public.asset_metadata.asset_type) {
                    metadata.public.asset_metadata.asset_type = existingMetadata && existingMetadata.public && existingMetadata.public.asset_metadata && existingMetadata.public.asset_metadata.asset_type;
                }
            }
            
            if (abrProfile) {
                if (((typeof abrProfile) == "string") && abrProfile.startsWith("@")) {
                    try {
                        abrProfile = JSON.parse(fs.readFileSync(abrProfile.substring(1)));
                    } catch(err){
                        this.reportProgress("Could not read or parse file ", abrProfile.substring(1));
                        this.Error("Could not read or parse file ", abrProfile.substring(1));
                        return -1;
                    }
                }
            }
            
            let access;
            if (this.Payload.parameters.aws_s3) {
                if (cloud_crendentials_path) {
                    access = JSON.parse(fs.readFileSync(cloud_crendentials_path));
                } else {
                    if (!signedUrls) {
                        if(!cloud_region || !cloud_bucket || !cloud_access_key_id || !cloud_secret_access_key) {
                            this.ReportProgress("ERROR - Missing required S3 environment variables: cloud_region, cloud_bucket, cloud_secret_access_key");
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
                    } else {
                        
                        
                        access = [];
                        for (let signedUrl of signedUrls) {
                            let s3Region,s3Bucket,s3Path;
                            let matcher = signedUrl.match(/^https:\/\/s3\.([^\.]+)\.[^\/]+\/([^\/]+)\/(.*)\?/);
                            if (!matcher) {
                                matcher = signedUrl.match(/^https:\/\/([^\.]+)\.s3\.([^\.]+)\.[^\/]+\/(.*)\?/);
                                s3Region = matcher[2];
                                s3Bucket = matcher[1]; //bucket name should not have escaped characters, if it does use decodeURI(matcher[2])
                                s3Path = decodeURI(matcher[3]);
                            } else {
                                s3Region = matcher[1];
                                s3Bucket = matcher[2]; //bucket name should not have escaped characters, if it does use decodeURI(matcher[2])
                                s3Path = decodeURI(matcher[3]);
                            }
                            
                            access.push({
                                path_matchers: [s3Path],
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
                            });
                        }
                    }
                }
            }
            /*
            for (let i=0; i < 20; i++) {
                this.reportProgress("sleeping",i);
                await this.sleep(5000);
            }
            */
            const originalType = type;
            if (type) {
                if (type.startsWith("iq__")) {
                    type = await this.getContentTypeVersionHash({objectId: type, client});
                } else if (!type.startsWith("hq__")) {
                    type = await client.ContentType({name: type});
                }
            }
            if (!type) {
                if (!existingMezzId) {
                    this.ReportProgress("Unable to find content type", originalType);
                    throw Error(`Unable to find content type "${originalType}"`);
                }
            }
            if (existingMezzId) { //check if object is locked
                await this.checkPending(existingMezzId, true, client);
                await this.cleanUpAbrMezzanine({libraryId: library, objectId: existingMezzId, client}); //ZOB
            }
            
            
            let createResponse;
            this.Debug("Creating ABR Mezzanine",{
                libraryId: library,
                objectId: existingMezzId,
                type,
                masterVersionHash: masterHash,
                variant,
                offeringKey: offeringKey,  
                mergeMetadata,
                metadata
            });
            
            let reporter = this;
            ElvOAction.TrackerPath = this.TrackerPath;
            client.ToggleLogging(true, {log: reporter.Debug, error: reporter.Error});
            
            try {
                createResponse = await this.CreateABRMezzanine({
                    libraryId: library,
                    objectId: existingMezzId,
                    type,
                    masterVersionHash: masterHash,
                    variant,
                    offeringKey: offeringKey,
                    mergeMetadata,
                    metadata,
                    abrProfile,
                    client: client
                });
            } catch(err) {
                this.Error("Error initializing mezzanine creation",err);
                return ElvOAction.EXECUTION_EXCEPTION;
            }
            existingMezzId = createResponse && createResponse.id;
            let downloadableSuffix = (this.Payload.parameters.add_downloadable_offering && this.Payload.inputs.downloadable_offering_suffix) || "";
            this.saveMezzanineInfo(existingMezzId, offeringKey, privateKey || "", configUrl || "", downloadableSuffix, library);
            
            try {
                this.Info("Starting mezzanine creation for " + existingMezzId);
                this.Debug("StartABRMezzanineJobs", { 
                    libraryId: library,
                    objectId: existingMezzId,
                    offeringKey,
                    access
                });
                const startResponse = await this.safeExec("client.StartABRMezzanineJobs", [{
                    libraryId: library,
                    objectId: existingMezzId,
                    offeringKey,
                    access,
                    client
                }]);       
                this.markLROStarted(startResponse.lro_draft.write_token,startResponse.lro_draft.node);
            } catch(err) {
                this.Error("Error starting mezzanine creation for " + existingMezzId,err);
                return ElvOAction.EXECUTION_EXCEPTION;
            }
            
            
            //await this.safeExec("client.SetVisibility", [{id: existingMezzId, visibility: 0, client}]);
            //this.ReportProgress("Visibility defaulted to 0");
            if (createResponse.masterDuration && createResponse.masterDuration < 60) {
                this.Debug("Master duration is under 60s, executing synchronous", createResponse.masterDuration);
                let status = ElvOAction.EXECUTION_ONGOING;
                while (status == ElvOAction.EXECUTION_ONGOING) {
                    await this.sleep(10000); // could vary polling depending on phase
                    status = await this.checkLROStatus(client, existingMezzId, library, offeringKey, process.pid, outputs, downloadableSuffix);
                }
                this.reportProgress("LRO ended", status);
                return status;
            } else {
                this.Debug("Master duration is over 60s, executing synchronous anyway but slower poll", createResponse.masterDuration);
                let status = ElvOAction.EXECUTION_ONGOING;
                while (status == ElvOAction.EXECUTION_ONGOING) {
                    await this.sleep(60000); // could vary polling depending on phase
                    status = await this.checkLROStatus(client, existingMezzId, library, offeringKey, process.pid, outputs, downloadableSuffix);
                }
                this.reportProgress("LRO ended", status);
                return status;
                /*
                this.Debug("Master duration is over 60s, executing asynchronous", createResponse.masterDuration || "-unknown-");
                return ElvOAction.EXECUTION_ONGOING;
                */
            }
        };
        
        etaString(seconds) {
            const days = Math.trunc(seconds / 86400);
            const unixTimestamp = moment.unix(seconds).utc();
            const hoursString = unixTimestamp.format("HH");
            const minutesString = unixTimestamp.format("mm");
            const secondsString = unixTimestamp.format("ss");
            
            let dataStarted = false;
            let result = "";
            if(days > 0) {
                dataStarted = true;
            }
            result += dataStarted ? days + "d " : "    ";
            
            if(hoursString !== "00") {
                dataStarted = true;
            }
            result += dataStarted ? hoursString + "h " : "    ";
            
            if(minutesString  !== "00") {
                dataStarted = true;
            }
            result += dataStarted ? minutesString + "m " : "    ";
            
            if(secondsString !== "00") {
                dataStarted = true;
            }
            result += dataStarted ? secondsString + "s " : "    ";
            
            return result;
        };
        
        async checkLROStatus(client, mezzanineObjId, libraryId, offeringKey, pid, outputs, downloadableSuffix) {
            //this.Debug("checkLROStatus", {mezzanineObjId, libraryId, offeringKey, pid, downloadableSuffix});
            try {
                let finalizedHash = this.getFinalizedMezzanineHash();
                if (finalizedHash) {
                    let latestObjectData = await this.getVersionHash({libraryId, objectId: mezzanineObjId, client: client});
                    if (latestObjectData == finalizedHash) { //should check if finalizingHash is one of the hash not the latest
                        outputs.mezzanine_object_id = mezzanineObjId;
                        outputs.mezzanine_object_version_hash = finalizedHash;
                        this.ReportProgress("ABR mezzanine Committed", latestObjectData);
                        
                        await this.grantAdminRights(client, mezzanineObjId)
                        
                        
                        return ElvOAction.EXECUTION_COMPLETE;
                    } else {
                        //Should record hash before finalizing and check hash found against it, if it does not match raise an exception
                        this.ReportProgress("Committing ABR mezzanine object, expecting "+ finalizedHash, latestObjectData);
                        return ElvOAction.EXECUTION_ONGOING; //TO DO: this never times out as the timestamp is changed, need to capture finalization start time
                    }
                }
                if (!this.isLROStarted()) {
                    if (pid && !ElvOAction.PidRunning(pid)) {
                        this.ReportProgress("LRO not started and PID not running", pid);
                        return ElvOAction.EXECUTION_EXCEPTION;
                    } else {
                        this.ReportProgress("LRO not started");
                        return ElvOAction.EXECUTION_ONGOING;
                    }
                }
                let status;
                let lroDraft = this.getLROInformation();
                try {
                    
                    status = await this.getMetadata({
                        libraryId: libraryId,
                        objectId: mezzanineObjId,
                        writeToken: lroDraft.write_token,
                        metadataSubtree: "lro_status",
                        client,
                        node_url: lroDraft.node
                    });
                    if (!status){
                        throw new Error("Could not read lro_status from write token");
                    }
                } catch (err) {
                    if (this.firstStrike) {
                        this.Error("Error reading LRO status for "+ mezzanineObjId, err);
                        this.ReportProgress("Error reading LRO status for "+ mezzanineObjId);
                        return ElvOAction.EXECUTION_EXCEPTION;
                    } else {
                        this.Error("Warning failed to read LRO status for "+ mezzanineObjId, err);
                        this.ReportProgress("Warning failed to read LRO status for "+ mezzanineObjId);
                        this.firstStrike = true;
                        return ElvOAction.EXECUTION_ONGOING;
                    }
                }
                
                let warningsAdded = false;
                let errorsAdded = false;
                let allStatus = {};
                let noProgress = true;
                for (const lroKey in status) {
                    let statusEntry = status[lroKey];
                    if (statusEntry.run_state === "running") {
                        const start = moment.utc(statusEntry.start).valueOf();
                        const now = moment.utc().valueOf();
                        const actualElapsedSeconds = Math.round((now - start) / 1000);
                        const reportedElapsed = Math.round(statusEntry.duration_ms / 1000);
                        const secondsSinceLastUpdate = actualElapsedSeconds - reportedElapsed;
                        
                        // off by more than tolerance?
                        if (secondsSinceLastUpdate > ElvOActionCreateMezzanine.MAX_REPORTED_DURATION_TOLERANCE) {
                            statusEntry.warning = "status has not been updated in " + secondsSinceLastUpdate + " seconds, process may have terminated";
                            this.ReportProgress("warning for "+ lroKey, statusEntry.warning);
                            warningsAdded = true;
                        } else {
                            noProgress = false;
                            let estSecondsLeft;
                            if (statusEntry.progress.percentage) {
                                if  (statusEntry.progress.percentage == 100) {
                                    estSecondsLeft = 0
                                } else {
                                    estSecondsLeft = (statusEntry.duration_ms / 1000) / (statusEntry.progress.percentage / 100) - (statusEntry.duration_ms / 1000);
                                }
                                statusEntry.estimated_time_remaining = this.etaString(estSecondsLeft);
                            }
                            allStatus[lroKey] = statusEntry;
                        }   
                    } else {
                        let percentComplete = status[lroKey].progress.percentage;
                        if (!Number.isFinite(percentComplete)) {
                            statusEntry.warning = "LRO " + lroKey + " is not running, but progress is an invalid number instead of 100";
                            this.ReportProgress("error: " + statusEntry.warning, status[lroKey]);
                            errorsAdded = true;
                        } else {
                            if ( percentComplete != 100) {
                                this.ReportProgress("LRO " + lroKey + " is not running, but progress does not equal 100");
                            }
                            let completeness = (this.Payload  && this.Payload.parameters && this.Payload.parameters.incomplete_tolerance) || 100
                            if ( percentComplete < completeness) {
                                statusEntry.warning = "LRO " + lroKey + " is not running, but progress did not reach "+ completeness;
                                this.ReportProgress("error: " + statusEntry.warning, status[lroKey]);
                                errorsAdded = true;
                            }
                            
                        }
                    }
                }
                
                if ((warningsAdded && noProgress) || errorsAdded) {
                    this.ReportProgress("warnings or errors found");
                    return ElvOAction.EXECUTION_EXCEPTION;
                }
                //this.Debug("status", status);
                if (!Object.values(status).every(job => job.run_state === "finished")) {
                    try {
                        let lroCount = 0;
                        let percentComplete;
                        let eta ="";
                        for (let lroKey in allStatus) {
                            lroCount++;
                            let lroStats = allStatus[lroKey];
                            if (lroStats.estimated_time_remaining && lroStats.estimated_time_remaining > eta){
                                eta = lroStats.estimated_time_remaining;
                            }
                            if (!percentComplete || ((lroStats && lroStats.progress && lroStats.progress.percentage && lroStats.progress.percentage) < percentComplete)) {
                                percentComplete = lroStats.progress.percentage;
                            }
                        }
                        this.ReportProgress("LROs running (" +lroCount+")" , "Estimated time remaining: "+eta.trim() + " (" +parseFloat(percentComplete).toFixed(2) +"% complete)");
                    } catch(errReport) {
                        this.ReportProgress("LRO running", JSON.stringify(allStatus));
                    }
                    this.ErrCheckLRO = 0;
                    return ElvOAction.EXECUTION_ONGOING;
                }
                const finalizeResponse = await this.FinalizeABRMezzanine({
                    libraryId,
                    objectId: mezzanineObjId,
                    offeringKey,
                    downloadableSuffix,
                    client,
                    lroDraft
                });
                this.markMezzanineFinalized(finalizeResponse.hash);
                this.ReportProgress("Mezzanine file finalized");
                this.ErrCheckLRO = 0
                return ElvOAction.EXECUTION_ONGOING;
            } catch(errCheckLRO) {
                this.reportProgress("Error checking LRO status", this.ErrCheckLRO);
                this.Error("Error checking LRO status", errCheckLRO);
                if (!this.ErrCheckLRO) {
                    this.ErrCheckLRO = 1;
                } else {
                    this.ErrCheckLRO++
                }
                if (this.ErrCheckLRO > 5) {
                    return ElvOAction.EXECUTION_EXCEPTION
                }
                return ElvOAction.EXECUTION_ONGOING;
            }
        };
        
        
        
        
        saveMezzanineInfo(mezzanineId, offeringKey, privateKey, configUrl, downloadable, mezzanineLibId) {
            this.trackProgress(ElvOActionCreateMezzanine.TRACKER_MEZZANINE_INFO,"Creation of ABR Mezzanine in progress",mezzanineId+","+offeringKey+","+ privateKey+","+configUrl+","+downloadable+","+mezzanineLibId);
        };
        
        retrieveMezzanineInfo(mezzanineId) {
            let infoTracker = this.Tracker[ElvOActionCreateMezzanine.TRACKER_MEZZANINE_INFO];
            if (infoTracker) {
                let info = infoTracker.details.split(",");
                return {mezzanine_object_id: info[0], offering_key: info[1], private_key: info[2], config_url: info[3], add_downloadable_offering: info[4], mezzanine_library_id: info[5]};
            } else {
                return null;
            }
        };
        
        markMezzanineFinalized(mezzanineVersionHash) {
            this.trackProgress(ElvOActionCreateMezzanine.TRACKER_FINALIZED_MEZZANINE, "ABR mezzanine object finalized", mezzanineVersionHash);
        };
        
        getFinalizedMezzanineHash() {
            return this.Tracker && this.Tracker[ElvOActionCreateMezzanine.TRACKER_FINALIZED_MEZZANINE] && this.Tracker[ElvOActionCreateMezzanine.TRACKER_FINALIZED_MEZZANINE].details;
        };
        
        markLROStarted(lroWriteToken,lroNode) {
            this.trackProgress(ElvOActionCreateMezzanine.TRACKER_LRO_STARTED, "ABR Mezzanine Jobs started", lroWriteToken+","+lroNode);
        };
        
        getLROInformation() {
            let info = this.Tracker && this.Tracker[ElvOActionCreateMezzanine.TRACKER_LRO_STARTED].details;
            if (info) {
                let parts = info.split(",");
                return {node: parts[1], write_token: parts[0]};
            } else {
                return null;
            }
        };
        
        isLROStarted() {
            return (this.Tracker && this.Tracker[ElvOActionCreateMezzanine.TRACKER_LRO_STARTED] && true) || false;
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
        
        
        async clipMezzanine(offering) {
            try{
                let inputs = this.Payload.inputs;
                let framerate = offering.media_struct.streams.video.rate;
                let matcher = framerate.match(/^([0-9]+)\/([0-9]+)$/);
                if (!matcher) {
                    throw Error("Invalid framerate format '"+ framerate + "'");
                }
                let entryPointRat =  inputs.entry_point_rat;
                if (inputs.entry_point_sec  != null) {
                    let frameCount = Math.round(inputs.entry_point_sec * matcher[1] / matcher[2]);
                    entryPointRat  = "" + (frameCount * matcher[2]) +"/" + matcher[1];
                } 
                if (entryPointRat != null) {
                    offering.entry_point_rat = entryPointRat;
                    this.reportProgress("Setting up entry point", entryPointRat);
                }
                let exitPointRat =  inputs.exit_point_rat;
                if (inputs.exit_point_sec  != null) {
                    let frameCount = Math.round(inputs.exit_point_sec * matcher[1] / matcher[2]);
                    exitPointRat  = "" + (frameCount * matcher[2]) +"/" + matcher[1];
                }        
                if (exitPointRat != null)  {
                    offering.exit_point_rat = exitPointRat;
                    this.reportProgress("Setting up exit point", entryPointRat);
                }
                return {entryPointRat, exitPointRat};   
            } catch(err) {
                this.Error("Clipping  error", err);
                return null
            }
        };
        
        
        async  cleanUpAbrMezzanine({libraryId, objectId, client}) {
            let existing  = await this.getMetadata({libraryId, objectId, client, metadataSubtree: "abr_mezzanine"});
            if (existing && existing.offerings) {
                this.reportProgress("Found obsolete abr_mezzanine data, deleting...");
                let writeToken = await this.getWriteToken({libraryId, objectId, client});
                await client.DeleteMetadata({
                    libraryId,
                    objectId,
                    writeToken,
                    metadataSubtree: "abr_mezzanine"
                });
                let response = await this.FinalizeContentObject( {
                    libraryId,
                    objectId,
                    writeToken,
                    client,
                    commitMessage: "Cleaned up obsolete data"
                });
                this.reportProgress("Deleted obsolete abr_mezzanine data");
                return response.hash;
            }
            return null;
        }
        async CreateABRMezzanine({
            libraryId,
            objectId,
            type,
            metadata,
            mergeMetadata,
            masterVersionHash,
            abrProfile,
            variant="default",
            offeringKey="default",
            client
        }) {
            if (!client) {
                client = this.Client;
            }
            if(!masterVersionHash) {
                throw Error("Master version hash not specified");
            }
            const existingMez = !!objectId;
            let options = type ? { type } : {};
            let id, write_token;
            if (existingMez) {
                // Edit existing
                id = objectId;
                //await this.cleanUpAbrMezzanine({libraryId, objectId, client});
                write_token = await this.getWriteToken({libraryId, objectId, options, client});
                /* ZOB
                //In case abr_mezzanine is populated, clear it out
                await client.DeleteMetadata({
                    libraryId,
                    objectId: id,
                    writeToken: write_token,
                    metadataSubtree: "abr_mezzanine"
                });
                
                let checkingFirst = await this.getMetadata({
                    libraryId,
                    objectId: id,
                    writeToken: write_token,
                    metadataSubtree: "abr_mezzanine",
                    client
                });
                this.reportProgress("after nixing", checkingFirst);
                */
            } else {
                // Create new
                const createResponse = await this.CreateContentObject({libraryId, options, client});
                id = createResponse.id;
                write_token = createResponse.write_token;
            }
            this.Debug("ABR mezz creation write-token",write_token);
            await this.CreateEncryptionConk({libraryId, objectId: id, writeToken: write_token, createKMSConk: true, client});
            let masterObjectId = client.utils.DecodeVersionHash(masterVersionHash).objectId;
            let masterLibraryId = await this.getLibraryId(masterObjectId, client);
            let masterMetadata = await this.getMetadata({
                libraryId: masterLibraryId,
                objectId: masterObjectId,
                versionHash: masterVersionHash,
                client
            });
            const masterName = masterMetadata.public.name;
            let duration = 0;
            let dimensions = {}
            let streams = masterMetadata.production_master.variants[variant].streams;
            for (let streamId in streams) {
                let sources = streams[streamId].sources;
                for (let source of sources) {
                    try {
                        let stream = masterMetadata.production_master.sources[source.files_api_path].streams[source.stream_index];
                        if (stream.type == "StreamVideo") {
                            duration = stream.duration;
                            dimensions.aspect_ratio = stream.display_aspect_ratio;
                            dimensions.height = stream.height;
                            dimensions.width = stream.width;
                        }
                    } catch(errSource) {
                        this.Info("Could not extract duration", source);
                    }
                }
            }
            // Include authorization for library, master, and mezzanine
            
            
            let timeBeforeAuth = (new Date()).getTime();
            let editToken = await this.EditAuthorizationToken({libraryId, objectId: id, client});
            this.Debug("Edit AuthorizationToken for "+ id + " generated in "  + ((new Date()).getTime() - timeBeforeAuth));
            
            let authorizationTokens = [
                editToken,
                await this.getLibraryToken(libraryId, client),
                await this.generateAuthToken(masterLibraryId, masterObjectId, true, client)
            ];
            const headers = {
                Authorization: authorizationTokens.map(token => `Bearer ${token}`).join(",")
            };
            const body = {
                offering_key: offeringKey,
                variant_key: variant,
                prod_master_hash: masterVersionHash
            };
            let storeClear = false;
            if (abrProfile) {
                if (abrProfile == "auto") {
                    //generate ABR Profile based on dimensions  {aspect_ratio, width,height}
                } else {
                    body.abr_profile = abrProfile;           
                    storeClear = abrProfile.store_clear;
                }
            } else {
                // Retrieve ABR profile from library to check store clear
                storeClear = await this.ContentObjectMetadata({
                    libraryId,
                    objectId: client.utils.AddressToObjectId(client.utils.HashToAddress(libraryId)),
                    metadataSubtree: "abr_profile/store_clear",
                    client
                });
            }
            
            
            if  (this.Payload.parameters.modify_existing_mezzanine){
                body.keep_other_streams = true;
                body.stream_keys = this.Payload.inputs.stream_keys;
            } 
            
            let checkingbeforebitcode = await this.getMetadata({
                libraryId,
                objectId: id,
                writeToken: write_token,
                metadataSubtree: "abr_mezzanine",
                client
            });
            this.reportProgress("before bitcode", checkingbeforebitcode);
            
            const {logs, errors, warnings} = await client.CallBitcodeMethod({
                libraryId,
                objectId: id,
                writeToken: write_token,
                method: "media/abr_mezzanine/init",
                headers,
                body,
                constant: false
            });
            
            if(!metadata) { metadata = {}; }
            if(!metadata.public) { metadata.public = {}; }
            if(!metadata.public.asset_metadata) { metadata.public.asset_metadata = {}; }
            
            metadata.master = {
                name: masterName,
                id: masterObjectId,
                hash: masterVersionHash,
                variant
            };
            
            if (!metadata.public.asset_metadata.sources) {
                metadata.public.asset_metadata.sources = {};
            }
            metadata.public.asset_metadata.sources[offeringKey] = {
                "/": `./rep/playout/${offeringKey}/options.json`
            };
            metadata.elv_created_at = new Date().getTime();
            
            if (mergeMetadata) {
                await client.MergeMetadata({
                    libraryId,
                    objectId: id,
                    writeToken: write_token,
                    metadata
                });
            } else {
                let tokenMetadata = await this.getMetadata({
                    libraryId,
                    objectId: id,
                    writeToken: write_token,
                    //metadataSubtree: "abr_mezzanine",
                    client: client
                });
                metadata.elv = tokenMetadata.elv;
                metadata.abr_mezzanine = tokenMetadata.abr_mezzanine;
                metadata.lro_draft_default = tokenMetadata.lro_draft_default;
                metadata.lro_status = tokenMetadata.lro_status;
                this.reportProgress("abr_mezzanine read from token", metadata.abr_mezzanine);
                let caps = Object.keys(tokenMetadata).filter(function(item){return item.match(/^eluv.caps/)});
                for (let capsKey of caps) {
                    this.reportProgress("Caps read from token "+ capsKey, tokenMetadata[capsKey]);
                    metadata[capsKey] = tokenMetadata[capsKey];
                }
                
                await client.ReplaceMetadata({
                    libraryId,
                    objectId: id,
                    writeToken: write_token,
                    metadata
                });
            }
            const finalizeResponse = await this.FinalizeContentObject({
                libraryId,
                objectId: id,
                writeToken: write_token,
                commitMessage: "Create ABR mezzanine",
                client
            });
            return {
                masterDuration: duration,
                logs: logs || [],
                warnings: warnings || [],
                errors: errors || [],
                ...finalizeResponse
            };
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
        
        
        async FinalizeABRMezzanine({libraryId, objectId, client, downloadableSuffix, offeringKey="default", lroDraft}) {
            if (!client) {
                this.Debug("FinalizeABRMezzanine client not provided");
                client = this.Client;
            }
            if (!lroDraft) {
                lroDraft = await this.getMetadata({
                    libraryId,
                    objectId,
                    client,
                    metadataSubtree: `lro_draft_${offeringKey}`
                });
            }
            if(!lroDraft || !lroDraft.write_token) {
                throw Error("No LRO draft found for this mezzanine");
            }
            
            let error, result;
            try {
                this.Debug("FinalizeABRMezzanine lroDraft token", lroDraft.write_token);
                
                // Authorization token for mezzanine (and master -- not required)
                let editAuthorizationToken = await this.EditAuthorizationToken({libraryId, objectId, client});
                let authorizationTokens = [
                    editAuthorizationToken
                ];
                const headers = {
                    Authorization: authorizationTokens.map(token => `Bearer ${token}`).join(",")
                };
                let data, errors, warnings, logs;
                let finalCompleted=false;
                for (let finalizeAttempt = 0; (finalizeAttempt < 3); finalizeAttempt++ ) {
                    let bitCodeRes = await this.CallBitcodeMethod({
                        objectId,
                        libraryId,
                        writeToken: lroDraft.write_token,
                        method: "media/abr_mezzanine/offerings/" + offeringKey + "/finalize",
                        headers,
                        client,
                        nodeUrl: lroDraft.node,
                        constant: false
                    });
                    data = bitCodeRes.data;
                    errors = bitCodeRes.errors;
                    warnings = bitCodeRes.warnings, 
                    logs = bitCodeRes.logs;
                    if (!errors || (errors.length == 0)) {
                        finalCompleted = true;
                        break;
                    } else {
                        this.Error("Issues encountered calling '"+"media/abr_mezzanine/offerings/" + offeringKey + "/finalize'");
                        for (let errFound of errors) {
                            this.reportProgress("Issue reported", errFound);
                            if (errFound == ("Offering '"+offeringKey+"' is already finalized")) {
                                if (errors.length == 1) {
                                    finalCompleted = true;
                                    break;
                                } else {
                                    this.Error("Error found and with offering already finalized", errors);
                                    return  ElvOAction.EXECUTION_EXCEPTION;
                                }
                            }
                        }               
                        await this.sleep(500);
                    }
                }
                if (!finalCompleted) {
                    throw new Error("finalization of mezzanine did not complete without errors");
                }
                const mezzanineMetadata = await this.getMetadata({
                    libraryId,
                    objectId,
                    client,
                    writeToken: lroDraft.write_token,
                    metadataSubtree: "offerings",
                    node_url: lroDraft.node,
                });
                let modifiedOfferings = false;
                if (this.Payload.parameters.unified_audio_drm_keys) {
                    if (this.unifyAudioDRMKeys(mezzanineMetadata)) {
                        modifiedOfferings = true;
                    }
                }
                if (this.Payload.inputs.entry_point_rat || this.Payload.inputs.entry_point_sec || this.Payload.inputs.exit_point_rat ||this.Payload.inputs.exit_point_sec) {
                    if (this.clipMezzanine(mezzanineMetadata[offeringKey])) {
                        modifiedOfferings = true;
                    }
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
                    try {
                        await client.ReplaceMetadata({
                            libraryId,
                            objectId: objectId,
                            writeToken: lroDraft.write_token,
                            metadataSubtree: "offerings",
                            metadata: mezzanineMetadata
                        });
                        this.reportProgress("Updated offerings to reflect unified Audio DRM keys or entry/exit points");
                    } catch(errMod) {
                        this.Error("Could not update offerings metadata on lro write token", errMod);
                    }
                }
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
                            writeToken: lroDraft.write_token,
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
                    writeToken: lroDraft.write_token,
                    editAuthorizationToken,
                    metadata: tobeMerged,
                    client,
                    nodeUrl: lroDraft.node
                });
                
                const finalizeResponse = await this.FinalizeContentObject({
                    libraryId,
                    objectId: objectId,
                    writeToken: lroDraft.write_token,
                    commitMessage: "Finalize ABR mezzanine",
                    //awaitCommitConfirmation: false,
                    client,
                    nodeUrl: lroDraft.node
                });
                result = {
                    data,
                    logs: logs || [],
                    warnings: warnings || [],
                    errors: errors || [],
                    ...finalizeResponse
                };
            } catch(err) {
                error = err;
            }
            if (error) { throw error; }
            return result;
        };
        
        
        
        
        static TRACKER_MEZZANINE_INFO = 60;
        static TRACKER_LRO_STARTED = 65;
        static TRACKER_FINALIZED_MEZZANINE = 70;
        
        static MAX_REPORTED_DURATION_TOLERANCE = 3600;
        
        static VERSION = "0.2.9";
        static REVISION_HISTORY = {
            "0.0.1": "Initial release",
            "0.0.2": "Private key input is encrypted",
            "0.0.3": "Check PID when LRO is not running",
            "0.0.4": "Uses reworked CreateABRMezzanine",
            "0.0.5": "cloud_secret_access_key now accepts encrypted values",
            "0.0.6": "adds option",
            "0.0.7": "uses PidRunning instead of deprecated pidIsRunning",
            "0.0.8": "only runs synchronously if master duration is under 60 seconds",
            "0.0.9": "allows to timeout if no progress are reported for a while",
            "0.0.10": "Adds option to provide an admin group for the created mezzanine object",
            "0.0.11": "Does not fail on first error reading LRO",
            "0.0.12": "Fixes option to provide an admin group",
            "0.0.13": "Moved in-class the Mezz specific methods previously in o-fabric",
            "0.0.14": "Avoids merging public metadata",
            "0.0.15": "Adds debugging hash when finalizing LRO",
            "0.0.16": "Adds exception if finalize mezzanine does not complete cleanly",
            "0.0.17": "Adds option to unify audio DRM keys",
            "0.1.0": "Adds clipping  option",
            "0.1.1": "Adds verification and retry to grant admin rights option",
            "0.1.2": "Errors out on failed LRO",
            "0.1.3": "Fix support for exit_point_sec clipping",
            "0.1.5": "Fix Audio DRM unifications and clipping",
            "0.1.6": "Clear pending on existing mezz to allow clean retry",
            "0.1.7": "Base entry/exit point on multiple of frame duration",
            "0.1.8": "Fixes bug with disappearing ip_title_id",
            "0.1.9": "Adds support for signed URL master by reference",
            "0.2.0": "Fixes support for signed URL using deprecated format",
            "0.2.1": "Makes downloadable offering a clear only offering",
            "0.2.2": "Adds option to modify existing mezzanine to add audio tracks",
            "0.2.3": "Looks for required field in existing mezzanine object even if metadata is provided.",
            "0.2.4": "Cleans up abr_mezzanine on existing mezzanine object",
            "0.2.5": "Adds option to finalize even if content was not 100 % transcoded",
            "0.2.6": "Use links in downloadable offering to avoid losing captions and storyboards",
            "0.2.7": "adds an info section by default in the asset_metadata",
            "0.2.8": "adds support for multiple signed URLs",
            "0.2.9": "improves readability of progress reports"
        };
    }
    
    
    if (ElvOAction.executeCommandLine(ElvOActionCreateMezzanine)) {
        ElvOAction.Run(ElvOActionCreateMezzanine);
    } else {
        module.exports=ElvOActionCreateMezzanine;
    }
