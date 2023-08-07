
const ElvOAction = require("../o-action").ElvOAction;
const ElvOFabricClient = require("../o-fabric");
const ElvOMutex = require("../o-mutex");
const { execSync } = require('child_process');
const fs = require("fs");


class ElvOActionManageSite extends ElvOAction  {
    
    ActionId() {
        return "manage_site";
    };
    
    Parameters() {
        return {"parameters": {
            action: {type: "string", required: true, values:["ADD","DELETE","SHAKE"]},
            identify_by_version: {type: "boolean", required:false, default: false},
            identify_titles_by: {type: "string", required:false, default: "ip_title_id", values:["ip_title_id","object_id"/*,"version_hash"*/]}
        }
    };
};

IdleTimeout() {
    return 120;
};

PollingInterval() {
    return 30;
};

IOs(parameters) {
    let inputs = {
        private_key: {"type": "password", "required": false},
        config_url: {"type": "string", "required": false},
        safe_update: {type: "boolean", required: false, default: true}
    };
    if (!parameters.identify_by_version) {
        inputs.site_object_id = {type: "string", required: true};
    } else {
        inputs.site_object_version_hash = {type: "string", required: true};
    }
    if (["add","delete"].includes(parameters.action.toLowerCase())){
        inputs.titles = {"type": "array", "required": true};
        inputs.title_libraries = {"type": "array", "required": true};
    }
    if (parameters.action.toLowerCase() == "shake"){
        inputs.title_libraries = {"type": "array", "required": true};
    }
    
    let outputs = {
        modified_object_version_hash: {type:"string"},
        updated_entries: {type: "array"},
        added_entries: {type: "array"},
        deleted_entries: {type: "array"}
    };
    return {inputs: inputs, outputs: outputs}
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
        this.SetMetadataMutex = await ElvOMutex.WaitForLock({name: objectId, holdTimeout: 600000}); 
        this.ReportProgress("Mutex reserved", this.SetMetadataMutex);
        return this.SetMetadataMutex
    }
    return null;
};

async Execute(handle, outputs) {
    //console.error("Execute",handle, this.Payload.inputs);
    let client;
    let objectId;
    let versionHash;
    try {
        if (!this.Payload.inputs.private_key && !this.Payload.inputs.config_url){
            client = this.Client;
        } else {
            let privateKey = this.Payload.inputs.private_key || this.Client.signer.signingKey.privateKey.toString();
            let configUrl = this.Payload.inputs.config_url || this.Client.configUrl;
            client = await ElvOFabricClient.InitializeClient(configUrl, privateKey)
            this.Client = client
        }
        
        let inputs = this.Payload.inputs;
        let cmd = this.Payload.parameters.action.toLowerCase();
        
        if (["add","delete"].includes(cmd) && (inputs.titles.length == 0)) {
            this.ReportProgress("No title to update");
            return ElvOAction.EXECUTION_COMPLETE;
        }
        await this.indexLibraries(inputs.title_libraries, client);
        
        objectId = inputs.site_object_id;
        versionHash = inputs.site_object_version_hash ;
        if (!objectId) {
            objectId = this.Client.utils.DecodeVersionHash(versionHash).objectId;
        }
        let libraryId = await this.getLibraryId(objectId, client);
        if (!versionHash) {
            versionHash = await this.getVersionHash({objectId, libraryId, client});
        }
        
        this.ReportProgress("Retrieving searchables");
        await this.acquireMutex(objectId);
        this.Searchables = await this.getMetadata({client, libraryId,  objectId, versionHash, metadataSubtree: "site_map/searchables", resolve: false}) || {};
        this.ReportProgress("Retrieved " + Object.keys(this.Searchables).length + " searchable entries");
        this.AddedEntries = [];
        this.ModifiedEntries = [];
        this.DeletedEntries = [];
        let count = 0;
        
        if (cmd == "add") {
            await this.processAdd(inputs.titles, inputs.title_libraries, client);
            count = (this.AddedEntries.length + this.ModifiedEntries.length);
            this.NoCachedVersion = true;
        }
        if (cmd == "shake") {
            await this.processAdd(Object.keys(this.Searchables), inputs.title_libraries, client); 
            count = (this.AddedEntries.length + this.ModifiedEntries.length);
            this.NoCachedVersion = false;
        }
        
        if (count > 0) {
            this.ReportProgress("Saving changes to  "+ count + " titles");
            try {
                let writeToken = await this.getWriteToken({objectId, libraryId, client});
                await client.ReplaceMetadata({
                    objectId, libraryId,
                    metadataSubtree: "site_map/searchables",
                    metadata: this.Searchables,
                    writeToken
                });
                this.ReportProgress("Write-token acquired ", writeToken);
                let msg = "Processed " + count + " link changes";
                let currentHash = await this.getVersionHash({objectId, libraryId, client});
                if (currentHash != versionHash) {
                    this.reportProgress("Object was modified while update was being processed" , {currentHash,versionHash});
                    throw new Error("Object was modified while update was being processed");
                }
                let response = await this.FinalizeContentObject({
                    libraryId: libraryId,
                    objectId: objectId,
                    writeToken: writeToken,
                    commitMessage: msg,
                    client
                });
                if (response) {
                    this.ReportProgress(msg);
                    outputs.modified_object_version_hash = response.hash;
                    outputs.updated_entries =  this.ModifiedEntries.map(function(item){return {ip_title_id: item.ip_title_id,  object_id: item.object_id, version_hash: item.version_hash}});
                    outputs.added_entries = this.AddedEntries.map(function(item){return {ip_title_id: item.ip_title_id,  object_id: item.object_id, version_hash: item.version_hash}});
                    outputs.deleted_entries = this.DeletedEntries.map(function(item){return {ip_title_id: item.ip_title_id,  object_id: item.object_id}});
                    return ElvOAction.EXECUTION_COMPLETE;
                } else {
                    this.ReportProgress("Could not finalize " + objectId, writeToken);
                    this.Error("Could not finalize " + objectId, writeToken);
                    return ElvOAction.EXECUTION_EXCEPTION;
                }
            } catch(err) {
                this.ReportProgress("Error - Could not save changes to site");
                this.Error("Could not save changes to site", err);
                return ElvOAction.EXECUTION_EXCEPTION;
            }
        } else {
            this.ReportProgress("No links to change for site object " + objectId);
            return ElvOAction.EXECUTION_FAILED;
        }
        
    } catch(err) {
        this.Error("Could not process site update for " + (objectId || versionHash), err);
        return ElvOAction.EXECUTION_EXCEPTION;
    } finally {
        this.releaseMutex();
    }
    
};


async getTitleData(title, client) {  //{ip_title_id, version_hash, object_id
    try {
        if (!this.ObjectMap) {
            let libraryId = await this.getLibraryId(title, client);
            let assetMetadata = await this.getMetadata({
                objectId: title,
                libraryId,
                client,
                metadataSubtree: "public/asset_metadata"
            });
            if (assetMetadata.title_type == "episode") {
                
            }
            let versionHash = await this.getVersionHash({objectId: title, libraryId, client});
            return {ip_title_id: assetMetadata.ip_title_id, version_hash: versionHash, object_id: title};
        }
    } catch(err) {
        this.Error("Could not retrieve title data for " + title, err);
        this.reportProgress("Could not retrieve title data for " + title);
        return null;
    }
};

async processAdd(titles, title_libraries, client) {
    let episodes = {};
    let seasons = {};
    let series = {};
    this.ReportProgress("Processing features");
    for (let title of titles) {
        try {
            let data = await this.ObjectMap[title];
            if (!data) {
                throw "No match found in provided libraries for " + title;
            }
            if (data.title_type == "episode") {
                episodes[title] = data;
                continue;
            }
            if (data.title_type == "season") {
                seasons[title] = data;
                continue;
            }
            if (data.title_type == "series") {
                series[title] = data;
                continue;
            }
            await this.addTitle(data, client); //features
        } catch(err)  {
            this.reportProgress("Could not process add update for title " + title);
            this.Error("Could not process add update for title " + title, err);
        }
    }
    this.ReportProgress("Processing episodes");
    for (let episode in episodes){
        let episodeData = episodes[episode];
        if (episodeData) {
            let seasonData = this.ObjectMap[episodeData.part_of];
            seasons[episodeData.part_of] = seasonData;
            await this.addTitle(episodeData, client);
        } else {
            this.reportProgress("No data found for episode " + episode);
        }
    }
    
    this.ReportProgress("Processing seasons");
    for (let season in seasons) {
        let seasonData = seasons[season];
        if (seasonData) {
            let seriesData = this.ObjectMap[seasonData.part_of];
            series[seasonData.part_of] = seriesData;
            await this.shakeSeason(seasonData, client);
            await this.addTitle(seasonData, client);
        } else {
            this.reportProgress("No data found for season " + season);
        }
    }
    this.ReportProgress("Processing series");
    for (let serie in series) {
        let seriesData = series[serie];
        if (seriesData) {
            await this.shakeSeries(seriesData, client);
            await this.addTitle(seriesData, client);
        } else {
            this.reportProgress("No data found for series " + serie);
        }
    }
    
};

async shakeSeries(seriesData, client) { //gentle shake: only updates the link that are known to be updated, no check on the other ones
    this.ReportProgress("Shaking series", seriesData.ip_title_id);
    try {
        let seasons = await this.getMetadata({
            client,
            objectId: seriesData.object_id,
            libraryId: seriesData.library_id,
            metadataSubtree: "public/asset_metadata/seasons",
            resolve: false
        }) || {};
        this.reportProgress("retrieved seasons data");
        let bySlug = {};
        if (seriesData.parts) {
            for (let seasonId of seriesData.parts) {
                let seasonData = this.ObjectMap[seasonId];
                if (seasonData) {
                    bySlug[seasonData.slug] = seasonData;
                } else {
                    this.ReportProgress("No season data found for " + seasonId);
                }
            }
        } else {
            this.reportProgress("No parts found for " + seriesData.ip_title_id);
        }
        let changed = 0;
        for (let index in seasons) {
            this.reportProgress("processing season",index, 3000);
            let seasonLink = seasons[index];
            let linkData = this.parseExternalLink(Object.values(seasonLink)[0]);
            let titleData = bySlug[Object.keys(seasonLink)[0]] ||  await this.getLatestData(linkData.linked_object_id, client);
            if (linkData.linked_hash != titleData.version_hash) {
                if (!titleData.version_confirmed) {
                    titleData.version_hash = await this.getVersionHash({objectId: titleData.object_id, libraryId: titleData.library_id, client});
                    if (titleData.version_hash) {
                        titleData.version_confirmed = true;
                    }
                }
                if ((linkData.linked_hash == titleData.version_hash) || !titleData.version_hash) {
                    continue;
                }
                Object.values(seasonLink)[0]["/"] = "/qfab/" + titleData.version_hash + "/meta/public/asset_metadata";
                changed++;
                this.reportProgress("Updating season link for " + titleData.ip_title_id + " in " + seriesData.ip_title_id);
            }
        }
        if (changed != 0) {
            let writeToken = await this.getWriteToken({
                client,
                objectId: seriesData.object_id,
                libraryId: seriesData.library_id
            });
            await client.ReplaceMetadata({
                client,
                objectId: seriesData.object_id,
                libraryId: seriesData.library_id,
                metadataSubtree: "public/asset_metadata/seasons",
                metadata: seasons,
                writeToken: writeToken
            });
            let response = await this.FinalizeContentObject({
                objectId: seriesData.object_id,
                libraryId: seriesData.library_id,
                writeToken: writeToken,
                commitMessage: "Updated " + changed + " season links",
                client
            });
            if (response) {
                this.reportProgress("Updated " + changed + " season links for " + seriesData.ip_title_id);
                seriesData.version_hash = response.hash;
                seriesData.version_confirmed = true;
            }
        }
    } catch(err) {
        this.Error("Could not shake series " + seriesData && seriesData.ip_title_id, err);
        throw err;
    }
};



async addTitle(data, client) {
    this.reportProgress("processing " + data.ip_title_id, null, 1000);
    let link = this.Searchables[data.ip_title_id];
    let foundHash;
    if (link) {
        let linkData = this.parseExternalLink(link);
        foundHash = linkData.linked_hash;
    }
    let currentHash;
    if (this.NoCachedVersion) { //we should test if value found in data.version_hash is reliable
        currentHash = await this.getVersionHash({objectId: data.object_id, libraryId: data.library_id, client});
        data.version_hash = currentHash;
        data.version_confirmed = true;
    } else {
        currentHash = data.version_hash;
    }
    if ((foundHash != currentHash) && currentHash) {
        if (!data.version_confirmed) {
            let latestHash = await this.getVersionHash({objectId: data.object_id, libraryId: data.library_id, client});
            if (data.version_hash) {
                data.version_hash = latestHash;
                data.version_confirmed = true;
            } else {
                this.reportProgress("Could not confirm current hash for "+data.object_id );//likely a permission error
            }
            
            if  (data.version_hash != currentHash) {
                this.Info("Discrepancy found between cached hash and current", {cached: currentHash, actual: data.version_hash});
                currentHash = data.version_hash;
            }
            if  (data.version_hash == foundHash) {
                return false;
            }
        }
        
        if (!foundHash) {
            link =  {".":{auto_update: {tag: "latest"}}};
            this.Searchables[data.ip_title_id] = link;
            this.AddedEntries.push(data);
        } else {
            this.ModifiedEntries.push(data);
        }
        if (currentHash) {
            link["/"] = "/qfab/" + currentHash + "/meta/searchables";
            this.ReportProgress("New version of "+ data.ip_title_id + " found", currentHash);
            return true;
        } else {
            this.ReportProgress("Could not update "+ data.ip_title_id);
        }
    }
    return false;
};

parseExternalLink(link) {
    let target = link["/"];
    let matcher = target.match(/\/qfab\/([^\/]+)\/([^\/]+)\/(.*)/);
    let versionHash = matcher[1];
    let decodedHash = this.Client.utils.DecodeVersionHash(versionHash);
    return {target: target, linked_object_id: decodedHash.objectId, linked_hash: versionHash, link_type: matcher[2], link_subtree: matcher[3]};
};

async indexLibraries(titlesLibs, client){
    this.ObjectMap = {};
    for (let i = 0; i < titlesLibs.length; i++) {
        try {
            this.ReportProgress("indexing " + titlesLibs[i]);
            let libMap = await this.buildIndex(titlesLibs[i], null, client);
            this.ObjectMap = {...this.ObjectMap, ...libMap};
        } catch(err) {
            this.Error("Could not index library", err);
        }
    }
    this.ReportProgress("index built");
};

async buildIndex(titlesLib, indexFilePath, client) {
    let token = await this.generateAuthToken(titlesLib, null, false, client);
    
    let url = (await this.getFabricUrl(client)) +"/qlibs/" + titlesLib + "/q?limit=50000&select=public/asset_metadata/ip_title_id&select=public/asset_metadata/info/parts&select=public/asset_metadata/info/part_of&select=public/asset_metadata/title_type&select=public/asset_metadata/slug";
    //console.error("curl -s '" + url + "' -H 'Authorization: Bearer " + token + "'");
    let stdout = execSync("curl -s '" + url + "' -H 'Authorization: Bearer " + token + "'" ,{maxBuffer: 100 * 1024 * 1024}).toString();
    let raw_list = JSON.parse(stdout);
    
    let objects = raw_list.contents;
    this.ReportProgress("indexing "+objects.length + " found objects");
    let indexMap = {};
    let totalToIndex = objects.length;
    for (let i=0; i < totalToIndex; i++) {
        this.reportProgress("indexing item " + i + " in " + totalToIndex, null, 5000);
        try {
            let ipTitleId = objects[i].versions[0].meta && objects[i].versions[0].meta.public && objects[i].versions[0].meta.public.asset_metadata && objects[i].versions[0].meta.public.asset_metadata.ip_title_id;
            if (!ipTitleId) {
                this.reportProgress("No ip-title-id found for " + objects[i].id);
                continue;
            }
            
            let info = objects[i].versions[0].meta.public.asset_metadata.info;
            let entry = {
                version_hash: objects[i].versions[0].hash,
                ip_title_id: ipTitleId,
                object_id: objects[i].id,
                library_id: titlesLib,
                slug: objects[i].versions[0].meta.public.asset_metadata.slug,
                title_type: objects[i].versions[0].meta.public.asset_metadata.title_type,
                parts: info && info.parts,
                part_of: info && info.part_of
            };
            if (this.Payload.parameters.identify_titles_by == "object_id") {
                indexMap[objects[i].id] = entry;
            }
            if (!indexMap[ipTitleId]) {
                indexMap[ipTitleId] = entry;
                
            } else {
                this.reportProgress("Duplicate for " + ipTitleId + ": " + indexMap[ipTitleId] + " vs. " + objects[i].id);
            }
        } catch(err) {
            this.reportProgress("Could not index ", objects[i].id);
            this.Error("Could not index " + objects[i].id, err);
        }
    }
    if (indexFilePath) {
        fs.writeFileSync(indexFilePath, JSON.stringify(indexMap, null, 2), 'utf8');
    }
    return indexMap;
};


async getLatestData(objectId, client) {
    return {
        version_confirmed: true,
        version_hash: await this.getVersionHash({objectId: objectId, client}),
        ip_title_id: await this.getMetadata({objectId: objectId, metadataSubtree: "public/asset_metadata/ip_title_id", client})
    }
};

async shakeSeason(seasonData, client) { //gentle shake: only updates the link that are known to be updated, no check on the other ones
    this.ReportProgress("Shaking season", seasonData.ip_title_id);
    try {
        let titles = await this.getMetadata({
            client,
            objectId: seasonData.object_id,
            libraryId: seasonData.library_id,
            metadataSubtree: "public/asset_metadata/titles",
            resolve: false
        }) || {};
        this.reportProgress("Retrieved " + Object.keys(titles).length + " title links");
        let bySlug = {};
        if (seasonData.parts) {
            for (let episodeId of seasonData.parts) {
                let episodeData = this.ObjectMap[episodeId];
                if (episodeData) {
                    bySlug[episodeData.slug] = episodeData;
                }
            }
            this.reportProgress("Indexed episode data by slug for " + seasonData.parts.length);
        } else {
            this.reportProgress("parts not found for " + seasonData.ip_title_id);
        }
        
        let changed = 0;
        for (let index in titles) {
            this.reportProgress("processing episode ",index, 3000);
            let titleLink = titles[index];
            let linkData = this.parseExternalLink(Object.values(titleLink)[0]);
            let titleData = bySlug[Object.keys(titleLink)[0]] || await this.getLatestData(linkData.linked_object_id, client);
            if (linkData.linked_hash != titleData.version_hash) {
                if (!titleData.version_confirmed) {
                    titleData.version_hash = await this.getVersionHash({objectId: titleData.object_id, libraryId: titleData.library_id, client});
                    titleData.version_confirmed = true;
                }
                if ((linkData.linked_hash == titleData.version_hash) || !titleData.version_hash) {
                    continue;
                }
                Object.values(titleLink)[0]["/"] = "/qfab/" + titleData.version_hash + "/meta/public/asset_metadata";
                changed++;
                this.ReportProgress("Updating episode link for " + titleData.ip_title_id + " in " + seasonData.ip_title_id);
            }
        }
        this.ReportProgress("Found " + changed + " modified episode links in " +  seasonData.ip_title_id);
        if (changed != 0) {
            let writeToken = await this.getWriteToken({
                client,
                objectId: seasonData.object_id,
                libraryId: seasonData.library_id
            });
            await client.ReplaceMetadata({
                client,
                objectId: seasonData.object_id,
                libraryId: seasonData.library_id,
                metadataSubtree: "public/asset_metadata/titles",
                metadata: titles,
                writeToken: writeToken
            });
            let response = await this.FinalizeContentObject({
                objectId: seasonData.object_id,
                libraryId: seasonData.library_id,
                writeToken: writeToken,
                commitMessage: "Updated " + changed + " episode links",
                client
            });
            if (response) {
                this.ReportProgress("Updated " + changed + " episode links for " + seasonData.ip_title_id);
                seasonData.version_hash = response.hash;
                seasonData.version_confirmed = true;
            }
        }
    } catch(err) {
        this.Error("Unable to shake season " + (seasonData && seasonData.ip_title_id), err);
        throw err;
    }
};



static VERSION = "0.1.1";
static REVISION_HISTORY = {
    "0.0.1": "Initial release",
    "0.0.2": "Exposes progress report",
    "0.0.3": "Fixes bug in version hash confirmation in shake-season",
    "0.0.4": "Fixes bug in handling of changed slugs in shake-season",
    "0.0.5": "Fixes bug in indexing by slugs in shake-series",
    "0.0.6": "Adds progress reporting",
    "0.0.7": "Implements 'shake' command",
    "0.0.8": "Fix shake for titles with unsufficient permissions",
    "0.0.9": "Fix glitch that overlooked provided key",
    "0.1.0": "Extends limit for number of object retrieved from 20K to 50k",
    "0.1.1": "Prevents cloberring of external changes made to the object"
};
};


if (ElvOAction.executeCommandLine(ElvOActionManageSite)) {
    ElvOAction.Run(ElvOActionManageSite);
} else {
    module.exports=ElvOActionManageSite;
}
