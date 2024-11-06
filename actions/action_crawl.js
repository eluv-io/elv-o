const ElvOAction = require("../o-action").ElvOAction;
const ElvOFabricClient = require("../o-fabric");
const ElvOMutexPool = require("../o-mutex-pool.js");
const Path = require('path');
const { fetchJSON } = require("../o-fabric");

class ElvOActionCrawl extends ElvOAction  {
    
    ActionId() {
        return "crawl";
    };
    
    IsContinuous() {
        return false; //indicates that the execution stays within a single PID
    };
    
    Parameters() {
        return {"parameters": {}};
    };
    
    IOs(parameters) {
        let inputs = {
            index_object_id: {type: "string", required:false},
            private_key: {type: "password", required:false},
            config_url: {type: "string", required:false},
            search_config_url: {type: "string", required:false},
            search_version: {type: "numeric", required:false, default: null},
            max_exceptions: {type: "numeric", required:false, default: null}
        };
        
        let outputs = {
            exceptions: {type: "array"},
            index_object_version_hash: {type: "string"},
            duration_ms: {type: "numeric"},
            duration: {type: "string"},
            crawled_site_version_hash: {type: "string"}
        };
        return { inputs : inputs, outputs: outputs };
    };
    
    
    PollingInterval() {
        return 60; //poll every minutes
    };
    
    async Execute(handle, outputs) {
        try {
            let searchConfigURL;
            if (!this.Payload.inputs.search_config_url) {
                let configUrl = this.Payload.inputs.config_url || this.Client.configUrl;
                searchConfigURL = await this.getSearchResource(configUrl, this.Payload.inputs.search_version, {debug:true});
            } else {
                searchConfigURL = this.Payload.inputs.search_config_url;
            }
            this.Debug("searchConfigURL", searchConfigURL);
            let privateKey = this.Payload.inputs.private_key || this.Client.signer.signingKey.privateKey.toString();
            let client = await ElvOFabricClient.InitializeClient(searchConfigURL, privateKey);
            let reporter = this;
            ElvOAction.TrackerPath = this.TrackerPath;
            client.ToggleLogging(true, {log: reporter.Debug, error: reporter.Error});
            
            let objectId = this.Payload.inputs.index_object_id
            let libraryId = await this.getLibraryId(objectId, client);
            
            let indexerRoot = await this.getMetadata({
                client,
                libraryId,
                objectId,
                metadataSubtree: "indexer/config/fabric/root"
            });
            this.Debug("indexer->root", indexerRoot);
            let siteObjectId = indexerRoot.content;
            let siteLibraryId = indexerRoot.library;
            let sitePreviousHash = await this.getMetadata({
                client,
                libraryId,
                objectId,
                metadataSubtree: "indexer/last_run"
            });
            this.reportProgress("Retrieved site information", {siteObjectId, siteLibraryId, sitePreviousHash})
            let siteCurrentHash = await this.getVersionHash({client, objectId: siteObjectId, libraryId: siteLibraryId});
            if (sitePreviousHash == siteCurrentHash) {
                this.ReportProgress("Site was not updated since last run. Skipping.");
                this.releaseSearchResource();
                return ElvOAction.EXECUTION_FAILED;
            }
            let writeToken = await this.getWriteToken({client, libraryId, objectId});
            let authorizationTokens = [
                await client.authClient.AuthorizationToken({libraryId, objectId, update: true})
            ];
            let result = await client.CallBitcodeMethod({
                objectId,
                libraryId,
                writeToken: writeToken,
                method: "search_update",
                constant: false
            });
            if (result) {
                this.markLROStarted(writeToken, result.lro_handle, siteCurrentHash, searchConfigURL, libraryId);
            }
            return ElvOAction.EXECUTION_ONGOING;
        } catch(eExec) {
            this.releaseSearchResource();
            throw eExec;
        }
    };
    
    
    async MonitorExecution(pid, outputs) {
        try {
            if (ElvOAction.PidRunning(pid)) {
                return ElvOAction.EXECUTION_ONGOING;
            }
            let lroInfo = this.getLROInfo();
            let objectId = this.Payload.inputs.index_object_id
            let privateKey = this.Payload.inputs.private_key || this.Client.signer.signingKey.privateKey.toString();
            let client = await ElvOFabricClient.InitializeClient(lroInfo.search_config_url, privateKey);
            //client.ToggleLogging(true);
            let libraryId = lroInfo.library_id || await this.getLibraryId(objectId, client);
            try {
                let results;
                try {
                    let authorizationToken = await this.generateAuthToken(libraryId, objectId, true, client);
                    
                    const headers = {
                        Authorization: `Bearer ${authorizationToken}`
                    };
                    
                    results = await client.CallBitcodeMethod({
                        libraryId,
                        objectId,
                        writeToken: lroInfo.write_token,
                        method: "crawl_status",
                        body: {"lro_handle": lroInfo.handle},
                        constant: false,
                        headers
                    });
                } catch(errBitcode) {
                    this.Debug("crawl_status bitcode error", errBitcode);
                    
                    results = await client.CallBitcodeMethod({
                        libraryId,
                        objectId,
                        writeToken: lroInfo.write_token,
                        method: "crawl_status",
                        body: {"lro_handle": lroInfo.handle},
                        constant: false
                    });
                } 
                
                this.Debug(" MonitorExecution - CallBitcodeMethod", results);
                
                if (results) {
                    if (results.custom && ((results.custom.run_state == "finished") || (results.custom.run_state == "failed"))) {
                        this.ReportProgress("Crawling is " + results.state + " after " + results.custom.duration);
                        if (results.custom.run_state == "finished") {
                            outputs.duration_ms = results.custom.duration_ms;
                            outputs.duration = results.custom.duration;
                            outputs.crawled_site_version_hash = lroInfo.site_hash;
                            outputs.exceptions = await this.getMetadata({
                                client, 
                                libraryId, 
                                writeToken: lroInfo.write_token,
                                //objectId,
                                metadataSubtree: "indexer/exceptions"
                            });
                            if ((this.Payload.inputs.max_exceptions != null) && outputs.exceptions && (outputs.exceptions.length > this.Payload.inputs.max_exceptions)){
                                this.ReportProgress("Abnormally high number of exceptions found ",outputs.exceptions.length);
                                this.releaseSearchResource();
                                return ElvOAction.EXECUTION_EXCEPTION;
                            }
                            let response = await this.finalizeIndex({objectId, libraryId, client, lroInfo});
                            if (response) {
                                outputs.index_object_version_hash = response.hash;
                                
                                this.releaseSearchResource();
                                return ElvOAction.EXECUTION_COMPLETE;
                            } else {
                                this.Debug("Failed to finalize index for " + this.JobId + "/" + this.StepId);
                                this.releaseSearchResource();
                                return ElvOAction.EXECUTION_EXCEPTION;
                            }
                        } else {
                            this.releaseSearchResource();
                            return ElvOAction.EXECUTION_EXCEPTION;
                        }
                    } else {
                        this.ReportProgress("Crawling is " + results.state + (results.custom && (" (progress: " + results.custom.progress.percentage + "%)")));
                    }
                } else {
                    //this.ReportProgress("No status returned"); //do not report status to avoid updating timestamp and facilitate idle timeout
                    this.Debug("No crawling status returned for " + this.JobId + "/" + this.StepId);
                    return ElvOAction.EXECUTION_ONGOING;
                }
            } catch (err) {
                this.Error("Could not retrieve status for "+ lroInfo.handle, err);
                this.releaseSearchResource();
                return ElvOAction.EXECUTION_EXCEPTION;
            }
            return ElvOAction.EXECUTION_ONGOING;
        } catch(eMon) {
            this.releaseSearchResource();
            throw eMon;
        }
    };
    
    
    async getSearchResource(configUrl, version) {
        let config = await ElvOFabricClient.fetchJSON(configUrl, {
            method: "GET",
            headers: {'Content-Type': 'application/json'},
            debug: true
        });
        let poolName = "SearchCrawlers_"+encodeURIComponent(configUrl);
        let searchResources;
        if (!version){
            searchResources = config.network.services.search || config.network.services.search_v2;
        } else {
            searchResources = config.network.services["search_v" + version];
        }
        this.reportProgress("Search pool "+poolName, searchResources)
        ElvOMutexPool.SetUp({name: poolName, resources: searchResources, reset: false});
        let crawler = await ElvOMutexPool.WaitForLock({name: poolName});
        if (crawler) {
            this.markSearchResource(crawler);
            return crawler.resource + "config?self=true&qspace=" + config.qspace.names[0];
        } else {
            throw new Error("Could not secure lock on a search node");
        }
    };
    
    async releaseSearchResource() {
        let resource = this.retrieveSearchResource();
        if (resource) {
            ElvOMutexPool.ReleaseSync(resource);
        }
    };
    
    
    async finalizeIndex({objectId, libraryId, client, lroInfo}) {
        this.reportProgress("Finalizing index...");
        await client.ReplaceMetadata({
            objectId,
            libraryId,
            writeToken: lroInfo.write_token,
            metadataSubtree: "indexer/last_run",
            metadata: lroInfo.site_hash
        });
        let result = await this.FinalizeContentObject({
            client,
            objectId,
            libraryId,
            writeToken: lroInfo.write_token,
            commitMessage:"Index Updated via O",
            publish: false
        });
        let versionHash = result.hash
        if (await this.smoketestIndex({objectId, libraryId, client, versionHash })){
            await this.PublishContentObject({objectId, libraryId, client, versionHash})
        } else {
            throw new Error("Index failed the smoke test");
        }
        return result;
    };
    
    async smoketestIndex({objectId, libraryId, client, versionHash}) {
        this.reportProgress("smoke testing", {objectId, libraryId, client, versionHash});
        let result
        try {
            this.reportProgress("Before rep/search", {
                libraryId,
                objectId,
                versionHash,
                rep: "search",
                queryParams: {terms: "id:ZOB"}
            });
            let repURL = await client.Rep({
                libraryId,
                objectId,
                versionHash,
                rep: "search",
                queryParams: {terms: "id:ZOB", select:"..."}
            });
            this.reportProgress("after rep search", repURL);
            result = await ElvOFabricClient.fetchJSON(repURL, {
                method: "GET",
                headers: {'Content-Type': 'application/json'},
                debug: true
            });
            this.reportProgress("after rep fetch", result);
        } catch (err) {
            this.reportProgress("Could not execute test query", err);
        }
        if (result && result.results) {
            this.ReportProgress("Index passed the smoke test");
            return true;
        } else {
            this.ReportProgress("Index failed the smoke test");
            return false;
        }
    }
    
    markSearchResource(resource) {
        this.trackProgress(ElvOActionCrawl.SEARCH_RESOURCE, "Crawl node", resource);
    };
    
    markLROStarted(lroWriteToken, lroHandle, siteCurrentHash, searchConfigURL, libraryId) {
        this.trackProgress(ElvOActionCrawl.TRACKER_LRO_STARTED, "Crawl Job started", {
            write_token: lroWriteToken,
            handle: lroHandle,
            site_hash: siteCurrentHash,
            search_config_url: searchConfigURL,
            library_id: libraryId
        });
    };
    
    
    getLROInfo() {
        let info = this.Tracker && this.Tracker[ElvOActionCrawl.TRACKER_LRO_STARTED];
        return info = info && info.details;
    };
    
    retrieveSearchResource() {
        let info = this.Tracker && this.Tracker[ElvOActionCrawl.SEARCH_RESOURCE];
        return info = info && info.details;
    };
    
    static SEARCH_RESOURCE = 64;
    static TRACKER_LRO_STARTED = 65;
    static INFO = 66;
    
    static VERSION = "0.0.8";
    static REVISION_HISTORY = {
        "0.0.1": "Initial release",
        "0.0.2": "Adds the ability to provide the search config url explicitly",
        "0.0.3": "adds a smoke test before committing",
        "0.0.4": "Uses self-signed token for status update",
        "0.0.5": "Uses POST for status update",
        "0.0.6": "Adds support for search v2",
        "0.0.7": "Adds option to fail if number of exception is deemed abnormally high",
        "0.0.8": "Adds explicit switch for version"
    };
}


if (ElvOAction.executeCommandLine(ElvOActionCrawl)) {
    ElvOAction.Run(ElvOActionCrawl);
} else {
    module.exports=ElvOActionCrawl;
}
