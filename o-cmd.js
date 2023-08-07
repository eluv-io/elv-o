const path = require('path');
const logger = require('./o-logger');
const ElvOQueue = require("./o-queue");
const ElvOProcess = require("./o-process");
const ElvO = require("./o-core");
const ElvOSvc = require("./o-svc");
const ElvOFabricClient = require("./o-fabric");
const ElvOJob = require("./o-job.js");
let { execSync } = require('child_process');
const glob = require("glob");
const fs = require("fs");
const { type } = require('os');

class ElvOCmd {
    
    static HEARTBBEAT = 1000;
    static PROD_CONFIG_URL = "https://main.net955305.contentfabric.io/config";
    
    static report(...msg) {
        if (this.Verbose) {
            console.error(...msg);
        }
    };
    
    
    
    
    static async RunJobCmd(o) {
        let jobId = ElvOProcess.getValueInArg("job-id","JOB_ID");
        let endMet = await ElvO.RunJob(o, jobId, {});
        return endMet
    };
    
    
    
    
    
    
    //node o.js instantiate-O --private-key=<private key used to create O instance - should be an elv-admin key, not a Tenant's> --name=<Name for the O object> --o-key=<address associated with the private key o will run as> [--o-id=<fabric object ID for object if one was created in advance> | --o-library-id=<library ID in which to create the o object if not provided>] [--max-running=<global throttle for o instance] [--admin-group=<group given access to the o object>] [--content-type=<id for content-type object of the O object>]
    static async CreateOCmd(o){
        let adminGroup = ElvOProcess.getValueInArgv("admin-group");
        let name = ElvOProcess.getValueInArgv("name");
        let maxRunning = ElvOProcess.getValueInArgv("max-running");
        let contentType = ElvOProcess.getValueInArgv("content-type");
        let service_url = ElvOProcess.getValueInArgv("service-url");
        o.LibraryId = ElvOProcess.getValueInArgv("o-library-id");
        let client = o.Client;
        logger.Info("CreateO", {name, oLibraryId: o.LibraryId, adminGroup, maxRunning, contentType});
        if (!o.LibraryId) {
            logger.Error("o-library-id must be specified");
            return 120;
        }
        let objStruct = await client.CreateContentObject({
            libraryId: o.LibraryId,
            options: {
                contentType: contentType,
                meta: {
                    public: {name: name},
                    service_url
                }
            }
        });
        o.ObjectId = objStruct && objStruct.id;
        if (!o.ObjectId) {
            logger.Error("Could not create O Instance");
            return 130;
        }
        await client.FinalizeContentObject({
            libraryId: o.LibraryId,
            objectId: o.ObjectId,
            writeToken: objStruct.write_token,
            commitMessage: "Creation"
        });
        if (adminGroup) {
            await this.CallContractMethodAndWait({
                contractAddress: adminGroup,
                methodName: "setContentObjectRights",
                methodArgs: [client.utils.HashToAddress(o.ObjectId), 1, 1]
            });
        }
        if (maxRunning) {
            o.SetWorkflowThrottle(null, parseInt(maxRunning));
        }
        console.log("Created O instance with ID " + o.ObjectId);
        return 0;
    };
    
    
    static async ListQueuesCmd() {
        let active = (!ElvOProcess.isPresentInArgv("active") && !ElvOProcess.isPresentInArgv("inactive")) ? null : ElvOProcess.isPresentInArgv("active");
        console.log(await ElvOQueue.List(active));
        return 0;
    };
    
    static async CreateQueueCmd() {
        let queues = await ElvOQueue.Create(ElvOProcess.getValueInArgv("queue-id"), ElvOProcess.getValueInArgv("priority"), ElvOProcess.isPresentInArgv("active"), ElvOProcess.getValueInArgv("name"));
        return (!queues) ? 1 : 0;
    };
    
    static async QueueItemCmd(){
        let item = JSON.parse(ElvOProcess.getValueInArgv("item"));
        let priority = ElvOProcess.getValueInArgv("priority") || "100";
        let itemId = ElvOProcess.getValueInArgv("item-id");
        let queueId= ElvOProcess.getValueInArgv("queue-id");
        if (itemId)  {
            item.id = itemId;
        }
        let queue  = new ElvOQueue(queueId);
        let itemPath = await  queue.Queue(queueId, item, priority);
        return (itemPath) ? 0 : 1;
    };
    
    static async QueuedCmd(){
        let queueId = ElvOProcess.getValueInArgv("queue-id");
        let limit = parseInt(ElvOProcess.getValueInArgv("limit" || "0"));
        let items =   ElvOQueue.Queued(queueId, limit);
        console.log(JSON.stringify(items, null, 2));
        return 0;
    };
    
    static async NextItemCmd(){
        let execCode;
        let popIt = ElvOProcess.isPresentInArgv("pop");
        let queues = ElvOProcess.getValuesInArgv("queue-id");
        let nextInQueue =  ElvOQueue.Next(queues, popIt);
        if (!nextInQueue) {
            this.report("No items queued")
            execCode = 1;
        } else {
            console.log(JSON.stringify(nextInQueue, null, 2));
            execCode = 0;
        }
        return execCode
    };
    
    static async ChangeQueueStatusCmd(activate) {
        let queueId = ElvOProcess.getValueInArgv("queue-id");
        if (activate) {
            if (ElvOQueue.Activate(queueId, true)) {
                console.log("Activated queue "+ queueId);
                return true;
            } else {
                console.log("Failed to Activate queue "+ queueId);
                return null;
            }
        } else {
            if (ElvOQueue.Activate(queueId, false)) {
                console.log("De-activated queue "+ queueId);
                return true;
            } else {
                console.log("Failed to de-activate queue "+ queueId);
                return null;
            }
        }
    };
    
    static async ResetStepCmd(o) {
        let jobId = ElvOProcess.getValueInArgv("job-id");
        let stepId = ElvOProcess.getValueInArgv("step-id");
        let result = await o.ResetStep(jobId, stepId);
        console.log("ResetStep", result);
        return 0;
    };
    
    static async RestartFromCmd(o) {
        let jobId =  ElvOProcess.getValueInArgv("job-id")
        let jobRef = ElvOProcess.getValueInArgv("job-reference");
        let jobRefHex = ElvOProcess.getValueInArgv("job-reference-hex");
        let stepId = ElvOProcess.getValueInArgv("step-id");
        let result = ElvOJob.RestartFrom({jobId, jobRef, jobRefHex, stepId});       
        return (result ? 0 : 1);
    };
    
    static async CancelJobCmd(o) {
        let jobId =  ElvOProcess.getValueInArgv("job-id")      
        let result = ElvOJob.CancelJob(jobId);
        return (result ? 0 : 1);
    };
    
    static async JobInfoCmd(o) {
        let jobRef =  ElvOProcess.getValueInArgv("job-reference");
        let result = o.JobInfo(jobRef);
        console.log(JSON.stringify(result, null, 2));
        return 0;
    };
    
    static async JobDetailsCmd(o) {
        let jobRef = ElvOProcess.getValueInArgv("job-reference");
        let stepId = ElvOProcess.getValueInArgv("step-id");
        if (!stepId) {
            let result = ElvOJob.ListStepFiles(jobRef);
            console.log(JSON.stringify(result, null, 2));
        } else {
            let stepsPath = path.join(ElvOJob.jobFolderPath(jobRef), "steps");
            let file;
            let found = false
            if  (ElvOProcess.isPresentInArg("payload")) {
                found = true;
                let stepRoot = path.join(stepsPath, stepId + "_payload.json");
                let candidates = glob.sync(stepRoot);
                if (candidates.length) {
                    let content =  fs.readFileSync(candidates[0], 'utf8');
                    console.log(JSON.stringify(JSON.parse(content, null, 2)));
                    console.error("\n"+candidates[0]+"\n");
                    return 0;
                } else {
                    console.log("No match for ", {job_reference: jobRef, step_id: stepId});
                    return  1;
                }
            } 
            if  (ElvOProcess.isPresentInArg("log")) {
                found = true;
                let stepRoot = path.join(stepsPath, stepId + "*.log");
                let candidates = glob.sync(stepRoot);
                if (candidates.length) {
                    let latest = candidates[candidates.length -1];
                    let content =  fs.readFileSync(latest, 'utf8');
                    console.log(content);
                    console.error("\n"+latest+"\n");
                    return 0;
                } else {
                    console.log("No match for ", {job_reference: jobRef, step_id: stepId});
                    return  1;
                }
            }
            if  (ElvOProcess.isPresentInArg("result") || !found) {
                let stepRoot = path.join(stepsPath, stepId + ".json");
                let candidates = glob.sync(stepRoot);
                if (candidates.length) {
                    let content =  fs.readFileSync(candidates[0], 'utf8');
                    console.log(content);
                    console.error("\n"+candidates[0]+"\n");
                    return 0;
                } else {
                    console.log("No match for ", {job_reference: jobRef, step_id: stepId});
                    return  1;
                }
            }
            
        }
        
        return 2;
    };
    
    
    static async EncryptValueCmd(o) {
        let value = ElvOProcess.getValueInArgv("value");
        if (!value) {
            return 1;
        }
        let encryptedInput = await o.Client.EncryptECIES({message: value});
        console.log({encrypted_input: "p__:"+encryptedInput});
        return 0;
    };
    
    static async DecryptValueCmd(o) {
        console.log("DecryptValueCmd")
        let value = ElvOProcess.getValueInArgv("value");
        if (!value) {
            return 1;
        }
        let matcher = value.match(/^p__:(.*)/);
        if (!matcher) {
            logger.Error("Invalid encrypted string", value);
            return 2;
        }
        console.log("DecryptValueCmd", matcher[1]);
        let decryptedInput = await o.Client.DecryptECIES({message: matcher[1]});
        console.log({decrypted_input: decryptedInput});
        return 0;
    }
    
    static async SetWorkflowThrottleCmd(o){
        let oId = ElvOProcess.getValueInArg("o-id","O_ID");
        let oLibraryId =  await o.getLibraryId(oId);
        let limit = ElvOProcess.getValueInArgv("limit");
        let workflowId = ElvOProcess.getValueInArgv("workflow-id");
        let workflowObjId = ElvOProcess.getValueInArgv("workflow-object-id");
        if (!workflowObjId) {
            let definitions = await o.RetrieveThrottles();
            let data = definitions[workflowId];
            if ((typeof data) == "object") {
                workflowObjId = data.workflow_object_id;
            } else {
                workflowObjId = workflowId;
            }
        }
        if (limit) {
            await o.SetWorkflowThrottle(workflowObjId, parseInt(limit), workflowId);
        }
        let existing = await o.GetWorkflowThrottle(workflowId || workflowObjId);
        console.log(existing);
        return 0;
    };
    
    static async WorkflowIOCmd(o){
        let workflowId = ElvOProcess.getValueInArg("workflow-id");
        logger.Debug("workflow_id", workflowId);
        let workflowDefinition = await o.GetWorkflowDefinition(workflowId, true);
        //logger.Debug("workflowDefinition", workflowDefinition);
        let inputs = workflowDefinition.parameters;
        let outputs = {};
        for (let step in workflowDefinition.steps) {
            let stepDef = workflowDefinition.steps[step];
            let parameters = stepDef.parameters;
            //let spec = ElvOAction.GetSpec({actionId: stepDef.action.action, force: body.force, parameters});
            //maybe we can just return the outputs of each steps as an object output
            outputs[step] = {type: "object"};
        }
        console.log(JSON.stringify({inputs, outputs}));
        return 0;
    };
    
    
    static async WorkflowRefreshCmd(o){
        let workflowId = ElvOProcess.getValueInArg("workflow-id");
        let workflowObjectId = ElvOProcess.getValueInArg("workflow-object-id");
        if (!workflowObjectId) {
            let definitions = await o.RetrieveThrottles();
            let data = definitions[workflowId];
            if ((typeof data) == "object") {
                workflowObjectId = data.workflow_object_id;
            } else {
                workflowObjectId = workflowId;
            }
        }
        logger.Debug("Workflow Id", {workflowId, workflowObjectId});
        await o.RetrieveWorkflowDefinition(workflowObjectId, true, workflowId);
        return 0;
    };
    
    static async MakeApiKeyCmd(o) {
        let clientAddress = ElvOProcess.getValueInArgv("client-address");
        let now = (new Date()).getTime();
        if (!clientAddress) {
            return 1;
        }
        let message = JSON.stringify({client: clientAddress, timestamp: now});
        let apiKey = await o.Client.EncryptECIES({message: message});
        console.log(JSON.stringify({api_key: apiKey}));
        return 0;
    };
    
    static async CheckApiKeyCmd(o) {
        let clientAddress = ElvOProcess.getValueInArgv("client-address");
        let apiKey = ElvOProcess.getValueInArgv("api-key");
        try {
            let message = await o.Client.DecryptECIES({message: apiKey});
            let data = JSON.parse(message);
            console.log({data});
            if (data.client != clientAddress) {
                console.log("Mismatched key and client address");
                return 2;
            }
            let now = (new Date()).getTime();
            if (now < data.timestamp) { //We could force an expiration instead by testing how old the key is
                console.log("Invalid key timestamp");
                return 3;
            }
            return 0;
        } catch(err) {
            console.log("Invalid key");
            return 1;
        }
    };
    
    static async AuthorizeClientAddressCmd(o) {
        try {
            let oId = ElvOProcess.getValueInArg("o-id", "O_ID");
            let oLibraryId = await o.getLibraryId(oId);
            let clientAddress = ElvOProcess.getValueInArg("client-address");
            if (!clientAddress) {
                console.log("A client address must be provided");
                return 1;
            }
            let authProfile = JSON.parse(ElvOProcess.getValueInArg("authorized-url") || "[\"/.*\"]");
            let writeToken = await o.getWriteToken({objectId: oId, libraryId: oLibraryId});
            await o.Client.ReplaceMetadata({
                objectId: oId,
                libraryId: oLibraryId,
                writeToken,
                metadataSubtree: "authorized_address/" + clientAddress,
                metadata: authProfile
            });
            let response = await o.safeExec("this.Client.FinalizeContentObject", [{
                objectId: oId,
                libraryId: oLibraryId,
                writeToken,
                commitMessage: "Added client address authorization"
            }]);
            if (response.hash) {
                console.log("Added client address authorization for " + clientAddress + " to  " + authProfile);
                return 0;
            }
        } catch(err) {
            logger.Error("Error adding client address authorization", err);
        }
        console.log("Failed to add client address authorization for " + clientAddress + " to  " + authProfile);
        return 1;
    };
    
    static async GetClientAddressAuthorizationProfileCmd(o) {
        let oId = ElvOProcess.getValueInArg("o-id","O_ID");
        let oLibraryId =  await o.getLibraryId(oId);
        let clientAddress = ElvOProcess.getValueInArg("client-address");
        let whitelist = await o.getMetadata({
            libraryId: oLibraryId,
            objectId: oId,
            metadataSubtree: "authorized_address"
        });
        let authProfile = {}
        authProfile[clientAddress] =  whitelist && whitelist[clientAddress];
        console.log(JSON.stringify(authProfile, null, 2));
        return 0;
    };
    
    static async RunServiceCmd(o) {
        let heartbeat = parseInt(ElvOProcess.getValueInArg("heartbeat-ms", "HEARTBEAT_MS", ElvO.HEARTBBEAT));
        let pidFilePath = path.join(ElvOProcess.getValueInArg("pid-file-dir", "PID_FILE_DIR","."), "o.pid");
        let apiPort = ElvOProcess.getValueInArg("api-port", "API_PORT");
        let oId = ElvOProcess.getValueInArg("o-id","O_ID");
        await ElvOSvc.RunService(o, {heartbeat, apiPort, oId, verbose: this.Verbose, pidFilePath});
    };
    
    static async StopServiceCmd(o) {
        let pidFilePath = path.join(ElvOProcess.getValueInArg("pid-file-dir", "PID_FILE_DIR","."), "o.pid");
        let oId = ElvOProcess.getValueInArg("o-id","O_ID");
        await ElvOSvc.StopService(o, {oId, verbose: this.Verbose, pidFilePath});
    };
    
    static async RunAPIServiceCmd(o) {
        let heartbeat = parseInt(ElvOProcess.getValueInArg("heartbeat-ms", "HEARTBEAT_MS", ElvO.HEARTBBEAT));
        let pidFilePath = path.join(ElvOProcess.getValueInArg("pid-file-dir", "PID_FILE_DIR","."), "o-api.pid");
        let apiPort = ElvOProcess.getValueInArg("api-port", "API_PORT");
        let oId = ElvOProcess.getValueInArg("o-id","O_ID");
        await ElvOSvc.RunAPIService(o, {heartbeat, apiPort, oId, verbose: this.Verbose, pidFilePath});
    };
    
    static async StopAPIServiceCmd(o) {
        let pidFilePath = path.join(ElvOProcess.getValueInArg("pid-file-dir", "PID_FILE_DIR","."), "o-api.pid");
        let oId = ElvOProcess.getValueInArg("o-id","O_ID");
        await ElvOSvc.StopAPIService(o, {oId, verbose: this.Verbose, pidFilePath});
    };
    
    static getMethods(obj){
        let properties = new Set();
        let currentObj = obj;
        do {
            Object.getOwnPropertyNames(currentObj).map(item => properties.add(item))
        } while ((currentObj = Object.getPrototypeOf(currentObj)))
        return [...properties.keys()].filter(item => typeof obj[item] === 'function')
    };
    
    static FILE_JOB_ID = null;
    static getFileJobId(message, something, somethinElse, yetOneMoreThing) {
        //console.log("typeof", (typeof message), (typeof something));
        //console.log("message", message);
        console.log("LOG", message, something, somethinElse, yetOneMoreThing);
        if ((typeof something) == "string") {
            if (something.match(/S3 file upload failed/)) {
                ElvOCmd.FILE_JOB_ID = something.match(/"id": "([^"]+)"/)[1];
            }
        }
    };
    
    static async clearS3Failure(o, params){ //libraryId, objectId, writeToken, filejobId, signedUrl, client
        let client = params.client || o.Client;
        let libId = params.libraryId;
        let writeToken = params.writeToken;
        if (writeToken && !params.node_url) {
            if (client.HttpClient.draftURIs[writeToken]) {
                params.node_url = "https://" + client.HttpClient.draftURIs[writeToken].hostname() + "/";
            }
        }
        let nodeUrl = params.node_url || (await this.getFabricUrl(client) + "/");
        let url = nodeUrl +"qlibs/" + libId + "/q/" + writeToken  + "/file_jobs/" + params.filejobId + "/resume";
        let headers = await client.authClient.AuthorizationHeader({libraryId:libId , objectId: params.objectId, update: true});
        //console.log("headers", headers.Authorization);
        //let token = headers.Authorization.replace('Bearer ',"");
        //let token = await o.generateAuthToken(libId, params.objectId, false, client); //await o.getLibraryToken(libId, client);
        let cmd = "curl -s -X PUT '" + url + "'  -d '{\"clear_resolve\":true,\"defaults\":{\"access\":{\"platform\":\"aws\",\"protocol\":\"s3\",\"cloud_credentials\":{\"signed_url\":\""+ params.signedUrl+"\"}}}}' -H 'Authorization: " + headers.Authorization + "' -H 'Content-Type: application/json'";
        console.log("cmd clearS3Failure", cmd);
        let stdout = execSync(cmd, {maxBuffer: 100 * 1024 * 1024}).toString();
        console.log("stdout", stdout);
    };
    
    static HelpCmd() {
        console.log("\nUsage:\n");
        console.log("node o.js instantiate-O --private-key=<private key used to create O instance - should be an elv-admin key, not a Tenant's> --name=<Name for the O object> --service-url=<external url for service> --service-url=<the external URL the API service can be reached at> --o-library-id=<library ID in which to create the o object if not provided> [--max-running=<global throttle for o instance] [--admin-group=<group given access to the o object>] [--content-type=<id for content-type object of the O object>]\n");
        console.log("node o.js service --o-id=<fabric object ID for O object> --private-key=<private key O is running as> [--pid-file-dir=<dir in which pid file is created>] [--api-port=<port for API listener, defaulted to 8080 - 0 indicates no listening>]\n");
        console.log("node o.js api-service --o-id=<fabric object ID for O object> --private-key=<private key O is running as> [--pid-file-dir=<dir in which pid file is created>] [--api-port=<port for API listener, defaulted to 8080>]\n");
        console.log("node o.js throttle --o-id=<fabric object ID for O object> --private-key=<private key O is running as> [--limit=<max number of concurrent job>] [--workflow-id=<workflow object Id>]\n");
        console.log("node o.js encrypt --o-id=<fabric object ID for O object> --private-key=<private key O is running as> --value=<string to encrypt>\n");
        console.log("node o.js refresh --private-key=<private key used to create O instance - should be an elv-admin key, not a Tenant's> --workflow-id=<id for workflow object> \n");
        console.log("node o.js list-queues\n");
        console.log("node o.js create-queue --queue-id=<string identifyer for the queue> [--active] --priority=<number indicating priority, 0 is absolute highest> [--name=<name of queue>]\n");
        console.log("node o.js activate --queue-id=<string identifyer for the queue>\n");
        console.log("node o.js deactivate --queue-id=<string identifyer for the queue>\n");
        console.log("node o.js make-api-key --o-id=<fabric object ID for O object> --private-key=<private key O is running as> --client-address=<address of client using the api-key>\n");
        console.log("node o.js authorize --o-id=<fabric object ID for O object> --private-key=<private key O is running as> --client-address=<address of client using the api-key> [--authorized-url=<JSON array of regex, defaulted to [\"/.*\"]>]\n")
        console.log("node o.js job-info --job-reference=<reference for the job>\n");
        console.log("node o.js rotate-log\n");
        console.log("node o.js purge-log [--days-kept=<number of days for which the logs are kept - default is 7>]\n");
        console.log("node o.js job-details --job-reference=<reference> [--step-if=<step id>] [--log] [--payload] [--result]\n");
        return 0;
    }
    
    static async Run(command) {
        let execCode;
        try {
            logger.Set("O-cmd");
            logger.Info("command", command);
            this.Verbose = ElvOProcess.isPresentInArg("verbose", "VERBOSE");
            this.Force = ElvOProcess.isPresentInArg("force", "FORCE");
            if (command == "rotate-log") {
                execCode = (await logger.Rotate()) ? 0 : 1;
            }
            if (command == "purge-log") {
                let daysKept =  parseInt(ElvOProcess.getValueInArg("days-kept") || 7);
                execCode = (await logger.Purge(daysKept)) ? 0 : 1;
            }
            if (command == "list-queues") {
                execCode = await this.ListQueuesCmd();
            }
            if (command == "create-queue") {
                execCode = await this.CreateQueueCmd();
            }
            if (command == "queue-item") {
                execCode = await this.QueueItemCmd();
                (itemPath) ? 0 : 1;
            }
            if (command == "queued") {
                execCode = await this.QueuedCmd();
            }
            if (command == "next-item") {
                execCode = await this.NextItemCmd();
            }
            if (command == "activate") {
                execCode = await this.ChangeQueueStatusCmd(true);
            }
            if (command == "deactivate") {
                execCode = await this.ChangeQueueStatusCmd(false);
            }
            if (!command || command == "help") {
                execCode =  this.HelpCmd(false);
            }
            if (execCode == null) {
                try {
                    let configUrl = ElvOProcess.getValueInArg("config-url", "CONFIG_URL", this.PROD_CONFIG_URL);
                    const privateKey = ElvOProcess.getValueInArg("private-key", "PRIVATE_KEY");
                    let oId =  ElvOProcess.getValueInArg("o-id", "O_ID");
                    let o = await ElvO.Initialize(oId, configUrl, privateKey);
                    
                    if (command == "instantiate-O") {
                        execCode = await this.CreateOCmd(o);
                    }
                    if ((command == "service") || (command == "start")) {
                        this.RunServiceCmd(o);
                        execCode = -1;
                    }
                    if (command == "run-job") {
                        let jobStatus = await this.RunJobCmd(o);
                        execCode = (jobStatus > 0) ? 0 : -1;
                    }
                    if (command == "stop") {
                        let stopped = await this.StopServiceCmd(o);
                        return (stopped) ? 0 : 1;
                    }
                    if ((command == "api-service") || (command == "start-api")) {
                        execCode = await this.RunAPIServiceCmd(o);
                    }
                    if (command == "stop-api") {
                        let stopped = await this.StopAPIServiceCmd(o);
                        return (stopped) ? 0 : 1;
                    }
                    if (command == "reset-step") {
                        execCode = await this.ResetStepCmd(o);
                    }
                    if (command == "restart-from") {
                        execCode = await this.RestartFromCmd(o);
                    }
                    if (command == "cancel-job") {
                        execCode = await this.CancelJobCmd(o);
                    }
                    if (command == "job-info") {
                        execCode = await this.JobInfoCmd(o);
                    }
                    if (command == "job-details") {
                        execCode = await this.JobDetailsCmd(o);
                    }                   
                    if (command == "make-api-key") {
                        execCode = await this.MakeApiKeyCmd(o);
                    }
                    if (command == "check-api-key") {
                        execCode = await this.CheckApiKeyCmd(o);
                    }
                    if (command == "authorize") {
                        execCode = await this.AuthorizeClientAddressCmd(o);
                    }
                    if (command == "get-client-address-authorization-profile") {
                        execCode = await this.GetClientAddressAuthorizationProfileCmd(o);
                    }
                    if (command == "encrypt") {
                        execCode = await this.EncryptValueCmd(o);
                    }
                    if (command == "decrypt") {
                        execCode = await this.DecryptValueCmd(o);
                    }
                    if (command == "throttle") {
                        execCode = await this.SetWorkflowThrottleCmd(o);
                    }
                    if (command == "workflow-io") {
                        execCode = await this.WorkflowIOCmd(o);
                    }
                    if (command == "refresh") {
                        execCode = await this.WorkflowRefreshCmd(o);
                    }
                    if(command == "test") {
                        console.log("jobs running", ElvOJob.GetExecutingJobsDataSync());
                        process.exit(0);
                    }
                } catch (e_o) {
                    console.log("Error executing o command", e_o);
                }
            }
            if (execCode == null) {
                console.log("command not found", command);
                execCode = 1;
            }
        } catch (err) {
            logger.Error("Top level command execution", err);
            execCode = 100;
        }
        if (execCode != -1) {
            process.exit(execCode);
        }
    };
    
    
};


module.exports=ElvOCmd;






//node o.js service --o-id=<fabric object ID for O object> --private-key=<private key O is running as> --job-library-id=<library in which job objects are to be created> [--job-content-type=<content type for job objects>] [-pid-file-dir=<dir in which pid file is created>] [--api-port=<port for API listener, defaulted to 8080>]


