const fs = require("fs");
const path = require('path');
const logger = require('./o-logger');
const ElvOFabricClient = require("./o-fabric");
const ElvOQueue = require("./o-queue");
const ElvOAction = require("./o-action").ElvOAction;
const ElvOJob = require("./o-job.js");
const {spawn, spawnSync, execSync} = require('child_process');
const ElvOProcess = require("./o-process");

class ElvO extends ElvOFabricClient {
    
    
    constructor(oId, client, configUrl, privateKey) {
        super();
        this.Client = client;
        this.ObjectId = oId;
        this.ConfigUrl = configUrl;
        this.PrivateKey = privateKey;
    };
    
    static async Initialize(oId, configUrl, privateKey) {
        let client = (privateKey) ? await ElvOFabricClient.InitializeClient(configUrl, privateKey) : null;
        let o = new ElvO(oId, client, configUrl, privateKey);
        if (oId) {
            o.LibraryId = await o.getLibraryId(oId);
        }
        return o;
    };
    
    testCondition(jobId, condition, stepsMap) {
        let nonComposite =  this.testSimpleCondition(jobId, condition, stepsMap);
        if (nonComposite && condition.hasOwnProperty("#")) {
            return this.testCompositeCondition(jobId, condition, stepsMap);
        } else {
            return nonComposite;
        }
    };
    
    testSimpleCondition(jobId, condition, stepsMap) {
        try {
            let stepsIds = Object.keys(condition);
            let missing = [];
            for (let i=0; i < stepsIds.length; i++) {
                let stepId = stepsIds[i];
                if ((stepId == ".") || (stepId == "#")) {
                    continue;
                }
                let expectedStatus = condition[stepId];
                let stepInfo = stepsMap[stepId];
                if (!stepInfo) {
                    logger.Error("Invalid step "+ stepId + " in condition", condition);
                    throw new Error("Invalid step "+ stepId);
                }
                if (ElvO.STEP_STATUSES[expectedStatus] != stepInfo.status_code) {
                    missing.push(stepId+":"+expectedStatus);
                }
            }
            if  (missing.length == 0){
                //report("Preconditions met", JSON.stringify(condition, null,2));
                return true;
            } else {
                //report("Preconditions not met", JSON.stringify(missing, null,2));
                return false;
            }
        } catch(err) {
            logger.Error("Could not evaluate condition", condition);
            throw err;
        }
    };
    
    testCompositeCondition(jobId, condition, stepsMap) {       
        let conditions = condition["."];
        let number = condition["#"] || conditions.length; //0 is treated as all (effectively an alternate form for AND)
        let met = 0;
        for (let i=0; (i < conditions.length) && (met < number); i++) {
            if (this.testCondition(jobId, conditions[i], stepsMap)) {
                met++;
            }
        }
        return (met >= number);
    };
    
    
    expandWorkflowStates(workflowDefinition) {
        let workflowStates = workflowDefinition.states || {};
        let stepsDefinition = workflowDefinition.steps;
        let steps = Object.keys(stepsDefinition);
        let ends = workflowStates || {};
        if (Object.keys(ends).length == 0) {
            ends.complete = {job_status_code: 100, prerequisites:{}};
            //Create default completion status: all steps are complete
            for (let i=0; i < steps.length; i++) {
                ends.complete.prerequisites[steps[i]] = "complete"
            }
        }
        
        let allStepsDefinition = {...stepsDefinition, ...(workflowStates || {})};
        let allSteps = Object.keys(allStepsDefinition);
        
        //find all hanging exceptions and failed state in steps
        let attached = {complete:{}, failed:{}, exception:{}};
        for (let i=0; i < allSteps.length; i++) {
            let stepId = allSteps[i];
            let prerequisites = allStepsDefinition[stepId].prerequisites || {};
            this.findAttachedSteps(attached, stepId, prerequisites);
        }
        let unattached = {complete:[], failed:[], exception:[]};
        for (let i=0; i < steps.length; i++) {
            let stepId = steps[i];
            if (stepsDefinition[stepId].fanned_out) {
                continue; //fanned out step are not part of the normal process, their conclusion is monitored by the fan-out
            }
            if (!attached.complete[stepId]) {
                unattached.complete.push(stepId);
            }
            if (!attached.failed[stepId]) {
                unattached.failed.push(stepId);
            }
            if (!attached.exception[stepId]) {
                unattached.exception.push(stepId);
            }
        }
        if (!ends.failed) {
            ends.failed = {job_status_code: 99, prerequisites:{}};
        }
        
        let unattachedFailed = [];
        for (let i=0; i < unattached.failed.length; i++) {
            let entry = {};
            entry[unattached.failed[i]] = "failed";
            unattachedFailed.push(entry);
        }
        if (Object.keys(ends.failed.prerequisites).length == 0) {
            ends.failed.prerequisites = {"#": 1, ".":unattachedFailed};
        } else {
            ends.failed.prerequisites = {"#": 1, ".": unattachedFailed.concat(ends.failed.prerequisites)};
        }
        
        if (!ends.exception) {
            ends.exception = {job_status_code: -1, prerequisites:{}};
        }
        let unattachedException = [];
        if (Object.keys(ends.exception.prerequisites).length == 0) {
            ends.exception.prerequisites = {"#": 1, ".":unattachedException};
        } else {
            ends.exception.prerequisites = {"#": 1, ".": unattachedException.concat(ends.exception.prerequisites)};
        }
        for (let i=0; i < unattached.exception.length; i++) {
            let entry = {};
            entry[unattached.exception[i]] = "exception";
            unattachedException.push(entry);
        }
        for (let wfEnd in ends) {
            if (!ends[wfEnd].job_status_code) {
                for (let statusCode in ElvO.JOB_STATUSES) {
                    if (ElvO.JOB_STATUSES[statusCode] == wfEnd) {
                        ends[wfEnd].job_status_code = parseInt(statusCode);
                    }
                }
            }
        }
        return ends;
    };
    
    findAttachedSteps(attached, stepId, prerequisites) {
        let prerequisitesSteps = Object.keys(prerequisites || {});
        for (let p=0; p < prerequisitesSteps.length; p++) {
            if ((prerequisitesSteps[p] == "#") || (prerequisitesSteps[p] == ".") ){
                if (prerequisitesSteps[p] == ".") {
                    let composedSteps = prerequisites[prerequisitesSteps[p]]
                    for (let i=0; i < composedSteps.length; i++) {
                        this.findAttachedSteps(attached, stepId, composedSteps[i]);
                    }
                }
            } else {
                if (!attached[prerequisites[prerequisitesSteps[p]]] )  {
                    attached[prerequisites[prerequisitesSteps[p]]] = {};
                }
                if (!attached[prerequisites[prerequisitesSteps[p]]][prerequisitesSteps[p]] )  {
                    attached[prerequisites[prerequisitesSteps[p]]][prerequisitesSteps[p]] = [];
                }
                attached[prerequisites[prerequisitesSteps[p]]][prerequisitesSteps[p]].push(stepId);
            }
        }
    };
    
    static async GetJobStatus({jobRef, jobId, details}) {
        let jobData =  ElvOJob.GetJobInfoSync({jobRef, jobId});
        let jobStatus = ElvO.JOB_STATUSES[(jobData && jobData.workflow_execution.status_code) || 0];
        if (!jobData) {
            return null;
        }
        
        let stepsExecuted = jobData.workflow_execution.steps;
        let response;
        if (!jobId) {
            jobId = jobData.workflow_execution.job_id;
        }
        if (!details) {
            return {
                job_id: jobId,
                status: jobStatus, /*unknown|queued|created|ongoing|complete|exception|failed"*/
                status_code: jobData.workflow_execution.status_code,
                status_details: {
                    steps: stepsExecuted
                } /*steps is the job_object/meta/worklow_execution/steps metadata if the status is created and forward. null otherwise*/
            };
        } else {
            let steps = jobData.workflow_definition.steps;
            let stepIds = Object.keys(steps);
            let stepStatuses = {};
            let workflowProgress = {details: "-", message: "-", time_stamp: "", step_id: null};
            for (let stepId of stepIds) {
                if (stepsExecuted && stepsExecuted[stepId]) {
                    stepStatuses[stepId] =  stepsExecuted[stepId];
                } else {
                    let stepInfo =  ElvOJob.GetStepInfoSync(jobId, stepId, true);
                    if (stepInfo && stepInfo.status_code) {
                        stepStatuses[stepId] = stepInfo;
                        if (stepInfo.status_code == 10) {
                            let retries = (steps[stepId].retries && steps[stepId].retries.exception && steps[stepId].retries.exception.max) || 0;
                            stepStatuses[stepId].progress = ElvOAction.GetProgressMessage(jobId, stepId, stepInfo.attempts);
                            
                            if (workflowProgress.time_stamp < stepStatuses[stepId].progress.time_stamp) {
                                workflowProgress.time_stamp = stepStatuses[stepId].progress.time_stamp;
                                workflowProgress.step_id  = stepId;
                                workflowProgress.details = stepStatuses[stepId].progress.details;
                                workflowProgress.message = stepStatuses[stepId].progress.message;
                            }
                        } 
                        if (stepInfo.status_code == -10) {
                            if (workflowProgress.time_stamp < stepStatuses[stepId].end_time) {
                                workflowProgress.time_stamp = stepStatuses[stepId].end_time;
                                workflowProgress.step_id  = stepId;
                                workflowProgress.message = "Step waiting to retry";
                            }
                        }
                        if (stepInfo.status_code == 100) {
                            if (workflowProgress.time_stamp < stepStatuses[stepId].end_time) {
                                workflowProgress.time_stamp = stepStatuses[stepId].end_time;
                                workflowProgress.step_id  = stepId;
                                workflowProgress.message = "Step completed";
                            }
                        }
                        if (stepInfo.status_code == 99) {
                            if (workflowProgress.time_stamp < stepStatuses[stepId].end_time) {
                                workflowProgress.time_stamp = stepStatuses[stepId].end_time;
                                workflowProgress.step_id  = stepId;
                                workflowProgress.message = "Step failed";
                            }
                        }
                        if (stepInfo.status_code == -1) {
                            if (workflowProgress.time_stamp < stepStatuses[stepId].end_time) {
                                workflowProgress.time_stamp = stepStatuses[stepId].end_time;
                                workflowProgress.step_id  = stepId;
                                workflowProgress.message = "Step encountered an exception";
                            }
                        }
                    }
                }
            }
            
            response = {
                job_id: jobId,
                status: jobStatus, /*unknown|queued|created|ongoing|complete|exception|failed"*/
                status_code: jobData.workflow_execution.status_code,
                status_details: {steps: stepStatuses}, /*steps is the job_object/meta/worklow_execution/steps metadata if the status is created and forward. null otherwise*/
            };
            if (workflowProgress.time_stamp) {
                response.progress = workflowProgress;
            }
        }
        
        return response;
    };
    
    async GetWorkflowThrottle(workflowId) {
        let maxRunning = await  this.getMetadata({
            objectId: this.ObjectId,
            libraryId: this.LibraryId,
            metadataSubtree: "throttle/global"
        });
        if (!workflowId) {
            return {global_max_running: (maxRunning || 0)};
        }
        let wfMaxRunning, wfObjectId;
        let throttle = await  this.getMetadata({
            objectId: this.ObjectId,
            libraryId: this.LibraryId,
            metadataSubtree: "throttle/" + workflowId
        });
        if ((typeof throttle) == "object") {
            wfMaxRunning = throttle.limit;
            wfObjectId = throttle.workflow_object_id;
        } else {
            wfMaxRunning = throttle;
            wfObjectId = workflowId;
        }
        return {workflow_id: workflowId, workflow_object_id: wfObjectId, max_running: (wfMaxRunning ||0), global_max_running: (maxRunning || 0)};
    };
    
    async RetrieveThrottles(force) {
        if (!fs.existsSync("./Workflows")) {
            fs.mkdirSync("./Workflows");
        }
        if (!this.MaxRunning  || force) {
            let throttlesFilePath = "./Workflows/throttles.json";
            if (!fs.existsSync(throttlesFilePath) || force) {
                logger.Info("Retrieving workflow throttles");
                this.MaxRunning = await this.getMetadata({
                    objectId: this.ObjectId,
                    libraryId: this.LibraryId,
                    metadataSubtree: "throttle"
                });
                if (this.MaxRunning ) {
                    fs.writeFileSync(throttlesFilePath, JSON.stringify(this.MaxRunning, null, 2));
                } else {
                    this.MaxRunning = {};
                    this.MaxRunning[null] = 0;
                }
                if (this.MaxRunning.global) {
                    this.MaxRunning[null] = this.MaxRunning.global;
                }
            } else {
                this.MaxRunning = JSON.parse(fs.readFileSync(throttlesFilePath));
            }
            for (let workflowId in this.MaxRunning) {
                if  (!workflowId) {
                    continue;
                }            
                let throttleData = this.MaxRunning[workflowId];
                let workflowObjId = ((typeof throttleData) == "object") ? throttleData.workflow_object_id : workflowId;
                if (workflowObjId && workflowObjId.match(/^iq__/)) {
                    try {
                        await this.RetrieveWorkflowDefinition(workflowObjId, force, workflowId);
                    } catch(err) {
                        logger.Error("Skipping workflow "+ workflowId, err);
                    }
                }
            }
        }
        return this.MaxRunning;
    };
    
    GetThrottles(force) {
        if (!this.MaxRunning || force) {
            if (!fs.existsSync("./Workflows")) {
                fs.mkdirSync("./Workflows");
            }
            let throttlesFilePath = "./Workflows/throttles.json";
            if (!fs.existsSync(throttlesFilePath)) {
                logger.Error("Throttles not set")
                return {null: 0};
            } else {
                this.MaxRunning = JSON.parse(fs.readFileSync(throttlesFilePath));
                if (this.MaxRunning.global) {
                    this.MaxRunning[null] = this.MaxRunning.global;
                }
            }
        }
        return this.MaxRunning;
    };
    
    async SetWorkflowThrottle(workflowObjId, limit, workflowId) { //setMaxRunning(uint _max_running, address workflow_id)
        let writeToken = await this.getWriteToken({objectId: this.ObjectId, libraryId: this.LibraryId});
        if (workflowId) {
            await this.Client.ReplaceMetadata({
                objectId: this.ObjectId,
                libraryId: this.LibraryId,
                writeToken,
                metadataSubtree: "throttle/" + workflowId,
                metadata: {workflow_object_id: workflowObjId, limit}
            });
        } else {
            await this.Client.ReplaceMetadata({
                objectId: this.ObjectId,
                libraryId: this.LibraryId,
                writeToken,
                metadataSubtree: "throttle/" + ((!workflowObjId)  ? "global" : workflowObjId),
                metadata: limit
            });
        }
        let commitMessage = ((!workflowObjId)  ? "Modified global throttle" : ("Modified throttle for workflow " + workflowObjId));
        let result = await this.FinalizeContentObject({
            objectId: this.ObjectId,
            libraryId: this.LibraryId,
            writeToken,
            commitMessage
        });
        if (result && result.hash) {
            logger.Info(commitMessage, limit);
            await this.RetrieveThrottles(true);
            return true;
        } else {
            logger.Error("Failed to modify throttle", {workflowObjId, limit});
            return false;
        }
    };
    
    
    
    
    //returns: {
    //   status: { state: 'failed', code: 99, progress_message: 'Execution Failed' },
    //   handle: '1612823693828',
    //   execution_node: ''
    // }
    async ExecuteStep(jobId, stepId, stepsMap, jobInfo) { //hanlde only provided in case of retry
        if (!jobInfo) {
            jobInfo = ElvOJob.GetJobInfoSync(jobId);
        }
        let workflowDefinition = jobInfo.workflow_definition;
        let workflowExecution = jobInfo.workflow_execution;
        let stepDefinition = workflowDefinition.steps[stepId];
        try {
            if (!stepDefinition || !stepDefinition.configuration) {
                throw new Error("No configuration found for " +stepId);
            }
            let prerequisites = stepDefinition.prerequisites || {};
            if (!this.testCondition(jobId, prerequisites, stepsMap)) {
                return null;
            }
            
            let payload = {
                parameters: stepDefinition.configuration.parameters,
                references: {step_id: stepId, job_id: jobId, workflow_id: jobInfo.workflow_object_id, group_reference: workflowExecution.group_reference || workflowExecution.reference},
                inputs: {}
            };
            let stepInputs = Object.keys(stepDefinition.configuration.inputs || {}) ;
            for (let i=0; i < stepInputs.length; i++) {            
                let stepInput = stepInputs[i];
                try {
                    let stepInputDefinition = stepDefinition.configuration.inputs[stepInput];
                    switch (stepInputDefinition.class) {
                        case "parameter": {
                            if (!workflowDefinition.parameters[stepInputDefinition.location]) {
                                logger.Error("Parameter does not exist ", stepInputDefinition.location);
                                throw "ERROR: Parameter does not exist " + stepInputDefinition.location;
                            }
                            if (workflowExecution.parameters.hasOwnProperty(stepInputDefinition.location)) {
                                payload.inputs[stepInput] = workflowExecution.parameters[stepInputDefinition.location];
                            } else {
                                if (workflowDefinition.parameters[stepInputDefinition.location].required) {
                                    logger.Error("Required parameter not provided ", stepInputDefinition.location);
                                    throw "ERROR: required parameter not provided " + stepInputDefinition.location;
                                } else {
                                    payload.inputs[stepInput] = workflowDefinition.parameters[stepInputDefinition.location].default;
                                }
                            }
                            break;
                        }
                        case "output": {
                            let outputSourceStepDefinition = workflowDefinition.steps[stepInputDefinition.step_id];
                            payload.inputs[stepInput] = await ElvOJob.GetStepInfo(jobId, stepInputDefinition.step_id, "outputs/"+stepInputDefinition.location);
                            break;
                        }
                        case "status": {
                            let outputSourceStepDefinition = workflowDefinition.steps[stepInputDefinition.step_id];
                            payload.inputs[stepInput] = await ElvOJob.GetStepInfo(jobId, stepInputDefinition.step_id, stepInputDefinition.location);
                            break;
                        }
                        case "constant": {
                            payload.inputs[stepInput] = stepInputDefinition.value;
                            break;
                        }
                        case "info": {
                            if (stepInputDefinition.location == "job_id") {
                                payload.inputs[stepInput] = jobId;
                            }
                            if (stepInputDefinition.location == "job_ref") {
                                payload.inputs[stepInput] = ElvOJob.GetJobRef(jobId);
                            }
                            break;
                        }
                        default: {
                            throw new Error("Unsupported input class", stepInputDefinition.class);
                        }
                    }
                } catch(err) {
                    logger.Error("Error retrieving input " + stepInput, err);
                    let jobRef = workflowExecution.reference;
                    ElvOJob.MarkJobExecutedSync({jobRef}, -1);
                    return {status: {code: -1, progress_message: "Error retrieving input " + stepInput, state: "exception"}};
                }
            }
            for (let i in payload.inputs) {
                let input = payload.inputs[i];
                if (input && ((typeof input) == "object") && (input.cluster_type)) {              
                    if (input.cluster_type == "round-robin") {
                        logger.Error("Round robin cluster type not implemented, using random mode instead");
                        input.cluster_type = "random";
                    }
                    if (input.cluster_type == "random") {
                        let random = (new Date()).getTime();
                        let value = input.cluster_values[ random % input.cluster_values.length];
                        //logger.Debug("Selecting "+ value);
                        payload.inputs[i + ".cluster_values"] = input.cluster_values;
                        payload.inputs[i] = value;
                    }
                }
            }
            
            for (let i=0; i < stepInputs.length; i++) {
                let stepInput = stepInputs[i];
                let stepInputDefinition = stepDefinition.configuration.inputs[stepInput];
                if (stepInputDefinition.preprocessing) {
                    try {
                        let input = payload.inputs[stepInput];
                        let inputs = payload.inputs;
                        eval(stepInputDefinition.preprocessing);
                        payload.inputs[stepInput] = input;
                    } catch (errProc) {
                        logger.Error("Preprocessing failed for " + stepId +"/" + stepInput, errProc);
                        logger.Debug("inputs", payload.inputs);
                        logger.Debug("stepInputDefinition.preprocessing", stepInputDefinition.preprocessing);
                    }
                }
            }
            
            try {
                let maxAttempts = stepDefinition.retries && stepDefinition.retries.exception && stepDefinition.retries.exception.max;
                if (!maxAttempts && maxAttempts != 0) {
                    maxAttempts = 1;
                }
                let retryDelay = stepDefinition.retries && stepDefinition.retries.exception && stepDefinition.retries.exception.delay;
                if (!retryDelay && retryDelay != 0) {
                    retryDelay = ElvOAction.DEFAULT_RETRY_DELAY;
                }
                payload.max_attempts = maxAttempts;
                payload.retry_delay = retryDelay;
                payload.idle_timeout = stepDefinition.idle_timeout || null;
                if (stepDefinition.hasOwnProperty("polling_interval")) {
                    payload.polling_interval = stepDefinition.polling_interval;
                }
                payload.action = stepDefinition.action.action;
                payload.action_type = (!stepDefinition.action.type || (stepDefinition.action.type == "node-js")) ? "actions" : stepDefinition.action.type;
                let actionResult;
                try {
                    actionResult = await ElvOAction.Launch(payload, this.Client);
                } catch(errExec) {
                    logger.Error("Could not parse result", errExec);
                    throw "Could not execute action"
                }
                if (actionResult && actionResult.pid) {
                    actionResult.status = {state: "started", progress_message: "Started with pid " + actionResult.pid, pid: actionResult.pid};
                }
                return actionResult;
            } catch(err) {
                logger.Error("Error executing " +jobId +"/" + stepId, err);
                ElvOJob.MarkStepExceptionSync(jobId, stepId);
                return {status: {code: -1, progress_message: "Execution error", state: "exception"}};
            }
        } catch(errStep) {
            logger.Error("Error executing step "+ stepId, errStep);
            let jobRef = workflowExecution.reference;
            ElvOJob.MarkJobExecutedSync({jobRef}, -1); 
            return null; 
        }
    };
    
    
    
    
    
    
    async CheckStepStatus(jobId, stepId, attempts, stepsMap){
        try {
            let statusResult = await ElvOAction.CheckStatus(jobId, stepId, attempts, this.Client);
            let actionPid = statusResult.pid;
            
            let foundState = statusResult.status.state && statusResult.status.state.toLowerCase() || "exception";
            logger.Debug("Status check for " + stepId + " (" + jobId + ")", foundState);
            let currentStatus = ElvO.STEP_STATUSES[foundState];
            return {
                status_code: currentStatus,
                state: foundState,
                progress_message: statusResult.status.progress_message
            };
        } catch(errCheck) {
            logger.Error("Could not check status for " + jobId + "/" + stepId, errCheck);
            throw errCheck;
        }
    };
    
    JobInfo(jobRef) {
        return ElvOJob.GetJobInfoSync({jobRef});
    };
    
    
    
    
    PopFromQueueAndCreateJobs() {
        this.Popped = 0;
        let jobCapacity = ElvOJob.JobCapacity(this, null, true);
        let jobCapacities = {};
        if (jobCapacity > 0) { 
            let items = ElvOQueue.AllQueued();
            if (items.length == 0) {
                return 0;
            }
            this.QueuedFound = items.length;
            logger.Debug("Initiating queue popping", {found: this.QueuedFound} );
            for (let j = 0; j < items.length; j++) {
                let item;
                try {
                    let itemWorkflowId = items[j].workflow_id;
                    if (jobCapacities[itemWorkflowId] == null) {
                        jobCapacities[itemWorkflowId] = ElvOJob.JobCapacity(this, itemWorkflowId);
                    }
                    if (jobCapacities[itemWorkflowId] > 0) {
                        //item = ElvOQueue.Item(items[j].path, items[j].queue_id);
                        let entry = ElvOQueue.Pop(items[j].queue_id, items[j].path);
                        if (!entry) {
                            continue;
                        }
                        item = entry.item;
                        let jobInfo = ElvOJob.GetJobInfoSync({jobRef: item.id, silent: true});
                        if (!jobInfo) {
                            let result =  ElvOJob.CreateJob(this, item);
                            jobCapacities[itemWorkflowId]--;
                            jobCapacity--;
                            if (result.error_code) {
                                throw "An  error (" + result.error_code + ") occurred while creating a Job for " + item.id;
                            }
                        } else {
                            logger.Info("Reference of object to pop already found, skipping...")
                        }
                        
                        this.Popped++;
                        if (jobCapacity <= 0) {
                            return this.Popped;
                        }
                    } else {
                        //logger.Info("Job "+ item.id+ " ("+workflowObjId+") is throttled out");
                    }
                } catch (err) {
                    logger.Error("Could not pop job " + items[j].path + " from " + items[j].queue_id, err);
                    if (item) {
                        let jobInfo = ElvOJob.GetJobInfoSync({jobRef: item.id});
                        if (jobInfo) { //if not registered, then no need to pop and err, it will retry on next iteration
                            if (jobInfo.workflow_execution.status_code != -1) {
                                ElvOJob.MarkJobExecutedSync({jobRef: item.id}, -1);
                            }
                            ElvOQueue.Pop(queueId, items[j].path, "error");
                        }
                    } else {
                        logger.Error("Item " + items[j].path + " is likely mis-formed and might be stuck in status queued.");
                        ElvOQueue.Pop(queueId, items[j].path, "error");
                    }
                }
            }
        } else {
            logger.Info("Max running job reached");
        }
        return this.Popped;
    };
    
    
    
    async executeReadySteps(jobId, stepsMap, jobInfo) {
        let stepsStarted = 0;
        for (let stepId in stepsMap) { //since all steps are independent, those could be executed asynchronously
            let info = stepsMap[stepId];
            if ((info.status_code == 0) || (info.status_code == 5)) {
                let launchResult = await this.ExecuteStep(jobId, stepId, stepsMap, jobInfo); //does not do anything if prerequisites are not met
                if (launchResult && launchResult.status) {
                    logger.Info("Initiated step " + jobId + "/" + stepId, launchResult);
                    info.status_code = launchResult.status.code;
                    info.polling_interval = launchResult.polling_interval || 0;
                    info.last_polled = (new Date()).getTime();
                    stepsStarted++;
                }
            }
            if (info.status_code == ElvOAction.EXECUTION_EXCEPTION_TO_BE_RETRIED) {
                //check if PID is running
                if (info.pid && !ElvOAction.PidRunning(info.pid)) {
                    let foundTimestamp = (new Date(info.end_time)).getTime();
                    let retryIn = (foundTimestamp + ((info.retry_delay || ElvOAction.DEFAULT_RETRY_DELAY) * 1000)) - (new Date()).getTime();
                    if (retryIn <= 0) {
                        logger.Debug("Found step marked for retry " + jobId + "/" + stepId + " with pid " + info.pid);
                        let launchResult = await this.ExecuteStep(jobId, stepId, stepsMap, jobInfo); //wastefull as we know pre-requisites are met
                        if (launchResult && launchResult.status) {
                            logger.Info("Restarted step " + jobId + "/" + stepId, launchResult);
                            info.status_code = launchResult.status.code;
                            info.polling_interval = launchResult.polling_interval || 0;
                            info.last_polled = (new Date()).getTime();
                            stepsStarted++;
                        }
                    } else {
                        //logger.Debug("Found step marked for retry  " + jobId + "/" + stepId + " in " + Math.round(retryIn / 1000));
                    }
                } else {
                    if (info.pid) {
                        logger.Debug("Step found marked to be retried is still running " + jobId + "/" + stepId, info.pid);
                        //we could check if we are within the time range of an expected delayed "to be retried"
                    } else {
                        logger.Error("Step found marked to be retried does not have a pid " + jobId + "/" + stepId, info);
                    }
                }
            }
        }
        return stepsStarted;
    };
    
    async checkStatusOfOngoingSteps(jobId, stepsMap) {
        let statuser =  {};
        for (let stepId in stepsMap) {
            statuser[stepId] = stepsMap[stepId].status_code;
        }
        //logger.Peek("checkStatusOfOngoingSteps for "+jobId , statuser);
        let stepsChecked = 0;
        for (let stepId in stepsMap) {
            try {
                let stepInfo =  stepsMap[stepId];
                if (stepInfo.status_code == 10) {   //check status of steps that are in progress
                    if (!stepInfo.last_polled || ((stepInfo.last_polled  + ((stepInfo.polling_interval ||0) * 1000)) < new Date().getTime()) ) {
                        let stepStatus = await this.CheckStepStatus(jobId, stepId, stepInfo.attempts, stepsMap); //check_status checks if pid is alive
                        //if job has been interrupted, decision on what to do should come here, re-try if specified, recover in case of complete but not updated
                        stepInfo.last_polled = new Date().getTime();
                        stepInfo.status_code = stepStatus.status_code;
                        stepsChecked++;
                    }
                }
            } catch(errStep) {
                logger.Error("Could not check status for step "+ stepId, errStep);
            }
        }
        return stepsChecked;
    };
    
    
    
    checkForCompletion(jobId, stepsMap, jobInfo){
        try {
            if (!jobInfo) { //not needed
                jobInfo =  ElvOJob.GetJobInfoSync(jobId);
            }
            let jobReference = jobInfo.workflow_execution.reference;
            let workflowDefinition = jobInfo.workflow_definition;
            if (!this.WorkflowStates) {
                this.WorkflowStates = {};
            }
            if (!this.WorkflowStates[jobReference]){
                this.WorkflowStates[jobReference] = this.expandWorkflowStates(workflowDefinition);
            }
            let workflowStates = this.WorkflowStates[jobReference];
            
            let ends = Object.keys(workflowStates);
            for (let i=0; i < ends.length; i++) {
                let endState = workflowStates[ends[i]];
                if (this.testCondition(jobId, endState.prerequisites, stepsMap)) {
                    logger.Info("Mark job '"+jobReference+"' as executed",  endState.job_status_code);
                    ElvOJob.MarkJobExecutedSync({jobId}, endState.job_status_code);
                    return ends[i];
                }
            }
            return null;
        } catch(errEnd) {
            logger.Error("Could not check completion for job "+jobId, errEnd);
            return "exception";
        }
    };
    
    
    
    
    async RetrieveWorkflowDefinition(workflowObjectId, force, workflowId) {
        if  (!workflowId) {
            workflowId = workflowObjectId;
        }
        if (workflowObjectId && !workflowObjectId.match(/^iq__/)) {
            throw new Error("Invalid worklfow object id "+ workflowObjectId);
        }
        if (!this.WorkflowDefinitions)  {
            this.WorkflowDefinitions = {}
        }
        if (!fs.existsSync("./Workflows")) {
            fs.mkdirSync("./Workflows");
        }
        let workflowFilePath = "./Workflows/" + workflowId  +".json";
        if (!fs.existsSync(workflowFilePath) || force) {
            if (!workflowObjectId) {
                throw new Error("A worklfow object id is required as workflow definition is not cached for "+ workflowId);
            }
            let workflowDefinition = await this.getMetadata({objectId: workflowObjectId, metadataSubtree: "workflow_definition"});
            workflowDefinition.workflow_object_id = workflowObjectId;
            workflowDefinition.workflow_object_version_hash = await this.getVersionHash({objectId: workflowObjectId});
            if (workflowDefinition) {
                fs.writeFileSync(workflowFilePath, JSON.stringify(workflowDefinition, null, 2));
                this.WorkflowDefinitions[workflowId] = workflowDefinition;
            } else {
                logger.Error("Could not retrieve workflow definition for " +workflowId);
            }
        } else {
            this.WorkflowDefinitions[workflowId] = JSON.parse(fs.readFileSync(workflowFilePath));
        }
        return this.WorkflowDefinitions[workflowId];
    };
    
    async LogExecution(jobInfo) {
        let workflowId, jobId;
        try {
            workflowId = jobInfo.workflow_id;
            jobId = jobInfo.workflow_execution.job_id;
            let workflowObjId = jobInfo.workflow_definition.workflow_object_id;
            let ref  = (workflowObjId) ? (workflowObjId + "/" + jobId) : jobId;
            await this.getMetadata({
                objectId: this.ObjectId,
                libraryId: this.LibraryId,
                metadataSubtree: "throttle/"+workflowId,
                options:  {headers: {"User-Agent":"o-execution-reporting", "Referer": ref}}
            });
        } catch(err) {
            logger.Error("Could not log workflow execution for job "+jobId, err);
        }
    };
    
    GetWorkflowDefinition(workflowId, force) {
        if (!this.WorkflowDefinitions)  {
            this.WorkflowDefinitions = {}
        }
        if (!this.WorkflowDefinitions[workflowId] || force) {
            let workflowFilePath = "./Workflows/" + workflowId  +".json";
            if (fs.existsSync(workflowFilePath)) {
                this.WorkflowDefinitions[workflowId] = JSON.parse(fs.readFileSync(workflowFilePath));
            } else {
                logger.Error("Workflow definition for "+workflowId + " is not accessible");
            }
        }
        return this.WorkflowDefinitions[workflowId];
    };
    
    
    async runJobLoop(jobId, stepsMap) {
        let stats = {steps_started: 0, steps_checked:0};
        try {
            let startTime = (new Date()).getTime();
            let jobInfo =  ElvOJob.GetJobInfoSync({jobId});
            if (!jobInfo) {
                logger.Info("Reference not found for job "+jobId);
                return true;
            }
            let jobData = jobInfo.workflow_execution;
            if (!jobData) {
                //job not created yet
                return false;
            }
            //logger.Debug("jobData for "+ jobId, jobData);
            if ((jobData.status_code < 0) || (jobData.status_code >= 99)) {
                ElvOJob.MarkJobExecutedSync({jobId}, jobData.status_code);
                stats.executed = true;
            }
            
            if (jobData.status_code == 5) {
                let jobStatus = await this.RestartJob(jobId);
                if (jobStatus == 10) {
                    stats.restarted = true;
                    jobData.status_code = jobStatus;
                }
            }
            
            if (jobData.status_code == 10) {
                let workflowDefinition =  jobInfo.workflow_definition;
                let steps = Object.keys(workflowDefinition.steps);
                for (let stepId of steps) {
                    let stepInfo = stepsMap[stepId] || {};
                    stepsMap[stepId] = Object.assign(stepInfo, ElvOJob.GetStepInfoSync(jobId, stepId)); //currently only last_polled would be clobbered without the assign, but other values might be added
                }
                
                let stepsStarted = await this.executeReadySteps(jobId, stepsMap, jobInfo);
                stats.steps_started += stepsStarted;
                
                let stepsChecked = await this.checkStatusOfOngoingSteps(jobId, stepsMap);
                stats.steps_checked += stepsChecked;
                
                let endMet = this.checkForCompletion(jobId, stepsMap, jobInfo);
                
                if (endMet) {
                    stats.executed = endMet;
                }
                let endTime = (new Date()).getTime();
                stats.loop_duration = (endTime - startTime);
                //logger.Debug("Job-loop info - stats " + jobId, stats);
            }
            
        } catch(errRef)  {
            logger.Error("Could not run job loop for " + jobId, errRef);
        }
        return (stats.executed != null);
    };
    
    
    static async RunJob(o, jobId, stepsMap) {
        let heartbeat = ElvO.DEFAULT_JOB_HEARTBEAT * 1000;
        try {
            //let jobData = ElvOJob.parseJobId(jobId);
            let jobInfo = ElvOJob.GetJobInfoSync({jobId});
            if (jobInfo.workflow_definition.heartbeat) {
                heartbeat = jobInfo.workflow_definition.heartbeat * 1000;
            }
            let endMet = null;
            while (ElvOJob.IsJobInProgress(jobId)) {
                try {
                    let endMet = await o.runJobLoop(jobId, stepsMap);
                    if (endMet) {
                        await o.LogExecution(jobInfo);
                        return endMet;
                    }
                    await o.sleep(heartbeat);                    
                } catch(errLoop) {
                    logger.Error("Error executing job loop", errLoop);
                    await o.sleep(heartbeat);
                }
            }
        } catch (err) {
            logger.Error("Error executing job", err);
        }
        logger.Info("Job "+ jobId + " is not running any longer");
        return 0;
    };
    
    async RunJobs() {
        let jobIds =  ElvOJob.GetRunningJobsDataSync();
        let executingJobs = ElvOJob.GetExecutingJobsDataSync();
        
        let inProgressJobs = jobIds.length;
        this.InProgress = inProgressJobs;
        for (let jobId of jobIds) {
            if (!jobId || (jobId == "undefined")) {
                continue;
            }
            if (!executingJobs[jobId]) {
                logger.Debug("Processing " + jobId);
                
                let actionEnv = process.env; //{...process.env, "PAYLOAD": payloadStr, "PERSISTENCE": action.Persistence};
                const subprocess = spawn("nohup", ["node", "o.js" ,  "run-job", "--job-id=" + jobId], {
                    detached: true,
                    stdio: 'ignore',
                    env: actionEnv
                });
                let jobPid = subprocess.pid;
                if (ElvOProcess.Platform() != "linux") { //detaching does not seem to work on LINUX and dead processes tend to stay as Zombies
                    subprocess.unref();
                } else {
                    subprocess.on('exit', function () {
                        logger.Debug("job process exited", jobPid);
                    })
                }
            }
        }
        if (jobIds.length > 0) {
            logger.Debug("Running jobs", jobIds.length);
        }
        return jobIds.length;
    };
    
    
    
    static STEP_STATUSES = {"complete":100,"started":10,"exception":-1,"initiated":5,"failed":99};
    static JOB_STATUSES = {100: "complete", 99: "failed", '-1': "exception", '-5': "canceled", 0: "unknown", 1:"queued", 5:"created", 10:"ongoing" };
    static HEARTBEAT = 5000; //ms
    static DEFAULT_JOB_HEARTBEAT = 1;
    
};


module.exports=ElvO;
