const logger = require('./o-logger');
const keccak256 = require('keccak256');
const fs = require("fs");
const path = require('path');
const glob = require("glob");
const ElvOQueue = require("./o-queue");
const ElvOProcess = require("./o-process");
let { execSync } = require('child_process');
const { lookup } = require('dns');
const ElvOAction = require('./o-action');

class ElvOJob {
    
    
    static CreateJob(o, item) {
        let workflowId = item.workflow_id;
        let receivedParameters = item.parameters;
        let workflowDefinition = o.GetWorkflowDefinition(workflowId, true);
        let parameters = Object.keys(workflowDefinition.parameters || {});
        let missing = [];
        for (let i = 0; i < parameters.length; i++) {
            let parameter = parameters[i];
            if (!receivedParameters.hasOwnProperty(parameter)) {
                if (workflowDefinition.parameters.required) {
                    missing.push(parameter)
                }
            }
        }
        if (missing.length != 0) {
            logger.Error("ERROR: can't create job with missing parameters", missing);
            return {
                error_code: 10,
                error_message: "Can't create job with missing parameters : " + JSON.stringify(missing)
            };
        }
        let jobRef = item.id;
        let name = workflowDefinition.name + " - " + (jobRef || (new Date()).toLocaleString());
        let startTime = new Date();
        let jobId = this.createJobId(jobRef, startTime, workflowId);
        let meta = {
            public: {name: name},
            workflow_id: workflowId,
            workflow_definition: workflowDefinition,
            workflow_execution: {
                parameters: receivedParameters,
                reference: jobRef,
                job_id: jobId,
                start_time: startTime.toISOString(),
                status_code: 10
            },
            queued_path: item.path
        };
        if (item.group_reference) {
            meta.workflow_execution.group_reference = item.group_reference;
        }
        try {
            let jobFolderPath = this.jobFolderPath(jobRef);
            fs.mkdirSync(path.join(jobFolderPath, "steps"), {recursive: true});
            let jobFilePath = path.join(jobFolderPath, "job.json");
            //fs.writeFileSync(jobFilePath, JSON.stringify(meta, null, 2), 'utf8');
            ElvOJob.writeJSON(jobFilePath, meta); 
            fs.linkSync(jobFilePath, this.runningJobPath(jobId));
        } catch (err) {
            logger.Error("ERROR: could not create job for " + item.id, err);
            return {error_code: 31, error_message: "could not create job"};
        }
        logger.Info("Job created", {job_id: jobId, job_name: name});
        return {job_id: jobId, job_name: name, error_code: 0};
    };

    //avoids overwritting a file with a half-written one when the disc is full
    static writeJSON(filePath, data) {
        let tempPath = filePath +".tmp";
        fs.writeFileSync(tempPath, JSON.stringify(data, null, 2), 'utf8');
        fs.renameSync(tempPath, filePath);
    };
    
    static JobCapacity(o, workflowId, force) {
        try {
            let maxRunning = this.getMaxRunning(o, workflowId, force);
            if (maxRunning == -1) {
                return this.MAX_JOBS;
            }
            if (!maxRunning) {
                return 0;
            }
            let runningFound;
            if (!workflowId) {
                runningFound = glob.sync(path.join(this.JOBS_ROOT, "running", "j_*"));
            } else {
                runningFound = glob.sync(path.join(this.JOBS_ROOT, "running", "j_*" + workflowId + "_*"));
            }
            return (maxRunning - runningFound.length);
        } catch(err) {
            logger.Error("Error retrieving job capacity for " + (workflowId || "global"), err);
            return 0;
        }
    };
    
    
    static MaxNotReached(o) {
        let maxRunning = this.getMaxRunning(o);
        if (maxRunning == -1) {
            return true;
        }
        if (!maxRunning) {
            return false;
        }
        let runningFound = glob.sync(path.join(this.JOBS_ROOT, "running", "j_*"));
        return (runningFound.length < maxRunning);
    };
    
    
    static getMaxRunning(o, workflowId, force) {
        if (!workflowId) {
            workflowId = null;
        }
        if (force || !o.MaxRunning || (workflowId && !o.MaxRunning.hasOwnProperty(workflowId))) {
            o.GetThrottles(force);
        }
        if (!o.MaxRunning.hasOwnProperty(workflowId)) {
            o.MaxRunning[workflowId] = 0; //to avoid re-reading if no limits are set
        }
        let wfData = o.MaxRunning[workflowId];
        if ((typeof wfData) == "object") {
            return wfData.limit
        } else {
            return wfData
        }
    };
    static notkeccak256(string) {
        if (string.length <= 255) {
            return string.replace(/[^a-zA-Z0-9_\-:]/g,"_");
        } else {
            let mainPart = string.substring(0, 240).replace(/[^a-zA-Z0-9_\-:]/g,"_");
            let suffix = keccak256(string).toString("hex").substring(0, 14);
            return mainPart + "-"+suffix;
        }
    }
    
    
    
    static toHex(jobRef) {
        let matcher = jobRef.match(/(.*)(--.*#[0-9]+)$/);
        if (matcher) {
            return "0x" + keccak256(matcher[1]).toString("hex")+"--"+ matcher[2];
        } else {
            return "0x" + keccak256(jobRef).toString("hex");
        }
    };
    
    static RenewWorkflowDefinitionSync({silent, jobRef, jobRefHex, jobId}) { //silent, jobRef || jobRefHex || jobId
        let jobFilePath;
        if (jobId) {
            let jobFolderPath = this.findJobPath(jobId);
            jobFilePath = path.join(jobFolderPath, "job.json");
        } else {
            if (jobRef) {
                jobRefHex = this.toHex(jobRef);
            }
            jobFilePath = this.jobFilePathFromRefHex(jobRefHex);
        }
        if (!fs.existsSync(jobFilePath)) {
            if (!silent) {
                logger.Info("Could not find info on disk for " + jobRefHex);
            }
            return null;
        }
        try {
            let meta = JSON.parse(fs.readFileSync(jobFilePath, 'utf8'));
            let workflowFilePath = "./Workflows/" + meta.workflow_id  +".json";
            let workflowDef = JSON.parse(fs.readFileSync(workflowFilePath));
            meta.workflow_definition = workflowDef;
            //fs.writeFileSync(jobFilePath, JSON.stringify(meta, null, 2),  'utf8');
            ElvOJob.writeJSON(jobFilePath, meta);
            return meta;
        } catch (err) {
            logger.Error("Could not read info from disk for " + jobRefHex, err);
            return null;
        }
    };
    
    static GetJobInfoSync({silent, jobRef, jobRefHex, jobId}) { //silent, jobRef || jobRefHex || jobId
        if (jobId) {
            return this.getJobInfoSync(jobId);
        }
        if (jobRef) {
            jobRefHex = this.toHex(jobRef);
        }
        let jobFilePath = this.jobFilePathFromRefHex(jobRefHex);
        if (!fs.existsSync(jobFilePath)) {
            if (!silent) {
                logger.Info("Could not find info on disk for " + jobRefHex);
            }
            return null;
        }
        try {
            let meta = JSON.parse(fs.readFileSync(jobFilePath, 'utf8'));
            return meta;
        } catch (err) {
            logger.Error("Could not read info from disk for " + jobRefHex, err);
            return null;
        }
    };
    
    static updateJobInfoSync(jobId, data, clobber) {
        let info;
        let jobPath = this.jobFilePath(jobId)
        if (!clobber && fs.existsSync(jobPath)) {
            info  = this.getJobInfoSync(jobId);
        } else {
            info = {};
        }
        Object.assign(info, data);
        //fs.writeFileSync(jobPath, JSON.stringify(info, null, 2), 'utf8');
        ElvOJob.writeJSON(jobPath, info);
        return info;
    };
    
    
    static MarkJobExecutedSync({jobRef, jobRefHex, jobId}, executionStatusCode) {
        let jobFilePath;
        if (jobRef) {
            jobFilePath = this.jobFilePathFromRef(jobRef);
        }
        if (jobRefHex) {
            jobFilePath = this.jobFilePathFromRefHex(jobRefHex);
        }
        if (jobId) {
            jobFilePath = this.jobFilePath(jobId);
        }
        let jobInfo = JSON.parse(fs.readFileSync(jobFilePath, 'utf8'));
        if (!jobId) {
            jobId = jobInfo.workflow_execution.job_id;
        }
        let steps = jobInfo.workflow_definition.steps;
        let changed = false;
        if (!jobInfo.workflow_execution) {
            jobInfo.workflow_execution = {};
        }
        if (!jobInfo.workflow_execution.steps) {
            jobInfo.workflow_execution.steps = {};
        }
        let stepsExecutionData = jobInfo.workflow_execution.steps;
        for (let stepId in steps) {
            let info = this.GetStepInfoSync(jobId, stepId, true);
            if (info) {
                changed = true;
                stepsExecutionData[stepId] = {
                    start_time: info.start_time,
                    end_time: info.end_time,
                    status_code: info.status_code,
                    handle: info.handle,
                    retries: info.retries,
                    outputs: info.outputs
                };
            } else {
                //we could grab the info from step which ended in exception and do not have outputs
            }
        }
        jobInfo.workflow_execution.status_code = executionStatusCode;
        jobInfo.workflow_execution.end_time = (new Date()).toISOString();
        
        //fs.writeFileSync(jobFilePath, JSON.stringify(jobInfo, null, 2), 'utf8');
        ElvOJob.writeJSON(jobFilePath, jobInfo);
        let runningFileLink = this.runningJobPath(jobId);
        if (fs.existsSync(runningFileLink)) {
            fs.unlinkSync(runningFileLink);
        }
        //logger.Peek("Removed running link", runningFileLink);
        //logger.Peek("executedFileLink ", {jobId, status: ElvOJob.JOB_STATUSES[executionStatusCode], executionStatusCode};
        let executedFileLink = ElvOJob.jobExecutionPath(jobId, ElvOJob.JOB_STATUSES[executionStatusCode]); 
        //logger.Peek("Executed link", executedFileLink);
        if (!fs.existsSync(executedFileLink)) {
            fs.linkSync(jobFilePath, executedFileLink);
        }
        return jobId;
    };
    
    static GetRunningJobsDataSync() {
        let jobsPath = glob.sync(path.join(this.JOBS_ROOT, "running", "j_*"));
        let jobs = [];
        for (let jobPath of jobsPath) {
            let jobId = path.basename(jobPath);
            let jobIdInfo = this.parseJobId(jobId);
            if (jobIdInfo) {
                jobs.push(jobId);
            }
        }
        return jobs;
    };
    
    
    
    static getJobInfoSync(jobId) {
        let jobMetadata;
        let jobFolderPath = this.findJobPath(jobId);
        let jobFilePath = path.join(jobFolderPath, "job.json");
        let reported=false;
        let read=false;
        let attempts = 0;
        let start = new Date().getTime();
        while ((new Date().getTime() - start) <= 250) {
            try {
                attempts++;
                jobMetadata = JSON.parse(fs.readFileSync(jobFilePath, 'utf8'));
                read = true;
            } catch(err){
                if (!reported) {
                    reported = true;
                    logger.Error("Could not read Job info at "+ jobFilePath, err);
                }
            }
        }
        if (reported) {
            if (read) {
                logger.Info("A temporary error occurred reading " +jobFilePath, {attempts});
            } else {
                logger.Error("An error occurred reading " +jobFilePath, {attempts});
                throw new Error("An error occurred reading " +jobFilePath);
            }
        }
        return jobMetadata;
    };
    
    
    
    
    static GetStepInfoSync(jobId, stepId, silent) {
        let stepInfo;
        let stepPath = this.stepFilePath(jobId, stepId);
        if (silent && !fs.existsSync(stepPath)) {
            //logger.Debug("Can not find output file for " + jobId + " / " + stepId, stepPath);
            return null;
        }
        try {
            let rawData = fs.readFileSync(stepPath, "utf8");
            stepInfo = JSON.parse(rawData);
        } catch (err) {
            stepInfo = {status_code: 0};
        }
        return stepInfo
    };
    
    
    static MarkStepInitiatedSync(jobId, stepId, pollingInterval, pid, retryDelay) {
        let data = this.GetStepInfoSync(jobId, stepId);
        if (data.status_code) {
            if (!data.attempts) { //0 or null - null found here would mean a step failed before having been initiated
                data.attempts = 1;
            } else {
                data.attempts++;
            }
        } else {
            data.attempts = 0;
        }
        if (!data.original_start_time) {
            data.original_start_time = (new Date()).toISOString();
        }
        if (pollingInterval != null) {
            data.polling_interval = pollingInterval;
        } else {
            data.polling_interval = 0; //default
        }
        if (retryDelay){
            data.retry_delay = retryDelay;
        }
        Object.assign(data, {
            start_time: (new Date()).toISOString(),
            status_code: 10,
            end_time: null,
            pid
        });
        return this.updateStepInfoSync(jobId, stepId, data, true);
    };
    
    static MarkStepExceptionSync(jobId, stepId) {
        return this.updateStepInfoSync(jobId, stepId, {status_code: -1, end_time: (new Date()).toISOString()});
    };
    
    static MarkStepExecutedSync(jobId, stepId, successful) {
        return this.updateStepInfoSync(jobId, stepId, {status_code: (successful) ? 100 : 99, end_time: (new Date()).toISOString()});
    };
    
    static MarkStepForRetrySync(jobId, stepId) {
        return this.updateStepInfoSync(jobId, stepId, {status_code: -10, end_time: (new Date()).toISOString()});
    };
    
    
    static PersistStepOutputsSync(outputs, jobId, stepId, statusCode, retries, progress) {
        this.updateStepInfoSync(jobId, stepId, {
            status_code: statusCode,
            outputs: outputs,
            end_time: (new Date()).toISOString(),
            retries: retries,
            message: progress && progress.message,
            details: progress && progress.details
        });
        return true;
    };
    
    
    static updateStepInfoSync(jobId, stepId, data, clobber) {
        let stepPath = this.stepFilePath(jobId, stepId);
        let info;
        if (!clobber && fs.existsSync(stepPath)) {
            info = this.GetStepInfoSync(jobId, stepId);
        } else {
            info = {};
        }
        Object.assign(info, data);
        //fs.writeFileSync(stepPath, JSON.stringify(info, null, 2), 'utf8');
        ElvOJob.writeJSON(stepPath, info);
        return info;
    }
    
    
    static async GetStepInfo(jobId, stepId, location) {
        let attempts = 0;
        let info;
        let errorEncountered;
        while ((attempts < 3) && !info) {
            try {
                info = this.GetStepInfoSync(jobId, stepId);
            } catch (err) {
                await ElvOProcess.Sleep(500);
                errorEncountered = err;
            }
            attempts++;
        }
        if (info) {
            if (info.status_code  == 0) {//if step has not executed, not point in looking at values
                return null; 
            }
            return this.getOutput(info, location);
            
        } else {
            logger.Error("Could not retrieve Step execution info for " + jobId + "/" + stepId, errorEncountered);
            return null;
        }
    };
    
    
    
    
    static getOutput(outputs, location) {
        try {
            let locationStr = location.split("/").map(function (item) {
                return "[\"" + item + "\"]"
            }).join("");
            let value = eval("outputs" + locationStr);
            return value;
        } catch (err) {
            logger.Error("Could not retrieve output " + location);
            //logger.Debug("Output "+ location, outputs);
            return null;
        }
    };
    
    
    static findJobFromRef(jobRefHex) {
        let jobFolderPath = this.jobFolderPathFromHex(jobRefHex);
        if (fs.existsSync(jobFolderPath)) {
            return jobFolderPath;
        }
        
        return null;
    };
    
    
    static findJobPath(jobId) {
        let jobIdInfo = this.parseJobId(jobId);
        return jobIdInfo && this.findJobFromRef(jobIdInfo.job_ref_hex);
    };
    
    static stepFilePath(jobId, stepId) {
        if (jobId == "-") {
            return "/tmp/"+stepId+".json";
        }
        let existing = this.findJobPath(jobId);
        if (existing) {
            return path.join(existing, "steps", stepId + ".json");
        } else {
            return null;
        }
    };
    
    static jobFilePath(jobId) {
        let jobIdInfo = this.parseJobId(jobId);
        let jobRefHex = (jobIdInfo && jobIdInfo.job_ref_hex);
        return this.jobFilePathFromRefHex(jobRefHex);
    };
    
    static GetJobRef(jobId) {
        let jobIdInfo = this.parseJobId(jobId);
        let jobRefHex = (jobIdInfo && jobIdInfo.job_ref_hex);
        let jobFilePath = this.jobFilePathFromRefHex(jobRefHex);
        let jobMetadata = JSON.parse(fs.readFileSync(jobFilePath, 'utf8'));      
        return jobMetadata.workflow_execution.reference;
    };
    
    static jobFilePathFromRef(jobRef) {
        let jobRefHex = this.toHex(jobRef);
        return this.jobFilePathFromRefHex(jobRefHex)
    };
    
    static jobFilePathFromRefHex(jobRefHex) {
        let jobFolder = this.jobFolderPathFromHex(jobRefHex);
        return path.join(jobFolder, "job.json");
    };
    
    static parseJobId(jobId) {
        try {
            let matcher = jobId.match(/j_([0-9]+)_(.*)_(0x.*)/);
            return matcher && {start_time: matcher[1], workflow_id: matcher[2], job_ref_hex: matcher[3]};
        } catch (err) {
            return null;
        }
    };
    
    static createJobId(jobRef, startTime, workflowObjId) {
        return "j_" + startTime.getTime() + "_" + workflowObjId + "_" + this.toHex(jobRef);
    };
    
    static jobFolderPath(jobRef) { //jobref of {hex: jobRefHex} or {hash: jobRefHash}
        let jobRefHex =  this.toHex(jobRef);
        return this.jobFolderPathFromHex(jobRefHex);
    };
    
    static jobFolderPathFromHex(jobRefHex) { //jobref of {hex: jobRefHex} or {hash: jobRefHash}
        return path.join(this.JOBS_ROOT, "data", jobRefHex);
    };
    
    static runningJobPath(jobId) {
        if (!this.RunningRoot) {
            this.RunningRoot = path.join(this.JOBS_ROOT, "running");
            fs.mkdirSync(this.RunningRoot, {recursive: true});
        }
        return path.join(this.RunningRoot, jobId);
    };
    /* //Deprecating to use status specific folders
    static executedJobPath(jobId) { //deprecated
        if (!this.ExecutedRoot) {
            this.ExecutedRoot = path.join(this.JOBS_ROOT, "executed");
            fs.mkdirSync(this.ExecutedRoot, {recursive: true});
        }
        return path.join(this.ExecutedRoot, jobId);
    };
    */


    static jobExecutionPath(jobId, status) {
        if (!this.Roots) {
            this.Roots = {};
        }
        if (!this.Roots[status]) {
            this.Roots[status] = path.join(this.JOBS_ROOT, status);
            fs.mkdirSync(this.Roots[status], {recursive: true});
        }
        return path.join(this.Roots[status], jobId);
    };
    
    static SaveStepPayloadSync(jobId, stepId, payload) {
        //let payloadStr = JSON.stringify(payload);
        let payloadFilePath = ElvOJob.StepPayloadPathSync(jobId, stepId);
        //fs.writeFileSync(payloadFilePath, payloadStr, "utf8");
        ElvOJob.writeJSON(payloadFilePath, payload);
        return payloadFilePath;
    };
    
    
    static RetrievePayloadSync(jobId, stepId) {
        let payloadFilePath = ElvOJob.StepPayloadPathSync(jobId, stepId);
        let payloadStr = fs.readFileSync(payloadFilePath, "utf8");
        return JSON.parse(payloadStr);
    };
    
    static StepPayloadPathSync(jobId, stepId) {
        if (jobId == "-") {
            return "/tmp/"+stepId+"_payload.json";
        }
        let existing = this.findJobPath(jobId);
        if (existing) {
            return path.join(existing, "steps", stepId + "_payload.json");
        } else {
            return null;
        }
    };
    
    static StepTrackerPathSync(jobId, stepId, attempt) {
        let existing = this.findJobPath(jobId);
        if (existing) {
            return path.join(existing, "steps", stepId + "_" + attempt + ".log");
        } else {
            return null;
        }
    };
    
    
    static ArchiveStepFiles({jobRef, jobRefHex}, stepId) {
        if  (!stepId) {
            stepId = "";
        }
        let jobPath = (jobRefHex) ? this.jobFolderPathFromHex(jobRefHex) : ElvOJob.jobFolderPath(jobRef);
        let stepsPath = path.join(jobPath, "steps");
         
        let stepRoot = path.join(stepsPath, stepId + "*.*");
        let candidates = glob.sync(stepRoot);
        if  (!stepId) {
            stepId = ".*";
        }
        let patterns = ["^" + stepId + "\\.json$", "^" + stepId + "_payload\\.json$", "^" + stepId + "_[0-9]+\\.log$"];
        for (let candidate of candidates) {
            let basename = path.basename(candidate);
            for (let pattern of patterns) {
                if (basename.match(pattern)) {
                    if (fs.existsSync(candidate + ".original")) {
                        fs.rmSync(candidate);
                    } else {
                        fs.renameSync(candidate, candidate + ".original");
                    }
                    break;
                }
            }
        }
    };
    
    static ListStepFiles(jobRef) {
        let stepsPath = path.join(ElvOJob.jobFolderPath(jobRef), "steps");
        let stepRoot = path.join(stepsPath, "*.+(json|log)");
        let candidates = glob.sync(stepRoot);
        let results = {};
        results.job_steps_root = stepsPath;
        let steps = [];
        for (let candidate of candidates) {
            let stats = fs.lstatSync(candidate);
            let basename = path.basename(candidate);
            steps.push({size: stats.size, modified:stats.mtime, name: basename});
        }
        results.steps = steps.sort(function(a,b){return (a.modified  > b.modified) ? 1 : -1});
        return results;
    };
    
    static CloneJob(jobId, stepId) {
        let jobInfo = this.GetJobInfoSync({jobId: jobId});
        if (!jobInfo.workflow_execution.group_reference) {
            jobInfo.workflow_execution.group_reference = jobInfo.workflow_execution.reference;
            this.updateJobInfoSync(jobId, jobInfo, true);
        }
        let jobDescription = {
            parameters: jobInfo.workflow_execution.parameters,
            workflow_object_id: jobInfo.workflow_object_id,
            workflow_id: jobInfo.workflow_id,
            group_reference: jobInfo.workflow_execution.group_reference || jobInfo.workflow_execution.reference
        };
        
        jobDescription.id = jobDescription.group_reference + "--" + stepId + "#"+ (new Date()).getTime();
        
        let queueingInfo = jobInfo.queued_path.match(/Archive\/([^\/]*)\/([0-9]+)__[0-9]+__[^_]*__(.*)$/);
        if (!queueingInfo) {
            logger.Error("Could not parse queued path " + jobInfo.queued_path);
            throw new Error("Invalid queued path");
        }
        let queueId = queueingInfo[1];
        let priority = parseInt(queueingInfo[2]);
        let pathInQueue = ElvOQueue.Queue(queueId, jobDescription, priority);
        return pathInQueue;
    };
    
    static findJobLink(jobId, notRunning) {
        for (let status of Object.values(!notRunning ? ElvOJob.JOB_STATUSES : ["complete", "failed", "exception"])) {
            let candidate = ElvOJob.jobExecutionPath(jobId, status);
            if (fs.existsSync(candidate)) {
                return candidate;
            }
        }
        return null;
    };

    static ClearJob({jobRef, jobRefHex, jobId}) { // jobRef || jobRefHex || jobId
        let stats = {};
        let changed = false;
        if (jobId) {
            jobRefHex = this.parseJobId(jobId).job_ref_hex;
        }
        if (jobRef) {
            jobRefHex = this.toHex(jobRef);
        }

        if (!jobId) {
            let jobInfo = this.GetJobInfoSync({jobRefHex});
            jobId = jobInfo.workflow_execution.job_id;
        }
        stats.job_id = jobId;
        let jobPath = this.findJobLink(jobId);
        if (jobPath) {
            if (jobPath.match(/\/running\//)) {
                stats.running = jobPath;
            } else {
                stats.executed = jobPath;
            }
            fs.rmSync(jobPath, {force: true, recursive: true});
            changed=true;
        }
                
        let jobFolderPath = this.jobFolderPathFromHex(jobRefHex);
        if (fs.existsSync(jobFolderPath)) {
            stats.job = jobFolderPath;
            fs.rmSync(jobFolderPath, {force: true, recursive: true});
            changed=true;
        }
        return (changed) ?  stats : null;
    };
    
    
    static RestartFrom({jobId, jobRef, jobRefHex, stepId, renew, simple}) {
        try {
            let jobInfo;
            if (renew) {
                jobInfo = this.RenewWorkflowDefinitionSync({jobRef, jobRefHex, jobId});
            } else {
                jobInfo = this.GetJobInfoSync({jobRef, jobRefHex, jobId});
            }
            jobInfo.workflow_execution.status_code = 10;
            jobInfo.workflow_execution.end_time = null;
            //logger.Peek("RestartFrom "+ stepId, Object.keys(jobInfo.workflow_execution.steps));
            let stepStartTime = jobInfo.workflow_execution.steps[stepId].start_time || jobInfo.workflow_execution.steps[stepId].end_time;
            if (!stepStartTime) {
                throw new Error("No start-time found for step " + stepId + " in job " + jobInfo.workflow_execution.job_id);
            }
            let stepsToReset = [];
            for (let candidateStepId in jobInfo.workflow_execution.steps) {
                let candidateStep = jobInfo.workflow_execution.steps[candidateStepId];
                let candidateStartTime = candidateStep.start_time || candidateStep.end_time;
                if (candidateStartTime && (candidateStartTime >= stepStartTime)) {
                    if ((candidateStep.status_code == 100) || (candidateStep.status_code == 99) || (candidateStep.status_code == -1)) {
                        stepsToReset.push(candidateStepId);
                    } else {
                        logger.Info("Unexpected status for step " + stepId + " in job " + jobInfo.workflow_execution.job_id, candidateStep.status_code);
                        stepsToReset.push(candidateStepId);
                    }
                }
            }
            if (!jobId) {
                jobId = jobInfo.workflow_execution.job_id;
            }
            if (!jobRefHex) {
                jobRefHex = this.parseJobId(jobId).job_ref_hex;
            }
            for (let stepToReset of stepsToReset) {
                logger.Info("Resetting step "+stepToReset);
                this.ArchiveStepFiles({jobRef: jobRef || jobInfo.workflow_execution.reference, jobRefHex}, stepToReset);
                delete jobInfo.workflow_execution.steps[stepToReset];
            }
            this.updateJobInfoSync(jobId, jobInfo, true);

            let jobFilePath = this.jobFilePathFromRefHex(jobRefHex);
            let runningFileLink = this.runningJobPath(jobId);
            let executedFileLink = this.findJobLink(jobId, true);
            if (executedFileLink) {
                fs.unlinkSync(executedFileLink);
            }
            if (!fs.existsSync(runningFileLink)) {
                fs.linkSync(jobFilePath, runningFileLink);
            }
            let matcher = jobInfo.workflow_execution.reference.match(/^s-workflow.*_(0x.*)--(.*)$/);
            if (!simple && matcher){
                let parentJobRefHex = matcher[1];
                let parentStepId = matcher[2];
                this.RestartAfter({jobRefHex: parentJobRefHex, stepId: parentStepId, inProgress: true});
            }
            return true;
        } catch(e) {
            logger.Error("Could not retry from step "+ stepId, e);
            return false;
        }
    };
    
    static RestartAfter({jobId, jobRef, jobRefHex, stepId, renew, inProgress}) {
        try {
            let jobInfo;
            if (renew) {
                jobInfo = this.RenewWorkflowDefinitionSync({jobRef, jobRefHex, jobId});
            } else {
                jobInfo = this.GetJobInfoSync({jobRef, jobRefHex, jobId});
            }
            jobInfo.workflow_execution.status_code = 10;
            jobInfo.workflow_execution.end_time = null;
            //logger.Peek("RestartFrom "+ stepId, Object.keys(jobInfo.workflow_execution.steps));
            let stepEndTime =  jobInfo.workflow_execution.steps[stepId].end_time;
            if (!stepEndTime) {
                throw new Error("No end-time found for step " + stepId + " in job " + jobInfo.workflow_execution.job_id);
            }
            let stepsToReset = [];
            for (let candidateStepId in jobInfo.workflow_execution.steps) {
                let candidateStep = jobInfo.workflow_execution.steps[candidateStepId];
                let candidateStartTime = candidateStep.start_time || candidateStep.end_time;
                if (candidateStartTime && (candidateStartTime >= stepEndTime)) {
                    if ((candidateStep.status_code == 100) || (candidateStep.status_code == 99) || (candidateStep.status_code == -1)) {
                        stepsToReset.push(candidateStepId);
                    } else {
                        logger.Info("Unexpected status for step " + stepId + " in job " + jobInfo.workflow_execution.job_id, candidateStep.status_code);
                        stepsToReset.push(candidateStepId);
                    }
                }
            }
            for (let stepToReset of stepsToReset) {
                logger.Info("Resetting step "+stepToReset);
                this.ArchiveStepFiles(jobRef || jobInfo.workflow_execution.reference, stepToReset);
                delete jobInfo.workflow_execution.steps[stepToReset];
            }
            if (!jobId) {
                jobId = jobInfo.workflow_execution.job_id;
            }
            if (inProgress) {
                logger.Info("Marking in progress step "+stepId);
                let attempt = jobInfo.workflow_execution.steps[stepId].retries;
                let trackerPath = this.StepTrackerPathSync(jobId, stepId, attempt);
                let lines = fs.readFileSync(trackerPath, {encoding: "utf8"}).split(/\n/);
                let resetLines = [];
                for (let line of lines) {
                    if (!line.match(/_-_80/) && !line.match(/_-_100/)) {
                        resetLines.push(line);
                    }                    
                }
                fs.renameSync(trackerPath, trackerPath+".original");
                fs.writeFileSync(trackerPath,  resetLines.join("\n")); 
                let stepPath = trackerPath.replace(/_[0-9]+\.log$/, ".json");
                let stepData = JSON.parse(fs.readFileSync(stepPath, {encoding: "utf8"}));
                stepData.status_code = 10;
                delete stepData.outputs;
                delete stepData.end_time;
                fs.writeFileSync(stepPath,  JSON.stringify(stepData, null, 2));
                delete jobInfo.workflow_execution.steps[stepId];
            }

            this.updateJobInfoSync(jobId, jobInfo, true);
            if (!jobRefHex) {
                jobRefHex = this.parseJobId(jobId).job_ref_hex;
            }
            let jobFilePath = this.jobFilePathFromRefHex(jobRefHex);
            let runningFileLink = this.runningJobPath(jobId);
            let executedFileLink = this.findJobLink(jobId, true);
            if (executedFileLink) {
                fs.unlinkSync(executedFileLink);
            }
            if (!fs.existsSync(runningFileLink)) {
                fs.linkSync(jobFilePath, runningFileLink);
            }
            let matcher = jobInfo.workflow_execution.reference.match(/^s-workflow.*_(0x.*)--(.*)$/);
            if (matcher){
                let parentJobRefHex = matcher[1];
                let parentStepId = matcher[2];
                this.RestartAfter({jobRefHex: parentJobRefHex, stepId: parentStepId, inProgress: true});
            }
            return true;
        } catch(e) {
            logger.Error("Could not retry after step "+ stepId, e);
            return false;
        }
    };

   
    
    static Restart({jobId, jobRef, jobRefHex, renew}) {
        try {
            let jobInfo;
            if (renew) {
                jobInfo = this.RenewWorkflowDefinitionSync({jobRef, jobRefHex, jobId});
            } else {
                jobInfo = this.GetJobInfoSync({jobRef, jobRefHex, jobId});
            }
            jobInfo.workflow_execution.status_code = 10;
            jobInfo.workflow_execution.end_time = null;
            
            if (!jobId) {
                jobId = jobInfo.workflow_execution.job_id;
            }
            this.updateJobInfoSync(jobId, jobInfo, true);
            if (!jobRefHex) {
                jobRefHex = this.parseJobId(jobId).job_ref_hex;
            }
            let jobFilePath = this.jobFilePathFromRefHex(jobRefHex);
            let runningFileLink = this.runningJobPath(jobId);
            let executedFileLink = this.findJobLink(jobId, true);
            if (executedFileLink) {
                fs.unlinkSync(executedFileLink);
            }
            if (!fs.existsSync(runningFileLink)) {
                fs.linkSync(jobFilePath, runningFileLink);
            }
            let matcher = jobInfo.workflow_execution.reference.match(/^s-workflow.*_(0x.*)--(.*)$/);
            if (matcher){
                let parentJobRefHex = matcher[1];
                let parentStepId = matcher[2];
                this.RestartAfter({jobRefHex: parentJobRefHex, stepId: parentStepId, inProgress: true});
            }
            return true;
        } catch(e) {
            logger.Error("Could not restart ", e);
            return false;
        }
    };
    
    static ListExecutedJobs({workflowId, groupId, minDate, maxDate, limit}) {
        let jobFilter = "j_*_" + (workflowId || "*") + "_0x*" + (groupId || "");
        let pathFilter = path.join(this.JOBS_ROOT, "{complete,failed,exception}", jobFilter);
        let rawCandidates = glob.sync(pathFilter);
        let candidates = (!limit) ? rawCandidates.reverse() : rawCandidates.sort(function(a,b){return (path.basename(a) > path.basename(b) ? -1 : 1)} );
        if (minDate || maxDate) {
            let jobIds = [];
            let min = (minDate && minDate.toString()) || "";
            let max = (maxDate && maxDate.toString()) || "A";
            for (let candidate of candidates) {
                let matcher = path.basename(candidate).match(/^j_([0-9]+)_/);
                if (matcher && (matcher[1] >= min) && (matcher[1] <= max)) {
                    jobIds.push(candidate);
                }
            }
            return jobIds.slice(0, limit);
        } else {
            return candidates.slice(0, limit);
        }
    };
    
    
    
    static IsJobExecuting(jobId) {
        let running = execSync("ps -ef | grep "+ jobId ).toString().split("\n");
        if (running.length > 0) {
            for (let line of running) {
                if (line.match(/run-job/)) {
                    return line.trim().split(/ +/)[1];
                }
            }
        } 
        return null;     
    };
    
    static FindExecutingSteps(jobId) {
        let running = execSync("ps -ef | grep "+ jobId ).toString().split("\n");
        let steps = {}; 
        if (running.length > 0) {
            for (let line of running) {
                if (line.match(/execute-sync/)) {
                    let pid = line.trim().split(/ +/)[1];
                    let matcher  =  line.match(/--step-id=([a_zA-Z0-9_]+)/)
                    steps[pid] = (matcher && matcher[1]) || "unknown";
                }
            }
            return steps;
        } 
        return null;     
    };
    
    static GetExecutingJobsDataSync() {
        let running = execSync("ps -ef | grep run-job").toString().split("\n");
        let jobs = {};
        for (let line of running) {
            let matcher = line.match(/--job-id=([^ ]+)/)
            if (matcher) {
                jobs[matcher[1]] = line.trim().split(/ +/)[1];
            }
        }       
        return jobs;    
    }
    
    static IsJobInProgress(jobId) {
        let jobPath = path.join(this.JOBS_ROOT, "running", jobId);
        return fs.existsSync(jobPath);
    };
    
    static GetExecutionStepsData(jobId, withOutputs) {
        let info = this.parseJobId(jobId);
        let detailsPath = path.join(this.jobFolderPathFromHex(info.job_ref_hex), "steps", "*.json");
        let candidates = glob.sync(detailsPath); 
        let stepsData = {};
        for (let candidate of candidates) {
            try {
                if (candidate.match(/_payload/)){
                    continue;
                }
                let detailsRaw = fs.readFileSync(candidate,"utf-8");
                let detail = JSON.parse(detailsRaw);
                if (!withOutputs) {
                    delete detail.outputs
                }
                let stepId = path.basename(candidate).replace(".json","");
                stepsData[stepId] = detail;
            } catch(err) {
                logger.Error("Could not retrieve details from "+candidate, err);
            }
        }
        return stepsData;
    };
    
    static CancelJob({jobId, jobRef}) {
        if (!jobId){
            let queuedJob = ElvOQueue.FindByJobReference(jobRef);
            if (queuedJob) {
                let canceled = (ElvOQueue.Pop(queuedJob.queue_id, queuedJob.item_file_name, "canceled") != null);
                if (canceled) {
                    return true;
                }
                // if queued job could not be canceled, it might have been popped, so reverting to running job logic
            }
        }
        let foundJobId = this.MarkJobExecutedSync({jobId, jobRef}, -5); 
        if (!jobId) {
            jobId = foundJobId;
        }
        let pid = this.IsJobExecuting(jobId);
        if (pid) { //kills job process
            try {
                let info = {};
                info[pid] = jobId;
                logger.Info("Killing job process ", info);
                process.kill(parseInt(pid), 9);
            }catch(errJob){
                logger.Info("Could not kill job process ", errJob);
            }
        }
        //Look for steps to kill
        let steps = this.FindExecutingSteps(jobId);
        for (let pid in steps) {
            let stepId = steps[pid];
            let info = {};
            info[pid] = jobId +"/"+stepId;
            try {
                logger.Info("Killing step process ", info);
                process.kill(parseInt(pid), 9);
            } catch(errStep) {
                logger.Info("Could not kill step process", errJob);
            }
        }
        let inProgress = this.IsJobInProgress(jobId);
        if (inProgress) {
            logger.Info("Canceled job "+ jobId);
        } else {
            logger.Error("Failed to cancel job "+ jobId);
        }
        return (!inProgress);
    };
    
    static async AutoPurge() {   
        let success = false;    
        try {
            let maxDate = (new Date()).getTime() - (ElvOJob.DAYS_KEPT * 24 * 3600 * 1000);
            let jobs = ElvOJob.ListExecutedJobs({maxDate});      
            logger.Info("Initiating jobs archive purge", {cutoffDate: new Date(maxDate), foundJobs: jobs.length});
            let count = 0;
            for (let jobPath of jobs) {
                let jobId;
                try {
                    jobId= path.basename(jobPath);
                    fs.unlinkSync(jobPath);
                    let info = ElvOJob.parseJobId(jobId);
                    if (info && info.job_ref_hex) {
                        fs.rmSync(ElvOJob.jobFolderPathFromHex(info.job_ref_hex), { recursive: true, force: true });
                    }
                    count++;
                } catch(errJob) {
                    logger.Error("Could not purge job files for " + jobId, errJob);
                }
            }
            logger.Info("Completed jobs archive purge", {purgedJobs: count, foundJobs: jobs.length});           
            success = true;
        } catch(err) {
            logger.Error("Could not perform jobs archive purge", err);
        } 
        setTimeout(ElvOJob.AutoPurge, 1000 * 3600 * 24);  
        return success;
    };
    
    static  JOB_STATUSES = {100: "complete", 99: "failed", '-1': "exception", 10: "running"};
    static JOBS_ROOT = "./Jobs";
    static DAYS_KEPT = 30;
    //static Workflows = {};
    //static LastSeenJobPath = {};
    static MAX_JOBS = 1000000; //limiting to 1 million
}

module.exports = ElvOJob;