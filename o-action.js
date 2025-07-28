const fs = require("fs");
const path = require('path');
const {spawn, spawnSync} = require('child_process');
const ElvOFabricClient = require("./o-fabric");
const ElvOProcess = require("./o-process");
const logger = require('./o-logger');
const ElvOJob = require("./o-job.js");


class ElvOAction extends ElvOFabricClient {
    
    constructor(params) {
        super();
        if (params.client) {
            this.Client = params.client;
        }
        if (params.payload) {
            this.Payload = params.payload;
            this.action_type = params.payload.action_type;
            if (params.payload.references) {
                this.JobId = params.payload.references.job_id;
                this.StepId = params.payload.references.step_id;
            }
            this.MaxAttempts = params.payload.max_attempts;
            this.RetryDelay = params.payload.retry_delay;
            this.expandParameters();
        }
    };
    
    Error(label, err) {
        logger.Error(label, err);       
        if (!err) {
            this.reportProgress("ERROR-" + label)
        } else {
            let msg;
            let data;
            if (!err.name) {
                msg = label;
                data = err;
            } else {
                msg = label + ": " + err.name;
                data = err.message
            }
            if  (this) {
                this.reportProgress("ERROR-" + msg, data);
                if (err.stack) {
                    this.logStack(err.stack.split("\n"));
                }
            } else {
                ElvOAction.trackProgress(ElvOAction.TRACKER_INTERNAL, "ERROR-" +msg, data); 
            }
        }
    };
    
    Debug(msg, data) {
        logger.Debug(msg, data);
        try {
            if (this) {
                this.reportProgress("DEBUG-" + msg, data); 
            } else {
                ElvOAction.trackProgress(ElvOAction.TRACKER_INTERNAL, "DEBUG-" + msg, data); 
            }
        } catch(err) {
            logger.Error("Could not log Debug entry", err);
        }
    };
    
    Info(msg, data) {
        logger.Info(msg, data);
        try {
            if (this) {
                this.reportProgress("INFO-" + msg, data);
            } else {
                ElvOAction.trackProgress(ElvOAction.TRACKER_INTERNAL, "INFO-" + msg, data);
            }
        } catch(err) {
            logger.Error("Could not log Info entry", err);
        }
    };
    
    Version() {
        return this.constructor.VERSION;
    };
    
    MaxMemory() {
        return null; //should be overloaded if default value for max-old-space-size is not appropriate
    };
    
    Parameters() {
        return {parameters: {}}; //should be overloaded with parameters required
    };
    
    IOs(parameters) {
        return {inputs: {}, outputs: {}}; //should be overloaded with parameters required
    };
    
    ActionId() {
        return "generic"; //should be overloaded with actual action name i.e.: ffprobe, test ...
    };
    
    Description() {
        return ""; //should be overloaded with actual action description...
    };
    
    IsContinuous() {
        return true; //indicates that the execution stays within a single PID
    };
    
    PollingInterval() {
        return 5;
    };
    
    IdleTimeout() {
        return null;
    };
    
    async Execute(inputs, outputs) {
        return 100;
    };
    
    //progressCode => {state, details, time_stamp};
    progressMessage() {
        return  ElvOAction.extractProgressMessage(this.Tracker);      
    };
    
    static extractProgressMessage(tracker) {
        if (tracker) {
            let progressTracker = tracker[ElvOAction.TRACKER_PROGRESS];
            let internalTracker = tracker[ElvOAction.TRACKER_INTERNAL];
            if (progressTracker && !internalTracker) {
                return {
                    message: progressTracker.state,
                    details: progressTracker.details,
                    time_stamp: progressTracker.time_stamp
                };
            }
            if (!progressTracker && internalTracker) {
                return {
                    message: internalTracker.state,
                    details: internalTracker.details,
                    time_stamp: internalTracker.time_stamp
                };
            }
            if (progressTracker && internalTracker) {
                if (progressTracker.time_stamp >= internalTracker.time_stamp) {
                    return {
                        message: progressTracker.state,
                        details: progressTracker.details,
                        time_stamp: progressTracker.time_stamp
                    };
                } else {
                    return { //returning public tracker, but use internal timestamp as it indicates the action is not stalled
                        message: progressTracker.state,
                        details: progressTracker.details,
                        time_stamp: internalTracker.time_stamp
                    };
                }
            }
            let initializationMark = tracker[ElvOAction.TRACKER_INITIATED];
            let finalizationMark = tracker[ElvOAction.TRACKER_EXECUTED];
            if (initializationMark && !finalizationMark) {
                let info = initializationMark.details.split(",");
                return {
                    message: "Initiated with pid " + info[0] + " at " + initializationMark.time_stamp,
                    time_stamp: (new Date()).toISOString()
                };
            }
            if (finalizationMark) {
                let finalStatusCode = finalizationMark.details;
                return {
                    message: "Step executed with status " + ElvOAction.STATUS_LABEL[finalStatusCode] + " at " + initializationMark.time_stamp,
                    time_stamp: (new Date()).toISOString()
                };
            }
        }
        return {message: "No tracking information yet", time_stamp: (new Date()).toISOString()};
    };
    
    static GetProgressMessage(jobId, stepId, attempt) {
        try {
            let trackerPath = ElvOAction.GetTrackerPath(jobId, stepId, attempt);
            let tracker = ElvOAction.readTracker(trackerPath);
            return ElvOAction.extractProgressMessage(tracker)
        } catch(err) {
            logger.Error("Could not retrieve progress message for "+stepId +"/"+jobId, err);
            return {message: "Could not retrieve progress message", time_stamp: (new Date()).toISOString()};
        }
    };
    
    
    
    static GetPid(jobId, stepId, handle) {
        let trackerPath = ElvOAction.GetTrackerPath(jobId, stepId, handle);
        let tracker = ElvOAction.readTracker(trackerPath);
        let initializationMark = tracker[ElvOAction.TRACKER_INITIATED];
        if (initializationMark) {
            let info = initializationMark.details.split(",");
            return parseInt(info[0])
        }
        return null;
    };
    
    parseDynamicVariables(expression, variablesDesc) {
        if (!variablesDesc) {
            variablesDesc = {};
        }
        let variables = [...expression.matchAll(/%[a-zA-Z0-9_\-]+%/g)];
        let dynamicVariables = {};
        for (let i = 0; i < variables.length; i++) {
            let variable = variables[i][0].replace(/%/g, '');
            if (!variablesDesc || !variablesDesc[variable]) {
                dynamicVariables[variable] = {"type": "string", "required": "true"};
            } else {
                dynamicVariables[variable] = variablesDesc[variable];
            }
        }
        return dynamicVariables;
    };
    
    static readTracker(trackerPath) {
        let tracker = {};
        if (fs.existsSync(trackerPath)) {
            let rawTracker = fs.readFileSync(trackerPath, 'utf8').split(/\n/);
            for (let i = 0; i < rawTracker.length; i++) {
                let matcher = rawTracker[i].match(/^(.*)_-_([0-9]+):(.*)_-_(.*)/);
                if (matcher) {
                    let details;
                    if (matcher[4]) {
                        try {
                            details = JSON.parse(matcher[4]);
                        } catch (err) {
                            details = matcher[4];
                        }
                    }
                    tracker[parseInt(matcher[2])] = {state: matcher[3], details: details, time_stamp: matcher[1]};
                } else {
                    if (rawTracker[i]) {
                        logger.Error("invalid tracker log entry", rawTracker[i]);
                    }
                }
            }
        }
        return tracker;
    };
    
    initializeTracker(starting) {
        if (!this.Attempts && (this.Attempts != 0)) {           
            this.Attempts = 0;
        }
        if (!this.TrackerPath) {
            this.TrackerPath = ElvOJob.StepTrackerPathSync(this.JobId, this.StepId, this.Attempts);
        }
        if (starting) {
            if (fs.existsSync(this.TrackerPath)) {
                logger.Error("Attempt already started");
                return null;
            }
        }
        this.Tracker = ElvOAction.readTracker(this.TrackerPath);
        return this.TrackerPath;
    };
    
    trackProgress(progressCode, statusMsg, details) {
        try {
            let timestamp = (new Date()).toISOString();
            if (this.Tracker) {
                this.Tracker[progressCode] = {state: statusMsg, details: details, time_stamp: timestamp};
            }
            let detailsJSON;
            if (details != null) {
                detailsJSON = JSON.stringify(details).replace(/\\n/g, " | ");
            }
            let line = timestamp + "_-_" + progressCode + ":" + statusMsg + "_-_" + (detailsJSON || "") + "\n";
            fs.writeFileSync(this.TrackerPath, line, {encoding: 'utf8', flag: "a"});
        } catch (err) {
            logger.Error("Could not trackProgress internally - " + statusMsg, err);
        }
    };
    
    static trackProgress(progressCode, statusMsg, details) {
        try {
            let timestamp = (new Date()).toISOString();
            let detailsJSON;
            if (details != null) {
                detailsJSON = JSON.stringify(details).replace(/\\n/g, " | ");
            }
            let line = timestamp + "_-_" + progressCode + ":" + statusMsg + "_-_" + (detailsJSON || "") + "\n";
            fs.writeFileSync(this.TrackerPath || ElvOAction.TrackerPath, line, {encoding: 'utf8', flag: "a"});
        } catch (err) {
            logger.Error("Could not trackProgress internally - " + statusMsg, err);
        }
    };
    
    logStack(lines) {
        let progressCode = ElvOAction.TRACKER_ERROR_STACK;
        try {
            let timestamp = (new Date()).toISOString();
            let textMsg = [];
            let index = 0;
            for (let line of lines) {
                index++;
                textMsg.push(timestamp + "_-_" + progressCode + ":" + index +" of " +lines.length + "_-_" + JSON.stringify(line) + "\n");
            }
            fs.writeFileSync(this.TrackerPath, textMsg.join(""), {encoding: 'utf8', flag: "a"});
        } catch (err) {
            logger.Error("Could not trackProgress internally - " + lines[0], err);
        }
    };
    
    
    static GetTrackerPath(jobId, stepId, attempt) {
        return ElvOJob.StepTrackerPathSync(jobId, stepId, attempt);
    };
    
    trackerPath() {
        return ElvOJob.StepTrackerPathSync(this.JobId, this.StepId, this.Attempts);
    };
    
    expandParameters() {
        if (!this.Payload.parameters) {
            this.Payload.parameters = {};
        }
        let paramSpec = this.Parameters().parameters;
        for (let parameter in paramSpec) {
            if (!this.Payload.parameters.hasOwnProperty(parameter) && (paramSpec[parameter].required == false)) {
                this.Payload.parameters[parameter] = paramSpec[parameter].default;
            }
        }
        let inputs = this.Payload.inputs;
        if (inputs) {
            for (let parameterName in this.Payload.parameters) {
                let parameterValue = this.Payload.parameters[parameterName];
                if ((typeof parameterValue) == "string") {
                    let matcher = parameterValue.match(/^%%(.*)%%$/);
                    if (matcher) {
                        let expression = matcher[1];
                        let variablesDesc = this.IOs(this.Payload.parameters || {});
                        this.Payload.parameters[parameterName] = this.expandDynamicVariables(inputs, expression, variablesDesc.inputs);
                    }
                }
            }
        }
    };
    
    
    
    expandDynamicVariables(inputs, expression, variablesDesc) {
        let expectedInputsSpec = variablesDesc || {}; //this.parseDynamicVariables(expression, variablesDesc); //only explicit definitions
        let expectedInputs = Object.keys(expectedInputsSpec);
        let expandedExpression = expression;
        for (let i = 0; i < expectedInputs.length; i++) {
            let expectedInput = expectedInputs[i];
            let inputType = expectedInputsSpec[expectedInput].type;
            let inputValueRaw, inputValue;
            if (inputs[expectedInput]) {
                inputValueRaw = inputs[expectedInput];
            } else {
                inputValueRaw = expectedInputsSpec[expectedInput].default;
            }
            if (!inputType || inputType == "string") {
                inputValue = inputValueRaw.toString().replace(/\"/g, "\\\"");
            }
            if (!inputType || inputType == "numeric") {
                inputValue = "" + inputValueRaw;
            }
            if (inputType == "file") {
                inputValue = JSON.stringify(inputValueRaw.replace(/^temp:\/\//,"")); //legacy (replace acquire file)
            }
            expandedExpression = expandedExpression.replace(new RegExp("%" + expectedInput + "%","g"), inputValue);
        }
        return JSON.parse(expandedExpression);
    };
    
    async validateInputs(errors) {
        //let expectedInputsSpec = this.parseDynamicVariables(this.Payload.parameters.command_line_options, this.Payload.variables);
        let expectedInputsSpec = this.IOs(this.Payload.parameters).inputs;
        let inputs = this.Payload.inputs;
        let expectedInputs = Object.keys(expectedInputsSpec);
        if (!errors) {
            errors = {}
        }
        for (let i = 0; i < expectedInputs.length; i++) {
            let expectedInput = expectedInputs[i];
            if (inputs.hasOwnProperty(expectedInput)) {
                //TO DO - Validate type of input against spec
            } else {
                if (expectedInputsSpec[expectedInput].required) {
                    errors[expectedInput] = "Expected input not found: "+expectedInput;
                } else {
                    inputs[expectedInput] = expectedInputsSpec[expectedInput].default;
                }
            }
        }
        if (Object.keys(errors).length == 0) {
            for (let i in inputs) {
                try {
                    let inputSpec = expectedInputsSpec[i];
                    let input = inputs[i];
                    if ((input == null) && (inputSpec && !inputSpec.required)) {
                        continue; //null is acceptable if input was not required
                    }
                    if (input && ((typeof input) == "object") && (input.cluster_type)) {
                        
                        if (input.cluster_type == "round-robin") {
                            logger.Error("Round robin cluster type not implemented, using random mode instead");
                            input.cluster_type = "random";
                        }
                        if (input.cluster_type == "random") {
                            let random = (new Date()).getTime();
                            let value = input.cluster_values[ random % input.cluster_values.length];
                            //logger.Debug("Selecting "+ value);
                            inputs[i + ".cluster"] = input;
                            inputs[i] = value;
                            input = inputs[i];
                        }
                    }
                    if (inputSpec && (inputSpec.type == "numeric")) {
                        let inputType = (typeof input);
                        switch(inputType) {
                            case "string": {
                                if (input.match(/^[0-9\.]$/) || input.match(/^[0-9\.]+\/[0-9\.]+$/)) {
                                    input = eval(input);
                                }
                                inputs[i + ".original"] = inputs[i];
                                inputs[i] = input;
                                break;
                            }
                            case "number": {
                                break;
                            }
                            default: {
                                logger.Error("Invalid numeric type "+inputType + " for "+ i, input);                               
                            }
                        }                       
                    }
                    if (inputSpec && (inputSpec.type == "date")) {
                        if (input) {                            
                            let inputType = (typeof input);
                            switch(inputType) {
                                case "string": {
                                    input = new Date(input);
                                    break;
                                }
                                case "number": {
                                    input = new Date(input);
                                    break;
                                }
                                case "object": {
                                    input = new Date(input);
                                    break;
                                }
                                default: {
                                    logger.Error("Invalid date type "+inputType  + " for "+ i, input);                       
                                }
                            }   
                            inputs[i + ".original"] = inputs[i];
                            inputs[i] = input;   
                        }              
                    }
                    if (inputSpec && (inputSpec.type == "password")) {
                        if (input) { //missing inputs have been flagged already
                            let matcher = input.match(/^p__:(.*)/);
                            if (matcher) {
                                inputs[i + ".original"] = inputs[i];
                                inputs[i] = await this.Client.DecryptECIES({message: matcher[1]});
                                //logger.Debug("Decrypting password for " + i, inputs[i]);
                            } else {
                                if (!inputs[i + ".original"]) {
                                    //logger.Error("Warning - Unencrypted content for input " + i);
                                }
                            }
                        }
                    }
                } catch (err) {
                    logger.Error("Could not validate input " + i, err);
                }
            }
            
            return true;
        } else {
            this.Error("Inputs validation errors", errors);
            return false;
        }
    };
    
    async initializeActionClient() {
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
        return client;
    };
    
    static async InitializeArgs(actionClass, payload, client) {
        let action = new actionClass({client: client, payload: payload});
        action.JobId = payload.references && payload.references.job_id;
        action.StepId = payload.references && payload.references.step_id;
        actionObj.MaxAttempts = payload.max_attempts;
        actionObj.RetryDelay = payload.retry_delay;
        return action;
    };
    
    
    //outputs go into statusData.outputs, function return true if job is still running, false if it is terminated (successfully or otherwise)
    async MonitorExecution(pid, outputs) { //default is for continuous execution
        if (pid && !ElvOAction.PidRunning(pid)) {
            //check if status did not change
            let info = ElvOJob.GetStepInfoSync(this.JobId, this.StepId);
            if ((info.status_code >= ElvOAction.EXECUTION_ONGOING) && (info.status_code < ElvOAction.EXECUTION_FAILED)) {
                this.ReportProgress("PID not running", pid);
                return ElvOAction.EXECUTION_EXCEPTION;
            } else {
                return info.status_code;
            }
        } else {
            return ElvOAction.EXECUTION_ONGOING;
        }
    }
    
    
    async checkStatus() {
        let statusData = {
            status: {},
            outputs: {}
        };
        try {
            let executionCode = this.getExecutionCode();
            if (!executionCode) {
                let info = ElvOJob.GetStepInfoSync(this.JobId, this.StepId);
                let pid = info && info.pid;
                //let libraryId = await this.getLibraryId(jobId); //DO NOT REMOVE, Library can not be retrieved after MonitorExecution (uncatchable exception)
                
                executionCode = await this.MonitorExecution(pid, statusData.outputs);
                if (ElvOAction.isFinal(executionCode)) {
                    if (!this.IsContinuous() || (executionCode == ElvOAction.EXECUTION_EXCEPTION)) { //if continuous, exception indicate killed process without 436-up
                        if (executionCode == ElvOAction.EXECUTION_EXCEPTION) {
                            this.ReportProgress("MonitorExecution reported an exception");
                        }
                        executionCode = await this.wrapUpExecution(statusData.outputs, executionCode);
                    }
                    /*if (this.IsContinuous()) { //don't think we need that here and we would not have the right outputs anyway
                        if (this.JobId && this.StepId) {
                            statusData.outputs = info.outputs;
                        }
                        executionCode = await this.wrapUpExecution(statusData.outputs, executionCode);
                    }*/
                } else {
                    statusData.pid = pid;
                }
            } 
            statusData.status.code = executionCode || ElvOAction.EXECUTION_ONGOING;
            statusData.status.state = ElvOAction.EXECUTION_STATUSES[executionCode] || "ongoing";
            //if (statusData.status.code >= ElvOAction.EXECUTION_FAILED)) { //don't think we need that here
            //   statusData.outputs = ElvOJob.GetStepInfoSync(this.JobId, this.StepId).outputs;
            //}
            statusData.progress = this.progressMessage();
            
        } catch (err) {
            logger.Error("Error encountered while checking status", err);
            this.reportProgress("Error encountered while checking status", err);
        }
        return statusData;
    };
    
    
    getExecutionInformation() {
        let initializationMark = this.Tracker[ElvOAction.TRACKER_INITIATED];
        if (initializationMark) {
            let info = initializationMark.details.split(",")
            return {
                pid: info[0],
                step_id: ((info[1] != "undefined") && info[1]) || null,
                job_id: ((info[2] != "undefined") && info[2])
            };
        } else {
            return null;
        }
    }
    
    
    reportProgress(message, details, max_frequency) {
        try {
            if (max_frequency) {
                let now = (new Date()).getTime();
                if (this.LastProgressReport && ((this.LastProgressReport + max_frequency) > now)) {
                    return null;
                }
                this.LastProgressReport = now;
            }
            this.trackProgress(ElvOAction.TRACKER_INTERNAL, message, details);
        } catch (err) {
            logger.Error("Could not report progress message: " + message, err);
        }
    };
    
    ReportProgress(message, details) {
        //details could be parsed to ensure it fits
        this.trackProgress(ElvOAction.TRACKER_PROGRESS, message, details);
    };
    
    markExecuted(executionCode) {
        let finalState = ElvOAction.EXECUTION_STATUSES[executionCode];
        this.trackProgress(ElvOAction.TRACKER_EXECUTED, finalState, executionCode);
    };
    
    getExecutionCode() {
        let finalized = this.Tracker[ElvOAction.TRACKER_FINALIZED];
        if (finalized) {
            return parseInt(this.Tracker[ElvOAction.TRACKER_EXECUTED].details);
        }
        return null;
    };
    
    markFinalized() {
        this.trackProgress(ElvOAction.TRACKER_FINALIZED, "Status saved");
    }
    
    
    async wrapUpExecution(outputs, executionCode) {
        let jobId = this.JobId;
        let stepId = this.StepId;
        let maxAttempts = this.MaxAttempts;
        logger.Debug("Wrap-up step " + stepId + " (" + jobId + ")", {executionCode, attempts: this.Attempts});
        //if (executionCode < 0) { logger.Error("Wrap-up step " + stepId + " (" + jobId + ")", new Error("wrap-up stack"));}
        
        if (executionCode >= 0) {
            try {
                if (executionCode == ElvOAction.EXECUTION_COMPLETE_TO_BE_CLONED) {
                    let clonePath = ElvOJob.CloneJob(jobId, stepId);
                    if (clonePath) {
                        logger.Info("Cloned job for " + jobId + "/" + stepId + " re-queued to " + clonePath);
                        this.reportProgress("Cloned job for " + jobId + "/" + stepId + " re-queued to " + clonePath);
                    } else {
                        logger.Error("Error cloning or re-queueing " + jobId + "/" + stepId);
                    }
                    executionCode = ElvOAction.EXECUTION_COMPLETE;
                }
                
                this.markExecuted(executionCode);
                if (outputs) {
                    if (executionCode > 0) {
                        let persisted = false;
                        try {
                            let progress = ElvOAction.GetProgressMessage(this.JobId, this.StepId, this.Attempts);
                            persisted = ElvOJob.PersistStepOutputsSync(outputs, this.JobId, this.StepId, executionCode, this.Attempts, progress);
                        } catch (err) {
                            this.reportProgress("Failed attempt at persisting", err);
                            this.Error("Failed attempt at persisting", err);
                        }
                        if (!persisted) {
                            this.Error("Outputs were not persisted", outputs);
                            throw ("Failed to Persist outputs");
                        }
                    }
                } else {
                    try {
                        ElvOJob.MarkStepExecutedSync(jobId, stepId, (executionCode == 100));
                    } catch (err) {
                        this.reportProgress("Updating attempt failed");
                        this.Error("Updating status to " + executionCode + " failed for " + jobId + "/" + stepId, err);
                        throw "Updating attempt failed";
                    }
                }
                this.markFinalized();
            } catch (err) {
                logger.Error("Error wrapping up", err);
                this.reportProgress("Error wrapping up");
                executionCode = -1;
            }
        }
        if (executionCode < 0) {
            //logger.Debug("Should retry "+ jobId +"/" +stepId, {maxAttempts});
            let retries;
            if ((maxAttempts == 0) || (maxAttempts && (maxAttempts > 1))) {
                retries = this.Attempts;
                if ((maxAttempts == 0) || (retries < (maxAttempts - 1))) {
                    logger.Info("Max retries not reached for " + stepId, {retries, maxAttempts});
                    ElvOJob.MarkStepForRetrySync(jobId, stepId);
                    this.reportProgress("Marked for retry", retries);
                    return ElvOAction.EXECUTION_EXCEPTION_TO_BE_RETRIED;
                }
            }
            try {
                let progress = ElvOAction.GetProgressMessage(this.JobId, this.StepId, retries);
                ElvOJob.PersistStepOutputsSync(outputs, this.JobId, this.StepId, executionCode, retries, progress);
            } catch (err) {
                this.reportProgress("Failed attempt at persisting", err);
                this.Error("Failed attempt at persisting", err);
            }
            ElvOJob.MarkStepExceptionSync(jobId, stepId);
        }
        return executionCode;
    };
    
    static isFinal(executionCode) {
        let finalState = this.EXECUTION_STATUSES[executionCode];
        if (finalState) {
            return true;
        } else {
            return false;
        }
    };
    
    static async ExecuteSyncCmd(action, retry) {
        let errors = {};
        let jobId = action.JobId;
        let stepId = action.StepId;
        
        let maxAttempts = action.MaxAttempts;
        //logger.Debug("ExecuteSyncCmd maxAttempts: " + maxAttempts, process.argv);
        let data;
        if (retry) {
            data = ElvOJob.MarkStepInitiatedSync(jobId, stepId, action.PollingInterval(), process.pid, action.RetryDelay);
        } else {
            data = ElvOJob.GetStepInfoSync(jobId, stepId);
        }
        let attempts = data.attempts || 0; //there is a race condition as the attempt is written by the caller process after making this call asynchronously
        if (attempts > maxAttempts) {
            action.Error("Attempting to retry is beyond maximum", {attempts, maxAttempts});
            return await action.wrapUpExecution(null, ElvOAction.EXECUTION_EXCEPTION);
        } else {
            action.Attempts = attempts;
        }
        action.initializeTracker(true);
        /*if (!action.initializeTracker(true)) {
            action.Error("Duplicate process action execution");
            process.exit(1);
        }*/
        action.trackProgress(ElvOAction.TRACKER_INITIATED, "Started v"+action.Version(), process.pid + "," + stepId + "," + jobId);
        
        if (await action.validateInputs(errors)) {
            let outputs = {};
            let executionCode;
            try {
                executionCode = await action.Execute(action.Payload.inputs, outputs);
            } catch (err_exec) {
                action.Error("Execution exception", err_exec);
                action.reportProgress("Execution exception");
                return await action.wrapUpExecution(outputs, ElvOAction.EXECUTION_EXCEPTION);
            }
            if (this.isFinal(executionCode)) {
                return await action.wrapUpExecution(outputs, executionCode);
            } else {
                if (action.IsContinuous()) {
                    action.reportProgress("Unexpected status code", executionCode);
                    return await action.wrapUpExecution(outputs, ElvOAction.EXECUTION_EXCEPTION);
                }
            }
            return executionCode;
        } else {
            action.reportProgress("Invalid inputs");
            return await action.wrapUpExecution(null, ElvOAction.EXECUTION_EXCEPTION); //no point in retrying for invalid inputs
        }
    };
    
    
    static async ExecuteStandaloneCmd(action) {
        throw "need to be re-implemented";
    };
    
    
    static async SpecsCmd(action) {
        if (!action.Payload || Object.keys(action.Payload).length == 0) {
            return {description: action.Description(), parameters: action.Parameters().parameters};
        } else {
            if (!action.Payload.parameters) {
                action.Payload.parameters = {};
            }
            let paramSpec = action.Parameters().parameters;
            for (let parameter in paramSpec) {
                if (!action.Payload.parameters.hasOwnProperty(parameter) && (paramSpec[parameter].required == false)) {
                    action.Payload.parameters[parameter] = paramSpec[parameter].default;
                }
            }
            let spec = action.IOs(action.Payload.parameters);
            spec.polling_interval = action.PollingInterval();
            spec.iddle_timeout = action.IdleTimeout();
            return spec
        }
    };
    
    static instantiateAction(payload, client) {
        if (!ElvOAction.Actions) {
            ElvOAction.Actions = {};
        }
        let actionClassName = ElvOAction.ActionClassName(payload.action);
        if (!ElvOAction.Actions[actionClassName]) {
            ElvOAction.Actions[actionClassName] = require("./" + payload.action_type + "/action_" + payload.action);
        }
        let actionClass = ElvOAction.Actions[actionClassName];
        let action = new actionClass({client: client, payload: payload});
        return action;
    };
    
    static async Launch(payload, client) {
        let action = this.instantiateAction(payload, client);
        action.initializeTracker(); 
        ElvOJob.SaveStepPayloadSync(action.JobId, action.StepId, payload);
        
        let errors = {};
        if (await action.validateInputs(errors)) {
            let actionDir = action.action_type || "actions";
            let actionEnv = process.env; //{...process.env, "PAYLOAD": payloadStr, "PERSISTENCE": action.Persistence};
            if (action.MaxMemory()) {
                actionEnv.NODE_OPTIONS = "--max-old-space-size=" +action.MaxMemory();
            }
            const subprocess = spawn("nohup", ["node", actionDir+"/action_" + action.ActionId() + ".js", "execute-sync", "--job-id=" + action.JobId, "--step-id=" + action.StepId], {
                detached: true,
                stdio: 'ignore',
                env: actionEnv
            });
            let actionPid = subprocess.pid;
            if (ElvOProcess.Platform() != "linux") { //detaching does not seem to work on LINUX and dead processes tend to stay as Zombies
                subprocess.unref();
            } else {
                subprocess.on('exit', function () {
                    logger.Debug("action process exited", actionPid);
                })
            }
            let pollingInterval = payload.hasOwnProperty("polling_interval") ? payload.polling_interval : action.PollingInterval();
            ElvOJob.MarkStepInitiatedSync(action.JobId, action.StepId, pollingInterval, actionPid, action.RetryDelay);
            return {polling_interval: pollingInterval, pid: actionPid};
        } else {
            return {pid: null, errors: {input_validation: errors}};
        }
    };
    
    static async CheckStatus(jobId, stepId, attempts, client) {
        let payload = ElvOJob.RetrievePayloadSync(jobId, stepId);
        let action = this.instantiateAction(payload, client);
        action.Attempts = attempts;
        action.initializeTracker();
        await action.validateInputs(); //used to retrieve defaults and decrypt passwords
        let actionStatus = await action.checkStatus();
        if (payload.idle_timeout == null) {
            let actionIdleTimeout = action.IdleTimeout();
            if (actionIdleTimeout != null) {
                actionStatus.idle_timeout = actionIdleTimeout;
            } else {
                actionStatus.idle_timeout = ElvOAction.DEFAULT_IDLE_TIMEOUT;
            }
        } else {
            actionStatus.idle_timeout = payload.idle_timeout;
        }
        actionStatus.max_attempts = payload.max_attempts;
        actionStatus.retry_delay = payload.retry_delay;
        
        let idleTimeout = actionStatus.idle_timeout || ElvOAction.MAX_IDLE_TIMEOUT; // no null value, so negative is 0, which is considered no timeout (MAX_IDLE_TIMEOUT)
        if (actionStatus.status.code == ElvOAction.EXECUTION_ONGOING) {
            let foundTimestamp = (new Date(actionStatus.progress.time_stamp)).getTime();
            if ((foundTimestamp + (idleTimeout * 1000)) < (new Date()).getTime()) {
                logger.Info("Step has not reported any progresses  " + jobId + "/" + stepId + " after " + idleTimeout + " sec, killing process (" + actionStatus.pid + ") and marking for retry...");
                ElvOAction.Kill(actionStatus.pid);
                actionStatus.status.code = action.wrapUpExecution({}, ElvOAction.EXECUTION_EXCEPTION);
            }
        }
        
        let actionPid = actionStatus.pid;
        if (actionStatus.progress.message == "No tracking information yet") {
            if (!ElvOAction.ActionPid[jobId+stepId+actionPid]) {
                ElvOAction.ActionPid[jobId+stepId+actionPid] = {};
            }
            let stepCache = ElvOAction.ActionPid[jobId+stepId+actionPid];
            logger.Debug("No tracking information yet as of " + (stepCache.no_tracking_found || "now"));
            if (!stepCache.no_tracking_found) {
                if (actionPid && !ElvOAction.PidRunning(actionPid)) {
                    logger.Info("No tracking information available for " + jobId + "/" + stepId + " and process not found, marking for retry...");
                    actionStatus.status.code = action.wrapUpExecution({}, ElvOAction.EXECUTION_EXCEPTION_TO_BE_RETRIED);
                } else {
                    stepCache.no_tracking_found = actionStatus.progress.time_stamp;
                    logger.Debug("Setting No tracking information yet to " + stepCache.no_tracking_found);
                }
            } else {
                let previousTimestamp = (new Date(stepCache.no_tracking_found)).getTime();
                let foundTimestamp = (new Date(actionStatus.progress.time_stamp)).getTime();
                //logger.Debug("No tracking information yet - check", {previousTimestamp, foundTimestamp});
                if (foundTimestamp > (previousTimestamp + 60000)) {
                    if (actionPid && ElvOAction.PidRunning(actionPid)) {
                        logger.Info("No tracking information available for " + jobId + "/" + stepId + " after reasonable wait, killing process (" + actionPid + ") and marking for retry...");
                        ElvOAction.Kill(actionPid);
                    } else {
                        logger.Info("No tracking information available for " + jobId + "/" + stepId + " after reasonable wait, marking for retry...");
                    }
                    actionStatus.status.code = action.wrapUpExecution({}, ElvOAction.EXECUTION_EXCEPTION_TO_BE_RETRIED);
                }
            }
        }
        actionStatus.attempts = action.Attempts;
        return actionStatus;
    };
    
    static async CheckStatusCmd(action) {
        action.initializeTracker();
        let actionStatus = await action.checkStatus();
        if (actionStatus.status.code >= ElvOAction.EXECUTION_FAILED) {
            actionStatus.outputs = ElvOJob.GetStepInfoSync(action.JobId, action.StepId).outputs;
        }
        return actionStatus;
    };
    
    static ActionClassName(action) {
        let fullName = "action_" + action;
        return "ElvO" + fullName.replace(/_/g, " ").replace(/(?:^\w|[A-Z]|\b\w)/g, function (word) {
            return word.toUpperCase()
        }).replace(/\s+/g, '');
    };
    
    static executeCommandLine(actionClass) {
        let filename = "action_" + new actionClass({}).ActionId() + ".js";
        let runScript = process.argv[1].replace(/.*\//, '');
        return (filename == runScript);
    };
    
    static PidRunning(pid) {
        return ElvOProcess.PidRunning(pid);
    }
    
    static Kill(pid) {
        try {
            process.kill(pid, 9);
        } catch (e) {
            logger.Info("Killing process " + pid + " failed", e.toString());
        }
        return !ElvOProcess.PidRunning(pid);
    }
    
    getPersistedData(scope) {
        if (!scope) {
            scope = this.Payload.parameters.persistence_scope || ElvOAction.PERSISTENCE_GROUP;
        }
        let filePath = this.persistenceFilePath(scope);
        if (fs.existsSync(filePath)) {
            let rawData = fs.readFileSync(filePath, {encoding: 'utf8'});
            return JSON.parse(rawData);
        } else {
            return null;
        }
    };
    
    persistData(data, scope) {
        if (!scope) {
            scope = this.Payload.parameters.persistence_scope || ElvOAction.PERSISTENCE_GROUP;
        }
        let filePath = this.persistenceFilePath(scope);
        let existing = this.getPersistedData(scope) || {};
        let updatedData = Object.assign(existing, data);
        fs.writeFileSync(filePath, JSON.stringify(updatedData), {encoding: 'utf8'});
        return updatedData;
    };
    
    persistenceFilePath(scope) {
        if (!scope) {
            scope = this.Payload.parameters.persistence_scope || ElvOAction.PERSISTENCE_GROUP;
        }
        return ElvOAction.persistenceFilePath({
            scope,
            group_reference: this.Payload.references.group_reference,
            workflow_id: this.Payload.references.workflow_id,
            job_id: this.Payload.references.job_id,
            step_id: this.Payload.references.step_id,
            action_name: this.ActionId()
        });
    };
    
    static persistenceFilePath({scope, group_reference, workflow_id, job_id, step_id, action_name}) {
        if (!this.PersistenceRoot) {
            this.PersistenceRoot = "./Store";
            fs.mkdirSync(this.PersistenceRoot, {recursive: true});
        }
        if (scope == ElvOAction.PERSISTENCE_WORKFLOW) {
            return path.join(this.PersistenceRoot, "w_" + workflow_id + ".json")
        }
        if (scope == ElvOAction.PERSISTENCE_WORKFLOW_STEP) {
            return path.join(this.PersistenceRoot, "w_" + workflow_id + "_s_" + step_id + ".json");
        }
        if (scope == ElvOAction.PERSISTENCE_NONE) {
            return path.join(this.PersistenceRoot, "j_" + job_id + "_s_" + step_id + ".json");
        }
        if (scope == ElvOAction.PERSISTENCE_ABSOLUTE) {
            return path.join(this.PersistenceRoot, "A.json");
        }
        if (scope == ElvOAction.PERSISTENCE_GROUP) {
            let referenceGroupHex = ElvOJob.toHex(group_reference);
            return path.join(this.PersistenceRoot, "g_" + referenceGroupHex + "_s_" + step_id + ".json");
        }
        if (scope == ElvOAction.PERSISTENCE_ACTION) {
            return path.join(this.PersistenceRoot, "a_" + action_name + ".json");
        }
    };
    
    static GetSpec({file, actionId, force, parameters, actionType}) {
        if (!force && this.Actions && this.Actions[actionId] && !parameters) {
            return this.Actions[actionId];
        }
        if (!this.Actions) {
            this.Actions = {};
        }
        if (!file) {
            file = "action_" + actionId + ".js";
        }
        let actionDir = actionType || "actions";
        let args = [actionDir+"/"+file, "specs"];
        if (parameters) {
            args.push("--parameters="+JSON.stringify(parameters));
        }
        let proc = spawnSync("node", args, {encoding : "utf8"});
        if (proc.status == 0) {
            let spec = JSON.parse(proc.stdout);
            if (!parameters) {
                this.Actions[actionId] = spec
            }
            return spec;
        } else {
            logger.Error("Invalid action spec returned for " + actionId, {exit_code: proc.status});
            return null;
        }
    };
    
    static List(force) {
        if (force || !this.Actions) {
            this.Actions = {};
        }
        fs.readdirSync("./actions").forEach(file => {
            let matcher = file.match(/^action_(.*).js$/);
            if (matcher) {
                this.GetSpec({file, actionId: matcher[1], force})
            } else {
                logger.Error("Invalid action file found ", file);
            }
        });
        return this.Actions;
    };
    
    areEqual(a,b) {
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
            if (!(aKeys[i] in b) || !this.areEqual(a[aKeys[i]], b[aKeys[i]])) {
                return false;
            }
        }
        return true;
    };
    
    
    static async Run(actionClass) {
        let result;
        try {
            logger.Set(actionClass.name);
            let command = process.argv[2];
            //action.report("Command: " + command);
            if (command == "specs") {
                let payloadStr = ElvOProcess.getValueInArg("payload", "PAYLOAD") || "{}";
                let payload = JSON.parse(payloadStr);
                let parameters = ElvOProcess.getValueInArgv("parameters");
                if (parameters) {
                    payload.parameters = JSON.parse(parameters);
                }
                let action = new actionClass({payload});
                result = await this.SpecsCmd(action);
            }
            if (command == "execute-sync") {
                let jobId = ElvOProcess.getValueInArg("job-id", "JOB_ID");
                let stepId = ElvOProcess.getValueInArg("step-id", "STEP_ID");
                let payload = ElvOJob.RetrievePayloadSync(jobId, stepId);
                let configUrl = ElvOProcess.getValueInArg("config-url", "CONFIG_URL", this.PROD_CONFIG_URL);
                const privateKey = ElvOProcess.getValueInArg("private-key", "PRIVATE_KEY");
                let client = (privateKey) ? await ElvOFabricClient.InitializeClient(configUrl, privateKey) : null;
                let action = new actionClass({payload, client});
                let retry = false;
                result = await this.ExecuteSyncCmd(action, retry);
                process.exit((result == ElvOAction.EXECUTION_EXCEPTION_COMPLETE) ? 0 : result);
            }
            
            if (command == "launch") {
                let payloadStr = ElvOProcess.getValueInArgv("payload") || "{}";
                let payload;
                let matcher = payloadStr.match(/^@(.*)/);
                if (matcher){
                    payload = JSON.parse(fs.readFileSync(matcher[1],"utf8"));
                } else {
                    payload = JSON.parse(payloadStr);
                }
                let inputs = ElvOProcess.getValueInArgv("inputs");
                if (inputs) {
                    payload.inputs = JSON.parse(inputs);
                }
                let parameters = ElvOProcess.getValueInArgv("parameters");
                if (parameters) {
                    payload.parameters = JSON.parse(parameters);
                }
                if (!payload.parameters) {
                    payload.parameters = {};
                }
                if (!payload.references) {
                    let now = (new Date()).getTime();
                    payload.references = {job_id: "-", step_id: actionClass.name + "_"+now};
                }
                let payloadFilePath = ElvOJob.SaveStepPayloadSync(payload.references.job_id, payload.references.step_id, payload);
                let configUrl = ElvOProcess.getValueInArg("config-url", "CONFIG_URL", this.PROD_CONFIG_URL);
                const privateKey = ElvOProcess.getValueInArg("private-key", "PRIVATE_KEY");
                let client = await ElvOFabricClient.InitializeClient(configUrl, privateKey);
                let action = new actionClass({payload, client});
                action.TrackerPath = "/tmp/"+ payload.references.step_id +".log";
                let retry = false;
                result = await this.ExecuteSyncCmd(action, retry);
                console.error({payload, execution_code: result, log_path: action.TrackerPath, result_path: "/tmp/"+payload.references.step_id + ".json", payload_path: payloadFilePath});
                console.error("\n");
            }
            if (command == "check-status") {
                let payloadStr = ElvOProcess.getValueInArgv("payload") || "{}";
                let payload;
                let matcher = payloadStr.match(/^@(.*)/);
                if (matcher) {
                    try {
                        payload = JSON.parse(fs.readFileSync(matcher[1], "utf8"));
                    } catch(errPayload) {
                        console.error("Invalid payload",errPayload)
                        process.exit(1);
                    }
                } else {
                    payload = JSON.parse(payloadStr);
                }
                console.error("read payload", payload);
                let configUrl = ElvOProcess.getValueInArg("config-url", "CONFIG_URL", this.PROD_CONFIG_URL);
                const privateKey = ElvOProcess.getValueInArg("private-key", "PRIVATE_KEY");
                let client = await ElvOFabricClient.InitializeClient(configUrl, privateKey);
                let action = new actionClass({payload, client});
                action.Attempts = 0;
                action.TrackerPath = "/tmp/" + payload.references.step_id + ".log";
                
                result = await this.CheckStatusCmd(action);
            }
            
            if (command == "execute") {
                let payloadStr = ElvOProcess.getValueInArgv("payload") || "{}";
                
                let payload;
                let matcher = payloadStr.match(/^@(.*)/);
                if (matcher){
                    payload = JSON.parse(fs.readFileSync(matcher[1],"utf8"));
                } else {
                    payload = JSON.parse(payloadStr);
                }
                let inputs = ElvOProcess.getValueInArgv("inputs");
                if (inputs) {
                    payload.inputs = JSON.parse(inputs);
                }
                let parameters = ElvOProcess.getValueInArgv("parameters");
                if (parameters) {
                    payload.parameters = JSON.parse(parameters);
                }
                if (!payload.parameters) {
                    payload.parameters = {};
                }
                if (!payload.references) {
                    let now = (new Date()).getTime();
                    payload.references = {job_id: "-", step_id: actionClass.name + "_"+now};
                }
                let payloadFilePath = ElvOJob.SaveStepPayloadSync(payload.references.job_id, payload.references.step_id, payload);
                let configUrl = ElvOProcess.getValueInArg("config-url", "CONFIG_URL", this.PROD_CONFIG_URL);
                const privateKey = ElvOProcess.getValueInArg("private-key", "PRIVATE_KEY");
                let client = (configUrl && privateKey) && (await ElvOFabricClient.InitializeClient(configUrl, privateKey));
                let action = new actionClass({payload, client});
                action.TrackerPath = "/tmp/"+ payload.references.step_id +".log";
                ElvOAction.TrackerPath = action.TrackerPath;
                let retry = false;
                result = await this.ExecuteSyncCmd(action, retry);
                if ((result >=0) && (result < 99)) {
                    let ongoing = true;
                    action.Attempts = 0;
                    let pollingInterval = parseInt(ElvOProcess.getValueInArgv("polling-interval") || 0) || action.PollingInterval() || 60;
                    console.error({log_path: action.TrackerPath, polling_interval: pollingInterval});
                    let resultCheck;
                    while (ongoing) {
                        await ElvOProcess.Sleep(pollingInterval * 1000);
                        resultCheck = await this.CheckStatusCmd(action);
                        //console.error(resultCheck);
                        console.error("");
                        result = resultCheck.status.code;
                        ongoing = ((result >=0) && (result < 99));
                    }
                }
                let resultPath = "/tmp/"+payload.references.step_id + ".json";
                console.error({payload, execution_code: result, log_path: action.TrackerPath, result_path: resultPath, payload_path: payloadFilePath});
                if (ElvOProcess.isPresentInArgv("result")) {
                    console.error("\nresult:\n");
                    console.log(fs.readFileSync(resultPath).toString());
                }
                if (ElvOProcess.isPresentInArgv("log")) {
                    console.error("\log:\n");
                    console.log(fs.readFileSync(action.TrackerPath).toString());
                }
            }
            
        } catch (err) {
            logger.Error("Top level action execution error", err);
            result = {ERROR: err};
        }
        //process.env["ACTION_RESULT"] = result;
        
        if (ElvOProcess.isPresentInArgv("result")) {
            process.stderr.write(JSON.stringify(result), () => {
                //console.error('The data has been flushed');
                console.error("\n");
                process.exit(0);
            });
        } else {
            process.stdout.write(JSON.stringify(result), () => {
                //console.error('The data has been flushed');
                console.error("\n");
                process.exit(0);
            });
        };
        return true;
    }
    
    static ActionPid = {};
    
    static PERSISTENCE_WORKFLOW = "workflow";
    static PERSISTENCE_WORKFLOW_STEP = "workflow-step";
    static PERSISTENCE_NONE = "none";
    static PERSISTENCE_ACTION = "action";
    static PERSISTENCE_GROUP = "group";
    static PERSISTENCE_ABSOLUTE = "absolute";
    static PERSISTENCE_SCOPES = [ElvOAction.PERSISTENCE_WORKFLOW, ElvOAction.PERSISTENCE_WORKFLOW_STEP, ElvOAction.PERSISTENCE_NONE, ElvOAction.PERSISTENCE_ACTION, ElvOAction.PERSISTENCE_GROUP, ElvOAction.PERSISTENCE_ABSOLUTE];
    
    static TRACKER_INITIATED = 10;
    static TRACKER_PROGRESS = 50;
    static TRACKER_EXECUTED = 80;
    static TRACKER_FINALIZED = 100;
    static TRACKER_INTERNAL = 20;
    static TRACKER_ERROR_STACK = 25;
    
    static EXECUTION_ONGOING = 10;
    static EXECUTION_EXCEPTION = -1;
    static EXECUTION_FATAL_EXCEPTION = -2; //Non retryable - not supported yet
    static EXECUTION_FAILED = 99;
    static EXECUTION_COMPLETE = 100;
    static EXECUTION_EXCEPTION_TO_BE_RETRIED = -10;
    static EXECUTION_COMPLETE_TO_BE_CLONED = 110; //For triggers - not supported yet
    
    static EXECUTION_STATUSES = {'-1': "Exception", '-5': "Canceled", 99: "Failed", 100: "Complete", 110: "Triggered"};
    static STATUS_LABEL = {
        10: "Started",
        '-10': "To be retried",
        '-1': "Exception",
        '-5': "Canceled",
        99: "Failed",
        100: "Complete",
        110: "Triggered",
        1: "Initialized"
    };
    
    static PROD_CONFIG_URL = "https://main.net955305.contentfabric.io/config";
    static DEFAULT_IDLE_TIMEOUT = 60; //in seconds
    static MAX_IDLE_TIMEOUT = 7 * 24 * 3600; //A week in seconds
    static DEFAULT_RETRY_DELAY = 15; //in seconds
    
    static VERSION = "0.0.1";
};

if ((typeof exports) != "undefined") {
    exports.ElvOAction = ElvOAction;
}
