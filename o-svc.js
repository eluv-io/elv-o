const logger = require('./o-logger');
const ElvOQueue = require("./o-queue");
const ElvOProcess = require("./o-process");
const ElvO = require("./o-core");
const http = require('http');
const fs = require("fs");
const path = require('path');
const ElvOJob = require("./o-job.js");
const ElvOFabricClient = require("./o-fabric");
const ElvOAction = require("./o-action").ElvOAction;

class ElvOSvc {
    
    static report(...msg) {
        if (this.Verbose) {
            console.error(...msg);
        }
    };
    
    
    static async ApiListener(request, response) {
        try {
            ElvOSvc.ReqNumber++;
            let {headers, method, url} = request;
            let body = [];
            request.on('error', (err) => {
                logger.Error("ApiListener request error", err);
                ElvOSvc.report(err);
            }).on('data', (chunk) => {
                body.push(chunk);
            }).on('end', async () => {
                try {
                    body = Buffer.concat(body).toString();
                    
                    response.on('error', (err) => {
                        logger.Error("ApiListener response error", err);
                        this.report(err);
                    });
                    let payload;
                    if (body) {
                        payload = JSON.parse(body)
                    } else {
                        payload = {};
                        let matcher = url.match(/(.*)(\?.*)/);
                        if (matcher) {
                            let urlParams =  new URLSearchParams(matcher[2]);
                            url = matcher[1];
                            for (let p of urlParams.keys()) {
                                payload[p] = urlParams.get(p);
                            }
                        }
                    }
                    
                    let responseData = await ElvOSvc.processApiRequest(payload, headers, method, url);
                    response.statusCode = responseData.status_code;
                    response.setHeader('Content-Type', 'application/json');
                    let rawHost = headers.host.replace(/:[0-9]+$/, "");
                    let rawOrigin = headers.origin && headers.origin.replace(/:[0-9]+$/, "").replace(/http[s]*:\/\//,"");
                    if ( (request.headers.origin && request.headers.origin.match(/contentfabric.io$/))
                    || (rawHost == rawOrigin) ) { 
                        response.setHeader('Access-Control-Allow-Origin', request.headers.origin);
                        response.setHeader('Access-Control-Allow-Methods', "GET, POST, PATCH, PUT, DELETE, OPTIONS");
                        response.setHeader('Access-Control-Allow-Headers', "*");
                    }
                    response.write(JSON.stringify(responseData.body));
                    response.end();
                } catch(errProcessing) {
                    logger.Error("Error processing API request", errProcessing);
                }
            });
        } catch(lstErr) {
            logger.Error("ApiListener error", lstErr);
        }
    };
    
    static async processApiRequest(body, headers, method, url) {
        logger.Debug("processApiRequest: ",body);
        let response={};
        try {
            logger.Info("url", url);
            if (method == "OPTIONS") {
                return {status_code: 200, body: {timestamp: (new Date().getTime())}};
            }
            if (url == "/") {
                if (body.action) {
                    url = "/" + body.action;
                } else {
                    return {
                        status_code: 200, body: {
                            commands: {
                                queue_job: {
                                    arguments: {
                                        queue_id: {type: "string", required: true}
                                    },
                                    priority: {type: "numeric", required: false, range: {min: 0, max: 9999}, default: 100},
                                    job_reference: {type: "string", required: true},
                                    job_description: {type: "object", required: true}
                                },
                                job_status: {
                                    arguments: {
                                        job_reference: {type: "string", required: true},
                                        details: {type: "boolean", required: false}
                                    }
                                },
                                cancel_job: {
                                    note: "to cancel a running job, a job_id or job_reference must be provided. To cancel a queued job, the queue_id and path must be provided",
                                    arguments: {
                                        job_id: {type: "string", required: false},
                                        job_reference: {type: "string", required: false},
                                        queue_id: {type: "string", required: false},
                                        path: {type: "string", required: false}
                                    }
                                },
                                restart_job: {
                                    note: "to restart a job, a job_id or job_reference must be provided",
                                    arguments: {
                                        job_id: {type: "string", required: false},
                                        job_reference: {type: "string", required: false},
                                        step_id: {type: "string", required: true}
                                    }
                                },
                                create_jobs_queue: {
                                    arguments: {
                                        queue_id: {type: "string", required: true},
                                        queue_name: {type: "string", required: false},
                                        queue_priority: {type: "numeric", required: false, range: {min: 0, max: 9999}, default: 100},
                                        queue_active: {type: "boolean", required: false, default: true}
                                    }
                                },
                                activate_jobs_queue: {arguments: {queue_id: {type: "string", required: true}}},
                                deactivate_jobs_queue: {arguments: {queue_id: {type: "string", required: true}}},
                                list_queued_items: {
                                    arguments: {
                                        queue_ids: {type: "array", required: false},
                                        limit: {type: "numeric", required: false, default: 0}
                                    }
                                },
                                get_queued_item_details: {
                                    arguments: {
                                        queue_id: {type: "string", required: true},
                                        path: {type: "string", required: true}
                                    }
                                },
                                list_running_jobs: {arguments: {}},
                                list_executed_jobs: {
                                    arguments: {
                                        limit: {type: "numeric", required: false, default: 0},
                                        from_date: {type: "date", required: false, default: null},
                                        to_date: {type: "date", required: false, default: null},
                                        status_code: {type: "numeric", required: false, default: null}
                                    }
                                },
                                clear_job_reference: {arguments: {job_reference: {type: "string", required: true}}},
                                execute_action: {
                                    arguments: {
                                        synchronous: {type: "boolean", required: false, default: true},
                                        action: {type: "string", required: true},
                                        parameters: {type: "object", required: true},
                                        inputs: {type: "object", required: true}
                                    }
                                },
                                action : {
                                    arguments: {
                                        action: {type: "string", required: false},
                                        parameters: {type: "object", required: false},
                                        inputs: {type: "object", required: false}
                                    }
                                },
                                workflow_io: {
                                    arguments: {
                                        workflow_id: {type: "string", required: true}
                                    }
                                },
                                get_throttles: {
                                    arguments: {
                                        workflow_id: {type: "string", required: false}
                                    }
                                }
                            }
                        }
                    };
                }
            }
            if (url == "/heartbeat")  {
                return {status_code: 200, body: {timestamp: (new Date().getTime())}};
            }
            if (!await this.validateAPIKey(headers, url)) {
                logger.Info("invalid API key provided");
                return {status_code: 401, body: {error: "Invalid API Key"}};
            }
            if (url.match(/\/queue_job/))  {
                return await this.queueJobApiRequest(body, headers, method, url);
            }
            if (url == "/create_jobs_queue")  {
                return await this.createJobsQueueApiRequest(body, headers, method, url);
            }
            if (url == "/job_status")  {
                return await this.jobStatusApiRequest(body, headers, method, url);
            }
            if (url == "/activate_jobs_queue") {
                return await this.activateJobsQueueApiRequest(body, headers, method, url);
            }
            if (url == "/deactivate_jobs_queue") {
                return await this.deactivateJobsQueueApiRequest(body, headers, method, url);
            }
            if (url == "/list_queued_items") {
                return await this.listQueuedItemsApiRequest(body, headers, method, url);
            } 
            if (url == "/get_queued_job_details") {
                return await this.getQueuedJobDetailsApiRequest(body, headers, method, url);
            } 
            if (url == "/list_running_jobs") {
                return await this.listRunningJobsApiRequest(body, headers, method, url);
            }
            if (url == "/list_executed_jobs") {
                return await this.listExecutedJobsApiRequest(body, headers, method, url);
            }
            if (url == "/get_job_execution_steps") {
                return await this.getJobExecutionStepsApiRequest(body, headers, method, url);
            }
            if (url == "/cancel_job") {
                return await this.cancelJobApiRequest(body, headers, method, url);
            }
            if (url == "/restart_job") {
                return await this.restartJobApiRequest(body, headers, method, url);
            }
            if (url == "/clear_job_reference") {
                return await this.ClearJobReferenceApiRequest(body, headers, method, url);
            }
            if (url == "/execute_action") {
                return await this.ExecuteActionApiRequest(body, headers, method, url);
            }
            if (url == "/refresh_authorizations") {
                return await this.RefreshAuthorizedAddressApiRequest(body, headers, method, url);
            }
            if (url == "/action") {
                return await this.ActionApiRequest(body, headers, method, url);
            }
            if (url == "/workflow_io") {
                return await this.WorkflowIOsApiRequest(body, headers, method, url);
            }
            if (url == "/get_throttles") {
                return await this.GetThrottlesApiRequest(body, headers, method, url);
            }
            response = {body, headers, method, url};
        } catch(err) {
            logger.Error("Process API request error", err);
            return {status_code: 500, body: {"error": err}};
        }
        return {status_code: 200, body: response};
    };
    
    static async validateAPIKey(headers, url) {
        let apiKey = headers["api-key"];
        let clientAddress = headers["client-address"];
        if (apiKey && apiKey.match(/^api_/)) {
            let token = apiKey.replace(/^api_/,"");
            let tokenData;
            try {                
                tokenData = this.O.Client.utils.DecodeAuthorizationToken(token);
                if  (tokenData && (this.O.Client.ContentSpaceId() == tokenData.qspace_id)) {
                    clientAddress = tokenData.addr;
                } else {
                    return false;
                }
            }  catch(errTok) {
                logger.Error("Could not decrypt API key token", errTok);
                return false;
            }
        } else {            
            //Legacy keys
            if (!apiKey) {
                logger.Error("Missing api-key");
                return false;
            }
            try {
                let clearMsg = await this.O.Client.DecryptECIES({message: apiKey});
                let data = JSON.parse(clearMsg);
                if (clientAddress && (data.client != clientAddress)) {
                    logger.Error("Mismatched key and client address");
                    return false;
                }
                clientAddress = data.client;
                let now = (new Date()).getTime();
                if (now < data.timestamp) { //We could force an expiration instead by testing how old the key is
                    logger.Error("Invalid key timestamp");
                    return false;
                }
            } catch(err) {
                logger.Error("Error validating key", err);
                return false;
            }
        }
        let authProfile = this.O.Whitelist && this.O.Whitelist[clientAddress];
        if (!authProfile) {
            logger.Error("Unauthorized client address");
            return false;
        }
        for (let authorizedUrl of authProfile ) {
            if (url.match(authorizedUrl)) {
                return true;
            }
        }
        logger.Error("Client address not authorized for " + url);
        return false;
    };
    
    static async createJobsQueueApiRequest(body, headers, method, url) {
        let queueId = body.queue_id;
        let priority = body.queue_priority;
        let name = body.queue_name;
        let active = (body.queue_active != false); //default is active
        let queues = ElvOQueue.Create(queueId, priority, active, name);
        if (queues) {
            return {status_code: 200, body: {queues: queues}};
        } else {
            return {status_code: 500, body: {error: "Could not create queue", queue_id: queueId}};
        }
    };
    
    static async activateJobsQueueApiRequest(body, headers, method, url) {
        let queueId = body.queue_id;
        let queues = ElvOQueue.Activate(queueId, true);
        if (queues) {
            return {status_code:200, body: {queues: queues}};
        } else {
            return {status_code: 500, body: {error:"Could not activate queue",queue_id:queueId}};
        }
    };
    
    static async deactivateJobsQueueApiRequest(body, headers, method, url) {
        let queueId = body.queue_id;
        let queues = ElvOQueue.Activate(queueId, false);
        if (queues) {
            return {status_code:200, body: {queues: queues}};
        } else {
            return {status_code: 500, body: {error:"Could not deactivate queue",queue_id:queueId}};
        }
    };
    
    static async queueJobApiRequest(body, headers, method, url) {
        logger.Debug("body", body);
        try {
            let newAPI = false;
            let urlElements = url.split(/[\/\?]/);
            if (urlElements.length >2) {
                newAPI = true;
                if(!body.job_description) {
                    body.job_description  = {};
                }
                body.job_description.workflow_id = urlElements[2];
                if (body.job_parameters) {
                    body.job_description.parameters = body.job_parameters;
                }
            }
            let queueId = body.queue_id;
            let priority = body.priority || 100;
            let itemId = body.job_reference || (body.job_description.parameters && body.job_description.parameters.job_reference) || body.job_description.id;
            if (!itemId) {
                itemId = "job_"+(new Date()).getTime();
                logger.Info("Job-reference not provided, using generated one", itemId);
            }
            if (!body.job_description.id) {
                body.job_description.id = itemId;
            }
            if (!body.job_description.workflow_id) {
                body.job_description.workflow_id = body.job_description.workflow_object_id;
            }
            
            let jobInfo = ElvOJob.GetJobInfoSync({jobRef: itemId, silent: true});
            if (jobInfo) {
                if (!status_code.status_code || ((status_code.status_code > 0) &&  (status_code.status_code  < 99))) {
                    logger.Error("Job reference "+ itemId + " not unique", jobInfo.job_id);
                    return {status_code: 400, body: {error: "Job reference not unique", item_id: itemId}};
                } else {
                    ElvOJob.ArchiveStepFiles(itemId); 
                }
            }
            let pathInQueue = ElvOQueue.Queue(queueId, body.job_description, priority);
            
            if (pathInQueue) {
                let response;
                if (!newAPI) {
                    
                    response = {
                        path: pathInQueue,
                        item: body.job_description,
                        queue_id: queueId,
                        queued: true
                    };
                } else {
                    response = {
                        queue_id: queueId, job_reference: itemId, queued: true
                    };
                }
                return {status_code: 200, body: response};
            } else {
                if (!newAPI) {
                    return {status_code: 400, body: {error: "Could not queue item", queue_id: queueId, item_id: itemId, queued: false}};
                } else {
                    return {status_code: 400, body: {error: "Could not queue item", queue_id: queueId, job_reference: itemId, queued: false}};
                }
            }
        } catch(err) {
            logger.Error("QueueJobApiRequest error", err);
            return {status_code: 500, body: {error: err}};
        }
    };
    
    
    
    /*
    static async jobStatusApiRequestOld(body, headers, method, url) {
        try{
            let jobData =  ElvOJob.GetJobInfoSync({jobRef: body.job_reference, jobId: body.job_id});
            let jobStatus = ElvO.JOB_STATUSES[(jobData && jobData.workflow_execution.status_code) || 0];
            if (!jobData) {
                return {
                    status_code: 200,
                    body: {
                        job_id: null,
                        status: jobStatus
                    }
                }
            }
            
            let stepsExecuted = jobData.workflow_execution.steps;
            let response;
            let jobId = jobData.workflow_execution.job_id;
            if (!body.details) {
                response = {
                    job_id: jobId,
                    status: jobStatus, //unknown|queued|created|ongoing|complete|exception|failed
                    status_code: jobData.workflow_execution.status_code,
                    status_details: {
                        steps: stepsExecuted
                    } 
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
                    status: jobStatus, // unknown|queued|created|ongoing|complete|exception|failed
                    status_code: jobData.workflow_execution.status_code,
                    status_details: {steps: stepStatuses}, 
                };
                if (workflowProgress.time_stamp) {
                    response.progress = workflowProgress;
                }
            }
            
            return {status_code: 200, body: response};
        } catch(err) {
            logger.Error("Job status API request error", err);
            return {status_code: 500, body: {error: err}};
        }
    };
    */
    
    static async jobStatusApiRequest(body, headers, method, url) {
        try{
            let response = await ElvO.GetJobStatus({jobRef: body.job_reference, jobId: body.job_id, details: body.details})
            if (!response) {
                return {
                    status_code: 200,
                    body: {
                        job_id: null,
                        status: 0
                    }
                }
            }                     
            return {status_code: 200, body: response};
        } catch(err) {
            logger.Error("Job status API request error", err);
            return {status_code: 500, body: {error: err}};
        }
    };
    
    static async listQueuedItemsApiRequest(body, headers, method, url) {
        let queueIds = body.queue_ids;
        let limit = parseInt(body.limit || 0);
        let items = ElvOQueue.Queued(queueIds, limit);
        return {status_code: 200, body: items};
    };
    
    static async getQueuedJobDetailsApiRequest(body, headers, method, url) {
        let queueId = body.queue_id;
        let queuedPath = body.path;
        let item = ElvOQueue.Item(queuedPath, queueId);
        if (item) {
            return {status_code: 200, body: item};
        } else {
            return {status_code: 204, body: null};
        }
    };
    
    static async listRunningJobsApiRequest(body, headers, method, url) {
        try {
            let jobIds = ElvOJob.GetRunningJobsDataSync();
            let jobs = [];
            for (let jobId of jobIds) {
                try {
                    let job = ElvOJob.GetJobInfoSync({jobId});
                    job.job_reference = job.workflow_execution.reference;
                    jobs.push(job);
                } catch(errJob) {
                    logger.Error("Could not retrieve job information for "+jobId,errJob);
                }
            }
            return {status_code: 200, body: jobs};
        } catch(err) {
            logger.Error("List running jobs API request error", err);
            return {status_code: 400, body: {error: err}};
        }
    };
    
    
    static async listExecutedJobsApiRequest(body, headers, method, url) {
        let workflowId = body.workflow_id;
        let groupId = body.group_id;
        let minDate = body.from_date;
        let maxDate = body.to_date;
        let limit = body.limit;
        try {
            let jobPaths = ElvOJob.ListExecutedJobs({workflowId, groupId, minDate, maxDate, limit});
            let jobs = [];
            for (let jobPath of jobPaths) {
                let jobId = path.basename(jobPath);
                try {
                    let job = ElvOJob.GetJobInfoSync({jobId});
                    job.job_reference = job.workflow_execution.reference;
                    jobs.push(job.workflow_execution);
                } catch(errJob) {
                    logger.Error("Could not retrieve job information for "+jobId,errJob);
                }
            }
            return {status_code: 200, body: jobs};
        } catch(err) {
            logger.Error("List running jobs API request error", err);
            return {status_code: 400, body: {error: err}};
        }
    };
    
    static async getJobExecutionStepsApiRequest(body, headers, method, url) {
        let jobId = body.job_id;
        try {
            let steps = ElvOJob.GetExecutionStepsData(jobId);
            return {status_code: 200, body: steps};
        } catch(err) {
            logger.Error("Get execution steps API request error", err);
            return {status_code: 400, body: {error: err}};
        }
    };
    
    
    static async cancelJobApiRequest(body, headers, method, url) {
        let jobRef = body.job_reference;
        let jobId = body.job_id;
        let queueId = body.queue_id;
        let queuedPath = body.path;
        try {
            let response;
            if (jobRef || jobId) {
                response = ElvOJob.CancelJob({jobId, jobRef});
            } else {
                response = (ElvOQueue.Pop(queueId, queuedPath, "canceled") != null);
            }
            return {status_code: 200, body: {canceled: response}};
        } catch(err) {
            logger.Error("Cancel job API request error", err);
            return {status_code: 400, body: {error: err}};
        }
    };
    
    static async restartJobApiRequest(body, headers, method, url) {
        let jobRef = body.job_reference;
        let jobId = body.job_id;
        let stepId = body.step_id;
        try {
            let reponse = ElvOJob.RestartFrom({jobId, jobRef, stepId});
            return {status_code: 200, body: {restarted: reponse}};
        } catch(err) {
            logger.Error("Restart job API request error", err);
            return {status_code: 500, body: {error: err}};
        }
    };
    
    
    static async ClearJobReferenceApiRequest(body, headers, method, url) {
        let reference = body.job_reference;
        try {
            let cleared = await ElvOJob.ClearJob({jobRef : reference});
            return {status_code: 200, body: {cleared: cleared}};
        } catch(err) {
            logger.Error("Clear job-reference API request error", err);
            return {status_code: 500, body: {error: err}};
        }
    };
    
    static async ActionApiRequest(body, headers, method, url) {
        try {
            if (!body.action) { //list all actions
                let actions = ElvOAction.List(body.force);
                return {status_code: 200, body: {actions: actions}};
            }
            let spec = ElvOAction.GetSpec({actionId: body.action, force: body.force, parameters: body.parameters});
            return {status_code: 200, body: spec};
        } catch(err) {
            logger.Error("Action API request error", err);
            return {status_code: 500, body: {error: err}};
        }
    };
    
    static async WorkflowIOsApiRequest(body, headers, method, url) {
        try {
            logger.Debug("body.workflow_id", body.workflow_id);
            let workflowDefinition;
            if (body.workflow_object_id) {
                workflowDefinition = await this.O.getMetadata({objectId: body.workflow_id, metadataSubtree: "workflow_definition"});
            } else {
                workflowDefinition = await this.O.GetWorkflowDefinition(workflowId, body.force); 
            }
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
            return {status_code: 200, body: {inputs, outputs}};
        } catch(err) {
            logger.Error("Workflow IOs API request error", err);
            return {status_code: 500, body: {error: err}};
        }
    };
    
    static async GetThrottlesApiRequest(body, headers, method, url) {
        try {           
            let throttles = await this.O.RetrieveThrottles(body.force);
            if (body.workflow_id) {
                let response = {};
                if (throttles && throttles[body.workflow_id]) {
                    response[body.workflow_id] = ((typeof throttles[body.workflow_id]) == "object") ? throttles[body.workflow_id].limit :  throttles[body.workflow_id]
                } else {                   
                    response[body.workflow_id] = 0
                    return   {status_code: 200, body: response};
                }
            }
            return {status_code: 200, body: throttles};
        } catch(err) {
            logger.Error("Workflow IOs API request error", err);
            return {status_code: 500, body: {error: err}};
        }
    };
    
    static async ExecuteActionApiRequest(body, headers, method, url) {
        try {
            let payload = {parameters: body.parameters, inputs: body.inputs, variables: body.variables, action: body.action};
            if (body.synchronous != false) {
                if (!payload.references) {
                    let now = (new Date()).getTime();
                    payload.references = {job_id: "-", step_id: payload.action + "_"+now};
                }
                //let payloadFilePath = ElvOJob.SaveStepPayloadSync(payload.references.job_id, payload.references.step_id, payload);
                let action = ElvOAction.instantiateAction(payload, this.O.Client);
                action.TrackerPath = "/tmp/"+ payload.references.step_id +".log";
                let result;
                try {
                    result = await this.ExecuteSyncCmd(action, false);
                    if (result >= 99) {
                        let results = JSON.parse(fs.readFileSync("/tmp/" + payload.references.step_id + ".json", "utf8"));
                        return {status_code: 200, body: results};
                    } else {
                        return {
                            status_code: 400, body: {
                                execution_code: result,
                                log_path: action.TrackerPath,
                                result_path: "/tmp/" + payload.references.step_id + ".json",
                            }
                        };
                    }
                } catch(eEx) {
                    return {status_code: 500, body: {execution_code: (result || "-"), error: eEx}};
                }
                
            } else {
                logger.Error("Execute Action Asynchronous not implemented yet");
                return {status_code: 400, body: {error: "Execute Action Asynchronous not implemented yet"}};
            }
            
        } catch(err) {
            logger.Error("Execute Action API request error", err);
            return {status_code: 500, body: {error: err}};
        }
    };
    
    static async RefreshAuthorizedAddressApiRequest(body, headers, method, url) {
        try {
            if (await this.getAuthorizedAddress()) {
                return {status_code: 200, body: {message: "Authorized address list updated"}};
            } else {
                return {status_code: 500, body: {error: "Could not refresh authorized address list"}};
            }
        } catch(err) {
            logger.Error("Execute RefreshAuthorizedAddress API request error", err);
            return {status_code: 500, body: {error: err}};
        }
    };
    
    
    static async getAuthorizedAddress() {
        try {
            let whitelist = await this.O.getMetadata({
                libraryId: this.O.Desc.oLibraryId,
                objectId: this.O.Desc.oId,
                metadataSubtree: "authorized_address"
            });
            if (whitelist) {
                this.O.Whitelist = whitelist;
                return this.O.Whitelist;
            }
        } catch(err) {
            logger.Error("Could not retrieve authorized addresses", err);
        }
        return null;
    };
    
    
    
    
    static async RunAPIService(o, params) {
        logger.SetLogFileName("o-api.log");
        ElvOSvc.ReqNumber = 0;
        o.PollingTable = {}; //indexed by handles
        logger.Set("O-svc");
        logger.AutoRotate();
        this.O = o;
        this.Verbose = params.verbose;
        let heartbeat = params.heartbeat;
        let pidFilePath = params.pidFilePath || "o-api.pid";
        if (fs.existsSync(pidFilePath + ".stopped")) {
            fs.rmSync(pidFilePath + ".stopped");
        }
        if (fs.existsSync(pidFilePath + ".stopping")) {
            fs.rmSync(pidFilePath + ".stopping");
        }
        let apiPort = params.apiPort || 8080;
        let oId = params.oId;
        let oLibraryId =  await o.getLibraryId(oId);
        o.Desc = {oId, oLibraryId};
        o.Jobs = {};
        await this.getAuthorizedAddress();
        
        const server = http.createServer(this.ApiListener); // THIS NEED TO BE BROKEN OUT AND GO TO o-svc TOGETHER WITH API CALLS
        server.listen(apiPort);
        
        fs.writeFileSync(pidFilePath, JSON.stringify({pid: process.pid, api_port: apiPort, o_id: oId}), 'utf8');
        logger.Info("O API service started", pidFilePath);
        while(fs.existsSync(pidFilePath)) {
            let hb = o.sleep(heartbeat);
            await hb;
        }
    };
    
    
    static async StopAPIService(o, params) {
        let pidFilePath = params.pidFilePath || "o-api.pid";
        let data;
        if (fs.existsSync(pidFilePath)) {
            data = JSON.parse(fs.readFileSync(pidFilePath, "utf8"));
            fs.renameSync(pidFilePath, pidFilePath + ".stopping");
        }
        let counter = 0;
        while (data && ElvOProcess.PidRunning(data.pid)) {
            await o.sleep(500);
            logger.Info("Stopping API listener...",data.pid);
            counter++;
        }
        logger.Info("API listener Stopped");
        if (data) {
            fs.renameSync(pidFilePath + ".stopping", pidFilePath + ".stopped");
        }
        return true;
    };
    
    static async StopService(o, params) {
        let pidFilePath = params.pidFilePath || "o.pid";
        let data;
        if (fs.existsSync(pidFilePath)) {
            data = JSON.parse(fs.readFileSync(pidFilePath, "utf8"));
            fs.renameSync(pidFilePath, pidFilePath + ".stopping");
        }
        let counter = 0;
        while (data && ElvOProcess.PidRunning(data.pid)) {
            await o.sleep(500);
            logger.Info("Stopping...",data.pid);
            counter++;
        }
        logger.Info("Stopped");
        if (data) {
            fs.renameSync(pidFilePath + ".stopping", pidFilePath + ".stopped");
        }
        return true;
    };
    
    static async RunService(o, params) {
        logger.AutoRotate();
        ElvOJob.AutoPurge();
        ElvOQueue.AutoPurge();
        ElvOSvc.ReqNumber = 0;
        o.PollingTable = {}; //indexed by handles
        logger.Set("O-svc");
        this.O = o;
        this.Verbose = params.verbose;
        this.Heartbeat = params.heartbeat;
        let pidFilePath = params.pidFilePath || "o.pid";
        if (fs.existsSync(pidFilePath + ".stopped")) {
            fs.rmSync(pidFilePath + ".stopped");
        }
        if (fs.existsSync(pidFilePath + ".stopping")) {
            fs.rmSync(pidFilePath + ".stopping");
        }
        let apiPort = params.apiPort || 8080;
        let oId = params.oId;
        let oLibraryId =  await o.getLibraryId(oId);
        o.Desc = {oId, oLibraryId};
        o.Jobs = {};
        await this.getAuthorizedAddress();
        if (apiPort) { //0 means no API response
            const server = http.createServer(this.ApiListener); // THIS NEED TO BE BROKEN OUT AND GO TO o-svc TOGETHER WITH API CALLS
            server.listen(apiPort);
        }
        await o.RetrieveThrottles(true); 
        fs.writeFileSync(pidFilePath, JSON.stringify({pid: process.pid, api_port: apiPort, o_id: oId}), 'utf8');
        o.PidFilePath = pidFilePath;
        logger.Info("O service started", pidFilePath);
        await this.executionLoop(o);
    };
    
    
    
    static async executionLoop(o) {
        while (true) {
            let startTime = (new Date()).getTime(); 
            if (!fs.existsSync(o.PidFilePath)) {
                process.exit(0);
            }
            try {
                o.PopFromQueueAndCreateJobs();
                await o.RunJobs();
            } catch (errLoop) {
                logger.Error("Engine loop error", errLoop);
            }
            let duration = (new Date()).getTime() - startTime;
            //logger.Debug("Loop info", {duration, popped_jobs: o.Popped, running_jobs: o.InProgress});
            if (duration + 500 < ElvOSvc.Heartbeat) {
                await o.sleep(ElvOSvc.Heartbeat - duration);
            } else {
                await o.sleep(500);
            }
        }
    }
    
};


module.exports=ElvOSvc;
