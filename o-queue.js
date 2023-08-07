const logger = require('./o-logger');
const fs = require("fs");
const path = require('path');
const glob = require("glob");

class ElvOQueue {
    
    constructor(queueId) {
        let metaPath = path.join(ElvOQueue.Q_DIR,queueId,".meta.json");
        let params = JSON.parse(fs.readFileSync(metaPath, 'utf8'));
        this.Priority = params.priority;
        this.Name = params.name || "Queue " + params.id;
        this.Id = params.id;
        this.active = params.active;
    }
    
    itemPath(item, priority) {
        return ElvOQueue.itemPath(this.Id, item, priority);
    };
    
    Queue(item, priority) {
        return ElvOQueue.Queue(this.Id, item, priority);
    };
    
    Queued(limit) {
        return ElvOQueue.Queued(this.Id, limit);
    };
    
    Pop(itemPath) {
        return ElvOQueue.Pop(this.Id, itemPath);
    };
    
    Activate() {
        return ElvOQueue.Activate(this.Id, true);
    };
    Deactivate() {
        return ElvOQueue.Activate(this.Id, false);
    };
    
    static Queue(queueId, item, priority) {
        try {
            let itemPath = this.itemPath(queueId, item, priority);
            fs.writeFileSync(itemPath, JSON.stringify(item, null, 2), 'utf8');
            return itemPath;
        } catch(err) {
            logger.Error("Could not queue item " + item.id, err);
            return null;
        }
    };
    
    static itemPath(queueId, item, priority) {
        let timeStamp = (new Date()).getTime();
        return path.join(this.Q_DIR, queueId, ('000' + priority).slice(-4) + "__" + timeStamp+"__" + this.slugit(item.id || "No id provided")+"__"+item.workflow_id);
    };
    
    static slugit(str) {
        return str.toLowerCase().replace(/ /g, "-").replace(/[^a-z0-9\-]/g,"");
    };
    
    static Q_DIR ="./Queues";
    static Q_ARCHIVE ="./Archive";
    static Queues = {};
    
    static List(active) { //true, false or null for all
        try {
            fs.mkdirSync(this.Q_DIR, "0744");
        } catch(err) {
            if (err) {
                if (err.code != 'EEXIST'){
                    logger.Error("Could not create folder", err);
                    throw err;
                };
            };
        };
        let queues = fs.readdirSync(this.Q_DIR);
        let list = [];
        for (let i=0; i < queues.length; i++) {
            try {
                let queue = new ElvOQueue(queues[i]);
                this.Queues[queues[i]] = queue;
                if ((active == null) || (queue.active == active)) {
                    list.push(queues[i]);
                }
            } catch(err){
                logger.Error("Invalid queue " + queues[i], err);
            }
        }
        return list;
    };
    
    static Create(queueId, priority, active,  name, silent) {
        let list = this.List();
        try {
            if (queueId && !this.Queues[queueId]) {
                let params = {};
                params.id = queueId;
                params.name = name || "Queue " + queueId.id;
                params.active = active || false;
                params.priority = priority || ((priority == 0) ? 0 : 100);
                fs.mkdirSync(path.join(ElvOQueue.Q_DIR, queueId), {mode: "0744", recursive: true});
                fs.mkdirSync(path.join(ElvOQueue.Q_ARCHIVE, queueId), {mode: "0744", recursive: true});
                fs.mkdirSync(path.join(ElvOQueue.Q_DIR, queueId, ".error"), {mode: "0744", recursive: true});
                let metaPath = path.join(ElvOQueue.Q_DIR, queueId,".meta.json");
                fs.writeFileSync(metaPath, JSON.stringify(params, null, 2), 'utf8');
                this.Queues[queueId] = new ElvOQueue(queueId);
            } else {
                if (!silent) {
                    logger.Info("Queue found " + queueId);          
                } 
            }
            return this.Queues;
        } catch(err) {
            logger.Error("Could not create Queue " + queueId, err);
            return null;
        }
    };
    
    static Activate(queueId, active) {
        let metaPath = path.join(this.Q_DIR, queueId, ".meta.json");
        let params = JSON.parse(fs.readFileSync(metaPath, 'utf8'));
        params.active = (active != false);
        fs.writeFileSync(metaPath, JSON.stringify(params, null, 2), 'utf8');
        this.Queues[queueId] = new ElvOQueue(queueId);
        return this.Queues;
    };
    
    static Item(itemPathId, queueId) {
        let itemPath = (!queueId) ? itemPathId : path.join(this.Q_DIR, queueId, itemPathId);
        try {
            let item = JSON.parse(fs.readFileSync(itemPath, 'utf8'));
            item.path = itemPath;
            return item;
        } catch(err) {
            logger.Error("Could not extract item from " + itemPath, err );
            return null;
        }
    };
    
    
    static AllQueued() {
        let activeQueues = this.List(true);
        let queues = activeQueues.sort(function(a,b){if (parseInt(ElvOQueue.Queues[a].Priority) <= parseInt(ElvOQueue.Queues[b].Priority)) {return -1;} else {return 1;}});
        let found = [];
        let totalItems = 0;
        for (let i = 0; i < queues.length; i++) {
            let queued = {};
            if (!this.Queues[queues[i]].active) {
                continue;
            }
            found = found.concat(this.Queued(queues[i]));
        }
        return found;
    };
    
    
    static FindByJobReference(jobReference, queueId) {
        let queues;
        if (queueId) {
            queues = [queueId];
        } else {
            queues = this.List(true);
        }
        let sluggedRef = this.slugit(jobReference);
        for (let i = 0; i < queues.length; i++) {
            let queueId = queues[i];
            let raw = fs.readdirSync(path.join(this.Q_DIR, queueId));
            for (let i = 0; i < raw.length; i++) {
                try {
                    let matcher = raw[i].match(sluggedRef);
                    if (!matcher) {
                        continue; 
                    }
                    let queuedPath = path.join(this.Q_DIR, queueId,raw[i]);
                    let item = JSON.parse(fs.readFileSync(queuedPath), 'utf8');
                    if (item.id == jobReference) {
                        item.queue_id = queueId;
                        item.queued_path = queuedPath;
                        item.item_file_name = path.basename(queuedPath);
                        return item;
                    }
                } catch(err) {
                    logger.Error("Item likely pulled from queue", err);
                }
            }
        }
        return null;
    };
    
    static Queued(queueId, limit) {
        if (!queueId || ((typeof queueId) ==  "object")) {
            let queues = queueId;
            let allActiveQueues = this.List(true);
            if (!queues || queues.length == 0) {
                queues = allActiveQueues;
            }
            queues = queues.sort(function(a,b){if (parseInt(ElvOQueue.Queues[a].Priority) <= parseInt(ElvOQueue.Queues[b].Priority)) {return -1;} else {return 1;}});
            let found = [];
            let totalItems = 0;
            for (let i=0; i < queues.length; i++) {
                let queued = {};
                if (!this.Queues[queues[i]].active) {
                    continue;
                }
                if (limit) {
                    if (totalItems < limit) {
                        queued[queues[i]] = this.Queued(queues[i], limit - totalItems);
                        totalItems += queued[queues[i]].length;
                    } else {
                        break;
                    }
                } else {
                    queued[queues[i]] = this.Queued(queues[i]);
                }
                if (queued[queues[i]].length) {
                    found.push(queued);
                }
            }
            return found;
        } else {
            let raw = fs.readdirSync(path.join(this.Q_DIR, queueId));
            let found = []
            for (let i = 0; i < raw.length; i++) {
                let matcher = raw[i].match(/[0-9]+__[0-9]+__[^_]*__(.*)$/);
                if (matcher) {
                    found.push({path: raw[i], workflow_id: matcher[1], queue_id: queueId});
                }
            }
            if (limit) {
                return found.sort().slice(0, limit);
            } else {
                return found.sort();
            }
        }
    };
    
    static Next(queues, popIt) {
        let allActiveQueues = this.List(true);
        if (!queues || queues.length == 0) {
            queues = allActiveQueues;
        }
        queues  = queues.sort(function(a,b){if (parseInt(ElvOQueue.Queues[a].Priority) <= parseInt(ElvOQueue.Queues[b].Priority)) {return -1;} else {return 1;}});
        for (let i=0; i < queues.length; i++) {
            if (!this.Queues[queues[i]].active) {
                continue;
            }
            let found = this.Queued(queues[i], 1);
            if (found && found.length != 0) {
                if (popIt) {
                    let popped =  this.Pop(queues[i], found[0]);
                    if (!popped) { //item already popped
                        return this.Next(queues, popIt); //pops the next one
                    } else {
                        return popped;
                    }
                } else {
                    let itemPath = path.join(this.Q_DIR, queues[i], found[0]);
                    return {path: itemPath,
                        item: this.Item(itemPath),
                        queue_id: queues[i],
                        queued: true
                    }
                }
            }
        }
        return null;
    };
    
    static Pop(queueId, itemFilename, error) {
        if (!fs.existsSync(path.join(this.Q_ARCHIVE, queueId))) {
            fs.mkdirSync(path.join(ElvOQueue.Q_ARCHIVE, queueId), {mode: "0744", recursive: true});
        }
        let itemPath = path.join(this.Q_DIR, queueId, itemFilename);
        let poppedPath = path.join(this.Q_ARCHIVE, queueId, itemFilename);
        if (error || fs.existsSync(poppedPath)) {
            poppedPath = poppedPath + "." + (error || "error");
        }
        try {
            fs.renameSync(itemPath, poppedPath);
            if (fs.existsSync(poppedPath) && !fs.existsSync(itemPath)) {
                return {path: poppedPath,
                    item: this.Item(poppedPath),
                    queue_id: queueId,
                    queued: false
                }
            }
        } catch(err) {
            if (fs.existsSync(itemPath)){
                logger.Error("Could not pop item " + itemPath, err);
            }
            return null;
        }
        return null;
    }
    
    static Purge(queueId, cutoff) {
        if (!cutoff) {
            cutoff = (new Date()).getTime() - (ElvOJob.DAYS_KEPT * 24 * 3600 * 1000);
        }
        let pathFilter = path.join(this.Q_ARCHIVE, queueId, "*__*__*__*");
        let candidates = glob.sync(pathFilter);
        let count = 0;
        let max = (cutoff && cutoff.toString()) || "A";
        for (let candidate of candidates) {
            try {
                let matcher = path.basename(candidate).match(/^[0-9]+__([0-9]+)__/);
                if (matcher && (matcher[1] <= max)) {
                    fs.unlinkSync(candidate);
                    count++;
                }
            } catch(err) {
                logger.Error("Could not delete item " + candidate, err);
            }
        }
        return count;       
    };
    
    static async AutoPurge() {
        let success = false
        try {
            let queues = ElvOQueue.List(true); //inactive queues are frozen, so no purges for them
            let cutoff = (new Date()).getTime() - (ElvOQueue.DAYS_KEPT * 24 * 3600 * 1000);     
            logger.Info("Initiating queues archive purge", {cutoffDate: new Date(cutoff)});
            let count = 0;
            for (let queue of queues) {
                try {
                    count += ElvOQueue.Purge(queue, cutoff);
                } catch(errQueue) {
                    logger.Error("Could not process purge of staled items in queue "+queue, errQueue);
                }
            }
            logger.Info("Completed queues archive purge", {count});
            success = true;
        } catch(err) {
            logger.Error("Could not process purge of staled archived queued items", err);
        }
        setTimeout(ElvOQueue.AutoPurge, 1000 * 3600 * 24);  
        return success;
    }
    
    static DAYS_KEPT=60;
};

module.exports=ElvOQueue;