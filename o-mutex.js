const logger = require('./o-logger');
const fs = require("fs");
const path = require('path');
const glob = require("glob");
const ElvOProcess = require("./o-process");

class ElvOMutex {

    static ForceReleaseSync(name) {
        let mutexDir = path.join(this.MUTEX_ROOT, name);
        let requests = glob.sync(path.join(mutexDir, "*_*_*")).sort();
        let validRequests = [];
        let idleMutex = path.join(mutexDir, "idle");

        let toDelete = [];
        for (let request of requests) {
            let matcher = request.match(/([0-9]+)_([0-9]+)_([0-9]+)([^\_/]*)$/);
            if (matcher) {
                if (matcher[4] == ".locked") {
                    if (!fs.existsSync(idleMutex)) {
                        fs.renameSync(request, idleMutex);
                    } else {
                        this.unlink(request);
                    }
                } else {
                    toDelete.push(request);
                }
            }
        }
        for (let request of toDelete) { //second pass to ensure that the locked one is tried first
            if (!fs.existsSync(idleMutex)) {
                fs.renameSync(request, idleMutex);
            } else {
                this.unlink(request);
            }
        }
        if (!fs.existsSync(idleMutex)) {
            logger.Info("Mutex file not found, re-creating empty...", idleMutex);
            fs.writeFileSync(idleMutex, "", "utf8");
        }
    };

    static LockSync(requestArgs /*{name, holdTimeout, waitTimeout, data}*/) {
        let mutexDir = path.join(this.MUTEX_ROOT, requestArgs.name);
        let idleMutex = path.join(mutexDir,"idle");
        if (!fs.existsSync(mutexDir)) {
            fs.mkdirSync(mutexDir, { recursive: true });
            fs.writeFileSync(idleMutex, (requestArgs.data && JSON.stringify(requestArgs.data)) || "", "utf8");
        }
        let requests = this.trim(requestArgs.name);
        if (requests.length == 0) {
            let holdTimeout =  requestArgs.holdTimeout|| this.DEFAULT_HOLD_TIMEOUT;
            let now = (new Date()).getTime();
            let myMutex = path.join(mutexDir, "" + now + "_" + (now + holdTimeout) + "_" + process.pid+".locked");
            if (fs.existsSync(idleMutex)) {
                if (requests.length == 0) { //can only lock if no requests are pending
                    let now = (new Date()).getTime();
                    fs.renameSync(idleMutex, myMutex);
                    if (fs.existsSync(myMutex)) {
                        fs.writeFileSync(myMutex,(new Error("Get stack")).stack, "utf8");
                        logger.Debug("LockSync - no requests", myMutex);
                        return myMutex;
                    }
                }
            } /*else {
                    logger.Info("Mutex file not found, re-creating...", idleMutex);
                    fs.writeFileSync(myMutex, (requestArgs.data && JSON.stringify(requestArgs.data)) || "", "utf8");
                    logger.Debug("LockSync - creating", myMutex);
                    return myMutex;
            }*/
        }
        if (requests[0] && requests[0].match(/locked$/)) {
            logger.Debug("Mutex already locked by " + path.basename(requests[0]) + ". Lock requests queued: " + requests.length);
            requestArgs.currentLock = requests[0];
        } else {
            logger.Debug("All requests pending, no lock", requests.length);
        }

        return null;
    };

    static async WaitForLock(requestArgs /*{name, holdTimeout, waitTimeout, data*/) {
        let mutexDir = path.join(this.MUTEX_ROOT, requestArgs.name);
        let lockedMutex = this.LockSync(requestArgs);
        if (lockedMutex) {
            return lockedMutex;
        }
        let requestTime = (new Date()).getTime();
        let now = requestTime;
        let waitUntil = now + (requestArgs.waitTimeout || this.DEFAULT_WAIT_TIMEOUT);
        let myMutexRequest = path.join(mutexDir, "" + now + "_" + waitUntil + "_" + process.pid);
        this.writeRequest(myMutexRequest);
        /*
        if (requestArgs.currentLock) {
            fs.linkSync(requestArgs.currentLock, myMutexRequest);
        } else { //in case the mutex was captured by a concurrent request, currentLock is not known
            fs.writeFileSync(myMutexRequest, (requestArgs.data && JSON.stringify(requestArgs.data)) || "", "utf8");
        }
        */
        let lastReported = 0;
        let reportFrequency = requestArgs.progress_frequency || 5000; //default is 5 seconds
        while (now < waitUntil) {
            await new Promise(resolve => setTimeout(resolve, 100));
            let requests = this.trim(requestArgs.name);
            let idleMutex = path.join(mutexDir,"idle");
            if (fs.existsSync(idleMutex)) {
                if ((requests.length == 0) || (requests[0] == myMutexRequest)) {
                    let holdTimeout =  requestArgs.holdTimeout|| this.DEFAULT_HOLD_TIMEOUT;
                    now = (new Date()).getTime();
                    let myMutex = path.join(mutexDir, "" + requestTime + "_" + (now + holdTimeout) + "_" + process.pid+".locked");
                    fs.renameSync(idleMutex, myMutex);
                    if (fs.existsSync(myMutex)) {
                        fs.writeFileSync(myMutex,(new Error("Get stack")).stack, "utf8");
                        this.unlink(myMutexRequest);
                        //logger.Debug("Mutex stack for " + myMutex, (new Error("Get stack")).stack.toString().replace(/Error.*?\n../,"Locked"));
                        return myMutex;
                    }
                }
            }
            now = (new Date()).getTime();
            if (requestArgs.progress_notifyer) {
                if ((lastReported + reportFrequency) < now) {
                    lastReported = now;
                    for (let i=0; i < requests.length; i++) {
                        if (requests[i] == myMutexRequest) {
                            requestArgs.progress_notifyer.report("Waiting for Mutex, number of requests ahead is " +i);
                            break;
                        }
                    }
                }
            }
        }
        return null;
    };

    static ReleaseSync(mutex) {
        logger.Debug("Releasing mutex", mutex);
        if (fs.existsSync(mutex)) {
            let mutexDir = path.dirname(mutex);
            let idleMutex = path.join(mutexDir, "idle");
            fs.renameSync(mutex, idleMutex);
            return true;
        } else {
            logger.Info("Mutex " + mutex + " was either not locked or was expired");
            return false
        }
    };

    static writeRequest(path) {
        let data = (new Error("Get stack")).stack;
        fs.writeFileSync(path, data || "", "utf8");
    };

    static trim(name) {
        let mutexDir = path.join(this.MUTEX_ROOT, name);
        let requests = glob.sync(path.join(mutexDir, "*_*_*")).sort();
        let validRequests = [];
        for (let request of requests) {
           let matcher = request.match(/([0-9]+)_([0-9]+)_([0-9]+)([^\_/]*)$/); //request_time,request_or_hold_timeout,pid,[.lock]
           if (matcher) {
               let pid = parseInt(matcher[3]); //we could create lock that survive a process by using special value as pid instead of actual
               let timeout = parseInt(matcher[2]);
               if ((timeout < (new Date()).getTime()) || !ElvOProcess.PidRunning(pid)) {
                   if (matcher[4] == ".locked") {        //release expired locks
                       fs.renameSync(request, path.join(mutexDir, "idle"));
                   } else {
                       this.unlink(request); //remove expired request
                   }
               } else {
                   validRequests.push(request);
               }
           }
        }
        return validRequests;
    };

    static unlink(path) {
      try {
          fs.unlinkSync(path);
      } catch(err) {
          if (err.code != "ENOENT") {
              throw err;
          }
       }
    };


    static MUTEX_ROOT = "./Mutex";
    static DEFAULT_HOLD_TIMEOUT = 3600000; //1 hour
    static DEFAULT_WAIT_TIMEOUT = 300000; //5 minutes
};

module.exports=ElvOMutex;