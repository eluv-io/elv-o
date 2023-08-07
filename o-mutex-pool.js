const logger = require('./o-logger');
const fs = require("fs");
const path = require('path');
const glob = require("glob");
const ElvOMutex = require("./o-mutex.js");

class ElvOMutexPool {

    static SetUp({name, resources, reset}) {
        if (Number.isInteger(resources)) {
            let number = resources;
            resources = [];
            for (let i=1; i <= number; i++) {
                resources.push("_generic_"+i+"_of_"+number);
            }
        }
        let mutexPoolDir = path.join(ElvOMutex.MUTEX_ROOT, name, "Resources");
        if (fs.existsSync(mutexPoolDir) && reset) {
            fs.rmSync(mutexPoolDir, {recursive: true});
        }
        if (!fs.existsSync(mutexPoolDir) || reset) {
            fs.mkdirSync(mutexPoolDir, {recursive: true});
            let idleMutex = path.join(ElvOMutex.MUTEX_ROOT, name, "idle");
            fs.writeFileSync(idleMutex,  JSON.stringify(resources), "utf8");
            for (let resource of resources) {
                let idleResource = path.join(mutexPoolDir, encodeURIComponent(resource)+".idle");
                fs.writeFileSync(idleResource,  "", "utf8");
            }
        }
    };

    static LockSync({name, holdTimeout, waitTimeout}) {
        let mutex = ElvOMutex.LockSync({name, holdTimeout, waitTimeout});
        let resource = mutex && this.lockAvailableResourceSync(name, holdTimeout);
        if (resource) {
            if (resource.left > 0) {
                ElvOMutex.ReleaseSync(mutex)
            }
            return resource;
        } else {
            return null;
        }
    };

    static ReleaseSync(resource) {
        if (fs.existsSync(resource.path)) {
            let idleResource = resource.path.replace(/[0-9_]+$/, "idle");
            fs.renameSync(resource.path, idleResource);
        }
        ElvOMutex.ForceReleaseSync(resource.name)
    };


    static async WaitForLock({name, holdTimeout, waitTimeout}) {
        let intervalId = setInterval(function() {
            ElvOMutexPool.cleanUpExpiredLocks(name);
        }, ElvOMutexPool.EXPIRATION_CHECK_FREQUENCY);
        let mutex = await ElvOMutex.WaitForLock({name, holdTimeout, waitTimeout});
        clearInterval(intervalId);
        let resource = mutex && this.lockAvailableResourceSync(name, holdTimeout);
        if (resource) {
            if (resource.left > 0) {
                ElvOMutex.ReleaseSync(mutex)
            }
            return resource;
        } else {
            return null;
        }
    };

    static cleanUpExpiredLocks(name) {
        let mutexPoolDir = path.join(ElvOMutex.MUTEX_ROOT, name, "Resources");
        let found = glob.sync(mutexPoolDir + "/*.*_*");
        let now = new Date().getTime();
        for (let resourcePath of found) {
            try {
                let matcher = resourcePath.match(/\.([0-9]+)_([0-9]+)$/);
                if (!matcher) {
                    logger.Error("Invalid resource name, skipping", resourcePath);
                    continue;
                }
                if (parseInt(matcher[2]) < now) {
                    logger.Info("Expired resource, de-allocating", resourcePath);
                    let idleResource = resourcePath.replace(/[0-9_]+$/, "idle");
                    fs.renameSync(resourcePath, idleResource);
                }
            } catch (eRes) {
               logger.Error("Could not check resource expiration for "+ resourcePath, eRes);
            }
        }
    };
    static ListResourcesStatus(name) {
        let mutexPoolDir = path.join(ElvOMutex.MUTEX_ROOT, name, "Resources");
        let found =  fs.readdirSync(mutexPoolDir);
        let now = new Date().getTime();
        let list = {};
        for (let fileName of found) {
            let matcher = fileName.match(/(.*)\.([^\.]+)/);
            if (matcher) {
                let extension = matcher[2];
                let resourceName = matcher[1];
                if (extension == "idle") {
                    list[resourceName] = {idle: true};
                } else {
                    list[resourceName] = {idle: false};
                    matcher = extension.match(/([0-9]+)_([0-9]+)$/);
                    list[resourceName].expired = (parseInt(matcher[2]) < now);
                    if (!list[resourceName].expired) {
                        list[resourceName].expires_in = Math.round((parseInt(matcher[2]) - now) / 1000);
                    }
                }
            }
        }
        return list;
    };

    static ListAvailableResourcesSync(name) {
        let mutexPoolDir = path.join(ElvOMutex.MUTEX_ROOT, name, "Resources");
        let found = glob.sync(mutexPoolDir + "/*.idle");
        return found;
    };

    static lockAvailableResourceSync(name, holdTimeout) {
        let found = this.ListAvailableResourcesSync(name);
        if (found.length == 0) {
            return null;
        }
        if (holdTimeout && !Number.isInteger(holdTimeout)){
            throw new Error("Can not use non integer value for holdTimeout: " + holdTimeout);
        }
        let selectedIndex = new Date().getTime() % found.length;
        let lockExpiration = new Date().getTime() + (holdTimeout || this.MAX_HOLD_DURATION) * 1000;
        let lockedPath = found[selectedIndex].replace(/\.idle$/,'.'+process.pid+"_"+lockExpiration);
        fs.renameSync(found[selectedIndex], lockedPath);
        if (!fs.existsSync(lockedPath)) {
            logger.Error("Failed to lock resource ", found[selectedIndex]);
            return  null;
        } else {
            let resource = decodeURIComponent(path.basename(found[selectedIndex]).replace(/\.idle$/,''));
            return {resource, path: lockedPath, left: (found.length - 1), name};
        }
    };

    static MAX_HOLD_DURATION = 24 * 3600;
    static EXPIRATION_CHECK_FREQUENCY = 5000
}

module.exports=ElvOMutexPool;