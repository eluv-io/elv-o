const fs = require("fs");
const path = require('path');
const glob = require('glob');

class ElvOLogger {
    
    static filename = "o.log";
    static directory = "./Logs";
    static DAYS_KEPT_DEFAULT = 7;
    
    static Set(svc) {
        this.Service = svc;
    }
    
    static log(level, msg) {
        if (!ElvOLogger.Filename) {
            ElvOLogger.SetLogFileName(ElvOLogger.filename);
        }
        try {
            let line = this.timestamp() + " [" + (this.Service || "*") + ":"+process.pid+"] " + level + ": " + msg + "\n";
            fs.writeFileSync(ElvOLogger.Filename, line, {encoding: 'utf8', flag: "a"});
        } catch(err)  {
            //console.error("Could not log",level, msg, err);
        }
    };
    
    static SetLogFileName(filename) {
        fs.mkdirSync(ElvOLogger.directory, {recursive: true});
        ElvOLogger.Filename = path.join(ElvOLogger.directory, filename);
    }
    
    static  timestamp() {
        return (new Date()).toISOString();
    }
    
    static Info(msg, data) {
        if (!data) {
            ElvOLogger.log("info", msg);
        } else {
            ElvOLogger.log("info", msg +": "+JSON.stringify(data));
        }
    };
    
    //logger.Error("baba", new Error());
    static Error(label, err, fn) {
        if (!err) {
            this.log("error",label);
        } else {
            let msg;
            if (!err.name) {
                msg = label + ": "  +  JSON.stringify(err);
            } else {
                msg = label + ": " + err.name + " - " + err.message;
            }
            ElvOLogger.log("error", msg);
            if (err.stack) {
                ElvOLogger.log("error-trace", err.stack);
            }
        }
    };
    
    static Debug(msg, data) {
        if (!data) {
            ElvOLogger.log("debug", msg);
        } else {
            ElvOLogger.log("debug", msg +": "+JSON.stringify(data));
        }
    };
    
    static Peek(label, err, fn) {
        if (!err) {
            this.log("peek",label);
        } else {
            let msg;
            if (!err.name) {
                msg = label + ": "  +  JSON.stringify(err);
            } else {
                msg = label + ": " + err.name + " - " + err.message;
            }
            ElvOLogger.log("peek", msg);
            if (err.stack) {
                ElvOLogger.log("trace", err.stack);
            }
        }
    };
    
    
    static initDir() {
        try {
            fs.mkdirSync(this.directory, "0744");
        } catch(err) {
            if (err) {
                if (err.code != 'EEXIST'){
                    console.error("Could not create folder", err);
                    process.exit(11);
                }
            }
        }
    }
    
    static Rotate() {
        this.initDir();
        let archivedPath;
        this.Purge();
        try {
            let currentLogPath = path.join(this.directory, this.filename);
            if (!fs.existsSync(currentLogPath)) {
                return null;
            }
            let now = (new Date()).toISOString();
            archivedPath = path.join(this.directory, now.replace(/T.*/, "--") + this.filename);
            if (fs.existsSync(archivedPath)) {
                archivedPath = path.join(this.directory, now.replace(/\..*/, "--") + this.filename);
            }
            fs.renameSync(currentLogPath, archivedPath);
            this.Info("Rotated log to " +archivedPath);
            return true;
        } catch(err) {
            this.Error("Could not rotate log to " +archivedPath, err);
            return false;
        }
    };
    
    static async AutoRotate(filename) {
        if (!filename) {
            filename = ElvOLogger.Filename || ElvOLogger.filename
        }
        ElvOLogger.initDir();
        let archivedPath;
        let success = false;
        try {
            let currentLogPath = path.join(filename);
            if (fs.existsSync(currentLogPath)) {
                let now = (new Date()).toISOString();
                archivedPath = path.join(ElvOLogger.directory, now.replace(/T.*/, "--") + path.basename(filename));
                if (!fs.existsSync(archivedPath)) {
                    ElvOLogger.Info("Rotating log to " + archivedPath);
                    fs.renameSync(currentLogPath, archivedPath);
                    ElvOLogger.Info("Rotated log to " + archivedPath);
                }
            }            
            success = true;
        } catch(err) {
            ElvOLogger.Error("Could not rotate log to " +archivedPath, err);
        }
        setTimeout(ElvOLogger.AutoRotate, 1000 * 3600 * 24, filename);
        return success;
    };
    
    static Purge(daysKept) {
        try {
            if (!daysKept && daysKept != 0) {
                daysKept = ElvOLogger.DAYS_KEPT_DEFAULT;
            }
            let candidates = glob.sync(path.join(ElvOLogger.directory, "*"+ ElvOLogger.filename));
            let cutoffDate = (new Date((new Date()).getTime() - (daysKept * 24 * 3600 * 1000))).toISOString().replace(/T.*/, "--");
            for (let candidate of candidates) {
                try {
                    let logName = path.basename(candidate);
                    let matcher = candidate.match(/([0-9][0-9][0-9])-([0-9][0-9])-([0-9][0-9])--/);
                    if (matcher) {
                        if  (logName < cutoffDate) {
                            fs.unlinkSync(candidate);
                            ElvOLogger.Info("Purged stale log file", logName);
                        }
                    }
                } catch(errLog) {
                    ElvOLogger.Info("Error purging " + candidate, errLog);
                }
            }
        } catch(err) {
            ElvOLogger.Info("Error processing purge", err);
        }
    };
};

/* THAT SHIT DOES NOT FLUSH
const winston = require('winston');

const oFormat = winston.format.printf(({ level, message, label, timestamp }) => {
    return `${timestamp} [${label}] ${level}: ${message}`;
});



class ElvOLogger {
    static logger = winston.createLogger({
        level: 'info',
        format: winston.format.combine(winston.format.label({label: "O-svc"}), winston.format.timestamp(), oFormat),
        defaultMeta: { service: 'o-service' },
        transports: [
            new winston.transports.File({ filename: './o.log' }),
        ],
        exitOnError: false
    });
    
    static info(msg, data) {
        if (!data) {
            this.logger.info(msg)
        } else {
            this.logger.info(msg +": "+JSON.stringify(data));
        }
    };
    
    //logger.error("baba", new Error());
    static error(label, err, fn) {
        if (!err) {
            this.logger.error(label);
        } else {
            let msg;
            if (!err.name) {
                msg = label + ": "  +  JSON.stringify(err);
            } else {
                msg = label + ": " + err.name + " - " + err.message;
            }
            this.logger.error(msg);
            if (err.stack) {
                this.logger.info(err.stack);
            }
        }
    };
};

*/

module.exports = ElvOLogger;
