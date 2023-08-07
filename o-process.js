const fs = require("fs");
const os = require('os');
class ElvOProcess {

    static isPresentInArgv(argument) {
        let pattern = new RegExp("--"+argument);
        return (process.argv.join(" ").match(pattern) != null);
    };

    static getValueInArgv(argument) {
        for (let i=0; i < process.argv.length; i++) {
            let arg = process.argv[i];
            let pattern = new RegExp("--"+argument+"=");
            if (arg && arg.match(pattern)) {
                return arg.replace(pattern, "");
            }
        }
        return null;
    };

    static getValueInArg(argument, envVar, defaultedValue) {
        let inArgV = this.getValueInArgv(argument);
        if (!inArgV && envVar) {
            inArgV = process.env[envVar] || defaultedValue;
        }
        if ((inArgV == "undefined") || (inArgV == "null") || (inArgV == ""))  {
            inArgV = null;
            process.env[envVar] = "";
        } else {
            process.env[envVar] = inArgV;
        }
        return inArgV;
    };

    static isPresentInArg(argument, envVar){
        let value = this.isPresentInArgv(argument);
        if (!value) {
            if (process.env[envVar] == "true") {
                value = true;
            }
        } else {
            process.env[envVar] = "true";
        }
        return value;
    };

    static pidIsRunning(pid) {  //to be deprecated
        try {
            process.kill(pid, 0);
            return true;
        } catch(e) {
            return false;
        }
    };

    static async Sleep(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    };

    static Platform() {
        return os.platform();
    }

    static PidRunning(pid) {
        try {
            process.kill(pid, 0);
            if (this.Platform() == "linux") {
                let status = this.processStatus(pid);
                if (!status || (status.state == "Z")) {
                    return false;
                }
            }
            return true;
        } catch(e) {
            return false;
        }
    };
// possible values for State value in /proc/pid/status
// R running,
// S is sleeping,
// D is sleeping in an uninterruptible wait,
// Z is zombie (not running but held by process owner)
// T is traced or stopped
    static processStatus(pid) { //only works on Linux
        /* //removed to avoid calling twice in a row os.platform()
        if (this.Platform() != "linux") {
            throw new Error("Only available on linux");
        }
        */
        let procInfo
        try {
            procInfo = fs.readFileSync('/proc/' + pid + '/status').toString();
        } catch (e) {
            return null;
        }

        let state = procInfo.match(/State:\s+([RSDT])/)[1];
        let ppid = parseInt(procInfo.match(/PPid:\s+([0-9]+)/)[1]);
        return {state, ppid};
    };

};


module.exports=ElvOProcess;


