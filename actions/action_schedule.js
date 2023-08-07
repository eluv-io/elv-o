const ElvOAction = require("../o-action").ElvOAction;
const ElvOProcess = require("../o-process");
const ElvOFabricClient = require("../o-fabric");

class ElvOActionSchedule extends ElvOAction {

    ActionId() {
        return "schedule";
    };

    IsContinuous() {
        return false; //indicates that the execution stays within a single PID
    };

    Parameters() {
        return {
            "parameters": {
                type: {type: "string", required: true, values: ["TIMED", "PERIODIC"]},
                ongoing: {type: "boolean", required: false, default: false},
                persistence_scope: {type: "string", required: false, values: ElvOAction.PERSISTENCE_SCOPES}
            }
        };
    };

    IOs(parameters) {
        let inputs = {};
        if (parameters.type == "TIMED") {
            inputs.ToD = {type: "time", required: false, default: "02:00:00"};
            inputs.DoW = {type: "array", required: false, default: [0, 1, 2, 3, 4, 5, 6]};
            //inputs.DoM = {type: "array", required: false};
        }
        if (parameters.type == "PERIODIC") {
            inputs.delay_seconds = {type: "numeric", required: false, default: 0};
            inputs.delay_minutes = {type: "numeric", required: false, default: 0};
            inputs.delay_hours = {type: "numeric", required: false, default: 0};
            inputs.delay_days = {type: "numeric", required: false, default: 0};
            inputs.start_now = {type: "boolean", required: false, default: false};
        }
        let outputs = {trigger_time: {type: "datetime"}, scheduled_trigger_time: {type: "datetime"}};
        if (parameters.ongoing) {
            outputs.next = {type: "datetime"};
        }
        return {inputs, outputs};
    };

    PollingInterval() {
        if (this.Payload) {
            let delay = this.calculateDelay(this.Payload.inputs);
            //this.Debug("PollingInterval", {raw: delay, actual: Math.max(1, Math.floor(delay / 10000))});
            return Math.min(Math.max(1, Math.floor(delay / 10000)), 3600);
        } else {
            return 60; //poll every minutes
        }
    };

    IdleTimeout() {
        if (this.Payload) {
            let delay = this.calculateDelay(this.Payload.inputs);
            return Math.max(5, Math.floor(delay / 5000)); // polling is twice as frequent
        }
        return 0; //no maximum
    };


    async Execute(handle, outputs) {
        if (["PERIODIC","TIMED"].includes(this.Payload.parameters.type) == false) {
            this.ReportProgress("Unsupported schedule type: " + this.Payload.parameters.type);
            this.Error("Unsupported schedule type", this.Payload.parameters.type);
            return ElvOAction.EXECUTION_FAILED;
        }
        let delay = this.calculateDelay(this.Payload.inputs);
        if (this.Payload.parameters.ongoing) {
            let persisted = this.getPersistedData() || {};
            if (persisted.next) {
                let now = new Date();
                if (new Date(persisted.next) <= now) {
                    outputs.trigger_time = now.toISOString();
                    outputs.scheduled_trigger_time = outputs.trigger_time;
                    outputs.next = (new Date(now.getTime() + delay)).toISOString();
                    this.persistData({next: outputs.next});
                    this.ReportProgress("Triggered at " + outputs.trigger_time);
                    return ElvOAction.EXECUTION_COMPLETE_TO_BE_CLONED;
                } else {
                    this.markTriggerInfo(persisted.next, true);
                    return ElvOAction.EXECUTION_ONGOING;
                }
            } else { //start_now makes no sense if not ongoing
                if (this.Payload.inputs.start_now) { //start_now is ignored if next was provided
                    let now = new Date();
                    outputs.trigger_time = now.toISOString();
                    outputs.scheduled_trigger_time = outputs.trigger_time;
                    outputs.next = (new Date(now.getTime() + delay)).toISOString();
                    this.persistData({next: outputs.next});
                    this.ReportProgress("Triggered at " + outputs.trigger_time);
                    return ElvOAction.EXECUTION_COMPLETE_TO_BE_CLONED;
                }
            }
        }

        this.Debug("triggerTime", {now: (new Date()).getTime(), delay, trigger_time: (new Date()).getTime() + delay});
        let triggerTime = (new Date((new Date()).getTime() + delay)).toISOString();
        this.markTriggerInfo(triggerTime, this.Payload.parameters.ongoing);
        return ElvOAction.EXECUTION_ONGOING;
    };

    async MonitorExecution(pid, outputs) {
        let info = this.retrieveTriggerInfo();
        this.Debug("MonitorExecution", info)
        if (!info) {
            if (ElvOAction.PidRunning(pid)) {
                this.reportProgress("Status probed before trigger information were persisted");
                return ElvOAction.EXECUTION_ONGOING;
            } else {
                this.Error("Trigger information not found");
                this.ReportProgress("Trigger information not found");
                return ElvOAction.EXECUTION_EXCEPTION;
            }
        }
        let now = new Date();
        if (now.toISOString() >= info.trigger_time) {
            outputs.trigger_time = now.toISOString();
            outputs.scheduled_trigger_time = info.trigger_time;
            this.ReportProgress("Triggered at " + outputs.trigger_time);
            if (this.Payload.parameters.ongoing) {
                let delay = this.calculateDelay(this.Payload.inputs);
                outputs.next = (new Date(now.getTime() + delay)).toISOString();
                this.persistData({next: outputs.next});
                return ElvOAction.EXECUTION_COMPLETE_TO_BE_CLONED;
            } else {
                return ElvOAction.EXECUTION_COMPLETE;
            }
        } else {
            this.ReportProgress("Status probed before scheduled time", {now: now.toISOString() ,past: (now.toISOString() >= info.trigger_time), target: info.trigger_time});
            return ElvOAction.EXECUTION_ONGOING;
        }
    };

    markTriggerInfo(triggerTime, ongoing) {
        this.trackProgress(ElvOActionSchedule.TRIGGER_TIME, "Calculated trigger time", {trigger_time: triggerTime, ongoing} );
        this.Debug("Calculated trigger time "+  triggerTime, {local_time:  (new Date(triggerTime)).toLocaleString()});
    };

    retrieveTriggerInfo() {
        let infoTracker = this.Tracker[ElvOActionSchedule.TRIGGER_TIME];
        return infoTracker && infoTracker.details;
    };

    setToD(dateRef, daysOffset, tOd) {
        let date = new Date(dateRef.getTime() + daysOffset * 24 * 3600 * 1000);
        let matcher = tOd.match(/([0-2][0-9]):([0-5][0-9]):([0-5][0-9])/);
        date.setUTCHours(matcher[1],matcher[2],matcher[3]);
        this.Debug("setToD", {dateRef, daysOffset, tOd, date, local: date.toLocaleString()});
        return date;
    };

    convertToISO(inputs) {
        let matcher = inputs.ToD.match(/([0-2][0-9]):([0-5][0-9]):([0-5][0-9])/);
        let zoneOffset = (new Date()).getTimezoneOffset();
        let hoursZoneOffset = Math.floor(zoneOffset / 60);
        let minutesZoneOffset = zoneOffset % 60; // let's ignore 1/2 timezones
        let iso={};
        let dayOffset=0;
        let hourOffset =0;
        if (minutesZoneOffset + parseInt(matcher[2]) >= 60) {
            hourOffset = 1;
        }
        if (minutesZoneOffset + parseInt(matcher[2]) < 0) {
            hourOffset = -1;
        }
        if (hoursZoneOffset + parseInt(matcher[1]) +  hourOffset >= 24) {
            dayOffset = 1;
        }
        if (hoursZoneOffset + parseInt(matcher[1]) < 0) {
            dayOffset = -1;
        }

        iso.hours = (hoursZoneOffset + parseInt(matcher[1]) +  hourOffset - (dayOffset * 24));
        iso.minutes = (minutesZoneOffset + parseInt(matcher[2])  - (hourOffset * 60));
        iso.seconds = parseInt(matcher[3]);
        let tOd = ('00' + iso.hours).slice(-2)  + ":" + ('00' + iso.minutes).slice(-2) + ":" + ('00' + iso.seconds).slice(-2);
        let dOw = [];
        if (dayOffset) {
            for (let day of inputs.DoW) {
                dOw.push((day + 7 + dayOffset) % 7);
            }
        } else {
            dOw = inputs.DoW;
        }
        return {ToD: tOd, DoW: dOw};
    };

    calculateDelay(inputs) {
        if (!this.Delay) {
            if (this.Payload.parameters.type == "PERIODIC") {
                this.Delay = ((inputs.delay_days * 24 * 3600) + (inputs.delay_hours * 3600) + (inputs.delay_minutes * 60) + inputs.delay_seconds) * 1000;
            }
            if (this.Payload.parameters.type == "TIMED") {
                let now = new Date();
                let isoTime = this.convertToISO(inputs);

                let nowDay = now.getDay();
                let nowTime = ('00' + now.getUTCHours()).slice(-2)  + ":" + ('00' + now.getUTCMinutes()).slice(-2) + ":" + ('00' + now.getUTCSeconds()).slice(-2);
                let triggerTime;
                //this.Debug("trigger data", {t_o_d: inputs.ToD, d_o_w: inputs.DoW, today_good: inputs.DoW.includes(nowDay), nowDay, nowTime});
                if (isoTime.DoW.includes(nowDay) && (nowTime <= isoTime.ToD)) {
                    triggerTime = this.setToD(now, 0, isoTime.ToD);
                } else {
                    for (let i = 1; i < 7; i++) {
                        let day = (nowDay + i) % 7;
                        if (isoTime.DoW.includes(day)) {
                            triggerTime = this.setToD(now, i, isoTime.ToD);
                            break;
                        }
                    }
                }
                this.Delay = triggerTime.getTime() - now.getTime();
            }
        }
        return this.Delay;
    };


    static TRIGGER_TIME = 60;
    static VERSION = "0.0.4";
    static REVISION_HISTORY = {
        "0.0.1": "Initial release",
        "0.0.2": "Keeps polling at minimum of once an hour",
        "0.0.3": "Uses PidRunning instead of deprecated pidIsRunning",
        "0.0.4": "Uses localtime for description of timed triggers"
    };
}


if (ElvOAction.executeCommandLine(ElvOActionSchedule)) {
    ElvOAction.Run(ElvOActionSchedule);
} else {
    module.exports=ElvOActionSchedule;
}
