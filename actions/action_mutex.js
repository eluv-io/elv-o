const ElvOAction = require("../o-action").ElvOAction;
const ElvOMutex = require("../o-mutex");

class ElvOActionMutex extends ElvOAction {

    ActionId() {
        return "mutex";
    };

    Parameters() {
        return {
            parameters: {
                action: {
                    type: "string",
                    values: ["ACQUIRE", "RELEASE"],
                    required: true
                }
            }
        };
    };

    IOs(parameters) {
        let inputs = {};
        let outputs = {};
        if (parameters.action == "ACQUIRE") {
            inputs.name = {type: "string", required: true};
            inputs.immortal = {type: "boolean", required: false, default: false};
            inputs.hold_timeout = {type: "numeric", required: false, default: null};
            inputs.wait_timeout = {type: "numeric", required: false, default: null};
            outputs.mutex = {type: "string"};
        }
        if (parameters.action == "RELEASE") {
            inputs.mutex = {type: "string", required: true};
        }
        return {inputs, outputs};
    };

    async Execute(inputs, outputs) {
        try {
            if (this.Payload.parameters.action == "ACQUIRE") {
                return await this.executeAcquire(inputs, outputs);
            }
            if (this.Payload.parameters.action == "RELEASE") {
                return await this.executeRelease(inputs, outputs);
            }
        } catch(err) {
            this.Error("action_mutex failed", err);
            return ElvOAction.EXECUTION_EXCEPTION;
        }
    };

    async executeAcquire(inputs, outputs) {
        let requestArgs = {
            name: inputs.name,
            immortal: !!inputs.immortal,
            progress_notifyer: {report: (msg) => this.ReportProgress(msg)}
        };
        if (inputs.hold_timeout != null) requestArgs.holdTimeout = inputs.hold_timeout;
        if (inputs.wait_timeout != null) requestArgs.waitTimeout = inputs.wait_timeout;

        this.ReportProgress("Acquiring mutex", inputs.name);
        let mutex = await ElvOMutex.WaitForLock(requestArgs);
        if (!mutex) {
            this.ReportProgress("Timed out waiting for mutex", inputs.name);
            return ElvOAction.EXECUTION_FAILED;
        }
        this.ReportProgress("Mutex acquired", mutex);
        outputs.mutex = mutex;
        return ElvOAction.EXECUTION_COMPLETE;
    };

    async executeRelease(inputs, outputs) {
        this.ReportProgress("Releasing mutex", inputs.mutex);
        let released = ElvOMutex.ReleaseSync(inputs.mutex);
        if (!released) {
            this.ReportProgress("Mutex was not held or already expired", inputs.mutex);
            return ElvOAction.EXECUTION_FAILED;
        }
        this.ReportProgress("Mutex released", inputs.mutex);
        return ElvOAction.EXECUTION_COMPLETE;
    };

};

if (require.main === module) {
    ElvOAction.Run(ElvOActionMutex);
} else {
    module.exports = ElvOActionMutex;
}
