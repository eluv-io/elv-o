const ElvOAction = require("../o-action").ElvOAction;
const { spawn } = require('child_process');
const fs = require("fs");
const os = require("os");
const Path = require("path");

class ElvOActionAsperaClient extends ElvOAction  {

    ActionId() {
        return "aspera_client";
    };

    Parameters() {
        return {
            "parameters": {
                action: {type: "string", values: ["DOWNLOAD"]}
            }
        };
    };

    IOs(parameters) {
        let inputs = {
            remote_server: {type: "string", required: true},
            ssh_port: {type: "numeric", required: false, default: 22},
            remote_user: {type: "string", required: true},
            key_path: {type: "string", required: false, default: "~/.ssh/id_rsa"},
            troughput: {type: "string", required: false, default: "1g"},
            ascp_path: {type: "string", required: false, default: "/Applications/IBM Aspera.app/Contents/Resources/transferd/bin/ascp"}
        };
        let outputs = {};
        if (parameters.action == "DOWNLOAD") {
            inputs.remote_file_path = {type: "string", required: true};
            inputs.local_file_path = {type: "string", required: true};
            outputs.local_file_path = {type: "string"};
        }
        return {inputs: inputs, outputs: outputs};
    };

    PollingInterval() {
        return 30;
    };

    // ascp is invoked directly (not through a shell), so a leading "~" in the key
    // path needs to be expanded here instead of relying on shell expansion.
    resolveKeyPath(keyPath) {
        if (keyPath == "~" || keyPath.startsWith("~/")) {
            return Path.join(os.homedir(), keyPath.slice(1));
        }
        return keyPath;
    };

    buildBaseArgs(inputs) {
        return [
            "-i", this.resolveKeyPath(inputs.key_path),
            "-l", inputs.troughput,
            "-k", "2",
            "-P" + inputs.ssh_port
        ];
    };

    // ascp -i <key_path> -l <troughput> -k 2 -P<ssh_port> remote_user@remote_server:<remote_file_path> <local_file_path>
    // Transfers can run for hours, so the process is monitored asynchronously instead
    // of using spawnSync - ascp's own progress output shows up on stdout or stderr
    // depending on version/platform, so both are watched, throttled to polling_interval
    // (like action_create_spritesheet.js) rather than a fixed delay.
    async executeDownload(inputs, outputs) {
        // ascp (like scp) only lands the transfer inside local_file_path if that path
        // already exists as a directory - otherwise local_file_path itself becomes the
        // transferred item, so this has to be checked before the transfer runs.
        let targetExisted = fs.existsSync(inputs.local_file_path);
        let args = this.buildBaseArgs(inputs);
        args.push(inputs.remote_user + "@" + inputs.remote_server + ":" + inputs.remote_file_path);
        args.push(inputs.local_file_path);
        this.reportProgress("Downloading " + inputs.remote_file_path, args);

        let tracker = this;
        let lastError = "";
        let progressFrequency = (this.Payload.polling_interval || this.PollingInterval() || 60) * 1000;
        try {
            let proc = spawn(inputs.ascp_path, args);

            proc.stdout.setEncoding("utf8");
            proc.stdout.on('data', function(data) {
                tracker.reportProgress("Transfer progress (stdout) " + data.trim(), null, progressFrequency);
            });

            proc.stderr.setEncoding("utf8");
            proc.stderr.on('data', function(data) {
                lastError = data.trim();
                tracker.reportProgress("Transfer progress (stderr) " + data.trim(), null, progressFrequency);
            });

            let executionCode = await new Promise(function(resolve, reject) {
                proc.on('error', reject);
                proc.on('close', resolve);
            });

            if (executionCode != 0) {
                throw new Error("ascp returned exec code " + executionCode + (lastError ? (": " + lastError) : ""));
            }
        } catch (error) {
            this.Error("Failed to download " + inputs.remote_file_path, error);
            return ElvOAction.EXECUTION_EXCEPTION;
        }
        // If local_file_path already existed as a directory, the transferred item landed
        // inside it under its remote basename (same as action_scp_transfer.js). If it
        // didn't exist, ascp created it as the transferred item itself.
        outputs.local_file_path = targetExisted
            ? Path.join(inputs.local_file_path, Path.basename(inputs.remote_file_path))
            : inputs.local_file_path;
        this.reportProgress("Download complete");
        return ElvOAction.EXECUTION_COMPLETE;
    };

    async Execute(inputs, outputs) {
        if (this.Payload.parameters.action == "DOWNLOAD") {
            return await this.executeDownload(inputs, outputs);
        }
        throw new Error("Unsupported action: " + this.Payload.parameters.action);
    };


    static VERSION = "0.0.1";
    static REVISION_HISTORY = {
        "0.0.1": "Initial release with DOWNLOAD via ascp"
    };
}

if (ElvOAction.executeCommandLine(ElvOActionAsperaClient)) {
    ElvOAction.Run(ElvOActionAsperaClient);
} else {
    module.exports=ElvOActionAsperaClient;
}
