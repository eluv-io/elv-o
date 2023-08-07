
const ElvOAction = require("../o-action").ElvOAction;
//const { execSync } = require('child_process');
const ElvOFabricClient = require("../o-fabric");
const ElvOMutex = require("../o-mutex");

class ElvOActionMasterInit extends ElvOAction  {

    ActionId() {
        return "master_init";
    };

    Parameters() {
        return {"parameters": {aws_s3: {type: "boolean"}}};
    };

    PollingInterval() {
        return 60; //poll every minutes
    };

    IOs(parameters) {
        let inputs = {
            production_master_object_id: {type: "string", required:true},
            private_key: {type: "password", required:false},
            config_url: {type: "string", required:false},
            safe_update: {type: "boolean", required: false, default: false}
        };
        if (parameters.aws_s3) {
            inputs.cloud_access_key_id = {type: "string", required:false};
            inputs.cloud_secret_access_key = {type: "string", required:false};
            inputs.cloud_crendentials_path = {type: "file", required:false};
            inputs.cloud_bucket = {type: "string", required:false};
            inputs.cloud_region = {type: "file", required:false};
        }
        let outputs = {
            mezzanine_object_version_hash: {type: "string"}
        };
        return {inputs: inputs, outputs: outputs}
    };

    async Execute(handle, outputs) {
        try {
            let client;
            if (!this.Payload.inputs.private_key && !this.Payload.inputs.config_url) {
                client = this.Client;
            } else {
                let privateKey = this.Payload.inputs.private_key || this.Client.signer.signingKey.privateKey.toString();
                let configUrl = this.Payload.inputs.config_url || this.Client.configUrl;
                client = await ElvOFabricClient.InitializeClient(configUrl, privateKey)
            }
            let access;
            if (this.Payload.parameters.aws_s3) {
                let cloud_access_key_id = this.Payload.inputs.cloud_access_key_id;
                let cloud_secret_access_key = this.Payload.inputs.cloud_secret_access_key;
                let cloud_region = this.Payload.inputs.cloud_region;
                let cloud_bucket = this.Payload.inputs.cloud_bucket;
                let cloud_crendentials_path = this.Payload.inputs.cloud_crendentials_path;
                if (cloud_crendentials_path) {
                    access = JSON.parse(fs.readFileSync(cloud_crendentials_path));
                } else {
                    if (!cloud_region || !cloud_bucket || !cloud_access_key_id || !cloud_secret_access_key) {
                        this.ReportProgress("ERROR - Missing required S3 environment variables: cloud_region, cloud_bucket, cloud_secret_access_key");
                        return -1
                    }
                    access = [
                        {
                            path_matchers: [".*"],
                            remote_access: {
                                protocol: "s3",
                                platform: "aws",
                                path: cloud_bucket + "/",
                                storage_endpoint: {
                                    region: cloud_region
                                },
                                cloud_credentials: {
                                    access_key_id: cloud_access_key_id,
                                    secret_access_key: cloud_secret_access_key
                                }
                            }
                        }
                    ];
                }
            }

            let objectId = this.Payload.inputs.production_master_object_id;
            let libraryId = await this.getLibraryId(objectId, client);

            this.ReportProgress("Initialize production master metadata for object " + objectId);

            let reporter = this;
            ElvOAction.TrackerPath = this.TrackerPath;
            client.ToggleLogging(true, {log: reporter.Debug, error: reporter.Error});

            await this.acquireMutex(objectId);

            let writeToken = await this.getWriteToken({
                libraryId: libraryId,
                objectId: objectId,
                client
            });

            const {data, errors, warnings, logs} = await client.CallBitcodeMethod({
                objectId,
                libraryId,
                method: "/media/production_master/init",
                writeToken,
                body: {access},
                constant: false
            });
            if (logs) {
                for (let i = 0; i < logs.length; i++) {
                    this.ReportProgress(logs[i]);
                }
            }
            if (warnings) {
                for (let i = 0; i < warnings.length; i++) {
                    this.ReportProgress("Bit code warning", warnings[i]);
                }
            }
            if (errors) {
                for (let i = 0; i < errors.length; i++) {
                    this.ReportProgress("Bit code error", errors[i]);
                    this.Error("Bit code error", errors[i])
                }
                throw Error("Bit code error");
            }

            let response = await this.FinalizeContentObject({
                libraryId: libraryId,
                objectId: objectId,
                writeToken: writeToken,
                commitMessage: "Initialized production master metadata",
                client
            });
            outputs.mezzanine_object_version_hash = response.hash;            
            this.releaseMutex();
            this.ReportProgress("Initialized production master metadata");
            return ElvOAction.EXECUTION_COMPLETE;
        } catch(err) {
            this.releaseMutex();
            this.Error("Could not initiliaze production master with handle: "+ handle, err);
            return ElvOAction.EXECUTION_EXCEPTION;
        }
    };


    releaseMutex() {
        if  (this.SetMetadataMutex) {
          ElvOMutex.ReleaseSync(this.SetMetadataMutex); 
          this.ReportProgress("Mutex released");
        }
      };
    
      async acquireMutex(objectId) {
        if  (this.Payload.inputs.safe_update) {
          this.ReportProgress("Reserving mutex");
          this.SetMetadataMutex = await ElvOMutex.WaitForLock({name: objectId, holdTimeout: 120000}); 
          this.ReportProgress("Mutex reserved", this.SetMetadataMutex);
          return this.SetMetadataMutex
        }
        return null;
      };


    static VERSION = "0.0.4";
    static REVISION_HISTORY = {
        "0.0.1": "Initial release",
        "0.0.2": "Private key input is encrypted",
        "0.0.3": "Use reworked finalize method",
        "0.0.4": "Adds option to use a Mutex for safer update"
    };
}


if (ElvOAction.executeCommandLine(ElvOActionMasterInit)) {
    ElvOAction.Run(ElvOActionMasterInit);
} else {
    module.exports=ElvOActionMasterInit;
}
