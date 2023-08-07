const ElvOAction = require("../o-action").ElvOAction;
const ElvOMutex = require("../o-mutex");
const fs = require('fs');
const ElvOFabricClient = require("../o-fabric");


class ElvOActionManagePermissions extends ElvOAction  {

    ActionId() {
        return "manage_permissions";
    };

    Parameters() {
        return {"parameters": {
                identify_by_version: {type: "boolean", required:false, default: false},
                action: {type: "string", required:false, default: "ADD", values:["ADD", "PURGE_EXPIRED", "ADD_FROM_JSON_PROFILE"]}
            }
        };
    };

    IOs(parameters) {
        let inputs = {
            private_key: {type: "password", required: false},
            config_url: {type: "string", required: false}
        };
        if (parameters.action == "ADD") {
            inputs.permissions = {type: "array", required: true};
            inputs.safe_update = {type: "boolean", required: false, default: false};
            /*
            [
              {  object_id: "iq__something", profile: "servicing", action: "add_asset_exception", asset_id: "lala.tif" },
              {  object_id: "iq__something", profile: "servicing", action: "remove_asset_exception", asset_id: "baba.tif" }
            ]
             */
        }
        if (parameters.action == "ADD_FROM_JSON_PROFILE") {
            inputs.json_profile = {type: "string", required: true}; //accept file name prefixed with @
            inputs.permitted_object_ids = {type: "array", required: true};
            inputs.safe_update = {type: "boolean", required: false, default: false};
        }
        if (parameters.action == "PURGE_EXPIRED") {
            inputs.retention_days = {type: "numeric", required: false, default: 7};
            inputs.safe_update = {type: "boolean", required: false, default: false};
        }
        if (!parameters.identify_by_version) {
            inputs.permissions_object_id = {type: "string", required: true};
        } else {
            inputs.permissions_object_version_hash = {type: "string", required: true};
        }
        let outputs = {
            modified_permissions_object_version_hash: {type:"string"},
            object_profile_modified: {type: "array"}
        };
        return {inputs: inputs, outputs: outputs}
    };

    async Execute(handle, outputs) {
        let client;
        try {
            if (!this.Payload.inputs.private_key && !this.Payload.inputs.config_url){
                client = this.Client;
            } else {
                let privateKey = this.Payload.inputs.private_key || this.Client.signer.signingKey.privateKey.toString();
                let configUrl = this.Payload.inputs.config_url || this.Client.configUrl;
                client = await ElvOFabricClient.InitializeClient(configUrl, privateKey)
            }
            let inputs = this.Payload.inputs;
            let objectId = inputs.permissions_object_id;
            let versionHash = inputs.permissions_object_version_hash;
            if (!objectId) {
                objectId = this.Client.utils.DecodeVersionHash(versionHash).objectId;
            }
            let libraryId = await this.getLibraryId(objectId, client);
            if ((this.Payload.parameters.action == "ADD") && (inputs.permissions.length == 0)) {
                this.reportProgress("No permissions to update");
                return ElvOAction.EXECUTION_COMPLETE;
            }
            this.ReportProgress("Retrieving permissions")
            await this.acquireMutex(objectId);
            this.AuthPolicySpec = (await this.getMetadata({client, libraryId,  objectId, versionHash, metadataSubtree: "auth_policy_spec"})) || {};

            if (this.Payload.parameters.action == "ADD") {
                return await this.executeAdd({objectId, libraryId, client, inputs, outputs})
            }
            if (this.Payload.parameters.action == "ADD_FROM_JSON_PROFILE") {
                return await this.executeAddFromJsonProfile({objectId, libraryId, client, inputs, outputs})
            } 
            if (this.Payload.parameters.action == "PURGE_EXPIRED") {
                return await this.executePurge({objectId, libraryId, client, inputs, outputs})
            }

        } catch(err) {
            this.Error("Could not process permissions change for " + (this.Payload.inputs.permissions_object_id || this.Payload.inputs.permissions_object_version_hash), err);
            return ElvOAction.EXECUTION_EXCEPTION;
        } finally {
            this.releaseMutex();
        }
    };

    async executeAdd({objectId, libraryId, client, inputs, outputs}) {
        let count = 0;
        for (let permission of inputs.permissions) {
            if (await this.processPermissionChange(permission)){
                count++;
            }
        }
        if (count == inputs.permissions.length) {
            let writeToken = await this.getWriteToken({objectId, libraryId, client});
            await client.ReplaceMetadata({
                objectId, libraryId, client,
                metadataSubtree: "auth_policy_spec",
                metadata: this.AuthPolicySpec,
                writeToken
            });
            let msg = "Processed "+count+" asset permission changes";
            let response = await this.FinalizeContentObject({
                libraryId: libraryId,
                objectId: objectId,
                writeToken: writeToken,
                commitMessage: msg,
                client
            });
            if (response) {
                this.reportProgress(msg);
                outputs.modified_permissions_object_version_hash = response.hash;
                return ElvOAction.EXECUTION_COMPLETE;
            } else {
                this.reportProgress("Could not finalize "+ objectId, writeToken);
                return ElvOAction.EXECUTION_EXCEPTION;
            }
        } else {
            return ElvOAction.EXECUTION_FAILED;
        }
    };

    async executeAddFromJsonProfile({objectId, libraryId, client, inputs, outputs}) {
        let profile = (inputs.json_profile.match(/^@/)) ? JSON.parse(fs.readFileSync(inputs.json_profile.replace(/^@/,""),"utf-8")) : inputs.json_profile;
        outputs.object_profile_modified = []
        for (let permittedObjectId of inputs.permitted_object_ids) {
            if  (this.executeAddFromJsonProfileForObject({objectId: permittedObjectId, profile})){
                outputs.object_profile_modified.push(permittedObjectId);
            } else {
                this.reportProgress("No changes for "+permittedObjectId);
            }
        }
       
        if (outputs.object_profile_modified.length > 0) {
            let writeToken = await this.getWriteToken({objectId, libraryId, client});
            await client.ReplaceMetadata({
                objectId, libraryId, client,
                metadataSubtree: "auth_policy_spec",
                metadata: this.AuthPolicySpec,
                writeToken
            });
            let msg = "Processed "+outputs.object_profile_modified.length+" asset permission changes";
            let response = await this.FinalizeContentObject({
                libraryId: libraryId,
                objectId: objectId,
                writeToken: writeToken,
                commitMessage: msg,
                client
            });
            if (response) {
                this.reportProgress(msg);
                outputs.modified_permissions_object_version_hash = response.hash;
                return ElvOAction.EXECUTION_COMPLETE;
            } else {
                this.reportProgress("Could not finalize "+ objectId, writeToken);
                return ElvOAction.EXECUTION_EXCEPTION;
            }
        } else {
            return ElvOAction.EXECUTION_FAILED;
        }
    };

    executeAddFromJsonProfileForObject({objectId, profile}) {
        let current = this.AuthPolicySpec[objectId];
        if (!current) {
            current = {};
            this.AuthPolicySpec[objectId] = current;
        }
        if (!current.permissions) {
            current.permissions = [];
        }
        if (!current.profiles) {
            current.profiles = {};
        }
        let changed = false;
        for (let permission of profile.permissions) {
            let existing=false;
            for (let currentPermission of current.permissions) {
                if ((currentPermission.profile == permission.profile) && (currentPermission.subject.id == permission.subject.id)) {
                    this.reportProgress("Found existing profile", currentPermission.profile);
                    existing = true;
                    if (!currentPermission.subject) {
                        currentPermission.subject = permission.subject;
                        changed = true;
                        this.reportProgress("Set subject on existing profile", currentPermission.subject);
                    } else {
                        for (let field in permission.subject) {
                            if (currentPermission.subject[field] != permission.subject[field]) {
                                currentPermission.subject[field] = permission.subject[field];
                                changed = true;
                                this.reportProgress("Modified subject on existing profile, field "+field, currentPermission.subject[field]);
                            }
                        }
                    }
                    break;
                }
            }
            if (!existing) {
                current.permissions.push(permission);
                this.reportProgress("Add permission profile for "+ objectId, permission);
                changed = true;
            }
        }
        for (let profileId in profile.profiles) {
            let currentProfile = current.profiles[profileId];
            let newProfile = profile.profiles[profileId]
            if (!this.areEqual(currentProfile, newProfile)) {
                //we could look at the custom permissions and add 
                current.profiles[profileId] = newProfile;
                changed = true;
            }
        }
        return changed;
    };

    async executePurge({objectId, libraryId, client, inputs, outputs}) {
        let daysKept = inputs.retention_days;
        let authPolicySpec = this.AuthPolicySpec;
        let deadline = new Date((new Date()).getTime() - (daysKept * 24 * 3600 * 1000));
        this.ReportProgress("Cut-off date", deadline);

        let count = 0;
        let kept = 0;
        for (let titleId in authPolicySpec) {
            let permissions = authPolicySpec[titleId].permissions;
            if (permissions) {
                this.reportProgress("filtering permissions",titleId, 1000);
                let permissionsLength = permissions.length;
                authPolicySpec[titleId].permissions = permissions.filter(function (permission) {
                    return (permission && (!permission.end || (permission.end && (new Date(permission.end) >= deadline))));
                });
                permissions = authPolicySpec[titleId].permissions;
                count = count + permissionsLength - permissions.length;
                kept = kept + permissions.length;
            }
        }
        if (count != 0) {
            this.ReportProgress("Proceeding with deletion of " + count + " expired permission entries, keeping " + kept);
            let commitMsg = "Deleted  " + count + " expired permission entries";

            let writeToken = await getWriteToken({
                objectId: objectId,
                libraryId: libraryId
            });
            this.reportProgress("write-token",writeToken);

            await Client.ReplaceMetadata({
                objectId: objectId,
                libraryId: libraryId,
                writeToken: writeToken,
                metadataSubtree: "auth_policy_spec",
                metadata: authPolicySpec
            });

            let response = await Client.FinalizeContentObject({
                objectId: objectId,
                libraryId: libraryId,
                writeToken: writeToken,
                commitMessage: commitMsg
            });
            if (response) {
                this.ReportProgress(commitMsg, response.hash);
                outputs.modified_object_version_hash = response.hash;
                return ElvOAction.EXECUTION_COMPLETE;
            } else {
                this.reportProgress("Could not finalize " + objectId, writeToken);
                return ElvOAction.EXECUTION_EXCEPTION;
            }
        } else {
            return ElvOAction.EXECUTION_FAILED;
        }
    };

    async processPermissionChange(permission) {
        try {
            if (permission.action.toLowerCase() == "add_asset_exception") {
                let objPermission = this.AuthPolicySpec[permission.object_id];
                if (!objPermission) {
                    this.reportProgress("No policies set for " + permission.object_id);
                    return null;
                }
                let profileSpec  =  objPermission.profiles && objPermission.profiles[permission.profile];
                if (!profileSpec) {
                    this.reportProgress("No profile  "+ permission.profile  +" set for " + permission.object_id);
                    return null;
                }
                if (!profileSpec.assets || !profileSpec.assets.default_permission) {
                    this.reportProgress("No assets profile in "+ permission.profile  +" set for " + permission.object_id);
                    return null;
                }
                if (!profileSpec.assets.custom_permissions) {
                    profileSpec.assets.custom_permissions = {};
                }
                profileSpec.assets.custom_permissions[permission.asset_id] = {"permission": "no-access"};
                this.reportProgress("Added permission exception for asset " +permission.asset_id  + " on " + permission.object_id + " for profile " + permission.profile);
                return true;
            }
            if (permission.action.toLowerCase() == "remove_asset_exception") {
                let objPermission = this.AuthPolicySpec[permission.object_id];
                if (!objPermission) {
                    this.reportProgress("No policies set for " + permission.object_id);
                    return null;
                }
                let profileSpec  =  objPermission.profiles && objPermission.profiles[permission.profile];
                if (!profileSpec) {
                    this.reportProgress("No profile  "+ permission.profile  +" set for " + permission.object_id);
                    return null;
                }
                if (!profileSpec.assets || !profileSpec.assets.default_permission) {
                    this.reportProgress("No assets profile in "+ permission.profile  +" set for " + permission.object_id);
                    return null;
                }
                if (profileSpec.assets.custom_permissions) {
                    this.reportProgress("Removed permission exception for asset " +permission.asset_id  + " on " + permission.object_id + " for profile " + permission.profile);
                    delete profileSpec.assets.custom_permissions[permission.asset_id];
                }
                return true;
            }
            throw "Action " + permission.action + " is unsupported";
        } catch(err) {
            let msg = "Could not process " + permission.action + " on " + permission.object_id + " for profile " + permission.profile;
            this.reportProgress(msg);
            this.Error(msg, err);
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

    static VERSION = "0.0.8";
    static REVISION_HISTORY = {
        "0.0.1": "Initial release",
        "0.0.2": "Private key input is encrypted",
        "0.0.3": "Use reworked finalize method",
        "0.0.4": "Adds purge expired permissions action",
        "0.0.5": "Fixes glitches introduced with purge",
        "0.0.7": "Adds option to specify JSON profile",
        "0.0.8": "Support profile with 2 different entries with the same profile name"
    };
}


if (ElvOAction.executeCommandLine(ElvOActionManagePermissions)) {
    ElvOAction.Run(ElvOActionManagePermissions);
} else {
    module.exports=ElvOActionManagePermissions;
}
