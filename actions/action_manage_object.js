const ElvOAction = require("../o-action").ElvOAction;
const ElvOProcess = require("../o-process");
const ElvOFabricClient = require("../o-fabric");



class ElvOActionManageObject extends ElvOAction  {
  ActionId() {
    return "manage_object";
  };
  
  
  Parameters() {
    return {
      parameters: {
        action: {type: "string", values:["CREATE", "DELETE", "DELETE_MULTIPLE", "GET_OWNER", "FINALIZE"], required: true},
        identify_by_version: {type: "boolean", required:false, default: false}
      }
    };
  };
  
  IOs(parameters) {
    let inputs = {
      private_key: {type: "password", "required":false},
      config_url: {type: "string", "required":false}
    };
    let outputs = {};
    if (parameters.action == "FINALIZE") {
      inputs.write_token = {type:"string", required: true};
      inputs.library_id = {type:"string", required: false};
      inputs.object_id = {type:"string", required: false};
      inputs.commit_message = {type:"string", required: false};
      outputs.version_hash = {type:"string"};
    }
    if (parameters.action == "CREATE") {
      inputs.library_id = {type:"string", required: true};
      inputs.name = {type:"string", required: true};
      inputs.ip_title_id = {type:"string", required: false};
      inputs.description = {type:"string", required: false, default: ""};
      inputs.metadata = {type: "object", required: false, default: {}};
      inputs.editor_groups = {type: "array", required: false, default: []};
      inputs.accessor_groups = {type: "array", required: false, default: []};
      inputs.content_type = {type: "string", required: false, default: null};
      inputs.visibility = {type: "numeric", required: false, default: 0};
      inputs.owner_address = {type: "string", required: false, default: null};
      outputs.object_id =  {type: "string"};
      outputs.object_version_hash = {type: "string"};
    }
    if (parameters.action == "DELETE") {
      if (!parameters.identify_by_version) {
        inputs.object_id =  {type: "string", required: true};
      } else {
        inputs.object_version_hash = {type: "string", required: true};
      }
      outputs.library_id = {type: "string"};
    }
    if (parameters.action == "GET_OWNER") {
      if (!parameters.identify_by_version) {
        inputs.object_id =  {type: "string", required: true};
      } else {
        inputs.object_version_hash = {type: "string", required: true};
      }
      outputs.owner_address = {type: "string"};
    }
    if (parameters.action == "DELETE_MULTIPLE") {
      if (!parameters.identify_by_version) {
        inputs.object_ids =  {type: "array", array_item_type: "string", required: true};
      } else {
        inputs.object_version_hashes = {type: "array", array_item_type: "string", required: true};
      }
      inputs.private_keys_set = {type: "array", array_item_type: "password", required: false, default:null};//Would not work if actual encoded password are provided as array are not read in current version of engine
      outputs.library_ids = {type: "array"};
      outputs.status_codes = {type: "array"};
    }
    return {inputs, outputs};
  };
  
  async Execute(handle, outputs) {
    let client;
    let privateKey;
    let configUrl;
    if (!this.Payload.inputs.private_key && !this.Payload.inputs.config_url) {
      client = this.Client;
    } else {
      privateKey = this.Payload.inputs.private_key || this.Client.signer.signingKey.privateKey.toString();
      configUrl = this.Payload.inputs.config_url || this.Client.configUrl;
      client = await ElvOFabricClient.InitializeClient(configUrl, privateKey);
    }
    if (this.Payload.parameters.action == "FINALIZE")   {
      //let tokenData = client.DecodeWriteToken(this.Payload.inputs.write_token); //NOT IMPLEMENTED YET
      let objectId = this.Payload.inputs.object_id;
      if (!objectId) {
        throw new Error("Not implemented yet the parsing of the write")
      }
      let libraryId = this.Payload.inputs.lirabry_id || (await this.getLibraryId(objectId, client));
      
      this.ReportProgress("Finalizing content object", {
        objectId, libraryId,
        writeToken: this.Payload.inputs.write_token,
        commitMessage: this.Payload.inputs.commit_message
      });
      let result = await this.FinalizeContentObject({
        client,
        objectId, libraryId,
        writeToken: this.Payload.inputs.write_token,
        commitMessage: this.Payload.inputs.commit_message
      });
      if (result && result.hash) {
        this.ReportProgress("Finalized content object", result);
        outputs.version_hash = result.hash;
        return ElvOAction.EXECUTION_COMPLETE;
      }
      this.ReportProgress("Failed to finalize content object", result);
      return ElvOAction.EXECUTION_EXCEPTION;
    }
    
    if (this.Payload.parameters.action == "GET_OWNER")   {
      let versionHash = this.Payload.inputs.object_version_hash;
      let objectId = this.Payload.inputs.object_id;
      if (!objectId) {
        objectId = this.Client.utils.DecodeVersionHash(versionHash).objectId;
      }
      return await this.executeGetOwner({client, objectId, outputs});
    }
    if (this.Payload.parameters.action == "CREATE")   {
      try {
        if (!this.Payload.inputs.metadata) {
          this.Payload.inputs.metadata = {};
        }
        if (!this.Payload.inputs.metadata.public) {
          this.Payload.inputs.metadata.public = {};
        }
        this.Payload.inputs.metadata.public.name = this.Payload.inputs.name;
        if (this.Payload.inputs.description) {
          this.Payload.inputs.metadata.public.description = this.Payload.inputs.description;
        }
        if (this.Payload.inputs.ip_title_id) {
          if (!this.Payload.inputs.metadata.public.asset_metadata) {
            this.Payload.inputs.metadata.public.asset_metadata = {};
          }
          this.Payload.inputs.metadata.public.asset_metadata.ip_title_id = this.Payload.inputs.ip_title_id;
        }
        
        
        let response = await this.safeExec("client.CreateAndFinalizeContentObject", [{
          name: this.Payload.inputs.name,
          libraryId: this.Payload.inputs.library_id,
          options: {
            meta: this.Payload.inputs.metadata,
            type: this.Payload.inputs.content_type,
            visibility: this.Payload.inputs.visibility
          },
          commitMessage: "Created by O",
          client
        }]);
        let objectId = response.id;
        outputs.object_id = objectId;
        outputs.object_version_hash = response.hash;
        
        for (let i=0; i < this.Payload.inputs.editor_groups.length; i++) {
          await this.grantRights(client, objectId, this.Payload.inputs.editor_groups[i], 2 ); 
          this.ReportProgress("Added editor group", this.Payload.inputs.editor_groups[i]);
        }
        for (let i=0; i < this.Payload.inputs.accessor_groups.length; i++) {
          await this.grantRights(client, objectId, this.Payload.inputs.accessor_groups[i], 1 ); 
          this.ReportProgress("Added accessor group", this.Payload.inputs.accessor_groups[i]);
        }
        if (this.Payload.inputs.owner_address && (client.CurrentAccountAddress().toLowerCase() != this.Payload.inputs.owner_address.toLowerCase())) {
          for (let attempt=0; attempt < 5; attempt++) {
            try {
              this.ReportProgress("Transfer ownership", this.Payload.inputs.owner_address);
              await this.CallContractMethodAndWait({
                contractAddress: client.utils.HashToAddress(objectId),
                methodName: "transferOwnership",
                methodArgs: [this.Payload.inputs.owner_address],
                client
              });
              let owner = await client.CallContractMethod({
                contractAddress: client.utils.HashToAddress(objectId),
                methodName: "owner"        
              });
              if (owner.toLowerCase() == this.Payload.inputs.owner_address.toLowerCase()) {
                this.ReportProgress("Transfer of ownership successful", owner);
                break;
              } else {
                this.ReportProgress("Transfer of ownership failed", owner);
              }
            } catch(errT) {
              this.Error("Error transferring ownership", errT);
            }
          }
        }
        
        
        this.ReportProgress("Object created", outputs.object_id);
        return 100;
      } catch(errExec) {
        this.Error("Manage object " + this.Payload.parameters.action + "  error", errExec);
        return -1;
      }
    }
    if (this.Payload.parameters.action == "DELETE")   {    
      let versionHash = this.Payload.inputs.object_version_hash;
      let objectId = this.Payload.inputs.object_id;
      if (!objectId) {
        objectId = this.Client.utils.DecodeVersionHash(versionHash).objectId;
      }
      return await this.executeDelete({client, objectId, outputs});
    }
    
    if (this.Payload.parameters.action == "DELETE_MULTIPLE") {
      let versionHashes = this.Payload.inputs.object_version_hashes;
      let objectIds = this.Payload.inputs.object_ids;
      if (!objectIds) {
        objectIds = versionHashes.map(function(item){return this.Client.utils.DecodeVersionHash(versionHash).objectId});
      }
      outputs.status_codes = [];
      outputs.library_ids = [];
      let overallStatus = ElvOAction.EXECUTION_FAILED;
      let privateKeys;
      
      for (let objectId  of objectIds) {
        try {
          let objectIdOutputs = {};
          let result = await this.executeDelete({client, objectId, outputs: objectIdOutputs});
          if ((result < 0) && this.Payload.inputs.private_keys_set) {
            this.reportProgress("Deletion with main key failed, trying with pooled key")
            let owner = (await client.CallContractMethod({
              contractAddress: client.utils.HashToAddress(objectId),
              methodName: "owner"        
            })).toLowerCase();
            if (!privateKeys) {
              privateKeys = {};
              for (let pKey of this.Payload.inputs.private_keys_set) {
                configUrl = this.Payload.inputs.config_url || this.Client.configUrl;
                let keyClient = await ElvOFabricClient.InitializeClient(configUrl, pKey);
                let keyAddress = keyClient.signer.address.toLowerCase();
                //console.log(keyAddress +"->"+pKey);
                privateKeys[keyAddress] = keyClient;
              }
            }
            let clientObj = privateKeys[owner];
            if (!clientObj) {
              throw new Error("Owner key not present in provided set for" +objectId);
            }
            result = await this.executeDelete({client: clientObj, objectId, outputs: objectIdOutputs});
          }
          
          outputs.status_codes.push(result);
          outputs.library_ids.push(objectIdOutputs.library_id);
          if (result == ElvOAction.EXECUTION_EXCEPTION) {
            overallStatus = ElvOAction.EXECUTION_EXCEPTION
          }
          if ((result == ElvOAction.EXECUTION_COMPLETE) && (overallStatus != ElvOAction.EXECUTION_EXCEPTION)){
            overallStatus = ElvOAction.EXECUTION_COMPLETE;
          }
        } catch(errProcessing) {
          this.reportProgress("Processing iteration exception for object "+ objectId, errProcessing);
          outputs.status_codes.push(ElvOAction.EXECUTION_EXCEPTION);
          overallStatus = ElvOAction.EXECUTION_EXCEPTION;
        }
      }
      return overallStatus;
    }
    this.Error("Action not supported: " + this.Payload.parameters.action);
    return -1;  
  };
  
  async grantRights(client, objectId, groupAddress, accessLevel) { //accessLevel 1 for access,  2 for edit
    let attempt = 0;  
    let objAddress = client.utils.HashToAddress(objectId);
    while (attempt < 5)  {
      attempt ++;
      await this.CallContractMethodAndWait({
        contractAddress: groupAddress,
        methodName: "setContentObjectRights",
        methodArgs: [objAddress, accessLevel, 1], //EDIT rights
        client
      });
      let hasRights = await client.CallContractMethod({
        contractAddress: groupAddress,
        methodName: "checkDirectRights",
        methodArgs: [1, objAddress, accessLevel]
      });
      if (hasRights) {
        this.reportProgress("Granted rights to group " + groupAddress);        
        return true;
      } else {
        this.reportProgress("Failed to grant rights to group "+ groupAddress, attempt); 
        await this.sleep(100);
      }
    }
    throw Error("Could not grant rights to " + groupAddress);       
  };
  
  
  async executeDelete({client, objectId, outputs})  {
    this.ReportProgress("Processing object: " + objectId);
    try {     
      
      let libraryId = await this.getLibraryId(objectId, client);
      if (!libraryId) {
        throw (new Error("Wrong network or deleted item"));
      }
      this.ReportProgress("Removing object: " + objectId);
      
      await this.safeExec("client.DeleteContentObject", [{
        objectId: objectId,
        libraryId: libraryId,
        client
      }]);  
      outputs.library_id = libraryId;
      
      //check if item exists
      this.Debug("Check if item exists");
      try {
        let owner = await client.CallContractMethod({
          contractAddress: client.utils.HashToAddress(objectId),
          methodName: "owner"        
        });
      } catch(errCheck) {
        this.ReportProgress("Object " + objectId + " deleted from " +  libraryId);
        return ElvOAction.EXECUTION_COMPLETE;       
      }
      throw (new Error("Object was not deleted"));
      
    } catch(errExec) {
      if (errExec.message && errExec.message.match(/Wrong network or deleted item/)) {
        this.Error("Nothing to delete: Object "+ objectId + " has already been deleted", errExec.message);
        return ElvOAction.EXECUTION_FAILED;
      } else {
        this.Error("Manage object " + this.Payload.parameters.action + "  error", errExec);
        return ElvOAction.EXECUTION_EXCEPTION;
      }
    }
  }
  
  async executeGetOwner({client, objectId, outputs})  {
    this.ReportProgress("Processing object: " + objectId);
    try {     
      let owner = await client.CallContractMethod({
        contractAddress: client.utils.HashToAddress(objectId),
        methodName: "owner"        
      });     
      outputs.owner_address = owner;      
    } catch(errExec) {
      this.Error("Manage object " + this.Payload.parameters.action + "  error", errExec);
      return ElvOAction.EXECUTION_EXCEPTION;
    }
  }
  
  
  static VERSION = "0.1.1";
  static REVISION_HISTORY = {
    "0.0.1": "Initial release",
    "0.0.2": "Private key input is encrypted",
    "0.0.3": "Adds option to create with a non-zero visibility",
    "0.0.4": "Deleting inexistant object returns failed instead of exception",
    "0.0.5": "Avoids losing name if description is not specified, adds option to set ip_title_id",
    "0.0.6": "Normalized logging",
    "0.0.7": "Verifies permissions after grants",
    "0.0.8": "Adds support for key set to allow deletion of legacy objects",
    "0.0.9": "Adds option to transfer object ownership after creation",
    "0.1.0": "Removes the handle from the creation commit message",
    "0.1.1": "Only transfer ownership if transfer address is different from current address"
  };
}


if (ElvOAction.executeCommandLine(ElvOActionManageObject)) {
  ElvOAction.Run(ElvOActionManageObject);
} else {
  module.exports=ElvOActionManageObject;
}
