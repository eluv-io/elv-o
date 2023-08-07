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
        action: {type: "string", values:["CREATE", "DELETE", "DELETE_MULTIPLE"], required: true},
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
    if (parameters.action == "DELETE_MULTIPLE") {
      if (!parameters.identify_by_version) {
        inputs.object_ids =  {type: "array", array_item_type: "string", required: true};
      } else {
        inputs.object_version_hashes = {type: "array", array_item_type: "string", required: true};
      }
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
      client = await ElvOFabricClient.InitializeClient(configUrl, privateKey)
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
          commitMessage: "Created by O ("+ handle +")",
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
      for (let objectId  of objectIds) {
        try {
          let objectIdOutputs = {};
          let result = await this.executeDelete({client, objectId, outputs: objectIdOutputs});
          outputs.status_codes.push(result);
          outputs.library_ids.push(objectIdOutputs.library_id);
          if (result == ElvOAction.EXECUTION_EXCEPTION) {
            overallStatus = ElvOAction.EXECUTION_EXCEPTION
          }
          if ((result == ElvOAction.EXECUTION_COMPLETE) && (overallStatus != ElvOAction.EXECUTION_EXCEPTION)){
            overallStatus = ElvOAction.EXECUTION_COMPLETE;
          }
        } catch(errProcessing) {
          this.reportProgress("Processing iteration exception for object", objectId);
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
      let owner = await client.CallContractMethod({
        contractAddress: client.utils.HashToAddress(objectId),
        methodName: "owner"        
      });     
      
      if (owner.toLowerCase() != client.signer.address.toLowerCase()) {
        this.Debug("Owner not caller");
        throw (new Error("Object can not deleted, only owner can delete"));
      }
      
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
        owner = await client.CallContractMethod({
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
  
  static VERSION = "0.0.7";
  static REVISION_HISTORY = {
    "0.0.1": "Initial release",
    "0.0.2": "Private key input is encrypted",
    "0.0.3": "Adds option to create with a non-zero visibility",
    "0.0.4": "Deleting inexistant object returns failed instead of exception",
    "0.0.5": "Avoids losing name if description is not specified, adds option to set ip_title_id",
    "0.0.6": "Normalized logging",
    "0.0.7": "Verifies permissions after grants"
  };
}


if (ElvOAction.executeCommandLine(ElvOActionManageObject)) {
  ElvOAction.Run(ElvOActionManageObject);
} else {
  module.exports=ElvOActionManageObject;
}
