const ElvOAction = require("../o-action").ElvOAction;
const ElvOProcess = require("../o-process");
const ElvOFabricClient = require("../o-fabric");
//const { execSync } = require('child_process');


class ElvOActionManageLibrary extends ElvOAction  {
  ActionId() {
    return "manage_library";
  };
  
  
  Parameters() {
    return {parameters: {action: {type: "string", values:["CREATE", "DELETE","LIST_ITEMS","SET_TENANT"], required: true}}};
  };
  
  IOs(parameters) {
    let inputs = {
      private_key: {type: "password", "required":false},
      config_url: {type: "string", "required":false}
    };
    let outputs = {};
    if (parameters.action == "CREATE") {
      inputs.name = {type:"string", required: true};
      inputs.description = {type:"string", required: false, default: ""};
      inputs.metadata = {type: "object", required: false, default: {}};
      inputs.kms_id = {type: "string", "required":false};
      inputs.contributor_groups = {type: "array", required: false, default: []};
      inputs.accessor_groups = {type: "array", required: false, default: []};
      inputs.reviewer_groups = {type: "array", required: false, default: []};
      inputs.content_types = {type: "array", required: false, default: []};
      outputs.library_id =  {type: "string"};
    }
    if (parameters.action == "LIST_ITEMS") {
      inputs.library_id = {type:"string", required: true};
      inputs.select_branches = {type:"array", required: false, default: []};
      inputs.remove_branches = {type:"array", required: false, default: []};
      inputs.limit = {type:"numeric", required: false, default: 30000};
      inputs.filters = {type:"array", required: false, default: []};
      inputs.resolve = {type:"boolean", required: false, default: false};
      outputs.items =  {type: "array"};
    }
    if (parameters.action == "DELETE") {
      inputs.library_id =  {type: "string", required: true};
      inputs.delete_content =  {type: "boolean", required: false, default: false};
      outputs.deleted_content = {type:"array"};
    }
    if ( parameters.action == "SET_TENANT") {
      inputs.library_id =  {type: "string", required: true};
      inputs.tenant_id =  {type: "string", required: true};
      inputs.override = {type: "boolean", required: false, default: false};
      outputs.action_taken = {type: "boolean"};
    }
    if (parameters.action == "SET_GROUP_PERMISSIONS") {
      inputs.contributor_groups = {type: "array", required: false, default: []};
      inputs.accessor_groups = {type: "array", required: false, default: []};
      inputs.reviewer_groups = {type: "array", required: false, default: []};
      inputs.library_id =  {type: "string"};
      outputs.permissions = {type: "object"};
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
    if (this.Payload.parameters.action == "LIST_ITEMS")   {
      return await this.executeListItems(client, outputs);
    }
    if (this.Payload.parameters.action == "SET_TENANT")   {
      return await this.executeSetTenant(client, outputs);
    }
    if (this.Payload.parameters.action == "SET_GROUP_PERMISSIONS") {
      return await this.executeSetGroupPermissions(client, outputs);
    }
    if (this.Payload.parameters.action == "CREATE")   {
      try {
        let libraryId = await this.safeExec("client.CreateContentLibrary", [{
          name: this.Payload.inputs.name,
          description: this.Payload.inputs.description,
          metadata: this.Payload.inputs.metadata,
          kmsId: this.Payload.inputs.kms_id,
          client
        }]);
        outputs.library_id = libraryId;
        
        for (let i=0; i < this.Payload.inputs.contributor_groups.length; i++) {
          await this.addGroupPermissions(libraryId, "contributor", this.Payload.inputs.contributor_groups[i], client);        
        }
        for (let i=0; i < this.Payload.inputs.accessor_groups.length; i++) {
          await this.addGroupPermissions(libraryId, "accessor", this.Payload.inputs.accessor_groups[i], client);           
        }
        for (let i=0; i < this.Payload.inputs.reviewer_groups.length; i++) {
          await this.addGroupPermissions(libraryId, "reviewer", this.Payload.inputs.reviewer_groups[i], client);          
        }
        for (let i=0; i < this.Payload.inputs.reviewer_groups.length; i++) {
          await this.safeExec("client.AddLibraryContentType", [{
            libraryId: libraryId,
            typeId: this.Payload.inputs.content_types[i].match(/^iq__/) || null,
            typeHash: this.Payload.inputs.content_types[i].match(/^hq__/) || null,
            client
          }]);
        }
        
        this.ReportProgress("Library created", outputs.library_id);
        return 100;
      } catch(errExec) {
        this.ReportProgress("Error " + this.Payload.parameters.action);
        this.Error("Manage library " + this.Payload.parameters.action + "  error", errExec);
        return -1;
      }
    }
    if (this.Payload.parameters.action == "DELETE")   {
      return await this.executeDelete(client, outputs)
    }
    this.ReportProgress("Error - Action not supported",this.Payload.parameters.action);
    this.Error("Action not supported: " + this.Payload.parameters.action);
    return -1;
    
  };
  
  async executeSetGroupPermissions(client, outputs) {
    let libraryId = this.Payload.inputs.library_id;       
    for (let i=0; i < this.Payload.inputs.contributor_groups.length; i++) {
      await this.addGroupPermissions(libraryId, "contributor", this.Payload.inputs.contributor_groups[i], client);        
    }
    for (let i=0; i < this.Payload.inputs.accessor_groups.length; i++) {
      await this.addGroupPermissions(libraryId, "accessor", this.Payload.inputs.accessor_groups[i], client);           
    }
    for (let i=0; i < this.Payload.inputs.reviewer_groups.length; i++) {
      await this.addGroupPermissions(libraryId, "reviewer", this.Payload.inputs.reviewer_groups[i], client);          
    }
    outputs.permissions = await client.ContentLibraryGroupPermissions({
      libraryId: libraryId,
      permissions: []
    });
    return ElvOAction.EXECUTION_COMPLETE;
  };
  
  async addGroupPermissions(libraryId, permissionType, groupAddress, client) {
    let permissionSet = false;
    let attempts=0;
    while (attempts < 20 && !permissionSet) {
      attempts++;
      await this.safeExec("client.AddContentLibraryGroup", [{
        libraryId: libraryId,
        groupAddress: groupAddress,
        permission: permissionType,
        client
      }]);
      let foundPermissions = await client.ContentLibraryGroupPermissions({
        libraryId: libraryId,
        permissions: [permissionType]
      });
      if (foundPermissions[groupAddress]) {
        permissionSet = true;
      }
    }
    if (permissionSet) {
      this.ReportProgress("Added " + permissionType + " permissions for group "+ groupAddress, attempts);
    } else {
      throw new Error("Could not add "+ permissionType + " permissions for group "+ groupAddress);
    }
  }
  
  async executeSetTenant(client, outputs) {
    let libraryId = this.Payload.inputs.library_id;
    let contractAddress = client.utils.HashToAddress(libraryId);
    let response  = await client.CallContractMethod({
      contractAddress,
      methodName: "getMeta",
      methodArgs: ["_tenantId"],
    });
    let tenantId =  this.hexToString(response);
    this.ReportProgress("Current Tenant found", tenantId);
    let targetTenant = this.Payload.inputs.tenant_id;
    if (targetTenant == tenantId) {
      this.ReportProgress("Current Tenant value matches target value, skipping...");
      outputs.action_taken = false;
      return ElvOAction.EXECUTION_COMPLETE;
    }
    
    if (response == "0x" || this.Payload.inputs.override) {
      await this.CallContractMethodAndWait({
        contractAddress,
        methodName: "putMeta",
        methodArgs: ["_tenantId", targetTenant],
        client
      });
      response  = await client.CallContractMethod({
        contractAddress,
        methodName: "getMeta",
        methodArgs: ["_tenantId"],
      });
      
      tenantId =  this.hexToString(response);
      this.ReportProgress("Tenant ID changed to " + tenantId);
      if (targetTenant == tenantId) {
        outputs.action_taken = true;
        return ElvOAction.EXECUTION_COMPLETE;
      } else {
        this.ReportProgress("Tenant was not changed to expected value", {expected: targetTenant, found: tenantId});
        return ElvOAction.EXECUTION_EXCEPTION;
      }
    } else {
      this.ReportProgress("Tenant already set to a different value, use override to replace");
      outputs.action_taken = false;
      return ElvOAction.EXECUTION_FAILED;
    }
  };
  
  async executeDelete(client, outputs) {
    try {
      let libraryId = this.Payload.inputs.library_id;
      
      let items = await this.listItems(libraryId, client, {});
      if ((items.length != 0) && !this.Payload.inputs.delete_content) {
        this.ReportProgress("Library not empty");
        return ElvOAction.EXECUTION_EXCEPTION;
      }
      outputs.deleted_content =[]
      for (let item of items) {
        try {
          this.ReportProgress("Removing object: " + item.id);
          await this.safeExec("client.DeleteContentObject", [{
            objectId: item.id,
            libraryId: libraryId,
            client
          }]);
          this.ReportProgress("Object " + item.id + " deleted from " +  libraryId);
          outputs.deleted_content.push(item.id);
        } catch(errDel) {
          this.ReportProgress("Object " + item.id + " deletion failed");
          return ElvOAction.EXECUTION_EXCEPTION;
        }
      }
      
      let libraryAddress = client.utils.HashToAddress(libraryId);
      let permissions  = await client.ContentLibraryGroupPermissions({
        libraryId: libraryId
      });
      let groups = Object.keys(permissions);
      for (let i=0; i < groups.length; i++) {
        let group = groups[i];
        let roles = permissions[group];
        for (let role of roles) {
          this.ReportProgress("Removing "+ role + " permission from group  " + group);
          await this.safeExec("client.RemoveContentLibraryGroup", [{
            libraryId: libraryId,
            groupAddress: group,
            permission: role,
            client: client
          }]);
        }
      }
      
      /*
      let groups, groupsLength;
      groupsLength = await client.CallContractMethod({
        methodName: "contributorGroupsLength",
        contractAddress: libraryAddress,
        methodArgs: []
      });
      if (!Number.isInteger(groupsLength)) {
        groupsLength = parseInt(groupsLength._hex);
      }
      for (let i=0; i < groupsLength; i++) {
        groups.push( await client.CallContractMethod({
          methodName: "contributorGroups",
          contractAddress: libraryAddress,
          methodArgs: [i]
        }) );
      }
      for  (let i=0; i < groupsLength; i++) {
        this.CallContractMethodAndWait({
          methodName: "removeContributorGroup",
          contractAddress: libraryAddress,
          methodArgs: [groups[i]]
        });
      }
      */
      
      //remove visibility from Owner wallet
      let ownerWalletAddress = await client.userProfileClient.WalletAddress();
      let ownerRight = ""+ await client.CallContractMethod({
        methodName: "getLibraryRights",
        contractAddress: ownerWalletAddress,
        methodArgs: [libraryAddress]
      });
      this.ReportProgress("Library owner permission aggregate: " + ownerRight);
      for  (let i=0; i < ownerRight.length; i++) {
        let level = ownerRight.length - 1 - i;
        if (ownerRight.charAt(i) != "0") {
          this.ReportProgress("Removing library owner permission level: " + level);
          await this.safeExec("client.CallContractMethodAndWait", [{
            methodName: "setLibraryRights",
            contractAddress: ownerWalletAddress,
            methodArgs: [libraryAddress, level, 0],
            client: client
          }]);
        }
      }
      
      this.ReportProgress("Library deleted (hidden)", libraryId);
      return  ElvOAction.EXECUTION_COMPLETE;
    } catch(errExec) {
      this.ReportProgress("Error " + this.Payload.parameters.action);
      this.Error("Manage library " + this.Payload.parameters.action + "  error", errExec);
      return  ElvOAction.EXECUTION_EXCEPTION;
    }
  };
  
  async listItems(libId, client, {selectBranches=[], removeBranches=[], limit=30000, resolve=true, filters=[]})  {
    let selectBranchesStr = (selectBranches.length > 0) ? "&select=" + selectBranches.join("&select=") : "";
    let removeBranchesStr = (removeBranches.length > 0) ? "&remove=" + removeBranches.join("&remove=") : "";
    let limitStr = "limit=" + limit;
    let resolveStr = "&resolve=" + ((resolve && "true") || "false");
    let filtersStr = (filters.length > 0) ? "&filter=" + filters.join("&filter=") : "";
    let url = (await this.getFabricUrl(client)) +"/qlibs/" + libId + "/q?"  + limitStr + resolveStr + selectBranchesStr + removeBranchesStr + filtersStr;
    //let token = await this.getLibraryToken(libId, client);
    let token = await this.generateAuthToken(libId, null, false, client); 
    this.Debug("curl -s '" + url + "' -H 'Authorization: Bearer " + token + "'");
    //let stdout = execSync("curl -s '" + url + "' -H 'Authorization: Bearer " + token + "'", {maxBuffer: 100 * 1024 * 1024}).toString();
    //let result = JSON.parse(stdout);
    let result = ElvOFabricClient.fetchJSON(url, {headers: { 'Authorization': "Bearer " + token }});
    if (result && result.errors && result.errors.length > 0) {
      this.ReportProgress("Failed to list items", result);
      return null;
    }
    this.Debug("result.content", result.contents.length);
    return result.contents.map(function(item) {return item.versions[0];});
  };
  
  
  async executeListItems(client, outputs) {
    try {
      let libId = this.Payload.inputs.library_id; 
      let result = await this.listItems(libId, client, {
        selectBranches: this.Payload.inputs.select_branches, 
        removeBranches:this.Payload.inputs.remove_branches,
        limit: this.Payload.inputs.limit,
        resolve: this.Payload.inputs.resolve,
        filters: this.Payload.inputs.filters
      });
      if (!result) {
        return ElvOAction.EXECUTION_EXCEPTION;
      }
      outputs.items = result;
      if (outputs.items.length != 0) {
        return ElvOAction.EXECUTION_COMPLETE;
      } else {
        return ElvOAction.EXECUTION_FAILED;
      }    
    } catch(err) {
      this.ReportProgress("Error listing items");
      this.Error("Error listing items", err);
      return ElvOAction.EXECUTION_EXCEPTION;
    }
  };
  
  hexToString(hexString) {
    let rawHex = hexString.replace(/^0x/,"");
    let result = "";
    for (var i=0, strLen=rawHex.length / 2; i < strLen; i++) {
      result = result + String.fromCharCode(parseInt("0x" + rawHex.substr(i * 2, 2)));
    }
    return result;
  };
  
  static VERSION = "0.0.6";
  static REVISION_HISTORY = {
    "0.0.1": "Initial release",
    "0.0.2": "Private key input is encrypted",
    "0.0.3": "Adds deletion of library's content",
    "0.0.3": "Adds action to set the Tenant ID",
    "0.0.4": "Fixes typo",
    "0.0.5": "Ensures group permissions are actually set as specified",
    "0.0.6": "Does not spawn process for listing"
  };
}


if (ElvOAction.executeCommandLine(ElvOActionManageLibrary)) {
  ElvOAction.Run(ElvOActionManageLibrary);
} else {
  module.exports=ElvOActionManageLibrary;
}
