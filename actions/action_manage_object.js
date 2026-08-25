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
        action: {
          type: "string", 
          values:[
            "CREATE", "DELETE", "DELETE_MULTIPLE", "DELETE_BY_IP_TITLE_ID", "GET_OWNER", "FINALIZE",
            "SET_GROUP_PERMISSIONS", "LIST_VERSIONS", "CHECK_LINKS", "COPY_CAPS", "LIST_PARTS", "SET_PERMISSION",
            "OPEN_WRITE_TOKEN"
          ], 
          required: true
        },
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
    if (parameters.action == "OPEN_WRITE_TOKEN") {
       inputs.object_id = {type:"string", required: true};
       outputs.write_token = {type:"string"};
       outputs.config_url = {type:"string"};
    }

    if (parameters.action == "SET_PERMISSION") {
      inputs.object_id = {type:"string", required: true};
      inputs.permission = {type:"string", required: true, values: [ 'OWNER', 'EDITABLE', 'VIEWABLE', 'LISTABLE', 'PUBLIC' ]};
      inputs.write_token = {type:"string", required: false};
      inputs.finalize = {type:"boolean", required: false, default: true};
      inputs.commit_message = {type:"string", required: false};
      outputs.object_version_hash = {type:"string", conditional: true};
      outputs.write_token = {type:"string", conditional: true};
      outputs.node_url = {type:"string", conditional: true};
      outputs.config_url = {type:"string", conditional: true};
       outputs.commit_message = {type:"string", conditional: true};
    }
    if (parameters.action =="LIST_PARTS"){
      if (parameters.identify_by_version) {
        inputs.object_version_hash = {type: "string", required: true};
      } else{
        inputs.object_id = {type:"string", required: true};
        inputs.write_token = {type:"string", required: false};
      }
      outputs.parts = {type: "array"};
      outputs.total_parts_size = {type: "numeric"};
    }
    if (parameters.action == "LIST_VERSIONS") {
      inputs.object_id = {type:"string", required: true};
      outputs.versions = {type: "array"};
    }
    if (parameters.action == "COPY_CAPS"){
      inputs.source_object_id = {type:"string", required: true};
      inputs.object_id = {type:"string", required: true};
      outputs.version_hash = {type:"string"};
    }
    if (parameters.action == "READ_CAPS"){
      inputs.object_id = {type:"string", required: true};
      outputs.version_hash = {type:"string"};
    }
    if (parameters.action == "FINALIZE") {
      inputs.write_token = {type:"string", required: true};
      inputs.library_id = {type:"string", required: false};
      inputs.object_id = {type:"string", required: false};
      inputs.commit_message = {type:"string", required: false};
      outputs.version_hash = {type:"string"};
    }
    if (parameters.action == "SET_GROUP_PERMISSIONS") {
      inputs.object_id =  {type: "string", required: true};
      inputs.private_keys_set = {type: "array", "required": false};
      inputs.group = {type:"string", required: true};
      inputs.permission_type = {type:"string", required: true, values: ["see", "access", "manage"]};
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
      inputs.create_kms_conk = {type: "boolean", require: false, default: false};
      inputs.no_encryption_conk = {type: "boolean", require: false, default: false};
      inputs.copy_caps_from = {type: "string", required: false, default: null};
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
    if (parameters.action == "DELETE_BY_IP_TITLE_ID") {
      inputs.ip_title_id =  {type: "string", required: true};    
      inputs.library_id = {type: "string", required: true}; 
      outputs.object_ids = {type: "array"};
    }
    if (parameters.action == "CHECK_LINKS") {
      return {
        inputs: {
          object_id: {type: "string", required:true},
        },
        outputs: {
          bad_links: "array"
        }
      };
    }
    return {inputs, outputs};
  };
  
  async Execute(inputs, outputs) {
    let client;
    let privateKey;
    let configUrl;
    if (!this.Payload.inputs.private_key && !this.Payload.inputs.config_url) {
      client = this.Client;
      configUrl = this.Client.configUrl;
    } else {
      privateKey = this.Payload.inputs.private_key || this.Client.signer.signingKey.privateKey.toString();
      configUrl = this.Payload.inputs.config_url || this.Client.configUrl;
      client = await ElvOFabricClient.InitializeClient(configUrl, privateKey);
    }
    if (!client) {
      throw new Error("Could not initilize client");
    }
    if (this.Payload.parameters.action == "OPEN_WRITE_TOKEN") {
      return await this.executeOpenWriteToken(client, inputs, outputs);     
    }
    if (this.Payload.parameters.action == "SET_PERMISSION") {
      return await this.executeSetPermission(client, inputs, outputs);     
    }
    if (this.Payload.parameters.action == "LIST_PARTS") {
      return await this.executeListParts(client, inputs, outputs, configUrl);     
    }
    if (this.Payload.parameters.action == "COPY_CAPS")   {
      return await this.executeCopyCaps(client, inputs, outputs, configUrl);      
    }
    if (this.Payload.parameters.action == "READ_CAPS")   {
      return await this.executeReadCaps(client, inputs, outputs, configUrl);      
    }
    if (this.Payload.parameters.action == "CHECK_LINKS")   {
      return await this.executeCheckLinks(client, inputs, outputs, configUrl);
      
    }
    if (this.Payload.parameters.action == "SET_GROUP_PERMISSIONS")   {
      return await this.executeSetGroupPermissions(client, inputs, outputs, configUrl);
      
    }
    if (this.Payload.parameters.action == "LIST_VERSIONS") {
      return await this.executeListVersions(client, inputs, outputs);
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
    if (this.Payload.parameters.action == "DELETE_BY_IP_TITLE_ID")   {
      let versionHash = this.Payload.inputs.object_version_hash;
      return await this.executeDeleteByIpTitleId({client, inputs, outputs});
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
      return await this.executeCreate(client, inputs, outputs);
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
        
        if (this.Payload.inputs.copy_caps_from) {
          let sourceLibraryId = await this.getLibraryId(inputs.source_object_id, client);
          let objectId = inputs.object_id;
          let libraryId = await this.getLibraryId(inputs.object_id, client);    
          let metadata = await this.getMetadata({client, objectId: inputs.source_object_id, libraryId: sourceLibraryId});
          //const permission = await this.Permission({objectId: originalObjectId});
          // User CAP
          const userCapKey = `eluv.caps.iusr${client.utils.AddressToHash(client.signer.address)}`;
          if (metadata[userCapKey]) {
            let writeToken = await this.getWriteToken({client, objectId, libraryId});
            let existingMeta = await this.getMetadata({client, objectId, libraryId});
            const userConkKey = await client.Crypto.DecryptCap(metadata[userCapKey], inputs.private_key);
            userConkKey.qid = objectId;
            
            for (let key in existingMeta) { // Delete existing keys if any
              if (key.startsWith("eluv.caps")) {
                this.reportProgress("Deleting existing caps found", key);
                await client.DeleteMetadata({
                  libraryId,
                  objectId,
                  writeToken,
                  metadataSubtree: key
                });
              }
            }
            await client.ReplaceMetadata({
              libraryId,
              objectId,
              writeToken,
              metadataSubtree: userCapKey,
              metadata: await client.Crypto.EncryptConk(userConkKey, client.signer._signingKey().publicKey)
            });
            outputs.imported_caps = userCapKey;
          }
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
        this.reportProgress("Created object", objectId);
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
              throw new Error("Owner key not present in provided set for " +objectId);
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
  
  async executeCreate(client, inputs, outputs) {
    try {
      if (!inputs.metadata) {
        inputs.metadata = {};
      }
      if (!inputs.metadata.public) {
        inputs.metadata.public = {};
      }
      inputs.metadata.public.name = inputs.name;
      if (inputs.description) {
        inputs.metadata.public.description = inputs.description;
      }
      if (inputs.ip_title_id) {
        if (!inputs.metadata.public.asset_metadata) {
          inputs.metadata.public.asset_metadata = {};
        }
        inputs.metadata.public.asset_metadata.ip_title_id = inputs.ip_title_id;
      }
      
      let objectId;
      if (this.Payload.inputs.copy_caps_from) {
        let response = await client.CreateContentObject({
          name: inputs.name,
          libraryId: inputs.library_id,
          options: {
            meta: inputs.metadata,
            type: inputs.content_type,
            visibility: inputs.visibility,
            noEncryptionConk: true
          },
          commitMessage: "Created by O",
          client
        });
        objectId = response.id;
        let writeToken = response.write_token;
        let sourceData = await this.getMetadata({client, objectId: inputs.copy_caps_from});
        //const permission = await this.Permission({objectId: originalObjectId});
        // User CAP
        const userCapKey = `eluv.caps.iusr${client.utils.AddressToHash(client.signer.address)}`;
        if (sourceData[userCapKey]) {
          const userConkKey = await client.Crypto.DecryptCap(sourceData[userCapKey], inputs.private_key);
          userConkKey.qid = objectId;
          
          await client.ReplaceMetadata({
            libraryId: inputs.library_id,
            objectId,
            writeToken,
            metadataSubtree: userCapKey,
            metadata: await client.Crypto.EncryptConk(userConkKey, client.signer._signingKey().publicKey)
          });
          if (inputs.create_kms_conk){
            await client.CreateEncryptionConk({libraryId: inputs.library_id, objectId, writeToken, createKMSConk: true});
          }
          outputs.imported_caps = userCapKey;
        } else {
          this.ReportProgress("No Caps found in source to match provided key", userCapKey);
          return ElvOAction.EXECUTION_EXCEPTION;
        }
        response = await this.FinalizeContentObject({
          objectId,
          libraryId: inputs.library_id,
          writeToken,
          commitMessage: "Created by O",
          client
        });
        if (response?.hash) {
          outputs.object_id = response.id;
          outputs.object_version_hash = response.hash;
        } else  {
          this.ReportProgress("Could not finalize object", response);
          return ElvOAction.EXECUTION_EXCEPTION;
        }
      } else {        
        let response = await client.CreateAndFinalizeContentObject({
          name: inputs.name,
          libraryId: inputs.library_id,
          options: {
            meta: inputs.metadata,
            type: inputs.content_type,
            visibility: inputs.visibility,
            createKMSConk: inputs.create_kms_conk,
            noEncryptionConk: inputs.no_encryption_conk
          },
          commitMessage: "Created by O",
          client
        });
        objectId = response.id;
        outputs.object_id = objectId;
        outputs.object_version_hash = response.hash;
      }
      this.reportProgress("Created object", objectId);
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
  
  async executeListVersions(client, inputs, outputs) {
    let objectId = inputs.object_id;
    let libraryId = await this.getLibraryId(objectId, client);
    let versions = await client.ContentObjectVersions({libraryId, objectId});
    outputs.versions = [];
    for (let version of versions.versions) {
      let meta = await this.getMetadata({client, libraryId, versionHash: version.hash, metadataSubtree: "commit"});
      outputs.versions.push({hash: version.hash, commit_message: meta.message, commit_timestamp: meta.timestamp});
    }
    return ElvOAction.EXECUTION_COMPLETE;
  };
  
  async executeReadCaps(client, inputs, outputs) {
    let objectId = inputs.object_id;
    let libraryId = await this.getLibraryId(inputs.object_id, client);    
    let metadata = await this.getMetadata({client, objectId, libraryId});
    //const permission = await this.Permission({objectId: originalObjectId});
    // User CAP
    if (inputs.caps_key) {
      if (metadata[inputs.caps_key]) {
        try {
          const userConkKey = await client.Crypto.DecryptCap(metadata[inputs.caps_key], inputs.private_key);
          outputs.conk_key = userConkKey;      
          return ElvOAction.EXECUTION_COMPLETE;
        } catch(err) {
          this.ReportProgress("Could not decrypt caps for key", inputs.caps_key);
          return ElvOAction.EXECUTION_EXCEPTION;
        }
      } else {
        this.ReportProgress("No caps found matching key", inputs.caps_key);
        return ElvOAction.EXECUTION_EXCEPTION;
      }
    }
    const userCapKey = `eluv.caps.iusr${client.utils.AddressToHash(client.signer.address)}`;
    outputs.user_cap_key = userCapKey;
    if (metadata[userCapKey]) {
      const userConkKey = await client.Crypto.DecryptCap(metadata[userCapKey], inputs.private_key);
      outputs.user_conk_key = userConkKey;      
      return ElvOAction.EXECUTION_COMPLETE;
    } else {
      this.ReportProgress("No caps found matching key", userCapKey);
      return ElvOAction.EXECUTION_EXCEPTION;
    }
  };
  
  async executeOpenWriteToken(client, inputs, outputs)  { //OPEN_WRITE_TOKEN
    outputs.write_token = await this.getWriteToken({objectId: inputs.object_id, client})
    if (client.HttpClient.draftURIs[outputs.write_token]) {
            outputs.node_url = "https://" + client.HttpClient.draftURIs[outputs.write_token].hostname() + "/";
            outputs.config_url = "https://" + client.HttpClient.draftURIs[outputs.write_token].hostname() + "/config?self&qspace=main";
            return ElvOAction.EXECUTION_COMPLETE;
    }
    this.reportProgress("Could not find node for write-token", outputs.write_token);
    return ElvOAction.EXECUTION_EXCEPTION;
  }

  async executeSetPermission(client, inputs, outputs)  { //SET_PERMISSION
    let libraryId = await this.getLibraryId(inputs.object_id, client);
    let writeToken = inputs.write_token || (await this.getWriteToken({objectId: inputs.object_id, libraryId, client}));
    await client.SetPermission({
      objectId: inputs.object_id,
      libraryId,
      writeToken: writeToken,
      permission: inputs.permission.toLowerCase()
    });
    this.reportProgress("Setting permissions", inputs.permission.toLowerCase());
    if (inputs.finalize) {
      let result = await this.FinalizeContentObject({
        client, objectId: inputs.object_id,
        libraryId,
        writeToken: writeToken,
        commitMessage: inputs.commit_message || ("Permissions set to "+ inputs.permission.toLowerCase())
      });
      if (result?.hash) {
        outputs.object_version_hash = result.hash;
        return ElvOAction.EXECUTION_COMPLETE;
      } else {
        this.ReportProgress("Could not finalize permission change", result);
        return ElvOAction.EXECUTION_EXCEPTION;
      }
    } else {
      outputs.write_token = writeToken;
      if (client.HttpClient.draftURIs[writeToken]) {
        outputs.node_url = "https://" + client.HttpClient.draftURIs[writeToken].hostname() + "/";
        outputs.config_url = "https://" + client.HttpClient.draftURIs[writeToken].hostname() + "/config?self&qspace=main";
        outputs.commit_message = "Permissions set to "+ inputs.permission.toLowerCase();
      } else {
        throw new Error("Could not get node for writeToken");
      }
    }
  };
  
  async executeListParts(client, inputs, outputs) { //write_token not supported yet in the elv-client-js
    if (inputs.write_token){
      throw "write_token not supported yet in the elv-client-js";
    }
    let objectId = inputs.object_id;
    if (!objectId) {
      objectId = this.Client.utils.DecodeVersionHash(inputs.object_version_hash).objectId;
    }
    let libraryId = await this.getLibraryId(objectId, client);
    console.log("params", {libraryId, objectId, versionHash: inputs.object_version_hash, writeToken: inputs.write_token});
    let parts = await client.ContentParts({libraryId, objectId, versionHash: inputs.object_version_hash, writeToken: inputs.write_token});
    if (parts && parts.length) {
      outputs.parts = parts;
      outputs.total_parts_size = 0;
      for (let part of parts) {
        outputs.total_parts_size += part.size;
      }
      return ElvOAction.EXECUTION_COMPLETE;
    }
    return ElvOAction.EXECUTION_FAILED;
  }
  
  async executeCopyCaps(client, inputs, outputs) {
    let sourceLibraryId = await this.getLibraryId(inputs.source_object_id, client);
    let objectId = inputs.object_id;
    let libraryId = await this.getLibraryId(inputs.object_id, client);    
    let metadata = await this.getMetadata({client, objectId: inputs.source_object_id, libraryId: sourceLibraryId});
    //const permission = await this.Permission({objectId: originalObjectId});
    // User CAP
    const userCapKey = `eluv.caps.iusr${client.utils.AddressToHash(client.signer.address)}`;
    if (metadata[userCapKey]) {
      let writeToken = await this.getWriteToken({client, objectId, libraryId});
      let existingMeta = await this.getMetadata({client, objectId, libraryId});
      const userConkKey = await client.Crypto.DecryptCap(metadata[userCapKey], inputs.private_key);
      userConkKey.qid = objectId;
      
      for (let key in existingMeta) { // Delete existing keys if any
        if (key.startsWith("eluv.caps")) {
          this.reportProgress("Deleting existing caps found", key);
          await client.DeleteMetadata({
            libraryId,
            objectId,
            writeToken,
            metadataSubtree: key
          });
        }
      }
      await client.ReplaceMetadata({
        libraryId,
        objectId,
        writeToken,
        metadataSubtree: userCapKey,
        metadata: await client.Crypto.EncryptConk(userConkKey, client.signer._signingKey().publicKey)
      });
      outputs.imported_caps = userCapKey;
      let result = await this.FinalizeContentObject({
        client, libraryId, objectId, writeToken,
        commitMessage: "Imported CAPS from " + inputs.source_object_id
      });
      if (result?.hash) {
        outputs.version_hash = result.hash;
        return ElvOAction.EXECUTION_COMPLETE;
      }
    } else {
      this.ReportProgress("No caps found matching key", userCapKey);
      return ElvOAction.EXECUTION_EXCEPTION;
    }
    
    //if(permission !== "owner") {
    //  await this.CreateEncryptionConk({libraryId, objectId, writeToken, createKMSConk: true});
    //}
  };
  
  async executeCheckLinks(client, inputs, outputs, configUrl) {
    let meta = await this.getMetadata({client, objectId: inputs.object_id, versionHash: inputs.version_hash, resolve: false});
    outputs.links = [];
    this.findLinks(meta, outputs.links);
    outputs.bad_links = [];
    for (let link of outputs.links) {
      if (this.checkLink(meta, link) == false) {
        outputs.bad_links.push(link);
      }
    }
    if (outputs.bad_links.length == 0) {
      return ElvOAction.EXECUTION_COMPLETE;
    } else {
      return ElvOAction.EXECUTION_FAILED;
    }
  };
  
  findLinks(meta, links) {  
    if (!links) {
      links = [];
    }  
    if (Array.isArray(meta)) {
      for (let element of meta) {
        this.findLinks(element, links)
      }
      return links
    }
    if ((typeof meta) == "object") {
      for (let field in meta) {
        if (field == "/") {
          links.push(meta[field]);
          break;
        }
        if (field == ".") {
          continue;
        }
        this.findLinks(meta[field], links)
      }
      return links;
    }
    return links;
  };
  
  checkLink(meta, link) {
    let elements = link.split(/\//);
    let fileRoot;
    switch(elements[1]) {
      case "files": {
        fileRoot = meta.files;        
        break;
      }
      case "meta": {
        fileRoot = meta;        
        break
      }
      default: {
        this.reportProgress("Ignoring link type "+ elements[1], link);
        return null;
      }
    }    
    for (let i=2; i < elements.length; i++) {
      let element = elements[i];
      if (fileRoot.hasOwnProperty(element)) {
        fileRoot = fileRoot[element];
      } else {
        return false;
      }
    }
    return true;
  };
  
  async executeSetGroupPermissions(client, inputs, outputs, configUrl) {
    let clients = {};
    clients[ client.CurrentAccountAddress().toLowerCase() ] = client;
    if (inputs.private_keys_set && inputs.private_keys_set.length > 0) {
      for (let key of inputs.private_keys_set) {
        let keyClient = await ElvOFabricClient.InitializeClient(configUrl, key);
        clients[ keyClient.CurrentAccountAddress().toLowerCase()] = keyClient;
      }
    }
    let objectId = inputs.object_id;
    await this.executeGetOwner({client, objectId, outputs});
    let clientToUse = clients[outputs.owner_address.toLowerCase()] || client;
    let permissionTypes = {"see":0 , "access":1, "manage":2};
    let permissionType = permissionTypes[inputs.permission_type];
    let rightGranted = await this.grantRights(clientToUse, objectId, inputs.group, permissionType);
    if (rightGranted) {
      return ElvOAction.EXECUTION_COMPLETE;
    } else {
      return ElvOAction.EXECUTION_EXCEPTION;
    }
  }
  
  
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
  
  async executeDeleteByIpTitleId({client, inputs, outputs}) {
    try {
      let libId = inputs.library_id; 
      let result = await this.listItems(libId, client, {
        selectBranches: ["public/asset_metadata/ip_title_id"], 
        filters: ["public/asset_metadata/ip_title_id:eq:"+ inputs.ip_title_id]
      });
      if (!result) {
        return ElvOAction.EXECUTION_EXCEPTION;
      }
      outputs.items = result;
      if (outputs.items.length == 0) {
        return ElvOAction.EXECUTION_FAILED;
      }    
      outputs.object_ids = [];
      outputs.status_codes = {};  
      let overallStatus;
      for (let entry of result) {
        try {
          let objectId = entry.id;
          let objectIdOutputs = {};
          result = await this.executeDelete({client: client, objectId, outputs: objectIdOutputs});
          outputs.status_codes[objectId] = result;
          outputs.object_ids.push(objectId);
          
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
    } catch(err) {
      this.ReportProgress("Error listing items");
      this.Error("Error listing items", err);
      return ElvOAction.EXECUTION_EXCEPTION;
    }
  };
  
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
    "0.1.1": "Only transfer ownership if transfer address is different from current address",
    "0.1.2": "Forces to abort if client can not be initialized",
    "0.1.3": "Adds links check action",
    "0.1.4": "2026-04-03 - Adds option to add on creation the kms key to make object editable",
    "0.1.5": "2026-04-05 - Adds action to modify object permission",
    "0.1.6": "2026-04-10 - Adds option for no conk at all on creation of objects",
    "0.1.7": "2026-04-11 - Supports creation of KMS conk when importing caps",
    "0.1.8": "2026-05-27 - Adds action to open a write-token on an object"
  };
  static VERSION = "0.1.8b";//fix OPEN_WRITE_TOKEN
}


if (ElvOAction.executeCommandLine(ElvOActionManageObject)) {
  ElvOAction.Run(ElvOActionManageObject);
} else {
  module.exports=ElvOActionManageObject;
}
