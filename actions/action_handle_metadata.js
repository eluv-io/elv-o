
const ElvOAction = require("../o-action").ElvOAction;
const { execSync } = require('child_process');
const ElvOFabricClient = require("../o-fabric");
const ElvOMutex = require("../o-mutex");


class ElvOActionHandleMetadata extends ElvOAction  {
  
  ActionId() {
    return "handle_metadata";
  };
  
  Parameters() {
    //we should add an absolute mode: set_permissions NONE, VIEW, ACCESS, EDIT
    // - setting to ACCESS would
    //
    // rescind EDIT if present and replace by ACCESS
    return {
      "parameters": {
        action: {type: "string", required:true, values:["READ","SET","SET_MULTIPLE", "LINK", "DELETE", "SIGN", "LIST_PUBLIC","LIST_PUBLIC_ACROSS_LIBRARIES"]}, 
        identify_by_version: {type: "boolean", required:false, default: false},
        allow_wildcard_in_field: {type: "boolean", required:false, default: false},
      }
    };
  };
  
  IOs(parameters) {
    let outputs =  {};
    let inputs = {
      private_key: {type: "password", required:false},
      config_url: {type: "string", required:false}
    };
    if (parameters.action == "LIST_PUBLIC") {
      inputs.library_id = {type: "string", required: true};
      inputs.select_branches = {type: "array", required: true};
      inputs.filters = {type: "array", required: false, value: null};
      inputs.flatten_outputs_keys = {type: "array", required: false, sample: {"ipm_id": "public/asset_metadata/ip_title_id", "status": "public/asset_metadata/info/status"}},
      outputs.values_by_object_id = {type: "object"}
      return {inputs, outputs}; 
    } 
    if (parameters.action == "LIST_PUBLIC_ACROSS_LIBRARIES") {
      inputs.libraries = {type: "array", required: true};
      inputs.select_branches = {type: "array", required: true};
      inputs.filters = {type: "array", required: false, value: null};
      inputs.flatten_outputs_keys = {type: "array", required: false, sample: {"ipm_id": "public/asset_metadata/ip_title_id", "status": "public/asset_metadata/info/status"}},
      outputs.values_by_object_id = {type: "object"}
      return {inputs, outputs}; 
    } 
    if (!parameters.identify_by_version) {
      inputs.target_object_id = {type: "string", required: true};
    } else {
      inputs.target_object_version_hash = {type: "string", required: true};
    }
    
    if (parameters.action == "READ") {
      inputs.field = {type:"string", required: false};
      inputs.write_token = {type:"string", required: false};
      inputs.remove_branches = {type:"array", required: false};
      inputs.resolve_links = {type:"boolean", required: false, default: true};
      outputs.value = {type: "object"};
      if (parameters.allow_wildcard_in_field) {
        outputs.field = {type: "string"};
      }
    }
    if (parameters.action == "SET") {
      inputs.field = {type:"string", required: false};
      inputs.value = {type:"object", required: false};
      inputs.content_type = {type:"string", required: false};
      inputs.write_token = {type:"string", required: false};
      inputs.safe_update = {type: "boolean", required: false, default: false};
      inputs.force_update = {type: "boolean", required: false, default: false};
      outputs.action_taken = {type: "boolean"};
      outputs.modified_object_version_hash = {type:"string"};
      if (parameters.allow_wildcard_in_field) {
        outputs.field = {type: "string"};
      }
    }
    if (parameters.action == "SET_MULTIPLE") {
      inputs.fields = {type:"array", required: true};
      inputs.values = {type:"array", required: false};
      inputs.safe_update = {type: "boolean", required: false, default: false};
      inputs.force_update = {type: "boolean", required: false, default: false};
      outputs.modified_object_version_hash = {type:"string"};
    }
    if (parameters.action == "DELETE") {
      inputs.write_token = {type:"string", required: false};
      inputs.field = {type:"string", required: false};
      inputs.safe_update = {type: "boolean", required: false, default: false};
      inputs.force_update = {type: "boolean", required: false, default: false};
      outputs.action_taken = {type: "boolean"};
      outputs.modified_object_version_hash = {type:"string"};
      if (parameters.allow_wildcard_in_field) {
        outputs.field = {type: "string"};
      }
    }
    if (parameters.action == "LINK") {
      inputs.field = {type:"string", required: true};
      inputs.signed = {type: "boolean", required: false};
      inputs.link_to = {type:"string", required: true};
      inputs.link_to_object_id = {type:"string", required: false};
      inputs.link_to_object_version_hash = {type:"string", required: false};
      inputs.signing_private_key = {type:"string", required: false};
      inputs.safe_update = {type: "boolean", required: false, default: false};
      inputs.force_update = {type: "boolean", required: false, default: false};
      outputs.action_taken = {type: "boolean"};
      outputs.modified_object_version_hash = {type:"string"};
      if (parameters.allow_wildcard_in_field) {
        throw "not implemented";
      }
    }
    if (parameters.action == "SIGN") {
      inputs.fields = {type:"array", required: true};
      inputs.update_to_latest = {type: "boolean", required: false, default: false};
      inputs.safe_update = {type: "boolean", required: false, default: false};
      inputs.force_update = {type: "boolean", required: false, default: false};
      outputs.action_taken = {type: "boolean"};
      outputs.modified_object_version_hash = {type:"string"};
      if (parameters.allow_wildcard_in_field) {
        throw "not implemented";
      }
    }
    return {inputs: inputs, outputs: outputs};
  };
  
  async Execute(inputs, outputs) {
    let client;
    if (!this.Payload.inputs.private_key && !this.Payload.inputs.config_url){
      client = this.Client;
    } else {
      let privateKey = this.Payload.inputs.private_key || this.Client.signer.signingKey.privateKey.toString();
      let configUrl = this.Payload.inputs.config_url || this.Client.configUrl;
      client = await ElvOFabricClient.InitializeClient(configUrl, privateKey)
    }
    if (this.Payload.parameters.action == "LIST_PUBLIC") {
      return await this.executeListPublic(inputs, outputs, client);
    }
    if (this.Payload.parameters.action == "LIST_PUBLIC_ACROSS_LIBRARIES") {
      return await this.executeListPublicAcrossLibraries(inputs, outputs, client);
    }
    
    let field = inputs.field;
    let objectId = inputs.target_object_id;
    let versionHash = inputs.target_object_version_hash;
    if (!objectId) {
      objectId = client.utils.DecodeVersionHash(versionHash).objectId;
    }
    let libraryId = await this.getLibraryId(objectId, client);
    try {
      if (this.Payload.parameters.action == "READ") {
        let removeBranches = inputs.remove_branches;
        return await this.executeRead({objectId, removeBranches, versionHash, libraryId, field, client}, outputs);
      }
      if (this.Payload.parameters.action == "SET") {
        let value = inputs.value;
        return await this.executeSet({objectId, libraryId, value, versionHash, field, client}, outputs);
      }
      if (this.Payload.parameters.action == "SET_MULTIPLE") {
        let value = inputs.value;
        return await this.executeSetMultiple({objectId, libraryId, inputs, versionHash, client, outputs});
      }
      if (this.Payload.parameters.action == "LINK") { //libraryId, objectId, field, to, targetObj
        try {
          let targetHash;
          if (inputs.link_to_object_id) {
            targetHash = await this.getVersionHash({objectId: inputs.link_to_object_id, client: client});
          } else {
            targetHash = inputs.link_to_object_version_hash;
          }
          let linkData;
          let to;
          if (targetHash) {
            to = "/qfab/" + targetHash + "/" + inputs.link_to;
          } else {
            to = (inputs.link_to.match(/^\.\//)) ? inputs.link_to : "./" + inputs.link_to
          }
          
          await this.acquireMutex(objectId);
          
          let existing = await this.getMetadata({
            libraryId: libraryId,
            objectId: objectId,
            versionHash: versionHash,
            metadataSubtree: field,
            client: client
          });
          
          if (existing && (existing["/"] == to) && existing["."] && (!inputs.signed || existing["."].authorization)) {
            this.releaseMutex();
            outputs.action_taken = false;
            this.ReportProgress("Target value for " + field + " already set to " + to);
            return ElvOAction.EXECUTION_COMPLETE;
          }
          if (inputs.signed) {
            if (!targetHash) {
              this.releaseMutex();
              this.ReportProgress("Only external links can be signed");
              this.Error("Only external links can be signed");
              return ElvOAction.EXECUTION_EXCEPTION;
            }
            let configUrl = process.env["CONFIG_URL"].replace(/\/config.*$/, '');
            let contentSpace = await this.getContentSpace(objectId, client);
            const privateKey = inputs.signing_private_key || this.Payload.inputs.private_key || this.Client.signer.signingKey.privateKey.toString();
            
            let cmd = "elv content signed-link create \"" + to + "\"  " + objectId + " --config-url " + configUrl + " -x " + privateKey + " --space " + contentSpace;
            this.Debug("Sign link cmd", cmd);
            let stdout = execSync(cmd).toString();
            this.Debug("Sign link stdout", stdout);
            linkData = JSON.parse(stdout);
          } else {
            linkData = {"/": to};
          }
          
          let writeToken = await this.getWriteToken({
            libraryId: libraryId,
            objectId: objectId,
            client: client,
            force: this.Payload.inputs.force_update
          });
          
          await client.ReplaceMetadata({
            libraryId: libraryId,
            objectId: objectId,
            writeToken: writeToken,
            metadata: linkData,
            metadataSubtree: field,
            client
          });
          let response = await this.FinalizeContentObject({
            libraryId: libraryId,
            objectId: objectId,
            writeToken: writeToken,
            commitMessage: "Linked " + field + (inputs.signed) ? " (signed)" : "",
            client
          });
          
          outputs.action_taken = true;
          outputs.modified_object_version_hash = response.hash;
          this.releaseMutex();
          this.ReportProgress("Target value for " + field + " set to " + to);
          return ElvOAction.EXECUTION_COMPLETE;
        } catch (err) {
          this.releaseMutex();
          this.Error("Could not link", err);
          return ElvOAction.EXECUTION_EXCEPTION;
        }
      }
      if (this.Payload.parameters.action == "SIGN") {
        return await this.executeSign({objectId, libraryId, versionHash, client}, outputs);
      }
      if (this.Payload.parameters.action == "DELETE") {
        return await this.executeDelete({objectId, libraryId, versionHash, client, field}, outputs);
      }
      throw "Operation not implemented yet" + this.Payload.parameters.action;
    } catch(err) {
      this.Error("Could not process " + this.Payload.parameters.action + " metadata for " + (objectId || versionHash), err);
      return ElvOAction.EXECUTION_EXCEPTION;
    }
  };
  
  async executeListPublicAcrossLibraries(inputs, outputs, client) {
    outputs.values_by_object_id = {};
    inputs.add_library_id = true;
    for (let libraryId of inputs.libraries) {
      inputs.library_id = libraryId
      await this.executeListPublic(inputs, outputs, client) 
    }
    return ElvOAction.EXECUTION_COMPLETE;
  };
  
  async executeListPublic(inputs, outputs, client) {
    let libraryId = inputs.library_id;
    let token = await this.getLibraryToken(libraryId, client);
    let done = false;
    let start = 0;
    let page = 1;
    let pages = null;
    let selectStr = "&select=" + inputs.select_branches.join("&select=");
    let filtersArr = inputs.filters;
    let filterStr =  (filtersArr && (filtersArr.length != 0)) ? ("&filter=" + filtersArr.join("&filter=")) : "";
    let limit = 10000;
    let all = outputs.values_by_object_id || {};
    while (!done) {
      let nodeUrl= (await ElvOFabricClient.getFabricUrls(client))[0] + "/";
      
      let url = nodeUrl + "qlibs/" + libraryId + "/q?limit="+ limit+"&start="+start + selectStr + filterStr;
      this.reportProgress("curl -s '" + url + "' -H 'Authorization: Bearer " + token + "'");
      let options =  {headers: {'Authorization': "Bearer " + token}};
      let results = await  ElvOFabricClient.fetchJSON(url, options);
      for (let entry of results.contents) {
        let data = this.extractValues(entry.versions[0].meta, inputs.flatten_outputs_keys);        
        if (inputs.add_library_id) {
          data.library_id = libraryId;
        }
        all[entry.id] = data;
      }  
      let stats = results.paging;
      if (!pages) {
        pages = stats.pages
      }
      if (page >= pages) {
        done = true;
      } else {
        start += limit;
        page++;
      }
    }
    if (!outputs.values_by_object_id) {
      outputs.values_by_object_id = all;
    }
    return ElvOAction.EXECUTION_COMPLETE;
  };
  
  extractValues(entry, keys) {
    if (!keys) {
      return entry;
    }
    let result = {};
    for (let key in keys) {
      let elements = keys[key].split("/");
      let value=entry;
      if (value == null) {
        continue;
      }
      for (let element of elements) {
        if (value.hasOwnProperty(element)) {
          value = value[element];
        } else {
          value = null;
          break;
        }
      }
      result[key] = value;
    }
    return result
  }
  
  async executeSet({objectId, libraryId, value, versionHash, field, client}, outputs) {
    try {
      if (this.Payload.parameters.allow_wildcard_in_field && field.match(/\*/)) {
        let knownPart = field.replace(/\*.*/, "").replace(/\/[^/]*$/,"");
        let toExpand = field.match(/\/*[^/]*\*[^/]*\/*/)[0].replace(/\//g,"");
        this.reportProgress("Looking for match ",{knownPart, knownPart} );
        let candidateMatcher = new RegExp(toExpand.replace(/\*/,".*"));
        let knownData = await this.getMetadata({
          objectId: objectId,
          libraryId,
          versionHash: versionHash,
          metadataSubtree: field,
          removeBranches: removeBranches,
          client: client
        });
        for (let candidate in knownData) {
          if (candidate.match(candidateMatcher)) {
            field = field.replace(toExpand, candidate);
            outputs.field = field;
            this.reportProgress("Found match ",  field );
            break;
          }
        }
      }
      
      await this.acquireMutex(objectId);
      let original = await this.getMetadata({
        objectId: objectId,
        libraryId: libraryId,
        versionHash: versionHash,
        writeToken: this.Payload.inputs.write_token,
        metadataSubtree: field,
        resolveLinks: false,
        client: client
      });
      let contentType = this.Payload.inputs.content_type;
      if (this.areEqual(value, original) && !contentType) {
        this.releaseMutex();
        outputs.action_taken = false;
        outputs.modified_object_version_hash = await this.getVersionHash({
          objectId: objectId,
          libraryId: libraryId,
          client: client
        });
        this.ReportProgress("Target value for " + field + " already set");
        return ElvOAction.EXECUTION_COMPLETE;
      }
      
      let msg = "Modified metadata field '" + field + "'"
      let editParams = {
        libraryId: libraryId,
        objectId: objectId,
        client: client,
        force:  this.Payload.inputs.force_update
      };
      if (contentType) {
        editParams.options = {type : contentType};
      }
      let writeToken = this.Payload.inputs.write_token || await this.getWriteToken(editParams);
      await client.ReplaceMetadata({
        libraryId: libraryId,
        objectId: objectId,
        writeToken: writeToken,
        metadataSubtree: field,
        metadata: value,
        client
      });
      outputs.action_taken = true;
      if (!this.Payload.inputs.write_token) {
        let response = await this.FinalizeContentObject({
          libraryId: libraryId,
          objectId: objectId,
          writeToken: writeToken,
          commitMessage: msg,
          client
        });
        
        outputs.modified_object_version_hash = response.hash;
      } else {
        this.ReportProgress("Metadata set in write-token", this.Payload.inputs.write_token);
      }
    } catch (errSet) {
      this.releaseMutex();
      this.Error("Could not set metadata for " + (objectId || versionHash), errSet);
      this.ReportProgress("Could not set metadata");
      return ElvOAction.EXECUTION_EXCEPTION;
    }
    this.releaseMutex();
    return ElvOAction.EXECUTION_COMPLETE;
  };
  
  async executeSetMultiple({objectId, libraryId, inputs, versionHash, client, outputs}){
    let fields = inputs.fields;
    try {
      await this.acquireMutex(objectId);
      let writeToken = await this.getWriteToken({libraryId: libraryId, objectId: objectId, versionHash, client});
      
      for (let i=0; i < fields.length; i++) {
        let field = fields[i];
        let value = inputs.values[i];
        await client.ReplaceMetadata({
          libraryId: libraryId,
          objectId: objectId,
          versionHash,
          writeToken: writeToken,
          metadataSubtree: field,
          metadata: value,
          client
        });
        this.ReportProgress("Modified value for ", field);
      }      
      let response = await this.FinalizeContentObject({
        libraryId: libraryId,
        versionHash,
        objectId: objectId,
        writeToken: writeToken,
        commitMessage: "Modified several metadata fields",
        client
      });
      outputs.modified_object_version_hash = response.hash;
    } catch (errSet) {
      this.releaseMutex();
      this.Error("Could not set metadata for " + (objectId || versionHash), errSet);
      this.ReportProgress("Could not set metadata");
      return ElvOAction.EXECUTION_EXCEPTION;
    }
    this.releaseMutex();
    return ElvOAction.EXECUTION_COMPLETE;
    
  };
  
  async executeRead({objectId, removeBranches, versionHash,libraryId, field, client}, outputs) {
    this.ReportProgress("Processing " + objectId + " read field: " + (field || "/"), {remove_branches: removeBranches});
    try {
      let resolve = this.Payload.inputs.resolve_links;
      if (this.Payload.parameters.allow_wildcard_in_field && field.match(/\*/)) {
        let knownPart = field.replace(/\*.*/, "").replace(/\/[^/]*$/,"");
        let toExpand = field.match(/\/*[^/]*\*[^/]*\/*/)[0].replace(/\//g,"");
        let candidateMatcher = new RegExp(toExpand.replace(/\*/,".*"));
        
        this.reportProgress("Looking for match ",{knownPart, toExpand, candidateMatcher: candidateMatcher.toString()} );
        let knownData = await this.getMetadata({
          objectId: objectId,
          libraryId,
          versionHash: versionHash,
          writeToken: this.Payload.inputs.write_token,
          metadataSubtree: knownPart,
          removeBranches: removeBranches,
          resolve: false,
          client: client
        });
        for (let candidate in knownData) {
          this.reportProgress("Candidate ", candidate);
          if (candidate.match(candidateMatcher)) {
            field = field.replace(toExpand, candidate);
            outputs.field = field;
            this.reportProgress("Found match ",  field );
            break;
          }
        }
      }
      
      outputs.value = await this.getMetadata({
        objectId: objectId,
        libraryId,
        versionHash: versionHash,
        metadataSubtree: field,
        removeBranches: removeBranches,
        resolve,
        client: client
      });
      if (outputs.value != null) {
        return ElvOAction.EXECUTION_COMPLETE;
      } else {
        return ElvOAction.EXECUTION_FAILED;
      }
    } catch (errMeta) {
      this.Error("Could not retrieve metadata for " + (objectId || versionHash), errMeta);
      this.ReportProgress("Could not retrieve metadata");
      return ElvOAction.EXECUTION_EXCEPTION;
    }
  };
  
  async executeDelete(params, outputs) {
    let field = params.field;
    let client = params.client;
    let objectId = params.objectId;
    let libraryId = params.libraryId;
    let versionHash = params.versionHash;
    let removeBranches = params.removeBranches;
    if (this.Payload.parameters.allow_wildcard_in_field && field.match(/\*/)) {
      let knownPart = field.replace(/\*.*/, "").replace(/\/[^/]*$/,"");
      let toExpand = field.match(/\/*[^/]*\*[^/]*\/*/)[0].replace(/\//g,"");
      this.reportProgress("Looking for match ",{knownPart, knownPart} );
      let candidateMatcher = new RegExp(toExpand.replace(/\*/,".*"));
      let knownData = await this.getMetadata({
        objectId: objectId,
        libraryId,
        versionHash: versionHash,
        resolve: false,
        metadataSubtree: field,
        removeBranches: removeBranches,
        client: client
      });
      for (let candidate in knownData) {
        if (candidate.match(candidateMatcher)) {
          field = field.replace(toExpand, candidate);
          outputs.field = field;
          this.reportProgress("Found match ",  field );
          break;
        }
      }
    }
    let writeToken = this.Payload.inputs.write_token;
    if (!writeToken) {
      await this.acquireMutex(objectId);
      writeToken = await this.getWriteToken({
        libraryId: params.libraryId,
        objectId: params.objectId,
        versionHash: params.versionHash, 
        force:  this.Payload.inputs.force_update,
        client
      });
    }
    
    await client.DeleteMetadata({
      libraryId: params.libraryId,
      objectId: params.objectId,
      versionHash: params.versionHash,
      writeToken,
      client,
      metadataSubtree: field
    });
    if (!this.Payload.inputs.write_token) {
      let response = await this.FinalizeContentObject({
        libraryId: params.libraryId,
        objectId: params.objectId,
        versionHash: params.versionHash,
        writeToken,
        client,
        commitMessage: "Removed " + field 
      });
      if (response && response.hash) {
        this.releaseMutex();
        outputs.action_taken = true;
        outputs.modified_object_version_hash = response.hash;
        this.ReportProgress("Removed metadata from " + params.objectId, field);
        return ElvOAction.EXECUTION_COMPLETE;
      } 
    } else  {
      outputs.action_taken = true;
      this.ReportProgress("Removed metadata from " + writeToken, field);
      return ElvOAction.EXECUTION_COMPLETE;
    }
    this.releaseMutex();
    this.Error("Could not finalize object "+ params.objectId, response);
    return ElvOAction.EXECUTION_EXCEPTION;
  };
  
  async executeSign(params, outputs) {
    let objectId = params.objectId;
    let versionHash = params.versionHash;
    if (!objectId) {
      objectId = this.Client.utils.DecodeVersionHash(versionHash).objectId;
    }
    let libraryId = params.libraryId || await this.getLibraryId(objectId, client);
    
    let client = params.client;
    await this.acquireMutex(objectId);
    let writeToken = await this.getWriteToken({
      objectId,
      libraryId,
      versionHash,
      force:  this.Payload.inputs.force_update,
      client
    });
    for (let field of this.Payload.inputs.fields) {
      let rawLink = await this.getMetadata({
        objectId,
        libraryId,
        versionHash,
        metadataSubtree: field, 
        resolve: false,
        client
      });
      let existing = rawLink["/"].match(/^\/qfab\/([^\/]+)\/(.*)/);
      let linkedVersionHash = existing[1];
      if  (this.Payload.inputs.update_to_latest) {
        let linkedObjectId = client.utils.DecodeVersionHash(linkedVersionHash).objectId;
        linkedVersionHash =  await getVersionHash({client, objectId: linkedObjectId}); 
      }
      let configUrl = client.configUrl.replace(/\/config.*$/, '');
      let contentSpace = await this.getContentSpace(objectId, client);
      const privateKey = this.Payload.inputs.signing_private_key || this.Payload.inputs.private_key;
      let to = "/qfab/" + linkedVersionHash +"/" + existing[2];
      let cmd = "elv content signed-link create \"" + to + "\"  " + objectId + " --config-url " + configUrl + " -x " + privateKey + " --space " + contentSpace;
      this.Debug("Sign link cmd", cmd);
      let stdout = execSync(cmd).toString();
      this.Debug("Sign link stdout", stdout);
      let linkData = JSON.parse(stdout);  
      await client.ReplaceMetadata({
        objectId,
        libraryId,
        versionHash,
        metadataSubtree: field, 
        writeToken,
        metadata: linkData,
        client
      });
    }
    let response = await this.FinalizeContentObject({
      libraryId: libraryId,
      objectId: objectId,
      writeToken: writeToken,
      commitMessage: "Signed links for " + this.Payload.inputs.fields.join(", "),
      client
    });
    if (response && response.hash) {
      this.releaseMutex();
      outputs.action_taken = true;
      outputs.modified_object_version_hash = response.hash;
      this.ReportProgress("Signed links for " +objectId, this.Payload.inputs.fields);
      return ElvOAction.EXECUTION_COMPLETE;
    } 
    this.releaseMutex();
    this.Error("Could not finalize object "+ objectId, response);
    return ElvOAction.EXECUTION_EXCEPTION;
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
  
  areEqual(a,b) {
    if (a == null) {
      return (b == null);
    }
    if (b == null) {
      return false; //since a==null has already been handled
    }
    if ((typeof a) != (typeof b)) {
      return false;
    }
    if ((typeof a) != "object") {
      return (a == b);
    }
    let aKeys = Object.keys(a);
    if (aKeys.length != Object.keys(b).length) {
      return false;
    }
    for (let i=0; i < aKeys.length; i++) {
      if (!(aKeys[i] in b) || !this.areEqual(a[aKeys[i]], b[aKeys[i]])) {
        return false;
      }
    }
    return true;
  };
  
  static VERSION = "0.2.2";
  static REVISION_HISTORY = {
    "0.0.1": "Initial release",
    "0.0.2": "Fix SET when use on remote instance",
    "0.0.3": "Private key input is encrypted",
    "0.0.4": "Use safe finalize method",
    "0.0.5": "Fixes key related issues when execution node is provided",
    "0.0.6": "Implements delete action",
    "0.0.7": "Returns hash as output on set even if no action taken",
    "0.0.8": "Adds optional Mutex on editing operations",
    "0.0.9": "Fixes bug in the delete operation",
    "0.0.10": "Adds force option to optionally clear commit pending",
    "0.0.11": "Adds option not to resolve links",
    "0.1.0": "Use given private key instead of signing key if ommitted when signing links",
    "0.1.1": "SET provides the option to change the content type",
    "0.2.0": "Adds an action to list public metadata across all objects from a library",
    "0.2.1": "Adds command to set multiple metadata fields",
    "0.2.2": "Adds support for write-token for SET and DELETE"
  };
}


if (ElvOAction.executeCommandLine(ElvOActionHandleMetadata)) {
  ElvOAction.Run(ElvOActionHandleMetadata);
} else {
  module.exports=ElvOActionHandleMetadata;
}
