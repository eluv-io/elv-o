
const ElvOAction = require("../o-action").ElvOAction;
const ElvOFabricClient = require("../o-fabric");


class ElvOActionChangePermissions extends ElvOAction  {

  ActionId() {
    return "change_permissions";
  };

  Parameters() {
    //we should add an absolute mode: set_permissions NONE, VIEW, ACCESS, EDIT
    // - setting to ACCESS would rescind EDIT if present and replace by ACCESS
    return {"parameters": {action: {type: "string", required:true, values:["GRANT","RESCIND"/*,"SET"*/]}}};
  };

  IOs(parameters) {
    let inputs = {
      target_object_id: {type: "string", required: true},
      private_key: {"type": "password", "required":false},
      config_url: {"type": "string", "required":false}
    };
    let outputs =  {action_taken: {type: "boolean"}};
    if (parameters.action == "GRANT") {
      inputs.permission_to_grant = {type:"string", required: true, values:["VIEW","EDIT","ACCESS"]};
      inputs.group_to_grant_permission_to = {type:"string", required: true};
      outputs.permission_granted = {type: "boolean"};
    }
    if (parameters.action == "RESCIND") {
      inputs.permission_to_rescind = {type:"string", required: true, values:["VIEW","EDIT","ACCESS"]};
      inputs.group_to_rescind_permission_from = {type:"string", required: true};
      outputs.permission_rescinded = {type: "boolean"};
    }
    return {inputs: inputs, outputs: outputs}
  };

  async Execute(handle, outputs) {
    try {
      let client;
      if (!this.Payload.inputs.private_key && !this.Payload.inputs.config_url){
        client = this.Client;
      } else {
        let privateKey = this.Payload.inputs.private_key || this.Client.signer.signingKey.privateKey.toString();
        let configUrl = this.Payload.inputs.config_url || Client.configUrl;
        client = await ElvOFabricClient.InitializeClient(configUrl, privateKey)
      }
      let inputs = this.Payload.inputs;
      const RIGHTS = {EDIT: 2, ACCESS: 1, VIEW: 0};
      let rightsType = RIGHTS[(inputs.permission_to_grant || inputs.permission_to_rescind).toUpperCase()];
      let objAddress = client.utils.HashToAddress(inputs.target_object_id);
      let groupAddress = (inputs.group_to_grant_permission_to || inputs.group_to_rescind_permission_from);

      let rights = await client.CallContractMethod({
        contractAddress: groupAddress,
        abi: ElvOActionChangePermissions.CONTENT_INDEXOR_ABI,
        methodName: "checkDirectRights",
        methodArgs: [1, objAddress, rightsType]
      });
      if (this.Payload.parameters.action == "GRANT") {
        if (!rights) {
          await this.CallContractMethodAndWait({
            contractAddress: groupAddress,
            abi: ElvOActionChangePermissions.CONTENT_INDEXOR_ABI,
            methodName: "setContentObjectRights",
            methodArgs: [objAddress, rightsType, 1],
            client
          });
          rights = await client.CallContractMethod({
            contractAddress: groupAddress,
            abi: ElvOActionChangePermissions.CONTENT_INDEXOR_ABI,
            methodName: "checkDirectRights",
            methodArgs: [1, objAddress, rightsType]
          });
          outputs.permission_granted = rights;
          outputs.action_taken = true;
        } else {
          outputs.permission_granted = true;
          outputs.action_taken = false;
        }
      } else { //(this.Payload.parameters.action == "RESCIND")
        if (rights) {
          await  this.CallContractMethodAndWait({
            contractAddress: groupAddress,
            abi: ElvOActionChangePermissions.CONTENT_INDEXOR_ABI,
            methodName: "setContentObjectRights",
            methodArgs: [objAddress, rightsType, 0],
            client
          });
          rights = await client.CallContractMethod({
            contractAddress: groupAddress,
            abi: ElvOActionChangePermissions.CONTENT_INDEXOR_ABI,
            methodName: "checkDirectRights",
            methodArgs: [1, objAddress, rightsType]
          });
          outputs.permission_rescinded = (!rights);
          outputs.action_taken = true;
        } else {
          outputs.permission_rescinded = true;
          outputs.action_taken = false;
        }
      }
      return (outputs.permission_rescinded || outputs.permission_granted) ? 100 : 99;
    } catch(err) {
      this.Error("Execution error", err);
      return -1;
    }
  };

  static CONTENT_INDEXOR_ABI = [{"constant":true,"inputs":[],"name":"creator","outputs":[{"name":"","type":"address"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[],"name":"cleanUpContentObjects","outputs":[{"name":"","type":"uint256"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"name":"content_space","type":"address"}],"name":"setContentSpace","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[{"name":"obj","type":"address"}],"name":"getContractRights","outputs":[{"name":"","type":"uint8"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"CATEGORY_CONTENT_OBJECT","outputs":[{"name":"","type":"uint8"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"getAccessGroupsLength","outputs":[{"name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"CATEGORY_GROUP","outputs":[{"name":"","type":"uint8"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"name":"group","type":"address"},{"name":"access_type","type":"uint8"}],"name":"checkAccessGroupRights","outputs":[{"name":"","type":"bool"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"CATEGORY_LIBRARY","outputs":[{"name":"","type":"uint8"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"ACCESS_CONFIRMED","outputs":[{"name":"","type":"uint8"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"name":"obj","type":"address"},{"name":"access_type","type":"uint8"},{"name":"access","type":"uint8"}],"name":"setContractRights","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[{"name":"position","type":"uint256"}],"name":"getAccessGroup","outputs":[{"name":"","type":"address"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[],"name":"cleanUpAll","outputs":[{"name":"","type":"uint256"},{"name":"","type":"uint256"},{"name":"","type":"uint256"},{"name":"","type":"uint256"},{"name":"","type":"uint256"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[{"name":"group","type":"address"}],"name":"getAccessGroupRights","outputs":[{"name":"","type":"uint8"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"accessGroups","outputs":[{"name":"category","type":"uint8"},{"name":"length","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"name":"obj","type":"address"},{"name":"access_type","type":"uint8"},{"name":"access","type":"uint8"}],"name":"setContentObjectRights","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[],"name":"kill","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[{"name":"candidate","type":"address"}],"name":"hasManagerAccess","outputs":[{"name":"","type":"bool"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"ACCESS_TENTATIVE","outputs":[{"name":"","type":"uint8"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"version","outputs":[{"name":"","type":"bytes32"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"getContentTypesLength","outputs":[{"name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"TYPE_EDIT","outputs":[{"name":"","type":"uint8"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"name":"obj","type":"address"},{"name":"access_type","type":"uint8"}],"name":"checkContentObjectRights","outputs":[{"name":"","type":"bool"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"CATEGORY_CONTRACT","outputs":[{"name":"","type":"uint8"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"name":"lib","type":"address"},{"name":"access_type","type":"uint8"}],"name":"checkLibraryRights","outputs":[{"name":"","type":"bool"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"CATEGORY_CONTENT_TYPE","outputs":[{"name":"","type":"uint8"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"name":"obj","type":"address"}],"name":"getContentObjectRights","outputs":[{"name":"","type":"uint8"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"contracts","outputs":[{"name":"category","type":"uint8"},{"name":"length","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"name":"newCreator","type":"address"}],"name":"transferCreatorship","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[{"name":"position","type":"uint256"}],"name":"getContract","outputs":[{"name":"","type":"address"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"name":"addr","type":"address"}],"name":"contractExists","outputs":[{"name":"","type":"bool"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"name":"lib","type":"address"},{"name":"access_type","type":"uint8"},{"name":"access","type":"uint8"}],"name":"setLibraryRights","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[{"name":"index_type","type":"uint8"},{"name":"obj","type":"address"},{"name":"access_type","type":"uint8"}],"name":"checkRights","outputs":[{"name":"","type":"bool"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"ACCESS_NONE","outputs":[{"name":"","type":"uint8"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[],"name":"cleanUpContentTypes","outputs":[{"name":"","type":"uint256"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"name":"obj","type":"address"},{"name":"access_type","type":"uint8"},{"name":"access","type":"uint8"}],"name":"setContentTypeRights","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"owner","outputs":[{"name":"","type":"address"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[],"name":"cleanUpLibraries","outputs":[{"name":"","type":"uint256"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"TYPE_SEE","outputs":[{"name":"","type":"uint8"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"contentTypes","outputs":[{"name":"category","type":"uint8"},{"name":"length","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"name":"index_type","type":"uint8"},{"name":"obj","type":"address"},{"name":"access_type","type":"uint8"}],"name":"checkDirectRights","outputs":[{"name":"","type":"bool"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"name":"obj","type":"address"}],"name":"getContentTypeRights","outputs":[{"name":"","type":"uint8"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"name":"obj","type":"address"},{"name":"access_type","type":"uint8"}],"name":"checkContractRights","outputs":[{"name":"","type":"bool"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"contentObjects","outputs":[{"name":"category","type":"uint8"},{"name":"length","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"name":"position","type":"uint256"}],"name":"getContentType","outputs":[{"name":"","type":"address"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"contentSpace","outputs":[{"name":"","type":"address"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[],"name":"setAccessRights","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"libraries","outputs":[{"name":"category","type":"uint8"},{"name":"length","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"getLibrariesLength","outputs":[{"name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"name":"position","type":"uint256"}],"name":"getContentObject","outputs":[{"name":"","type":"address"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"name":"position","type":"uint256"}],"name":"getLibrary","outputs":[{"name":"","type":"address"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"TYPE_ACCESS","outputs":[{"name":"","type":"uint8"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[],"name":"cleanUpAccessGroups","outputs":[{"name":"","type":"uint256"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"getContentObjectsLength","outputs":[{"name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"name":"group","type":"address"},{"name":"access_type","type":"uint8"},{"name":"access","type":"uint8"}],"name":"setAccessGroupRights","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"name":"newOwner","type":"address"}],"name":"transferOwnership","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[{"name":"lib","type":"address"}],"name":"getLibraryRights","outputs":[{"name":"","type":"uint8"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"getContractsLength","outputs":[{"name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"name":"obj","type":"address"},{"name":"access_type","type":"uint8"}],"name":"checkContentTypeRights","outputs":[{"name":"","type":"bool"}],"payable":false,"stateMutability":"view","type":"function"},{"inputs":[],"payable":true,"stateMutability":"payable","type":"constructor"},{"payable":true,"stateMutability":"payable","type":"fallback"},{"anonymous":false,"inputs":[{"indexed":false,"name":"principal","type":"address"},{"indexed":false,"name":"entity","type":"address"},{"indexed":false,"name":"aggregate","type":"uint8"}],"name":"RightsChanged","type":"event"}];
  static VERSION = "0.0.2";
  static REVISION_HISTORY = {
    "0.0.1": "Initial release",
    "0.0.2": "Private key input is encrypted",
  };
}

if (ElvOAction.executeCommandLine(ElvOActionChangePermissions)) {
  ElvOAction.Run(ElvOActionChangePermissions);
} else {
  module.exports=ElvOActionChangePermissions;
}
