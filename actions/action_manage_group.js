const ElvOAction = require("../o-action").ElvOAction;
const ElvOProcess = require("../o-process");
const ElvOFabricClient = require("../o-fabric");



class ElvOActionManageGroup extends ElvOAction  {
  ActionId() {
    return "manage_group";
  };


  Parameters() {
    return {parameters: {action: {type: "string", values:["CREATE","LIST_MEMBERS" /*,"DELETE"*/], require: true}}};
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
      inputs.managers = {type: "array", required: false, default: []};
      inputs.manager_groups = {type: "array", required: false, default: []};
      inputs.members = {type: "array", required: false, default: []};
      inputs.member_groups = {type: "array", required: false, default: []};
      outputs.group_address =  {"type": "string"};
    }
    if (parameters.action == "DELETE") {
      inputs.group_address =  {"type": "string", required: true};
    }
    if (parameters.action == "LIST_MEMBERS") {
      inputs.group_address =  {"type": "string", required: true};
      outputs.members = {type: "array"};
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

        outputs.group_address = await this.safeExec("client.CreateAccessGroup", [{
          name: this.Payload.inputs.name,
          description: this.Payload.inputs.description,
          metadata: this.Payload.inputs.metadata,
          client
        }]);
        this.ReportProgress("Created group "  +  outputs.group_address);

        for (let i=0; i < this.Payload.inputs.managers.length; i++) {
          await this.safeExec("client.AddAccessGroupManager", [{
            contractAddress: outputs.group_address,
            memberAddress: this.Payload.inputs.managers[i],
            client
          }]);
          this.ReportProgress("Added group manager " + this.Payload.inputs.managers[i]);
        }
        for (let i=0; i < this.Payload.inputs.members.length; i++) {
          await this.safeExec("client.AddAccessGroupMember", [{
            contractAddress: outputs.group_address,
            memberAddress: this.Payload.inputs.members[i],
            client
          }]);
          this.ReportProgress("Added group member " + this.Payload.inputs.members[i]);
        }

        for (let i=0; i < this.Payload.inputs.manager_groups.length; i++) {
          await this.safeExec("client.CallContractMethodAndWait", [{
            methodName: "setAccessGroupRights",
            contractAddress: this.Payload.inputs.manager_groups[i],
            methodArgs: [outputs.group_address, 2, 1],
            client: client
          }]);
          this.ReportProgress("Added  manager group" + this.Payload.inputs.manager_groups[i]);
        }

        for (let i=0; i < this.Payload.inputs.member_groups.length; i++) {
          await this.safeExec("client.CallContractMethodAndWait", [{
            methodName: "setAccessGroupRights",
            contractAddress: this.Payload.inputs.member_groups[i],
            methodArgs: [outputs.group_address, 1, 1],
            client: client
          }]);
          this.ReportProgress("Added  member group" + this.Payload.inputs.member_groups[i]);
        }

        this.ReportProgress("Group created");
        return 100;
      } catch(errExec) {
        this.ReportProgress("Error " + this.Payload.parameters.action);
        this.Error("Manage group " + this.Payload.parameters.action + "  error", errExec);
        return -1;
      }
    }
    if (this.Payload.parameters.action == "DELETE")   {
      try {
        let groupAddress = this.Payload.inputs.group_address;
        this.ReportProgress("Removing group: " + groupAddress);
        await this.safeExec("client.DeleteAccessGroup", [{ //NOT SUPPORTED
          contractAddress: groupAddress,
          client
        }]);
        this.ReportProgress("Group " + groupAddress + " deleted");
        return 100;
      } catch(errExec) {
        this.ReportProgress("Error "+ this.Payload.parameters.action);
        this.Error("Manage object " + this.Payload.parameters.action + "  error", errExec);
        return -1;
      }
    }
    if (this.Payload.parameters.action == "LIST_MEMBERS") {
      return await this.executeListMembers(this.Payload.inputs, outputs, client);
    }
    this.ReportProgress("Error - Action not supported",this.Payload.parameters.action);
    this.Error("Action not supported: " + this.Payload.parameters.action);
    return -1;
  };

  async executeListMembers(inputs, outputs, client) {
    let groupAddress = inputs.group_address;
    let number = await client.CallContractMethod({
      methodName: "membersNum",
      contractAddress: groupAddress,
      methodArgs: []
    });
    if (number._hex) {
      number = parseInt(number._hex, 16);
    }
    this.ReportProgress("Found " + number + " members for group "+ groupAddress);
    outputs.members = [];
    for (let i=0; i < number; i++) {
      this.reportProgress("retrieving user",i,1000);
      let member = await client.CallContractMethod({
        methodName: "membersList",
        contractAddress: groupAddress,
        methodArgs: [i]
      });
      outputs.members.push(member);
    }
    return ElvOAction.EXECUTION_COMPLETE;
  };

  static VERSION = "0.0.3";
  static REVISION_HISTORY = {
    "0.0.1": "Initial release",
    "0.0.2": "Private key input is encrypted",
    "0.0.3": "Adds listing of members"
  };
}




if (ElvOAction.executeCommandLine(ElvOActionManageGroup)) {
  ElvOAction.Run(ElvOActionManageGroup);
} else {
  module.exports=ElvOActionManageGroup;
}
