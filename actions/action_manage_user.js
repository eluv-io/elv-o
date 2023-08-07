const ElvOAction = require("../o-action").ElvOAction;
const ElvOProcess = require("../o-process");
const ElvOFabricClient = require("../o-fabric");



class ElvOActionManageUser extends ElvOAction  {
    ActionId() {
        return "manage_user";
    };


    Parameters() {
        return {parameters: {action: {type: "string", values:["CREATE","TOPOFF"], require: true}}};
    };

    IOs(parameters) {
        let inputs = {
            private_key: {type: "password", "required":false},
            config_url: {type: "string", "required":false}
        };
        let outputs = {};
        if (parameters.action == "CREATE") {
            inputs.name = {type:"string", required: true};
            inputs.mnemonic = {type:"string", required: false, default: null};
            inputs.seeding_funds = {type: "numeric", required: false, default: 1};
            inputs.membership_groups = {type: "array", required: false, default: []};
            outputs.address = {type:"string"};
            outputs.private_key = {type:"string"};
            outputs.balance = {type: "numeric"};
            outputs.wallet_address = {type:"string"};
            outputs.mnemonic = {type:"string"};
        }
        if (parameters.action == "TOPOFF") {
            inputs.addresses = {type:"array", required: true};
            inputs.target_balance = {type:"string", required: true};
            inputs.minimum_balance = {type:"numeric", required: false, default: null};
            outputs.funds_added = {type: "object"};
        }
        return {inputs, outputs};
    };

    async executeCreate(handle, outputs, client, configUrl) {
        try {
            let name = this.Payload.inputs.name;
            let fund = this.Payload.inputs.seeding_funds;
            let wallet = client.GenerateWallet();
            outputs.mnemonic = this.Payload.inputs.mnemonic || wallet.GenerateMnemonic();
            let user = wallet.AddAccountFromMnemonic({mnemonic: outputs.mnemonic, name: name});
            outputs.address  =  await user.getAddress();
            outputs.private_key  = user.signingKey.privateKey;
            this.ReportProgress("Created user " + outputs.address);

            await this.SendFunds({recipient: outputs.address, ether: fund, client});
            this.ReportProgress("Funded user with " + fund);

            let otherClient =  await ElvOFabricClient.InitializeClient(configUrl, outputs.private_key);
            outputs.wallet_address = await this.safeExec("client.userProfileClient.WalletAddress", [{
                client: otherClient
            }]);
            this.ReportProgress("Created user wallet " + outputs.wallet_address);
            for (let i=0; i < 3; i++) { //I have seen this call mysteriously fail the first time
                try {
                    await this.safeExec("client.userProfileClient.MergeUserMetadata", [{
                        metadata: {public: {name: name}},
                        client: otherClient
                    }]);
                    break;
                } catch (err) {
                    this.Debug("Failed to name user on attemps " + (i+1), err);
                }
            }

            for (let group of this.Payload.inputs.membership_groups )  {
                await this.safeExec("client.AddAccessGroupMember", [{
                    contractAddress: group,
                    memberAddress: outputs.address,
                    client
                }]);
                this.ReportProgress("User added to group " + group);
            }

            let balanceWei = await user.getBalance();
            let balanceEther =  client.utils.WeiToEther(balanceWei.toString());
            outputs.balance = balanceEther.toNumber();
            this.ReportProgress("User balance", outputs.balance);

            this.ReportProgress("User provisioned");
            return ElvOAction.EXECUTION_COMPLETE;
        } catch(errExec) {
            this.ReportProgress("Error creating user");
            this.Error("Error creating user", errExec);
            return ElvOAction.EXECUTION_EXCEPTION;
        }
    };

    async executeTopOff(handle, outputs, client) {
      try {
        let addresses = this.Payload.inputs.addresses;
        let targetBalance = this.Payload.inputs.target_balance;
        outputs.funds_added = {};
        for (let address of addresses) {
            let currentBalance = await this.GetBalance(address, client);
            if (currentBalance + 0.1 < targetBalance) {
                let fund = (targetBalance - currentBalance);
                await this.SendFunds({recipient: address, ether: fund, client});
                this.ReportProgress("Funded user " + address + " with " + fund);
                outputs.funds_added[address] = fund;
            } else {
                this.reportProgress("User " + address + " does not need topping off", currentBalance);
            }
        }
        return ElvOAction.EXECUTION_COMPLETE;


      } catch(errExec) {
          this.ReportProgress("Error encountered during top-off of client account");
          this.Error("Error encountered during top-off of client account", errExec);
          return ElvOAction.EXECUTION_EXCEPTION;
      }
    };

    async Execute(handle, outputs) {
        let client;
        let privateKey;
        let configUrl;
        if (!this.Payload.inputs.private_key && !this.Payload.inputs.config_url) {
            client = this.Client;
            configUrl = client.configUrl;
        } else {
            privateKey = this.Payload.inputs.private_key || this.Client.signer.signingKey.privateKey.toString();
            configUrl = this.Payload.inputs.config_url || this.Client.configUrl;
            client = await ElvOFabricClient.InitializeClient(configUrl, privateKey)
        }
        if (this.Payload.parameters.action == "CREATE")   {
            return await this.executeCreate(handle, outputs, client, configUrl)
        }

        if (this.Payload.parameters.action == "TOPOFF") {
            return await this.executeTopOff(handle, outputs, client);
        }

        this.ReportProgress("Error - Action not supported",this.Payload.parameters.action);
        this.Error("Action not supported: " + this.Payload.parameters.action);
        return -1;

    };

    static VERSION = "0.0.5";
    static REVISION_HISTORY = {
        "0.0.1": "Initial release",
        "0.0.2": "Private key input is encrypted",
        "0.0.3": "Adds top-off action",
        "0.0.4": "fixes typo in top-off arguments",
        "0.0.5": "Does not top-off if difference is less than 0.01"
    };
}


if (ElvOAction.executeCommandLine(ElvOActionManageUser)) {
    ElvOAction.Run(ElvOActionManageUser);
} else {
    module.exports=ElvOActionManageUser;
}
