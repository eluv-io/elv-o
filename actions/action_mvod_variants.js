const ElvOAction = require("../o-action").ElvOAction
const ElvOFabricClient = require("../o-fabric")
const { execSync } = require('child_process')

class ElvOActionMvodVariants extends ElvOAction  {
    ActionId() {
        return "mvod_variants";
    };
    
    Parameters() {
        return {
            parameters: {
                action: {
                    type: "string", required: true, 
                    values: ["PARSE_ADS_SPECS"]
                }
            }
        };
    };
    
    IOs(parameters) {
        let inputs = {
            private_key: {type: "password", required:false},
            config_url: {type: "string", required:false}
        };
        let outputs = {};
        if (parameters.action == "PARSE_ADS_SPECS") {
            inputs.marker_specs = {type: "string", required:true};
            outputs.result = "string";
        }
        return {inputs, outputs};
    };


    async Execute(inputs, outputs) {
        let client;
        if (!this.Payload.inputs.private_key && !this.Payload.inputs.config_url) {
            client = this.Client;
        } else {
            let privateKey = this.Payload.inputs.private_key || this.getPrivateKey();
            let configUrl = this.Payload.inputs.config_url || this.Client.configUrl;
            client = await ElvOFabricClient.InitializeClient(configUrl, privateKey)
        }
        let objectId = this.Payload.inputs.object_id || null;
        let libraryId = null;
        if (objectId == null || objectId == undefined || this.Payload.inputs.master_library != null) {
            libraryId = this.Payload.inputs.master_library || null;
        } else {
            libraryId = await this.getLibraryId(objectId, client);
        }   


        if (this.Payload.parameters.action == "PARSE_ADS_SPECS") {
          return await this.executeAddScteMarkers({client, libraryId, inputs, outputs});
        }
    }

    async executeAddScteMarkers({client, libraryId, inputs, outputs}) {        
        try {
            this.reportProgress("Parsing Ads Specs",inputs.marker_specs);
            const adsSpecs = inputs.marker_specs;
            if (adsSpecs && adsSpecs.length > 0) {
              this.reportProgress("Received Ads specs: ", adsSpecs);
              outputs.result = "Ads specs parsed successfully.";
              return ElvOAction.EXECUTION_COMPLETE;
            } else {
              this.reportProgress("No Ads spec provided");
              outputs.result = "No Ads specs provided.";
              return ElvOAction.EXECUTION_COMPLETE;
            }
        } catch (error) {
            this.reportProgress("Error parsing Ads specs: ", error.message);
            outputs.result = `Error parsing Ads specs: ${error.message}`;
            return ElvOAction.EXECUTION_EXCEPTION;
        }        
    }

    static REVISION_HISTORY = {
        "0.0.1": "ADM - Initial release",
    }

    static VERSION = "0.0.1"
}

if (ElvOAction.executeCommandLine(ElvOActionMvodVariants)) {
    ElvOAction.Run(ElvOActionMvodVariants)
} else {
    module.exports=ElvOActionMvodVariants
}

  