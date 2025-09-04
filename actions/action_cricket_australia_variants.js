const ElvOAction = require("../o-action").ElvOAction
const EventListener = require("./action_cricket_australia_variants_dependecies/cricket_australia_event_listener")
const ElvOFabricClient = require("../o-fabric")
const { execSync } = require('child_process')
const fs = require("fs")
const path = require("path")

const STORE_PLAY_DATA_FOLDER = "/home/o/elv-o/cricket_play_data"


class ElvOActionCricketAustraliaVariants extends ElvOAction  {
    
    ActionId() {
        return "cricket_a__variants";
    };
    
    Parameters() {
        return {
            parameters: {
                action: {
                    type: "string", required: true, 
                    values: ["STORE_PLAY_DATA"]
                }
            }
        };
    };
    
    IOs(parameters) {
        let inputs = {
            private_key: {type: "password", required:false},
            config_url: {type: "string", required:false},
        };
        let outputs = {};
        if (parameters.action == "STORE_PLAY_DATA") {
            // This is used to expose a webhook to receive play data
            inputs.web_hooks = {type: "object", required: false};
            outputs.event_file_path = "string";
        }
        return {inputs, outputs};
    };
    
    async Execute(inputs, outputs) {        
        if (this.Payload.parameters.action == "STORE_PLAY_DATA") {
            return await this.executeStorePlayData({inputs, outputs})
        }      
        throw Error("Action not supported: "+this.Payload.parameters.action);
    };

	async executeStorePlayData({inputs, outputs}) { //new version created by ML on 08/25 to just append the JSON to a JSON array per game
		let payload = inputs.web_hooks || inputs;
		const eventFilePath = path.join(STORE_PLAY_DATA_FOLDER, "event-"+payload.event?.providerMatchId + ".json")
		let event;
		if (fs.existsSync(eventFilePath)){
			event = JSON.parse(fs.readFileSync(eventFilePath));
			event.push(payload);
		} else  {
			event = [payload];
		}
		fs.writeFileSync(eventFilePath, JSON.stringify(event, null, 2));
		outputs.event_file_path = eventFilePath;
		return ElvOAction.EXECUTION_COMPLETE;
	}
    
    async executeStorePlayData_ADM({inputs, outputs}) {
        let payload = inputs.web_hooks
        this.reportProgress("Storing play data for event " + payload)
        const processor = EventListener.createCricketProcessor(payload,STORE_PLAY_DATA_FOLDER)
        // ADM workaround to retrieve the video start absolute timestamp
        const video_start_time_file_path = path.join(STORE_PLAY_DATA_FOLDER, payload.event.providerMatchId + "-start_time.json")
        let video_start_absolute_timestamp = null
        if ( (video_start_time_file_path != null) && fs.existsSync(video_start_time_file_path)) {
            video_start_absolute_timestamp = this.load_video_start_time_from_file(video_start_time_file_path)
        }
        if (video_start_absolute_timestamp) {
            processor.set_start_time_ts(video_start_absolute_timestamp)
        }
        const transformed = processor.transformEvent(payload.event)
        if (!transformed) {
            this.reportProgress("No new transformed data found for event " + payload.event.type)
            return ElvOAction.EXECUTION_FAILED;
        }
        // We need to find a way to clean up STORE_PLAY_DATA_FOLDER
        // and remove the files that are not needed anymore
        return ElvOAction.EXECUTION_COMPLETE
    }

    load_video_start_time_from_file(filename) {        
        if (fs.existsSync(filename)) {
          try {
            const fileContent = fs.readFileSync(filename, 'utf-8')
            const parsed = JSON.parse(fileContent)
            return parsed.video_start_absolute_timestamp
          } catch (err) {
            console.error(`Failed to parse JSON from ${filename}:`, err.message)
            return null
          }   
        }
        return null
    }
    
    
    static REVISION_HISTORY = {
        "0.0.1": "ADM - Initial release - simplified from URC",
        "0.0.2": "ADM - Simplified code to just store the play data as received from the webhook",
        "0.0.3": "ML - Changed to append JSON to a JSON array per game"        
    }

    static VERSION = "0.0.3"
}

if (ElvOAction.executeCommandLine(ElvOActionCricketAustraliaVariants)) {
    ElvOAction.Run(ElvOActionCricketAustraliaVariants)
} else {
    module.exports=ElvOActionCricketAustraliaVariants
}
