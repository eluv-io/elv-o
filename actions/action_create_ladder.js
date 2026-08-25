const ElvOAction = require("../o-action").ElvOAction;
const ElvOFabricClient = require("../o-fabric");

class ElvOCreateLadder extends ElvOAction  {
    
    ActionId() {
        return "create_ladder";
    };
    
    Parameters() {
        return {parameters: {
            input_type: {type: "string", values:["ratio", "master_object_id", "match_original"] }, 
            action: {type: "string", values:["CREATE", "ADD_HIGH_BITRATE_RUNGS"] }
        }};
    };
    
    
    IOs(parameters) {
        if (!parameters.action) {
            parameters.action = "CREATE";
        }
        let inputs={}, outputs={};
        if (parameters.action == "CREATE") {
            inputs = {
                video_bitrate_tiers: {type: "array", required: false, default:[15000000, 9500000, 4222222, 2375000, 1055556, 834020, 690000]},
                video_resolution_tiers: {type: "array", required: false, default: null}, //default:[1, 1, 0.75, 0.5, 0.25],
                min_video_width: {type: "numeric", required:false, default: 320},        
                reference_video_ratio: {type: "object", required:false, default:{width:1920, height:1080, bit_rate: 9500000}},
                audio_bitrates: {type: "object", required:false, default:{1:128000, 2:256000, 6: 384000, 10: 384000}},
                no_upscale: {type: "boolean", required: false, default: true},
                drm_optional: {type: "boolean", required: false, default: true},
                store_clear: {type: "boolean", required: false, default: false},
                playout_formats: {type: "array", required: false, default: null, values:["dash-clear","hls-clear","hls-aes128", "hls-sample-aes","hls-fairplay","dash-widevine"]},
                video_encoding: {type: "string", required: false, default: null},
                video_bit_depth: {type: "numeric", required:false, default: null}
            }; 
                   
            if (parameters.input_type == "ratio") {
                inputs.video_resolution = {type: "string", required: true, description: "format is <width>x<height>, i.e.  16x9"};
            } else {
                inputs.master_object_id =  {type: "string", required: true};
                inputs.master_write_token =  {type: "string", required: false};
                inputs.variant =  {type: "string", required: false, default: "default"};
                inputs.private_key = {type: "password", required: false};
                inputs.config_url = {type: "string", required: false};
            }        
            outputs.ladder = "object";
        }
        if (parameters.action == "ADD_HIGH_BITRATE_RUNGS") {
            inputs.abr_profile = {type: "object", required: true};
            inputs.highest_bitrate = {type: "numeric", required: false, default: 15000000};
            outputs.abr_profile = "object"
        }
        
        return {inputs, outputs};
    };
    
    
    async Execute(inputs, outputs) {
        let action = this.Payload.parameters.action || "CREATE";
        
        if (action == "CREATE")  {
            return await this.executeCreate(inputs, outputs)
        }
        if (action == "ADD_HIGH_BITRATE_RUNGS")  {
            return this.executeHighBitrateRungs(inputs, outputs)
        }
        this.ReportProgress("Unsupported action", action);
        return ElvOAction.EXECUTION_EXCEPTION;
    }
    
    executeHighBitrateRungs(inputs, outputs) { //ADD_HIGH_BITRATE_RUNGS
        let ladder;
        if ((typeof inputs.abr_profile) == "string") {
            let matcher = inputs.abr_profile.match(/^@(.*)/);
            if (!matcher) {
                this.ReportProgress("File path are to be marked with leading @", inputs.abr_profile);
                return ElvOAction.EXECUTION_EXCEPTION;
            }
            ladder = JSON.parse(fs.readFileSync(matcher[1]));
        } else {
            ladder = inputs.abr_profile;
        }
        let topRung, topBitrate=0;
        let mainLadderKey;
        for (let ladderKey in ladder.ladder_specs){
            let rungsSpec = ladder.ladder_specs[ladderKey].rung_specs;
            if (!rungsSpec) continue;
            for (let rungSpec of rungsSpec) {
                if (rungSpec.media_type != "video") continue;
                if (rungSpec.bit_rate > topBitrate) {
                    topRung = rungSpec;
                    topBitrate = rungSpec.bit_rate;
                }
                mainLadderKey = ladderKey;
            }
        }
        let bitrate = inputs.highest_bitrate;
        let newRungs = []
        let first = true;
        while (bitrate > topBitrate) {
            newRungs.push({
                "bit_rate": bitrate,
                "height": topRung.height,
                "media_type": "video",
                "pregenerate": first,
                "width": topRung.width
            });
            first = false;
            bitrate = Math.round(bitrate / 2);
        }
        for (let rungSpec of ladder.ladder_specs[mainLadderKey].rung_specs) {
            rungSpec.pregenerate = false;
            newRungs.push(rungSpec);
        }
        ladder.ladder_specs[mainLadderKey].rung_specs = newRungs;
        outputs.abr_profile = ladder;
        return ElvOAction.EXECUTION_COMPLETE;
    };
    
    async executeCreate(inputs, outputs) {
        let videoResolution = inputs.video_resolution;
        let videoTiers = inputs.video_bitrate_tiers;
        let referenceRatio = inputs.reference_video_ratio;
        let noUpscale = inputs.no_upscale;
        let drmOptional = inputs.drm_optional;
        let playoutFormats;
        if (!inputs.playout_formats) {
            if (drmOptional) {
                playoutFormats = ["dash-clear","hls-clear","hls-aes128", "hls-sample-aes","hls-fairplay","dash-widevine"];
            } else {
                playoutFormats = ["hls-aes128", "hls-sample-aes","hls-fairplay","dash-widevine"];
            }
        } else {
            playoutFormats = inputs.playout_formats
        }
        let storeClear = inputs.store_clear;
        let audioSpecs = inputs.audio_bitrates;
        let aspectRatio;
        let originalBitrate;
        let crf = inputs.crf;
        if ((this.Payload.parameters.input_type == "master_object_id") ||(this.Payload.parameters.input_type == "match_original"))   {
            let client;
            let privateKey;
            let configUrl;
            if (!inputs.private_key && !inputs.config_url){
                client = this.Client;
            } else {
                privateKey = inputs.private_key || this.Client.signer.signingKey.privateKey.toString();
                configUrl = inputs.config_url || this.Client.configUrl;
                client = await ElvOFabricClient.InitializeClient(configUrl, privateKey)
            }
            
            let masterData = await this.getMetadata({
                objectId: inputs.master_object_id,
                writeToken: inputs.master_write_token || inputs.write_token,
                metadataSubtree: "production_master",
                client
            });
            let videoSourceLocation = masterData.variants[inputs.variant].streams.video.sources[0];
            let videoSource = masterData.sources[videoSourceLocation.files_api_path].streams[videoSourceLocation.stream_index];
            this.reportProgress("video-source read from master", videoSource);
            videoResolution = {width: videoSource.width, height: videoSource.height};
            aspectRatio = videoSource.display_aspect_ratio;
            if (!crf) {
                crf = masterData.variants[inputs.variant].streams.video.crf;
            }
            originalBitrate = videoSource.bit_rate;
            if (this.Payload.parameters == "match_original") {
                //video_bitrate_tiers: {type:"array", required: false, default:[15000000, 9500000, 4222222, 2375000, 1055556, 834020, 690000]},
                //reference_video_ratio: {type:"object", required:false, default:{width:1920, height:1080, bit_rate: 9500000}},
                let calculatedVideoBitrateTiers = [];
                let referenceBitrate = (referenceRatio.bit_rate <= 1) ? referenceRatio.bit_rate * originalBitrate : referenceRatio.bit_rate;
                referenceRatio = {width: videoSource.width, height: videoSource.height, bit_rate: referenceBitrate};
                for (let tier of videoTiers) {
                    if (tier <= 1) {
                        calculatedVideoBitrateTiers.push(tier * originalBitrate);
                    } else {
                        calculatedVideoBitrateTiers.push(tier);
                    }
                }
                videoTiers = calculatedVideoBitrateTiers;
            }
        }
        if (this.Payload.parameters.input_type == "probe") {
            //TO BE FLUSHED OUT
        }
        
        outputs.ladder = this.createLadder({videoResolution, aspectRatio, videoTiers, referenceRatio, noUpscale, drmOptional, storeClear, audioSpecs, playoutFormats, crf, originalBitrate});
        return ElvOAction.EXECUTION_COMPLETE;
    };
    
    createLadder({videoResolution, aspectRatio, videoTiers, referenceRatio, noUpscale, drmOptional, storeClear, audioSpecs, playoutFormats, crf, originalBitrate}) {
        this.reportProgress("Creating ladder", {videoResolution, aspectRatio, videoTiers, referenceRatio, noUpscale, drmOptional, storeClear, audioSpecs});
        let resolution = this.parseRatio(videoResolution);
        let ratio = (aspectRatio && this.parseRatio(aspectRatio)) || this.calculateRatio(resolution.width, resolution.height);
        let rungs = this.createVideoLadderRungs({ratio,resolution, tiers: videoTiers, referenceRatio, crf, originalBitrate});
        let videoKey = "{\"media_type\":\"video\",\"aspect_ratio_height\":" + ratio.height + ",\"aspect_ratio_width\":" + ratio.width +"}";
        let ladder = {
            "no_upscale": noUpscale,
            "drm_optional": drmOptional,
            "store_clear": storeClear            
        };
        if (this.ForceUpscale) {
            ladder.no_upscale = false;
        }
        ladder.ladder_specs = this.createAudioLadder(audioSpecs);
        ladder.ladder_specs[videoKey] = { "rung_specs": rungs };
        this.addPlayoutFormat(ladder, playoutFormats);
        return ladder;
    };
    
    createAudioLadder(audioSpecs){
        let audioLadder = {}
        for (let channels in audioSpecs) {
            let audioKey =  "{\"media_type\":\"audio\",\"channels\": " + channels+ "}";
            audioLadder[audioKey] = {
                "rung_specs": [{
                    "bit_rate": audioSpecs[channels],
                    "media_type": "audio",
                    "pregenerate": true
                }]
            };
        }
        return audioLadder;
    }
    
    addPlayoutFormat(ladderSpec, playoutFormats) {
        ladderSpec.segment_specs = {
            "audio": {
                "segs_per_chunk": 15,
                "target_dur": 2
            },
            "video": {
                "segs_per_chunk": 15,
                "target_dur": 2
            }
        };  
        if (this.Payload.inputs.video_encoding) {
            ladderSpec.segment_specs.video.encoding = this.Payload.inputs.video_encoding;
        }
        if (this.Payload.inputs.video_bit_depth) {
            ladderSpec.segment_specs.video.bit_depth = this.Payload.inputs.video_bit_depth;
        }
        ladderSpec.playout_formats = {};
        for (let playoutFormat of playoutFormats) {
            if (playoutFormat == "dash-widevine") {
                ladderSpec.playout_formats[playoutFormat] = {
                    "drm": {
                        "content_id": "",
                        "enc_scheme_name": "cenc",
                        "license_servers": [],
                        "type": "DrmWidevine"
                    },
                    "protocol": {
                        "min_buffer_length": 2,
                        "type": "ProtoDash"
                    }
                };
            }
            if (playoutFormat == "hls-fairplay") {
                ladderSpec.playout_formats[playoutFormat] = {
                    "drm": {
                        "enc_scheme_name": "cbcs",
                        "license_servers": [],
                        "type": "DrmFairplay"
                    },
                    "protocol": {
                        "type": "ProtoHls"
                    }
                }
            }
            if (playoutFormat == "hls-sample-aes") {
                ladderSpec.playout_formats[playoutFormat] = {
                    "drm": {
                        "enc_scheme_name": "cbcs",
                        "type": "DrmSampleAes"
                    },
                    "protocol": {
                        "type": "ProtoHls"
                    }
                }
            }
            if (playoutFormat == "hls-aes128") {
                ladderSpec.playout_formats[playoutFormat] =  {
                    "drm": {
                        "enc_scheme_name": "aes-128",
                        "type": "DrmAes128"
                    },
                    "protocol": {
                        "type": "ProtoHls"
                    }
                }
            };
            if (playoutFormat == "dash-clear") {
                ladderSpec.playout_formats[playoutFormat] = {
                    "drm": null,
                    "protocol": {
                        "min_buffer_length": 2,
                        "type": "ProtoDash"
                    }
                };               
            };
            if (playoutFormat =="hls-clear") {
                ladderSpec.playout_formats[playoutFormat] ={
                    "drm": null,
                    "protocol": {
                        "type": "ProtoHls"
                    }
                };         
            }
        }
    };
    
    createVideoLadderRungs({ratio, resolution, tiers, referenceRatio, crf, originalBitrate}) {
        //let originalHeight = resolution.height;
        //let originalWidth = resolution.height  * ratio.width / ratio.height; 
        let originalWidth = resolution.width;
        let originalHeight = resolution.height;
        
        let calculatedHeight = Math.floor(resolution.width  * ratio.height / ratio.width);
        //if calculated height is larger than specified
        if (calculatedHeight > originalHeight) {
            //   if specified height is undex 65 % of calculated one, it indicates that the height is mis-reported as it happens in some interlaced cases
            if ((originalHeight / calculatedHeight) < 0.65 ) {
                this.reportProgress("Video resolution does not match ratio, height value ignored as widely out of range");
                originalHeight = calculatedHeight;
                this.ForceUpscale = true;
            } else {
                this.reportProgress("Video resolution does not match ratio, using height to calculate width");
                originalWidth = Math.floor(resolution.height  * ratio.width / ratio.height);
            }
        } else {
            this.reportProgress("Video resolution does not match ratio, using width to calculate height");
            originalHeight = calculatedHeight;
        }
        
        //let originalHeight = resolution.width  * ratio.height / ratio.width;
        let pixelsToBitrate =  (referenceRatio.bit_rate * 1.0) / (referenceRatio.height * referenceRatio.width);
        let rungs = [];
        let projectedOriginalBitRate = pixelsToBitrate * originalHeight * originalWidth;
        for (let i=0; i < (tiers.length - 1); i++) {
            if ((projectedOriginalBitRate < tiers[i]) && (projectedOriginalBitRate <= tiers[i+1])) {
                continue;
            }
            let tier = tiers[i];
            let multiplier = Math.sqrt( (tier * 1.0) / projectedOriginalBitRate);
            if ((multiplier >= 1) || (i == 0 )) {
                rungs.push({
                    "crf": crf,
                    "bit_rate": tier,
                    "height": Math.floor(originalHeight / 2.0) * 2, //uses floor for first rung to avoid up-resing
                    "media_type": "video",
                    "pregenerate": true,
                    "width": Math.floor(originalWidth / 2.0) * 2
                });
            } else {
                rungs.push({
                    "bit_rate": tier,
                    "height": Math.round(originalHeight * multiplier / 2.0) * 2,
                    "media_type": "video",
                    "width": Math.round(originalWidth *  multiplier / 2.0) * 2
                });
            }
        }
        if (rungs.length > 0) {
            rungs.push({
                "bit_rate": tiers[tiers.length -1],
                "height": rungs[rungs.length -1].height,
                "media_type": "video",
                "width": rungs[rungs.length -1].width
            });
        } else {
            rungs.push({
                "bit_rate": tiers[tiers.length -1],
                "height": originalHeight,
                "media_type": "video",
                "pregenerate": true,
                "width": Math.floor(originalWidth / 2.0) * 2
            });
        }
        return rungs;
    };
    
    primeFactors(n) {
        const factors = [];
        let divisor = 2;
        
        while (n >= 2) {
            if (n % divisor == 0) {
                factors.push(divisor);
                n = n / divisor;
            } else {
                divisor++;
            }
        }
        return factors;
    };
    
    parseRatio(ratio) {
        if ((typeof ratio) == "string") {
            let matcher = ratio.match(/([0-9]+)x([0-9]+)/);
            if (matcher) {
                return {height: parseInt(matcher[1]), width: parseInt(matcher[2])};
            } 
            matcher = ratio.match(/([0-9]+)\/([0-9]+)/);
            if (matcher) {
                return {height: parseInt(matcher[2]), width: parseInt(matcher[1])};
            }     
            matcher = ratio.match(/^[0-9]+$/);      
            if (matcher) {
                return {height: 1, width: parseInt(ratio)};
            } else {
                throw new Error("Unknown ratio format: " + ratio);
            }
        } else {
            return {height: ratio.height, width: ratio.width};
        }
    };
    
    calculateRatio(width, height) {
        let wFactors = this.primeFactors(width);
        let hFactors = this.primeFactors(height);
        let wIndex=0;
        for (let  factor   of wFactors) {
            let hIndex = hFactors.indexOf(factor);
            if (hIndex >= 0) {
                wFactors[wIndex] = 1;
                hFactors[hIndex] = 1;
            }
            wIndex++;
        }
        let wRatio = wFactors.reduce((a, b)=> a*b, 1) 
        let hRatio = hFactors.reduce((a, b)=> a*b, 1)
        return {
            ratio: "" + wRatio +"x" + hRatio,
            width: wRatio,
            height: hRatio
        }
    };
    
    
    static REVISION_HISTORY = {
        "0.0.1": "Initial release",
        "0.0.2": "Fixes source location when pulling from master object",
        "0.0.3": "Fixes edge case of very low resolution",
        "0.0.4": "Fixes the Fix for edge case of very low resolution",
        "0.0.5": "Uses display_aspect_ratio instead of resolution",
        "0.0.6": "Ensures original width is always even integer",
        "0.0.7": "Supports display ratio expressed as single integer instead of fraction",
        "0.0.8": "Fixes aspect ratio of 1st video rung",
        "0.0.9": "Adds clear playout formats when drm optional is set",
        "1.0.0": "Adds explicit parameter for playout formats to be generated",
        "1.0.1": "Fixes hls-clear playout format",
        "1.0.2": "Modifies default rate to generate more standard resolution rungs. Relies on Width instead of height when using aspect ratio, as height is often mis-reported when files are interlaced",
        "1.0.3": "Forces original height to be an even number",
        "1.0.4": "Adds support for CRF",
        "1.0.5": "Avoids upres in case of non-square pixels",
        "1.0.6": "Adds support for reading source spec from write-token on master object",
        "1.0.7": "Adds action to add High bitrate rungs to a ladder",
        "1.0.8": "2026-06-18 - Adds option to set encoding and bit_depth"
    };
    
    static VERSION = "1.0.8";
}


if (ElvOAction.executeCommandLine(ElvOCreateLadder)) {
    ElvOAction.Run(ElvOCreateLadder);
} else {
    module.exports=ElvOCreateLadder;
}