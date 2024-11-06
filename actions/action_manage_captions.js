const ElvOAction = require("../o-action").ElvOAction;
const ElvOFabricClient = require("../o-fabric");
const fs = require("fs");
const parser = require("xml2json");
const path = require("path");
const ElvOMutex = require("../o-mutex");
//const { relativeTimeThreshold } = require("moment");
const { execSync } = require('child_process');


class ElvOManageCaptions extends ElvOAction  {
    
    ActionId() {
        return "manage_captions";
    };
    
    Parameters() {     
        return {
            "parameters": {
                action: {type: "string", required:true, values:["ADD","TRANSLATE", "CLEAR"]}, 
                identify_by_version: {type: "boolean", required:false, default: false}
            }
        };
    };
    
    IOs(parameters) {
        let inputs ={};
        let outputs = {};
        if (parameters.action == "TRANSLATE") {
            inputs.file_path = {type: "string", required: true};
            inputs.offset_sec = {type: "numeric", required: false, default: 0};
            inputs.force_offset = {type: "boolean", required: false, default: false};
            inputs.force_framerate = {type: "boolean", required: false, default: false};
            inputs.encoding_framerate = {type: "numeric", required: false, default: 24};
            inputs.playout_framerate = {type: "numeric", required: false, default: 24};
            inputs.output_file_path = {type: "string", required: false, default: null};
            inputs.line_cues = {type: "string", required: false, default: null};
            inputs.source_type = {type: "string", required: false, default: null, values:["VTT","ITT","SRT", "SCC", "STL", "TTML", "SMPTE-TT 608"/*,"Lambda Cap"*/]};
            outputs.file_path = {type: "string"};
            outputs.offset_sec = {type: "numeric"};
            outputs.anomalies = {type: "array"};
        }
        if (parameters.action == "CLEAR") {
            inputs.offering_key = {type: "string", required:false, default: "default"};
            inputs.label  = {type: "string", required: false,  description: "Label to display for caption stream to be removed"};
            inputs.language = {type: "string", required: false,  description: "Language code for caption stream(s) to be removed"};
            inputs.stream_key =  {type: "string", required: false,  description: "Key for caption stream to be removed"};
            inputs.clear_all = {type: "boolean", required: false, default: false};
            if (parameters.identify_by_version) {
                inputs.mezzanine_object_version_hash = {type: "string", required: true};
            } else {
                inputs.mezzanine_object_id = {type: "string", required: true};
            }
            inputs.safe_update = {type: "boolean", required: false, default: false};
            inputs.private_key = {type: "password", required:false};
            inputs.config_url = {type: "string", required:false};
            outputs.mezzanine_object_version_hash = {type: "string"};
            outputs.removed_stream_keys = {type: "array"};
        }
        if (parameters.action == "ADD") {
            inputs.file_path = {type: "string", required: true};
            inputs.label  = {type: "string", required: true,  description: "Label to display for caption stream"};
            inputs.language = {type: "string", required: false,  description: "Language code for caption stream (some older players may use this as the label)"};
            inputs.stream_key =  {type: "string", required: false,  description: "Key for new caption stream (if omitted, will be generated from label and filename)"};
            if (parameters.identify_by_version) {
                inputs.mezzanine_object_version_hash = {type: "string", required: true};
            } else {
                inputs.mezzanine_object_id = {type: "string", required: true};
            }
            inputs.safe_update = {type: "boolean", required: false, default: false};
            inputs.private_key = {type: "password", required:false};
            inputs.config_url = {type: "string", required:false};
            inputs.offering_key = {type: "string", required:false, default: "default"};
            inputs.store_encrypted = {type: "boolean", required:false, default: false};
            inputs.forced = {type: "boolean", required:false, default: false, description: "Flag captions as forced subtitles"};
            inputs.is_default = {type: "boolean", required:false, default: false, description: "Set as default caption stream"};
            inputs.offset_sec = {type: "numeric", required: false, default: null, description: "Number of seconds to add or (-) subtract from timestamps in captions file"};
            outputs.mezzanine_object_version_hash = {type: "string"};
            outputs.caption_key = {type: "string"};
        }
        return {inputs, outputs};
    };
    
    async Execute(handle, outputs) {
        let parameters = this.Payload.parameters;
        if (parameters.action == "TRANSLATE") {
            return this.executeTranslate(this.Payload.inputs, outputs);
        }
        if (parameters.action == "ADD") {
            return this.executeAdd(this.Payload.inputs, outputs);
        }
        if (parameters.action == "CLEAR") {
            return this.executeClear(this.Payload.inputs, outputs);
        }
        this.Error("Unknown action",parameters.action);
        return ElvOAction.EXECUTION_EXCEPTION;
    };
    
    
    executeTranslate(inputs, outputs){
        outputs.anomalies = [];
        let filepath = inputs.file_path;
        let captionsText;
        let extension;
        try {
            let sourceType = inputs.source_type &&  inputs.source_type.toLowerCase().replace(/^\./,"");
            extension = (path.basename(filepath).match(/\.([^.]+)$/) || ["",""])[1].toLowerCase();
            outputs.force_offset = inputs.force_offset;
            outputs.force_framerate = inputs.force_framerate;
            /*if (inputs.force_framerate)  {
                this.reportProgress("Playout framerate forced to match encoding framerate");
                inputs.playout_framerate = inputs.encoding_framerate;
            }*/
            if ((sourceType && (sourceType == "vtt")) || (extension == "vtt")) {
                captionsText = this.translateVTT(filepath, inputs.offset_sec, inputs.encoding_framerate, inputs.playout_framerate, outputs);
            }
            if ((sourceType && (sourceType == "itt")) || (extension == "itt")) {
                captionsText = this.translateITT(filepath, inputs.offset_sec, inputs.encoding_framerate, inputs.playout_framerate, outputs);
            }
            if ((sourceType && ((sourceType == "ttml") || (sourceType == "SMPTE-TT 608"))) || (extension == "xml")) {
                captionsText = this.translateSMPTE(filepath, inputs.offset_sec, inputs.encoding_framerate, inputs.playout_framerate, outputs);
            }
            if ((sourceType && (sourceType == "scc")) || (extension == "scc")) {
                if (this.Payload.parameters.native) {
                    captionsText = this.translateSCCNative(filepath, inputs.offset_sec, inputs.encoding_framerate, inputs.playout_framerate, outputs);
                }
                if (this.Payload.parameters.debug) {
                    captionsText = this.translateSCCNative(filepath, inputs.offset_sec, inputs.encoding_framerate, inputs.playout_framerate, outputs);
                }
                if (this.Payload.parameters.ffmpeg) {
                    captionsText = this.translateSCC(filepath, inputs.offset_sec, inputs.encoding_framerate, inputs.playout_framerate, outputs);
                }   
                if (this.Payload.parameters.reconciled) {
                    let textSafe = this.translateSCCNative(filepath, inputs.offset_sec, inputs.encoding_framerate, inputs.playout_framerate, outputs);
                    let timecodeSafe = this.translateSCC(filepath, inputs.offset_sec, inputs.encoding_framerate, inputs.playout_framerate, outputs);
                    captionsText = this.reconcileVTT(timecodeSafe, textSafe);
                }             
                if (this.Payload.parameters.preprocessed_ffmpeg) {
                    let preprocessedFile = this.preprocessForFfmpeg(filepath);
                    captionsText = this.translateSCC(preprocessedFile, inputs.offset_sec, inputs.encoding_framerate, inputs.playout_framerate, outputs);
                } 
                if (!captionsText) { //default
                    captionsText = this.translateSCCNative(filepath, inputs.offset_sec, inputs.encoding_framerate, inputs.playout_framerate, outputs);
                }               
            }
            if ((sourceType && (sourceType == "srt")) || (extension == "srt")) {
                captionsText = this.translateSRT(filepath, inputs.offset_sec, inputs.encoding_framerate, inputs.playout_framerate, outputs);
            }
            if ((sourceType && (sourceType == "stl")) || (extension == "stl")) {
                captionsText = this.translateSTL(filepath, inputs.offset_sec, inputs.encoding_framerate, inputs.playout_framerate, outputs);
            }
            if (!captionsText) {               
                throw new Error("Unsupported format "+ (extension || sourceType));
            }
        } catch(err) {
            this.Error("Could not translate "+ filepath, err);
            if (err.message) {
                outputs.anomalies.push(err.message);
            } else {
                outputs.anomalies.push("Could not translate -" + err);
            }
            let usedOffset = (outputs.offset_sec != null) ? outputs.offset_sec : inputs.offset_sec;
            if (err.message  && err.message.match(/Timecode with offset is negative/) && !inputs.force_offset && (usedOffset < 0)) {
                this.reportProgress("re-trying with no-offset");
                inputs.offset_sec = 0;
                inputs.force_offset = true;
                return  this.executeTranslate(inputs, outputs)
            }
            return ElvOAction.EXECUTION_EXCEPTION;
        }
        if (!outputs.file_path) {
            let outputFilePath = inputs.output_file_path || (((extension && filepath.replace(/\.[^.]+$/, "")) || filepath) + "_converted.vtt");
            fs.writeFileSync(outputFilePath, captionsText);
            outputs.file_path = outputFilePath;
        }
        return ElvOAction.EXECUTION_COMPLETE;
    };
    
    toTimecode(sec) {
        let hours   = Math.floor(sec / 3600); // get hours
        let minutes = Math.floor((sec - (hours * 3600)) / 60); // get minutes
        let seconds = sec - (hours * 3600) - (minutes * 60); //  get seconds
        
        // add 0 if value < 10; Example: 2 => 02
        if (hours   < 10) {hours   = "0"+hours;}
        if (minutes < 10) {minutes = "0"+minutes;}
        if (seconds < 10) {seconds = "0"+seconds.toFixed(3);} else {seconds = seconds.toFixed(3);} 
        return hours+':'+minutes+':'+seconds; // Return is HH : MM : SS
    };
    
    // "00:02:58:19" -> 178.79166666666666 
    // "01:01:49.083" -> 3709.083
    // "00:09:15;17" -> 555.7083333333334
    // "01:16:25,878" -> 4585.878
    fromTimecode(timecode, encodingFramerate) {
        if (!encodingFramerate) {
            encodingFramerate = 24;
        }
        if (timecode.match(/([0-9]+):([0-9]+)/) && !timecode.match(/([0-9]+):([0-9]+):([0-9]+)/)) {
            timecode = "00:" + timecode;
        }
        let matcher = timecode.match(/([0-9]+):([0-9]+):([0-9]+)[\.,]([0-9]+)/);
        let time;
        if (!matcher) {
            matcher = timecode.match(/([0-9]+):([0-9]+):([0-9]+)[:;]([0-9]+)/);
            if (matcher) {
                time = parseInt(matcher[1]) * 3600 + parseInt(matcher[2]) * 60 + parseInt(matcher[3]) + parseInt(matcher[4]) / encodingFramerate;
            } else {
                throw new Error("Invalid timecode format " + timecode);
            }
        } else {
            time = parseInt(matcher[1]) * 3600 + parseInt(matcher[2]) * 60 + parseInt(matcher[3]) + parseInt(matcher[4]) / 1000;
        }
        if (!Number.isFinite(time)) {
            
            throw new Error("Invalid  timecode with offset " +timecode + "-> " + time);
        }
        return  time;
    };
    
    convertTimecodeOld(timecode, offsetSec, encodingFramerate, playoutFramerate) {   
        //this.Debug("convertTimecode", {timecode, offsetSec, encodingFramerate, playoutFramerate});    
        //let matcher = timecode.match(/([0-9]+):([0-9]+):([0-9]+)[\.,]([0-9]+)/);
        let time = this.fromTimecode(timecode, encodingFramerate);
        let adjustedTime = (time + offsetSec);
        if (!Number.isFinite(adjustedTime)) {
            throw new Error("Invalid  timecode with offset " +timecode +"-> " + adjustedTime);
        }
        if (adjustedTime < 0) {
            throw new Error("Timecode with offset is negative " +timecode +"/" + offsetSec.toString());
        }
        //let scaledTime = adjustedTime / encodingFramerate * playoutFramerate
        return this.toTimecode( adjustedTime / encodingFramerate * playoutFramerate );
    };
    
    convertTimecode(timecode, offsetSec, encodingFramerate, playoutFramerate) {  
        let matcher = timecode.match(/[0-9]+:[0-9]+:[0-9]+([:;])([0-9]+)/);
        let dropframe;
        if (!matcher) {
            dropframe = (encodingFramerate != playoutFramerate); 
        } else {
            if (matcher[1] == ":") {
                dropframe = false;
            } else { // ";"
                dropframe = (encodingFramerate != playoutFramerate);  //can be forced into non-drop
            }
        }
        let multiplier = 1;
        let forceFramerate = this.Payload && this.Payload.inputs && this.Payload.inputs.force_framerate;
        if (dropframe && !forceFramerate) {
            let adjustmedFramerate = ((encodingFramerate * 60 - 2) * 9  + (encodingFramerate * 60)) /600;
            multiplier = encodingFramerate / adjustmedFramerate;
        } 
        if (forceFramerate) {
            //this.reportProgress("forceFramerate", {forceFramerate, encodingFramerate, playoutFramerate});
            multiplier =  playoutFramerate / encodingFramerate;
        }
        //this.Debug("convertTimecode", {timecode, offsetSec, encodingFramerate, playoutFramerate});    
        //let matcher = timecode.match(/([0-9]+):([0-9]+):([0-9]+)[\.,]([0-9]+)/);
        let time = this.fromTimecode(timecode, encodingFramerate);
        
        let adjustedTime = (time + offsetSec);
        if (!Number.isFinite(adjustedTime)) {
            throw new Error("Invalid  timecode with offset " +timecode +"-> " + adjustedTime);
        }
        if (adjustedTime < 0) {
            if (adjustedTime > -1.5) {
                this.ReportProgress("Slightly negative timecode rounded to 0", adjustedTime);
                adjustedTime = 0
            } else {
                throw new Error("Timecode with offset is negative " +timecode +"/" + offsetSec.toString());
            }
        }
        let scaledTime = adjustedTime * multiplier ;
        let adjustedTimecode = this.toTimecode( scaledTime);
        //console.log("timecode", timecode, offsetSec, multiplier, time, scaledTime, adjustedTimecode);
        return adjustedTimecode
    };
    
    parseSRTLine(line, offsetSec, encodingFramerate, playoutFramerate) {
        //8
        //01:03:48,491 --> 01:03:51,744
        //-Fui pescar em Cuernavaca.
        //-Claro que sim.
        if (!this.SRT_LINECOUNTER) {
            this.SRT_LINECOUNTER = 1;
        }
        let matcher = line.match(/([0-9:.;,]+) --> ([0-9:.;,]+)/);
        if (matcher) {
            return this.convertTimecode(matcher[1],  offsetSec, encodingFramerate, playoutFramerate) + " --> " + this.convertTimecode(matcher[2],  offsetSec, encodingFramerate, playoutFramerate); 
        } else {
            if (line.match(/^[0-9]+$/) && (parseInt(line) == this.SRT_LINECOUNTER )) {
                this.SRT_LINECOUNTER++;
                return null;
            }
        }
        let parsedLine = line.replace(/{(\/*[iub])}/g,"<$1>").replace(/^{\\[^}]+}/,"").replace(/{\an*[0-9]+}/g, "");
        return parsedLine;
    };
    
    
    parseVTT(text) {
        let parsed = {};
        let lines = text.split(/\n/);
        let key = null;
        let value = [];
        for (let line of lines) {
            let matcher = line.match(/([0-9:.;,]+) --> ([0-9:.;,]+)/);
            if (matcher) {
                if (key) {
                    parsed[key] = value.join("\n");
                    value = [];
                }
                key = line;
            } else {
                if (key) {
                    value.push(line);
                }
            }
        }
        if (key) {
            parsed[key] = value.join("\n");
        }
        return parsed;
    };
    
    reconcileVTT(timecodeSafe, textSafe) {
        let vttTime = this.parseVTT(timecodeSafe);
        let vttText = this.parseVTT(textSafe);
        let keysTime = Object.keys(vttTime).sort();
        let keysText = Object.keys(vttText).sort();
        let reconciledVTT = ["WEBVTT\n"];
        for (let i=0; i < keysTime.length; i++) {
            let keyTime = keysTime[i];
            let keyText = keysText[i];
            let line = keyTime +"\n"+ vttText[keyText];
            reconciledVTT.push(line);
        }
        return reconciledVTT.join("\n");
    };
    
    reconcileVTTOld(timecodeSafe, textSafe) {
        let vttTime = this.parseVTT(timecodeSafe);
        let vttText = this.parseVTT(textSafe);
        let keysTime = Object.keys(vttTime).sort();
        let keysText = Object.keys(vttText).sort();
        let offset=0;//in case lines are split in vttTime
        let changes = 0;
        for (let i=0; (i+offset) < keysTime.length; i++) {
            let keyTime = keysTime[i+offset];
            let keyText = keysText[i];
            let lineTime = vttTime[keyTime].replace(/<.*?>/g,'').replace(/[^A-Za-z0-9]/g,"");
            let lineText = (vttText[keyText] || "").replace(/<.*?>/g,'').replace(/[^A-Za-z0-9]/g,"");
            if (lineText && (lineTime != lineText) && (lineText.length >= lineTime)) {  
                let matcher = new RegExp("^"+lineTime);
                if (!lineText.match(matcher)) { //line was not split, replace completely
                    this.reportProgress("Discrepency found replacing ",{old: vttTime[keyTime], new: vttText[keyText]})
                    vttTime[keyTime] = vttText[keyText];  
                    changes++;                 
                } else {
                    
                    let nextKey = keysTime[i + 1 + offset];
                    let nextLine = vttTime[nextKey] || "";
                    let compositeLine = lineTime + nextLine.replace(/<.*?>/g,'').replace(/[^A-Za-z0-9]/g,"");
                    if (compositeLine == lineText) {
                        this.reportProgress("split line found ",{found: [vttTime[keyTime], vttTime[keyTime + 1]], original: vttText[keyText]});
                        offset++;
                    } else {
                        this.reportProgress("Entry " + keyTime + "--" + keyText);
                        this.reportProgress("Compared single", lineTime,lineText);
                        this.reportProgress("Compared composite", compositeLine,lineText);
                        this.reportProgress("Could not reconcile discrency",{found: vttTime[keyTime], expecting: vttText[keyText]});
                    }
                }
            }
        }
        if (changes == 0) {
            this.reportProgress("No reconciliation changes were made");
            return timecodeSafe;
        } else {
            let reconciledVTT = ["WEBVTT\n"];
            for (let key of keysTime) {
                let line = key +"\n"+ vttTime[key];
                reconciledVTT.push(line);
            }
            return reconciledVTT.join("\n");
        }
    };
    
    parseVTTLine(line, offsetSec, encodingFramerate, playoutFramerate, lineCues) {
        //10:01:38.625 --> 10:01:40.625
        let matcher = line.match(/([0-9:.;,]+) --> ([0-9:.;,]+)/);
        if (matcher) {
            if  (lineCues) {
                return this.convertTimecode(matcher[1],  offsetSec, encodingFramerate, playoutFramerate) + " --> " + this.convertTimecode(matcher[2],  offsetSec, encodingFramerate, playoutFramerate) + " " + lineCues; 
            } else {
                return this.convertTimecode(matcher[1],  offsetSec, encodingFramerate, playoutFramerate) + " --> " + this.convertTimecode(matcher[2],  offsetSec, encodingFramerate, playoutFramerate); 
            }
        } 
        return line;
    };
    
    translateVTT(filePath, offsetSec, encodingFramerate, playoutFramerate, outputs) {
        this.reportProgress("translateVTT", {filePath, offsetSec, encodingFramerate, playoutFramerate, outputs});
        let lines = []; 
        let rawtext = fs.readFileSync(filePath, "utf-8");
        if (outputs) {
            outputs.offset_sec = offsetSec;
        }
        for (let line of rawtext.split(/\n/)) {
            try {
                lines.push(this.parseVTTLine(line, offsetSec, encodingFramerate, playoutFramerate, this.Payload.inputs.line_cues));
            } catch(err) {
                if (outputs && outputs.trim && err.message && err.message.match(/Timecode with offset is negative/)){
                    this.reportProgress("Trimming off captions in parts before entry-point", line)
                } else {
                    throw err;
                }
            }
        }
        return lines.join("\n");
    };
    
    translateSRT(filePath, offsetSec, encodingFramerate, playoutFramerate, outputs) {
        let lines = ["WEBVTT\n"]; 
        let rawtext = fs.readFileSync(filePath, "utf-8");
        if (outputs) {
            outputs.offset_sec = offsetSec;
        }
        for (let line of rawtext.split(/\n/)) {
            let parsedLine = this.parseSRTLine(line.trim(), offsetSec, encodingFramerate, playoutFramerate);
            if (parsedLine != null) {
                lines.push(parsedLine);
            }
        }
        return lines.join("\n");
    };
    
    
    getToDeepestSpan(text, section) {
        //this.Debug("getToDeepestSpan", text);
        if (!section) {
            section = text.match(/<span([^>]*?)>(.*?)<\/span>/);
        }
        if  (!section) {
            return null;
        }
        if (section[2].match(/<span/)) {
            return this.getToDeepestSpan(section[2]+"</span>");
        }
        if (section[1].match(/italic/)){
            return  {from: section[0], to: "__ITALIC_START__"+section[2]+"__ITALIC_END__"}; 
        } else {
            return {from: section[0], to: section[2]};
        }
    };
    
    translateITT(filePath, offsetSec, encodingFramerate, playoutFramerate, outputs)  {
        let rawtext = fs.readFileSync(filePath, "utf-8");
        
        let textLines = rawtext.split(/[\n\r]+/);
        let parsable = textLines.join("__LINEFEED__");
        parsable = parsable.replace(/<[bB][rR] *\/*> */g,"__BR__").replace(/<\/[bB][rR]> */g,"");
        
        //removes $ 
        parsable = parsable.replace(/\$/g,"__DOLLAR__");
        
        //removes the empty spans <span />
        parsable = parsable.replace(/<span[^>]*\/>/g,"");
        
        //ruby processing
        //<span ry:kind="rb">ら</span><span ry:kind="rt">・</span>
        //--><b>ら</b>
        parsable = parsable.replace(/<span +ry:kind="rb">([^<]+)<\/span><span ry:kind="rt">・<\/span>/g,"&lt;b&gt;$1&lt;/b&gt;");
        
        //<p><span style="italic"><span ry:kind="ruby" style="ruby-before-between"><span ry:kind="rb">冥</span><span ry:kind="rt">めい</span></span>王サウロンは人知れず⸺</span></p>
        //--> <i>冥(めい)</i>王サウロンは人知れず⸺
        parsable = parsable.replace(/<span +ry:kind="rt">([^<]+)<\/span>/g,"($1)");
        
        while  (true){
            let section = parsable.match(/<span([^>]*?)>(.*?)<\/span>/);
            if  (!section) {
                break;
            }
            let textSub = this.getToDeepestSpan(parsable, section);
            if (textSub) {
                parsable = parsable.replace(textSub.from, textSub.to);
            }
            /*
            let section = parsable.match(/<span([^>]+?)>(.*?)<\/span>/);
            if  (!section) {
                break;
            }
            if (section[1].match(/<span>/)) {
                
            }
            if (section[1].match(/italic/)){
                parsable = parsable.replace(section[0], "__ITALIC_START__"+section[2]+"__ITALIC_END__"); 
            } else {
                parsable = parsable.replace(section[0], section[2]); 
            }
            */
        }
        let textToParse = parsable.replace(/__LINEFEED__/g,"\n").replace(/__DOLLAR__/g,"$$");  
        let parsed = parser.toJson(textToParse, { object: true , reversible: true});
        let result = {};
        result.language_code = parsed.tt['xml:lang'];
        result.offset = parsed.tt.body.div.begin;
        let offsetSign = result.offset && (result.offset.match(/^-/) ? -1 : 1)
        let documentOffsetSec = result.offset && (Math.round(this.fromTimecode(result.offset)) * offsetSign);
        if ((offsetSec != null)  && (documentOffsetSec != null) && (offsetSec != documentOffsetSec)  && (!outputs || !outputs.force_offset) ) {
            this.reportProgress("Mismatched offset, using document", {document: documentOffsetSec, provided: offsetSec});
            offsetSec = documentOffsetSec;
        }
        if ((offsetSec == null)  &&  documentOffsetSec) {
            this.reportProgress("No offset provided, using document", {document: documentOffsetSec, provided: offsetSec});
            offsetSec = documentOffsetSec;
        }
        if (outputs) {
            outputs.offset_sec = offsetSec;
        }
        let documentFramerate;
        if (parsed.tt['ttp:frameRate'] && parsed.tt['ttp:frameRateMultiplier']){
            let frameRateMultiplier = parsed.tt['ttp:frameRateMultiplier'].split(" ")
            documentFramerate = parseInt(parsed.tt['ttp:frameRate']) * 1.0 * parseInt(frameRateMultiplier[0]) / parseInt(frameRateMultiplier[1]);
        }
        
        if (documentFramerate && (documentFramerate != playoutFramerate) && (!outputs || !outputs.force_framerate) ) {
            this.reportProgress("Mismatched playout framerate, using document", {document: documentFramerate, provided: playoutFramerate});
            if (parsed.tt["ttp:dropMode"] == "nonDrop") {
                this.reportProgress("Non-Drop, using specified playout", {document: documentFramerate, provided: playoutFramerate});
                playoutFramerate = documentFramerate;
                encodingFramerate = documentFramerate;
            } else {                
                this.reportProgress("Drop, using specified playout", {document: documentFramerate, provided: playoutFramerate});
                encodingFramerate = documentFramerate;
                playoutFramerate = parsed.tt['ttp:frameRate'];
            }
        }
        this.reportProgress("Framerate used", {encodingFramerate, playoutFramerate});
        let rawLines = parsed.tt.body.div.p;
        let lines = ["WEBVTT\n"];
        if (!(rawLines instanceof Array)) {
            rawLines = [rawLines];
        }
        for (let rawLine of rawLines) {
            /* {
                style: 'basic',
                region: 'pop14',
                begin: '10:10:44:06',
                end: '10:10:46:23',
                'tts:origin': '10.00% 79.33%',
                'tts:extent': '95.00% 5.33%',
                '$t': "j'étais le juif de service, moi aussi."
            }
            to
            10:01:38.625 --> 10:01:40.625
            Composição e interpretação
            */
            //this.Debug("rawLine", rawLine);
            let text = this.getText(rawLine);
            let lineCues = this.Payload.inputs.line_cues ? (" "+this.Payload.inputs.line_cues) : "";
            if (text) {
                text = text.replace(/__BR__/g,"\n").replace(/__ITALIC_START__/g,"<i>").replace(/__ITALIC_END__/g,"</i>");
                text = text.split("\n").map(function(l){return l.trim()}).filter(function(l){return l}).join("\n");
                let entry = {
                    start: this.convertTimecode(rawLine.begin, offsetSec, encodingFramerate, playoutFramerate),
                    end: this.convertTimecode(rawLine.end, offsetSec, encodingFramerate, playoutFramerate),
                    text: text
                }
                
                lines.push("\n"+ entry.start+ " --> " + entry.end + lineCues + "\n" + entry.text + "\n") 
            } else {
                if (rawLine["$t"]) {
                    this.reportProgress("parsing error", rawLine);
                    throw new Error("parsing error - " + JSON.stringify(rawLine));
                }
            }           
        }
        return lines.join("");
    };
    
    getText(rawLine) {
        //this.Debug("rawLine", rawLine);
        let text= "";
        if ((typeof rawLine) == "object") {
            for (let k in rawLine) {
                if (k  == "$t") {                      
                    let italic =  false;
                    for (let kk in rawLine) {
                        if ((kk !=  "$t") && (kk.toLowerCase().match(/italic/) || (((typeof rawLine[kk]) == "string") && rawLine[kk].toLowerCase().match(/italic/)))) {
                            italic=true;
                            break;
                        } 
                    }
                    if (italic) {
                        text += ("<i>"+ rawLine["$t"] + "</i>")
                    } else {
                        text += rawLine["$t"];
                    }
                    
                } else {
                    text += this.getText(rawLine[k]);
                }
            }            
            return text;
        } 
        if ((typeof rawLine) == "array") {
            for (let item of rawLine) {
                text += this.getText(rawLine[item]);
            }
        }
        if ((typeof rawLine) == "string") {            
            return "";
        }
        this.Debug("Line is not an object", rawLine)
        throw new Error("Line is not an object "+ rawLine);
    }
    
    translateSMPTE(filePath, offsetSec, encodingFramerate, playoutFramerate, outputs)  {
        let rawtext = fs.readFileSync(filePath, "utf-8");
        let textLines = rawtext.split(/[\n\r]+/);
        let parsable = textLines.join("__LINEFEED__");
        parsable = parsable.replace(/<[bB][rR] *\/*> */g,"__BR__").replace(/<\/[bB][rR]> */g,"");
        
        
        while  (true){
            let section = parsable.match(/<span([^>]+?)>(.*?)<\/span>/);
            if  (!section) {
                break;
            }
            if (section[1].match(/italic/)){
                parsable = parsable.replace(section[0], "__ITALIC_START__"+section[2]+"__ITALIC_END__"); 
            } else {
                parsable = parsable.replace(section[0], section[2]); 
            }
        }
        let textToParse = parsable.replace(/__LINEFEED__/g,"\n")
        this.Debug("textToParse", textToParse);
        let parsed = parser.toJson(textToParse, { object: true, reversible: true });
        let result = {};
        result.language_code = parsed.tt['xml:lang'];
        //result.offset = parsed.tt.body.div.begin;
        if (outputs) {
            outputs.offset_sec = offsetSec;
        }
        let rawLines = parsed.tt.body.div.p;
        if (!(rawLines instanceof Array)) {
            rawLines = [rawLines];
        }
        let lines = ["WEBVTT\n"];
        let entries = [];
        let lineCues = this.Payload.inputs.line_cues ? (" "+this.Payload.inputs.line_cues) : "";
        for (let rawLine of rawLines) {
            /* {
                style: 'basic',
                region: 'pop14',
                begin: '10:10:44:06',
                end: '10:10:46:23',
                'tts:origin': '10.00% 79.33%',
                'tts:extent': '95.00% 5.33%',
                '$t': "j'étais le juif de service, moi aussi."
            }
            to
            10:01:38.625 --> 10:01:40.625
            Composição e interpretação
            */
            let text = this.getText(rawLine);
            if (text) {
                text = text.replace(/__BR__/g,"\n").replace(/__ITALIC_START__/g,"<i>").replace(/__ITALIC_END__/g,"</i>");
                text = text.split("\n").map(function(l){return l.trim()}).filter(function(l){return l}).join("\n");
            }
            let entry = {
                start: this.convertTimecode(rawLine.begin, offsetSec, encodingFramerate, playoutFramerate),
                end: this.convertTimecode(rawLine.end, offsetSec, encodingFramerate, playoutFramerate),
                text: text
            }
            
            if (!entry.text) {
                this.reportProgress("parsing error", rawLine);
            }
            entries.push(entry);
            //lines.push("\n"+ entry.start+ " --> " + entry.end + "\n" + entry.text + "\n"); 
        }
        let previousStart;
        let previousEnd;
        for  (let entry of entries) {
            if ((entry.start != previousStart) || (entry.end != previousEnd)) {
                previousStart = entry.start; 
                previousEnd = entry.end;
                lines.push("\n"+ entry.start+ " --> " + entry.end+lineCues + "\n" + entry.text + "\n"); 
            } else {
                lines.push(entry.text+ "\n");
            }
        }
        return lines.join("");
    };
    
    translateSCC(filePath, offsetSec, encodingFramerate, playoutFramerate, outputs)  {
        try {
            if (outputs) {
                outputs.offset_sec = offsetSec;
            }
            let extension = (path.basename(filePath).match(/\.([^.]+)$/) || ["",""])[1].toLowerCase();
            let outputFilePath =  ((extension && filePath.replace(/\.[^.]+$/, "")) || filePath) + "_converted.vtt";
            
            let commandLine = "ffmpeg -y  -i \""+ filePath+ "\" \"" + outputFilePath + "\"";
            this.trackProgress(15, "Command line prepared",commandLine);
            let results = execSync(commandLine).toString();
            this.ReportProgress("Conversion executed ", results);
            let rawtext = fs.readFileSync(outputFilePath, "utf-8");
            let lines = [];
            let rawLines = rawtext.split(/\n/);
            let prevEnd;
            for (let rawLine of rawLines) {
                let matcher = rawLine.match(/([0-9:.;,]+) --> ([0-9:.;,]+)/);
                if (matcher) {
                    if (matcher[1] == prevEnd) {
                        let newStart = this.convertTimecode(matcher[1], 0.001, encodingFramerate, encodingFramerate);
                        rawLine = newStart + " --> " + matcher[2];
                    } else {
                        let newStart = this.convertTimecode(matcher[1], 0, encodingFramerate, encodingFramerate); //to force HH: when ommited in start timecode
                        rawLine = newStart + " --> " + matcher[2]; 
                    }
                    prevEnd = matcher[2];
                    lines.push(rawLine);
                } else {
                    lines.push(rawLine.replace(/\\h/g," ")); 
                }               
            }
            let captionsText = lines.join("\n");
            if (offsetSec || ((encodingFramerate != playoutFramerate) && outputs.force_framerate)) {
                let tempFilePath = ((extension && filePath.replace(/\.[^.]+$/, "")) || filePath) + ".vtt";
                fs.writeFileSync(tempFilePath ,captionsText);
                return this.translateVTT(tempFilePath, offsetSec, encodingFramerate, playoutFramerate, outputs)
            }
            return captionsText;
        } catch(errSCC) {
            if ((offsetSec != 0) && !outputs.force_offset && errSCC.message && errSCC.message.match(/Timecode with offset is negative/)){
                this.reportProgress("SCC with negative offset timecodes are typically not offset, using 0 instead");
                return this.translateSCC(filePath, 0, encodingFramerate, playoutFramerate, outputs); 
            } else {
                throw errSCC;
            }
        }
    };
    
    preprocessForFfmpeg(filePath) { //(94f2 91ae(italic)
        let rawtext = fs.readFileSync(filePath, "utf-8");
        let rawLines = rawtext.split(/\n/);
        let lines = [];
        let modified = false;
        let toBeRemoved = ["94f8", "947c", "91ae", "947a", "94fe", "917a", "917c"];
        let toBeReplaced = [];//[{f:"9170", s:"942d"}, {f:"9452", s:"942d"}];
        for (let rawLine of rawLines) {
            let line = rawLine;
            for (let exp of  toBeRemoved) {
                line = line.replace(new RegExp(" "+exp, "g"),"").replace(new RegExp(exp+ " ", "g"),"").replace(new RegExp(exp, "g"),"");
            }
            for (let rep of toBeReplaced) {
                line = line.replace(new RegExp(rep.f, "g"), rep.s);
                console.log("replacing", rep.f, rep.s )
            }
            //let line = rawLine.replace(/947a /g,"").replace(/ 947a/g,"").replace(/947a/g,"").replace(/94f8 /g,"").replace(/ 94f8/g,"").replace(/94f8/g,""); 
            //let line = rawLine.replace(/94f8 /g,"").replace(/ 94f8/g,"").replace(/94f8/g,"").replace(/91ae /g,"").replace(/ 91ae/g,"").replace(/91ae/g,""); 
            lines.push(line);
            if (line != rawLine) {
                modified = true;
            }
        }
        if (modified) {
            let newPath= filePath.replace(/\.[^\.]*$/,"") + ".ffmpeg";
            fs.writeFileSync(newPath, lines.join("\n"));
            return newPath
        } 
        return filePath
    };
    
    translateSCCNativeBad(filePath, offsetSec, encodingFramerate, playoutFramerate, outputs)  {
        let debugMode = this.Payload.parameters.debug;
        try {
            if (outputs) {
                outputs.offset_sec = offsetSec;
            }
            let rawtext = fs.readFileSync(filePath, "utf-8");
            let rawLines = rawtext.split(/\n/).filter(function(l){return l.match(/[a-z0-9A-Z]+/)})
            let entries = [];        
            
            let previousEnd;
            for (let i = 0; i < rawLines.length; i++) {
                let rawLine = rawLines[i];
                let matcher = rawLine.match(/([0-9]+:[0-9]+:[0-9]+[;:.][0-9]+)\t* *(.*)/);
                if (matcher && matcher[2]) {            
                    if (debugMode) {this.Debug("matcher[2]", matcher[2])};
                    let sublines =  matcher[2].split(" 9420 942c "); 

                        let subline = matcher[2];
                        let rawPairs = subline.split(" ")                
                        let pairString = "";
                        let textString = "";
                        let end;
                        let start = matcher[1];
                        for (let item of rawPairs) {
                            let pair =  this.parseSCCPair(item);
                            textString = textString + pair;
                            pairString = pairString + "("+item+":"+pair + ") ";
                        }
                        if (debugMode) {this.Debug("pairs", pairString)};
                        let text = this.addNonCompliantAccents(textString).replace(/[\t ]+\n/g,"\n").replace(/\n+/g,"\n").replace(/^[\t ]*\n/, "");
                        if (debugMode) {this.Debug("Accented text", text)};
                        //console.log("text", text, {linecode: matcher[1], end, previousEnd});

                        entries.push({
                            start, 
                            end,
                            text
                        });                            
                } else {
                    this.reportProgress("error", rawLine);
                }
            }
            
            let lines =  ["WEBVTT\n"];
            let lineCues = this.Payload.inputs.line_cues ? (" "+this.Payload.inputs.line_cues) : "";
            for (let i=0; i< entries.length; i++) {
                let entry = entries[i];
                let entryStart = this.convertTimecode(entry.start, offsetSec, encodingFramerate, playoutFramerate);
                let entryEnd;
                if (i != (entries.length - 1)) {
                    entryEnd =  this.convertTimecode( entries[i + 1].start, offsetSec - 0.001, encodingFramerate, playoutFramerate); // -0.001 is to avoid collisions between lines                
                } else {
                    entryEnd = this.convertTimecode(entry.start, offsetSec + 1, encodingFramerate, playoutFramerate);
                }
                if (entry.text) {
                    lines.push("\n"+ entryStart+ " --> " + entryEnd + lineCues + "\n" + entry.text + "\n");
                }
            }
            return lines.join("");
        } catch(errSCC) {
            if ((offsetSec != 0) && !outputs.force_offset && errSCC.message && errSCC.message.match(/Timecode with offset is negative/)){
                this.reportProgress("SCC with negative offset timecodes are typically not offset, using 0 instead");
                return this.translateSCCNative(filePath, 0, encodingFramerate, playoutFramerate, outputs); 
            } else {
                throw errSCC;
            }
        }
    };
    
    translateSCCNativeAlmost(filePath, offsetSec, encodingFramerate, playoutFramerate, outputs)  {
        let debugMode = this.Payload.parameters.debug;
        try {
            if (outputs) {
                outputs.offset_sec = offsetSec;
            }
            let rawtext = fs.readFileSync(filePath, "utf-8");
            let rawLines = rawtext.split(/\n/).filter(function(l){return l.match(/[a-z0-9A-Z]+/)})
            let entries = [];        
            let buffer;
            let start;
            let end;
            let index = -1;
            for (let rawLine of rawLines) {
                let matcher = rawLine.match(/([0-9]+:[0-9]+:[0-9]+[;:.][0-9]+)\t* *(.*)/);
                //console.log("ligne", rawLine);
                if (matcher) {            
                    let timecode = matcher[1];
                    let  rawPairs = matcher[2].split(" ")     
                    let latest = null;           
                    for (let rawPair of rawPairs) {
                        let item = rawPair.toLowerCase();
                        if (item == latest) {
                            continue; //skip double up commands
                        }
                        if (item == "94ae") { //clear buffer
                            latest = item;
                            buffer = "";
                            continue;
                        }
                        if (item == "9420"){ //start new caption  
                            latest = item;
                            if (!entries.length || entries[entries.length -1].text) {   //ignore 9420 if duplicate                         
                                
                                entries.push({
                                    raw: matcher[2],
                                    start: null,
                                    end: null,
                                    text: ""
                                });

                            }
                            continue;
                        }
                        if (item == "942c") { //clear screen -- i.e. end previous caption
                            latest = item;
                            if (entries.length == 0) {
                                entries.push({
                                    raw: matcher[2],
                                    start: null,
                                    end: null,
                                    text: ""
                                });
                            }
                            if ((index >= 0) && !entries[index].end){
                                entries[index].end = timecode;
                                console.log("setting end code for ", entries[index]);
                            }
                            continue;
                        }
                        if (item == "942f") { //print caption to screen
                            latest = item;
                            index++;
                            entries[index].start = timecode;
                            console.log("setting start code for ", entries[index]);
                            continue;
                        }
                        
                        let pair =  this.parseSCCPair(item);
                        console.log("pair "+ item+ ": "+ pair);
                        latest = null;
                        if (entries.length > 0){                                     
                            entries[entries.length -1].text = entries[entries.length -1].text + pair;
                        } else {                                                        
                            this.reportProgress("No buffer to add pair " + pair, item);
                        }     
                    }                   
                }
            }            
            //console.log(JSON.stringify(entries, null, 2));
            
            
            let lines =  ["WEBVTT\n"];
            let lineCues = this.Payload.inputs.line_cues ? (" "+this.Payload.inputs.line_cues) : "";
            for (let i=0; i < entries.length; i++ ) {
                let entry = entries[i];
                if ((entry.text == null) || !entry.start ) {
                    this.reportProgress("Skipping misformed entry #"+i+"/"+(entries.length-1), entry);
                    continue;
                }
                let text = this.addNonCompliantAccents(entry.text).replace(/[\t ]+\n/g,"\n").replace(/\n+/g,"\n").replace(/^[\t ]*\n/, "");
                let entryStart = this.convertTimecode(entry.start, offsetSec, encodingFramerate, playoutFramerate);                
                let entryEnd;
                if (entry.end)
                    entryEnd = this.convertTimecode(entry.end, offsetSec - 0.001, encodingFramerate, playoutFramerate);
                else {
                    if (i < (entries.length - 1)) {
                        entryEnd = this.convertTimecode(entries[i+1].start, offsetSec - 0.001, encodingFramerate, playoutFramerate);
                    } else {
                        entryEnd = this.convertTimecode(entry.start, offsetSec+1, encodingFramerate, playoutFramerate); 
                    }
                } 

                lines.push("\n"+ entryStart+ " --> " + entryEnd + lineCues + "\n" + text + "\n");
            }
            return lines.join("");
            
            /*
            let previousEnd;
            for (let i = 0; i < rawLines.length; i++) {
                let rawLine = rawLines[i];
                let matcher = rawLine.match(/([0-9]+:[0-9]+:[0-9]+[;:.][0-9]+)\t* *(.*)/);
                if (matcher && matcher[2]) {            
                    if (debugMode) {this.Debug("matcher[2]", matcher[2])};
                    let sublines =  matcher[2].split(" 9420 942c "); 
                    for (let subIndex=0;  subIndex < sublines.length; subIndex++) {
                        let subline = sublines[subIndex];
                        let  rawPairs = subline.split(" ")                
                        let pairString = "";
                        let textString = "";
                        let end;
                        let start;
                        for (let item of rawPairs) {
                            let pair =  this.parseSCCPair(item);
                            textString = textString + pair;
                            pairString = pairString + "("+item+":"+pair + ") ";
                        }
                        if (debugMode) {this.Debug("pairs", pairString)};
                        let text = this.addNonCompliantAccents(textString).replace(/[\t ]+\n/g,"\n").replace(/\n+/g,"\n").replace(/^[\t ]*\n/, "");
                        if (debugMode) {this.Debug("Accented text", text)};
                        //console.log("text", text, {linecode: matcher[1], end, previousEnd});
                        if (!text) {
                            continue;
                        }
                        if (!end) {
                            if (!previousEnd) {
                                start = matcher[1];
                            } else {
                                for (let tIndex=0; tIndex < timecodes.length; tIndex++) {
                                    if (timecodes[tIndex] > previousEnd) {
                                        start = timecodes[tIndex];
                                        break;
                                    }
                                }
                                
                            }
                        } else {
                            start = end;
                        }
                        for (let tIndex=0; tIndex < timecodes.length; tIndex++) {
                            if (timecodes[tIndex] > start) {
                                end = timecodes[tIndex];
                                break;
                            }
                        }
                        
                        if (start && end) {
                            let entry = {
                                start, 
                                end,
                                text
                            };
                            entries.push(entry);
                            previousEnd = entry.end;
                        }
                    }
                } else {
                    this.reportProgress("error", rawLine);
                }
            }
            
            let lines =  ["WEBVTT\n"];
            let lineCues = this.Payload.inputs.line_cues ? (" "+this.Payload.inputs.line_cues) : "";
            for (let entry of entries.filter(function(entry) {return entry.text;})) {
                let entryStart = this.convertTimecode(entry.start, offsetSec, encodingFramerate, playoutFramerate);
                let entryEnd = entry.end ? this.convertTimecode(entry.end, offsetSec - 0.001, encodingFramerate, playoutFramerate) : this.convertTimecode(entry.start, 1, encodingFramerate, playoutFramerate); // -0.001 is to avoid collisions between lines
                lines.push("\n"+ entryStart+ " --> " + entryEnd + lineCues + "\n" + entry.text + "\n");
            }
            return lines.join("");
            */
        } catch(errSCC) {
            if ((offsetSec != 0) && !outputs.force_offset && errSCC.message && errSCC.message.match(/Timecode with offset is negative/)){
                this.reportProgress("SCC with negative offset timecodes are typically not offset, using 0 instead");
                return this.translateSCCNative(filePath, 0, encodingFramerate, playoutFramerate, outputs); 
            } else {
                throw errSCC;
            }
        }
    };

    translateSCCNative(filePath, offsetSec, encodingFramerate, playoutFramerate, outputs)  {
        let debugMode = this.Payload.parameters.debug;
        try {
            if (outputs) {
                outputs.offset_sec = offsetSec;
            }
            let rawtext = fs.readFileSync(filePath, "utf-8");
            if (!rawtext.match(/9420/)){
                this.reportProgress("File is not compliant, it does not manage captions buffer");
                return this.translateSCCNativeBad(filePath, offsetSec, encodingFramerate, playoutFramerate, outputs);
            }
            let rawLines = rawtext.split(/\n/).filter(function(l){return l.match(/[a-z0-9A-Z]+/)})
            let entries = [];        
            let buffer;

            for (let rawLine of rawLines) {
                let matcher = rawLine.match(/([0-9]+:[0-9]+:[0-9]+[;:.][0-9]+)\t* *(.*)/);
                if (this.Payload.parameters.debug) {
                    this.reportProgress("SCC ligne", rawLine);
                }
                if (matcher) {            
                    let timecode = matcher[1];
                    let  rawPairs = matcher[2].split(" ")     
                    let latest = null;           
                    for (let rawPair of rawPairs) {
                        let item = rawPair.toLowerCase();
                        if (item == latest) {
                            continue; //skip double up commands
                        }
                        if (item == "94ae") { //clear buffer
                            latest = item;
                            buffer = ""; //not doing anything. We could instead clear the current entry, but it does not seem necessary
                            continue;
                        }
                        if (item == "9420"){ //start new caption  
                            latest = item;
                            if (!entries.length) {   //ignore 9420 if duplicate                         
                                entries.push({
                                    raw: matcher[2],
                                    start: null,
                                    end: null,
                                    text: ""
                                });
                            } else {
                                if  (entries[entries.length -1].text) {
                                    if (!entries[entries.length -1].start) {
                                        entries[entries.length -1].text = entries[entries.length -1].text +"\n";
                                    } else {
                                        entries.push({
                                            raw: matcher[2],
                                            start: null,
                                            end: null,
                                            text: ""
                                        });
                                    }
                                }
                            }
                            continue;
                        }
                        if (item == "942c") { //clear screen -- i.e. end previous caption
                            latest = item;
                            if (entries.length == 0) {
                                entries.push({
                                    raw: matcher[2],
                                    start: null,
                                    end: null,
                                    text: ""
                                });
                            }
                            if ((entries.length >= 0) && !entries[entries.length - 1].end && entries[entries.length - 1].start){
                                entries[entries.length - 1].end = timecode;
                                if (this.Payload.parameters.debug) {
                                    this.reportProgress("setting end code for ", entries[entries.length - 1]);
                                }
                            }
                            continue;
                        }
                        if (item == "942f") { //print caption to screen
                            latest = item;
                            entries[entries.length - 1].start = timecode;
                            if (this.Payload.parameters.debug) {
                                this.reportProgress("setting start code for ", entries[entries.length - 1]);
                            }
                            continue;
                        }
                        if ((item == "9270") || (item == "92f8") || (item == "92f4")) { //off spec - seems to be a carriage return
                            latest = item;
                            if (entries[entries.length - 1]) {
                                entries[entries.length - 1].text = entries[entries.length - 1].text +"\n";
                            }
                            if (this.Payload.parameters.debug) {
                                this.reportProgress("off-spec linefeed ", entries[entries.length - 1]);
                            }
                            continue;
                        }
                        let pair =  this.parseSCCPair(item);
                        if (this.Payload.parameters.debug) {
                            this.reportProgress("pair "+ item+ ": "+ pair);
                        }
                        latest = null;
                        if (entries.length > 0){                                     
                            entries[entries.length -1].text = entries[entries.length -1].text + pair;
                        } else {                                                        
                            this.reportProgress("No buffer to add pair " + pair, item);
                        }     
                    }                   
                }
            }            
            //console.log(JSON.stringify(entries, null, 2));
            
            
            let lines =  ["WEBVTT\n"];
            let lineCues = this.Payload.inputs.line_cues ? (" "+this.Payload.inputs.line_cues) : "";
            for (let i=0; i < entries.length; i++ ) {
                let entry = entries[i];
                if ((entry.text == null) || !entry.start ) {
                    this.reportProgress("Skipping misformed entry #"+i+"/"+(entries.length-1), entry);
                    continue;
                }
                let text = this.addNonCompliantAccents(entry.text).replace(/[\t ]+\n/g,"\n").replace(/\n+/g,"\n").replace(/^[\t ]*\n/, "");
       
                let entryStart = this.convertTimecode(entry.start, offsetSec, encodingFramerate, playoutFramerate);                
                let entryEnd;
                if (entry.end &&  (entry.end > entry.start)) {
                    entryEnd = this.convertTimecode(entry.end, offsetSec - 0.001, encodingFramerate, playoutFramerate);
                } else {
                    this.reportProgress("No end provided for entry #"+i, {start: entry.start, end:entry.end});
                    if (i < (entries.length - 1)) {
                        this.reportProgress("Using next entry start as bookend");
                        entryEnd = this.convertTimecode(entries[i+1].start, offsetSec - 0.001, encodingFramerate, playoutFramerate);
                    } else {
                        this.reportProgress("Defaulting to 1 second duration");
                        entryEnd = this.convertTimecode(entry.start, offsetSec+1, encodingFramerate, playoutFramerate); 
                    }
                } 

                lines.push("\n"+ entryStart+ " --> " + entryEnd + lineCues + "\n" + text + "\n");
            }
            return lines.join("");
            
     } catch(errSCC) {
            if ((offsetSec != 0) && !outputs.force_offset && errSCC.message && errSCC.message.match(/Timecode with offset is negative/)){
                this.reportProgress("SCC with negative offset timecodes are typically not offset, using 0 instead");
                return this.translateSCCNative(filePath, 0, encodingFramerate, playoutFramerate, outputs); 
            } else {
                throw errSCC;
            }
        }
    };
    
    parseSCCPair(item) {
        let charInt = parseInt(item, 16) % (256 * 128);
        if  ((charInt >= 8192)  && (charInt <= 32767)) { //bit 13 or 14 are set
            return  this.mapSCCBasicNorthAmericanCharacter(item.slice(0, 2)) + this.mapSCCBasicNorthAmericanCharacter(item.slice(2, 4));
        }
        if  (((charInt >= 4352)  && (charInt <= 4607)) || ((charInt >= 6400)  && (charInt <= 6655))) { //first byte of 0x11 or 0x19 
            return  this.mapSCCSpecialNorthAmericanCharacter(item.slice(2, 4));
        }
        if  (((charInt >= 4608)  && (charInt <= 4863)) || ((charInt >= 6656)  && (charInt <= 6911))) { //has a first byte of 0x12 or 0x1A 
            return  this.mapSCCExtendedWesternEuropeanCharacterSPFR(item.slice(2, 4));
        }
        if  (((charInt >= 4864)  && (charInt <= 5119)) || ((charInt >= 6912)  && (charInt <= 7167))) {  //has a first byte of 0x13 or 0x1B
            return  this.mapSCCExtendedWesternEuropeanCharacterPTGEDA(item.slice(2, 4));
        }
        if  (((charInt >= 5120)  && (charInt <= 5631)) || ((charInt >= 7168)  && (charInt <= 7679))) { //0x14 (CC1) or 0x1c (CC2) or 0x15 (CC3) or 0x1D (CC4)
            return  this.mapSCCControl_1(item.slice(2, 4));
        }
        if  (((charInt >= 5888)  && (charInt <= 6143)) || ((charInt >= 7936)  && (charInt <= 8191))) {  //0x17 (CC1/3) or 0x1F (CC2/4)
            return  this.mapSCCControl_2(item.slice(2, 4));
        }
        this.reportProgress("Non-compliant SCCPair", item);
        return "";
    };
    
    mapSCCControl_1(item) { //94xx,  //14xx
        let charInt = parseInt(item, 16) % 128;
        let mapSCC = {
            32: "", //resume caption loading  -- 9420
            33: "", //backspace               -- 9421
            36: "\n", //delete to end of row  -- 9424
            37: "\n\n", //roll up 2           -- 9425
            38: "\n\n\n", //roll up 3         -- 9426
            39: "\n\n\n\n", //roll up 4       -- 9427
            40: "", //flash  caption          -- 9428
            41: "\n", //resume direct captioning -- 9429
            42: "\n", //text restart          -- 942a
            44: "", //	erase display memory  -- 942c
            45: "\n", //carriage return       -- 942d
            46: "", //erase non displayed memory -- 942e
            47:"", //end of caption           -- 942f
            49: " ", // tab offset 1 (add spacing) -- 9431
            50: "  ", // tab offset 2 (add spacing) -- 9432
            51: "   ", //tab offset 3 (add spacing) -- 9433
            64: "\n",//not in spec  - from bb2  -- 9440
            78: "\n", //not in spec          -- 944e
            80: "\n", //not in spec          -- 9450
            82: "\n", //not in spec - from sls -- 9452
            84: "\n", //not in spec - from sls -- 9454
            86: "\n", //not in spec - from bb2 -- 9456
            88:"", //not in spec/            -- 9458
            90:"\n", //not in spec           -- 946a
            92:"", //not in spec             -- 946c
            94:"", //not in spec             -- 946e
            96:"\n", //not in spec           -- 9470
            110: "\n", //not in spec         -- 947e
            112: "\n", //not in spec         -- 9480
            114: "\n", //not in spec         -- 9482
            116: "\n", //not in spec         -- 9484
            118: "\n", //not in spec         -- 9486
            120: "\n", //not in spec         -- 9488
            122: "\n", //not in spec         -- 948a
            124: "\n", //not in spec         -- 948c
            126: "\n" //not in spec          -- 948e
        };
        let  specialChar = mapSCC[charInt] 
        if  (specialChar != null) {
            return specialChar
        }
        this.reportProgress("mapSCCControl_1 anomaly", charInt);
        return ""; 
    };
    
    mapSCCControl_2(item) {
        let charInt = parseInt(item, 16) % 128;
        let mapSCC = {
            33: "	", //tab offset 1
            34: "		", //tab offset 2
            35: "			", //tab offset 3
        };
        let  specialChar = mapSCC[charInt] 
        if  (specialChar != null) {
            return specialChar
        }
        this.reportProgress("mapSCCControl_2 anomaly", charInt);
        return ""; 
    };
    
    mapSCCBasicNorthAmericanCharacter(item) {
        let mapSCC = {
            0: "", 
            27: "'",
            42: "á",
            92: "é",
            94: "í",
            95: "ó",
            96: "ú",
            123: "ç",
            124: "÷",
            125: "Ñ",
            126: "ñ",
            127: "█" //Solid block
        };
        let charInt = parseInt(item, 16) % 128;
        let specialChar = mapSCC[charInt];
        if (specialChar != null) {
            return specialChar;
        }
        if (charInt <= 122){
            return String.fromCharCode(charInt);
        }
        this.reportProgress("mapSCCBasicNorthAmericanCharacter anomaly", charInt);
        return ""; //not sure what this is supposed to be
    };
    
    mapSCCSpecialNorthAmericanCharacter(item) {
        let charInt = parseInt(item, 16) % 128;
        let mapSCC = {
            32: " ",
            46: " ",
            48: "®",
            49:	"°",    
            50: "½",
            51:"¿",
            52:"™",
            53:"¢",
            54:"£",
            55:"♪",
            56:"à",
            57: " ", //non breaking space
            58:"è",
            59:"â",
            60:"ê",
            61:"î",
            62:"ô",
            63:"û",
            78: "",//not part of the spec
            80: "",//not part of the spec
            82: "",//not part of the spec
            84: "\n",//not part of the spec
            86: "",//not part of the spec
            96: " ", //not part of the spec  - Step brothers
            112: "\n", //not part of the spec
            114: "\n", //not part of the spec
            116: "\n", //not part of the spec
            118: " ", //not part of the spec
            120: " " //not part of the spec
        };
        let specCar = mapSCC[charInt] ;
        if (specCar != null) {
            return specCar;
        }
        this.reportProgress("mapSCCSpecialNorthAmericanCharacter anomaly", charInt);
        return "";
    };
    
    addNonCompliantAccents(text) {
        text = text.replace(/'+/g, "'");
        text = text.replace(/EÉÉ/g, "É");
        text = text.replace(/ÉÉ/g, "É");
        text = text.replace(/EÉ/g, "É");
        text = text.replace(/EÊÊ/g, "Ê");
        text = text.replace(/ÊÊ/g, "Ê");
        text = text.replace(/EÈÈ/g, "È");
        text = text.replace(/ÈÈ/g, "È");
        text = text.replace(/EËË/g, "Ë");
        text = text.replace(/ËË/g, "Ë");
        text = text.replace(/AÀÀ/g, "À");
        text = text.replace(/ÀÀ/g, "À");
        text = text.replace(/AÂÂ/g,"Â");
        text = text.replace(/ÂÂ/g,"Â");
        text = text.replace(/AÄÄ/g,"Ä")
        text = text.replace(/ÄÄ/g,"Ä")
        text = text.replace(/AÅÅ/g,"Å");
        text = text.replace(/ÅÅ/g,"Å");
        text = text.replace(/AÃÃ/g, "Ã");
        text = text.replace(/ÃÃ/g, "Ã");
        text = text.replace(/IÏÏ/g,"Ï");
        text = text.replace(/ÏÏ/g,"Ï");
        text = text.replace(/IÎÎ/g,"Î");
        text = text.replace(/ÎÎ/g,"Î");
        text = text.replace(/IÍÍ/g,"Í");
        text = text.replace(/ÍÍ/g,"Í");
        text = text.replace(/IÌÌ/g,"Ì");
        text = text.replace(/ÌÌ/g,"Ì");
        text = text.replace(/OÔÔ/g,"Ô");
        text = text.replace(/ÔÔ/g,"Ô");
        text = text.replace(/OÓÓ/g, "Ó");
        text = text.replace(/ÓÓ/g, "Ó");
        text = text.replace(/OÒÒ/g, "Ò");
        text = text.replace(/ÒÒ/g, "Ò");
        text = text.replace(/OÕÕ/g, "Õ");
        text = text.replace(/ÕÕ/g, "Õ");
        text = text.replace(/OÖÖ/g, "Ö");
        text = text.replace(/ÖÖ/g, "Ö");
        text = text.replace(/UÙÙ/g, "Ù");
        text = text.replace(/ÙÙ/g, "Ù");
        text = text.replace(/UÛÛ/g, "Û");
        text = text.replace(/ÛÛ/g, "Û");
        text = text.replace(/UÜÜ/g,"Ü");
        text = text.replace(/ÜÜ/g,"Ü");
        text = text.replace(/UÚÚ/g, "Ú");
        text = text.replace(/ÚÚ/g, "Ú");
        text = text.replace(/CÇÇ/g,"Ç");
        text = text.replace(/ÇÇ/g,"Ç");
        text = text.replace(/NÑÑ/g,"Ñ");
        text = text.replace(/ÑÑ/g,"Ñ");
        text = text.replace(/uùù/g,"ù");
        text = text.replace(/ùù/g,"ù");
        text = text.replace(/uûû/g,"û");
        text = text.replace(/ûû/g,"û");
        text = text.replace(/uüü/g, "ü");
        text = text.replace(/üü/g, "ü");
        text = text.replace(/uúú/g,"ú");
        text = text.replace(/úú/g,"ú");
        text = text.replace(/eéé/g,"é") 
        text = text.replace(/éé/g,"é") 
        text = text.replace(/eëë/g, "ë");
        text = text.replace(/ëë/g, "ë");
        text = text.replace(/eèè/g, "è");
        text = text.replace(/èè/g, "è");
        text = text.replace(/eêê/g, "ê");
        text = text.replace(/êê/g, "ê");
        text = text.replace(/aàà/g, "à");
        text = text.replace(/àà/g, "à");
        text = text.replace(/aââ/g, "â");
        text = text.replace(/ââ/g, "â");
        text = text.replace(/aáá/g,"á");
        text = text.replace(/áá/g,"á");
        text = text.replace(/aåå/g,"å");
        text = text.replace(/åå/g,"å");
        text = text.replace(/aãã/g, "ã");
        text = text.replace(/ãã/g, "ã");
        text = text.replace(/aää/g, "ä");
        text = text.replace(/ää/g, "ä");
        text = text.replace(/oôô/g, "ô");
        text = text.replace(/ôô/g, "ô");
        text = text.replace(/oóó/g, "ó");
        text = text.replace(/óó/g, "ó");
        text = text.replace(/oòò/g,"ò");
        text = text.replace(/òò/g,"ò");
        text = text.replace(/iïï/g,"ï");
        text = text.replace(/ïï/g,"ï");
        text = text.replace(/iîî/g,"î");
        text = text.replace(/îî/g,"î");
        text = text.replace(/iíí/g,"í");
        text = text.replace(/íí/g,"í");
        text = text.replace(/iìì/g,"ì");
        text = text.replace(/ìì/g,"ì");
        text = text.replace(/eëë/g, "ë");
        text = text.replace(/ëë/g, "ë");
        text = text.replace(/cçç/g, "ç");
        text = text.replace(/çç/g, "ç");
        text = text.replace(/nññ/,"ñ");
        text = text.replace(/ññ/,"ñ");
        text = text.replace(/\t\t\t\t/g,"\n");
        text = text.replace(/\t\t/g,"\n");
        text = text.replace(/\n+/g,"\n");
        text = text.replace(/^\n/,"");
        text = text.replace(/  /g," ");
        text = text.replace(/¿¿/g,"¿");       
        text = text.replace(/!¡¡/g,"¡");
        text = text.replace(/¡¡/g,"¡");
        text = text.replace(/([a-zA-Z])"([a-zA-Z])/g, "$1'$2"); // replace mis-used double quotes
        return text;
    };
    
    mapSCCExtendedWesternEuropeanCharacterSPFR(item) {
        let charInt = parseInt(item, 16) % 128;
        let mapSCC = {
            32:"Á",
            33: "É", 
            34: "Ó",
            35:	"Ú",
            36: "Ü",
            37:	"ü",
            38:	"´",
            39:	"¡",
            40:	"*",
            41:	"'",
            42:	"─",
            43:	"©",
            44:	"℠",
            45:	"·",
            46:	"“",
            47:	"”",
            48: "À",
            49: "Â",
            50: "Ç",
            51: "È",
            52: "Ê",
            53: "Ë",
            54: "ë",
            55: "Î",
            56: "Ï",
            57:	"ï",
            58: "Ô",
            59: "Ù",
            60: "ù",
            61: "Û",
            62: "«",
            63: "»",
            80: "", //not in spec
            82: "",
            84: ""
        };
        let specCar = mapSCC[charInt] ;
        if  (specCar != null) {
            return specCar;
        }
        this.reportProgress("mapSCCExtendedWesternEuropeanCharacterSPFR anomaly", charInt);
        return "";
    };
    
    
    mapSCCExtendedWesternEuropeanCharacterPTGEDA(item) {
        let charInt = parseInt(item, 16) % 128;
        let mapSCC = { //13xx, 93xx
            32:"Ã",
            33:"ã",
            34:"Í",
            35:"Ì",
            36:"ì",
            37:"Ò",
            38:"ò",
            39:"Õ",
            40:"õ",
            41:"{",
            42:"}",
            43:"\\",
            44:"^",
            45: "_",
            46:"|",
            47:"~",
            48:"Ä",
            49:"ä",
            50:"Ö",
            51:"ö",
            52:"ß",
            53:"¥",
            54:"¤",
            55:"│",
            56:"Å",
            57: "å",
            58:"Ø",
            59:"ø",
            60:"┌",
            61: "┐",
            62:"└",
            63:"┘", 
            96: "\n", //not in spec -- bb2
            110: "\n", //not in spec
            112: "", //not in spec
            114: "", //not in spec
            116: "", //not in spec
            118: "\n", //not in spec  -- bb2
            120:"", //not in spec
            122:"", //not in spec
            124:"", //not in spec
            126:"" //not in spec
        };
        let  specCar = mapSCC[charInt];
        if (specCar != null) {
            return specCar;
        }
        this.reportProgress("mapSCCExtendedWesternEuropeanCharacterPTGEDA anomaly", charInt);
        return  "";
    };
    
    
    
    
    
    convertSTLTTI(languageData,  ttiBlock) {
        let languageSTLconversions = {     
            "00" : ElvOManageCaptions.convertLatinCharLine,   //english - not in spec 
            "21": ElvOManageCaptions.convertLatinCharLine, //portuguese
            "01": ElvOManageCaptions.convertLatinCharLine, //Albanian
            "02": ElvOManageCaptions.convertLatinCharLine, //Breton
            "03": ElvOManageCaptions.convertLatinCharLine, //Catalan
            "04": ElvOManageCaptions.convertLatinCharLine, //Croatian
            "05": ElvOManageCaptions.convertLatinCharLine, //Welsh
            "06": ElvOManageCaptions.convertLatinCharLine, //Czech
            "07": ElvOManageCaptions.convertLatinCharLine, //"Danish",
            "1D": ElvOManageCaptions.convertLatinCharLine, //"Dutch",
            "08": ElvOManageCaptions.convertLatinCharLine, //"German",
            "1E": ElvOManageCaptions.convertLatinCharLine, //"Norwegian",
            "09": ElvOManageCaptions.convertLatinCharLine, //"English",
            "1F": ElvOManageCaptions.convertLatinCharLine, //"Occitan",
            "0A": ElvOManageCaptions.convertLatinCharLine, //"Spanish",
            "20": ElvOManageCaptions.convertLatinCharLine, //"Polish",
            "0B": ElvOManageCaptions.convertLatinCharLine, //"Esperanto",  
            "0C": ElvOManageCaptions.convertLatinCharLine, //"Estonian",
            "22": ElvOManageCaptions.convertLatinCharLine, //"Romanian",
            "38": null, //"national",
            "0D": ElvOManageCaptions.convertLatinCharLine, //"Basque",
            "23": ElvOManageCaptions.convertLatinCharLine, //"Romansh",
            "0E": ElvOManageCaptions.convertLatinCharLine, //"Faroese",
            "24": null, //"Serbian",
            "0F": ElvOManageCaptions.convertLatinCharLine, //"French",
            "25": ElvOManageCaptions.convertLatinCharLine, //"Slovak",
            "10": ElvOManageCaptions.convertLatinCharLine, //"Frisian",
            "26": ElvOManageCaptions.convertLatinCharLine, //"Slovenian",
            "11": ElvOManageCaptions.convertLatinCharLine, //"Irish",
            "27": ElvOManageCaptions.convertLatinCharLine, //"Finnish",
            "12": ElvOManageCaptions.convertLatinCharLine, //"Gaelic",
            "28": ElvOManageCaptions.convertLatinCharLine, //"Swedish",
            "13": ElvOManageCaptions.convertLatinCharLine, //"Galician",
            "29": ElvOManageCaptions.convertLatinCharLine, //"Turkish",
            "14": ElvOManageCaptions.convertLatinCharLine,//"Icelandic"
            "2A": ElvOManageCaptions.convertLatinCharLine, //"Flemish",
            "15": ElvOManageCaptions.convertLatinCharLine, //"Italian",
            "2B": ElvOManageCaptions.convertLatinCharLine, //"Wallon"
            "65": ElvOManageCaptions.convertUnicode, //"Korean"
            "37": null //"Reserved"
        };        
        let conversion = languageSTLconversions[languageData.languageCode];
        if (conversion) {
            return conversion(ttiBlock);
        } else {
            throw new Error("Unsupported language code "+ languageData.languageCode)
        }
    };
    
    static  convertUnicode(ttiBlock) {
        let line = "";
        let i=0;
        
        while (i < ttiBlock.length - 1) {
            let byte1 = ttiBlock[i].charCodeAt(0);
            let specialChar = ElvOManageCaptions.lookupSRLSpecialCharacter(byte1);
            if (specialChar == null) {
                if  ((byte1 >=32) && (byte1 < 127)) {
                    specialChar = String.fromCodePoint(byte1);
                }
            }
            if  (specialChar != null) {
                line = line + specialChar;
                i = i + 1;
            } else {
                let byte2 = ttiBlock[i+1].charCodeAt(0);
                let uCode =  String.fromCodePoint(byte1 * 256 + byte2);// "\\u" + byte1.toString(16) + byte2.toString(16);           
                line = line + uCode;
                i = i + 2;
            }           
        }
        return line;
    }
    
    static convertLatinCharLine(ttiBlock) {
        let line = "";
        let ASCIIExceptions = {
            36: "¤",
            164: "$",
            166: "",
            168: "",
            169: "`",
            170: " ̏",
            171: "«",
            180: "×",
            //181: "µ",
            185: "´",
            186: "˝", //Found in Brazilian Portuguese in seemingly random places, would benefit from being replaced by null string.
            187: "»",
            193:"`",
            194: "´",       
            195: "^",
            196: "~",
            197: "¯",
            198: "ˇ",
            199: "·",
            200: "¨",
            201:"×",
            202: "°",
            203: "¸",
            204: "_",
            210: "®",
            211: "©",
            213: "♪",
            225: "Æ",
            226: "Ð",
            233: "Ø",                   
            234:"Œ", 
            235: "ọ",
            236: "Þ",
            241: "æ",
            242: "",
            243: "ð",
            249: "ø",
            250: "œ",
            251: "ß",
            252: "þ",
            255: "-"
        };
        let latinCharAccents = [
            {from: new RegExp("´A","g"), to: "Á"},
            {from: new RegExp("`A","g"), to: "À"},
            {from: new RegExp("¨A","g"), to: "Ä"},
            {from: new RegExp("\\^A","g"), to: "Â"},
            {from: new RegExp("~A","g"), to: "Ã"},
            {from: new RegExp("°A","g"), to: "Å"},
            {from: new RegExp("ˇA","g"), to: "Ă"},
            {from: new RegExp("´E","g"), to: "É"},
            {from: new RegExp("`E","g"), to: "È"},
            {from: new RegExp("¨E","g"), to: "Ë"},
            {from: new RegExp("\\^E","g"), to: "Ê"},
            {from: new RegExp("¯E","g"), to: "Ē"},
            {from: new RegExp("´I","g"), to: "Í"},
            {from: new RegExp("`I","g"), to: "Ì"},
            {from: new RegExp("¨I","g"), to: "Ï"},
            {from: new RegExp("\\^I","g"), to: "Î"},
            {from: new RegExp("¯I","g"), to: "Ī"},
            {from: new RegExp("¸I","g"), to: "Į"},
            {from: new RegExp("´O","g"), to: "Ó"},
            {from: new RegExp("`O","g"), to: "Ò"},
            {from: new RegExp("¨O","g"), to: "Ö"},
            {from: new RegExp("\\^O","g"), to: "Ô"},
            {from: new RegExp("~O","g"), to: "Õ"},
            {from: new RegExp("´U","g"), to: "Ù"},
            {from: new RegExp("`U","g"), to: "Ù"},
            {from: new RegExp("¨U","g"), to: "Ü"},
            {from: new RegExp("\\^U","g"), to: "Û"},
            {from: new RegExp("¸C","g"), to: "Ç"},
            {from: new RegExp("´C","g"), to: "Ć"},
            {from: new RegExp("ˇC","g"), to: "Č"},
            {from: new RegExp("ˇD","g"), to: "Ď"},
            {from: new RegExp("ˇG","g"), to: "Ğ"},
            {from: new RegExp("´N","g"), to: "Ń"},
            {from: new RegExp("~N","g"), to: "Ñ"},
            {from: new RegExp("¸S","g"), to: "Ş"},
            {from: new RegExp("ˇS","g"), to: "Š"},
            {from: new RegExp("¸T","g"), to: "Ț"},
            {from: new RegExp("ˇT","g"), to: "Ť"},
            {from: new RegExp("´Y","g"), to: "Ý"},
            {from: new RegExp("`Y","g"), to: "Ỳ"},
            {from: new RegExp("\\^Y","g"), to: "Ŷ"},
            {from: new RegExp("¨Y","g"), to: "Ÿ"},
            {from: new RegExp("ˇZ","g"), to: "Ž"},
            {from: new RegExp("´a","g"), to: "á"},
            {from: new RegExp("`a","g"), to: "à"},
            {from: new RegExp("¨a","g"), to: "ä"},
            {from: new RegExp("\\^a","g"), to: "â"},
            {from: new RegExp("~a","g"), to: "ã"},
            {from: new RegExp("°a","g"), to: "å"},
            {from: new RegExp("ˇa","g"), to: "ă"},
            {from: new RegExp("´e","g"), to: "é"},           
            {from: new RegExp("`e","g"), to: "è"},
            {from: new RegExp("¨e","g"), to: "ë"},
            {from: new RegExp("\\^e","g"), to: "ê"},
            {from: new RegExp("´i","g"), to: "í"},
            {from: new RegExp("`i","g"), to: "ì"},
            {from: new RegExp("¨i","g"), to: "ï"},
            {from: new RegExp("\\^i","g"), to: "î"},
            {from: new RegExp("´o","g"), to: "ó"},
            {from: new RegExp("`o","g"), to: "ò"},
            {from: new RegExp("¨o","g"), to: "ö"},
            {from: new RegExp("\\^o","g"), to: "ô"},
            {from: new RegExp("~o","g"), to: "õ"},
            {from: new RegExp("´u","g"), to: "ú"},
            {from: new RegExp("`u","g"), to: "ù"},
            {from: new RegExp("¨u","g"), to: "ü"},
            {from: new RegExp("\\^u","g"), to: "û"},
            {from: new RegExp("¸c","g"), to: "ç"},
            {from: new RegExp("´c","g"), to: "ć"},
            {from: new RegExp("ˇc","g"), to: "č"},
            {from: new RegExp("ˇd","g"), to: "ď"},
            {from: new RegExp("´d","g"), to: "ď"},
            {from: new RegExp("ˇg","g"), to: "ğ"},
            {from: new RegExp("´n","g"), to: "ń"},
            {from: new RegExp("~n","g"), to: "ñ"},
            {from: new RegExp("¸s","g"), to: "ş"},
            {from: new RegExp("ˇs","g"), to: "š"},
            {from: new RegExp("´t","g"), to: "ť"},
            {from: new RegExp("¸t","g"), to: "ț"},
            {from: new RegExp("ˇt","g"), to: "ť"},
            {from: new RegExp("`y","g"), to: "ỳ"},
            {from: new RegExp("\\^y","g"), to: "ŷ"},
            {from: new RegExp("¨y","g"), to: "ÿ"},
            {from: new RegExp("ˇz","g"), to: "ž"}
        ]
        let i=0;
        while (i < ttiBlock.length) {
            let byte = ttiBlock[i].charCodeAt(0);
            let exceptionChar = ASCIIExceptions[byte];
            if (exceptionChar == null) {
                let specialChar = ElvOManageCaptions.lookupSRLSpecialCharacter(byte);
                if (specialChar == null) {
                    line = line + String.fromCharCode(byte);
                } else {
                    line = line + specialChar;
                }
            } else {
                line = line + exceptionChar;
            }
            i++;
        }
        for (let sub of latinCharAccents) {
            line=line.replace(sub.from, sub.to);
        }
        return line;
    };
    
    /* spec at https://tech.ebu.ch/docs/tech/tech3264.pdf */  /*http://www.zeitanker.com/sites/zeitanker.com/content/e31/e71/e3015/tec_doc_t3264_tcm6-10528.pdf*/
    translateSTL(filePath, offsetSec, encodingFramerate, playoutFramerate, outputs)  {
        let rawtext = fs.readFileSync(filePath, "binary");
        let gsiBlock = rawtext.slice(0, 1024);
        let rawBlock = Array.from(rawtext);
        let CCT = rawtext.charCodeAt(12) + "-"+rawtext.charCodeAt(13);
        let CPN = gsiBlock.slice(0,3)
        this.reportProgress("CPN: " + CPN) ;
        this.reportProgress("CCT: " +  CCT);
        
        let languageCode = gsiBlock.charAt(14) + gsiBlock.charAt(15);
        let countryCode = gsiBlock.charAt(274) + gsiBlock.charAt(275) + gsiBlock.charAt(276)
        this.reportProgress("languageCode: " + languageCode) ;
        this.reportProgress("countryCode: " +  countryCode);
        let i  = 1024;
        let entries = [];
        while (i < rawBlock.length) {
            let tti = rawtext.slice(i, i + 127);
            let ttiBlock = rawBlock.slice(i, i + 127);
            let textBlock = ttiBlock.slice(16, 127);    
            let lineSTL  = this.convertSTLTTI({languageCode, CCT, CPN},  textBlock);            
            let line = {
                start: ("00"+ tti.charCodeAt(5)).slice(-2) + ":" + ("00"+ tti.charCodeAt(6)).slice(-2) + ":" +  ("00"+ tti.charCodeAt(7)).slice(-2) + ":"+ ("00"+ tti.charCodeAt(8)).slice(-2),
                end: ("00"+ tti.charCodeAt(9)).slice(-2) + ":" + ("00"+ tti.charCodeAt(10)).slice(-2) + ":" +  ("00"+ tti.charCodeAt(11)).slice(-2) + ":"+ ("00"+ tti.charCodeAt(12)).slice(-2),
                text: lineSTL
            };
            if  (this.Payload.parameters.debug) {
                line.raw_text = textBlock.map(function(item){return  [item.charCodeAt(0), item.charCodeAt(0).toString(16), item]});
                this.reportProgress(line, line);
            }
            entries.push(line);
            i += 128;
        }
        
        let lines =  ["WEBVTT\n"];
        let lineCues = this.Payload.inputs.line_cues ? (" "+this.Payload.inputs.line_cues) : "";
        /*
        this.Info("Ignoring offset for stl format",  offsetSec);
        if (!outputs || !outputs.force_offset) {
            this.reportProgress("Using 0 has stl seems to not have an offset, use force_offset to overrid")
            offsetSec = 0; //stl seems to not have an offset
        } else {
            this.reportProgress("force_offset was set, so using provided offset instead of 0")
        }
        */
        if (outputs) {
            outputs.offset_sec = offsetSec;
        }
        //this.reportProgress("Ignoring provided framerate using 24 instead for both", {encodingFramerate,playoutFramerate});
        //encodingFramerate = 24;
        //playoutFramerate = 24;
        for (let entry of entries) {
            let entryStart = this.convertTimecode(entry.start, offsetSec, encodingFramerate, playoutFramerate);
            let entryEnd = this.convertTimecode(entry.end, offsetSec, encodingFramerate, playoutFramerate);
            lines.push("\n"+ entryStart+ " --> " + entryEnd + lineCues + "\n" + entry.text + "\n");
        }
        return lines.join("");
    };
    
    
    static lookupSRLSpecialCharacter(code) {   
        if (code  < 32) {
            return "";
        } 
        if (code == 128) {//80h Open Italics ON -
            return "<i>";
        }
        if (code == 129) {//81h Open Italics OFF
            return "</i>";
        }
        if (code == 130) {//82h Open Underline ON -
            return "<i>";
        }
        if (code == 131) {//83h Open Underline OFF
            return "</i>";
        }
        if (code == 132) {//84h Open Boxing ON -
            return ""; //VTT support to be confirmed
        }
        if (code == 133) {//85h Open Boxing OFF
            return ""; //VTT support to be confirmed
        }
        if ((code >= 134) &&  (code <= 137)) {//86h..89h - Reserved for future use
            return ""; 
        }
        if (code == 138) {//8Ah Both CR/LF
            return "\n"; 
        }
        if ((code >= 139) &&  (code <= 142)) {//8Bh..8Eh - Reserved for future use
            return ""; 
        }
        if (code == 143) {//8Fh Both Code for unused space in TF
            return ""; 
        }
        if ((code >= 144) &&  (code <= 159)) {//90h..9Fh - Reserved for future use
            return ""; 
        }
        return  null; //String.fromCharCode(code);
    };
    
    /*
    mainCharacterTable = {
        "32":" ","33":"!","34":"\"","35":"#","36":"¤","37":"%","38":"&","39":"'","40":"(","41":")","42":"*","43":"+","44":",","45":"-","46":".","47":"/",
        "48":"0","49":"1","50":"2","51":"3","52":"4","53":"5","54":"6","55":"7","56":"8","57":"9","58":":","59":";","60":"<","61":"=","62":">","63":"?",
        "64":"@","65":"A","66":"B","67":"C","68":"D","69":"E","70":"F","71":"G","72":"H","73":"I","74":"J","75":"K","76":"L","77":"M","78":"N","79":"O",
        "80":"P","81":"Q","82":"R","83":"S","84":"T","85":"U","86":"V","87":"W","88":"X","89":"Y","90":"Z","91":"[","92":"\\","93":"]","94":"^","95":"_",
        "96":"`","97":"a","98":"b","99":"c","100":"d","101":"e","102":"f","103":"g","104":"h","105":"i","106":"j","107":"k","108":"l","109":"m","110":"n","111":"o",
        "112":"p","113":"q","114":"r","115":"s","116":"t","117":"u","118":"v","119":"w","120":"x","121":"y","122":"z","123":"{","124":"|","125":"}","126":"~",
        "160": " ", "161":"¡", "162":"¢", "163": "£", "164": "$", "165":"¥","166":"","167":"§","168":"","169":"`","170":"\"","171":"«","172":"","173":"­","174":"","175":"",
        "176":"°","177":"±","178":"²","179":"³","180":"×","181":"µ","182":"¶","183":"·","184":"÷","185":"'","186":"\"","187":"»","188":"¼","189":"½","190":"¾","191":"¿",
        "192":"","193":"`","194":"'","195":"^","196":"-","197":"Å","198":"Æ","199":"Ç","200":"È","201":"É","202":"Ê","203":"Ë","204":"Ì","205":"Í","206":"Î","207":"Ï","208":"Ð",
        "209":"Ñ","210":"Ò","211":"©","212":"®","213":"Õ","214":"Ö","215":"×","216":"Ø","217":"Ù","218":"Ú","219":"Û","220":"Ü","221":"Ý","222":"Þ","223":"ß","224":"à","225":"á","226":"â","227":"ã","228":"ä","229":"å","230":"æ","231":"ç","232":"è","233":"é","234":"ê","235":"ë","236":"ì","237":"í","238":"î","239":"ï","240":"ð","241":"ñ","242":"ò","243":"ó","244":"ô","245":"õ","246":"ö","247":"÷","248":"ø","249":"ù","250":"ú","251":"û","252":"ü","253":"ý","254":"þ"
        
        nordicCharacterTable = [
            ["", "▸", "",  "0", "@", "P", "`", "p", "Ç", "É", "á", "░", "└", "╩", "α", "≡" ],
            ["😊", "◂", "!", "1", "A", "Q", "a", "q", "ü", "æ", "í", "▒", "┴", "╤", "ß", "±"],
            ["☻", "⇕", "\"", "2", "B", "R", "b", "r", "é", "Æ", "ó", "▓", "┬", "╥", "Γ", "≥"],
            ["🖤", "!!","#", "3", "C", "S", "c", "s", "â", "ô", "ú", "|", "├", "╙", "π", "≤"],
            ["♦", "¶", "$", "4", "D", "T", "d", "t" ,"ä", "ö", "ñ", "┤", "─", "╘", "Σ", "⌠" ],
            ["♣","§", "%","5", "E", "U", "e", "u", "à", "ò", "Ñ", "", "", "", "σ", "⌡"], //incomplete
            ["♠", "-", "&", "6", "F", "V", "f", "v", "å", "û", "ª","","","", "µ",  "÷"], //incomplete
            ["", "", "'", "7", "G", "W", "g", "w", "ç", "ú", "º","","","", "", "τ", "≈"], //incomplete
            ["", "", "(", "8", "H", "X", "h", "x", "ê", "ÿ", "¿", "","","", "Φ", ""], //incomplete
            ["", "", ")", "9", "I", "Y", "i", "y", "ë", "Ö", "","","","", "", "Θ", ""], //incomplete
            ["", "", "*", ":", "J", "Z", "j", "z", "è", "Ü", "","","","", "", "Ω", "" ], //incomplete
            ["", "", "+", ";", "K", "[", "k", "{", "ï", "ø","½", "", "", "", "δ", "√"], //incomplete
            ["", " ", ",", "<", "L", "\\", "l", "|", "î", "£", "¼", "", "", "", "∞", "ⁿ"], //incomplete
            ["♪", "", "-", "=", "M", "]", "m", "}", "ì", "Ø", "¡", "", "", "","ø", "²"], //incomplete
            ["🎵","",".", ">", "N", "^", "n", "~", "Ä", "", "«", "", "","", "ɛ", "■"], //incomplete
            ["☀", "", "/", "?", "O", "_", "o", "", "Å", "ƒ", "¤", "", "", "", "∩", ""  ] //incomplete
        ];
        
        
        lookupSTLCharacterSet(cpn) {
            if (cpn == "865") { //38h 36h 35h -> Nordic
                let nordicCharacterTable = [
                    ["", "▸", "",  "0", "@", "P", "`", "p", "Ç", "É", "á", "░", "└", "╩", "α", "≡" ],
                    ["😊", "◂", "!", "1", "A", "Q", "a", "q", "ü", "æ", "í", "▒", "┴", "╤", "ß", "±"],
                    ["☻", "⇕", "\"", "2", "B", "R", "b", "r", "é", "Æ", "ó", "▓", "┬", "╥", "Γ", "≥"],
                    ["🖤", "!!","#", "3", "C", "S", "c", "s", "â", "ô", "ú", "|", "├", "╙", "π", "≤"],
                    ["♦", "¶", "$", "4", "D", "T", "d", "t" ,"ä", "ö", "ñ", "┤", "─", "╘", "Σ", "⌠" ],
                    ["♣","§", "%","5", "E", "U", "e", "u", "à", "ò", "Ñ", "", "", "", "σ", "⌡"], //incomplete
                    ["♠", "-", "&", "6", "F", "V", "f", "v", "å", "û", "ª","","","", "µ",  "÷"], //incomplete
                    ["", "", "'", "7", "G", "W", "g", "w", "ç", "ú", "º","","","", "", "τ", "≈"], //incomplete
                    ["", "", "(", "8", "H", "X", "h", "x", "ê", "ÿ", "¿", "","","", "Φ", ""], //incomplete
                    ["", "", ")", "9", "I", "Y", "i", "y", "ë", "Ö", "","","","", "", "Θ", ""], //incomplete
                    ["", "", "*", ":", "J", "Z", "j", "z", "è", "Ü", "","","","", "", "Ω", "" ], //incomplete
                    ["", "", "+", ";", "K", "[", "k", "{", "ï", "ø","½", "", "", "", "δ", "√"], //incomplete
                    ["", " ", ",", "<", "L", "\\", "l", "|", "î", "£", "¼", "", "", "", "∞", "ⁿ"], //incomplete
                    ["♪", "", "-", "=", "M", "]", "m", "}", "ì", "Ø", "¡", "", "", "","ø", "²"], //incomplete
                    ["🎵","",".", ">", "N", "^", "n", "~", "Ä", "", "«", "", "","", "ɛ", "■"], //incomplete
                    ["☀", "", "/", "?", "O", "_", "o", "", "Å", "ƒ", "¤", "", "", "", "∩", ""  ] //incomplete
                ];
                return nordicCharacterTable;
            }
            if (cpn == "863") { //38h 36h 33h -> Canada - French
                
            }
            if (cpn == "860") {//38h 36h 30h -> Portugal
            }
            if (cpn == "850") {//38h 35h 30h -> Multilingual
            }
            if (cpn == "437") {//34h 33h 37h -> US
            }
        };
        
        lookupCharacterTable(cct) {
            if (cct = "48-48") { //30h 30h - >latin
                
            }
            if (cct = "48-49") { //30h 31h - >latin/cyrillic
                
            }
            if (cct = "48-40") { //30h 32h - >latin/arabic
                
            }
            if (cct = "48-51") { //30h 33h - >latin/greek
                
            }
            if (cct = "48-52") { //30h 34h - >latin/hebrew
                
            }
        }
        
        */
        
        async executeAdd(inputs, outputs) {
            try {
                let client;
                if (!this.Payload.inputs.private_key && !this.Payload.inputs.config_url){
                    client = this.Client;
                } else {
                    let privateKey = this.Payload.inputs.private_key || this.Client.signer.signingKey.privateKey.toString();
                    let configUrl = this.Payload.inputs.config_url || this.Client.configUrl;
                    client = await ElvOFabricClient.InitializeClient(configUrl, privateKey)
                }
                
                const encrypt = inputs.store_encrypted;
                let objectId = inputs.mezzanine_object_id || client.utils.DecodeVersionHash(inputs.mezzanine_object_version_hash).objectId;
                const libraryId = await this.getLibraryId(objectId, client);
                if (!libraryId) {
                    throw (new Error("Could not retrieve library for " + objectId));
                }
                const offeringKey = inputs.offering_key;
                const filePath = inputs.file_path;
                const fileName = path.basename(filePath);
                const isDefault = inputs.is_default;
                const forced = inputs.forced;
                const language = inputs.language || (ElvOManageCaptions.LANGUAGES[inputs.label.replace(/_SDH/,"").replace(/--.*/,"")] + (inputs.label.match(/_SDH/) ? "-sdh" : ""));
                const label = inputs.label + ((forced) ? "_forced" : "");
                let timeShift;
                if (inputs.offset_sec != null) {
                    timeShift = inputs.offset_sec;
                    this.reportProgress("Offset provided as input", timeShift);
                } else { //offset by entry_point
                    let entryPointRat = await this.getMetadata({
                        client, objectId, libraryId,
                        metadataSubtree: "offerings/"+offeringKey+"/entry_point_rat"
                    });
                    if (entryPointRat) {
                        let entryPointType = typeof entryPointRat;
                        if (((entryPointType == "string") && entryPointRat.match(/^[0-9\/.]+$/)) || (entryPointType == "number")) {
                            timeShift = eval(entryPointRat) * -1;
                        } else {
                            this.Error("Invalid entry_point_rat found in " +objectId, entryPointRat);
                            timeShift = 0;
                        }
                    } else {
                        timeShift = 0;
                    }
                    this.reportProgress("Offset to account for entry-point", timeShift);
                }
                const streamKey = inputs.stream_key;
                
                await  this.acquireMutex(objectId);
                
                let finalData;
                if (timeShift != 0) {
                    finalData = this.translateVTT(filePath, timeShift, 24, 24, {trim: true});
                } else {
                    finalData = fs.readFileSync(filePath);;
                }
                let writeToken = await this.getWriteToken({
                    client,
                    libraryId,
                    objectId
                });
                
                let uploadPartResponse = await client.UploadPart({
                    libraryId,
                    objectId,
                    writeToken,
                    data: finalData,
                    encryption: encrypt ? "cgck" : "none"
                });
                let partHash = uploadPartResponse.part.hash;
                let finalizeResponse = await client.FinalizeContentObject({
                    libraryId,
                    objectId,
                    writeToken,
                    commitMessage: "Captions uploaded as new part for " + label
                });
                this.ReportProgress("Captions uploaded as new part: " + partHash);
                let hashAfterUpload = finalizeResponse.hash;
                
                // wait for publish to finish before re-reading metadata
                let publishFinished = false;
                let latestHash;
                this.reportProgress("waiting for publish to finish...");
                while (!publishFinished) {
                    latestHash = await this.getVersionHash({libraryId, objectId, client});
                    if(latestHash === hashAfterUpload) {
                        publishFinished = true;
                    } else {
                        this.Debug("Waiting 15 seconds and checking again if publish is complete");
                        await this.sleep(15 * 1000);
                    }
                }
                
                // =======================================
                // add metadata for caption stream
                // =======================================
                
                // reload metadata (now includes updated '/eluv-fb.parts' metadata from captions upload)
                let metadata = await this.getMetadata({
                    client,
                    libraryId,
                    versionHash: hashAfterUpload
                });
                
                let offeringMetadata = metadata.offerings[offeringKey];
                
                
                //find if new caption overlaps with existing caption file
                try {
                    if (offeringMetadata && offeringMetadata.media_struct ) {
                        for (let streamId in offeringMetadata.media_struct.streams) {
                            if (streamId.match(/^captions-/)) {
                                let stream = offeringMetadata.media_struct.streams[streamId];
                                //if  ((stream.label == label) || ((stream.language == language) && ((stream.forced == true) == forced)) {
                                if  (stream.label == label) { //removed other test to avoid clobbering in the case of SDH
                                    this.reportProgress("Removing overlapping caption file for "+ label, streamId);
                                    delete  offeringMetadata.media_struct.streams[streamId];
                                    delete offeringMetadata.playout.streams[streamId];
                                }
                            }
                        }
                    }
                } catch(errOverlap) {
                    this.Error("Could not remove overlap", errOverlap);
                }
                
                
                // create stream key
                const slugInput = streamKey || ("captions-" + label + fileName);
                
                let captionStreamKey = this.slugit(slugInput);
                let captionRepKey = captionStreamKey + "-vtt"; // representation is VTT, append as suffix as convention
                
                // copy temporal info from video stream
                let vidStream;
                for (let streamId in offeringMetadata.media_struct.streams) {
                    if (offeringMetadata.media_struct.streams[streamId].codec_type == "video") {
                        vidStream = offeringMetadata.media_struct.streams[streamId];
                        break;
                    }
                }
                const timeBase = vidStream.duration.time_base;
                const durationRat = vidStream.duration.rat;
                const durationTs = vidStream.duration.ts;
                const rate = vidStream.rate;
                
                // construct metadata for caption stream media_struct
                
                const mediaStructStream = {
                    bit_rate: 100,
                    codec_name: "none",
                    codec_type: "captions",
                    default_for_media_type: isDefault,
                    duration: {
                        time_base: timeBase,
                        ts: durationTs
                    },
                    label: label,
                    language: language,
                    optimum_seg_dur: {
                        "time_base": timeBase,
                        "ts": durationTs
                    },
                    rate: rate,
                    sources: [
                        {
                            duration: {
                                time_base: timeBase,
                                ts: durationTs
                            },
                            entry_point: {
                                rat: "0",
                                time_base: timeBase
                            },
                            source: partHash,
                            timeline_end: {
                                rat: durationRat,
                                time_base: timeBase
                            },
                            timeline_start: {
                                rat: "0",
                                time_base: timeBase
                            }
                        }
                    ],
                    start_time: {
                        time_base: timeBase,
                        ts: 0
                    },
                    time_base: timeBase
                };
                
                if (forced) {
                    mediaStructStream.forced = true;
                }
                
                // construct metadata for caption stream playout
                
                let playoutStream = {
                    encryption_schemes: {},
                    representations: {}
                };
                playoutStream.representations[captionRepKey] = {
                    bit_rate: 100,
                    media_struct_stream_key: captionStreamKey,
                    type: "RepCaptions"
                };
                
                // merge into object offering metadata
                offeringMetadata.media_struct.streams[captionStreamKey] = mediaStructStream;
                offeringMetadata.playout.streams[captionStreamKey] = playoutStream;
                
                // write back to object
                writeToken = await this.getWriteToken({
                    client, objectId, libraryId
                });
                await client.ReplaceMetadata({
                    libraryId: libraryId,
                    objectId: objectId,
                    writeToken: writeToken,
                    metadataSubtree:  "offerings/" + offeringKey,
                    metadata: offeringMetadata,
                    client
                });
                
                let response = await this.FinalizeContentObject({
                    libraryId: libraryId,
                    objectId: objectId,
                    writeToken: writeToken,
                    commitMessage: "Caption stream added using stream key: " + captionStreamKey,
                    client
                });
                outputs.caption_key = captionStreamKey;
                outputs.mezzanine_object_version_hash = response.hash;
                
                this.ReportProgress("Caption stream added using stream key: " + captionStreamKey, response.hash);
            } catch(err) {
                this.releaseMutex();
                this.Error("Adding captions failed", err);
                return ElvOAction.EXECUTION_EXCEPTION;
            }
            this.releaseMutex();
            return ElvOAction.EXECUTION_COMPLETE
        };
        
        
        async executeClear(inputs, outputs) {
            try {
                let client;
                if (!this.Payload.inputs.private_key && !this.Payload.inputs.config_url){
                    client = this.Client;
                } else {
                    let privateKey = this.Payload.inputs.private_key || this.Client.signer.signingKey.privateKey.toString();
                    let configUrl = this.Payload.inputs.config_url || this.Client.configUrl;
                    client = await ElvOFabricClient.InitializeClient(configUrl, privateKey)
                }
                
                let objectId = inputs.mezzanine_object_id || client.utils.DecodeVersionHash(inputs.mezzanine_object_version_hash).objectId;
                const libraryId = await this.getLibraryId(objectId, client);
                if (!libraryId) {
                    throw (new Error("Could not retrieve library for " + objectId));
                }
                const offeringKey = inputs.offering_key;
                let streamKeys = [];
                if (inputs.stream_key) {
                    streamKeys.push(inputs.stream_key);
                }
                
                
                await  this.acquireMutex(objectId)
                let offeringData = await this.getMetadata({
                    client,
                    libraryId,
                    objectId,
                    metadataSubtree: "offerings/"+offeringKey
                });
                
                if  (inputs.clear_all) {
                    for (let streamId in offeringData.media_struct.streams) {
                        if (streamId.match(/^captions-/)) {
                            streamKeys.push(streamId);
                            this.reportProgress("All captions streams are to be removed", streamId);
                        }
                    }
                } else {
                    for (let streamId in offeringData.media_struct.streams) {
                        let streamData = offeringData.media_struct.streams[streamId];
                        if  (!inputs.stream_key && inputs.label && (streamData.label == inputs.label) ){
                            streamKeys.push(streamId);
                            this.reportProgress("Captions stream matches label "+ inputs.label, streamId);
                            continue;
                        }
                        if  (!inputs.stream_key && inputs.language && (streamData.language == inputs.language)) {
                            streamKeys.push(streamId);
                            this.reportProgress("Captions stream matches language "+ inputs.language, streamId);
                            continue;
                        }
                    }
                }
                if  (streamKeys.length == 0) {
                    this.ReportProgress("No captions streams to remove");
                    this.releaseMutex()
                    return ElvOAction.EXECUTION_FAILED;
                } else {
                    this.ReportProgress("Captions streams to removed", streamKeys);
                }
                for (let captionStreamKey  of streamKeys) {
                    // Delete caption stream from metadata
                    delete offeringData.media_struct.streams[captionStreamKey];
                    delete offeringData.playout.streams[captionStreamKey];
                }
                let writeToken = await this.getWriteToken({
                    client,
                    libraryId,
                    objectId
                });
                
                await  client.ReplaceMetadata({
                    writeToken,
                    libraryId,
                    objectId,
                    metadataSubtree: "offerings/"+offeringKey,
                    metadata: offeringData
                });
                
                
                let response = await this.FinalizeContentObject({
                    libraryId: libraryId,
                    objectId: objectId,
                    writeToken: writeToken,
                    commitMessage: (streamKeys.length == 1) ? ("Removed captions  stream " + streamKeys[0])  : ("Removed " + streamKeys.length + " captions streams") ,
                    client
                });
                outputs.removed_stream_keys = streamKeys;
                outputs.mezzanine_object_version_hash = response.hash;
                
                this.ReportProgress("Caption streams removed " + streamKeys.length, response.hash);
            } catch(err) {
                this.releaseMutex();
                this.Error("Removing captions failed", err);
                return ElvOAction.EXECUTION_EXCEPTION;
            }
            this.releaseMutex();
            return ElvOAction.EXECUTION_COMPLETE
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
        
        slugit(str) {
            return str.toLowerCase().replace(/ /g, "-").replace(/[^a-z0-9\-]/g,"");
        };
        
        
        static LANGUAGES = {
            "Afghan": "ps",
            "Afghan / Pashto": "ps",
            "Afghan/Pashto": "ps",
            "Afrikaans": "af",
            "Albanian": "sq",
            "Arabic": "ar",
            "Azerbaijani": "az",
            "Bangla": "bn",
            "Bangla / Bengali": "bn",
            "Basque": "eu",
            "Bengali": "bn",
            "Bosnian": "bs",
            "Breton":"br",
            "Bulgarian": "bg",
            "Burmese": "my",
            "Cambodian": "km",
            "Catalan": "ca",
            "Chinese (Hong Kong)":	"zh-hk",
            "Chinese (PRC)": "zh-cn",
            "Chinese (Mandarin, PRC)": "zh-cn",
            "Chinese (Singapore)":	"zh-sg",
            "Chinese (Taiwan)":	"zh-tw",
            "Chinese (Taiwanese)":	"zh-tw",
            "Chinese (Cantonese)": "zh-hk",
            "Chinese (Mandarin)": "zh-cn",
            "Chinese (Mandarin, Taiwanese)": "zh-tw",
            "Chinese (Mandarin Simplified)": "zh-hans",
            "Chinese (Mandarin Simplified) [Text]": "zh-hans",
            "Chinese (Mandarin Traditional)": "zh-hant",
            "Chinese (Mandarin Traditional) [Text]": "zh-hant",
            "Chinese - Traditional": "zh-hant",
            "Chinese - Simplified": "zh-hans",
            "Croatian":	"hr",
            "Czech": "cs",
            "Danish": "da",
            "Dutch (Belgium)":	"nl-be",
            "Dutch (Standard)":	"nl",
            "Dutch (Flemish)":	"nl-be",
            "Dutch (Netherlands)": "nl",
            "Dutch":	"nl",
            "English (UK)": "en-uk",
            "English": "en",
            "English (United States)": "en-us", 
            "English (Australia)": "en-au",
            "English (Belize)":	"en-bz",
            "English (Canada)":	"en-ca",
            "English (Ireland)": "en-ie",
            "English (Jamaica)": "en-jm",
            "English (New Zealand)": "en-nz",
            "English (South Africa)": "en-za",
            "English (Trinidad)": "en-tt",
            "Estonian": "et",  
            "Farsi": "fa",
            "Finnish": "fi", 
            "French": "fr",
            "French (Parisian)": "fr",
            "French (Canadian)": "fr-ca",
            "Français (Canada)": "fr-ca",
            "French (Belgium)":	"fr-be",
            "French (Luxembourg)":	"fr-lu",
            "French (Standard)":	"fr",
            "French - Continental": "fr",
            "French (Continental)": "fr",
            "French (Switzerland)":	"fr-ch",
            "Georgian": "ka",   
            "German (Germany)": "de",
            "German (Austria)":	"de-at",
            "German (Liechtenstein)": "de-li",
            "German (Luxembourg)": "de-lu",
            "German (Standard)":	"de",
            "German (Switzerland)": "de-ch",
            "German (Swiss)":"de-ch",
            "German": "de",
            "Greek": "el",
            "Gujarati": "gu",
            "Hebrew": "he",
            "Hindi": "hi",
            "Hungarian": "hu",
            "Icelandic": "is",
            "Indonesian": "id",  
            "Indonesian / Bahasa": "id",
            "Bahasa Indonesia": "id", 
            "Italian": "it",  
            "Italian (Standard)": "it",
            "Italian (Switzerland)": "it-ch",
            "Japanese": "ja",
            "Kannada": "kn",
            "Kazakh": "kk",
            "Khmer": "km",
            "Korean": "ko",
            "Kurdish": "ku",
            "Laothian": "lo",
            "Latvian": "lv",
            "Latvian (Lettish)": "lv",
            "Lettish": "lv",
            "Lithuanian": "lt",
            "Macedonian": "mk",
            "Malay": "ms",
            "Malagasy": "mg",
            "Malayalam": "ml",
            "Manx": "gv",
            "Maori": "mi",
            "Marathi": "mr",
            "Moldavian": "mo",
            "Mongolian": "mn",
            "Nauvhal": "nwi", //ISO 639-3, no ISO 639-1 code
            "Nawal": "nwi",
            "Nivai": "nwi",
            "Nivhaal": "nwi",
            "None": "none", 
            "M&E": "none",
            "Norwegian": "no",
            "Northern Sotho": "nso", //ISO 639-2, no ISO 639-1 code
            "Nepali": "ne",
            "Pashto": "ps",
            "Pedi": "nso",   //alternate name for Northern Sotho
            "Polish": "pl",
            "Portuguese (Brazil)": "pt-br",
            "Portuguese (Portugal)": "pt",
            "Portuguese": "pt",
            "Punjabi": "pa",
            "Romanian": "ro",
            "Romanian (Republic of Moldova)": "ro-md",
            "Romany": "rom", //iso ISO 639-2  - no ISO 639-1
            "Russian (Russia)": "ru",
            "Russian (Republic of Moldova)": "ru-md",
            "Russian (Ukraine)": "ru-uk",
            "Russian": "ru",
            "Sepedi": "nso", //alternate name for Northern Sotho
            "Serbian": "sr",
            "Serbo-Croatian": "sh",
            "Slovak": "sk",
            "Slovakian": "sk",
            "Slovenian": "sl",
            "Slovene": "sl",
            "Somali": "so",
            "Sotho, Southern": "st",
            "Spanish (Argentinean)": "es-ar",
            "Spanish (Castilian)": "es-es",
            "Spanish (Latin Am)": "es-419",
            "Spanish (Bolivia)": "es-bo",
            "Spanish (Chile)": "es-cl",
            "Spanish (Chilean)": "es-cl",
            "Spanish (Colombia)": "es-co",
            "Spanish (Costa Rica)": "es-cr",
            "Spanish (Dominican Republic)": "es-do",
            "Spanish (Ecuador)": "es-ec",
            "Spanish (El Salvador)": "es-sv",
            "Spanish (Guatemala)": "es-gt",
            "Spanish (Honduras)": "es-hn",
            "Spanish (Mexico)": "es-mx",
            "Spanish (Mexican)": "es-mx",
            "Spanish (Nicaragua)": "es-ni",
            "Spanish (Panama)": "es-pa",
            "Spanish (Paraguay)": "es-py",
            "Spanish (Peru)": "es-pe",
            "Spanish (Puerto Rico)": "es-pr",
            "Spanish (Spain)": "es",
            "Spanish (Uruguay)": "es-uy",
            "Spanish (Venezuela)": "es-ve",
            "Spanish": "es",
            "Swedish": "sv",
            "Tagalog": "tl",
            "Tamil": "ta",
            "Telugu":  "te",
            "Thai": "th",
            "Turkish": "tr",
            "Ukrainian": "uk",
            "Urdu": "ur",
            "Vietnamese": "vi"    
        };
        
        static LANGUAGE_LABELS = {
            "ps": "Pashto",
            "af": "Afrikaans",
            "sq": "Albanian",
            "ar": "Arabic",
            "az": "Azerbaijani",
            "bn": "Bangla / Bengali",
            "eu": "Basque",
            "bs": "Bosnian",
            "br": "Breton",
            "bg": "Bulgarian",
            "my": "Burmese",
            "km": "Cambodian",
            "ca": "Catalan",
            "zh-hk": "Chinese (Cantonese)",
            "zh-cn": "Chinese (PRC)",
            "zh-sg": "Chinese (Singapore)",
            "zh-tw": "Chinese (Taiwan)",
            "zh-hans": "Chinese (Mandarin Simplified)",
            "zh-hant": "Chinese (Mandarin Traditional)",
            "hr": "Croatian",
            "cs": "Czech",
            "da": "Danish",
            "nl-be": "Dutch (Flemish)",
            "nl": "Dutch (Netherlands)",
            "en-uk": "English (UK)",
            "en": "English",
            "en-us": "English (United States)",
            "en-au": "English (Australia)",
            "en-bz": "English (Belize)",
            "en-ca": "English (Canada)",
            "en-ie": "English (Ireland)",
            "en-jm": "English (Jamaica)",
            "en-nz": "English (New Zealand)",
            "en-za": "English (South Africa)",
            "en-tt": "English (Trinidad)",
            "et": "Estonian",
            "fa": "Farsi",
            "fi": "Finnish",
            "fr": "French (Parisian)",
            "fr-ca": "French (Canadian)",
            "fr-be": "French (Belgium)",
            "fr-lu": "French (Luxembourg)",
            "fr-ch": "French (Switzerland)",
            "ka": "Georgian",
            "de": "German (Germany)",
            "de-at": "German (Austria)",
            "de-li": "German (Liechtenstein)",
            "de-lu": "German (Luxembourg)",
            "de-ch": "German (Swiss)",
            "el": "Greek",
            "he": "Hebrew",
            "hi": "Hindi",
            "hu": "Hungarian",
            "gu": "Gujarati",
            "is": "Icelandic",
            "id": "Indonesian / Bahasa",
            "it": "Italian",
            "it-ch": "Italian (Switzerland)",
            "ja": "Japanese",
            "kn": "Kannada",
            "kk": "Kazakh",
            "ko": "Korean",
            "ku": "Kurdish",
            "lo": "Laothian",
            "lv": "Latvian",
            "lt": "Lithuanian",
            "mk": "Macedonian",
            "ms": "Malay",
            "mg": "Malagasy",
            "ml": "Malayalam",
            "gv": "Manx",
            "mi": "Maori",
            "mr": "Marathi",
            "mo": "Moldavian",
            "mn": "Mongolian",
            "nwi": "Nauvhal",
            "none": "None",
            "no": "Norwegian",
            "nso": "Sepedi",
            "ne": "Nepali",
            "pa": "Punjabi",
            "pl": "Polish",
            "pt-br": "Portuguese (Brazil)",
            "pt": "Portuguese (Portugal)",
            "ro": "Romanian",
            "ro-md": "Romanian (Republic of Moldova)",
            "rom": "Romany",
            "ru": "Russian (Russia)",
            "ru-md": "Russian (Republic of Moldova)",
            "ru-uk": "Russian (Ukraine)",
            "sr": "Serbian",
            "sh": "Serbo-Croatian",
            "sk": "Slovakian",
            "sl": "Slovene",
            "so": "Somali",
            "st": "Sotho, Southern",
            "es-ar": "Spanish (Argentinean)",
            "es-es": "Spanish (Castilian)",
            "es-419": "Spanish (Latin Am)",
            "es-bo": "Spanish (Bolivia)",
            "es-cl": "Spanish (Chilean)",
            "es-co": "Spanish (Colombia)",
            "es-cr": "Spanish (Costa Rica)",
            "es-do": "Spanish (Dominican Republic)",
            "es-ec": "Spanish (Ecuador)",
            "es-sv": "Spanish (El Salvador)",
            "es-gt": "Spanish (Guatemala)",
            "es-hn": "Spanish (Honduras)",
            "es-mx": "Spanish (Mexico)",
            "es-ni": "Spanish (Nicaragua)",
            "es-pa": "Spanish (Panama)",
            "es-py": "Spanish (Paraguay)",
            "es-pe": "Spanish (Peru)",
            "es-pr": "Spanish (Puerto Rico)",
            "es": "Spanish",
            "es-uy": "Spanish (Uruguay)",
            "es-ve": "Spanish (Venezuela)",
            "sv": "Swedish",
            "tl": "Tagalog",
            "ta": "Tamil",
            "te": "Telugu",
            "th": "Thai",
            "tr": "Turkish",
            "uk": "Ukrainian",
            "ur": "Urdu",
            "vi": "Vietnamese"
        };
        
        static VERSION = "0.6.9";
        static REVISION_HISTORY = {
            "0.0.1": "Initial release",
            "0.0.2": "Adds support for stl",
            "0.0.3": "Improves parsing for itt",
            "0.0.4": "Improves handling of XML br tag",
            "0.0.5": "Handles span of plain text",
            "0.0.6": "Improves Handling of italic in span",
            "0.0.7": "Improves of br tags in ITT (supports for <br /> - with spaces)",
            "0.0.8": "Adds support for dropframe timecodes",
            "0.1.0": "Normalizes logging",
            "0.1.1": "Adds support for SRT, stops reliance on  solely on file extension for type evaluation",
            "0.1.2": "Marked label _force when forced captions",
            "0.1.3": "Fixes multi-line  entry for SMPTE",
            "0.1.4": "Fixes handling   of <BR /> for SMPTE",
            "0.1.5": "Supports forcing of provided offset, returns used offset as output",
            "0.1.6": "Returns anomalies as output",
            "0.1.7": "Fixes doubling up of italic text",
            "0.1.8": "Preprocess the span sections out of xml formats (ITT and TTML)",
            "0.1.9": "Preprocess non-italic span sections out of xml formats (ITT and TTML)",
            "0.2.0": "Adds a few language codes ",
            "0.2.1": "With ITT do not convert empty line",
            "0.2.2": "Removes overlapping caption files when adding a new one",
            "0.2.3": "Cleans up empty spans as they are not supported by XML parser in ITT",
            "0.2.4": "Added Slovene and Slovakian",
            "0.2.5": "Added a dozen uncommon languages, fixed SCC support for French and Spanish",
            "0.2.6": "Added expected non-conforming SCC character configurations",
            "0.2.7": "Tweaked parsing of XML to ensure simple span are correctly processed",
            "0.2.8": "Avoids issue  with SCC when finished by non blank line",
            "0.2.9": "Uses 0 for offset when encountering negative offset timecode in SCC processing",
            "0.3.0": "Adds handling of formatting in SRT",
            "0.3.1": "Changes SCC behavior of 9452 and 9454",
            "0.3.2": "Removes overlapping caption files when adding a new one (fix)",
            "0.3.3": "Address case of SMPTE files with single caption line",
            "0.3.4": "Changes SCC behavior of 9440, 9456, 13e0 and 1376",
            "0.3.5": "Adds option to force the specified framerate over the document specified one",
            "0.3.6": "Avoids collisions of label when adding captions",
            "0.3.7": "Adds reverse lookup for language labels",
            "0.3.8": "Fixes collisions avoidance of label when adding captions",
            "0.3.9": "Fixes a few label cross reference for languages",
            "0.4.0": "Added Mexican Spanish code",
            "0.4.1": "Adds support for non-drop timecodes in SCC",
            "0.4.2": "Changes label from Flemish and Cantonese",
            "0.4.3": "Adds a catch all for non compliant SCC pair",
            "0.4.4": "Fixed escaping of $ sign when processing ITT",
            "0.4.5": "Adds an auto retry with no offset when timecode are negative",
            "0.4.6": "Change M&E label to None",
            "0.4.7": "Fix  retrying of negative timecodes with 0 offset",
            "0.4.8": "Forcing offset to 0 on retry",
            "0.4.9": "Adds treatment of {\an2} in SRT",
            "0.5.0": "Enforce forced_framerate with all formats",
            "0.5.1": "Improves STL support",
            "0.5.2": "Take entry_point_rat into account when adding titles",
            "0.5.3": "Added support for ù in SCC Spanish",
            "0.5.4": "Adds support for VTT line cues",
            "0.5.5": "Typo fix in entry point offset",
            "0.5.6": "Fix glitch in parsing of nested span in itt",
            "0.5.7": "Adds kn, pa and gu",
            "0.6.0": "Uses FFMPEG for scc translation",
            "0.6.1": "Uses debug switch to select FFMPEG or native for scc translation",
            "0.6.2": "Reconcile ffmpeg and native SCC parsing",
            "0.6.3": "Fix glitch in reconciliation",
            "0.6.4": "Parse out CRID for language code lookup",
            "0.6.5": "Enforces force_framerate",
            "0.6.6": "Fix handling of slighly negative timecode",
            "0.6.7": "New native conversion for SCC",
            "0.6.8": "Modified SCC parsing with off-spec linefeeds",
            "0.6.9": "Adds non-buffered mode for non-compliant SCC files"
        };
    };
    
    
    if (ElvOAction.executeCommandLine(ElvOManageCaptions)) {
        ElvOAction.Run(ElvOManageCaptions);
    } else {
        module.exports=ElvOManageCaptions;
    }
