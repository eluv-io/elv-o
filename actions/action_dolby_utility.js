const ElvOAction = require("../o-action").ElvOAction;
const Path = require('path');
const fs = require('fs');
const { execSync, spawn } = require('child_process');


class ElvOActionDolbyUtility extends ElvOAction  {
    
    Parameters() {
        return {
            parameters: {
                atmos_executable_path:{type:"string", required: false, default: "Tools/enc_atmos_to_mp4.sh"},
                vision_executable_path:{type:"string", required: false, default: "Tools/enc_dv_prores_to_mp4.sh"},
                extract_frames_executable_path:{type:"string", required: false, default: "Tools/enc_dv20_extract_frames.sh"},
                run_pipeline_executable_path:{type:"string", required: false, default: "Tools/enc_dv20_run_pipeline.sh"},
                action: {type: "string", required: true, values: ["ENCODE_ATMOS_TO_MP4", "ENCODE_2D_DOLBY_VISION_TO_MP4", "ENCODE_3D_DOLBY_VISION_TO_MP4", "EXTRACT_3D_DOLBY_VISION_FRAMES", "ENCODE_3D_DOLBY_VISION_FROM_FRAMES"]}
            }
        };
    };
    
    IOs(parameters) {
        let inputs ={}, outputs={};
        if (parameters.action == "ENCODE_ATMOS_TO_MP4") {          
            inputs.input_file_path = {type: "file", required: true};
            inputs.output_dir_path = {type: "file", required: true};
            inputs.profile = {type: "string", required: false, default: "av", values: ["av", "music", "podcast", "audiobook"]};
            outputs.stderr = {type: "string"};
            outputs.execution_code = {type:"numeric"};
            outputs.output_file_path = {type: "string"};
        }
        if (parameters.action == "ENCODE_2D_DOLBY_VISION_TO_MP4") {          
            inputs.input_file_path = {type: "file", required: true};
            inputs.input_metadata_path = {type: "file", required: true};
            inputs.output_dir_path = {type: "file", required: true};
            inputs.profile = {type: "string", required: false, default: "medium", values: ["veryslow", "slower", "slow", "medium", "fast", "faster", "veryfast", "superfast", "ultrafast"]};
            outputs.stderr = {type: "string"};
            outputs.execution_code = {type:"numeric"};
            outputs.output_file_path = {type: "string"};
        }
        if (parameters.action == "ENCODE_3D_DOLBY_VISION_TO_MP4") {
            inputs.left_eye_file_path = {type: "file", required: true};
            inputs.right_eye_file_path = {type: "file", required: true};
            inputs.input_metadata_path = {type: "file", required: true};
            inputs.output_dir_path = {type: "file", required: true};
            inputs.profile = {type: "string", required: false, default: "medium", values: ["veryslow", "slower", "slow", "medium", "fast", "faster", "veryfast", "superfast", "ultrafast"]};
            outputs.stderr = {type: "string"};
            outputs.execution_code = {type:"numeric"};
            outputs.output_file_path = {type: "string"};
        }
        if (parameters.action == "EXTRACT_3D_DOLBY_VISION_FRAMES") {
            inputs.left_eye_file_path = {type: "file", required: true};
            inputs.right_eye_file_path = {type: "file", required: true};
            inputs.output_dir_path = {type: "file", required: true};
            outputs.frames_dir_path = {type: "string"};
            outputs.frame_rate = {type: "string"};
            outputs.transfer = {type: "string"};
            outputs.primaries = {type: "string"};
            outputs.execution_code = {type: "numeric"};
        }
        if (parameters.action == "ENCODE_3D_DOLBY_VISION_FROM_FRAMES") {
            inputs.frames_dir_path = {type: "file", required: true};
            inputs.input_metadata_path = {type: "file", required: true};
            inputs.output_dir_path = {type: "file", required: true};
            inputs.rung = {type: "string", required: true};
            inputs.tier = {type: "string", required: false, default: null};
            inputs.vbv_rate_bps = {type: "numeric", required: false, default: null};
            inputs.vbv_buffer_bits = {type: "numeric", required: false, default: null};
            inputs.frame_rate = {type: "string", required: true};
            inputs.transfer = {type: "string", required: false, default: "pq"};
            inputs.primaries = {type: "string", required: false, default: "p3d65"};
            outputs.stderr = {type: "string"};
            outputs.execution_code = {type: "numeric"};
            outputs.output_file_path = {type: "string"};
        }
        return { inputs, outputs };
    };
    
    ActionId() {
        return "dolby_utility";
    };
    
    
    
    async Execute(inputs, outputs) {
        if (this.Payload.parameters.action == "ENCODE_ATMOS_TO_MP4") {
            return await this.executeEncodeAtmosToMp4(inputs, outputs);
        }
        if (this.Payload.parameters.action == "ENCODE_2D_DOLBY_VISION_TO_MP4") {
            return await this.executeEncode2dDolbyVisionToMp4(inputs, outputs);
        }
        if (this.Payload.parameters.action == "ENCODE_3D_DOLBY_VISION_TO_MP4") {
            return await this.executeEncode3dDolbyVisionToMp4(inputs, outputs);
        }
        if (this.Payload.parameters.action == "EXTRACT_3D_DOLBY_VISION_FRAMES") {
            return await this.executeExtract3dDolbyVisionFrames(inputs, outputs);
        }
        if (this.Payload.parameters.action == "ENCODE_3D_DOLBY_VISION_FROM_FRAMES") {
            return await this.executeEncode3dDolbyVisionFromFrames(inputs, outputs);
        }
        throw "Unsupported action "+this.Payload.parameters.action;
    }
    
    async executeEncodeAtmosToMp4(inputs, outputs) {
        
        let exe = this.Payload.parameters.atmos_executable_path;
        this.reportProgress("Setting encoding file path to ", inputs.output_dir_path);
        let args = ["-c", exe + " -p " + inputs.profile +" \"" + inputs.input_file_path + "\" \"" + inputs.output_dir_path +"\" " ];
        this.reportProgress("Command line args", args);
        let tracker = this;
        let lastReported = null;
        try {
            let expectedFilePath = Path.join(inputs.output_dir_path, Path.basename(inputs.input_file_path).replace(/\.[^.]*$/,"")+".mp4");
            if (fs.existsSync(expectedFilePath)) {
                fs.renameSync(expectedFilePath, expectedFilePath + ".old");
            }
            
            var outsideResolve;
            var outsideReject;
            var commandExecuted = new Promise(function(resolve, reject) {
                outsideResolve = resolve;
                outsideReject = reject;
            });
            
            var proc = spawn("sh",  args);
            
            proc.stdout.on('data', function(data) {
                try {
                    let dataString = data.toString('utf8');
                    let matcher = dataString.match(/Output : (.*\.mp4)/);
                    if (matcher) {
                        outputs.output_file_path = matcher[1];
                    }
                    tracker.ReportProgress("Stdout " + dataString);
                } catch(err) {
                    tracker.ReportProgress("Stdout err" + data, err);
                }
            });
            
            proc.stderr.setEncoding("utf8")
            proc.stderr.on('data', function(data) {                
                let now = new Date().getTime();
                if (!lastReported || (lastReported + 5000 <  now)) {
                    tracker.ReportProgress("Encoding " + data.trim());
                    lastReported = now;
                }
            });
            
            proc.on('close', function(executionCode) {
                outsideResolve(executionCode);
                tracker.ReportProgress("Command executed");
            });
            
            outputs.execution_code = await commandExecuted;
            if (outputs.execution_code == 0) {
                this.ReportProgress("Encoding complete");
                if (!outputs.output_file_path) {
                    this.reportProgress("Using calculated path", expectedFilePath);
                    outputs.output_file_path = expectedFilePath;
                }
                
            } else {
                throw Error("Encoding returned exec code: " +  outputs.execution_code)
            }
        } catch(error) {
            this.Error("Execution failed", error)
            return ElvOAction.EXECUTION_EXCEPTION
        }
        return 100;
    };
    
    
    async executeEncode2dDolbyVisionToMp4(inputs, outputs) {
        let rungMatcher = inputs.rung.match(/([0-9]+x[0-9]+).*@([0-9]+)[0-9][0-9][0-9]/)
        let rung = " --resolution " + rungMatcher[1];
        let rate = " --data-rate " + rungMatcher[2];
        let exe = "python3 /opt/dolby/bin/python_scripts/encode_dvmezz_to_dv8.py" ;//this.Payload.parameters.vision_executable_path;
        this.reportProgress("Setting encoding file path to ", inputs.output_dir_path);
        /*
        python3 /opt/dolby/bin/python_scripts/encode_dvmezz_to_dv8.py --print-all 1 --input-format prores_mov 
        --input /home/elv-o/data/test/20260417155743_VUBX2401921065895570__titl_VU_da6e855b-b32e-436a-9550-b317bfb1d0fa_Jurassic_World_Rebirth_uhd-2d-hdr_US/1ba569bb94ff0fa0c3b0a5467c8b58b4.mpg 
        --input-metadata /home/elv-o/data/test/20260417155743_VUBX2401921065895570__titl_VU_da6e855b-b32e-436a-9550-b317bfb1d0fa_Jurassic_World_Rebirth_uhd-2d-hdr_US/a8cae79c069d0fa892a97b571937b992.xml 
        --output data/test/Preps/titl_VU_da6e855b-b32e-436a-9550-b317bfb1d0fa_DV_1ba569bb94ff0fa0c3b0a5467c8b58b4-DV768x432_h265@1000000.mp4 
        --temp-dir data/test/Preps/titl_VU_da6e855b-b32e-436a-9550-b317bfb1d0fa_DV_1ba569bb94ff0fa0c3b0a5467c8b58b4-DV768x432_h265@1000000 
        --data-stream 1 --data-stream-arch parallel --codec-id hvc1 --resolution 768x432  --data-rate 1000  --progress 1
        */
        let metadataPath;
        if (fs.existsSync(inputs.input_metadata_path)) metadataPath = inputs.input_metadata_path
        else {
            this.reportProgress("Color metadata file not found",  inputs.input_metadata_path);
            let dirPath = Path.dirname(inputs.input_file_path);
            metadataPath = Path.join(dirPath, inputs.input_metadata_path);
            if (!fs.existsSync(metadataPath)) {
                this.reportProgress("Color metadata file not found in source folder", metadataPath);
                throw "Color Metadata file not found"
            }
        }
        let args = [
            "-c", exe + " --input-format " + inputs.profile +" --input \"" + inputs.input_file_path 
            + "\" --input-metadata \"" + metadataPath + "\" --output \"" + inputs.output_dir_path +".mp4\" --temp-dir \"" 
            + inputs.output_dir_path +"\"  --data-stream 1 --data-stream-arch parallel --codec-id hvc1  --progress 1 " + rung + rate 
        ];
        this.reportProgress("Command line args", args);
        let tracker = this;
        let lastReported = null;
        try {
            if (!fs.existsSync(inputs.output_dir_path)) {
                fs.mkdirSync(inputs.output_dir_path);
                this.reportProgress("Created output dir", inputs.output_dir_path);
            }
            let expectedFilePath = Path.join(inputs.output_dir_path + ".mp4");
            if (fs.existsSync(expectedFilePath)) {
                fs.renameSync(expectedFilePath, expectedFilePath + ".old");
            }
            
            var outsideResolve;
            var outsideReject;
            var commandExecuted = new Promise(function(resolve, reject) {
                outsideResolve = resolve;
                outsideReject = reject;
            });
            
            let buffer = "";                                                                                                                                                                                                                        
            var proc = spawn("stdbuf", ["-oL", "sh"].concat(args));
            
            proc.stdout.on('data', function(data) {                                                                                                                                                                                                 
                buffer += data.toString('utf8');                                                                                                                                                                                                    
                let lines = buffer.split('\n');                                                                                                                                                                                                     
                buffer = lines.pop();                                 
                lines.forEach(function(line) {
                    if (!line) return;
                    try {
                        let matcher = line.match(/Output : (.*\.mp4)/);
                        if (matcher) {                                                                                                                                                                                                              
                            outputs.output_file_path = matcher[1];
                        }        
                        let now = new Date().getTime();
                        if (!lastReported || (lastReported + 5000 <  now)) {
                            tracker.ReportProgress("Stdout " + line);  
                            lastReported = now;
                        }                                                                                                                                                                                                                   
                        
                    } catch(err) {
                        tracker.ReportProgress("Stdout err" + line, err);
                    }                                                                                                                                                                                                                               
                });
            }); 
            
            proc.stderr.setEncoding("utf8")
            proc.stderr.on('data', function(data) {                
                let now = new Date().getTime();
                if (!lastReported || (lastReported + 5000 <  now)) {
                    tracker.ReportProgress("Encoding " + data.trim());
                    lastReported = now;
                }
            });
            
            proc.on('close', function(executionCode) {
                outsideResolve(executionCode);
                tracker.ReportProgress("Command executed");
            });
            
            outputs.execution_code = await commandExecuted;
            if (outputs.execution_code == 0) {
                this.ReportProgress("Encoding complete");
                if (!outputs.output_file_path) {
                    this.reportProgress("Using calculated path", expectedFilePath);
                    outputs.output_file_path = expectedFilePath;
                }
                
            } else {
                throw Error("Encoding returned exec code: " +  outputs.execution_code)
            }
        } catch(error) {
            this.Error("Execution failed", error)
            return ElvOAction.EXECUTION_EXCEPTION
        }
        return 100;
    }
    

    async executeEncode3dDolbyVisionToMp4(inputs, outputs) {
        let rungMatcher = inputs.rung.match(/([0-9]+x[0-9]+).*@([0-9]+)[0-9][0-9][0-9]/)
        let rung = rungMatcher[1];
        let rate = rungMatcher[2];
        let exe = "Tools/enc_dv20_prores_to_mp4.sh" ;//this.Payload.parameters.vision_executable_path;
        this.reportProgress("Setting encoding file path to ", inputs.output_dir_path);
        /*
        python3 /opt/dolby/bin/python_scripts/encode_dvmezz_to_dv8.py --print-all 1 --input-format prores_mov 
        --input /home/elv-o/data/test/20260417155743_VUBX2401921065895570__titl_VU_da6e855b-b32e-436a-9550-b317bfb1d0fa_Jurassic_World_Rebirth_uhd-2d-hdr_US/1ba569bb94ff0fa0c3b0a5467c8b58b4.mpg 
        --input-metadata /home/elv-o/data/test/20260417155743_VUBX2401921065895570__titl_VU_da6e855b-b32e-436a-9550-b317bfb1d0fa_Jurassic_World_Rebirth_uhd-2d-hdr_US/a8cae79c069d0fa892a97b571937b992.xml 
        --output data/test/Preps/titl_VU_da6e855b-b32e-436a-9550-b317bfb1d0fa_DV_1ba569bb94ff0fa0c3b0a5467c8b58b4-DV768x432_h265@1000000.mp4 
        --temp-dir data/test/Preps/titl_VU_da6e855b-b32e-436a-9550-b317bfb1d0fa_DV_1ba569bb94ff0fa0c3b0a5467c8b58b4-DV768x432_h265@1000000 
        --data-stream 1 --data-stream-arch parallel --codec-id hvc1 --resolution 768x432  --data-rate 1000  --progress 1
        */
        let metadataPath;
        if (fs.existsSync(inputs.input_metadata_path)) metadataPath = inputs.input_metadata_path
        else {
            this.reportProgress("Color metadata file not found",  inputs.input_metadata_path);
            let dirPath = Path.dirname(inputs.input_file_path);
            metadataPath = Path.join(dirPath, inputs.input_metadata_path);
            if (!fs.existsSync(metadataPath)) {
                this.reportProgress("Color metadata file not found in source folder", metadataPath);
                throw "Color Metadata file not found"
            }
        }
        let args = [
            "-c", exe + " \"" + inputs.left_eye_file_path  + "\"  \"" + inputs.right_eye_file_path
            + "\"  \"" + metadataPath + "\" \"" + rung + "\" \"" + rate + "\" \""  +  inputs.output_dir_path +"\""
        ];
        this.reportProgress("Command line args", args);
        let tracker = this;
        let lastReported = null;
        try {
            if (!fs.existsSync(inputs.output_dir_path)) {
                fs.mkdirSync(inputs.output_dir_path);
                this.reportProgress("Created output dir", inputs.output_dir_path);
            }
            let expectedFilePath = Path.join(inputs.output_dir_path + ".mp4");
            if (fs.existsSync(expectedFilePath)) {
                fs.renameSync(expectedFilePath, expectedFilePath + ".old");
            }
            
            var outsideResolve;
            var outsideReject;
            var commandExecuted = new Promise(function(resolve, reject) {
                outsideResolve = resolve;
                outsideReject = reject;
            });
            
            let buffer = "";                                                                                                                                                                                                                        
            var proc = spawn("stdbuf", ["-oL", "sh"].concat(args));
            
            proc.stdout.on('data', function(data) {                                                                                                                                                                                                 
                buffer += data.toString('utf8');                                                                                                                                                                                                    
                let lines = buffer.split('\n');                                                                                                                                                                                                     
                buffer = lines.pop();                                 
                lines.forEach(function(line) {
                    if (!line) return;
                    try {
                        let matcher = line.match(/Output : (.*\.mp4)/);
                        if (matcher) {                                                                                                                                                                                                              
                            outputs.output_file_path = matcher[1];
                        }        
                        let now = new Date().getTime();
                        if (!lastReported || (lastReported + 5000 <  now)) {
                            tracker.ReportProgress("Stdout " + line);  
                            lastReported = now;
                        }                                                                                                                                                                                                                   
                        
                    } catch(err) {
                        tracker.ReportProgress("Stdout err" + line, err);
                    }                                                                                                                                                                                                                               
                });
            }); 
            
            proc.stderr.setEncoding("utf8")
            proc.stderr.on('data', function(data) {                
                let now = new Date().getTime();
                if (!lastReported || (lastReported + 5000 <  now)) {
                    tracker.ReportProgress("Encoding " + data.trim());
                    lastReported = now;
                }
            });
            
            proc.on('close', function(executionCode) {
                outsideResolve(executionCode);
                tracker.ReportProgress("Command executed");
            });
            
            outputs.execution_code = await commandExecuted;
            if (outputs.execution_code == 0) {
                this.ReportProgress("Encoding complete");
                if (!outputs.output_file_path) {
                    this.reportProgress("Using calculated path", expectedFilePath);
                    outputs.output_file_path = expectedFilePath;
                }
                
            } else {
                throw Error("Encoding returned exec code: " +  outputs.execution_code)
            }
        } catch(error) {
            this.Error("Execution failed", error)
            return ElvOAction.EXECUTION_EXCEPTION
        }
        return 100;
    }

    async executeEncode2dDolbyVisionToMp4ViaShellScript(inputs, outputs) {
        let rung = " -r " + inputs.rung.match(/([0-9]+x[0-9]+)/)[1];
        let exe = this.Payload.parameters.vision_executable_path;
        this.reportProgress("Setting encoding file path to ", inputs.output_dir_path);
        let args = ["-c", exe + rung + " -p " + inputs.profile +" \"" + inputs.input_file_path + "\" \"" + inputs.input_metadata_path + "\" \"" + inputs.output_dir_path +"\" " ];
        this.reportProgress("Command line args", args);
        let tracker = this;
        let lastReported = null;
        try {
            let expectedFilePath = Path.join(inputs.output_dir_path, Path.basename(inputs.input_file_path).replace(/\.[^.]*$/,"")+".mp4");
            if (fs.existsSync(expectedFilePath)) {
                fs.renameSync(expectedFilePath, expectedFilePath + ".old");
            }
            
            var outsideResolve;
            var outsideReject;
            var commandExecuted = new Promise(function(resolve, reject) {
                outsideResolve = resolve;
                outsideReject = reject;
            });
            
            let buffer = "";                                                                                                                                                                                                                        
            var proc = spawn("stdbuf", ["-oL", "sh"].concat(args));
            
            proc.stdout.on('data', function(data) {                                                                                                                                                                                                 
                buffer += data.toString('utf8');                                                                                                                                                                                                    
                let lines = buffer.split('\n');                                                                                                                                                                                                     
                buffer = lines.pop();                                 
                lines.forEach(function(line) {
                    if (!line) return;
                    try {
                        let matcher = line.match(/Output : (.*\.mp4)/);
                        if (matcher) {                                                                                                                                                                                                              
                            outputs.output_file_path = matcher[1];
                        }                                                                                                                                                                                                                           
                        tracker.ReportProgress("Stdout " + line);     
                    } catch(err) {
                        tracker.ReportProgress("Stdout err" + line, err);
                    }                                                                                                                                                                                                                               
                });
            }); 
            
            proc.stderr.setEncoding("utf8")
            proc.stderr.on('data', function(data) {                
                let now = new Date().getTime();
                if (!lastReported || (lastReported + 5000 <  now)) {
                    tracker.ReportProgress("Encoding " + data.trim());
                    lastReported = now;
                }
            });
            
            proc.on('close', function(executionCode) {
                outsideResolve(executionCode);
                tracker.ReportProgress("Command executed");
            });
            
            outputs.execution_code = await commandExecuted;
            if (outputs.execution_code == 0) {
                this.ReportProgress("Encoding complete");
                if (!outputs.output_file_path) {
                    this.reportProgress("Using calculated path", expectedFilePath);
                    outputs.output_file_path = expectedFilePath;
                }
                
            } else {
                throw Error("Encoding returned exec code: " +  outputs.execution_code)
            }
        } catch(error) {
            this.Error("Execution failed", error)
            return ElvOAction.EXECUTION_EXCEPTION
        }
        return 100;
    }
    
    
    
    async executeExtract3dDolbyVisionFrames(inputs, outputs) {
        let exe = this.Payload.parameters.extract_frames_executable_path;
        let args = ["-c", exe + " \"" + inputs.left_eye_file_path + "\" \"" + inputs.right_eye_file_path + "\" \"" + inputs.output_dir_path + "\""];
        this.reportProgress("Command line args", args);
        let tracker = this;
        let lastReported = null;
        try {
            if (!fs.existsSync(inputs.output_dir_path)) {
                fs.mkdirSync(inputs.output_dir_path, {recursive: true});
                this.reportProgress("Created output dir", inputs.output_dir_path);
            }

            var outsideResolve;
            var outsideReject;
            var commandExecuted = new Promise(function(resolve, reject) {
                outsideResolve = resolve;
                outsideReject = reject;
            });

            let buffer = "";
            var proc = spawn("stdbuf", ["-oL", "sh"].concat(args));

            proc.stdout.on('data', function(data) {
                buffer += data.toString('utf8');
                let lines = buffer.split('\n');
                buffer = lines.pop();
                lines.forEach(function(line) {
                    if (!line) return;
                    try {
                        let fpsMatcher = line.match(/FPS\s*:\s*(\S+)/);
                        if (fpsMatcher) outputs.frame_rate = fpsMatcher[1];
                        let transferMatcher = line.match(/Transfer\s*:\s*(\S+)/);
                        if (transferMatcher) outputs.transfer = transferMatcher[1];
                        let primariesMatcher = line.match(/Primaries\s*:\s*(\S+)/);
                        if (primariesMatcher) outputs.primaries = primariesMatcher[1];
                        let now = new Date().getTime();
                        if (!lastReported || (lastReported + 5000 < now)) {
                            tracker.ReportProgress("Stdout " + line);
                            lastReported = now;
                        }
                    } catch(err) {
                        tracker.ReportProgress("Stdout err" + line, err);
                    }
                });
            });

            proc.stderr.setEncoding("utf8");
            proc.stderr.on('data', function(data) {
                let now = new Date().getTime();
                if (!lastReported || (lastReported + 5000 < now)) {
                    tracker.ReportProgress("Extracting " + data.trim());
                    lastReported = now;
                }
            });

            proc.on('close', function(executionCode) {
                outsideResolve(executionCode);
                tracker.ReportProgress("Command executed");
            });

            outputs.execution_code = await commandExecuted;
            if (outputs.execution_code == 0) {
                outputs.frames_dir_path = inputs.output_dir_path;
                this.ReportProgress("Frame extraction complete", outputs.frames_dir_path);
            } else {
                throw Error("Frame extraction returned exec code: " + outputs.execution_code);
            }
        } catch(error) {
            this.Error("Execution failed", error);
            return ElvOAction.EXECUTION_EXCEPTION;
        }
        return 100;
    }

    async executeEncode3dDolbyVisionFromFrames(inputs, outputs) {
        let rungMatcher = inputs.rung.match(/([0-9]+x[0-9]+).*@([0-9]+)/);
        let resolution = rungMatcher[1];
        let bitrate = rungMatcher[2]; // bps, as expected by run_pipeline.py --bitrate
        let exe = this.Payload.parameters.run_pipeline_executable_path;
        let transfer = inputs.transfer || "pq";
        let primaries = inputs.primaries || "p3d65";
        let args = [
            "-c", exe + " -f " + inputs.frame_rate + " -t " + transfer + " -p " + primaries
            + (inputs.tier ? " -r " + inputs.tier : "")  
            + (inputs.vbv_rate_bps ? " -V " + inputs.vbv_rate_bps : "")  
            + (inputs.vbv_buffer_bits ? " -B " + inputs.vbv_buffer_bits : "")  
            + " \"" + inputs.frames_dir_path + "\" \"" + inputs.input_metadata_path
            + "\" \"" + resolution + "\" \"" + bitrate + "\" \"" + inputs.output_dir_path + "\""
        ];
        this.reportProgress("Command line args", args);
        let tracker = this;
        let lastReported = null;
        try {
            if (!fs.existsSync(inputs.output_dir_path)) {
                fs.mkdirSync(inputs.output_dir_path, {recursive: true});
                this.reportProgress("Created output dir", inputs.output_dir_path);
            }
            let expectedFilePath = Path.join(inputs.output_dir_path, resolution + "@" + bitrate + "_dv20.mp4");

            var outsideResolve;
            var outsideReject;
            var commandExecuted = new Promise(function(resolve, reject) {
                outsideResolve = resolve;
                outsideReject = reject;
            });

            let buffer = "";
            var proc = spawn("stdbuf", ["-oL", "sh"].concat(args));

            proc.stdout.on('data', function(data) {
                buffer += data.toString('utf8');
                let lines = buffer.split('\n');
                buffer = lines.pop();
                lines.forEach(function(line) {
                    if (!line) return;
                    try {
                        let matcher = line.match(/Output : (.*\.mp4)/);
                        if (matcher) outputs.output_file_path = matcher[1];
                        let now = new Date().getTime();
                        if (!lastReported || (lastReported + 5000 < now)) {
                            tracker.ReportProgress("Stdout " + line);
                            lastReported = now;
                        }
                    } catch(err) {
                        tracker.ReportProgress("Stdout err" + line, err);
                    }
                });
            });

            proc.stderr.setEncoding("utf8");
            proc.stderr.on('data', function(data) {
                let now = new Date().getTime();
                if (!lastReported || (lastReported + 5000 < now)) {
                    tracker.ReportProgress("Encoding " + data.trim());
                    lastReported = now;
                }
            });

            proc.on('close', function(executionCode) {
                outsideResolve(executionCode);
                tracker.ReportProgress("Command executed");
            });

            outputs.execution_code = await commandExecuted;
            if (outputs.execution_code == 0) {
                this.ReportProgress("Encoding complete");
                if (!outputs.output_file_path) {
                    this.reportProgress("Using calculated path", expectedFilePath);
                    outputs.output_file_path = expectedFilePath;
                }
            } else {
                throw Error("Encoding returned exec code: " + outputs.execution_code);
            }
        } catch(error) {
            this.Error("Execution failed", error);
            return ElvOAction.EXECUTION_EXCEPTION;
        }
        return 100;
    }

    static REVISION_HISTORY = {
        "0.0.1": "2026-05-13 - Initial release for Dolby ATMOS encoding to MP4 only",
        "0.0.2": "2026-05-21 - Adds DV encode utility for 2D",
        "0.0.3": "2026-05-28 - Add automatic output directory creation for Dolby Vision",
        "0.0.4": "2026-05-28 - Look for Dolby Vision metadata at provided location and media source file directory",
        "0.1.0": "2026-05-29 - Adds action for 3D Dolby vision preparation",
        "0.2.0": "2026-05-29 - Adds EXTRACT_3D_DOLBY_VISION_FRAMES and ENCODE_3D_DOLBY_VISION_FROM_FRAMES actions",
        "0.2.1": "2026-06-08 - Adds Tier as parameter for ENCODE_3D_DOLBY_VISION_FROM_FRAMES",
        "0.2.2": "2026-06-09 - Adds vbv-rate-bps and vbv-buffer-bits as parameters for ENCODE_3D_DOLBY_VISION_FROM_FRAMES"
    };
    static VERSION = "0.2.2a"; 
}


if (ElvOAction.executeCommandLine(ElvOActionDolbyUtility)) {
    ElvOAction.Run(ElvOActionDolbyUtility);
} else {
    module.exports=ElvOActionDolbyUtility;
}
