const ElvOAction = require("../o-action").ElvOAction;
const fs = require("fs");
const path = require("path");



class ElvOActionLocalFileWatcher extends ElvOAction  {
    
    ActionId() {
        return "local_file_watcher";
    };
    
    Parameters() {
        return {
            "parameters": {
                action: {
                    type: "string", values:[
                        "FOLDER_WATCH", "ASPERA_MANIFEST_WATCH"
                    ]
                },
                one_off: {type: "boolean"},
                check_stability: {type: "boolean"}, 
                baseline: {type: "boolean"}
            }
        };
    };
    
    IsContinuous() {
        return false; //indicates that the execution stays within a single PID
    };
    
    IOs(parameters) {
        let inputs = {};
        let outputs= {found_item : "string", found_path : "string"};
        if (parameters.one_off == null) {
            inputs.one_off = {type: "boolean", required: true};
        }
        if (parameters.check_stability) {
            inputs.stability_cooloff_time = {type: "numeric", required: false, default: 60};
        }
        if (parameters.action == "FOLDER_WATCH") {
            inputs.watch_folder_path = {type: "string", required: true};
            inputs.archive_path = {type: "string", required: false, default: null};
            inputs.pattern = {type: "string", required: false, default: ".*"};
        }
        if (parameters.action == "ASPERA_MANIFEST_WATCH") {
            inputs.watch_folder_path = {type: "string", required: true};
            inputs.manifests_archive_path = {type: "string", required: true}; //without is more complicated, will do later
        }
        if (parameters.action == "ADI_MANIFEST_WATCH") {
            inputs.watch_folder_path = {type: "string", required: true};
            inputs.manifest_pattern = {type: "string", required: false, default: "ADI.XML"};
            inputs.archive_path = {type: "string", required: true}; //without is more complicated, will do later
        }
        return {inputs, outputs}
    };
    
    async Execute(inputs, outputs) {
        if (!fs.existsSync(this.resourcePath())) fs.mkdirSync(this.resourcePath(),{recursive: true});
        if (this.Payload.parameters.action == "FOLDER_WATCH") {
            return await this.executeWatchFolder(inputs, outputs);
        }
        if (this.Payload.parameters.action == "ASPERA_MANIFEST_WATCH") {
            return await this.executeWatchAsperaManifest(inputs, outputs);
        }
        if (this.Payload.parameters.action == "ADI_MANIFEST_WATCH") {
            return await this.executeWatchADIManifest(inputs, outputs);
        }
    }
    
    async executeWatchADIManifest(inputs, outputs) {
        if (!fs.existsSync(inputs.watch_folder_path)) {
            throw "Watcher folder not found at "+inputs.watch_folder_path;
        }
        if (!fs.lstatSync(inputs.watch_folder_path).isDirectory()) {
            throw "Watcher file found at watch folder path is not a directory";
        }
        if (!fs.existsSync(inputs.archive_path)) {
            throw "Manifests archive not found at "+inputs.archive_path;
        }
        if (!fs.lstatSync(inputs.archive_path).isDirectory()) {
            throw "Manifests archive found at watch folder path is not a directory";
        }
        
        return await this.watchForADIManifest(process.pid, outputs);
    }
    
    async MonitorExecution(pid, outputs) {         
        let inputs = this.Payload.inputs; 
        let oneOff = this.Payload.parameters.one_off || inputs.one_off;
        if (this.Payload.parameters.action == "ADI_MANIFEST_WATCH") {
            return await this.watchForADIManifest(pid, outputs);
        }
        if (this.Payload.parameters.action == "FOLDER_WATCH") {
            try {
                let candidate = this.scanWatchFolder(inputs, outputs, "FOLDER");
                if (!candidate) {
                    return ElvOAction.EXECUTION_ONGOING;
                }
                outputs.found_item = candidate.item;
                let archivePath = inputs.archive_path;
                if (archivePath) {
                    let fileName = candidate.item;
                    let targetPath = path.join(archivePath, fileName);
                    fs.renameSync(candidate.path, targetPath);
                    outputs.found_path = targetPath;
                } else {
                    this.touchbase(candidate.item);
                    outputs.found_path = candidate.path
                }
                
                return oneOff ? ElvOAction.EXECUTION_COMPLETE : ElvOAction.EXECUTION_COMPLETE_TO_BE_CLONED;
            } catch(err) {
                this.Error("Error scanning", err);
            }
        }
        throw "Unsupported action "+ this.Payload.parameters.action;
    }
    
    async watchForADIManifest(pid, outputs) {
        let inputs = this.Payload.inputs;
        let oneOff = inputs.one_off || this.Payload.parameters.one_off;
        let fileNames = fs.readdirSync(inputs.watch_folder_path);
        for (let dirName of fileNames) {
            let sourceDir = path.join(inputs.watch_folder_path, dirName);
            if (!fs.lstatSync(sourceDir).isDirectory()) {
                continue;
            }
            let adiPath = path.join(sourceDir, inputs.manifest_pattern); //to do - could be a regexp
            if (fs.existsSync(adiPath)) {
                this.reportProgress("Manifest found", adiPath);
                let targetDir = path.join(inputs.archive_path, dirName);
                this.reportProgress("Moving found folder", {source: sourceDir, targetDir: targetDir});
                fs.renameSync(sourceDir, targetDir);
                outputs.manifest_path = adiPath;
                outputs.folder_path = targetDir;
                return oneOff ? ElvOAction.EXECUTION_COMPLETE : ElvOAction.EXECUTION_COMPLETE_TO_BE_CLONED;
            } 
        }
        return ElvOAction.EXECUTION_ONGOING;
    }
    
    
    async executeWatchAsperaManifest(inputs, outputs) {
        let archivePath = inputs.archive_path;
        if (!fs.existsSync(inputs.watch_folder_path)) {
            throw "Watcher folder not found at "+inputs.watch_folder_path;
        }
        if (!fs.lstatSync(inputs.watch_folder_path).isDirectory()) {
            throw "Watcher file found at watch folder path is not a directory";
        }
        if (!fs.existsSync(inputs.manifests_archive_path)) {
            throw "Manifests archive not found at "+inputs.manifests_archive_path;
        }
        if (!fs.lstatSync(inputs.manifests_archive_path).isDirectory()) {
            throw "Manifests archive found at watch folder path is not a directory";
        }
        if (this.Payload.parameters.baseline) {
            let dirPath =inputs.watch_folder_path;
            let pattern = inputs.pattern;
            
            let fileNames = fs.readdirSync(dirPath);
            for (let fileName of fileNames) {
                if (!fileName.match(/^aspera-transfer-.*.manifest.txt$/)) continue;
                let filePath = path.join(dirPath, fileName);
                let targetPath = path.join(inputs.manifests_archive_path, fileName);
                fs.renameSync()
            }
        }
    }
    
    async executeWatchFolder(inputs, outputs) {
        let archivePath = inputs.manifests_archive_path;
        if (!fs.existsSync(inputs.watch_folder_path)) {
            throw "Watcher folder not found at "+inputs.watch_folder_path;
        }
        if (!fs.lstatSync(inputs.watch_folder_path).isDirectory()) {
            throw "Watcher file found at watch folder path is not a directory";
        }
        let baseline = this.Payload.parameters.baseline
        if (!archivePath || baseline) {           
            if (baseline) {
                this.baseline();
            }
        }
        
        return await this.MonitorExecution(process.pid, outputs);
        
    }
    
    scanWatchFolder(inputs, ouputs, watchType) {
        let coolOffTime = inputs.stability_cooloff_time;
        let dirPath =inputs.watch_folder_path;
        let pattern = inputs.pattern;
        
        let fileNames = fs.readdirSync(dirPath);
        for (let fileName of fileNames) {
            if (!fileName.match(pattern) || this.isBased(fileName)) continue;
            let filePath = path.join(dirPath, fileName);
            if ((watchType == "FOLDER") && !fs.lstatSync(filePath).isDirectory()) continue;
            if ((watchType != "FOLDER") && fs.lstatSync(filePath).isDirectory()) continue;
            if (this.isStable(filePath, coolOffTime)) {
                return {path: filePath, item: fileName};
            }
        }
    }
    
    touchbase(filename) {
        let filepath = path.join(this.resourcePath(), this.flatten(filename));
        fs.closeSync(fs.openSync(filepath, 'a'));
        return filepath;
    }
    
    isBased(filename) {
        let filepath = path.join(this.resourcePath(), this.flatten(filename));
        return fs.existsSync(filepath);
    }
    
    baseline() {
        let fileNames = fs.readdirSync(dirPath);
        for (let fileName of fileNames) {
            if (!fileName.match(pattern)) continue;            
            this.touchbase(filename);           
        }
    }
    
    
    
    isStable(filePath, coolOffTime) {
        if (!coolOffTime) return true;
        if (fs.lstatSync(filePath).isDirectory()) {
            return this.isFolderStable(filePath, coolOffTime);
        } else {
            return this.isFileStable(filePath, coolOffTime);
        }
    }
    
    isFolderStable(dirPath, coolOffTime) {
        let fileNames = fs.readdirSync(dirPath);
        for (let fileName of fileNames) {
            let filePath = path.join(dirPath, fileName);
            if (!this.isFileStable(filePath, coolOffTime)) return false;
        }
        return true;
    }
    
    isFileStable(filePath, coolOffTime) {
        console.log("isFileStable", filePath, coolOffTime);
        let stats = fs.lstatSync(filePath);
        let info = {
            size: stats.size,
            mtimeMs: stats.mtimeMs,
            timestamp: (new Date()).getTime()
        }
        let infoTracker = this.Tracker[stats.ino + 1000]; //add 1000 to avoid collision with internal codes
        let pastInfo = infoTracker?.details;
        if (!pastInfo) {
            this.trackProgress(stats.ino + 1000, filePath, info );
            return false;     
        } else {
            if ((pastInfo.size == info.size) && (pastInfo.mtimeMs == info.mtimeMs)) {
                if (pastInfo.timestamp + coolOffTime * 1000 < info.timestamp) {
                    return true
                } 
            } else {
                this.trackProgress(stats.ino + 1000, filePath, info );
                return false;
            }
        }
    };
    
    
    hashPath(filePath) {                                                                                                                                                                                                   
        let hash = 5381;                                                                                                                                                                                                        
        for (let i = 0; i < filePath.length; i++) {                                                                                                                                                                                   
            hash = (hash * 33) ^ filePath.charCodeAt(i);                                                                                                                                                                                
        }                                                                                                                                                                                                                         
        return hash >>> 0; // convert to unsigned 32-bit integer                                                                                                                                                                  
    }; 
    
    flatten(filepath) {
        return filepath.replace(/\//g,"__").replace(/[^0-9a-zA-Z_]/g,"-");
    }
    
    resourcePath(){
        if (!this.BASELINER_PATH) {
            this.BASELINER_PATH = path.join(ElvOAction.RESOURCES_ROOT, this.ActionId(), this.flatten(this.Payload.inputs.watch_folder_path));
        }
        return this.BASELINER_PATH;
    }
    
    static VERSION = "0.0.1";
}




if (ElvOAction.executeCommandLine(ElvOActionLocalFileWatcher)) {
    ElvOAction.Run(ElvOActionLocalFileWatcher);
} else {
    module.exports=ElvOActionLocalFileWatcher;
}

