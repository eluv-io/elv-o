const ElvOAction = require("../o-action").ElvOAction;
const ElvOFabricClient = require("../o-fabric");
const ElvOMutex = require("../o-mutex");
const fs = require("fs");
const { execSync } = require("child_process");

class ElvOActionFabricIndexerQuery extends ElvOAction  {
    ActionId() {
        return "fabric_indexer_query";
    };
    
    Parameters() {
        return {
            parameters: {
                action: {
                    type: "string", required: true, 
                    values:["LIST_UPDATED_OBJECTS", "LIST_UPDATED_VERSIONS", "LIST_OBJECTS"]
                }
            }
        };
    };
    
    IOs(parameters) {
        let inputs = {
            psql_bin_path : {type: "string", required: false, default: "psql"},
            psql_server: {type: "string", required: false, default: "76.74.28.243"},//rep1.elv
            psql_password: {type: "password", required: true},
            psql_login: {type: "string", required: true},
            psql_port: {type: "numeric", required: false, default: 5432}
        };
        let outputs = {};
        if ((parameters.action == "LIST_UPDATED_OBJECTS")  || (parameters.action == "LIST_UPDATED_VERSIONS")) {
            inputs.libraries = {type: "array", required: false, default: null};
            inputs.date_from = {type: "date", required: false};
            inputs.date_to = {type: "date", required: false};
            outputs.updated_objects = {type: "object"};
            outputs.date_from = {type: "date"};
            outputs.date_to = {type: "date"};
        }
        if (parameters.action == "LIST_OBJECTS") {
            inputs.libraries = {type: "array", required: true};
            inputs.as_of_date = {type: "date", required: false, default: null};
            outputs.objects = {type: "array"};
            outputs.as_of_date = {type: "date"};
        }
        return {inputs, outputs};
    };
    
    
    async Execute(inputs, outputs) {
        switch(this.Payload.parameters.action) {
            case "LIST_OBJECTS": {
                return this.executeListObjects(inputs, outputs);
            }
            case "LIST_UPDATED_OBJECTS": {
                return this.executeListUpdatedObjects(inputs, outputs);
            }
            case "LIST_UPDATED_VERSIONS": {
                return this.executeListUpdatedVersions(inputs, outputs);
            }
            default: {
                throw new Error("Unsupported action " +this.Payload.parameters.action);
            }
        }
    };
    
    psql_date(date) {
        return date.toISOString().replace(/\....Z/, "+00").replace("T", " ");
    }

    executeListObjects(inputs, outputs) {
        let sql;
        let libraries = "ANY(ARRAY['" + inputs.libraries.join("','") + "'])";
        outputs.as_of_date = inputs.as_of_date || new Date();
        if (!inputs.as_of_date) {
            sql = "SELECT id,library_id  FROM contents WHERE deleted_block_number is null AND library_id=" +libraries ;    
        } else {
            let date = this.psql_date(inputs.as_of_date);
            //select id, min(time), max(time) from contents,blocks  where  library_id='ilib4BewNLq4tUU6X7PJnsWarxuTdE6G'  AND ((deleted_block_number = blocks.number ) or (created_block_number = blocks.number )) group by id having min(time) < '2023-03-04 21:46:11+00' AND max(time) > '2023-03-04 21:46:11+00'
            sql = "SELECT id,library_id FROM contents, blocks WHERE library_id=" +libraries + " AND ((deleted_block_number = blocks.number ) OR (created_block_number = blocks.number )) GROUP BY id  HAVING min(time) <= '"+date+"' AND ((min(time) = max(time)) OR (max(time) >= '"+date+"'))";
        }
        this.reportProgress("sql: " + sql);
        let cmd = inputs.psql_bin_path + " -U " +inputs.psql_login +" -h " + inputs.psql_server + " -p " + inputs.psql_port + " -d indexer -c \""+ sql +"\" ";
        let all = execSync(cmd, {env: {"PGPASSWORD": inputs.psql_password}, maxBuffer: 2* 1024 * 1024 * 1024}).toString();
        outputs.objects = [];
        let foundObjects = 0;
        for (let line of all.split(/\n/)) {
            let matcher = line.match(/(iq__[^ ]+).*(ilib[^ ]+)/);
            if (matcher) {
                outputs.objects.push({object_id: matcher[1], library_id: matcher[2]});
                foundObjects++;
            } else {
                console.log(line);
            }
        }
        if (foundObjects > 0) {
            return ElvOAction.EXECUTION_COMPLETE;
        } else {
            return ElvOAction.EXECUTION_FAILED;
        }
    };




    /*
    select max(to_char(blocks.time,'YYYYMMDDHH24MISS')::float - 20230720000000)  as range,  max(blocks.time) as latest,  max(blocks.time::text  || versions.hash) as latest_hash,   max(LPAD(GREATEST(-1 / (to_char(blocks.time,'YYYYMMDDHH24MISS')::float - 20230720000000), 0)::text,11, '0') || versions.hash),  content_id  from versions,blocks,contents where contents.id=versions.content_id and blocks.number=versions.created_block_number and  blocks.time < '2023-07-21 00:00:00+00' and library_id=ANY(ARRAY['ilib3srN4FM5iPQp8nf4wNvbjH9qScvD', 'ilib3691LecDh9yNyqKHpwXtmej8kS4v']) group by content_id having (max(to_char(blocks.time,'YYYYMMDDHH24MISS')::float - 20230720000000) >= 0);
    */
    executeListUpdatedObjects(inputs, outputs) {
        if (!inputs.date_to) {
            let now = new Date();
            inputs.date_to = new Date(now.toISOString().replace(/T.*/,"T00:00:00.000Z"));
        }
        if (!inputs.date_from) {
            inputs.date_from = new Date(inputs.date_to - 24 * 3600000);
        }
        outputs.date_from = inputs.date_from;
        outputs.date_to = inputs.date_to;
        let date_from = this.psql_date(inputs.date_from);
        let date_to = this.psql_date(inputs.date_to);
        let range_min = date_from.replace(/\+.*/,'').replace(/[^0-9]/g,'');
        let sql;
        if (!inputs.libraries || (inputs.libraries.size==0)) {
            //sql = "select versions.hash,content_id, blocks.time from versions,blocks where blocks.number=versions.created_block_number and blocks.time >= '"+ date_from+"' and blocks.time < '" + date_to +"' order by blocks.time desc"; 
            sql = "select max(to_char(blocks.time,'YYYYMMDDHH24MISS')::float - " +range_min+ ")  as range,  max(blocks.time) as latest,  max(blocks.time::text  || versions.hash) as latest_hash,   max(LPAD(round(GREATEST(-1e11 / (to_char(blocks.time,'YYYYMMDDHH24MISS')::float - " + range_min +"), 0))::text,11, '0') || versions.hash),  content_id  from versions,blocks where  blocks.number=versions.created_block_number and  blocks.time < '" + date_to+"'  group by content_id having (max(to_char(blocks.time,'YYYYMMDDHH24MISS')::float - " + range_min + ") >= 0)";    

        } else {
            let libraries = "ANY(ARRAY['" + inputs.libraries.join("','") + "'])";
            //sql = "select versions.hash,content_id, blocks.time from contents,versions,blocks where contents.id = versions.content_id and blocks.number=versions.created_block_number and blocks.time >= '"+ date_from+"' and blocks.time < '" + date_to +"' and library_id = "+ libraries +" order by blocks.time desc";           
            sql = "select max(to_char(blocks.time,'YYYYMMDDHH24MISS')::float - " +range_min+ ")  as range,  max(blocks.time) as latest,  max(blocks.time::text  || versions.hash) as latest_hash,   max(LPAD(round(GREATEST(-1e11 / (to_char(blocks.time,'YYYYMMDDHH24MISS')::float - " + range_min +"), 0))::text,11, '0') || versions.hash),  content_id  from versions,blocks,contents where contents.id=versions.content_id and blocks.number=versions.created_block_number and  blocks.time < '" + date_to+"' and library_id=" +libraries + " group by content_id having (max(to_char(blocks.time,'YYYYMMDDHH24MISS')::float - " + range_min + ") >= 0)";    
        }
        this.reportProgress("sql: " + sql);
        let cmd = inputs.psql_bin_path + " -U " +inputs.psql_login +" -h " + inputs.psql_server + " -p " + inputs.psql_port + " -d indexer -c \""+ sql +"\" ";
        let all = execSync(cmd, {env: {"PGPASSWORD": inputs.psql_password}, maxBuffer: 2* 1024 * 1024 * 1024}).toString();
        outputs.updated_objects = {};
        let updatedObjects = 0;
        for (let line of all.split(/\n/)) {
            let matcher = line.match(/(hq__[^ ]+).*?([^ ]+)(hq__[^ ]+).*(iq__[^ ]+)/);
            if (matcher) {
                outputs.updated_objects[matcher[4]] = {
                    lastest: matcher[1], 
                    previous: ((matcher[2] != '00000000000') ? matcher[3] :  null)
                };
                updatedObjects++;
            } else {
                console.log(line);
            }
        }
        if (updatedObjects > 0) {
            return ElvOAction.EXECUTION_COMPLETE;
        } else {
            return ElvOAction.EXECUTION_FAILED;
        }
    };


    executeListUpdatedVersions(inputs, outputs) {
        let date_from = this.psql_date(inputs.date_from);
        let date_to = this.psql_date(inputs.date_to);
        let sql;
        if (!inputs.libraries || (inputs.libraries.size==0)) {
            sql = "select versions.hash,content_id, blocks.time from versions,blocks where blocks.number=versions.created_block_number and blocks.time >= '"+ date_from+"' and blocks.time < '" + date_to +"' order by blocks.time desc"; 
        } else {
            let libraries = "ANY(ARRAY['" + inputs.libraries.join("','") + "'])";
            sql = "select versions.hash,content_id, blocks.time from contents,versions,blocks where contents.id = versions.content_id and blocks.number=versions.created_block_number and blocks.time >= '"+ date_from+"' and blocks.time < '" + date_to +"' and library_id = "+ libraries +" order by blocks.time desc";           
        }
        this.reportProgress("sql: " + sql);
        let cmd = inputs.psql_bin_path + " -U " +inputs.psql_login +" -h " + inputs.psql_server + " -p " + inputs.psql_port + " -d indexer -c \""+ sql +"\" ";
        let all = execSync(cmd, {env: {"PGPASSWORD": inputs.psql_password}, maxBuffer: 2* 1024 * 1024 * 1024}).toString();
        outputs.updated_objects = {};
        let updatedObjects = 0;
        for (let line of all.split(/\n/)) {
            let matcher = line.match(/(hq__[^ ]+).*(iq__[^ ]+)/);
            if (matcher) {
                if (!outputs.updated_objects[matcher[2]]) {
                    outputs.updated_objects[matcher[2]] = [matcher[1]];
                    updatedObjects++;
                } else {
                    outputs.updated_objects[matcher[2]].push(matcher[1]);
                }
            } 
        }
        if (updatedObjects > 0) {
            return ElvOAction.EXECUTION_COMPLETE;
        } else {
            return ElvOAction.EXECUTION_FAILED;
        }
    };

    static VERSION = "0.0.2";
    static REVISION_HISTORY = {
        "0.0.1": "Initial release",
        "0.0.2": "adds date to the outputs of list action"
    };
}


if (ElvOAction.executeCommandLine(ElvOActionFabricIndexerQuery)) {
    ElvOAction.Run(ElvOActionFabricIndexerQuery);
} else {
    module.exports=ElvOActionFabricIndexerQuery;
}