const { ElvClient } = require("@eluvio/elv-client-js");
const fs = require("fs");
const { execSync } = require('child_process');

let isPresentInArgv = function(argument) {
  let pattern = new RegExp("--"+argument);
  return (process.argv.join(" ").match(pattern) != null);
};

let getValueInArgv =function(argument) {
  for (let i=0; i < process.argv.length; i++) {
    let arg = process.argv[i];
    let pattern = new RegExp("--"+argument+"=");
    if (arg && arg.match(pattern)) {
      return arg.replace(pattern, "");
    }
  }
  return null;
};

let getValuesInArgv =function(argument) {
    let listValues = [];
    for (let i=0; i < process.argv.length; i++) {
        let arg = process.argv[i];
        let pattern = new RegExp("--"+argument+"=");
        if (arg && arg.match(pattern)) {
            listValues.push(arg.replace(pattern, ""));
        }
    }
    if (listValues.length == 1) {
        return listValues[0].split(/,/);
    } else {
        return listValues;
    }
};

let getValueInArg = function(argument, envVar, defaultedValue) {
    let inArgV = getValueInArgv(argument);
    if (!inArgV && envVar) {
        inArgV = process.env[envVar] || defaultedValue;
    }
    if ((inArgV == "undefined") || (inArgV == "null") || (inArgV == ""))  {
        inArgV = null;
        process.env[envVar] = "";
    } else {
        process.env[envVar] = inArgV;
    }
    return inArgV;
};

let Client, ConfigUrl, PrivateKey;




let InitializeClient = async function() {
    ConfigUrl = getValueInArg("config-url", "CONFIG_URL");
    Client = await ElvClient.FromConfigurationUrl({
        configUrl: ConfigUrl
    });
    PrivateKey = getValueInArg("private-key", "PRIVATE_KEY");
    if (!PrivateKey) {
        console.error("ERROR: a private key must be provided");
    }
    const wallet = Client.GenerateWallet();
    const signer = wallet.AddAccount({privateKey: PrivateKey});
    await Client.SetSigner({signer});
};

let persist = function(config) {
    let fullConfig;
    if (fs.existsSync("test_config.json")) {
        fullConfig = JSON.parse(fs.readFileSync("test_config.json", "utf8"));
    } else {
        fullConfig = {};
    }
    Object.assign(fullConfig, config);
    fs.writeFileSync("test_config.json", JSON.stringify(fullConfig, null, 2), 'utf8');
};

let retrieve = function(variable) {
    let fullConfig;
    if (fs.existsSync("test_config.json")) {
        fullConfig = JSON.parse(fs.readFileSync("test_config.json", "utf8"));
    } else {
        fullConfig = {};
    }
    return fullConfig[variable];
};

let Run = async function(items) {
    await InitializeClient();

    //create Workflow library
    let libId;
    if (items.includes("library")) {
        let d = new Date();
        let libName = "O Workflows " + ([d.getMonth() + 1, d.getDate(), d.getFullYear()].join("/"))
        libId = await Client.CreateContentLibrary({
            name: libName
        });
        console.log("Created library '" + libName + "': " + libId);
        persist({library: libId});
    } else {
        libId = getValueInArgv("library") || retrieve("library");
        console.log("Using library "+ libId);
    }
    //create an O object in that library
    let oId;
    if (items.includes("o-id")) {
        let oDesc = await Client.CreateAndFinalizeContentObject({
            libraryId: libId,
            options: {meta: {public: {name: "O"}}}
        });
        oId = oDesc.id;
        console.log("Created O host object: ", oId);
        persist({o_id: oId});
    } else {
        oId = getValueInArgv("o-id") || retrieve("o-id");
        console.log("Using O host object: ", oId);
    }


    //create a workflow object
    let testProbeWFId;
    if (items.includes("probe-workflow")) {
        let wfMeta = JSON.parse(fs.readFileSync("test_probe_workflow.json", "utf8"));
        let wfDesc = await Client.CreateAndFinalizeContentObject({
            libraryId: libId,
            options: {meta: wfMeta}
        });
        testProbeWFId = wfDesc.id;
        console.log("Created test workflow object: ", wfDesc.id);
        persist({probe_workflow_id: testProbeWFId});
    } else {
        testProbeWFId = getValueInArgv("probe-workflow-id") || retrieve("probe-workflow-id");
    }
    //instantiate O contract
    let oContract;
    if (items.includes("o-contract")) {
        let cmd = "node o.js instantiate-O --o-id=" + oId + " --verbose --force";
        let stdout = execSync(cmd, {maxBuffer: 100 * 1024 * 1024}).toString();
        console.log("Instantiate O contract", stdout);
        oContract = JSON.parse(stdout).o_contract;
        persist({o_contract: oContract});
    }

    //create an API key
    let apiKey;
    if (items.includes("api-key")) {
        let clientAddress = await Client.CurrentAccountAddress();
        let cmd = "node o.js make-api-key --o-id=" + oId + " --client-address=" + clientAddress;
        let stdout = execSync(cmd, {maxBuffer: 100 * 1024 * 1024}).toString();
        console.log("Create API-Key", stdout);
        apiKey = JSON.parse(stdout).api_key;
        persist({api_key: apiKey});
    } else {
        apiKey = getValueInArgv("api-key") || retrieve("api_key");
        console.log("Using API-Key: ", apiKey);
    }

    //authorize API key
    if (items.includes("authorize")) {
        let authorizedAddresses = retrieve("authorized_addresses") || [];
        let clientAddress = await Client.CurrentAccountAddress();
        if (!authorizedAddresses.includes(clientAddress)) {
            let cmd = "node o.js authorize --o-id=" + oId + " --client-address=" + clientAddress;
            let stdout = execSync(cmd, {maxBuffer: 100 * 1024 * 1024}).toString();
            console.log("Authorize API-Key", stdout);
            authorizedAddresses.push(clientAddress);
            persist({authorized_addresses: authorizedAddresses});
        }
    }

     //list queues
    if (items.includes("queues")) {
        let cmd = "node o.js list-queues";
        let stdout = execSync(cmd, {maxBuffer: 100 * 1024 * 1024}).toString();
        let queues =JSON.parse(stdout);
        let queuesInfo = {
            testing: {priority: 1000, name: "Testing", flag: "--active"},
            low_priority: {priority: 500, name: "Low priority", flag: "--active"},
            normal_priority: {priority: 100, name: "Normal priority", flag: "--active"},
            high_priority: {priority: 50, name: "High priority", flag: "--active"},
            urgent: {priority: 5, name: "Urgent", flag: "--active"}
        };
        for (let queue in queuesInfo) {
            if (!queues.includes(queue)) {
                let queueInfo = queuesInfo[queue];
                cmd = "node o.js create-queue --o-id=" + oId + " --queue-id=" + queue + " --name=\"" + queueInfo.name + "\" --priority=" + queueInfo.priority + " "+queueInfo.flag;
                stdout = execSync(cmd, {maxBuffer: 100 * 1024 * 1024}).toString();
                console.log("Create queue "+ queue, stdout);
            } else {
                console.log("Queue "+ queue + " already configured");
            }
        }
    }

};


if (process.argv.length > 2) {
    let items = getValuesInArgv("items");
    if (items[0] == "all") {
        items = ["library", "o-id","o-contract", "api-key",  "probe-workflow", "authorize", "queues"];
    }
    if (items[0] == "basic") {
        items = ["library", "o-id","o-contract", "api-key", "authorize", "queues"];
    }
  Run(items);
} else {
  console.error("Usage: node TestInstall.js --items=all --config-url=<url> --private-key=<key>")
    console.error("Usage: node TestInstall.js --items=<all|basic|library|o-id|o-contract|api-key|authorize|queues|workflow>  --config-url=<url> --private-key=<key>")
}