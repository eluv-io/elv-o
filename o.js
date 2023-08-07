const logger = require('./o-logger');
const ElvOCmd = require("./o-cmd");



const Run = async function() {
    let command = process.argv[2];
    await ElvOCmd.Run(command)
}


Run();