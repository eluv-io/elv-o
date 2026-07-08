const logger = require('./o-logger');
const ElvOCmd = require("./o-cmd");

// for better stopping in container

process.on('SIGINT',  () => process.exit(0))
process.on('SIGTERM', () => process.exit(0))


const Run = async function() {
    let command = process.argv[2];
    await ElvOCmd.Run(command)
}


Run();
