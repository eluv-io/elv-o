const { ElvClient } = require("@eluvio/elv-client-js");
const ElvOMutex = require("./o-mutex");
const fs = require("fs");
const path = require('path');
const { execSync } = require('child_process');
const logger = require('./o-logger');
const fetch = require('node-fetch');
const URI = require("urijs");
const Ethers = require("ethers");



class ElvOFabricClient {
    
    setClient(configUrl, privateKey) {
        this.Client = ElvOFabricClient.InitializeClient(configUrl, privateKey)
    };
    
    report(...msg) {
        if (this.reportProgress) {
            this.reportProgress(...msg);
        }
    };
    
    sleep(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    };
    
    promiseState(p) {
        const t = {};
        return Promise.race([p, t])
        .then(v => (v === t)? "pending" : "fulfilled", () => "rejected");
    }
    
    static promiseState(p) {
        const t = {};
        return Promise.race([p, t])
        .then(v => (v === t)? "pending" : "fulfilled", () => "rejected");
    }
    
    getPrivateKey(client) {
        if (!client) {
            client = this.Client
        }
        return (client.signer.signingKey && client.signer.signingKey.privateKey.toString())
        || client.signer._signingKey().privateKey.toString() ;
    }
    
    getPublicKey(client) {
        if (!client) {
            client = this.Client
        }
        return (client.signer.signingKey && client.signer.signingKey.publicKey.toString())
        || client.signer._signingKey().publicKey.toString() ;
    }

    async markTimeout(timeout, options) {
        await this.sleep(timeout * 1000);
        options.expired = true;
    }
    
    async safeExec(fnStr, params, options) {
        let mutex;
        try {
            let startTimer = (new Date()).getTime();
            let result;
            let attempts = 0;
            let error;
            let client;
            let lastParam = params.length - 1;
            if (params[lastParam] && params[lastParam].client) {
                client = params[lastParam].client;
                //delete params.client;
            } else {
                client = this.Client
            }
            if (!options) {
                options = {timeout: 300};
            }
            let frPrnt = (fnStr.match(/CallContractMethodAndWait/)) ? ("CallContractMethodAndWait->" + params[0].methodName) : fnStr
            let maxAttempts = options && options.max_attempts;
            //let absoluteTimeout = options && options.absolute_timeout;
            let timeout = options && options.timeout;
            if (!params.timeout) {
                params.timeout = timeout * 1000;
            }
            while ((!maxAttempts || (attempts < maxAttempts)) && !(options && options.expired)) {
                if (mutex) {
                    ElvOMutex.ReleaseSync(mutex);
                    mutex = null;
                }
                try {
                    let result;
                    this.report(frPrnt + ": requesting Mutex");
                    let notifyer = this;
                    mutex = await ElvOMutex.WaitForLock({name: client.CurrentAccountAddress(), wait_timeout: timeout, hold_timeout: 60*1000, progress_notifyer: notifyer});
                    if (!mutex) {
                        this.report(frPrnt + ": Mutex wait timeout");
                        throw (new Error("Mutex wait timeout"));
                    }
                    this.report(frPrnt + ": Mutex acquired");
                    let execPromise = eval(fnStr + "(...params)");
                    if (timeout) {//null or 0
                        let execTimeout = this.markTimeout(timeout, options);
                        result = await Promise.race([execPromise, execTimeout]);
                        if (options.expired) {
                            this.report(frPrnt + ": exec-timeout at " + timeout + " sec");
                            throw "exec-timeout at " + timeout + " sec";
                        }
                    } else {
                        result = await execPromise;
                    }
                    ElvOMutex.ReleaseSync(mutex);
                    this.report(frPrnt + ": Mutex released");
                    let elapsed = (new Date()).getTime() - startTimer;
                    logger.Debug("Safe-exec " + frPrnt + "(" + attempts + ") in " + elapsed + " ms");
                    this.report(frPrnt + "(" + attempts + ") in " + elapsed + " ms");
                    
                    return result;
                } catch (err) {
                    if (mutex) {
                        ElvOMutex.ReleaseSync(mutex);
                        mutex = null;
                    }
                    if (err.message == "Mutex wait timeout") {
                        attempts++;
                        logger.Debug("Safe-exec Mutex wait timeout " + frPrnt + "(" + attempts + ")");
                        error = err;
                    } else {
                        if ((err.code == "NONCE_EXPIRED") || (err.code == "REPLACEMENT_UNDERPRICED")) {
                            attempts++;
                            logger.Debug("Safe-exec Nonce error " + frPrnt + "(" + attempts + ")", err.stack);
                            error = err;
                        } else {
                            if (err.toString().match(/Timed out waiting for completion of/)) {
                                attempts++;
                                logger.Debug("Safe-exec " + err.toString() + " (" + attempts + ")", err.stack);
                                error = err;
                            } else {
                                logger.Error("Safe-exec Error executing function " + frPrnt + "(" + attempts + ")", err);
                                throw err;
                            }
                        }
                    }
                }
                await this.sleep(100 + Math.floor(Math.random() * 900));
            }
            throw error;
        } catch(errCatchAll) {
            if (mutex) {
                ElvOMutex.ReleaseSync(mutex);
            }
            throw errCatchAll;
        }
    };
    
    
    async CallContractMethodAndWait(params)  {
        let options = {};
        if (params.timeout) {
            options.timeout = params.timeout / 1000;
        }
        if (!params.timeout) { //defaulting transaction timeout at 45 sec
            params.timeout = 45 * 1000;
        }
        return await this.safeExec("client.CallContractMethodAndWait", [params], options);
    };
    
    async SendFunds(params) {
        let client = (params.client) || this.Client;
        return await this.safeExec("client.SendFunds",[params], {timeout: 120});
    };
    
    async DeployContract(params) {
        let client = (params.client) || this.Client;
        return await this.safeExec("client.DeployContract",[params], {timeout: 120});
    };
    
    
    async CreateContentObject(params) { //{libraryId, options}
        let client = (params.client) || this.Client;
        return await this.safeExec("client.CreateContentObject", [params]);
    };
    
    //params are the same as the client.finalizeContentObject method with the addition of:
    // - maxAttempts: the maximum time finalization is attempted if not confirmCommit is received. Defaulted to 10
    // - timeout: the number of milliseconds to wait for confirmCommit to be received
    // - client: to provide the client to be used in case o is not running on target environment
    async FinalizeContentObject(params) {
        let objectHash, versionHash, objectId;
        try {
            if (params.attempt) {
                params.attempt++;
            } else {
                params.attempt = 1;
            }
            if (params.attempt > (params.maxAttempts || 10)) {
                let msg = "Could not finalize  " + (params.objectId || params.versionHash) + " after " + params.attempt;
                this.report(msg);
                throw Error(msg);
            }
            let client = (params.client) || this.Client;
            let originalPublish = (params.publish != false);
            params.publish = false;
            let response = await this.safeExec("client.FinalizeContentObject", [params]);
            if (!response || !response.hash) {
                let msg = "FinalizeContentObject did not return a valid version hash for " + params.objectId || params.versionHash;
                this.report(msg);
                throw Error(msg);
            }
            if (!originalPublish) {
                return response;
            }
            versionHash = response.hash;
            objectId = params.objectId || client.utils.DecodeVersionHash(versionHash).objectId;
            let contentObjectAddress = client.utils.HashToAddress(objectId);
            let commitError = false;
            let commit;
            let startCommit = (new Date()).getTime();
            //log time, write_token, commit.blockNumber, commit.tx_hash?,   versionhash, objAddress
            try {
                this.report("CommitContent",  {versionHash, writeToken: params.writeToken} );
                commit = await this.safeExec("client.ethClient.CommitContent", [{
                    contentObjectAddress,
                    versionHash,
                    signer: client.signer,
                    client
                }]); 
                this.report("Commit event ", {transactionHash: commit.transactionHash, blockNumber:  commit.blockNumber, blockHash: commit.blockHash});
                
            } catch (errCommit) {
                this.report("Commit did not report completion for " + versionHash, errCommit);
                logger.Info("Commit did not report completion for " + versionHash, errCommit);
                commitError = true;
            }
            if  (!commit) {
                let msg = "commitPending did not have a valid return";
                this.report(msg);
                throw Error(msg);
            }
            const abi = await client.ContractAbi({id: objectId});
            
            objectHash = await client.ExtractValueFromEvent({
                abi,
                event: commit,
                eventName: "CommitPending",
                eventValue: "objectHash"
            });
            const pendingHash = await client.CallContractMethod({
                contractAddress: contentObjectAddress,
                methodName: "pendingHash",
            });
            if (!pendingHash && commitError) {
                let msg = "Commit likely timed-out and no pending hash was found";
                this.report(msg);
                throw Error(msg);
            }
            if (pendingHash && objectHash && (pendingHash != objectHash)) {
                let msg = `Pending version hash mismatch on ${objectId}: expected ${objectHash}, currently ${pendingHash}`;
                this.report(msg);
                throw Error(msg);
            }
            
            const fromBlock = commit.blockNumber - 500;//we use 500 as the commit might have been resubmitted several time in safe-exec before getting here causing a lot of block elapsed
            let  pollingInterval = /*client.ethClient.Provider().pollingInterval ||*/ 500;
            // eslint-disable-next-line no-constant-condition
            let timeout = 45 * 60000;//params.timeout || 10000;
            //let loopsToTimeout = 20; //Math.ceil(timeout / pollingInterval);
            let loop = 0;
            let confirmEvent = false;
            let tryUntil = (new Date()).getTime() + timeout;
            while (((new Date()).getTime() <= tryUntil) && (!confirmEvent || confirmEvent.length == 0)) {
                this.report("Polling for event, attempt: "+ loop);
                loop++;
                await this.sleep(pollingInterval);
                const events = await client.ContractEvents({
                    contractAddress: contentObjectAddress,
                    abi,
                    fromBlock,
                    count: 1000 + loop * 100
                });
                logger.Peek("events", events);
                
                //confirmEvent = events.find(blockEvents =>blockEvents.find(event => (versionHash === (event && event.args && event.args[2])) && (event.name && event.name == "VersionConfirm")));
                confirmEvent = [];
                for (let blockEvents of events) {
                    for (let event of blockEvents) {
                        let eventName = event && event.name
                        if (eventName != "VersionConfirm") {
                            continue;
                        }
                        let eventObjectHash = (event.args && event.args[2]) || (event.values && event.values.objectHash);
                        if (eventObjectHash == versionHash) {
                            confirmEvent.push(event);
                        }
                    }
                }
                
                
                
                if (pollingInterval < 60000) { //lengthen polling interval up to a minute
                    pollingInterval = Math.min((pollingInterval * 2), 60000);
                }
            }
            if (confirmEvent && confirmEvent.length > 0) {
                logger.Debug("commit for "+pendingHash + " confirmed", (new Date()).getTime() - startCommit);
                this.report("commit for "+pendingHash + " confirmed", (new Date()).getTime() - startCommit);
                this.report("Confirm event ", {transactionHash: confirmEvent[0].transactionHash, blockNumber:  confirmEvent[0].blockNumber, blockHash: confirmEvent[0].blockHash});
                
                try {
                    //In case the transaction was submitted multiple times, commitPending should be canceled
                    let hangingPendingHash = await client.CallContractMethod({
                        contractAddress: contentObjectAddress,
                        methodName: "pendingHash",
                    });
                    if (hangingPendingHash == objectHash) {
                        this.report("The version was confirmed and an identical hash is still pending, canceling it...");
                        await client.CallContractMethodAndWait({
                            contractAddress: contentObjectAddress,
                            methodName: "clearPending",
                            methodArgs: []
                        });
                    }
                } catch(errHanging) {
                    this.report("Error in finalize hanging logic");
                    logger.Error("Error in finalize hanging logic", errHanging);
                }
                
                return response;
            } else {
                this.report("commit confirmation for "+pendingHash + " not found", (new Date()).getTime() - startCommit)
                throw Error("commit confirmation for "+pendingHash + " not found", (new Date()).getTime() - startCommit);
            }
        } catch (err) {
            logger.Error("Finalize error", err);
            this.report("Finalize error", err)
            if (versionHash) {
                params.force = true; //to clear existing commitPending
                params.options = {copyFrom: versionHash};
                let newWriteToken = await this.getWriteToken(params);
                logger.Info("Re-submitting " + versionHash + " for " + objectId);
                this.report("Re-submitting " + versionHash + " for " + objectId);
                params.commitMessage = "Re-submitting " + versionHash;
                return await this.FinalizeContentObject(params);
            } else {
                logger.Error("Missing objectHash can not Finalize", {objectId: params.objectId, writeToken: params.writeToken});
                this.report("Missing objectHash can not Finalize", {objectId: params.objectId, writeToken: params.writeToken});
                throw err;
            }
        }
    };
    
    
    async PublishContentObject(params) {
        let objectHash, versionHash, objectId;
        let client = (params.client) || this.Client;
        try {                
            versionHash = params.versionHash;
            objectId = params.objectId || client.utils.DecodeVersionHash(versionHash).objectId;
            let contentObjectAddress = client.utils.HashToAddress(objectId);
            let commitError = false;
            let commit;
            let startCommit = (new Date()).getTime();
            //log time, write_token, commit.blockNumber, commit.tx_hash?,   versionhash, objAddress
            try {
                this.report("CommitContent",  {versionHash, writeToken: params.writeToken} );
                commit = await this.safeExec("client.ethClient.CommitContent", [{
                    contentObjectAddress,
                    versionHash,
                    signer: client.signer,
                    client
                }]); 
                this.report("Commit event ", {transactionHash: commit.transactionHash, blockNumber:  commit.blockNumber, blockHash: commit.blockHash});
                
            } catch (errCommit) {
                this.report("Commit did not report completion for " + versionHash, errCommit);
                logger.Info("Commit did not report completion for " + versionHash, errCommit);
                commitError = true;
            }
            if  (!commit) {
                let msg = "commitPending did not have a valid return";
                this.report(msg);
                throw Error(msg);
            }
            const abi = await client.ContractAbi({id: objectId});
            
            objectHash = await client.ExtractValueFromEvent({
                abi,
                event: commit,
                eventName: "CommitPending",
                eventValue: "objectHash"
            });
            const pendingHash = await client.CallContractMethod({
                contractAddress: contentObjectAddress,
                methodName: "pendingHash",
            });
            if (!pendingHash && commitError) {
                let msg = "Commit likely timed-out and no pending hash was found";
                this.report(msg);
                throw Error(msg);
            }
            if (pendingHash && objectHash && (pendingHash != objectHash)) {
                let msg = `Pending version hash mismatch on ${objectId}: expected ${objectHash}, currently ${pendingHash}`;
                this.report(msg);
                throw Error(msg);
            }
            
            const fromBlock = commit.blockNumber - 500;//we use 500 as the commit might have been resubmitted several time in safe-exec before getting here causing a lot of block elapsed
            let  pollingInterval = /*client.ethClient.Provider().pollingInterval ||*/ 500;
            // eslint-disable-next-line no-constant-condition
            let timeout = 45 * 60000;//params.timeout || 10000;
            //let loopsToTimeout = 20; //Math.ceil(timeout / pollingInterval);
            let loop = 0;
            let confirmEvent = false;
            let tryUntil = (new Date()).getTime() + timeout;
            while (((new Date()).getTime() <= tryUntil) && (!confirmEvent || confirmEvent.length == 0)) {
                this.report("Polling for event, attempt: "+ loop);
                loop++;
                await this.sleep(pollingInterval);
                const events = await client.ContractEvents({
                    contractAddress: contentObjectAddress,
                    abi,
                    fromBlock,
                    count: 1000 + loop * 100
                });
                logger.Peek("events", events);
                
                confirmEvent = [];
                for (let blockEvents of events) {
                    for (let event of blockEvents) {
                        let eventName = event && event.name
                        if (eventName != "VersionConfirm") {
                            continue;
                        }
                        let eventObjectHash = (event.args && event.args[2]) || (event.values && event.values.objectHash);
                        if (eventObjectHash == versionHash) {
                            confirmEvent.push(event);
                        }
                    }
                }
                
                if (pollingInterval < 60000) { //lengthen polling interval up to a minute
                    pollingInterval = Math.min((pollingInterval * 2), 60000);
                }
            }
            if (confirmEvent && confirmEvent.length > 0) {
                logger.Debug("commit for "+pendingHash + " confirmed", (new Date()).getTime() - startCommit);
                this.report("commit for "+pendingHash + " confirmed", (new Date()).getTime() - startCommit);
                this.report("Confirm event ", {transactionHash: confirmEvent[0].transactionHash, blockNumber:  confirmEvent[0].blockNumber, blockHash: confirmEvent[0].blockHash});
                
                try {
                    //In case the transaction was submitted multiple times, commitPending should be canceled
                    let hangingPendingHash = await client.CallContractMethod({
                        contractAddress: contentObjectAddress,
                        methodName: "pendingHash",
                    });
                    if (hangingPendingHash == objectHash) {
                        this.report("The version was confirmed and an identical hash is still pending, canceling it...");
                        await client.CallContractMethodAndWait({
                            contractAddress: contentObjectAddress,
                            methodName: "clearPending",
                            methodArgs: []
                        });
                    }
                } catch(errHanging) {
                    this.report("Error in finalize hanging logic");
                    logger.Error("Error in finalize hanging logic", errHanging);
                }
                
                return commit;
            } else {
                this.report("commit confirmation for "+pendingHash + " not found", (new Date()).getTime() - startCommit)
                throw Error("commit confirmation for "+pendingHash + " not found", (new Date()).getTime() - startCommit);
            }
        } catch (err) {
            logger.Error("Publish error", err);
            this.report("Publish error", err)
            if (versionHash) {
                params.force = true; //to clear existing commitPending
                params.options = {copyFrom: versionHash};
                params.publish = true;
                let newWriteToken = await this.getWriteToken(params);
                logger.Info("Re-submitting " + versionHash + " for " + objectId);
                this.report("Re-submitting " + versionHash + " for " + objectId);
                params.commitMessage = "Re-submitting " + versionHash;
                return await this.FinalizeContentObject(params);
            } else {
                logger.Error("Missing objectHash can not Finalize", {objectId: params.objectId, writeToken: params.writeToken});
                this.report("Missing objectHash can not Finalize", {objectId: params.objectId, writeToken: params.writeToken});
                throw err;
            }
        }
    };    
    
    async EditAuthorizationToken({libraryId, objectId, client}) {
        if (!client) {
            client = this.Client;
        }
        /*
        let token = await this.safeExec("client.authClient.AuthorizationToken", [{
            libraryId,
            objectId,
            update: true,
            client
        }]);
        
        */
        let contractAddress = client.utils.HashToAddress(objectId);
        const event = await this.CallContractMethodAndWait({
            contractAddress,
            methodName: "updateRequest",
            methodArgs: [],
            client
        });
        const { isV3, accessType, abi } = await client.authClient.ContractInfo({id: objectId, address: contractAddress});
        const updateRequestEvent = client.ExtractEventFromLogs({
            abi,
            event,
            eventName: "UpdateRequest"
        });
        
        if (event.logs.length === 0 || !updateRequestEvent) {
            throw Error(`Update request denied for ${objectId}`);
        }
        
        let token = {
            qspace_id: client.authClient.contentSpaceId,
            addr: client.utils.FormatAddress(((client.signer && client.signer.address) || ""))
        };
        if (event.transactionHash) {
            token.tx_id = event.transactionHash;
        }
        if (libraryId) {
            token.qlib_id = libraryId;
        }
        token = client.utils.B64(JSON.stringify(token));
        
        const signature = await client.authClient.Sign(Ethers.utils.keccak256(Ethers.utils.toUtf8Bytes(token)));
        const multiSig = client.utils.FormatSignature(signature);
        let authToken = `${token}.${client.utils.B64(multiSig)}`;
        return authToken
    };
    
    async CallBitcodeMethod({
        libraryId,
        objectId,
        versionHash,
        writeToken,
        method,
        queryParams = {},
        body = {},
        headers = {},
        constant = true,
        nodeUrl,
        client,
        format = "json"
    }) {
        if (!client) {
            client = this.Client;
        }
        if(!method) { throw "Bitcode method not specified"; }
        
        if(versionHash) { objectId = this.utils.DecodeVersionHash(versionHash).objectId; }
        
        let path = "q" +"/" + (writeToken || versionHash || objectId) +  "/call/" + method;
        
        if (libraryId) {
            path = "qlibs/" +  libraryId + "/" + path;
        }
        
        let authHeader = headers.authorization || headers.Authorization;
        if(!authHeader) {
            if (constant) {
                headers.Authorization = (
                    await client.authClient.AuthorizationHeader({
                        libraryId,
                        objectId,
                        update: false
                    })
                    ).Authorization;
                } else {
                    let authorizationToken = await this.EditAuthorizationToken({
                        libraryId,
                        objectId,
                        client
                    });
                    headers.Authorization = "Bearer " + authorizationToken;
                }
            }
            let url = nodeUrl || (await this.getFabricUrl(client));
            logger.Debug("Authorization for "+ url, headers.Authorization);
            logger.Debug("path: " + path, queryParams);
            //client.ToggleLogging(true, {log: logger.Debug, error: logger.Error});
            /*return client.utils.ResponseToFormat(
                format,
                await client.HttpClient.Request({
                    url,
                    body,
                    headers,
                    method: constant ? "GET" : "POST",
                    path,
                    queryParams,
                    failover: false
                })
                );
                */
                let result = await ElvOFabricClient.fetchJSON(url + path /*+ "?"+queryParams*/, {
                    body,
                    headers,
                    method: constant ? "GET" : "POST"
                }
                );
                //client.ToggleLogging(false);
                return result;
            };
            
            async MergeMetadata({libraryId, objectId, writeToken, metadataSubtree = "", metadata = {}, client, nodeUrl, editAuthorizationToken}) {
                if (!client) {
                    client = this.Client;
                }
                let authorizationToken = editAuthorizationToken || await this.EditAuthorizationToken({
                    libraryId,
                    objectId,
                    client
                });
                let headers = {Authorization: "Bearer " + authorizationToken, "Content-Type": "application/json"};
                let url = nodeUrl || (await this.getFabricUrl(client));
                let path = "q/" + writeToken + "/meta/" + metadataSubtree;
                let result = await ElvOFabricClient.fetchJSON(url + path /*+ "?"+queryParams*/, {
                    body: metadata,
                    headers,
                    method: "POST"
                }
                );
                
            }
            
            
            
            
            async CreateEncryptionConk({libraryId, objectId, versionHash, writeToken, createKMSConk=true, client}) {
                if (!client) {
                    client = this.Client;
                }
                if(!objectId) {
                    objectId = client.DecodeVersionHash(versionHash).objectId;
                }
                if(!libraryId) {
                    libraryId = await this.getLibraryId(objectId, client);
                }
                const capKey = `eluv.caps.iusr${client.utils.AddressToHash(client.signer.address)}`;
                const existingUserCap =
                await this.getMetadata({
                    libraryId,
                    objectId,
                    writeToken,
                    metadataSubtree: capKey,
                    client
                });
                if(existingUserCap) {
                    client.encryptionConks[objectId] = await client.Crypto.DecryptCap(existingUserCap, this.getPrivateKey(client));
                } else {
                    client.encryptionConks[objectId] = await client.Crypto.GeneratePrimaryConk({
                        spaceId: client.contentSpaceId,
                        objectId
                    });
                    await client.ReplaceMetadata({
                        libraryId,
                        objectId,
                        writeToken,
                        metadataSubtree: capKey,
                        metadata: await client.Crypto.EncryptConk(client.encryptionConks[objectId], this.getPublicKey(client))
                    });
                }
                if(createKMSConk) {
                    try {
                        const kmsAddress = await client.authClient.KMSAddress({objectId});
                        const kmsPublicKey = (await client.authClient.KMSInfo({objectId})).publicKey;
                        const kmsCapKey = `eluv.caps.ikms${client.utils.AddressToHash(kmsAddress)}`;
                        const existingKMSCap =
                        await this.getMetadata({
                            libraryId,
                            // Cap may only exist in draft
                            objectId,
                            writeToken,
                            metadataSubtree: kmsCapKey,
                            client
                        });
                        if(!existingKMSCap) {
                            await client.ReplaceMetadata({
                                libraryId,
                                objectId,
                                writeToken,
                                metadataSubtree: kmsCapKey,
                                metadata: await client.Crypto.EncryptConk(client.encryptionConks[objectId], kmsPublicKey)
                            });
                        }
                    } catch(error) {
                        logger.Error("Failed to create encryption cap for KMS", error);
                        throw new Error("Failed to create encryption cap for KMS");
                    }
                }
                return client.encryptionConks[objectId];
            };
            
            
            
            
            
            async GetBalance(address, client) {
                if (!client) {
                    client = this.Client;
                }
                let ethereumURIs = client.ethClient.client.ethereumURIs;
                let body = JSON.stringify({
                    method: "eth_getBalance",
                    params:[address,"latest"],
                    id:54,
                    jsonrpc: "2.0"
                });
                for (let ethereumURI of ethereumURIs) {
                    try {
                        let result = await ElvOFabricClient.fetchJSON(ethereumURI, {
                            method: "post",
                            body,
                            headers: {'Content-Type': 'application/json'}
                        });
                        return parseFloat(client.utils.WeiToEther(result.result));
                    } catch(err) {
                        logger.Error("Could not query "+ ethereumURI, err);
                    }
                }
            };
            
            
            async getWriteToken(params, timeout) { //timeout in seconds
                let client = (params.client) || this.Client;
                if (!timeout) {
                    timeout = 10;
                }
                let previousPendingHash = null;
                let expiresAt = (new Date()).getTime() + timeout * 1000;
                while ( (new Date()).getTime() < expiresAt ) {
                    let pendingHash = await this.checkPending(params.objectId, params.force, client);
                    if (!pendingHash) {
                        return (await this.safeExec("client.EditContentObject", [params])).write_token;
                    } else {
                        if (previousPendingHash != pendingHash) {
                            if (previousPendingHash) {
                                logger.Info("Pending hash found different from the one previously encountered, resetting timeout...");
                                expiresAt = (new Date()).getTime() + timeout * 1000;
                            }
                            previousPendingHash = pendingHash;
                            await this.sleep(500);
                        }
                    }
                }
                logger.Error("ERROR: Can't process asset on pending commit for ", params.objectId);
                throw "Commit pending";
            };
            
            async checkPending(objId, clearPending, client) {
                try {
                    if (!client) {
                        client = this.Client;
                    }
                    let objectAddress = client.utils.HashToAddress(objId);
                    let pendingHash = await client.CallContractMethod({
                        contractAddress: objectAddress,
                        methodName: "pendingHash",
                        methodArgs: [],
                        cacheContract: false,
                        overrideCachedContract: true
                    });
                    if (pendingHash) {
                        logger.Error("WARNING: a commit was pending for " + objId);
                        if (clearPending) {
                            await this.safeExec("client.CallContractMethodAndWait", [{
                                //abi: ElvOFabricClient.BASE_CONTENT_ABI,
                                contractAddress: objectAddress,
                                methodName: "clearPending",
                                methodArgs: [],
                                client: client
                            }]);
                            logger.Info("WARNING: commit pending cleared for " + objId);
                            return null;
                        } else {
                            return pendingHash;
                        }
                    }
                    return null;
                } catch(err) {
                    logger.Error("Could not clear pending commit on "+ objId, err);
                    return null;
                }
            };
            
            static fetchJSON(url, options) {
                let stdout, cmd, status;
                if (!options) {
                    options = {};
                }
                if (!options.method) {
                    options.method = "GET";
                }
                if (!options.headers) {
                    options.headers = {};
                }
                try {
                    let headers = " ";
                    for (let header in options.headers) {
                        headers = headers + "-H \"" + header + ":" + options.headers[header] + "\" ";
                    }
                    let body = "";
                    let bodyFile;
                    if (options.body) {
                        bodyFile = "/tmp/post__"+url.replace(/\//g,"_")+"__"+ (new Date().getTime())
                        fs.writeFileSync(bodyFile, JSON.stringify(options.body));
                        body = " -d @'"+bodyFile+"' ";
                    }
                    cmd = "curl -s -X " + options.method + headers + body + "\""+url+"\"";
                    if (options.debug) {
                        logger.Debug("fetchJSON cmd", cmd);
                    }
                    stdout = execSync(cmd, {maxBuffer: 100 * 1024 * 1024}).toString();
                    if (options.debug) {
                        logger.Debug("fetchJSON stdout", stdout);
                    }
                    if (bodyFile) {
                        fs.unlinkSync(bodyFile);
                    }
                    status = stdout && JSON.parse(stdout);
                } catch(err) {
                    logger.Error("Error fetching "+ url, err);
                    if (cmd) {
                        logger.Error("cmd", cmd);
                    }
                    if (stdout) {
                        logger.Error("stdout", stdout);
                    }
                    throw err;
                }
                return status;
            };
            
            
            static async asyncfetchJSON(url, options) {
                try {
                    let fetchPromise = await fetch(url, options);
                    let result = await fetchPromise.json();
                    return result;
                } catch(err) {
                    logger.Error("fetchJSON failed for " + url, err);
                    throw err;
                }
            };
            
            static async getFabricUrls(client) {
                if (!this.FabricUrls) {
                    this.FabricUrls = {};
                }
                if (!this.FabricUrls[client.configUrl]) {
                    //let stdout = execSync("curl -s '" + client.configUrl + "'").toString();
                    let result = await ElvOFabricClient.fetchJSON(client.configUrl, {});
                    this.FabricUrls[client.configUrl] = result.network.seed_nodes.fabric_api;
                }
                return this.FabricUrls[client.configUrl];
            };
            
            async getFabricUrl(client) {
                if (client) {
                    let fabricUrl = (await ElvOFabricClient.getFabricUrls(client))[0];
                    return fabricUrl;
                }
                if (!this.FabricUrl) {
                    this.FabricUrl = (await ElvOFabricClient.getFabricUrls(this.Client))[0];
                }
                return this.FabricUrl;
            };
            
            async generateAuthToken(libId, objId, noAuth, client) {
                if (!client)  {
                    client = this.Client;
                }
                if (noAuth == null) {
                    noAuth = true;
                }
                let attempt = 0;
                while (true) {
                    try {
                        attempt++;
                        let token =  await client.authClient.AuthorizationToken({
                            libraryId: libId,
                            objectId: objId,
                            channelAuth: false,
                            noCache: true,
                            noAuth: noAuth
                        });
                        return token;
                    } catch(err) {
                        if ((attempt <= 5) && err.message && err.message.toLowerCase().match(/nonce/)) {
                            logger.Error(err.message, {attempt});
                            await this.sleep(attempt * 400)
                        } else {
                            throw err;
                        }
                    }
                }
            };
            
            
            async getLibraryToken(libId, client) {
                if (!client) {
                    client = this.Client;
                }
                if (!client.LibraryTokens) {
                    client.LibraryTokens = {};
                }
                if (!client.LibraryTokens[libId]) {
                    client.LibraryTokens[libId] = await this.generateAuthToken(libId, null, true, client);
                }
                return client.LibraryTokens[libId];
            };
            
            
            
            async getLibraryId(objectId, client) {
                try {
                    if (!client) {
                        client = this.Client;
                    }
                    if (!client.LibraryMap) {
                        client.LibraryMap = {};
                    }
                    if (!client.LibraryMap[objectId]) {
                        let libAddress = await client.CallContractMethod({
                            contractAddress: client.utils.HashToAddress(objectId),
                            methodName: "libraryAddress"                                
                        });
                        client.LibraryMap[objectId] = client.utils.AddressToLibraryId(libAddress);
                    }
                    return client.LibraryMap[objectId];
                } catch(err) {
                    logger.Error("Could not retrieve library ID for "+objectId, err);
                    return null;
                }
            };
            
            async getCustomContractAddress(params){
                let client = params.client || this.Client;
                if (!client.CustomContractMap) {
                    client.CustomContractMap = {};
                }
                if (!client.CustomContractMap[params.objectId]) {
                    client.CustomContractMap[params.objectId] = await client.CustomContractAddress(params);
                }
                return client.CustomContractMap[params.objectId];
            };
            
            async getContentTypeVersionHash(params) {
                let objectId = params.objectId;
                let client = params.client || this.Client;
                return  await client.CallContractMethod({
                    contractAddress: client.utils.HashToAddress(objectId),
                    methodName: "objectHash"
                });
            };
            
            
            async getVersionHash(params) {
                let objId = params.objectId;
                let libId = params.libraryId || (objId && await this.getLibraryId(objId, params.client));
                let url = (await this.getFabricUrl(params.client)) +"/qlibs/" + libId + "/q/" + objId +"?resolve=false";
                let token = await this.getLibraryToken(libId, params.client);
                //logger.Debug("curl -s '" + url + "' -H 'Authorization: Bearer " + token + "'");
                //let stdout = execSync("curl -s '" + url + "' -H 'Authorization: Bearer " + token + "'", {maxBuffer: 100 * 1024 * 1024}).toString();
                let result = await ElvOFabricClient.fetchJSON(url, {headers: { 'Authorization': "Bearer " + token }});
                if (result && result.errors && result.errors.length > 0) {
                    return null;
                }
                return result.hash;
            };
            
            async ContentObjectMetadata(params) {
                return this.getMetadata(params)
            };
            
            async getMetadata(params) {
                let client = params.client || this.Client;
                let objId = params.objectId;
                let libId = params.libraryId || (await this.getLibraryId(objId, client));
                let version = params.versionHash;
                let resolve = (params.resolve == false) ? "false" : "true";
                let writeToken = params.writeToken;
                let timeoutms = (params.timeout || 30) * 1000;
                if (writeToken && !params.node_url) {
                    if (client.HttpClient.draftURIs[writeToken]) {
                        params.node_url = "https://" + client.HttpClient.draftURIs[writeToken].hostname() + "/";
                    }
                }
                let metadataSubtree = encodeURI(params.metadataSubtree ? ("/" + params.metadataSubtree) : "");
                let selectBranches = encodeURI((params.selectBranches && (params.selectBranches.length > 0)) ? "&select=" + params.selectBranches.join("&select=") : "");
                let removeBranches = encodeURI((params.removeBranches && (params.removeBranches.length > 0)) ? "&remove=" + params.removeBranches.join("&remove=") : "");
                let nodeUrls= (params.node_url) ? [params.node_url] : (await ElvOFabricClient.getFabricUrls(client)).map(function(item){return item + "/"});
                for (let nodeUrl of nodeUrls) {
                    let url = nodeUrl + "qlibs/" + libId + "/q/" + (version || writeToken || objId) + "/meta" + metadataSubtree + "?limit=20000&resolve=" + resolve + removeBranches + selectBranches;
                    let token = await this.getLibraryToken(libId, client);
                    //let stdout = execSync("curl -s '" + url + "' -H 'Authorization: Bearer " + token + "'", {maxBuffer: 100 * 1024 * 1024}).toString();
                    logger.Debug("curl -s '" + url + "' -H 'Authorization: Bearer " + token + "'");
                    let timeoutPromise = this.sleep(timeoutms).then(function () {
                        return "--OUT--"
                    });
                    let options = (params.options || {headers: {}});
                    if (!options.headers) {
                        options.headers = {};
                    }
                    options.headers['Authorization'] = "Bearer " + token;
                    let result = await Promise.race([timeoutPromise, ElvOFabricClient.fetchJSON(url, options)]);
                    //let result = await ElvOFabricClient.fetchJSON(url, {headers: { 'Authorization': "Bearer " + token }});
                    if (result != "--OUT--") {
                        if (result && result.errors && result.errors.length > 0) {
                            if (result.errors[0].kind != "item does not exist") {
                                logger.Error("Error fetching metadata "+ url, result.errors);
                                continue;
                            } else {
                                return null;
                            }
                        }
                        return result;
                    } else {
                        logger.Error("Timeout fetching metadata from node " + nodeUrl);
                    }
                }
                throw new Error("Could not retrieve metadata from any available node");
            };
            
            async getContentSpace(objectId, client) {
                if (!client) {
                    client = this.Client;
                }
                if (objectId.match(/^iq__/)) {
                    let objectAddress = client.utils.HashToAddress(objectId);
                    let spaceAddress = await client.CallContractMethod({
                        contractAddress: objectAddress,
                        methodName: "contentSpace",
                        methodArgs: []
                    });
                    return "ispc" + client.utils.AddressToHash(spaceAddress);
                }
                return null;
            };
            
            
            toBytes32(value) {
                if ((typeof value == "string") && value.match(/^0x/) && (value.length == 66)) {
                    let hexArray = value.replace(/^0x/,"").match(/../g);
                    let buf = Buffer.alloc(32);
                    for (let i=0; i < 32; i++) {
                        buf[i] = parseInt("0x"+hexArray[i]);
                    }
                    return buf;
                }
                logger.Error("Unknown bytes32 format",value, (typeof value));
                return value;
            };
            
            
            static async Configuration({
                configUrl,
                kmsUrls=[],
                region
            }) {
                try {
                    const uri = new URI(configUrl);
                    uri.pathname("/config");
                    
                    if(region) {
                        uri.addSearch("elvgeo", region);
                    }
                    
                    const fabricInfo = await this.fetchJSON(uri.toString());
                    // If any HTTPS urls present, throw away HTTP urls so only HTTPS will be used
                    const filterHTTPS = uri => uri.toLowerCase().startsWith("https");
                    
                    let fabricURIs = fabricInfo.network.services.fabric_api;
                    if(fabricURIs.find(filterHTTPS)) {
                        fabricURIs = fabricURIs.filter(filterHTTPS);
                    }
                    
                    let ethereumURIs = fabricInfo.network.services.ethereum_api;
                    if(ethereumURIs.find(filterHTTPS)) {
                        ethereumURIs = ethereumURIs.filter(filterHTTPS);
                    }
                    
                    let authServiceURIs = fabricInfo.network.services.authority_service || [];
                    if(authServiceURIs.find(filterHTTPS)) {
                        authServiceURIs = authServiceURIs.filter(filterHTTPS);
                    }
                    
                    const fabricVersion = Math.max(...(fabricInfo.network.api_versions || [2]));
                    
                    return {
                        nodeId: fabricInfo.node_id,
                        contentSpaceId: fabricInfo.qspace.id,
                        networkId: (fabricInfo.qspace.ethereum || {}).network_id,
                        networkName: ((fabricInfo.qspace || {}).names || [])[0],
                        fabricURIs,
                        ethereumURIs,
                        authServiceURIs,
                        kmsURIs: kmsUrls,
                        fabricVersion
                    };
                } catch(error) {
                    // eslint-disable-next-line no-console
                    console.error("Error retrieving fabric configuration:");
                    // eslint-disable-next-line no-console
                    console.error(error);
                    
                    throw error;
                }
            }
            
            static async FromConfigurationUrl({
                configUrl,
                region,
                trustAuthorityId,
                staticToken,
                ethereumContractTimeout=10,
                noCache=false,
                noAuth=false
            }) {
                const {
                    contentSpaceId,
                    networkId,
                    networkName,
                    fabricURIs,
                    ethereumURIs,
                    authServiceURIs,
                    fabricVersion
                } = await this.Configuration({
                    configUrl,
                    region
                });
                
                const client = new ElvClient({
                    contentSpaceId,
                    networkId,
                    networkName,
                    fabricVersion,
                    fabricURIs,
                    ethereumURIs,
                    authServiceURIs,
                    ethereumContractTimeout,
                    trustAuthorityId,
                    staticToken,
                    noCache,
                    noAuth
                });
                
                client.configUrl = configUrl;
                
                return client;
            }
            
            static async InitializeClient(configUrl, privateKey) {
                let client;
                try {
                    client = await this.FromConfigurationUrl({
                        configUrl: configUrl
                    });
                    if (!privateKey) {
                        logger.Error("ERROR: a private key must be provided");
                    }
                    const wallet = client.GenerateWallet();
                    const signer = wallet.AddAccount({privateKey});
                    await client.SetSigner({signer});
                    client.Signer = signer;
                    
                } catch(error) {
                    logger.Error("Could not initialize elv-client" ,error);
                    logger.Error("InitializeClient stack", new Error("InitializeClient failed"));
                }
                return client;
            };
            
            
            static NULL_ADDRESS = "0x0000000000000000000000000000000000000000";
            NULL_ADDRESS = ElvOFabricClient.NULL_ADDRESS;
            static PROD_CONFIG_URL = "https://main.net955305.contentfabric.io/config";
            static VERSION = "0.0.1";
        };
        
        
        module.exports=ElvOFabricClient;
