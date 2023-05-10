module.exports = function (RED) {
    "use strict";

    const { EventHubProducerClient } = require("@azure/event-hubs");

    function dlEventHubSend(config) {
        // Create the Node-RED node
        RED.nodes.createNode(this, config);
        var node = this;
        let connectionString = '';
        let connection = {
            
        };
        let entityName = '';
        let producerClient;

        node.on('input', async function (msg) {
            var DEBUG = config.debug;
            const batchOptions = { /*e.g. batch size*/ };
            try {

                node.status({
                    fill: 'grey',
                    shape: 'dot',
                    text: ''
                });

                //dynamical parametrization: connectionString
                connectionString = '';
                connectionString = node.credentials.connectionString;
                if (typeof msg.connectionString !== 'undefined' && typeof msg.connectionString === 'string' && msg.connectionString.length > 0) {
                    connectionString = msg.connectionString;
                }

                //TODO: check format
                /*
                (?<endpoint>\w*[e|E]ndpoint=[^;]+)
                Regex (?<endpoint>Endpoint=.+;)(?<sasname>SharedAccessKeyName=.+;)(?<saskey>SharedAccessKey=.+;)(?<entity>EntityPath=.+)
                Endpoint=sb://dpd-data.servicebus.windows.net/;SharedAccessKeyName=Test;SharedAccessKey=GQg/uDB68vXT2DoiOUbSQPJ+Gv5owAOKFibrQS5UZ7o=;EntityPath=test-daten
                Endpoint=sb://dpd-data.servicebus.windows.net/;SharedAccessKey=GQg/uDB68vXT2DoiOUbSQPJ+Gv5owAOKFibrQS5UZ7o=;EntityPath=test-daten
                FileEndpoint=sb://dpd-data.servicebus.windows.net/;SharedAccessKeyName=Test;SharedAccessKey=GQg/uDB68vXT2DoiOUbSQPJ+Gv5owAOKFibrQS5UZ7o=;EntityPath=test-daten
                BlobEndpoint=sb://dpd-data.servicebus.windows.net/;SharedAccessKeyName=Test;SharedAccessKey=GQg/uDB68vXT2DoiOUbSQPJ+Gv5owAOKFibrQS5UZ7o=;EntityPath=test-daten
                BlobEndpoint=https://storagesample.blob.core.windows.net;SharedAccessSignature=sv=2015-04-05&amp;sr=b&amp;si=tutorial-policy-635959936145100803&amp;sig=9aCzs76n0E7y5BpEi2GvsSv433BZa22leDOZXX%2BXXIU%3D
                BlobEndpoint=https://storagesample.blob.core.windows.net;FileEndpoint=https://storagesample.file.core.windows.net;SharedAccessSignature=sv=2015-07-08&sig=iCvQmdZngZNW%2F4vw43j6%2BVz6fndHF5LI639QJba4r8o%3D&spr=https&st=2016-04-12T03%3A24%3A31Z&se=2016-04-13T03%3A29%3A31Z&srt=s&ss=bf&sp=rwl
                DefaultEndpointsProtocol=[http|https];BlobEndpoint=myBlobEndpoint;FileEndpoint=myFileEndpoint;QueueEndpoint=myQueueEndpoint;TableEndpoint=myTableEndpoint;AccountName=myAccountName;AccountKey=myAccountKey;SharedAccessKeyName=Test;SharedAccessKey=GQg/uDB68vXT2DoiOUbSQPJ+Gv5owAOKFibrQS5UZ7o=;EntityPath=test-daten;Endpoint=www.google.de;                
                */

                //extract info from connectionstring
                const regex = new RegExp(/(?<endpoint>Endpoint=.+;)(?<sasname>SharedAccessKeyName=.+;)(?<saskey>SharedAccessKey=.+;)(?<entity>EntityPath=.+)/);
                let match = connectionString.match(regex);
                if (match && match.groups && Object.keys(match.groups).length) {
                    
                } else {
                    if(DEBUG) {
                        node.send(`${err}; entityName:${entityName}; connectionString:${connectionString}`);
                    }
                }

                //dynamical parametrization: entityName
                entityName = '';
                if (node.credentials) {
                    if (!node.credentials.entityName || node.credentials.entityName.length === 0) {
                        entityName = (connectionString.split(';').pop()).substring('EntityPath='.length);
                    } else {
                        entityName = node.credentials.entityName;
                    }
                }

                if (typeof msg.entityName !== 'undefined' && typeof msg.entityName === 'string' && msg.entityName.length > 0) {
                    entityName = msg.entityName;
                }
                 
                producerClient = new EventHubProducerClient(connectionString, entityName);
                node.log("connecting the producer client...");
            } catch(err) {
                if(DEBUG) {
                    node.send(`${err}; entityName:${entityName}; connectionString:${connectionString}`);
                }

                node.status({
                    fill: 'red',
                    shape: 'dot',
                    text: 'invalid connection string or path'
                });
                return null;
            }
            
            node.status({
                fill: 'yellow',
                shape: 'dot',
                text: "connecting..."
            });

            if(DEBUG === true) {
                node.warn("open the producerClient connection.");
                node.warn("producerClient object:");
                node.send(producerClient);
            }

            try {
                //create new batch with options
                var batch = await producerClient.createBatch(batchOptions);
                var msgJSON;
                node.log("create empty batch with following options:");
                node.log(batchOptions);
                if(DEBUG === true) {
                    node.warn("create empty batch.");
                    node.warn("batchOptions:");
                    node.send(batchOptions);
                }

                //transform string to JSON if necessary
                if (typeof (msg.payload) != "string") {
                    node.log("Is JSON");
                    msgJSON = msg.payload;
                } else {
                    node.log("Is String...converting");
                    //Converting string to JSON Object
                    msgJSON = JSON.parse(msg.payload);
                }

                //try to add an event to the batch
                const isAdded = batch.tryAdd({ body: msgJSON });
                node.log("try to add the following event to the batch:");
                node.log(JSON.stringify(msg.payload));
                if(DEBUG === true) {
                    node.warn("try to add message to the batch.");
                    node.warn("message content:");
                    node.send(msg);
                }

                if( isAdded === false ) {
                    var warnText = "Failed to add event to the batch. Possible information loss.";
                    node.warn(warnText);
                    node.log(warnText);
                    if(DEBUG === true) {
                        node.warn("adding failed.");
                    }
                }

                //send batch
                await producerClient.sendBatch(batch);
                node.log("sent batch to event hub.");
                node.status({
                    fill: 'blue',
                    shape: 'dot',
                    text: "sent message"
                });
                if(DEBUG === true) {
                    node.warn("sent the batch.");
                }

            } 
            catch (err) {
                node.log("Error when creating & sending a batch of events: ", err);
                node.status({
                    fill: 'red',
                    shape: 'dot',
                    text: "communication failed"
                });
                if(DEBUG === true) {
                    node.warn("got an unexpected error.");
                    node.warn("error object:");
                    node.send(err);
                }
            }
            //close connection
            await producerClient.close();
            node.log("Disconnect producer client.");
            node.status({
                fill: 'blue',
                shape: 'dot',
                text: "sent & closed"
            });
            if(DEBUG === true) {
                node.warn("close the producerClient connection.");
            }
        });
    }

    // Registration of the node into Node-RED
    RED.nodes.registerType("dlEventHubSend", dlEventHubSend, {
        defaults: {
            name: {
                value: "Send Message To Azure EventHub"
            }
        },
        credentials: {
            connectionString: {
                type: "text"
            },
            entityName: {
                type: "text"
            }
        }
    });
}
