/**
 * HVAC Lambda version: 1.02
 */

const topic = process.env.topic_name;
const kafka = require('./kafkaProducer');
 
/**
* Kafka Status 
*/

const kafkaState = {
  CONNECTING: 0, CONNECTED: 1, DISCONNECTING: 2, DISCONNECTED: 3, ERROR: 4
}

/**
* Global Instance
*/

const kafkaObj = new kafka();

kafkaObj.initiateKafka();

kafkaObj.createProducer();

/**
* Private Instance
*/

var hvac_lambda_process = function() {
  this.event = {};
  this.context = {};
  this.thingName = '';
};

/**
 * 
 * @param {object} eventObj 
 * @param {object} context 
 * When lambda is invoked, Lambda application runs the handler method.
 * When the handler exits or returns a response, it becomes available to handle another event.
*/
exports.handler = async function(eventObj,context) {

  const promise = new Promise(async (resolve,reject)=> {
    console.log("Invoking the kafka",kafkaObj.kafkaStatus);

    if(kafkaObj.kafkaStatus === kafkaState.CONNECTING || kafkaObj.kafkaStatus === kafkaState.DISCONNECTING) {
      console.error(`Kafka is in ${kafkaObj.kafkaStatus} state. Rejecting the handle for next retry`);
      reject(Error("Kafka is in connecting/disconnecting state. Rejecting the handle for next retry"));
      return;
    }
    
    console.log(eventObj);

    var currentProcess = exports.createHVACLambdaObj();

    /*
    if(event.Records && event.Records[0].body){
      console.log('--Processing Failed message from SQS--',JSON.stringify(event));
      event = currentProcess.processFailedMsgFromSQS(event, reject);
    }
    */

    if(eventObj.Records) {
      let batchSize = eventObj.Records.length;
      console.log("Batch size: ", batchSize);
      let successfulRecords = 0;
      for( const record of eventObj.Records ) {
        let event = JSON.parse(record.body);
        if(event.state.reported && event.state.reported.status && event.state.reported.status.thermostat_mode){
          let eventData = event.state.reported.status
          eventData['mode'] = event.state.reported.status.thermostat_mode;
          delete eventData.thermostat_mode;
        }
        console.log("event data : "+JSON.stringify(event));
        console.log("context data : "+JSON.stringify(context));
  
        currentProcess.event = event;
        currentProcess.context = context;
  
        var thing_name = currentProcess.getThingName();
  
        if(thing_name === '') {
          console.error("No thing name found");
          reject(Error("No thing name found"));
          return;
        }
  
        currentProcess.thingName = thing_name;
  
        let result = await currentProcess.sendHvacData();
        if(result === 'success') {
          successfulRecords++;
          //console.log("Record success:", successfulRecords);
          if(successfulRecords == batchSize){
            console.log("success");
            resolve("success");
          }
        }
        else {
          await kafkaObj.disconnectProducer();
          console.log("Error", successfulRecords);
          reject(Error(result));
        };
      }

    }
  })
  return promise;
}

/**
 * 
 * @param {Object} event  
 * @returns the event object in desired format for further processing.
 */
 hvac_lambda_process.prototype.processFailedMsgFromSQS = function(event, reject){
  let eventData = JSON.parse(event.Records[0].body);
  if(eventData.ruleName && eventData.base64OriginalPayload){ //AWS IOT rule errorAction 
    let regExp = /\/update\/accepted/;
    if(regExp.exec(eventData.topic) !== null){
      return JSON.parse(Buffer.from(eventData.base64OriginalPayload, "base64").toString());
    
    } else{ //Failed event from AWS IOT error Action - mismatch in the rule (update/accepted)
      console.error("RuleMatch failure - Nothing to process");
      reject(null, 'RuleMatch failure - Nothing to process');
    }  
  } else { //Lambda retry failure
   return eventData;
  }
}


/**
 * 
 * @returns result to indicate the success state.
 * The below method will push the data to kafka cluster
 */

hvac_lambda_process.prototype.sendHvacData = async function () {

  let event = this.event;
  let thing_name = this.thingName;
  let result = '';

  try{
    //stateMachine is checked, to create a new producer obj
    if(kafkaObj.kafkaStatus === kafkaState.DISCONNECTED || kafkaObj.kafkaStatus === kafkaState.ERROR) 
      await kafkaObj.connectProducer();
    await kafkaObj.kafkaProducer.send({
          topic,
          messages: [ {key: thing_name, value:JSON.stringify(event)} ],
          timeout: 1000,
          retry: {
            retries: 0
    }})
    console.log("Message sent",event);
    result = "success";
  } catch (e) {
    console.error(`[example-producer] ${e.message}`, e);
    result = e;
  }

  return result;
}

/**
 * 
 * @returns thingName from the event object.
 */

hvac_lambda_process.prototype.getThingName = function () {
  var itemData = this.event;

  var thingName = '';

  if(itemData.state && itemData.state.reported && itemData.state.reported.thing_name) {

    thingName = itemData.state.reported.thing_name;

  } 
  return thingName;
};
  
exports.createHVACLambdaObj = function () {
  var Obj = new hvac_lambda_process();
  return Obj;
};

exports.module = hvac_lambda_process;
