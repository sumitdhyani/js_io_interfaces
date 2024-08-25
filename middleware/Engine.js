const createMiddlewareInterface = require('./kafka/Engine')
const SystemValues              = require('../SystemValues')
const [tags, tagValues, topics, appGroups]  =
[SystemValues.tags, SystemValues.tagValues, SystemValues.topics, SystemValues.appGroups]

function validateOutgoingMsg(msg) {
  if(undefined === msg[tags.message_type]) {
    throw new Error(`Mandatory tag ${tags.message_type} absent`)
  }
}

function initCallback(middlewareInterface, err, callback){
  if(err){
    callback(null, err)
  } else {
    callback({...middlewareInterface,
              produce : (topic, key, msgObj, headers, errCallback)=> {
                try {
                  validateOutgoingMsg(msgObj)
                  middlewareInterface.produce(topic, key, JSON.stringify(msgObj), {...headers, [tags.message_type] : msgObj[tags.message_type]}, errCallback)
                } catch (err) {
                  errCallback(err)
                }
              }},
             null)
  }
}

module.exports.createMiddlewareInterface = 
(brokers,
  appId,
  appGroup,
  logger,
  heartbeatInterval,
  heartbeatTimeout,
  latencyMetricsOn,
  callback)=>
{
  createMiddlewareInterface.init(brokers,
    appId,
    appGroup,
    logger,
    heartbeatInterval,
    heartbeatTimeout,
    latencyMetricsOn,
    (middlewareInterface, err) => { initCallback(middlewareInterface, err, callback) })    
}