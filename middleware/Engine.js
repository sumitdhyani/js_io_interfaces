const createMiddlewareInterface             = require('./kafka/Engine')
const {tags, tagValues, topics, appGroups}  = require('../SystemValues')
const {uuidGen}                           = require('../UUidUtils')

function validateOutgoingMsg(msg) {
  if(undefined === msg[tags.message_type]) {
    throw new Error(`Mandatory tag ${tags.message_type} absent`)
  }
}

function initCallback(middlewareInterface, err, appReqHandler, appId, logger, callback){
  const methodsForCallback = {...middlewareInterface,
    produce : (topic, key, msgObj, headers, errCallback)=> {
      try {
        validateOutgoingMsg(msgObj)
        middlewareInterface.produce(topic, key, JSON.stringify(msgObj), {...headers, [tags.message_type] : msgObj[tags.message_type]}, errCallback)
      } catch (err) {
        errCallback(err)
      }
    },

    subscribeAsIndividual : (topic, appCallback, errCallback) => {
      middlewareInterface.subscribeAsIndividual(topic,
        (msgObj) => { 
          onMsg(msgObj, appCallback)
        },
        errCallback
      )
    },

    subscribeAsGroupMember : (topic, appCallback, errCallback) => {
      middlewareInterface.subscribeAsGroupMember(topic, 
        (msgObj) => { 
          onMsg(msgObj, appCallback)
        },
        errCallback
      )
    },

    request : request
  }
  
  function onReq(msgObj, reqId, destTopic)
  {
    function respSender(responseObj, headers, errCallback) {
      methodsForCallback.produce(destTopic,
                                 reqId,
                                 responseObj,
                                 {...headers, [tags.respId]: reqId},
                                 errCallback)
    }

    appReqHandler(msgObj, respSender)
  }

  //Key: reqId, reapCallback
  const pendingReqStore = new Map()
  function onResp(msgObj, respId) {
    const respCallback = pendingReqStore.get(respId)
    if(undefined === respCallback) {
      logger.warn(`Response for unknown reqId: ${respId}, msg: ${msgObj.message}`)
    } else {
      respCallback(msgObj)
    }
  }

  function request(topic,
                   key,
                   msgObj,
                   headers,
                   respCallback,
                   errCallback)
  {
    const reqId = uuidGen()
    pendingReqStore.set(reqId, (msgObj) => {
      respCallback(msgObj)
      pendingReqStore.delete(reqId)
    })
    methodsForCallback.produce(topic,
                               key,
                               msgObj,
                               {...headers, [tags.reqId] : reqId, [tags.destination_topic] : appId},
                               errCallback)
  }

  function onMsg(msgObj, appCallback) {
    const reqId = msgObj.headers[tags.reqId]
    const respId = msgObj.headers[tags.respId]
    logger.debug(`reqId: ${reqId}, respId: ${respId}`)
    if (undefined !== reqId) {
      onReq(msgObj, reqId, msgObj.headers[tags.destination_topic])
    } else if (undefined !== reqId) {
      onResp(msgObj, respId)
    } else {
      appCallback(msgObj)
    }
  }

  if(err){
    callback(null, err)
  } else {
    callback(methodsForCallback, null)
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
  appReqHandler,
  callback)=>
{
  createMiddlewareInterface.init(brokers,
    appId,
    appGroup,
    logger,
    heartbeatInterval,
    heartbeatTimeout,
    latencyMetricsOn,
    (middlewareInterface, err) => { initCallback(middlewareInterface, err, appReqHandler, appId, logger, callback) })    
}