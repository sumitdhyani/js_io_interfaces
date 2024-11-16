const createMiddlewareInterface             = require('./kafka/Engine')
const {tags, tagValues, topics, appGroups}  = require('../SystemValues')
const {uuidGen}                           = require('../UUidUtils')

function validateOutgoingMsg(msg) {
  if(undefined === msg[tags.message_type]) {
    throw new Error(`Mandatory tag ${tags.message_type} absent`)
  }
}

function msgHook(callback, hook) {
  return (msgObj) => {
    if(hook(msgObj) === false) {
      callback(msgObj)
    }
  }
}

function initCallback(middlewareInterface, err, appReqHandler, appId, logger, callback){
  let reqHandler = (null != appReqHandler)? appReqHandler :
  (msgObj, respSender) =>{
    respSender({[tags.message_type] : tagValues.message_type.dummy,
                [tags.errorDesc] : tagValues.errorDesc.not_a_responder},
               true,
               (err) => {
                 logger.error(`Error while sending response: ${err.message}`)
               })
  }

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
      if (topic === appId) {
        middlewareInterface.unsubscribe(appId, (err=>{
          if (!err) {
            middlewareInterface.subscribeAsIndividual(topic,
                                                      msgHook(appCallback, onDedicatedMsg),
                                                      errCallback)
          } else {
            errCallback(err)
          }
        }))
      } else {
        middlewareInterface.subscribeAsIndividual(topic,
                                                  msgHook(appCallback, onDedicatedMsg),
                                                  errCallback)
      }
    },

    subscribeAsGroupMember : middlewareInterface.subscribeAsGroupMember,

    unsubscribe : (topic, errCallback) => {
      middlewareInterface.unsubscribe(topic, (err) => {
        if(err) {
          errCallback(err)
          return
        }

        if (appId !== topic) {
          return
        }

        middlewareInterface.subscribeAsIndividual(appId, 
                                                  onDedicatedMsg,
                                                  errCallback)

      })
    },

    request : request
  }
  
  function onReq(msgObj, reqId, destTopic)
  {
    function respSender(responseObj, isLastResp, errCallback) {
      methodsForCallback.produce(destTopic,
                                 reqId,
                                 responseObj,
                                 {[tags.respId]: reqId, [tags.isLastResp] : isLastResp? "Y" : "N"},
                                 errCallback)
    }

    reqHandler(msgObj, respSender)
  }

  //Key: reqId, reapCallback
  const pendingReqStore = new Map()
  function onResp(msgObj, respId, isLastResp) {
    const respCallback = pendingReqStore.get(respId)
    if(undefined === respCallback) {
      logger.warn(`Response for unknown reqId: ${respId}, msg: ${msgObj.message}`)
    } else {
      respCallback(msgObj, isLastResp)
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
    pendingReqStore.set(reqId, (msgObj, isLastResp) => {
      respCallback(msgObj, isLastResp)
      pendingReqStore.delete(reqId)
    })
    methodsForCallback.produce(topic,
                               key,
                               msgObj,
                               {...headers, [tags.reqId] : reqId, [tags.destination_topic] : appId},
                               errCallback)
  }

  function onDedicatedMsg(msgObj) {
    const reqId = msgObj.headers[tags.reqId]
    const respId = msgObj.headers[tags.respId]
    const isLastResp = msgObj.headers[tags.isLastResp]
    let retVal = true
    if (undefined !== reqId) {
      onReq(msgObj, reqId.toString(), msgObj.headers[tags.destination_topic].toString())
    } else if (undefined !== respId) {
      logger.debug(`last rep recd., reqId: ${respId}`)
      onResp(msgObj, respId.toString(), isLastResp.toString() === "Y")
    } else {
      retVal = false
    }

    return false
  }

  if(err){
    callback(null, err)
  } else {
    middlewareInterface.subscribeAsIndividual(appId,
      (msgObj) => { 
        onDedicatedMsg(msgObj)
      },
      (err) => {
        if(!err){
          callback(methodsForCallback, null)
        } else {
          callback(null, err)
        }
      }
    )
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