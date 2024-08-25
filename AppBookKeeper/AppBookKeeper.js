const {createMiddlewareInterface} = require('../middleware/Engine')
const {uuidGen}                   = require('../UUidUtils')
const SystemValues                = require('../SystemValues')

const [tags, tagValues, topics, appGroups]  =
[SystemValues.tags, SystemValues.tagValues, SystemValues.topics, SystemValues.appGroups]                    
const appId   = process.argv[3] + "_" + uuidGen()

function produceCallback(err) {
  if (err) {
    logger.error(`Error while trying to produce message, details : ${err.message}`)
  }
}

function consumeCallback(err) {
  if (err) {
    logger.error(`Error while trying to consume topic, details : ${err.message}`)
  }
}

function middlewareInitCallback(middlewareInterface, err) {
  if (err) {
    logger.error(`Error while initializiing middleware, details: ${err.message}, exiting...`)
    return
  }

  const appMap = new Map()
  const deathTimerMap = new Map()

  function sendDeathNotice (appId) {
    const msgObj = {
      [tags.message_type]   : tagValues.message_type.app_event,
      [tags.app_event_type] : tagValues.app_event_type.app_down,
      [tags.appId]          : appId
    }
    
    middlewareInterface.produce(topics.app_events,
                                appId,
                                msgObj,
                                {},
                                produceCallback)
  }

  function onHeartbeat(msgObj) {
    const dict = msgObj.deserializer(msgObj.message)
    const otherAppId = dict[tags.appId]
    const appDetails = appMap.get(otherAppId)
    if (otherAppId === appId) {
      return
    }
    else if (undefined === appDetails) {
      logger.info(`Recieved heartbeat for unregistered app: ${otherAppId}`)
      middlewareInterface.produce(otherAppId,
                                  tagValues.message_type.component_enquiry,
                                  {[tags.message_type] : tagValues.message_type.component_enquiry,
                                   [tags.destination_topic] : appId},
                                  {},
                                  (err) => {
                                    if(!err) {
                                      logger.info(`Sent Component enquiry to app: ${otherAppId}`)
                                    } else {
                                      logger.error(`Error while sending Component enquiry to app: ${otherAppId}, details: ${err.message}`)
                                    }
                                  }
                                )
    } else {
      logger.debug(`Recieved heartbeat for app: ${otherAppId}`)
      const appDeathimerId = deathTimerMap.get(otherAppId)
      clearTimeout(appDeathimerId)
      const heartbeatTimeout = dict[tags.heartbeatTimeout]
      deathTimerMap.set(otherAppId, setTimeoutForDeathFunction(otherAppId, heartbeatTimeout*1000))
    }
  }

  function setTimeoutForDeathFunction(otherAppId, timeout) {
    return setTimeout(()=>{
      logger.warn(`Sending app_down event for app: ${otherAppId}`)
      sendDeathNotice(otherAppId)
      appMap.delete(otherAppId)
      deathTimerMap.delete(otherAppId)}, timeout)    
  }

  function onComponentQuery(msgObj) {
    const dict = msgObj.deserializer(msgObj.message)
    const eqTags = dict[tags.component_query_eq]
    let results = null
    if( undefined === eqTags ) {
      results = Array.from(appMap).map(([appId, appDict]) =>{
        return appDict
      })
    } else {
      results = Array.from(appMap)
      .filter(([appId, appDict]) =>{
        let retVal = false
        eqTags.forEach(([tag, value]) => {
          const tagValueInDict = appDict[tag]
          retVal = retVal &&
                   tagValueInDict !== undefined &&
                   tagValueInDict === value
          if (!retVal) {
            return
          }
        })
        return retVal
      })
      .map(([appId, appDict]) => {
        return appDict
      })
    }

    const responseObj = {[tags.message_type] : tagValues.message_type.component_query_response,
                      [tags.component_query_results] : results
    }

    middlewareInterface.produce(dict[tags.destination_topic],
                                tagValues.message_type.component_enquiry_response),
                                responseObj,
                                produceCallback
  }
    
  function onIncomingMessage(msgObj) {
    const dict = msgObj.deserializer(msgObj.message)
    const msgType = dict[tags.message_type]
    const otherAppId = dict[tags.appId]
    if (msgType === tagValues.message_type.component_enquiry_response) {
      logger.info(`Received component info from app: ${otherAppId}`)
      if (appMap.has(otherAppId)) {
        return
      }

      appMap.set(otherAppId, dict)
      const heartbeatTimeout = dict[tags.heartbeatTimeout]
      logger.info(`Setting death timer for ${heartbeatTimeout} seconds`)
      deathTimerMap.set(otherAppId, setTimeoutForDeathFunction(otherAppId, heartbeatTimeout*1000))
    }
  }

  middlewareInterface.subscribeAsIndividual(topics.heartbeats,
                                            onHeartbeat,
                                            consumeCallback)

  middlewareInterface.subscribeAsIndividual(appId,
                                            onIncomingMessage,
                                            consumeCallback)

  middlewareInterface.subscribeAsGroupMember(topics.component_query,
                                             onComponentQuery,
                                             consumeCallback)
}

const brokers = process.argv[2].split(",")
const heartbeatInterval = parseInt(process.argv[4])
const heartbeatTimeout = parseInt(process.argv[5])

const logger = 
{ debug : (str)=> { console.log(str)},
  info : (str)=> { console.log(str)},
  warn : (str)=> { console.log(str)},
  error : (str)=> { console.log(str)}
}

createMiddlewareInterface(brokers,
  appId,
  appGroups.app_book_keeper,
  logger,
  heartbeatInterval,
  heartbeatTimeout,
  false,
  middlewareInitCallback)

