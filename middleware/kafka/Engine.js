const CreateKafkaInterface = require('./LibWrapper')
const SystemValues = require('../../SystemValues')
async function init(brokers,
              appId,
              appGroup,
              logger,
              heartbeatInterval,
              heartbeatTimeout,
              latencyMetricsOn)
{
  const [tags, tagValues, topics] = [SystemValues.tags, SystemValues.tagValues, SystemValues.topics]
  const { produce, subscribeAsGroupMember, subscribeAsIndividual, unsubscribe, createTopic } = 
  await CreateKafkaInterface.createKafkaLibrary(brokers,
                                                appId,
                                                appGroup,
                                                logger)

  

  //Create self topic
  let selfTopicCreated = false
  while (selfTopicCreated === false) {
    try {
      await createTopic(appId, 1, 2)
      selfTopicCreated = true
    } catch(err) {
      switch (err.type) {
      case 'TOPIC_ALREADY_EXISTS':
        selfTopicCreated = true
      default:
        logger.error(`Error while creating self topic: ${appId}, details ${err.name}:${err.message}:${JSON.stringify(err.errors)}, retrying...`)
        await new Promise(r => setTimeout(r, 5000))
      }
    }
  }
  logger.info(`Created app topic: ${appId}`)

  //Subscribe to self topic
  const comp_enquiry_resp = JSON.stringify({
    [tags.message_type] : tagValues.message_type.component_enquiry_response,
    [tags.appId] : appId,
    [tags.appGroup] : appGroup,
    [tags.heartbeatInterval] : heartbeatInterval,
    [tags.heartbeatTimeout] : heartbeatTimeout })
  async function onDedicatedMsg(msgObj) {
    const msgDict = JSON.parse(msgObj.message)
    const msgType = msgDict[tags.message_type]
    const retVal = true
    if (0 === msgType.localeCompare(tagValues.message_type.component_enquiry)) {
      const destTopic = msgDict[tags.destination_topic]
      logger.debug(`Recieved component enquiry, destination topic: ${destTopic}`)
      try {
        await produce(destTopic, topics.component_query, comp_enquiry_resp, {})
        logger.debug(`Send component enquiry response to topic: ${destTopic}`)
      } catch(err) {
        logger.debug(`Error while sending component enquiry response to topic: ${destTopic}, details: ${err.message}`)
      }
    } else {
      retVal = false
    }
    
    return retVal
  }

  let selfTopicSubscribed = false
  while (!selfTopicSubscribed) {
    try {
      await subscribeAsIndividual(appId, onDedicatedMsg)
      selfTopicSubscribed = true
    } catch(err) {
      logger.error(`Error while subscribing self topic: ${appId}, retrying...`)
    }
  }
  logger.info(`Self topic, ${appId} subscribed`)

  const heartbestMsg = JSON.stringify({[tags.message_type]: tagValues.message_type.heartbeat,
    [tags.appId] : appId,
    [tags.heartbeatTimeout] : heartbeatTimeout})

  await produce(topics.heartbeats, appGroup, heartbestMsg, {})
  logger.info(`Sent 1st heartbeat, app: ${appId}`)

  setInterval(()=>{
    produce("heartbeats", appGroup, heartbestMsg, {})
    .then(()=>{})
    .catch((err)=>{
      logger.error(`Error while sending heartbeat for app: ${appId}, details: ${err.message}`)
    })},
    heartbeatInterval*1000)
  
    return {
      produce: produce,
      
      subscribeAsGroupMember : async (topic, dataCallback)=>{
        if (topic === appId) {
          throw new Error(`Can't subscribe self topic as individual`)
        } else {
          await subscribeAsGroupMember([topic], dataCallback)
        }
      },

      subscribeAsIndividual : async (topic, dataCallback)=>{
        if (topic === appId) {
          await unsubscribe([topic])
          await subscribeAsIndividual([topic],
            async (msgObj)=>{
              if (!(await onDedicatedMsg(msgObj))) {
                await dataCallback(msgObj)
              }
          })
        } else {
          await subscribeAsIndividual([topic], dataCallback)
        }
      },

      unsubscribe : unsubscribe,
      
      createTopic : createTopic
    }
}

function nonAsyncInterface( brokers,
                            appId,
                            appGroup,
                            logger,
                            heartbeatInterval,
                            heartbeatTimeout,
                            latencyMetricsOn,
                            callback)
{
  init(brokers,
    appId,
    appGroup,
    logger,
    heartbeatInterval,
    heartbeatTimeout,
    latencyMetricsOn)
  .then(({ produce, subscribeAsGroupMember, subscribeAsIndividual, unsubscribe, createTopic })=>{
    callback({
      produce : (topic, key, message, headers, errCallback) => {
        produce(topic, key, message, headers)
        .then(()=> { errCallback(null) })
        .catch((err) => { errCallback(err) })
      },

      subscribeAsGroupMember : (topic, dataCallback, errCallback) => {
        subscribeAsGroupMember(topic, async (msgObj) => { dataCallback(msgObj) })
        .then( ()=> { errCallback(null) } )
        .catch( (err) => { errCallback(err) } )
      },

      subscribeAsIndividual : (topic, dataCallback, errCallback) => {
        subscribeAsIndividual(topic, async (msgObj) => { dataCallback(msgObj) })
        .then( ()=> { errCallback(null) } )
        .catch( (err) => { errCallback(err) } )
      },

      unsubscribe : (topic, errCallback) => {
        unsubscribe([topic])
        .then( ()=> { errCallback(null) } )
        .catch( (err) => { errCallback(err) } )
      },
      
      createTopic : (topicName, numPartitions, replicationFactor, errCallback) => {
        createTopic(topicName, numPartitions, replicationFactor)
        .then( ()=> { errCallback(null) } )
        .catch( (err)=> { errCallback(err) } )
      }
    }, null)
  })
  .catch( (err)=> { callback(null, err) })

}

module.exports.init = nonAsyncInterface