const CreateKafkaInterface = require('./LibWrapper')
const SystemValues = require('../../SystemValues')
function init(brokers,
              appId,
              appGroup,
              logger,
              heartbeatInterval,
              heartbeatTimeout,
              latencyMetricsOn,
              callback)
{
  const [tags, tagValues, topics] = [SystemValues.tags, SystemValues.tagValues, SystemValues.topics]
  CreateKafkaInterface.createKafkaLibrary(brokers,
                       appId,
                       appGroup,
                       logger,
                       callback)
  .then(({ produce, subscribeAsGroupMember, subscribeAsIndividual, unsubscribe, createTopic })=>{
    createTopic(appId, 1, 2)
    .then(()=>{ 
      logger.info(`Created app topic: ${appId}`)
      const comp_enquiry_resp = JSON.stringify({
            [tags.message_type] : tagValues.message_type.component_enquiry_response,
            [tags.appId] : appId,
            [tags.appGroup] : appGroup,
            [tags.heartbeatInterval] : heartbeatInterval,
        [tags.heartbeatTimeout] : heartbeatTimeout })
      const onDedicatedMsg = (msgObj)=> {
        const msgDict = JSON.parse(msgObj.message)
        const msgType = msgDict[tags.message_type]
        if (0 === msgType.localeCompare(tagValues.message_type.component_enquiry)) {
          const destTopic = msgDict[tags.destination_topic]
          produce(destTopic, component_enquiry_resp, comp_enquiry_resp, {})
          .then(()=>{ logger.debug(`Send component enquiry response to topic: ${destTopic}`) })
          .catch((err) => { logger.debug(`Error while sending component enquiry response to topic: ${destTopic}, details: ${err.message}`) })
        }
      }

      subscribeAsIndividual(appId, onDedicatedMsg)
      .then(()=> {
        const heartbestMsg = JSON.stringify({[tags.message_type]: tagValues.message_type.heartbeat, [tags.appId] : appId})
        produce(topics.heartbeats, appGroup, heartbestMsg, {})
        .then(()=>{
          const notifyCallbackWithLibMethods = ()=>{
            callback({
            produce: (topic, key, message, headers, errCallback)=>{
              produce(topic, key, message, headers)
              .then(()=>{ errCallback(null) })
              .catch((err)=>{ errCallback(err) })
            },
            subscribeAsGroupMember : (topic, dataCallback, errCallback)=>{
              subscribeAsGroupMember([topic], dataCallback)
              .then(()=>{})
              .catch((err)=>{ errCallback(err) })
            },
            subscribeAsIndividual : (topic, dataCallback, errCallback)=>{
              subscribeAsIndividual([topic], dataCallback)
              .then(()=>{})
              .catch((err)=>{ errCallback(err) })
            },
            unsubscribe : (topic, errCallback)=>{
              unsubscribe([topic])
              .then(()=>{})
              .catch((err)=>{ errCallback(err) })
            },
            createTopic : (topicName, numPartitions, replicationFactor, errCallback)=>{
              createTopic(topicName, numPartitions, replicationFactor)
              .then(()=>{})
              .catch((err)=>{ errCallback(err) })
            }},
            null)
           //Send heart-beats
              const startHeartbeatSendingMechanism = ()=>{
                setInterval(()=>{
                produce("heartbeats", appGroup, heartbestMsg, {})
                .then(()=>{})
                .catch((err)=>{
                  logger.error(`Error while sending heartbeat for app: ${appId}, details: ${err.message}`)
                })},
                heartbeatInterval*1000)
                }
  
                notifyCallbackWithLibMethods()
                startHeartbeatSendingMechanism()
        
          }

          const startHeartbeatSendingMechanism = ()=>{
            setInterval(()=>{
            produce("heartbeats", appGroup, heartbestMsg, {})
            .then(()=>{})
            .catch((err)=>{
              logger.error(`Error while sending heartbeat for app: ${appId}, details: ${err.message}`)
            })},
            heartbeatInterval*1000)
          }
          
          notifyCallbackWithLibMethods()
          startHeartbeatSendingMechanism()
        })//Produce 1st heartbeat then
        .catch((err) => { callback(null, err) })
      })//Subscribe to self topic then
      .catch((err) => { callback(null, err) } )
    })//Create app topic then
    .catch((err) => { callback(null, err) })
  })//Initialize lib then
  .catch((err)=>{ callback(null, err) })
}

module.exports.init = init

