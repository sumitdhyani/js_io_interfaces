const CreateKafkaInterface = require('./LibWrapper')

function init(brokers,
              appId,
              appGroup,
              logger,
              heartbeatInterval,
              heartbeatTimeout,
              latencyMetricsOn,
              callback)
{
    CreateKafkaInterface.createKafkaLibrary(brokers,
                         appId,
                         appGroup,
                         logger,
                         callback)
    .then(({ produce, 
      subscribeAsGroupMember,
      subscribeAsIndividual,
      unsubscribe,
      createTopic })=>{
        
        const appDetails = {appId : appId,
          appGroup : appGroup,
          heartbeatInterval : heartbeatInterval,
          heartbeatTimeout : heartbeatTimeout }
        produce("registrations", appGroup, JSON.stringify(appDetails), {})
        .then(()=>{
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
          const heartbestMsg = JSON.stringify({appId : appId})
          setInterval(()=>{
            produce("heartbeats", appGroup, heartbestMsg, {})
            .then(()=>{})
            .catch((err)=>{
              logger.error(`Error while sending heartbeat for app: ${appId}, details: ${err.message}`)
            })},
            heartbeatInterval*1000)
        })
        .catch((err) => { callback(null, err) })
        
    })
    .catch((err)=>{
      callback(null, err)
    })
}

module.exports.init = init

