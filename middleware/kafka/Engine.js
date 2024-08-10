const CreateKafkaInterface = require('./LibWrapper')


function uuidv4() {
  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'
  .replace(/[xy]/g, function (c) {
      const r = Math.random() * 16 | 0, 
          v = c == 'x' ? r : (r & 0x3 | 0x8);
      return v.toString(16);
  });
}

function init(brokers,
              appId,
              appGroup,
              logger,
              heartbeatInterval,
              latencyMetricsOn,
              callback) {
    const uuid = uuidv4()
    const kafkaAppId = appGroup + ":" + appId + ":" + uuid
    const kafkaAppGroup = appGroup

    CreateKafkaInterface.createKafkaLibrary(brokers,
                         kafkaAppId,
                         kafkaAppGroup,
                         logger,
                         callback)
    .then(({ produce, 
      subscribeAsGroupMember,
      subscribeAsIndividual,
      unsubscribe,
      createTopic })=>{
        
        const appDetails = JSON.stringify({appId : kafkaAppId, appGroup : kafkaAppGroup})
        produce("registrations", kafkaAppGroup, appDetails, {})
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
          setInterval(()=>{
            produce("heartbeats", kafkaAppGroup, appDetails, {})
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

