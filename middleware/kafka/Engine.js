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
    const kafkaAppGroup = appGroup + ":" + uuid

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
        callback({
          produce: (topic, key, message, headers, errCallback)=>{
            produce(topic, key, message, headers)
            .then(()=>{ errCallback(null) })
            .catch((err)=>{ errCallback(err) })
          },
          subscribeAsGroupMember : (topics, dataCallback, errCallback)=>{
            subscribeAsGroupMember(topics, dataCallback)
            .then(()=>{})
            .catch((err)=>{ errCallback(err) })
          },
          subscribeAsIndividual : (topics, dataCallback, errCallback)=>{
            subscribeAsIndividual(topics, dataCallback)
            .then(()=>{})
            .catch((err)=>{ errCallback(err) })
          },
          unsubscribe : (topics, errCallback)=>{
            unsubscribe(topics)
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
        const heartBeatMsg = JSON.stringify({appId : kafkaAppId, appGroup : kafkaAppGroup})
        setInterval(()=>{ 
          produce("heartbeats", kafkaAppGroup, heartBeatMsg, {})
          .then(()=>{})
          .catch((err)=>{
            logger.error(`Error while sending heartbeat for app: ${appId}, details: ${err.message}`)
          })},
          heartbeatInterval*1000)
    })
    .catch((err)=>{
      callback(null, err)
    })
}

module.exports.init = init

