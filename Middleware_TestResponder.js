const {createMiddlewareInterface} = require('./middleware/Engine')
const {tags, tagValues, topics, appGroups} = require('./SystemValues')
function uuidv4() {
    return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'
    .replace(/[xy]/g, function (c) {
        const r = Math.random() * 16 | 0, 
            v = c == 'x' ? r : (r & 0x3 | 0x8);
        return v.toString(16);
    });
}
const appId = "test_consumer_" + uuidv4()

function initCallback(middlewareINterface, err) {
    if (err) {
        return
    }

    const topic = "test_topic"
    middlewareINterface.subscribeAsIndividual(topic,
      (msgObj) => { logger.info( `Message received by responder, content ${msgObj.message}, headers: ${JSON.stringify(msgObj.headers)}`) },
      (err) => {
          if(err) {
              logger.error(`Error while subscribing to topic ${topic}, details: ${err.message}`) 
          } else {
              logger.info(`Listening for messsages on topic: ${topic}`)
          }
      }
    )
}

const logger = 
{ debug : (str)=> { console.log(str)},
  info : (str)=> { console.log(str)},
  warn : (str)=> { console.log(str)},
  error : (str)=> { console.log(str)}
}

createMiddlewareInterface(["node_1:9092", "node_2:9093", "node_3:9094"],
    appId,
    "test_responder",
    logger,
    10,
    30,
    false,
    (reqObj, responseFunc)=> {
      const dict = reqObj.deserializer(reqObj.message)
      logger.debug(`Req: ${JSON.stringify(dict)}`)
      const echoText = dict["echo_text"]
      if (undefined !== dict["echo_text"]) {
        responseFunc({[tags.message_type] : tagValues.message_type.dummy, echo_text : echoText},
                     true,
                     (err) => {
                       if(err) {
                         logger.error(`Error whil sending response: ${err.message}`)
                       }
                     })
      } else {
        responseFunc({[tags.message_type] : tagValues.message_type.dummy},
                     true,
                     (err) => {
                      if(err) {
                        logger.error(`Error while sending response: ${err.message}`)
                      }
                    })
      }
    },
    initCallback)