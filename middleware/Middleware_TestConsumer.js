const createMiddlewareInterface = require('./kafka/Engine')
const appId = "test_consumer_" + uuidv4()
function uuidv4() {
    return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'
    .replace(/[xy]/g, function (c) {
        const r = Math.random() * 16 | 0, 
            v = c == 'x' ? r : (r & 0x3 | 0x8);
        return v.toString(16);
    });
}

function initCallback(middlewareINterface, err) {
    if (err) {
        return
    }

    const topic = "test_topic"
    middlewareINterface.subscribeAsIndividual(topic,
        (msgObject) => { logger.info( `Message received, content ${msgObject.message}`) },
        (err) => {
            if(err) {
                logger.error(`Error while subscribing to topic ${topic}, details: ${err.message}`) 
            } else {
                logger.info(`Listening for messsages on topic: ${topic}`)
            }
    })
}

const logger = 
{ debug : (str)=> { console.log(str)},
  info : (str)=> { console.log(str)},
  warn : (str)=> { console.log(str)},
  error : (str)=> { console.log(str)}
}

createMiddlewareInterface.init(["node_1:9092", "node_2:9093", "node_3:9094"],
    appId,
    "test_consumer",
    logger,
    10,
    30,
    false,
    initCallback)