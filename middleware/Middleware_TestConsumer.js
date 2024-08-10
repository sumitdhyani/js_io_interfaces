const createMiddlewareInterface = require('./kafka/Engine')

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

    middlewareINterface.subscribeAsIndividual("heartbeats",
        (msgObject) => { console.log( `Message received, content ${msgObject.message}`) },
        (err) => { console.log(`Error while subscribing to topic test_topic, details: ${err.message}`) })
}

const logger = 
{ debug : (str)=> { console.log(str)},
  info : (str)=> { console.log(str)},
  warn : (str)=> { console.log(str)},
  error : (str)=> { console.log(str)}
}

createMiddlewareInterface.init(["node_1:9092", "node_2:9093", "node_3:9094"],
    "test_app_2" + ":" + uuidv4(),
    "test_app_3",
    logger,
    10,
    30,
    false,
    initCallback)