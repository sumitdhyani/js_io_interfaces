const readline = require('readline')
const {createMiddlewareInterface} = require('./middleware/Engine')
const {uuidGen} = require('./UUidUtils')
const SystemValues                = require('./SystemValues')
const [tags, tagValues, topics, appGroups]  =
[SystemValues.tags, SystemValues.tagValues, SystemValues.topics, SystemValues.appGroups]

const logger = 
{ debug : (str)=> { console.log(str)},
  info : (str)=> { console.log(str)},
  warn : (str)=> { console.log(str)},
  error : (str)=> { console.log(str)}
}

const reader = readline.createInterface({
  input: process.stdin,
  output: process.stdout
})

function readNextLineAndProduce(middlewareInterface) {
  reader.question('Type next message please: ', (msg)=>{
    if (msg !== "End") {
      middlewareInterface.request("test_topic",
        uuidGen(),
        {[tags.message_type] : tagValues.message_type.dummy, echo_text : msg},
        {},
        (msgObj, isLastResp)=> {
            logger.debug(`Received response: ${msgObj.message}, lastResp: ${isLastResp}`)
          readNextLineAndProduce(middlewareInterface)
        },
        (err)=>{
        if (err) {
          logger.error(`Error while trying to produce message, details : ${err.message}`)
        }
      })    
    }
  })
}

function initCallback(middlewareInterface, err){
  if(err){
    logger.error(`Error while initializinng the middleware, details: ${err.message}, stack: ${err.stack}`)
    return
  }

  readNextLineAndProduce(middlewareInterface)
}

createMiddlewareInterface(["node_1:9092", "node_2:9093", "node_3:9094"],
  "test_requestor_" + uuidGen(),
  "test_requestor",
  logger,
  10,
  30,
  false,
  null,
  initCallback
)


