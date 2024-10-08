const readline = require('readline-sync')
const GetMiddlewareInterface = require('./kafka/Engine')
const {uuidGen} = require('../UUidUtils')

const logger = 
{ debug : (str)=> { console.log(str)},
  info : (str)=> { console.log(str)},
  warn : (str)=> { console.log(str)},
  error : (str)=> { console.log(str)}
}

function readNextLineAndProduce(middlewareInterface) {
  const msg = readline.question('Type next message please: ')
  if (0 == msg.localeCompare("End")){
    return
  }

  middlewareInterface.produce("test_topic", "test_topic", msg, {}, (err)=>{
    if (err) {
      logger.error(`Error while trying to produce message, details : ${err.message}`)
    }
    readNextLine(middlewareInterface)
  })
}

function initCallback(middlewareInterface, err){
  if(err){
    logger.error(`Error while initializinng the middleware, details: ${err.message}, stack: ${err.stack}`)
    return
  }

  readNextLineAndProduce(middlewareInterface)
}

const middlewareInterface = 
GetMiddlewareInterface.init(["node_1:9092", "node_2:9093", "node_3:9094"],
  "test_app_1_123",
  "test_app",
  logger,
  10,
  30,
  false,
  initCallback
)


