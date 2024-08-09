const readline = require('readline')
const GetMiddlewareInterface = require('./kafka/Engine')

const logger = 
{ debug : (str)=> { console.log(str)},
  info : (str)=> { console.log(str)},
  warn : (str)=> { console.log(str)},
  error : (str)=> { console.log(str)}
}

const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout
})

function readNextLineAndProduce(middlewareInterface) {
  rl.question('Type next message? ', msg => {
    if (0 == msg.localeCompare("End")){
      return
    }

    middlewareInterface.produce("test_topic", "test_topic", msg, {}, (err)=>{
      if (err) {
        logger.error(`Error while trying to produce message, details : ${err.message}`)
      }
      readNextLineAndProduce(middlewareInterface)
    })
  })
}

function initCallback(middlewareInterface, err){
  if(err){
    logger.err(`Error while initializinng the middleware, details: ${err.message}`)
    return
  }

  readNextLineAndProduce(middlewareInterface)
}

const middlewareInterface =
GetMiddlewareInterface.init(["node_1:9092", "node_2:9093", "node_3:9094"],
  "test_app_1",
  "test_app",
  logger,
  10,
  false,
  initCallback
)


