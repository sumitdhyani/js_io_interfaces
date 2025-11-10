const [err_codes, getErrorObject] = require('./ErrorCodes.js')

function addClientForInstrument(instrumentToClients, instrument, client)
{
  const clients = instrumentToClients.get(instrument)
  if (undefined === clients) {
    instrumentToClients.set(instrument, new Set([client]))
    return true
  } else if(!clients.has(client)) {
    clients.add(client)
    return true
  } else {
    return false
  }
}

function addInstrumentForClient(clientToInstruments, client, instrument) {
  const instruments = clientToInstruments.get(client)
  if (undefined === instruments) {
    clientToInstruments.set(client, new Set([instrument]))
    return true
  } else if (!instruments.has(instrument)) {
    instruments.add(instrument)
    return true
  } 
  
  return false
}

function removeClientForInstrument(instrumentToClients, instrument, client) {
  const clients = instrumentToClients.get(instrument)
  if (undefined !== clients) {
    if (clients.delete(client)) {
      if (clients.size === 0) instrumentToClients.delete(instrument)
      return true
    }
  }
  
  return false
}

function removeInstrumentForClient(clientToInstruments, client, instrument) {
  const instruments = clientToInstruments.get(client)
  if (undefined !== instruments) {
    if(instruments.delete(instrument)) {
      if (instruments.size === 0) clientToInstruments.delete(client)
      return true
    }
  }

  return false
}

function getClientInteractionFunctions(subscriptionForwarder,
                                       unsubscriptionForwarder,
                                       priceForwarder,
                                       bucketIdGenerator,
                                       messageSendingFailedHandlerFunction)
{
  const clientToInstruments = new Map()
  const instrumentToClients = new Map()

  const onSubscription = (instrument, clientId, cb)=>{
    const clients = instrumentToClients.get(instrument)
    if (undefined !== clients  && clients.has(clientId)) {
      setImmediate(() => { cb(getErrorObject(err_codes.duplicate_subscription)) })
    } else if (undefined !== instrumentToClients.get(instrument)) {
      addInstrumentForClient(clientToInstruments, clientId, instrument)
      addClientForInstrument(instrumentToClients, instrument, clientId)
      setImmediate(()=> { cb(null) })
    } else {
      subscriptionForwarder(bucketIdGenerator(instrument), instrument, (err)=>{
        if (!err) {
          addInstrumentForClient(clientToInstruments, clientId, instrument) &&
          addClientForInstrument(instrumentToClients, instrument, clientId)
        }
        cb(err)
      })
    }
  }

  const onUnSubscription = (instrument, clientId, cb) => {
    const clients = instrumentToClients.get(instrument)
    if (undefined === clients && !clients.has(clientId)) {
      setImmediate(() => { cb(getErrorObject(errOr_codes.spurious_unsubscription)) })
      return
    }

    const numClients = clients.size
    const hasClient = clients.has(clientId)

    removeInstrumentForClient(clientToInstruments, clientId, instrument)
    removeClientForInstrument(instrumentToClients, instrument, clientId)
    // Last client for this instrument
    if (numClients === 1 && hasClient) {
      unsubscriptionForwarder(bucketIdGenerator(instrument), instrument, cb)
    } else {
      setImmediate(()=>{ cb(null) })
    }
  }

  // const onPrice = (instrument, price) => {
  //   const clientsForThisInstrument = instrumentToClients.get(instrument)
  //   // Defensive coding, not ideal
  //   if (undefined === clientsForThisInstrument) {
  //     // Undesired price, the unsubsccription was not forwarded perhaps, unsubscribe now 
  //     unsubscriptionForwarder(bucketIdGenerator(instrument), instrument, (err)=>{})
  //     return
  //   }

  //   clientsForThisInstrument.forEach(client => {
  //     priceForwarder(client, price)
  //   })
  // }

  const onClientDown = clientId=>{
    const instrumentsForThisClient = clientToInstruments.get(clientId) || []
    Array.from(instrumentsForThisClient.values()).forEach(instrument=>{
      const clients = instrumentToClients.get(instrument)
      const numClients = clients.size
      const hasClient = clients.has(clientId)
      removeInstrumentForClient(clientToInstruments, clientId, instrument)
      removeClientForInstrument(instrumentToClients, instrument, clientId)

      // Last client for this instrument
      if (numClients === 1 && hasClient) {
        unsubscriptionForwarder(bucketIdGenerator(instrument), instrument, (err)=>{
          if(err) {
            messageSendingFailedHandlerFunction(err)
          }
        })
      }
    })
  }

  //return [onSubscription, onUnSubscription, onPrice, onClientDown]
  return [onSubscription, onUnSubscription, onClientDown]
}

module.exports = getClientInteractionFunctions