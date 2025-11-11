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
                                       bucketIdGenerator)
{
  const clientToInstruments = new Map()
  const instrumentToClients = new Map()

  const onSubscription = (instrument, clientId, cb)=>{
    Array.from(clientToInstruments.entries())
    const clients = instrumentToClients.get(instrument)
    if (undefined !== clients  && clients.has(clientId)) {
      setImmediate(() => { cb(getErrorObject(err_codes.duplicate_subscription)) })
    } else if (undefined !== instrumentToClients.get(instrument)) {
      addInstrumentForClient(clientToInstruments, clientId, instrument)
      addClientForInstrument(instrumentToClients, instrument, clientId)
      setImmediate(()=> { cb(null) })
    } else {
      addInstrumentForClient(clientToInstruments, clientId, instrument)
      addClientForInstrument(instrumentToClients, instrument, clientId)
      const forwardSubscriptionFunc = ()=>{
        subscriptionForwarder(bucketIdGenerator(instrument), instrument, (err)=>{
          if (err) {
            // removeInstrumentForClient(clientToInstruments, clientId, instrument)
            // removeClientForInstrument(instrumentToClients, instrument, clientId)
            if (err.err_code === err_codes.price_provider_down) {
              forwardSubscriptionFunc()
            } else {
              cb(err)
            }
          } else {
            cb(err)
          }
        })
      }

      forwardSubscriptionFunc()
    }
  }

  const onUnSubscription = (instrument, clientId, cb) => {
    const clients = instrumentToClients.get(instrument)
    if (undefined === clients || !clients.has(clientId)) {
      setImmediate(() => { cb(getErrorObject(errOr_codes.spurious_unsubscription)) })
      return
    }

    const numClientsBeforeCleanup = clients.size
    removeInstrumentForClient(clientToInstruments, clientId, instrument)
    removeClientForInstrument(instrumentToClients, instrument, clientId)
    // Last client for this instrument
    if (numClientsBeforeCleanup > 1) {
      setImmediate(() => { cb(null) })
      return
    }

    const func = ()=>{
      unsubscriptionForwarder(bucketIdGenerator(instrument), instrument, (err)=>{
        if(err) {
          if(cb(err)) {
            func()
          }
        } else {
          cb(null)
        }
      })
    }

    func()
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

  const onClientDown = (clientId, cb)=>{
    const instrumentsForThisClient = clientToInstruments.get(clientId) || []
    let io_error = null
    Array.from(instrumentsForThisClient.values()).forEach(instrument=>{
      onUnSubscription(instrument, clientId, (err)=>{
        if(err) {
          if (err.err_code === err_codes.price_provider_down) {
            return true
          } else {
            io_error = err
          }
        }
      })
    })

    setImmediate(()=>{
      cb(io_error? io_error : null)
    })
  }

  return [onSubscription, onUnSubscription, onClientDown]
}

module.exports = getClientInteractionFunctions