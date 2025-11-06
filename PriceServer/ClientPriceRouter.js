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
  const instruments = clientToInstruments.get(instrument)
  if (undefined === instruments) {
    clientToInstruments.set(client, new Set([instrument]))
    return true
  } else if (!instruments.has(instrument)) {
    instruments.add(instr)
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
                                       bucketIdGenerator)
{
  const clientToInstruments = new Map()
  const instrumentToClients = new Map()
  const active = false
  const synced = false

  const onSubscription = (instrument, clientId)=>{
    if (!(addInstrumentForClient(clientToInstruments, clientId, instrument) &&
          addClientForInstrument(instrumentToClients, instrument, clientId))) {
      throw Error(`Duplicate subscription for : ${instrument}`)
    }

    if(clientToInstruments.get(instrument).size > 1) {
      return
    }
    
    if(!subscriptionForwarder(bucketIdGenerator(instrument), instrument)) {
      removeInstrumentForClient(clientToInstruments, clientId, instrument)
      removeClientForInstrument(instrumentToClients, instrument, clientId)
      throw Error(`No price provider for this exchange`)
    }
  }

  const onUnSubscription = (instrument, clientId) => {
    if (!(removeInstrumentForClient(clientToInstruments, clientId, instrument) &&
          removeClientForInstrument(instrumentToClients, instrument, clientId))) {
      throw Error(`Spurious unsubscription for : ${instrument}`)
    }


    // Atleast 1client is thereto receive the prices for this instrument
    if(instrumentToClients.has(instrument)) {
      return
    }

    if (!unsubscriptionForwarder(bucketIdGenerator(instrument), instrument)) {
      // Roolback thedata structures and throw an error
      addInstrumentForClient(clientToInstruments, clientId, instrument)
      addClientForInstrument(instrumentToClients, instrument, clientId)
      throw Error(`No price provider for this exchange, unsubscription can't be forwarded, retry!`)
    }
  }

  const onPrice = (instrument, price) => {
    const clientsForThisInstrument = instrumentToClients.get(instrument)
    if (undefined === clientsForThisInstrument) {
      // Undesired price, the unsubsccription was not forwarded perhaps, unsubscribe now 
      unsubscriptionForwarder(bucketIdGenerator(instrument), instrument)
      return
    }

    clientsForThisInstrument.forEach(client => {
      priceForwarder(client, price)
    })
  }

  const onClientDown = clientId=>{
    const instrumentsForThisClient = clientToInstruments.get(clientId) || []
    Array.from(instrumentsForThisClient.values()).forEach(instrument=>{
      const clients = instrumentToClients.get(instrument)
      const numClients = clients.size
      if (removeClientForInstrument(instrumentToClients, instrument, clientId) && numClients === 1) {
          unsubscriptionForwarder(bucketIdGenerator(instrument), instrument)
      }
    })

    clientToInstruments.delete(clientId)
  }

  return [onSubscription, onUnSubscription, onPrice, onClientDown]
}

module.exports = getClientInteractionFunctions