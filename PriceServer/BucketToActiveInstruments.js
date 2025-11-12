
function getBucketToActiveInstrumentsFunctions(subsriptionForwarder,
                                               unsubscriptionForwarder) {
  const instrumentToBucket = new Map()
  const bucketToInstruments = new Map()

  // instrument : "Exchange:Instrument:PriceTypeToSubscribe"
  const addIntrument = (bucket, instrument) => {
    instrumentToBucket.set(instrument, bucket)
    let instruments = bucketToInstruments.get(bucket)
    if (undefined === instruments) {
      instruments = new Set()
      bucketToInstruments.add(instruments)
    }

    instruments.add(instrument)
  }

  const removeInstrument = (bucket, instrument) => {
    instrumentToBucket.delete(instrument)
    const instruments = bucketToInstruments.get(bucket)
    if (undefined !== instruments) {
      instruments.delete(instrument)
      if (instruments.size === 0) {
        bucketToInstruments.delete(bucket)
      }
    }
  }  
    

  const getInstrumentsForBucket = bucket=>{
    return bucketToInstruments.get(bucket) || []
  }

  return [addIntrument, removeInstrument, getInstrumentsForBucket]
}

module.exports = getBucketToActiveInstrumentsFunctions