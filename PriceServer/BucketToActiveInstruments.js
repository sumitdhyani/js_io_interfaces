
function getBucketToActiveInstrumentsFunctions(subsriptionForwarder,
                                               unsubscriptionForwarder)
{
  const instrumentToBucket = new Map()
  const bucketToInstruments = new Map()

  // instrument : "Exchange:Instrument:PriceTypeToSubscribe"
  const onSubscription = (bucket, instrument, cb) => { 
    subsriptionForwarder(bucket, instrument, (err)=> {
      if (err !== null) {
        instrumentToBucket.set(instrument, bucket)
        let instruments = bucketToInstruments.get(bucket)
        if(undefined === instruments) {
          instruments = new Set()
          bucketToInstruments.add(instruments)
        }

        instruments.add(instrument)
      }
      
      cb(err)
    })

  }

  const onUnSubscription = (bucket, instrument) => {
    if(!unsubscriptionForwarder(bucket, instrument)) return false

    instrumentToBucket.delete(instrument)
    const instruments = bucketToInstruments.get(bucket)
    instruments.delete(instrument)
    if (instruments.size === 0) {
      bucketToInstruments.delete(bucket)
    }
  }

  const getInstrumentsForBucket = bucket=>{
    return bucketToInstruments.get(bucket) || []
  }

  return [onSubscription, onUnSubscription, getInstrumentsForBucket]
}

module.exports = getBucketToActiveInstrumentsFunctions