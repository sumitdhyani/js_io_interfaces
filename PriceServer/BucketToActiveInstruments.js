
function getBucketToActiveInstrumentsFunctions(subsriptionForwarder,
                                               unsubscriptionForwarder)
{
  const instrumentToBucket = new Map()
  const bucketToInstruments = new Map()

  // instrument : "Exchange:Instrument:PriceTypeToSubscribe"
  const onSubscription = (bucket, instrument, cb) => { 
    subsriptionForwarder(bucket, instrument, (err)=> {
      if (!err) {
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

  const onUnSubscription = (bucket, instrument, cb) => {
    unsubscriptionForwarder(bucket, instrument, (err)=> {
      if (!err) {
        instrumentToBucket.delete(instrument)
        const instruments = bucketToInstruments.get(bucket)
        if (undefined !== instruments) {
          instruments.delete(instrument)
          if (instruments.size === 0) {
            bucketToInstruments.delete(bucket)
          }
        }
      }

      cb(err)
    })
  }  
    

  const getInstrumentsForBucket = bucket=>{
    return bucketToInstruments.get(bucket) || []
  }

  return [onSubscription, onUnSubscription, getInstrumentsForBucket]
}

module.exports = getBucketToActiveInstrumentsFunctions