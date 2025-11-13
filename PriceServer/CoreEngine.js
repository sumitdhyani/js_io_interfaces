const [err_codes, getErrorObject] = require('./ErrorCodes.js')
const getClientInteractionFunctions = require('./ClientPriceRouter')
const SubscriptionRouter = require('./SubscriptionRouter')
const BucketAssigner = require('../utils/BucketAssigner')
const getBucketToActiveInstrumentsFunctions = require('./BucketToActiveInstruments')

function getHash(str, bucketSize) {
  const seed = 0
  let h1 = 0xdeadbeef ^ seed, h2 = 0x41c6ce57 ^ seed;
  for (let i = 0, ch; i < str.length; i++) {
    ch = str.charCodeAt(i);
    h1 = Math.imul(h1 ^ ch, 2654435761);
    h2 = Math.imul(h2 ^ ch, 1597334677);
  }
  h1 = Math.imul(h1 ^ (h1 >>> 16), 2246822507);
  h1 ^= Math.imul(h2 ^ (h2 >>> 13), 3266489909);
  h2 = Math.imul(h2 ^ (h2 >>> 16), 2246822507);
  h2 ^= Math.imul(h1 ^ (h1 >>> 13), 3266489909);

  return (4294967296 * (2097151 & h2) + (h1 >>> 0)) % bucketSize;
}

function getCoreEngineFunctions(ioSubscriptionForwarder,
                                ioUnsubscriptionForwarder,
                                numBuckets,
                                hashGenator = null) {

  if (hashGenator) {
    getHash = (instrument) => { return hashGenator(instrument) % numBuckets}
  }
  
  const [addIntrument, removeInstrument, getInstrumentsForBucket] = getBucketToActiveInstrumentsFunctions()

  const modifiedIoSubscriptionForwarder = (key, instrument, cb)=> {
    addIntrument(getHash(instrument, numBuckets), instrument)
    ioSubscriptionForwarder(key, instrument, cb)
  }

  const modifiedIoUnSubscriptionForwarder = (key, instrument, cb)=> {
    removeInstrument(getHash(instrument, numBuckets), instrument)
    ioUnsubscriptionForwarder(key, instrument, cb)
  }

  const router = new SubscriptionRouter(new BucketAssigner(numBuckets),
                                        getInstrumentsForBucket,
                                        modifiedIoSubscriptionForwarder,
                                        modifiedIoUnSubscriptionForwarder)

  const notifyPriceProviderUp = (providerId, cb_sub, cb_unsub)=> {
    router.onPriceProviderUp(providerId, cb_sub, cb_unsub)
  }

  const notifyPriceProviderDown = (providerId, cb_sub) => {
    router.onPriceProviderDown(providerId, cb_sub)
  }

  const [onSubscription, onUnSubscription, onClientDown] =
    getClientInteractionFunctions(router.onSubscriptionRequest.bind(router),
                                  router.onUnsubscriptionRequest.bind(router),
                                  (instrument)=> { return getHash(instrument, numBuckets) })

  return [onSubscription, onUnSubscription, onClientDown, notifyPriceProviderUp, notifyPriceProviderDown]
}

module.exports = getCoreEngineFunctions