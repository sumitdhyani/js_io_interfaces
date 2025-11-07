class SubscriptionRouter
{
  constructor(bucketAssigner,
              getInstrumentListFromBucketFunction,
              sendSubscriptionFunction, 
              sendUnsubscriptionFunction)
  {
    // bucketId -> key
    this.bucketIdToKey = new Map()
    // key -> set of bucketIds
    this.keytoBucketIds = new Map()

    this.bucketAssigner                       = bucketAssigner
    this.getInstrumentListFromBucketFunction  = getInstrumentListFromBucketFunction
    this.sendSubscriptionFunction             = sendSubscriptionFunction
    this.sendUnsubscriptionFunction           = sendUnsubscriptionFunction

    // These 2 functions are passed to external code
    this.onBucketAssignment = this.onBucketAssignment.bind(this);
    this.onBucketUnassignment = this.onBucketUnassignment.bind(this);
  }

  onSubscriptionRequest(bucket, request, cb) {
    const key = this.bucketIdToKey.get(bucket)
    if (undefined === key) {
      setImmediate(()=>{cb(new Error("No Price Providers for this exchange"))})
      return
    }

    this.sendSubscriptionFunction(key, request, cb)
  }

  onUnsubscriptionRequest(bucket, request, cb) {
    const key = this.bucketIdToKey.get(bucket)
    if (undefined === key) {
      setImmediate(() => { cb(new Error("No Price Providers for this exchange")) })
      return
    }

    this.sendUnsubscriptionFunction(key, request, cb)
  }

  onBucketAssignment(bucket, key, cb) {
    const requestList = this.getInstrumentListFromBucketFunction(bucket)
    requestList.forEach(request => {
      this.sendSubscriptionFunction(key, request, cb)
    })
    // Remove existing arrangement for the bucket
    const existingKey = this.bucketIdToKey.get(bucket)
    if (undefined !== existingKey) {
      this.bucketIdToKey.delete(bucket)
      this.keytoBucketIds.get(existingKey).delete(bucket)
    }

    // Add the relevant context for the new bucket-key pair
    this.bucketIdToKey.set(bucket, key)
    let bucketIdSet = this.keytoBucketIds.get(key)
    if (undefined === bucketIdSet) {
      bucketIdSet = new Set();
      this.keytoBucketIds.set(key, bucketIdSet)
    }
    bucketIdSet.add(bucket)
  }

  onBucketUnassignment(bucket, key, cb) {
    const requestList = this.getInstrumentListFromBucketFunction(bucket)
    requestList.forEach(request => {
      this.sendUnsubscriptionFunction(key, request, cb)
    })

    if (this.bucketIdToKey.get(bucket) === key) {
      this.bucketIdToKey.delete(bucket)
      this.keytoBucketIds.get(key).delete(bucket)
    }
  }

  // Delete the all data related to 'key' before calling the deleteKeyNotification function
  onPriceProviderDown(key, cb_sub) {
    // First get the bicket belongins to this key and remove their references
    const bucketSet = this.keytoBucketIds.get(key)
    if (undefined !== bucketSet) {
      Array.from(bucketSet.values()).forEach(bucket => {
        this.bucketIdToKey.delete(bucket)
      })
      this.bucketIdToKey.delete(key)
    }
  
    this.bucketAssigner.removeKey(key, (bucket, key)=>{this.onBucketAssignment(bucket, key, cb_sub)})
  }

  onPriceProviderUp(key, cb_sub, cb_unsub) {
    this.bucketAssigner.addKey(key, (bucket, key)=>{this.onBucketAssignment(bucket, key, cb_sub)}, (bucket, key)=>{ this.onBucketUnassignment(bucket, key, cb_unsub) })
  }
}

module.exports = SubscriptionRouter