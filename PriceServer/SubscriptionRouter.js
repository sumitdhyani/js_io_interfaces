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

  onSubscriptionRequest(bucket, request) {
    const key = this.bucketIdToKey.get(bucket)
    if (undefined === key) return false
    this.sendSubscriptionFunction(key, request)
    return true
  }

  onUnsubscriptionRequest(bucket, request) {
    const key = this.bucketIdToKey.get(bucket)
    if (undefined === key) return false
    this.sendUnsubscriptionFunction(key, request)
    return true
  }

  onKeyAssignment(bucket, key) {
    const requestList = this.getInstrumentListFromBucketFunction(bucket)
    requestList.forEach(request => {
      this.sendSubscriptionFunction(key, request)
    })

    this.bucketIdToKey.set(bucket, key)
    let bucketIdSet = this.keytoBucketIds.get(key)
    if (undefined == bucketIdSet) {
      bucketIdSet = new Set();
      this.keytoBucketIds.set(key, bucketIdSet)  
    }
    bucketIdSet.add(bucket)
  }

  onBucketAssignment(bucket, key) {
    const requestList = this.getInstrumentListFromBucketFunction(bucket)
    requestList.forEach(request => {
      this.sendSubscriptionFunction(key, request)
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

  onBucketUnassignment(bucket, key) {
    const requestList = this.getInstrumentListFromBucketFunction(bucket)
    requestList.forEach(request => {
      this.sendUnsubscriptionFunction(key, request)
    })

    if (this.bucketIdToKey.get(bucket) === key) {
      this.bucketIdToKey.delete(bucket)
      this.keytoBucketIds.get(key).delete(bucket)
    }
  }

  // Delete the all data related to 'key' before calling the deleteKeyNotification function
  onPriceProviderDown(key) {
    // First get the bicket belongins to this key and remove their references
    const bucketSet = this.keytoBucketIds.get(key)
    if (undefined !== bucketSet) {
      Array.from(bucketSet.values()).forEach(bucket => {
        this.bucketIdToKey.delete(bucket)
      })
      this.bucketIdToKey.delete(key)
    }
  
    this.bucketAssigner.removeKey(key, this.onBucketAssignment)
  }

  onPriceProviderUp(key) {
    this.bucketAssigner.addKey(key, this.onBucketAssignment, this.onBucketUnassignment)
  }
}

module.exports = SubscriptionRouter