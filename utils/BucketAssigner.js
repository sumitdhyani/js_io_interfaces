class BucketAssigner
{
  constructor(numBuckets)
  {
    this.numBuckets = numBuckets

    // Weight -> list of keys
    this.weightMap = new Map()

    // bucketIdx -> key
    this.bucketIdxToKey = new Map()

    // This DataStructure seems redundant but keeping it for now
    // as we have not yet implemented removeKey functionality
    // It might be useful there
    // key -> list of bucketIdxs
    this.keyToBucketIdxs = new Map()

    this.reserveKeys = []
  }

  numKeys(){
    return this.keyToBucketIdxs.size
  }

  full(){
    return this.numKeys() === this.numBuckets
  }

  empty() {
    return this.numKeys() === 0
  }

  addKey(key, assignmentCallback, unassignmentCallback)
  {
    // Each key has exactly 1 partition, so this new key can't do anything for now
    if (this.full()) {
      this.reserveKeys.push(key)
      return true
    }
    // Duplicate key
    else if (this.keyToBucketIdx.has(key)) {
      return false
    }
    // 1st key being added
    else if (this.empty()) {
      this.weightMap.set(key, this.numBuckets)
      const bucketIdxs = []
      this.keyToBucketIdxs.set(key, bucketIdxs)
      for (let i in 0..numBuckets) {
        this.bucketIdxToKey.set(i, key)
        bucketIdxs.push(i)
        assignmentCallback(key, i)
      }
      return true
    }


    const numKeysToReassign = this.numBuckets / (this.keyToBucketIdxs.size() + 1)

    // Place the new key into weightMap
    let keyList = this.weightMap.get(numKeysToReassign)
    if (keyList === undefined) {
      this.weightMap.set(numKeysToReassign, [key])
    } else {
      keyList.push(key)
    }
    // key -> num of buckets to unassign
    // unassigned buckets will be reassigned to the new key
    let toUnassign = new Map()

    for (let i in 0..numKeysToReassign) {
      const begin = this.weightMap.entries().next()
      const [weight, keyList] = begin.value
      let numUnassignmentForThisKey = toUnassign.get(keyList)
      if (numUnassignmentForThisKey === undefined) {
        toUnassign.set(keyList[0], 1)
      } else {
        toUnassign.set(keyList[0], numUnassignmentForThisKey + 1)
      }

      let [nextWeight, nextKeyList] = begin.next()
      if (nextWeight === undefined || nextWeight < weight - 1) {
        this.weightMap.set(weight - 1, [keyList.shift()])
      } else {
        nextKeyList.push(keyList.shift())
      }

      if (keyList.length === 0) {
        this.weightMap.delete(weight)
      }
    }

    for (let [unassignKey, numUnassign] of toUnassign) {
      // reassign buckets from unassignKey to key
      const bucketIdxs = this.keyToBucketIdxs.get(unassignKey)
      
      // Assign empty list for the new key and hold the reference to fill it later
      const bucketIdxsForThisKey = []
      this.keyToBucketIdxs.set(key, bucketIdxsForThisKey)
      for (let i in 0..numUnassign) {
        // The 1st bucketIdx of the unassignKey is reassigned to the new key
        const bucketIdx = bucketIdxs.shift()
        bucketIdxsForThisKey.push(bucketIdx)
        this.bucketIdxToKey.set(bucketIdx, key)

        assignmentCallback(bucketIdx, key)
        unassignmentCallback(bucketIdx, unassignKey)
      }
    }

    return true
  }
}
