class SortedMap extends Map {
  constructor(...args) {
    super(...args);
  }


}
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
      this.keyToBucketIdxs.set(key, [...Array(this.numBuckets).keys()])
      for (let i in 0..numBuckets) {
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
      const begin = this.weightMap.entries().Array().next()
      const [highestWeight, highestWeightKeys] = begin.value
      let numUnassignmentForThisKey = toUnassign.get(highestWeightKeys[0])
      if (numUnassignmentForThisKey === undefined) {
        toUnassign.set(highestWeightKeys[0], 1)
      } else {
        toUnassign.set(highestWeightKeys[0], numUnassignmentForThisKey + 1)
      }

      let [secondHighestWeight, secondHighestKeyList] = begin.next()
      if (secondHighestWeight === undefined) {
        this.weightMap.set(highestWeight - 1, [highestWeightKeys.shift()])
      } else {
        secondHighestKeyList.push(highestWeightKeys.shift())
      }

      if (highestWeightKeys.length === 0) {
        this.weightMap.delete(highestWeight)
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
        assignmentCallback(bucketIdx, key)
        unassignmentCallback(bucketIdx, unassignKey)
      }
    }

    return true
  }

  removeKey(key, assignmentCallback, unassignmentCallback){
    const bucketIds = this.keyToBucketIdxs.get(key)
    if (bucketIds === undefined) {
      return false
    } else if (!this.reserveKeys.empty()) {
      this.keyToBucketIdxs.delete(key)
      const newKey = this.reserveKeys.shift()
      this.keyToBucketIdxs.set(newKey, bucketIds)
      bucketIds.forEach(bucketIdx => {
        assignmentCallback(bucketIdx, newKey)
      })

      return true
    } else if (this.numKeys() === 1) {
      // Removing the last key
      this.keyToBucketIdxs.clear()
      this.weightMap.clear()
      return true
    }

    // Get the bucket indices for the key
    const bucketIdxs = this.keyToBucketIdxs.get(key)
    bucketIdxs.forEach(bucketIdx => {
      const end = this.weightMap.entries().Array().reverse().next()
      const [lowestWeight, lowestWeightKeys] = end.value
      const bucketIdxToTopup = bucketIdxs.shift()
      this.keyToBucketIdxs.get(bucketIdxToTopup).push(bucketIdx)
      assignmentCallback(bucketIdx, bucketIdxToTopup)// Here
      const [secondHighestKeyList, secondHighestWeight] = end.next()
      if (secondHighestKeyList === undefined) {
        this.weightMap.set(lowestWeight + 1, [lowestWeightKeys.shift()])
      } else {
        secondHighestKeyList.push(lowestWeightKeys.shift())
      }


      
      this.bucketIdxToKey.delete(bucketIdx)
      unassignmentCallback(bucketIdx, key)
    })
    
    return true
  }
}
