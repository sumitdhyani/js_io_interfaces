
// Expectation from caller:
// 1. numBuckets > 0
// 2. Don't call addKey/removeKey methods in the assignment/Unassignment callbacks
class BucketAssigner
{
  constructor(numBuckets)
  {
    this.numBuckets = numBuckets

    // Weights are the number of buckets that are assigned to a key
    // It can be mathematically shown that in the most efficient distribution of buckets among keys,
    // the weight of any key can only be ceil(numBuckets / numKeys) or floor(numBuckets / numKeys)
    // So we only need to maintain only 2 weight levels, let's call them lower level and upper level
    // weightTable[0] -> lower level (floor)
    // weightTable[1] -> upper level (ceil)

    // Each weight level contains a list of keys that are assigned to it
    // When a key is added, the weights are reduced, starting from upper level keys
    // If, while adding a key, the upper level becomes empty, we swap the levels, so the new state of weightTable has an empty lower level

    // When a key is removed, the weights are increased, starting from lower level keys
    // If, while removing a key, the lower level becomes empty, and also we happen to increase the weight of a upper level key,
    // we add 1 more level to the weight table, above the current upper level, and remove the lower level

    // This way, we always maintain only 2 weight levels
    // If there are keys in just one level, it implies that all keys have the same weight and that level will always be the upper level
    // For non-zero no. of keys, lower level can be empty, but upper level will never be empty
    this.weightTable = [new Set(),new Set()]

    this.keyToBucketIdxs = new Map()

    this.reserveKeys = []
  }

  numKeys(){
    return this.keyToBucketIdxs.size
  }

  full(){
    return this.weightTable[1].size === this.numBuckets
  }

  empty() {
    return this.numKeys() === 0
  }

  addKey(key, assignmentCallback, unassignmentCallback)
  {
    // Duplicate key check before potentially putting this key in the reserve store
    // Otherwise, same key will be there in the keyToBucketIdxs and reserveKeys, which is incorrect state management
    // This bug was found by the test:
    // "no duplicate bucket indices after many cycles"
    if (this.keyToBucketIdxs.has(key)) {
      return false
    }
    // Each key has exactly 1 partition, so this new key can't do anything for now
    else if (this.full()) {
      // This bug was found by the test:
      // "sequential random operations with client cleanup before remove"
      if (this.reserveKeys.indexOf(key) === -1) {
        this.reserveKeys.push(key)
        return true
      } 
      
      return false
    }
    
    // 1st key being added
    else if (this.empty()) {
      this.weightTable[1].add(key)
      this.keyToBucketIdxs.set(key, [])
      for (let i = 0; i < this.numBuckets; i++) {
        this.keyToBucketIdxs.get(key).push(i)
        assignmentCallback(i, key)
      }
      return true
    }

    const mean = this.numBuckets / (this.keyToBucketIdxs.size + 1)
    const numKeysToReassign = Math.floor(mean)
    const evenDistribution = (this.numBuckets % (this.keyToBucketIdxs.size + 1)) === 0

    // key -> num of buckets to snatch
    const snatchMap = new Map()
    for (let i = 0; i < numKeysToReassign; i++) {
      const keySnatchedFrom = this.weightTable[1].values().next().value
      this.weightTable[1].delete(keySnatchedFrom)
      this.weightTable[0].add(keySnatchedFrom)
      const numSnatchForThisKey = snatchMap.get(keySnatchedFrom)
      if (numSnatchForThisKey === undefined) {
        snatchMap.set(keySnatchedFrom, 1)
      } else {
        snatchMap.set(keySnatchedFrom, numSnatchForThisKey + 1)
      }

      if (this.weightTable[1].size === 0) {
        this.weightTable.pop()
        // insert empty list at the beginning
        this.weightTable.unshift(new Set())
      }
    }


    if (evenDistribution) {
      this.weightTable[1].add(key)
    } else {
      this.weightTable[0].add(key)
    }

    const bucketIdxsForNewKey = []
    this.keyToBucketIdxs.set(key, bucketIdxsForNewKey)
    snatchMap.forEach((numSnatch, snatchKey) => {
      const bucketIdxs = this.keyToBucketIdxs.get(snatchKey)
      for (let i = 0; i < numSnatch; i++) {
        const bucketIdx = bucketIdxs.pop()
        bucketIdxsForNewKey.push(bucketIdx)
        assignmentCallback(bucketIdx, key)
        unassignmentCallback(bucketIdx, snatchKey)
      }
    })

    return true
  }

  removeKeyFromWeightMap(key) {
    if (this.weightTable[0].delete(key)) {
      return  
    }

    this.weightTable[1].delete(key)

    // If there's only 1 non-empty level, it should be the top level
    if (this.weightTable[1].size === 0 &&
        this.weightTable[0].size > 0) {
      this.weightTable.pop()
      this.weightTable.unshift(new Set())
    }
  }

  replaceKeyInWeightMap(oldKey, newKey) {
    if (this.weightTable[0].delete(oldKey)) {
      this.weightTable[0].add(newKey)
      return
    }

    this.weightTable[1].delete(oldKey)
    this.weightTable[1].add(newKey)
  }

  removeKey(key, assignmentCallback) {
    const orphannedBucketIdxs = this.keyToBucketIdxs.get(key)
    // Non-existent key
    if (orphannedBucketIdxs === undefined) {
      return false
    } else if (this.reserveKeys.length !== 0) {
      this.keyToBucketIdxs.delete(key)
      const newKey = this.reserveKeys.pop()
      this.keyToBucketIdxs.set(newKey, orphannedBucketIdxs)
      orphannedBucketIdxs.forEach(bucketIdx => {
        assignmentCallback(bucketIdx, newKey)
      })
      this.replaceKeyInWeightMap(key, newKey)

      return true
    } else if (this.numKeys() === 1) {
      // Removing the last key
      this.keyToBucketIdxs.clear()
      this.weightTable = [new Set(),new Set()]
      return true
    }

    this.keyToBucketIdxs.delete(key)
    this.removeKeyFromWeightMap(key)
    // Get the bucket indices for the key
    orphannedBucketIdxs.forEach(bucketIdx => {
      // Get the lowest weight key
      if (this.weightTable[0].size === 0) {
        const keyToHandleOrphannedBucket = this.weightTable[1].values().next().value
        this.weightTable[1].delete(keyToHandleOrphannedBucket)
        this.weightTable.push(new Set())
        this.weightTable.shift()
        this.weightTable[1].add(keyToHandleOrphannedBucket)
        this.keyToBucketIdxs.get(keyToHandleOrphannedBucket).push(bucketIdx)
        assignmentCallback(bucketIdx, keyToHandleOrphannedBucket)
      } else {
        const keyToHandleOrphannedBucket = this.weightTable[0].values().next().value
        this.weightTable[0].delete(keyToHandleOrphannedBucket)
        this.weightTable[1].add(keyToHandleOrphannedBucket)
        this.keyToBucketIdxs.get(keyToHandleOrphannedBucket).push(bucketIdx)
        assignmentCallback(bucketIdx, keyToHandleOrphannedBucket)
      }
    })
    
    return true
  }
}

module.exports = BucketAssigner