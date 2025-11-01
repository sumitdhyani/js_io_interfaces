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
    this.weightTable = [[],[]]

    this.keyToBucketIdxs = new Map()

    this.reserveKeys = []
  }

  numKeys(){
    return this.keyToBucketIdxs.size
  }

  full(){
    return this.weightTable[1].length === this.numBuckets
  }

  empty() {
    return this.numKeys() == 0
  }

  addKey(key, assignmentCallback, unassignmentCallback)
  {
    // Each key has exactly 1 partition, so this new key can't do anything for now
    if (this.full()) {
      this.reserveKeys.push(key)
      return true
    }
    // Duplicate key
    else if (this.keyToBucketIdxs.has(key)) {
      return false
    }
    // 1st key being added
    else if (this.empty()) {
      this.weightTable[1].push(key)
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

    console.log(`Adding key ${key}: mean=${mean}, numKeysToReassign=${numKeysToReassign}, evenDistribution=${evenDistribution}, weightTable=${JSON.stringify(this.weightTable)}`);
    // key -> num of buckets to snatch
    const snatchMap = new Map()
    for (let i = 0; i < numKeysToReassign; i++) {
      const keySnatchedFrom = this.weightTable[1].pop()
      this.weightTable[0].push(keySnatchedFrom)
      const numSnatchForThisKey = snatchMap.get(keySnatchedFrom)
      if (numSnatchForThisKey === undefined) {
        snatchMap.set(keySnatchedFrom, 1)
      } else {
        snatchMap.set(keySnatchedFrom, numSnatchForThisKey + 1)
      }

      if (this.weightTable[1].length === 0) {
        this.weightTable.pop()
        // insert empty list at the beginning
        this.weightTable.unshift([])
      }
    }

    console.log('Snatch map:', snatchMap);

    if (evenDistribution) {
      this.weightTable[1].push(key)
    } else {
      this.weightTable[0].push(key)
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
    let keyList = this.weightTable[0]
    let keyIndex = keyList.indexOf(key) 
    if (keyIndex !== -1) {
      keyList.splice(keyIndex, 1)
      return
    }

    keyList = this.weightTable[1]
    keyIndex = keyList.indexOf(key)
    if (keyIndex !== -1) {
      keyList.splice(keyIndex, 1)
      return
    }

    if(this.weightTable[1].length === 0) {
      this.weightTable.pop()
      this.weightTable.unshift([])
    }
  }

  replaceKeyInWeightMap(oldKey, newKey) {
    for (let weight = 0; weight < this.weightTable.length; weight++) {
      const keyList = this.weightTable[weight]
      const keyIndex = keyList.indexOf(oldKey)
      if (keyIndex !== -1) {
        keyList[keyIndex] = newKey
        return
      }
    }
  }

  removeKey(key, assignmentCallback) {
    const bucketIdxs = this.keyToBucketIdxs.get(key)
    // Non-existent key
    if (bucketIdxs === undefined) {
      return false
    } else if (this.reserveKeys.length !== 0) {
      this.keyToBucketIdxs.delete(key)
      const newKey = this.reserveKeys.pop()
      this.keyToBucketIdxs.set(newKey, bucketIdxs)
      bucketIdxs.forEach(bucketIdx => {
        assignmentCallback(bucketIdx, newKey)
      })
      this.replaceKeyInWeightMap(key, newKey)

      return true
    } else if (this.numKeys() === 1) {
      // Removing the last key
      this.keyToBucketIdxs.clear()
      this.weightTable = [[],[]]
      return true
    }

    this.keyToBucketIdxs.delete(key)
    this.removeKeyFromWeightMap(key)
    // Get the bucket indices for the key
    bucketIdxs.forEach(bucketIdx => {
      if (this.weightTable[0].length === 0) {
        const keyToReassign = this.weightTable[1].pop()
        this.weightTable.push([])
        this.weightTable.shift()
        this.weightTable[1].push(keyToReassign)
         // Get the lowest weight key
        this.keyToBucketIdxs.get(keyToReassign).push(bucketIdx)
        assignmentCallback(bucketIdx, keyToReassign)
      } else {
        const keyToReassign = this.weightTable[0].pop() // Get the lowest weight key
        this.weightTable[1].push(keyToReassign)
        this.keyToBucketIdxs.get(keyToReassign).push(bucketIdx)  
        assignmentCallback(bucketIdx, keyToReassign)
      }
    })
    
    return true
  }
}

module.exports = BucketAssigner