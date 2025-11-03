// BucketAssigner.test.js
const BucketAssigner = require('./BucketAssigner');

const assignmentCallback = (keyToBucketIdxs, bucketIdx, key) => {
  if (!keyToBucketIdxs.has(key)) {
    keyToBucketIdxs.set(key, []);
  }
  const bucketList = keyToBucketIdxs.get(key);
  bucketList.push(bucketIdx);
};

const unassignmentCallback = (keyToBucketIdxs, bucketIdx, key) => {
  if (keyToBucketIdxs.has(key)) {
    const bucketList = keyToBucketIdxs.get(key);
    let keyIndex = bucketList.indexOf(bucketIdx);
    if (keyIndex !== -1) {
      bucketList.splice(keyIndex, 1);
    }
    if (bucketList.length === 0) {
      keyToBucketIdxs.delete(key);
    }
  }
};

function getAssignmentCallback(keyToBucketIdxs)
{
  return (bucketIdx, key) =>{
    assignmentCallback(keyToBucketIdxs, bucketIdx, key)
  }
}

function getUnassignmentCallback(keyToBucketIdxs) {
  return (bucketIdx, key) => {
    unassignmentCallback(keyToBucketIdxs, bucketIdx, key)
  }
}


function arraysAreEqual(arr1, arr2) {
  if (arr1.length !== arr2.length) {
    return false;
  }
  for (let i = 0; i < arr1.length; i++) {
    if (arr1[i] !== arr2[i]) {
      return false;
    }
  }
  return true;
}

function logInternalState(assigner) {
  const netSum = Array.from(assigner.keyToBucketIdxs.values()).reduce((acc, list) =>{
    return acc + list.length
  }, 0)
  console.log(`Internal State: ${JSON.stringify(Array.from(assigner.keyToBucketIdxs.entries()))}, \nreserve: ${JSON.stringify(assigner.reserveKeys)}, \nNetSum: ${netSum}, \nweightTable: ${JSON.stringify(assigner.weightTable)}`)

}
// Return array with opimal distribution, i.e distribution with minimum variance
// param {number} numBuckets - Total number of buckets
// param {number} numKeys - Total number of keys
function getOptimalDistribution(numBuckets, numKeys) {
  if (numBuckets == 0 || numKeys == 0) return []
  else if (numBuckets < numKeys) return Array(numBuckets).fill(1)
  else if (numBuckets % numKeys == 0) return Array(numKeys).fill(numBuckets / numKeys)



  const mean = numBuckets / numKeys;
  const meanFloor = Math.floor(mean);
  const meanCeil = Math.ceil(mean);

  let expected = []
  let totalBucketsUnassigned = numBuckets;
  let totalKeysLeft = numKeys
  while (totalBucketsUnassigned > 0) {
    if (totalBucketsUnassigned === totalKeysLeft * meanCeil) {
      expected = expected.concat(Array(totalKeysLeft).fill(meanCeil))
      break;
    } else {
      totalBucketsUnassigned -= meanFloor
      totalKeysLeft -= 1
      expected.push(meanFloor)
    }
  }

  return expected;
}

// Test helper to verify if the distribution of buckets among keys is optimal
// param {Map} testMap - Map of key to list of bucket indices assigned to it
// param {number} numBuckets - Total number of buckets
// param {number} numKeys - Total number of keys
function verifyOptimalDistribution(testMap, numBuckets, numKeys) {

  const expected = getOptimalDistribution(numBuckets, numKeys);
  

  const actual = []

  Array.from(testMap.values()).forEach((buckets) => actual.push(buckets.length));
  actual.sort();
  //console.log(`Expected: ${JSON.stringify(expected)}, actual: ${JSON.stringify(actual)}`)
  if (expected.reduce((acc, val) => {
    acc += val;
    return acc;
  }, 0) !== numBuckets && numKeys > 0 && numBuckets > 0) {
    throw new Error(`Expected distribution does not sum up to numBuckets, `);
  }
  return arraysAreEqual(expected, actual);
}


describe('BucketAssigner_BasicTests', () => {
  let assigner;
  let keyToBucketIdxs;
  
  beforeEach(() => {
    assigner = new BucketAssigner(5);
    keyToBucketIdxs = new Map();
  });

  test('empty and full states', () => {
    expect(assigner.empty()).toBe(true);
    expect(assigner.full()).toBe(false);

    assigner.addKey('key1', getAssignmentCallback(keyToBucketIdxs), getUnassignmentCallback(keyToBucketIdxs));

    expect(verifyOptimalDistribution(keyToBucketIdxs, 5, 1)).toBe(true);

    assigner.addKey('key2', getAssignmentCallback(keyToBucketIdxs), getUnassignmentCallback(keyToBucketIdxs));
    expect(verifyOptimalDistribution(keyToBucketIdxs, 5, 2)).toBe(true);

    assigner.addKey('key3', getAssignmentCallback(keyToBucketIdxs), getUnassignmentCallback(keyToBucketIdxs));
    expect(verifyOptimalDistribution(keyToBucketIdxs, 5, 3)).toBe(true);

    assigner.addKey('key4', getAssignmentCallback(keyToBucketIdxs), getUnassignmentCallback(keyToBucketIdxs));
    expect(verifyOptimalDistribution(keyToBucketIdxs, 5, 4)).toBe(true);

    assigner.addKey('key5', getAssignmentCallback(keyToBucketIdxs), getUnassignmentCallback(keyToBucketIdxs));
    expect(verifyOptimalDistribution(keyToBucketIdxs, 5, 5)).toBe(true);

    expect(assigner.empty()).toBe(false);
    expect(assigner.full()).toBe(true);
  });

  test('adding first key', () => {
    let result = assigner.addKey('key1', getAssignmentCallback(keyToBucketIdxs), getUnassignmentCallback(keyToBucketIdxs));
    expect(result).toBe(true);
    result = assigner.addKey('key1', getAssignmentCallback(keyToBucketIdxs), getUnassignmentCallback(keyToBucketIdxs));
    expect(result).toBe(false);

    expect(keyToBucketIdxs.size).toBe(1);
  });

  test('adding key when full', () => {
    assigner.addKey('key1', getAssignmentCallback(keyToBucketIdxs), getUnassignmentCallback(keyToBucketIdxs));
    assigner.addKey('key2', getAssignmentCallback(keyToBucketIdxs), getUnassignmentCallback(keyToBucketIdxs));
    assigner.addKey('key3', getAssignmentCallback(keyToBucketIdxs), getUnassignmentCallback(keyToBucketIdxs));
    assigner.addKey('key4', getAssignmentCallback(keyToBucketIdxs), getUnassignmentCallback(keyToBucketIdxs));
    assigner.addKey('key5', getAssignmentCallback(keyToBucketIdxs), getUnassignmentCallback(keyToBucketIdxs));
    
    const result = assigner.addKey('key6', getAssignmentCallback(keyToBucketIdxs), getUnassignmentCallback(keyToBucketIdxs));
    expect(result).toBe(true);
  });

  test('removing non-existent key', () => {
    const result = assigner.removeKey('nonexistent', getAssignmentCallback(keyToBucketIdxs));
    expect(result).toBe(false);
  });

  test('removing last key', () => {
    assigner.addKey('key1', getAssignmentCallback(keyToBucketIdxs), getUnassignmentCallback(keyToBucketIdxs));
    keyToBucketIdxs.delete('key1');
    const result = assigner.removeKey('key1', getAssignmentCallback(keyToBucketIdxs));
    
    expect(result).toBe(true);
    expect(assigner.empty()).toBe(true);
    verifyOptimalDistribution(keyToBucketIdxs, 0, 0);
  });

  test('removing key with reserve keys', () => {
    assigner.addKey('key1', getAssignmentCallback(keyToBucketIdxs), getUnassignmentCallback(keyToBucketIdxs));
    assigner.addKey('key2', getAssignmentCallback(keyToBucketIdxs), getUnassignmentCallback(keyToBucketIdxs));
    assigner.addKey('key3', getAssignmentCallback(keyToBucketIdxs), getUnassignmentCallback(keyToBucketIdxs));
    assigner.addKey('key4', getAssignmentCallback(keyToBucketIdxs), getUnassignmentCallback(keyToBucketIdxs));
    assigner.addKey('key5', getAssignmentCallback(keyToBucketIdxs), getUnassignmentCallback(keyToBucketIdxs)); // goes to reserve

    const result = assigner.removeKey('key1', getAssignmentCallback(keyToBucketIdxs));
    
    expect(result).toBe(true);
  });
});

// ...existing code...
describe('BucketAssigner_CoreLogicTests', () => {
  let assigner;
  let keyToBucketIdxs;

  beforeEach(() => {
    keyToBucketIdxs = new Map();
  });

  test('prime number of buckets with coprime number of keys', () => {
    // Prime number of buckets (7) with coprime number of keys (4)
    // This tests if the distribution logic handles non-divisible cases correctly
    assigner = new BucketAssigner(7);
    for (let i = 0; i < 4; i++) {
      assigner.addKey(`key${i}`, getAssignmentCallback(keyToBucketIdxs), getUnassignmentCallback(keyToBucketIdxs));
      expect(verifyOptimalDistribution(keyToBucketIdxs, 7, i + 1)).toBe(true);
    }
  });

  test('rapid remove-add cycle with same key', () => {
    // This tests if internal state gets corrupted when same key is rapidly removed and added
    assigner = new BucketAssigner(5);
    for (let i = 0; i < 100; i++) {
      assigner.addKey('key1', getAssignmentCallback(keyToBucketIdxs), getUnassignmentCallback(keyToBucketIdxs));
      keyToBucketIdxs.delete('key1'); // Client handles cleanup before removal
      assigner.removeKey('key1', getAssignmentCallback(keyToBucketIdxs));
    }
    assigner.addKey('key1', getAssignmentCallback(keyToBucketIdxs), getUnassignmentCallback(keyToBucketIdxs));
    expect(verifyOptimalDistribution(keyToBucketIdxs, 5, 1)).toBe(true);
  });

  test('fibonacci sequence buckets and keys', () => {
    // Test with Fibonacci numbers which have special division properties
    const fib = [1, 1, 2, 3, 5, 8, 13];
    for (let i = 2; i < fib.length; i++) {
      assigner = new BucketAssigner(fib[i]);
      for (let j = 0; j < fib[i - 1]; j++) {
        assigner.addKey(`key${j}`, getAssignmentCallback(keyToBucketIdxs), getUnassignmentCallback(keyToBucketIdxs));
      }
      
      expect(verifyOptimalDistribution(keyToBucketIdxs, fib[i], fib[i - 1])).toBe(true);
      keyToBucketIdxs.clear()
    }
  });

  test('power of two transitions', () => {
    // Test transitions between power-of-two numbers of keys
    // This can expose binary arithmetic errors
    assigner = new BucketAssigner(16);
    let i
    for (i = 1; i <= 16; i *= 2) {
      for (let j = 0; j < i; j++) {
        assigner.addKey(`key${j}`, getAssignmentCallback(keyToBucketIdxs), getUnassignmentCallback(keyToBucketIdxs));
      }
      expect(verifyOptimalDistribution(keyToBucketIdxs, 16, i)).toBe(true);
    }
  });

  test('bucket index uniqueness', () => {
    // Test if any bucket index is assigned more than once
    assigner = new BucketAssigner(5);
    assigner.addKey('key1', getAssignmentCallback(keyToBucketIdxs), getUnassignmentCallback(keyToBucketIdxs));
    assigner.addKey('key2', getAssignmentCallback(keyToBucketIdxs), getUnassignmentCallback(keyToBucketIdxs));

    const allBuckets = Array.from(keyToBucketIdxs.values()).flat();
    const uniqueBuckets = new Set(allBuckets);
    expect(allBuckets.length).toBe(uniqueBuckets.size);
  });

  test('distribution symmetry', () => {
    // Test if order of key addition affects final distribution
    const sequences = [
      ['key1', 'key2', 'key3'],
      ['key3', 'key1', 'key2'],
      ['key2', 'key3', 'key1']
    ];

    const distributions = sequences.map(seq => {
      assigner = new BucketAssigner(6);
      keyToBucketIdxs.clear();
      seq.forEach(key => assigner.addKey(key, getAssignmentCallback(keyToBucketIdxs), getUnassignmentCallback(keyToBucketIdxs)));
      return Array.from(keyToBucketIdxs.values()).map(arr => arr.length).sort();
    }); 

    // All distributions should be identical regardless of addition order
    expect(distributions[0]).toEqual(distributions[1]);
    expect(distributions[1]).toEqual(distributions[2]);
  });

  test('removal chain reaction', () => {
    // Test if removing keys in specific order causes cascade failures
    assigner = new BucketAssigner(7);
    const keys = ['key1', 'key2', 'key3', 'key4', 'key5'];

    // Add all keys
    keys.forEach(key => assigner.addKey(key, getAssignmentCallback(keyToBucketIdxs), getUnassignmentCallback(keyToBucketIdxs)));

    // Remove keys in specific order to try to break the redistribution logic
    keys.forEach(key => {
      keyToBucketIdxs.delete(key)
      assigner.removeKey(key, getAssignmentCallback(keyToBucketIdxs));
      const remainingKeys = keys.filter(k => keyToBucketIdxs.has(k));
      if (remainingKeys.length > 0) {
        expect(verifyOptimalDistribution(keyToBucketIdxs, 7, remainingKeys.length)).toBe(true);
      }
    });
  });

  test('maximum redistribution scenario', () => {
    // Test scenario where maximum number of bucket reassignments needed
    assigner = new BucketAssigner(8);

    // Add keys to create maximum imbalance
    for (let i = 1; i < 8; i++) {
      assigner.addKey(`key${i}`, getAssignmentCallback(keyToBucketIdxs), getUnassignmentCallback(keyToBucketIdxs));
    }

    // Remove middle key to force maximum redistribution
    keyToBucketIdxs.delete(`key4`)
    assigner.removeKey('key4', getAssignmentCallback(keyToBucketIdxs));
    expect(verifyOptimalDistribution(keyToBucketIdxs, 8, 6)).toBe(true);
  });

  test('Recruiting reserve keys', () => {
    // Test scenario where maximum number of bucket reassignments needed
    assigner = new BucketAssigner(8);
    keyToBucketIdxs.clear()

    // Add keys to create maximum imbalance
    for (let i = 0; i < 9; i++) {
      assigner.addKey(`key${i}`, getAssignmentCallback(keyToBucketIdxs), getUnassignmentCallback(keyToBucketIdxs));
    }

    // Remove middle key to force maximum redistribution
    expect(verifyOptimalDistribution(keyToBucketIdxs, 8, 9)).toBe(true);
    keyToBucketIdxs.delete(`key4`)
    assigner.removeKey('key4', getAssignmentCallback(keyToBucketIdxs));
    expect(verifyOptimalDistribution(keyToBucketIdxs, 8, 8)).toBe(true);

    keyToBucketIdxs.delete(`key3`)
    assigner.removeKey('key3', getAssignmentCallback(keyToBucketIdxs));
    expect(verifyOptimalDistribution(keyToBucketIdxs, 8, 7)).toBe(true);
  });
});

describe('BucketAssigner_RobustTests', () => {
  let assigner;
  let keyToBucketIdxs;

  beforeEach(() => {
    keyToBucketIdxs = new Map();
    assigner = new BucketAssigner(5);
  });

  test('sequential random operations with client cleanup before remove', () => {
    const keys = Array.from({ length: 10 }, (_, i) => `k${i}`);
    for (let iter = 0; iter < 200; iter++) {
      const key = keys[Math.floor(Math.random() * keys.length)];
      //console.log(`key: ${key}`)
      if (Math.random() < 0.6) {
        // add
        //logInternalState(assigner)
        //console.log(`addKey: ${key}`)
        assigner.addKey(key, getAssignmentCallback(keyToBucketIdxs), getUnassignmentCallback(keyToBucketIdxs));
        //logInternalState(assigner)
      } else {
        // client must clean up visible state before calling removeKey
        if (keyToBucketIdxs.delete(key)) {
          //logInternalState(assigner)
          //console.log(`removeKey: ${key}`)
          assigner.removeKey(key, getAssignmentCallback(keyToBucketIdxs));
          //logInternalState(assigner)
        }
      }
      // Verify visible assignments are optimal for the current visible key count
      expect(verifyOptimalDistribution(keyToBucketIdxs, 5, keyToBucketIdxs.size)).toBe(true);
    }
  });

  test('clearing client map must coincide with assigner reset', () => {
    // populate
    assigner.addKey('a', getAssignmentCallback(keyToBucketIdxs), getUnassignmentCallback(keyToBucketIdxs));
    assigner.addKey('b', getAssignmentCallback(keyToBucketIdxs), getUnassignmentCallback(keyToBucketIdxs));
    expect(verifyOptimalDistribution(keyToBucketIdxs, 5, keyToBucketIdxs.size)).toBe(true);

    // if client clears its map it must also recreate assigner to keep in sync
    keyToBucketIdxs.clear();
    assigner = new BucketAssigner(5); // recreate to stay in sync
    assigner.addKey('x', getAssignmentCallback(keyToBucketIdxs), getUnassignmentCallback(keyToBucketIdxs));
    expect(verifyOptimalDistribution(keyToBucketIdxs, 5, keyToBucketIdxs.size)).toBe(true);
  });

  test('reserve promotion requires client cleanup prior to remove', () => {
    // fill all buckets
    for (let i = 1; i <= 5; i++) {
      assigner.addKey(`k${i}`, getAssignmentCallback(keyToBucketIdxs), getUnassignmentCallback(keyToBucketIdxs));
    }
    // add a reserve key (no buckets assigned yet)
    assigner.addKey('reserve', getAssignmentCallback(keyToBucketIdxs), getUnassignmentCallback(keyToBucketIdxs));

    // remove an active key: client deletes its local entries first (per contract)
    keyToBucketIdxs.delete('k3');
    const res = assigner.removeKey('k3', getAssignmentCallback(keyToBucketIdxs));
    expect(res).toBe(true);

    // after removal, client-visible assignments should again represent optimal distribution
    expect(verifyOptimalDistribution(keyToBucketIdxs, 5, keyToBucketIdxs.size)).toBe(true);
    // ensure all bucket indices are unique
    const allBuckets = Array.from(keyToBucketIdxs.values()).flat();
    expect(new Set(allBuckets).size).toBe(allBuckets.length);
    // sum of assigned buckets equals numBuckets
    const sumAssigned = allBuckets.length;
    expect(sumAssigned).toBe(5);
  });

  test('idempotent removal semantics (client cleanup + remove)', () => {
    assigner.addKey('z', getAssignmentCallback(keyToBucketIdxs), getUnassignmentCallback(keyToBucketIdxs));
    // client deletes its local view before calling removeKey
    keyToBucketIdxs.delete('z');
    const first = assigner.removeKey('z', getAssignmentCallback(keyToBucketIdxs));
    expect(first).toBe(true);
    // second removal should return false (already removed)
    const second = assigner.removeKey('z', getAssignmentCallback(keyToBucketIdxs));
    expect(second).toBe(false);
  });

  test('no duplicate bucket indices after many cycles', () => {
    assigner = new BucketAssigner(10);
    const names = Array.from({ length: 20 }, (_, i) => `n${i}`);
    //console.log(`names: ${JSON.stringify(names)}`)
    for (let i = 0; i < 200; i++) {
      //console.log(`i: ${i}`)
      const name = names[i % names.length];
      //logInternalState(assigner)
      //console.log(`Addkey: ${name}`) 
      assigner.addKey(name, getAssignmentCallback(keyToBucketIdxs), getUnassignmentCallback(keyToBucketIdxs));
      //logInternalState(assigner)
      // simulate occasional removals with client cleanup
      //console.log(`Internal KeyToBucketIdxs: ${JSON.stringify(Array.from(assigner.keyToBucketIdxs.entries()))}, reserve: ${JSON.stringify(assigner.reserveKeys)}, weightTable: ${JSON.stringify(assigner.weightTable)}`)
      if (i % 7 === 0) {
        const deleteKey = names[(i + 3) % names.length]
        if (keyToBucketIdxs.delete(deleteKey)){
          //logInternalState(assigner)
          //console.log(`deleteKey: ${deleteKey}`)
          assigner.removeKey(deleteKey, getAssignmentCallback(keyToBucketIdxs));
          //logInternalState(assigner)
        }
      }
      // verify visible distribution
      expect(verifyOptimalDistribution(keyToBucketIdxs, 10, keyToBucketIdxs.size)).toBe(true);
      // uniqueness
      const allBuckets = Array.from(keyToBucketIdxs.values()).flat();
      expect(new Set(allBuckets).size).toBe(allBuckets.length);
    }
  });
});