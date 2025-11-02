// BucketAssigner.test.js
const BucketAssigner = require('./BucketAssigner');

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

// Return array with opimal distribution, i.e distribution with minimum variance
// param {number} numBuckets - Total number of buckets
// param {number} numKeys - Total number of keys
function getOptimalDistribution(numBuckets, numKeys) {
  if(numBuckets == 0 || numKeys == 0) return []
  else if (numBuckets < numKeys) return Array(numBuckets).fill(1)
  const mean = numBuckets / numKeys;
  const meanFloor = Math.floor(mean);
  const meanCeil = Math.ceil(mean);
  const remainder = numBuckets % numKeys;
  const equalDistributionPossible = remainder === 0;

  let expected = []
  if (equalDistributionPossible) {
    expected = Array(numKeys).fill(numBuckets / numKeys);
  } else {
    const floorDistance = mean - meanFloor;
    const ceilDistance = meanCeil - mean;

    // If youre thinking,  it's same as Math.ceil(numKeys / 2), you're wrong.
    // It's not when numKeys is even, it's when numKeys is odd.
    const higherNumber = Math.floor(numKeys / 2) + 1;
    const lowerNumber = numKeys - higherNumber;

    if (floorDistance === ceilDistance) {
      expected = Array(Math.floor(numKeys / 2)).fill(meanFloor).
        concat(Array(Math.ceil(numKeys / 2)).fill(meanCeil)).sort();
    } else if (floorDistance < ceilDistance) {
      expected = Array(Math.ceil(higherNumber)).fill(meanFloor).
        concat(Array(lowerNumber).fill(meanCeil));
    } else {
      expected = Array(Math.ceil(lowerNumber)).fill(meanFloor).
        concat(Array(Math.floor(higherNumber)).fill(meanCeil));
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
  console.log(`Expected: ${JSON.stringify(expected)}, actual: ${JSON.stringify(actual)}`)
  if (expected.reduce((acc, val) => {
    acc += val;
    return acc;
  }, 0) !== numBuckets) {
    throw new Error('Expected distribution does not sum up to numBuckets');
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

  const assignmentCallback = (bucketIdx, key) => {
    if (!keyToBucketIdxs.has(key)) {
      keyToBucketIdxs.set(key, []);
    }
    const bucketList = keyToBucketIdxs.get(key);
    bucketList.push(bucketIdx);
  };

  const unassignmentCallback = (bucketIdx, key) => {
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

  test('empty and full states', () => {
    expect(assigner.empty()).toBe(true);
    expect(assigner.full()).toBe(false);

    assigner.addKey('key1', assignmentCallback, unassignmentCallback);

    expect(verifyOptimalDistribution(keyToBucketIdxs, 5, 1)).toBe(true);

    assigner.addKey('key2', assignmentCallback, unassignmentCallback);
    expect(verifyOptimalDistribution(keyToBucketIdxs, 5, 2)).toBe(true);

    assigner.addKey('key3', assignmentCallback, unassignmentCallback);
    expect(verifyOptimalDistribution(keyToBucketIdxs, 5, 3)).toBe(true);

    assigner.addKey('key4', assignmentCallback, unassignmentCallback);
    expect(verifyOptimalDistribution(keyToBucketIdxs, 5, 4)).toBe(true);

    assigner.addKey('key5', assignmentCallback, unassignmentCallback);
    expect(verifyOptimalDistribution(keyToBucketIdxs, 5, 5)).toBe(true);

    expect(assigner.empty()).toBe(false);
    expect(assigner.full()).toBe(true);
  });

  test('adding first key', () => {
    let result = assigner.addKey('key1', assignmentCallback, unassignmentCallback);
    expect(result).toBe(true);
    result = assigner.addKey('key1', assignmentCallback, unassignmentCallback);
    expect(result).toBe(false);

    expect(keyToBucketIdxs.size).toBe(1);
  });

  test('adding key when full', () => {
    assigner.addKey('key1', assignmentCallback, unassignmentCallback);
    assigner.addKey('key2', assignmentCallback, unassignmentCallback);
    assigner.addKey('key3', assignmentCallback, unassignmentCallback);
    assigner.addKey('key4', assignmentCallback, unassignmentCallback);
    assigner.addKey('key5', assignmentCallback, unassignmentCallback);
    
    const result = assigner.addKey('key6', assignmentCallback, unassignmentCallback);
    expect(result).toBe(true);
  });

  test('removing non-existent key', () => {
    const result = assigner.removeKey('nonexistent', assignmentCallback);
    expect(result).toBe(false);
  });

  test('removing last key', () => {
    assigner.addKey('key1', assignmentCallback, unassignmentCallback);
    keyToBucketIdxs.delete('key1');
    const result = assigner.removeKey('key1', assignmentCallback);
    
    expect(result).toBe(true);
    expect(assigner.empty()).toBe(true);
    verifyOptimalDistribution(keyToBucketIdxs, 0, 0);
  });

  test('removing key with reserve keys', () => {
    assigner.addKey('key1', assignmentCallback, unassignmentCallback);
    assigner.addKey('key2', assignmentCallback, unassignmentCallback);
    assigner.addKey('key3', assignmentCallback, unassignmentCallback);
    assigner.addKey('key4', assignmentCallback, unassignmentCallback);
    assigner.addKey('key5', assignmentCallback, unassignmentCallback); // goes to reserve

    const result = assigner.removeKey('key1', assignmentCallback);
    
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

  const assignmentCallback = (bucketIdx, key) => {
    if (!keyToBucketIdxs.has(key)) {
      keyToBucketIdxs.set(key, []);
    }
    const bucketList = keyToBucketIdxs.get(key);
    bucketList.push(bucketIdx);
  };

  const unassignmentCallback = (bucketIdx, key) => {
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

  test('prime number of buckets with coprime number of keys', () => {
    // Prime number of buckets (7) with coprime number of keys (4)
    // This tests if the distribution logic handles non-divisible cases correctly
    assigner = new BucketAssigner(7);
    for (let i = 0; i < 4; i++) {
      assigner.addKey(`key${i}`, assignmentCallback, unassignmentCallback);
      expect(verifyOptimalDistribution(keyToBucketIdxs, 7, i + 1)).toBe(true);
    }
  });

  test('rapid remove-add cycle with same key', () => {
    // This tests if internal state gets corrupted when same key is rapidly removed and added
    assigner = new BucketAssigner(5);
    for (let i = 0; i < 100; i++) {
      assigner.addKey('key1', assignmentCallback, unassignmentCallback);
      keyToBucketIdxs.delete('key1'); // Client handles cleanup before removal
      assigner.removeKey('key1', assignmentCallback);
    }
    assigner.addKey('key1', assignmentCallback, unassignmentCallback);
    expect(verifyOptimalDistribution(keyToBucketIdxs, 5, 1)).toBe(true);
  });

  test('fibonacci sequence buckets and keys', () => {
    // Test with Fibonacci numbers which have special division properties
    const fib = [1, 1, 2, 3, 5, 8, 13];
    for (let i = 2; i < fib.length; i++) {
      assigner = new BucketAssigner(fib[i]);
      for (let j = 0; j < fib[i - 1]; j++) {
        assigner.addKey(`key${j}`, assignmentCallback, unassignmentCallback);
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
        assigner.addKey(`key${j}`, assignmentCallback, unassignmentCallback);
      }
      console.log(`i : ${i}`)
      expect(verifyOptimalDistribution(keyToBucketIdxs, 16, i)).toBe(true);
    }
  });

  test('bucket index uniqueness', () => {
    // Test if any bucket index is assigned more than once
    assigner = new BucketAssigner(5);
    assigner.addKey('key1', assignmentCallback, unassignmentCallback);
    assigner.addKey('key2', assignmentCallback, unassignmentCallback);

    const allBuckets = Array.from(keyToBucketIdxs.values()).flat();
    const uniqueBuckets = new Set(allBuckets);
    expect(allBuckets.length).toBe(uniqueBuckets.size);
  });

  test('distribution symmetry', () => {
    // Test if order of key addition affects final distribution
    assigner = new BucketAssigner(6);
    const sequences = [
      ['key1', 'key2', 'key3'],
      ['key3', 'key1', 'key2'],
      ['key2', 'key3', 'key1']
    ];

    const distributions = sequences.map(seq => {
      keyToBucketIdxs.clear();
      seq.forEach(key => assigner.addKey(key, assignmentCallback, unassignmentCallback));
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
    keys.forEach(key => assigner.addKey(key, assignmentCallback, unassignmentCallback));

    // Remove keys in specific order to try to break the redistribution logic
    keys.forEach(key => {
      assigner.removeKey(key, assignmentCallback);
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
    for (let i = 0; i < 7; i++) {
      assigner.addKey(`key${i}`, assignmentCallback, unassignmentCallback);
    }

    // Remove middle key to force maximum redistribution
    assigner.removeKey('key3', assignmentCallback);
    expect(verifyOptimalDistribution(keyToBucketIdxs, 8, 6)).toBe(true);
  });
});