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
  if( numBuckets == 0 || numKeys == 0) return [];
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
  if (expected.reduce((acc, val) => {
    acc += val;
    return acc;
  }, 0) !== numBuckets) {
    throw new Error('Expected distribution does not sum up to numBuckets');
  }

  const actual = []

  Array.from(testMap.values()).forEach((buckets) => actual.push(buckets.length));
  actual.sort();
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