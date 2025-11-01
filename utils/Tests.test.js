// BucketAssigner.test.js
const BucketAssigner = require('./BucketAssigner');

describe('BucketAssigner_BasicTests', () => {
  let assigner;
  let keyToBucketIdxs;
  
  beforeEach(() => {
    assigner = new BucketAssigner(5);
    keyToBucketIdxs = new Map();
  });

  const assignmentCallback = (bucketIdx, key) => {
    console.log('Assigning bucket', bucketIdx, 'to key', key);
    if (!keyToBucketIdxs.has(key)) {
      keyToBucketIdxs.set(key, []);
    }
    const bucketList = keyToBucketIdxs.get(key);
    bucketList.push(bucketIdx);
  };

  const unassignmentCallback = (bucketIdx, key) => {
    console.log('Unassigning bucket', bucketIdx, 'from key', key);
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

  function verifyOptimalDistribution(testMap, numBuckets, numKeys)  {
    const mean = numBuckets / numKeys;
    const floor = Math.floor(mean);
    const ceil = Math.ceil(mean);
    const remainder = numBuckets % numKeys;
    const equalDistributionPossible = remainder === 0;

    console.log(`Floor: ${floor}, Ceil: ${ceil}, Mean: ${mean}, Remainder: ${remainder}, EqualDistributionPossible: ${equalDistributionPossible}`);
    let expected = []
    if (equalDistributionPossible) {
      expected = Array(numKeys).fill(numBuckets / numKeys);
    } else {
      const floorDistance = mean - floor;
      const ceilDistance = ceil - mean;

      // If youre thinking,  it's same as Math.ceil(numKeys / 2), you're wrong.
      // It's not when numKeys is even, it's when numKeys is odd that it differs.
      const higherNumber = Math.floor(numKeys / 2) + 1;
      const lowerNumber = numKeys - higherNumber;

      if (floorDistance === ceilDistance) {
        expected = Array(Math.floor(numKeys / 2)).fill(floor).
                   concat(Array(Math.ceil(numKeys / 2)).fill(ceil)).sort();
      } else if (floorDistance < ceilDistance) {
        expected = Array(Math.ceil(higherNumber)).fill(floor).
                   concat(Array(lowerNumber).fill(ceil));
      } else {
        expected = Array(Math.ceil(lowerNumber)).fill(floor).
                   concat(Array(Math.floor(higherNumber)).fill(ceil));
      }
    }

    const actual = []
    
    Array.from(testMap.values()).forEach((buckets) => actual.push(buckets.length));
    actual.sort();
    console.log('Expected distribution:', expected);
    console.log('Actual distribution:', actual);
    console.log('Test map:', testMap);
    return arraysAreEqual(expected, actual);
  }

  test('constructor initializes correctly', () => {
    expect(assigner.numBuckets).toBe(5);
    expect(assigner.weightTable).toEqual([[], []]);
    expect(assigner.keyToBucketIdxs.size).toBe(0);
    expect(assigner.reserveKeys).toEqual([]);
  });

  test('empty and full states', () => {
    expect(assigner.empty()).toBe(true);
    expect(assigner.full()).toBe(false);

    console.log('Adding key1')
    assigner.addKey('key1', assignmentCallback, unassignmentCallback);

    console.log('keyToBucketIdxs:', keyToBucketIdxs);
    expect(verifyOptimalDistribution(keyToBucketIdxs, 5, 1)).toBe(true);

    console.log('Adding key2')
    assigner.addKey('key2', assignmentCallback, unassignmentCallback);
    expect(verifyOptimalDistribution(keyToBucketIdxs, 5, 2)).toBe(true);

    console.log('Adding key3')
    assigner.addKey('key3', assignmentCallback, unassignmentCallback);
    expect(verifyOptimalDistribution(keyToBucketIdxs, 5, 3)).toBe(true);

    console.log('Adding key4')
    assigner.addKey('key4', assignmentCallback, unassignmentCallback);
    expect(verifyOptimalDistribution(keyToBucketIdxs, 5, 4)).toBe(true);

    console.log('Adding key5')
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

    expect(assigner.reserveKeys.length).toBe(0);
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

    expect(assigner.weightTable[1].length).toEqual(5);
    expect(assigner.reserveKeys).toContain('key6');
    //assigner.removeKey('key1', assignmentCallback);
    //expect(assigner.reserveKeys.length).toBe(0);
    const myMap = keyToBucketIdxs
    console.log('Reserve Keys after removal:', myMap);
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
    expect(assigner.weightTable).toEqual([[], []]);
    //this.verifyOptimalDistribution(0, 0);
  });

  test('removing key with reserve keys', () => {
    assigner.addKey('key1', assignmentCallback, unassignmentCallback);
    assigner.addKey('key2', assignmentCallback, unassignmentCallback);
    assigner.addKey('key3', assignmentCallback, unassignmentCallback);
    assigner.addKey('key4', assignmentCallback, unassignmentCallback);
    assigner.addKey('key5', assignmentCallback, unassignmentCallback); // goes to reserve

    const result = assigner.removeKey('key1', assignmentCallback);
    
    expect(result).toBe(true);
    expect(assigner.reserveKeys).not.toContain('key5');
  });
});