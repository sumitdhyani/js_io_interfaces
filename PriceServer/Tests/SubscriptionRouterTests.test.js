// ...existing code...
const SubscriptionRouter = require('../SubscriptionRouter');
const BucketAssigner = require('../../utils/BucketAssigner'); // <--- added


// ...existing code...

describe('SubscriptionRouter - core logic robustness (targeted at core behavior)', () => {
  let router;
  let getInstrumentListFromBucketFunction;
  let sendSubscriptionFunction;
  let sendUnsubscriptionFunction;
  let bucketAssigner; // <--- added

  beforeEach(() => {
    getInstrumentListFromBucketFunction = jest.fn(() => ['ins-A', 'ins-B']);
    sendSubscriptionFunction = jest.fn();
    sendUnsubscriptionFunction = jest.fn();

    // create a real BucketAssigner and pass it to the router
    bucketAssigner = new BucketAssigner(10); // 10 buckets for tests

    // construct with a real BucketAssigner; we only exercise SubscriptionRouter methods
    router = new SubscriptionRouter(
      bucketAssigner,
      getInstrumentListFromBucketFunction,
      sendSubscriptionFunction,
      sendUnsubscriptionFunction
    );
  });

  test('onSubscriptionRequest/unsubscription: routes only when bucket->key exists', () => {
    // Purpose: Verify basic routing logic for subscription/unsubscription requests.
    // Mechanics: when no mapping exists the call returns false and no send function is invoked;
    // when a mapping exists the corresponding send function is called once and true returned.
    expect(router.onSubscriptionRequest(10, { foo: 1 })).toBe(false);
    expect(sendSubscriptionFunction).not.toHaveBeenCalled();

    // create mapping manually and verify routing uses it
    router.bucketIdToKey.set(10, 'provider-10');
    expect(router.onSubscriptionRequest(10, { foo: 2 })).toBe(true);
    expect(sendSubscriptionFunction).toHaveBeenCalledTimes(1);
    expect(sendSubscriptionFunction).toHaveBeenCalledWith('provider-10', { foo: 2 });

    // unsubscription mirrors the same contract
    expect(router.onUnsubscriptionRequest(11, { id: 7 })).toBe(false);
    router.bucketIdToKey.set(11, 'provider-11');
    expect(router.onUnsubscriptionRequest(11, { id: 8 })).toBe(true);
    expect(sendUnsubscriptionFunction).toHaveBeenCalledWith('provider-11', { id: 8 });
  });

  test('onKeyAssignment should subscribe bucket instruments and update maps', () => {
    // Purpose: Ensure assigning a (bucket,key) pair:
    // - calls getInstrumentListFromBucketFunction(bucket)
    // - invokes sendSubscriptionFunction for each returned instrument
    // - sets bucketIdToKey[bucket] = key
    // - adds bucket to keytoBucketIds[key]
    //
    // Mechanics: call onKeyAssignment and assert the above invariants.
    router.onKeyAssignment(5, 'key-5');

    // instrument list must be queried for the bucket
    expect(getInstrumentListFromBucketFunction).toHaveBeenCalledWith(5);

    // expects 2 subscription attempts (as our mock returns 2 instruments)
    expect(sendSubscriptionFunction).toHaveBeenCalledTimes(2);

    // mapping must be recorded
    expect(router.bucketIdToKey.get(5)).toBe('key-5');

    // key->bucket set must exist and contain the bucket
    const setForKey = router.keytoBucketIds.get('key-5');
    expect(setForKey).toBeDefined();
    expect(setForKey.has(5)).toBe(true);
  });

  test('onBucketAssignment moves bucket from old key to new key and subscribes new key instruments', () => {
    // Purpose: When a bucket moves from oldKey -> newKey, ensure:
    // - oldKey no longer contains the bucket
    // - newKey contains the bucket
    // - subscriptions for newKey are created
    //
    // Mechanics: Prepare an existing mapping and call onBucketAssignment.
    router.bucketIdToKey.set(7, 'old-key');
    router.keytoBucketIds.set('old-key', new Set([7]));

    router.onBucketAssignment(7, 'new-key');

    // we should have subscribed for the instruments for bucket 7
    expect(getInstrumentListFromBucketFunction).toHaveBeenCalledWith(7);
    expect(sendSubscriptionFunction).toHaveBeenCalled();

    // bucket mapping must now point to new-key
    expect(router.bucketIdToKey.get(7)).toBe('new-key');

    // old-key must no longer contain bucket 7
    const oldSet = router.keytoBucketIds.get('old-key');
    if (oldSet) expect(oldSet.has(7)).toBe(false);

    // new-key must contain bucket 7
    const newSet = router.keytoBucketIds.get('new-key');
    expect(newSet).toBeDefined();
    expect(newSet.has(7)).toBe(true);
  });

  test('onBucketUnassignment unsubscribes instruments and removes bucket from key set', () => {
    // Purpose: Verify unassignment removes the bucket from the key's set and
    // calls unsubscription for every instrument of the bucket.
    //
    // Mechanics: prepare mapping where key owns multiple buckets, call unassignment,
    // and assert that bucket is removed from the set and unsubscriptions were sent.
    router.bucketIdToKey.set(8, 'k8');
    router.keytoBucketIds.set('k8', new Set([8, 9]));

    router.onBucketUnassignment(8, 'k8');

    // unsubscription calls for the bucket's instruments
    expect(getInstrumentListFromBucketFunction).toHaveBeenCalledWith(8);
    expect(sendUnsubscriptionFunction).toHaveBeenCalledTimes(2);

    // bucket should be removed from the key's set (if the implementation preserves the set)
    const setAfter = router.keytoBucketIds.get('k8');
    if (setAfter) {
      expect(setAfter.has(8)).toBe(false);
    }

    // bucket->key mapping must not point to the unassigned key anymore
    const mapping = router.bucketIdToKey.get(8);
    expect(mapping === 'k8' ? false : true).toBe(true);
  });

  test('onPriceProviderDown clears mappings for provider and calls notifyKeyDeletionFunction', () => {
    // Purpose: When a provider goes down:
    // - all bucket->key references for that key must be removed
    // - notifyKeyDeletionFunction must be invoked once with (key, handler)
    //
    // Mechanics: prepare several buckets owned by the key, set a spy for notifyKeyDeletionFunction
    // and then call onPriceProviderDown.
    router.keytoBucketIds.set('bad-key', new Set([1, 2, 3]));
    router.bucketIdToKey.set(1, 'bad-key');
    router.bucketIdToKey.set(2, 'bad-key');
    router.bucketIdToKey.set(3, 'bad-key');

    router.notifyKeyDeletionFunction = jest.fn();

    router.onPriceProviderDown('bad-key');

    // bucket->key mappings for those buckets should be removed
    expect(router.bucketIdToKey.has(1)).toBe(false);
    expect(router.bucketIdToKey.has(2)).toBe(false);
    expect(router.bucketIdToKey.has(3)).toBe(false);

    // notifyKeyDeletionFunction must have been called exactly once with key and a handler
    expect(router.notifyKeyDeletionFunction).toHaveBeenCalledTimes(1);
    const callArgs = router.notifyKeyDeletionFunction.mock.calls[0];
    expect(callArgs[0]).toBe('bad-key');
    expect(typeof callArgs[1]).toBe('function');
  });

  test('onPriceProviderUp invokes notifyKeyAdditionFunction with handlers', () => {
    // Purpose: Ensure bringing a provider up results in notifyKeyAdditionFunction being called
    // with the proper handler functions for assignment/unassignment.
    router.notifyKeyAdditionFunction = jest.fn();

    router.onPriceProviderUp('new-key');

    expect(router.notifyKeyAdditionFunction).toHaveBeenCalledTimes(1);
    const args = router.notifyKeyAdditionFunction.mock.calls[0];
    expect(args[0]).toBe('new-key');
    expect(typeof args[1]).toBe('function'); // expected assignment handler
    expect(typeof args[2]).toBe('function'); // expected unassignment handler
  });
});
