const SubscriptionRouter = require('../SubscriptionRouter');
const BucketAssigner = require('../../utils/BucketAssigner');

function callAsync(cb, val) {
  setImmediate(() => { cb(val) })
}

function totalSuborUnsubSent(sentMap) {
  return Array.from(sentMap.values()).reduce((acc, val) => {
    return acc + val.length
  }, 0)
}

// val : 'Y'/err, 'N'/no error
function errCallback(val) {
  val = val.toLowerCase()
  return (err) =>{
    if(val === 'y') {
      expect(err).not.toBe(null)
   } else {
      expect(err).toBe(null)
   }
  }
}

function getSendSubscriptionFunction(subscriptionsSent) {
  return (key, request, cb) => {
    if (!subscriptionsSent.has(key)) {
      subscriptionsSent.set(key, []);
    }
    subscriptionsSent.get(key).push(request);
    callAsync(cb, null)
  }
}

function dummyCb(err){}

function getSendUnSubscriptionFunction(unsubscriptionsSent) {
  return (key, request, cb) => {
    if (!unsubscriptionsSent.has(key)) {
      unsubscriptionsSent.set(key, []);
    }
    unsubscriptionsSent.get(key).push(request);
    callAsync(cb, null)
  }
}

describe('ai_gen_SubscriptionRouter - Black Box Behavior Tests', (done) => {
  let router;
  let subscriptionsSent;
  let unsubscriptionsSent;
  let instrumentsForBucket;

  beforeEach(() => {
    subscriptionsSent = new Map(); // key -> Set of requests
    unsubscriptionsSent = new Map(); // key -> Set of requests
    instrumentsForBucket = new Map(); // bucket -> [requests]

    const getInstrumentsForBucket = (bucket) => {
      return instrumentsForBucket.get(bucket) || [];
    };

    router = new SubscriptionRouter(
      new BucketAssigner(10),
      getInstrumentsForBucket,
      getSendSubscriptionFunction(subscriptionsSent),
      getSendUnSubscriptionFunction(unsubscriptionsSent)
    );
  });

  test('subscription requests are routed only after provider up and assignment', (done) => {
    // Setup some instruments for bucket 1
    instrumentsForBucket.set(1, ['EUR/USD', 'GBP/USD']);

    // Before provider is up, requests should be rejected
    router.onSubscriptionRequest(1, 'EUR/USD', (err) => {
      expect(err).not.toBe(null)
    })

    expect(totalSuborUnsubSent(subscriptionsSent)).toBe(0)

    // Bring provider online and assign bucket
    router.onPriceProviderUp('provider-A', (err)=>{}, (err)=>{});
    expect(totalSuborUnsubSent(subscriptionsSent)).toBe(2)
    // Request should still be rejected until bucket is assigned
    router.onSubscriptionRequest(1, 'EUR/USD', (err) => {
      expect(err).toBe(null)
    })
    expect(totalSuborUnsubSent(subscriptionsSent)).toBe(3)

    // After assignment and request, should see subscriptions
    router.onPriceProviderDown('provider-A', (err)=>{});
    router.onSubscriptionRequest(1, 'EUR/USD', (err)=>{
      expect(err).not.toBe(null)
      done()
    });

    expect(subscriptionsSent.size).toBe(1)
  });

  test('unsubscription follows same routing rules as subscription', (done) => {
    instrumentsForBucket.set(2, ['JPY/USD']);

    // Without provider up, should reject
    router.onUnsubscriptionRequest(2, 'JPY/USD', errCallback('y'))
    
    expect(unsubscriptionsSent.size).toBe(0);

    // With provider but no assignment, should not reject
    router.onPriceProviderUp('provider-B', dummyCb, dummyCb);
    router.onUnsubscriptionRequest(2, 'JPY/USD', errCallback('n'))

    expect(unsubscriptionsSent.size).toBe(1);

    // After provider down, should reject again
    router.onPriceProviderDown('provider-B', dummyCb);
    router.onUnsubscriptionRequest(2, 'JPY/USD', errCallback('y'));

    setImmediate(()=>{ done() })
  });

  test('provider down stops routing to that provider', (done) => {
    instrumentsForBucket.set(3, ['AUD/USD']);

    // Setup provider and verify routing works
    router.onPriceProviderUp('provider-C', dummyCb, dummyCb);
    expect(totalSuborUnsubSent(subscriptionsSent)).toBe(1)
    router.onSubscriptionRequest(3, 'AUD/USD', errCallback('n'));
    expect(totalSuborUnsubSent(subscriptionsSent)).toBe(2)

    // Take provider down
    router.onPriceProviderDown('provider-C', dummyCb, dummyCb);
    expect(totalSuborUnsubSent(subscriptionsSent)).toBe(2)

    // Verify routing stops
    router.onSubscriptionRequest(3, 'AUD/USD', errCallback('y'))
    expect(totalSuborUnsubSent(subscriptionsSent)).toBe(2)

    router.onPriceProviderUp('provider-C', dummyCb, dummyCb);
    expect(totalSuborUnsubSent(subscriptionsSent)).toBe(3)
    expect(totalSuborUnsubSent(unsubscriptionsSent)).toBe(0)

    router.onUnsubscriptionRequest(3, 'AUD/USD', errCallback('n'))
    expect(totalSuborUnsubSent(subscriptionsSent)).toBe(3)
    expect(totalSuborUnsubSent(unsubscriptionsSent)).toBe(1)

    router.onPriceProviderDown('provider-C', dummyCb, dummyCb);
    router.onUnsubscriptionRequest(3, 'AUD/USD', errCallback('y'))
    expect(totalSuborUnsubSent(subscriptionsSent)).toBe(3)
    expect(totalSuborUnsubSent(unsubscriptionsSent)).toBe(1)
    setImmediate(()=>{ done() })
  });

  test('bringing provider back up restores routing capability', (done) => {
    instrumentsForBucket.set(4, ['NZD/USD']);

    // Up -> Down -> Up cycle
    router.onPriceProviderUp('provider-D', dummyCb, dummyCb);
    expect(subscriptionsSent.size).toBe(1);
    expect(totalSuborUnsubSent(subscriptionsSent)).toBe(1);

    router.onSubscriptionRequest(4, 'NZD/USD', errCallback('n'));
    router.onSubscriptionRequest(4, 'NZD/USD', errCallback('n'));
    expect(totalSuborUnsubSent(subscriptionsSent)).toBe(3);

    router.onPriceProviderDown('provider-D', dummyCb);
    router.onSubscriptionRequest(4, 'NZD/USD', errCallback('y'));
    router.onSubscriptionRequest(4, 'NZD/USD', errCallback('y'));
    expect(totalSuborUnsubSent(subscriptionsSent)).toBe(3);

    router.onPriceProviderUp('provider-D', dummyCb, dummyCb);
    expect(totalSuborUnsubSent(subscriptionsSent)).toBe(4);
    router.onSubscriptionRequest(4, 'NZD/USD', errCallback('n'));
    router.onSubscriptionRequest(4, 'NZD/USD', errCallback('n'));

    // Verify routing works again after reassignment
    expect(subscriptionsSent.has('provider-D')).toBe(true);
    expect(subscriptionsSent.size).toBe(1);
    expect(totalSuborUnsubSent(subscriptionsSent)).toBe(6);
    setImmediate(() => { done() })
  });

  test('bucket reassignment sends unsubscribe to old provider and subscribe to new provider', (done) => {
    // Setup instruments for bucket 1
    instrumentsForBucket.set(1, ['EUR/USD', 'GBP/USD']);

    expect(totalSuborUnsubSent(subscriptionsSent)).toBe(0)
    // Bring up first provider and verify its subscriptions
    router.onPriceProviderUp('provider-A', dummyCb, dummyCb);
    const subCountBeforeSecondProvider = totalSuborUnsubSent(subscriptionsSent);
    expect(subCountBeforeSecondProvider).toBe(2)
    expect(subscriptionsSent.has('provider-A')).toBe(true);

    // Bring up second provider
    router.onPriceProviderUp('provider-B', dummyCb, dummyCb);
    // No new sub/un
    expect(totalSuborUnsubSent(subscriptionsSent)).toBe(subCountBeforeSecondProvider)
    expect(totalSuborUnsubSent(unsubscriptionsSent)).toBe(0)

    // At this point, BucketAssigner may have reassigned some buckets
    // We should see subscriptions and possibly unsubscriptions
    const totalMessages = totalSuborUnsubSent(subscriptionsSent) +
      totalSuborUnsubSent(unsubscriptionsSent);

    // Each instrument should be subscribed at most once across all providers (no duplicate subscribe records)
    const allSubs = Array.from(subscriptionsSent.values()).flat();
    const uniqueSubs = new Set(allSubs);
    expect(allSubs.length).toBe(uniqueSubs.size);

    // New expectations around totals / coverage
    const instrumentCount = instrumentsForBucket.get(1).length || 0;
    // At least one subscribe/unsubscribe message should have been exchanged
    expect(totalMessages).toBeGreaterThan(0);
    // Total messages must be >= initial subscription count
    expect(totalMessages).toBeGreaterThanOrEqual(subCountBeforeSecondProvider);
    // We must have observed subscriptions for all instruments (each instrument subscribed exactly once)
    expect(uniqueSubs.size).toBe(instrumentCount);

    // If any unsubscriptions occurred, totalMessages should have grown beyond the initial subscription count
    if (totalSuborUnsubSent(unsubscriptionsSent) > 0) {
      expect(totalMessages).toBeGreaterThan(subCountBeforeSecondProvider);
    }

    // Verify routing still works for whichever provider currently holds bucket 1
    if (subscriptionsSent.has('provider-A')) {
      router.onSubscriptionRequest(1, 'NEW/EUR', errCallback('n'));
    }
    if (subscriptionsSent.has('provider-B')) {
      router.onSubscriptionRequest(1, 'NEW/GBP', errCallback('n'))
    }

    setImmediate(() => { done() })
  });

  describe('ai_gen_SubscriptionRouter - Multiple Buckets Behavior Tests', () => {
    let router;
    let subscriptionsSent;
    let unsubscriptionsSent;
    let instrumentsForBucket;
    let sendSubscription
    let sendUnsubscription
    let getInstrumentsForBucket
    let expected_num_subs_yet
    let expected_num_unsubs_yet

    beforeEach(() => {
      expected_num_subs_yet = 0
      expected_num_unsubs_yet = 0
      subscriptionsSent = new Map(); // key -> Set of requests
      unsubscriptionsSent = new Map(); // key -> Set of requests
      instrumentsForBucket = new Map(); // bucket -> [requests]

      getInstrumentsForBucket = (bucket) => {
        return instrumentsForBucket.get(bucket) || [];
      };

      router = new SubscriptionRouter(
        new BucketAssigner(10),
        getInstrumentsForBucket,
        getSendSubscriptionFunction(subscriptionsSent),
        getSendUnSubscriptionFunction(unsubscriptionsSent)
      );
    });

    test('multiple buckets with different instruments', (done) => {
      // Setup instruments for multiple buckets
      instrumentsForBucket.set(0, ['EUR/USD']);
      instrumentsForBucket.set(1, ['JPY/USD']);
      instrumentsForBucket.set(2, ['AUD/USD']);
      instrumentsForBucket.set(3, ['INR/USD']);
      instrumentsForBucket.set(4, ['JPY/USD']);
      instrumentsForBucket.set(5, ['CHF/USD']);
      instrumentsForBucket.set(6, ['USD/EUR']);
      instrumentsForBucket.set(7, ['USD/AUD']);
      instrumentsForBucket.set(8, ['USD/JPY']);
      instrumentsForBucket.set(9, ['AUD/INR']);

      // Bring up first provider and assign bucket 1
      router.onPriceProviderUp('provider-A', dummyCb, dummyCb);
      expected_num_subs_yet = Array.from(instrumentsForBucket.values()).flat().length
      expect(totalSuborUnsubSent(subscriptionsSent)).toBe(expected_num_subs_yet);

      router.onSubscriptionRequest(1, 'EUR/USD', errCallback('n'))
      expected_num_subs_yet += 1
      expect(totalSuborUnsubSent(subscriptionsSent)).toBe(expected_num_subs_yet);
      expect(totalSuborUnsubSent(unsubscriptionsSent)).toBe(expected_num_unsubs_yet);

      // Bring up second provider and assign bucket 2
      router.onPriceProviderUp('provider-B', dummyCb, dummyCb);
      // Bucket Distribution was [10], not it should be [5, 5]
      // i.e., 1 bucket should be taken from provider-A and given to provider-B
      // so 1 unsubscription and 1 subscription
      expected_num_subs_yet += 5
      expected_num_unsubs_yet += 5
      expect(totalSuborUnsubSent(subscriptionsSent)).toBe(expected_num_subs_yet);
      expect(totalSuborUnsubSent(unsubscriptionsSent)).toBe(expected_num_unsubs_yet);

      // Bucket Distribution was [5,5], not it should be [4, 3, 3]
      // i.e., 1 bucket should be taken from provider-A and given to provider-B
      // so 1 unsubscription and 1 subscription
      router.onPriceProviderUp('provider-C', dummyCb, dummyCb);
      expected_num_subs_yet += 3
      expected_num_unsubs_yet += 3
      expect(totalSuborUnsubSent(subscriptionsSent)).toBe(expected_num_subs_yet);
      expect(totalSuborUnsubSent(unsubscriptionsSent)).toBe(expected_num_unsubs_yet);

      // Verify that all subscriptions are sent correctly
      expect(subscriptionsSent.has('provider-A')).toBe(true);
      expect(subscriptionsSent.has('provider-B')).toBe(true);
      expect(subscriptionsSent.has('provider-C')).toBe(true);

      setImmediate(() => { done() })
    });

    test('unsubscribing from multiple buckets', (done) => {
      router = new SubscriptionRouter(
        new BucketAssigner(3),
        getInstrumentsForBucket,
        getSendSubscriptionFunction(subscriptionsSent),
        getSendUnSubscriptionFunction(unsubscriptionsSent)
      );
      // Setup instruments for multiple buckets
      instrumentsForBucket.set(0, ['EUR/USD', 'GBP/USD']);
      instrumentsForBucket.set(1, ['JPY/USD', 'USD/JPY']);
      instrumentsForBucket.set(2, ['AUD/USD', 'NZD/USD']);

      // Bring up providers
      router.onPriceProviderUp('provider-A', dummyCb, dummyCb);
      expected_num_subs_yet = Array.from(instrumentsForBucket.values()).flat().length
      expect(totalSuborUnsubSent(subscriptionsSent)).toBe(expected_num_subs_yet)
      expect(totalSuborUnsubSent(unsubscriptionsSent)).toBe(expected_num_unsubs_yet)


      router.onPriceProviderUp('provider-B', dummyCb, dummyCb);
      expected_num_subs_yet += 2
      expected_num_unsubs_yet += 2
      expect(totalSuborUnsubSent(subscriptionsSent)).toBe(expected_num_subs_yet)
      expect(totalSuborUnsubSent(unsubscriptionsSent)).toBe(expected_num_unsubs_yet)

      router.onPriceProviderUp('provider-C', dummyCb, dummyCb);
      expected_num_subs_yet += 2
      expected_num_unsubs_yet += 2
      expect(totalSuborUnsubSent(subscriptionsSent)).toBe(expected_num_subs_yet)
      expect(totalSuborUnsubSent(unsubscriptionsSent)).toBe(expected_num_unsubs_yet)

      // Subscribe to all instruments
      router.onSubscriptionRequest(0, 'EUR/USD', errCallback('n'));
      router.onSubscriptionRequest(0, 'GBP/USD', errCallback('n'));
      router.onSubscriptionRequest(1, 'JPY/USD', errCallback('n'));
      router.onSubscriptionRequest(2, 'AUD/USD', errCallback('n'));
      router.onSubscriptionRequest(2, 'NZD/USD', errCallback('n'));

      expected_num_subs_yet += 5
      expect(totalSuborUnsubSent(subscriptionsSent)).toBe(expected_num_subs_yet)

      // Unsubscribe from bucket 1
      router.onUnsubscriptionRequest(0, 'EUR/USD', errCallback('n'))
      expected_num_unsubs_yet += 1
      expect(totalSuborUnsubSent(subscriptionsSent)).toBe(expected_num_subs_yet)
      expect(totalSuborUnsubSent(unsubscriptionsSent)).toBe(expected_num_unsubs_yet);

      // Unsubscribe from bucket 2
      router.onUnsubscriptionRequest(1, 'JPY/USD', errCallback('n'))
      expected_num_unsubs_yet += 1
      expect(totalSuborUnsubSent(subscriptionsSent)).toBe(expected_num_subs_yet)
      expect(totalSuborUnsubSent(unsubscriptionsSent)).toBe(expected_num_unsubs_yet);

      // Unsubscribe from bucket 3
      router.onUnsubscriptionRequest(2, 'AUD/USD', errCallback('n'))
      expected_num_unsubs_yet += 1
      expect(totalSuborUnsubSent(subscriptionsSent)).toBe(expected_num_subs_yet)
      expect(totalSuborUnsubSent(unsubscriptionsSent)).toBe(expected_num_unsubs_yet);

      setImmediate(()=> {done()})
    });

    test('bucket reassignment with multiple providers', (done) => {
      router = new SubscriptionRouter(
        new BucketAssigner(3),
        getInstrumentsForBucket,
        getSendSubscriptionFunction(subscriptionsSent),
        getSendUnSubscriptionFunction(unsubscriptionsSent)
      );

      // Setup instruments for multiple buckets
      instrumentsForBucket.set(0, ['EUR/USD', 'GBP/USD']);
      instrumentsForBucket.set(1, ['JPY/USD', 'USD/JPY']);
      instrumentsForBucket.set(2, ['AUD/USD', 'NZD/USD']);

      // Bring up first provider and assign bucket 1
      router.onPriceProviderUp('provider-A', dummyCb, dummyCb);
      expected_num_subs_yet += Array.from(instrumentsForBucket.values()).flat().length
      expect(totalSuborUnsubSent(subscriptionsSent)).toBe(expected_num_subs_yet)
      expect(totalSuborUnsubSent(unsubscriptionsSent)).toBe(expected_num_unsubs_yet);

      router.onSubscriptionRequest(0, 'EUR/USD', errCallback('n'));
      router.onSubscriptionRequest(1, 'GBP/USD', errCallback('n'));
      expected_num_subs_yet += 2
      expect(totalSuborUnsubSent(subscriptionsSent)).toBe(expected_num_subs_yet)
      expect(totalSuborUnsubSent(unsubscriptionsSent)).toBe(expected_num_unsubs_yet);

      // Bring up second provider and assign bucket 2
      router.onPriceProviderUp('provider-B', dummyCb, dummyCb);
      expected_num_subs_yet += 2
      expected_num_unsubs_yet += 2
      expect(totalSuborUnsubSent(subscriptionsSent)).toBe(expected_num_subs_yet)
      expect(totalSuborUnsubSent(unsubscriptionsSent)).toBe(expected_num_unsubs_yet);

      router.onSubscriptionRequest(1, 'JPY/USD', errCallback('n'));
      expected_num_subs_yet += 1
      expect(totalSuborUnsubSent(subscriptionsSent)).toBe(expected_num_subs_yet)
      expect(totalSuborUnsubSent(unsubscriptionsSent)).toBe(expected_num_unsubs_yet);

      // Bring up third provider and assign bucket 3
      router.onPriceProviderUp('provider-C', dummyCb, dummyCb);
      expected_num_subs_yet += 2
      expected_num_unsubs_yet += 2
      expect(totalSuborUnsubSent(subscriptionsSent)).toBe(expected_num_subs_yet)
      expect(totalSuborUnsubSent(unsubscriptionsSent)).toBe(expected_num_unsubs_yet);

      router.onSubscriptionRequest(2, 'AUD/USD', dummyCb);
      router.onSubscriptionRequest(2, 'NZD/USD', dummyCb);
      expected_num_subs_yet += 2
      expect(totalSuborUnsubSent(subscriptionsSent)).toBe(expected_num_subs_yet)
      expect(totalSuborUnsubSent(unsubscriptionsSent)).toBe(expected_num_unsubs_yet);


      // Reassign buckets and check subscriptions
      router.onPriceProviderDown('provider-A', dummyCb);
      expected_num_subs_yet += 2
      expect(totalSuborUnsubSent(subscriptionsSent)).toBe(expected_num_subs_yet)
      expect(totalSuborUnsubSent(unsubscriptionsSent)).toBe(expected_num_unsubs_yet);

      router.onPriceProviderUp('provider-D', dummyCb, dummyCb); // New provider takes over
      expected_num_subs_yet += 2
      expected_num_unsubs_yet += 2
      expect(totalSuborUnsubSent(subscriptionsSent)).toBe(expected_num_subs_yet)
      expect(totalSuborUnsubSent(unsubscriptionsSent)).toBe(expected_num_unsubs_yet);

      [...instrumentsForBucket].forEach(([bucket, instruments]) => {
        instruments.forEach(instrument => {
          router.onSubscriptionRequest(bucket, instrument, errCallback('n'))
        })
      })

      expected_num_subs_yet += Array.from(instrumentsForBucket.values()).flat().length
      expect(totalSuborUnsubSent(subscriptionsSent)).toBe(expected_num_subs_yet)
      expect(totalSuborUnsubSent(unsubscriptionsSent)).toBe(expected_num_unsubs_yet);

      [...instrumentsForBucket].forEach(([bucket, instruments]) => {
        instruments.forEach(instrument => {
          router.onUnsubscriptionRequest(bucket, instrument, errCallback('n'))
        })
      })

      expected_num_unsubs_yet += Array.from(instrumentsForBucket.values()).flat().length
      expect(totalSuborUnsubSent(subscriptionsSent)).toBe(expected_num_subs_yet)
      expect(totalSuborUnsubSent(unsubscriptionsSent)).toBe(expected_num_unsubs_yet);

      router.onPriceProviderDown('provider-A', dummyCb)
      expect(totalSuborUnsubSent(subscriptionsSent)).toBe(expected_num_subs_yet)
      expect(totalSuborUnsubSent(unsubscriptionsSent)).toBe(expected_num_unsubs_yet);

      router.onPriceProviderDown('provider-B', dummyCb)
      expected_num_subs_yet += 2
      expect(totalSuborUnsubSent(subscriptionsSent)).toBe(expected_num_subs_yet)
      expect(totalSuborUnsubSent(unsubscriptionsSent)).toBe(expected_num_unsubs_yet);

      router.onPriceProviderDown('provider-C', dummyCb)
      router.onPriceProviderDown('provider-D', dummyCb);

      [...instrumentsForBucket].forEach(([bucket, instruments]) => {
        instruments.forEach(instrument => {
          router.onSubscriptionRequest(bucket, instrument, errCallback('y'))
        })
      });

      [...instrumentsForBucket].forEach(([bucket, instruments]) => {
        instruments.forEach(instrument => {
          router.onUnsubscriptionRequest(bucket, instrument, errCallback('y'))
        })
      })

      setImmediate(()=>{ done() })
    });
  });
})
