const SubscriptionRouter = require('../SubscriptionRouter');
const BucketAssigner = require('../../utils/BucketAssigner');

function totalSuborUnsubSent(sentMap) {
  return Array.from(sentMap.values()).reduce((acc, val) => {
    return acc + val.length
  }, 0)  
}

describe('SubscriptionRouter - Black Box Behavior Tests', () => {
  let router;
  let subscriptionsSent;
  let unsubscriptionsSent;
  let instrumentsForBucket;

  beforeEach(() => {
    subscriptionsSent = new Map(); // key -> Set of requests
    unsubscriptionsSent = new Map(); // key -> Set of requests
    instrumentsForBucket = new Map(); // bucket -> [requests]

    // Mock functions that track what was sent where
    const sendSubscription = (key, request) => {
      if (!subscriptionsSent.has(key)) {
        subscriptionsSent.set(key, []);
      }
      subscriptionsSent.get(key).push(request);
    };

    const sendUnsubscription = (key, request) => {
      if (!unsubscriptionsSent.has(key)) {
        unsubscriptionsSent.set(key, []);
      }
      unsubscriptionsSent.get(key).push(request);
    };

    const getInstrumentsForBucket = (bucket) => {
      return instrumentsForBucket.get(bucket) || [];
    };

    router = new SubscriptionRouter(
      new BucketAssigner(10),
      getInstrumentsForBucket,
      sendSubscription,
      sendUnsubscription
    );
  });

  test('subscription requests are routed only after provider up and assignment', () => {
    // Setup some instruments for bucket 1
    instrumentsForBucket.set(1, ['EUR/USD', 'GBP/USD']);
    
    // Before provider is up, requests should be rejected
    expect(router.onSubscriptionRequest(1, 'EUR/USD')).toBe(false);
    expect(subscriptionsSent.size).toBe(0);

    // Bring provider online and assign bucket
    router.onPriceProviderUp('provider-A');
    
    // Request should still be rejected until bucket is assigned
    expect(router.onSubscriptionRequest(1, 'EUR/USD')).toBe(true);
    expect(subscriptionsSent.size).toBe(1);

    // After assignment and request, should see subscriptions
    router.onPriceProviderDown('provider-A');
    expect(router.onSubscriptionRequest(1, 'EUR/USD')).toBe(false);
  });

  test('unsubscription follows same routing rules as subscription', () => {
    instrumentsForBucket.set(2, ['JPY/USD']);
    
    // Without provider up, should reject
    expect(router.onUnsubscriptionRequest(2, 'JPY/USD')).toBe(false);
    expect(unsubscriptionsSent.size).toBe(0);

    // With provider but no assignment, should reject
    router.onPriceProviderUp('provider-B');
    expect(router.onUnsubscriptionRequest(2, 'JPY/USD')).toBe(true);
    expect(unsubscriptionsSent.size).toBe(1);

    // After provider down, should reject again
    router.onPriceProviderDown('provider-B');
    expect(router.onUnsubscriptionRequest(2, 'JPY/USD')).toBe(false);
  });

  test('provider down stops routing to that provider', () => {
    instrumentsForBucket.set(3, ['AUD/USD']);
    
    // Setup provider and verify routing works
    router.onPriceProviderUp('provider-C');
    router.onSubscriptionRequest(3, 'AUD/USD');
    
    // Take provider down
    router.onPriceProviderDown('provider-C');
    
    // Verify routing stops
    expect(router.onSubscriptionRequest(3, 'AUD/USD')).toBe(false);
    expect(router.onUnsubscriptionRequest(3, 'AUD/USD')).toBe(false);
  });

  test('bringing provider back up restores routing capability', () => {
    instrumentsForBucket.set(4, ['NZD/USD']);
    
    // Up -> Down -> Up cycle
    router.onPriceProviderUp('provider-D');
    expect(subscriptionsSent.size).toBe(1);
    expect(totalSuborUnsubSent(subscriptionsSent)).toBe(1);

    router.onSubscriptionRequest(4, 'NZD/USD');
    router.onSubscriptionRequest(4, 'NZD/USD');
    expect(totalSuborUnsubSent(subscriptionsSent)).toBe(3);

    router.onPriceProviderDown('provider-D');
    router.onSubscriptionRequest(4, 'NZD/USD');
    router.onSubscriptionRequest(4, 'NZD/USD');
    expect(totalSuborUnsubSent(subscriptionsSent)).toBe(3);

    router.onPriceProviderUp('provider-D');
    expect(totalSuborUnsubSent(subscriptionsSent)).toBe(4);
    router.onSubscriptionRequest(4, 'NZD/USD');
    router.onSubscriptionRequest(4, 'NZD/USD');

    // Verify routing works again after reassignment
    expect(subscriptionsSent.has('provider-D')).toBe(true);
    expect(subscriptionsSent.size).toBe(1);
    expect(totalSuborUnsubSent(subscriptionsSent)).toBe(6);
  });
})
