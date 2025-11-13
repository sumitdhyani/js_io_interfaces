const getClientInteractionFunctions = require('../CoreEngine')
const [err_codes, getErrorObject] = require('../ErrorCodes')
/*
  Helpers:
  - makeForwarder: creates a mock forwarder that records calls and invokes cb asynchronously
  - makeFailingForwarder: same but invokes cb with an error
*/
function makeForwarder(recordArray) {
  return (bucket, instrument, cb) => {
    recordArray.push({ bucket, instrument })
    setImmediate(() => cb(null))
  }
}

// Forwarder that returns error 1st time, but works fine 2nd time
function forwarderReturningIOErrorAndThenNoError(recordArray, numFailedAttempts = 1, finalErr = null, delay = 0) {
  return (bucket, instrument, cb) => {
    recordArray.push({ bucket, instrument })
    if (numFailedAttempts !== 0) {
      if (delay === 0) {
        setImmediate(() => { numFailedAttempts -= 1; cb(getErrorObject(err_codes.price_provider_down)) })
      } else {
        setTimeout(() => { numFailedAttempts -= 1; cb(getErrorObject(err_codes.price_provider_down)) }, delay)
      }
    } else {
      setImmediate(() => cb(finalErr))
    }
  }
}

describe('CoreEngine - manual', (done) => {
  test('Subscription rejected as no price provider has registered', (done) => {
    const subs = []
    const unsubs = []
    const subscriptionForwarder = makeForwarder(subs)
    const unsubscriptionForwarder = makeForwarder(unsubs)

    const [onSubscription, onUnSubscription, onClientDown] = getClientInteractionFunctions(
      subscriptionForwarder,
      unsubscriptionForwarder,
      5)

    // Call each API independently; callbacks only assert on recorders.
    onSubscription('EUR/USD', 'clientA', (err) => {
      expect(err).not.toBeNull()
      expect(err.err_code).toBe(err_codes.all_price_provider_down)
    })

    // Allow async callbacks to run, then finish the test
    setImmediate(() => { done() })
  })

  test('2 Subscriptions accepted, a rebalance happens by adding a new PP, subs and unsubs happen', (done) => {
    const subs = []
    const unsubs = []
    const subscriptionForwarder = makeForwarder(subs)
    const unsubscriptionForwarder = makeForwarder(unsubs)

    const [onSubscription, onUnSubscription, onClientDown, notifyPriceProviderUp] = getClientInteractionFunctions(
      subscriptionForwarder,
      unsubscriptionForwarder,
      2,
      (instrument) => { return instrument === 'EUR/USD'? 0 : 1})

    setImmediate(() => {
      notifyPriceProviderUp('provider-A', (err) => { }, (err) => { })
      expect(subs.length).toBe(0)
    })
    
    // Call each API independently; callbacks only assert on recorders.
    setImmediate(()=>{
      onSubscription('EUR/USD', 'clientA', (err) => {
        expect(err).toBeNull()
      })
      expect(subs.length).toBe(1)
    })    

    setImmediate(()=>{
      onSubscription('GBP/USD', 'clientA', (err) => {
        expect(err).toBeNull()
      })
      expect(subs.length).toBe(2)
    })

    setImmediate(() => {
      notifyPriceProviderUp('provider-B',
        (err) => { 
        expect(err).toBeNull()
      },
        (err) => { 
          expect(err).toBeNull()
      })
      expect(subs.length).toBe(3)
      expect(unsubs.length).toBe(1)
    })

    // Allow async callbacks to run, then finish the test
    setTimeout(() => { done() }, 1000)
  })

  test('5 Subscriptions accepted, a rebalance happens by adding a new PP, subs and unsubs happen', (done) => {
    const subs = []
    const unsubs = []
    const subscriptionForwarder = makeForwarder(subs)
    const unsubscriptionForwarder = makeForwarder(unsubs)

    const instruments = ['EUR/USD', 'GBB/USD', 'INR/USD', 'THB/USD', 'CHF/USD']

    const [onSubscription, onUnSubscription, onClientDown, notifyPriceProviderUp] = getClientInteractionFunctions(
      subscriptionForwarder,
      unsubscriptionForwarder,
      5,
      (instrument) => { 
        return instrument === 'EUR/USD' ? 0 :
        instrument === 'GBB/USD' ? 1 :
        instrument === 'INR/USD' ? 2 :
        instrument === 'THB/USD' ? 3 :
        4
    })

    setImmediate(() => {
      notifyPriceProviderUp('provider-A', (err) => { }, (err) => { })
      expect(subs.length).toBe(0)
    })

    let totalExpectedSubs = instruments.length
    let totalExpectedUnSubs = 0

    instruments.forEach((instrument, index)=>{
      setImmediate(() => {
        onSubscription(instrument, 'clientA', (err) => {
          expect(err).toBeNull()
        })
        expect(subs.length).toBe(index + 1)
      }) 
    })

    setImmediate(() => {
      totalExpectedSubs += 2
      totalExpectedUnSubs += 2
      notifyPriceProviderUp('provider-B', (err) => { }, (err) => { })
      expect(subs.length).toBe(totalExpectedSubs)
      expect(unsubs.length).toBe(totalExpectedUnSubs)
    })
     
    setImmediate(() => {
      totalExpectedSubs += 1
      totalExpectedUnSubs += 1
      notifyPriceProviderUp('provider-C', (err) => { }, (err) => { })
      expect(subs.length).toBe(totalExpectedSubs)
      expect(unsubs.length).toBe(totalExpectedUnSubs)
    })

    // Allow async callbacks to run, then finish the test
    setTimeout(() => { done() }, 1000)
  })

  test('Failed subscription should not be forwarded after the 1st PP comes up', (done) => {
    const subs = []
    const unsubs = []
    const subscriptionForwarder = makeForwarder(subs)
    const unsubscriptionForwarder = makeForwarder(unsubs)

    const [onSubscription, onUnSubscription, onClientDown, notifyPriceProviderUp] = getClientInteractionFunctions(
      subscriptionForwarder,
      unsubscriptionForwarder,
      5)

    // Call each API independently; callbacks only assert on recorders.
    onSubscription('EUR/USD', 'clientA', (err) => {
      expect(err).not.toBeNull()
      expect(err.err_code).toBe(err_codes.all_price_provider_down)
    })

    setImmediate(()=>{
      notifyPriceProviderUp('provider - A', (err)=> {}, (err)=> {})
      expect(subs.length).toBe(0)
    })

    // Allow async callbacks to run, then finish the test
    setImmediate(() => { done() })
  })

  // PP is up
  // Subscrion attempted
  // Replied with PP down error with a delay of 200 ms
  // Within this time, notifyPriceProviderDown is called
  // After the PP down error is received, the engine retries subscription
  // The retry is replied with all_price_provider_down error
  test('Subsccription attempted, but replied with all PP down error', (done) => {
    const subs = []
    const unsubs = []
    const subscriptionForwarder = forwarderReturningIOErrorAndThenNoError(subs, 1, null, 200)
    const unsubscriptionForwarder = makeForwarder(unsubs)

    const [onSubscription, onUnSubscription, onClientDown, notifyPriceProviderUp, notifyPriceProviderDown] = getClientInteractionFunctions(
      subscriptionForwarder,
      unsubscriptionForwarder,
      5)

   setImmediate(() => {
      notifyPriceProviderUp('provider - A', (err) => { }, (err) => { })
      expect(subs.length).toBe(0)
    })
    // Call each API independently; callbacks only assert on recorders.
    setImmediate(() => {
      onSubscription('EUR/USD', 'clientA', (err) => {
        expect(err).not.toBeNull()
        expect(err.err_code).toBe(err_codes.all_price_provider_down)
      })
      expect(subs.length).toBe(1)
    })

    setImmediate(()=>{
      notifyPriceProviderDown('provider - A', (err)=> {})
    })

    // Allow async callbacks to run, then finish the test
    setTimeout(() => { 
      expect(subs.length).toBe(1)
      done() }, 1000)
  })

  test('Subscription attempted, but replied with io_error', (done) => {
    const subs = []
    const unsubs = []
    const subscriptionForwarder = forwarderReturningIOErrorAndThenNoError(subs, 1, getErrorObject(err_codes.io_error))
    const unsubscriptionForwarder = makeForwarder(unsubs)

    const [onSubscription, onUnSubscription, onClientDown, notifyPriceProviderUp, notifyPriceProviderDown] = getClientInteractionFunctions(
      subscriptionForwarder,
      unsubscriptionForwarder,
      5)

    setImmediate(() => {
      notifyPriceProviderUp('provider - A', (err) => { }, (err) => { })
      expect(subs.length).toBe(0)
    })
    // Call each API independently; callbacks only assert on recorders.
    setImmediate(() => {
      onSubscription('EUR/USD', 'clientA', (err) => {
        expect(err).not.toBeNull()
        expect(err.err_code).toBe(err_codes.io_error)
      })
      expect(subs.length).toBe(1)
    })

    setImmediate(() => {
      setImmediate(()=>{
        notifyPriceProviderDown('provider - A', (err) => { })
      })
    })

    // Allow async callbacks to run, then finish the test
    setTimeout(() => {
      expect(subs.length).toBe(2)
      done()
    }, 1000)
  })
})