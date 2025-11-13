const getClientInteractionFunctions = require('../ClientPriceRouter')
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
function forwarderReturningIOErrorAndThenNoError(recordArray, numFailedAttempts  = 1, finalErr = null) {
  return (bucket, instrument, cb) => {
    recordArray.push({ bucket, instrument })
    if (numFailedAttempts !== 0) {
      setImmediate(() => {numFailedAttempts -= 1; cb(getErrorObject(err_codes.price_provider_down)) } )
    } else {
      setImmediate(() => cb(finalErr))
    }
  }
}

describe('ClientPriceRouter - done-style async tests (subscriptions / unsubscriptions) - flat callbacks', (done) => {
  test('first subscribe forwards; second client update only; duplicate subscription yields error', (done) => {
    const subs = []
    const unsubs = []
    const subscriptionForwarder = makeForwarder(subs)
    const unsubscriptionForwarder = makeForwarder(unsubs)
    const bucketIdGenerator = (instr) => `bucket-${instr}`

    const [onSubscription, onUnSubscription, onClientDown] = getClientInteractionFunctions(
      subscriptionForwarder,
      unsubscriptionForwarder,
      bucketIdGenerator
    )

    // Call each API independently; callbacks only assert on recorders.
    onSubscription('EUR/USD', 'clientA', (err) => {
      expect(err).toBeNull()
    })

    expect(subs.length).toBe(1)
    expect(subs[0].bucket).toBe('bucket-EUR/USD')

    onSubscription('EUR/USD', 'clientB', (err2) => {
      expect(err2).toBeNull()
    })
    
    // subscriptionForwarder should not be called again for clientB
    expect(subs.length).toBe(1)

    onSubscription('EUR/USD', 'clientA', (err3) => {
      // duplicate subscription should result in an error
      expect(err3).not.toBeNull()
    })

    // Allow async callbacks to run, then finish the test
    setImmediate(() => { done() })
  })

  test('unsubscription: non-last does local cleanup; last triggers forwarder', (done) => {
    const subs = []
    const unsubs = []
    const subscriptionForwarder = makeForwarder(subs)
    const unsubscriptionForwarder = makeForwarder(unsubs)
    const bucketIdGenerator = (instr) => `b-${instr}`

    const [onSubscription, onUnSubscription] = getClientInteractionFunctions(
      subscriptionForwarder,
      unsubscriptionForwarder,
      bucketIdGenerator)

    // Subscribe two clients independently
    onSubscription('JPY/USD', 'c1', (e1) => {
      expect(e1).toBeNull()
    })
    onSubscription('JPY/USD', 'c2', (e2) => {
      expect(e2).toBeNull()
    })

    // Non-last unsubscribe (c2) should not invoke unsubscriptionForwarder
    onUnSubscription('JPY/USD', 'c2', (erA) => {
      expect(erA).toBeNull()
    })

    expect(unsubs.length).toBe(0)

    // Last unsubscribe (c1) should invoke unsubscriptionForwarder
    onUnSubscription('JPY/USD', 'c1', (erB) => {
      expect(erB).toBeNull()
    })

    expect(unsubs.length).toBe(1)
    expect(unsubs[0].bucket).toBe('b-JPY/USD')

    setImmediate(() => { done() })
  })
})

describe('manual', (done) => {
  test('Cient down: no unsubscription forwarded, until last client down', (done) => {
    const subs = []
    const unsubs = []
    const subscriptionForwarder = makeForwarder(subs)
    const unsubscriptionForwarder = makeForwarder(unsubs)
    const bucketIdGenerator = (instr) => `bucket-${instr}`

    const [onSubscription, onUnSubscription, onClientDown] = getClientInteractionFunctions(
      subscriptionForwarder,
      unsubscriptionForwarder,
      bucketIdGenerator)

    // Call each API independently; callbacks only assert on recorders.
    onSubscription('EUR/USD', 'clientA', (err) => {
      expect(err).toBeNull()
    })

    expect(subs.length).toBe(1)
    expect(subs[0].bucket).toBe('bucket-EUR/USD')

    onSubscription('EUR/USD', 'clientB', (err2) => {
      expect(err2).toBeNull()
    })

    onSubscription('EUR/USD', 'clientC', (err2) => {
      expect(err2).toBeNull()
    })

    // subscriptionForwarder should not be called again for clientB/C
    expect(subs.length).toBe(1)

    onClientDown('clientA', (err) => {})
    expect(unsubs.length).toBe(0)

    onClientDown('clientB', (err) => {})
    expect(unsubs.length).toBe(0)

    onClientDown('clientC', (err) => { })
    expect(unsubs.length).toBe(1)
   
    // Allow async callbacks to run, then finish the test
    setImmediate(() => { done() })
  })

  test('Client down, unsubscription forwarded, but the forwarder returns error', (done) => {
    const subs = []
    const unsubs = []
    const subscriptionForwarder = makeForwarder(subs)
    const unsubscriptionForwarder = forwarderReturningIOErrorAndThenNoError(unsubs)
    const bucketIdGenerator = (instr) => `bucket-${instr}`
    
    const [onSubscription, onUnSubscription, onClientDown] = getClientInteractionFunctions(
      subscriptionForwarder,
      unsubscriptionForwarder,
      bucketIdGenerator)

    // Call each API independently; callbacks only assert on recorders.
    onSubscription('EUR/USD', 'clientA', (err) => {
      expect(err).toBeNull()
    })

    expect(subs.length).toBe(1)
    expect(subs[0].bucket).toBe('bucket-EUR/USD')

    onSubscription('EUR/USD', 'clientB', (err2) => {
      expect(err2).toBeNull()
    })

    // subscriptionForwarder should not be called again for clientB
    expect(subs.length).toBe(1)

    onClientDown('clientA', err => {})
    expect(unsubs.length).toBe(0)

    onClientDown('clientB', err => {})
    expect(unsubs.length).toBe(1)

    // Unsubscription would have been called in the callback
    setImmediate(() => { 
      expect(unsubs.length).toBe(2)
    })

    // Allow async callbacks to run, then finish the test
    setImmediate(() => { done() })
  })

  test('1st subscription yields error, retries in the callback', (done) => {
    const subs = []
    const unsubs = []
    const subscriptionForwarder = forwarderReturningIOErrorAndThenNoError(subs)
    const unsubscriptionForwarder = makeForwarder(unsubs)
    const bucketIdGenerator = (instr) => `bucket-${instr}`

    const [onSubscription, onUnSubscription, onClientDown] = getClientInteractionFunctions(
      subscriptionForwarder,
      unsubscriptionForwarder,
      bucketIdGenerator)

    // // Call each API independently; callbacks only assert on recorders.
    onSubscription('EUR/USD', 'clientA', (err) => {
      expect(err).toBeNull()
    })
    // Should forward the subscription internally

    expect(subs.length).toBe(1)
    expect(subs[0].bucket).toBe('bucket-EUR/USD')

    onSubscription('EUR/USD', 'clientB', (err2) => {
      expect(err2).toBeNull()
    })
    expect(subs[0].bucket).toBe('bucket-EUR/USD')

    // subscriptionForwarder should not be called again for clientB
    //expect(subs.length).toBe(1)

    setImmediate(() => {
      // 1st call for subscription should have received error and re-forwarded
      // the subscription internally
      expect(subs.length).toBe(2)
    })

    // A pending callback is still there, that was sent in the retry
    setTimeout(() => { done() }, 1000)
  })

  test('subscription yields error, retries until the subscription is forwarded', (done) => {
    const numFailedAttempts = 3
    const subs = []
    const unsubs = []
    const subscriptionForwarder = forwarderReturningIOErrorAndThenNoError(subs, numFailedAttempts)
    const unsubscriptionForwarder = makeForwarder(unsubs)
    const bucketIdGenerator = (instr) => `bucket-${instr}`

    const [onSubscription, onUnSubscription, onClientDown] = getClientInteractionFunctions(
      subscriptionForwarder,
      unsubscriptionForwarder,
      bucketIdGenerator)

    // // Call each API independently; callbacks only assert on recorders.
    onSubscription('EUR/USD', 'clientA', (err) => {
      expect(err).toBeNull()
    })
    // Should forward the subscription internally

    expect(subs.length).toBe(1)
    expect(subs[0].bucket).toBe('bucket-EUR/USD')

    onSubscription('EUR/USD', 'clientB', (err2) => {
      expect(err2).toBeNull()
    })
    expect(subs[0].bucket).toBe('bucket-EUR/USD')

    // subscriptionForwarder should not be called again for clientB
    //expect(subs.length).toBe(1)

    setTimeout(() => {
      // 1st call for subscription should have received error and re-forwarded
      // the subscription internally
      expect(subs.length).toBe(numFailedAttempts + 1)
    }, 500)

    // A pending callback is still there, that was sent in the retry
    setTimeout(() => { done() }, 1000)
  })

  test('subscription yields error, retries until io error is received', (done) => {
    const numFailedAttempts = 3
    const subs = []
    const unsubs = []
    const subscriptionForwarder = forwarderReturningIOErrorAndThenNoError(subs, numFailedAttempts, getErrorObject(err_codes.io_error))
    const unsubscriptionForwarder = makeForwarder(unsubs)
    const bucketIdGenerator = (instr) => `bucket-${instr}`

    const [onSubscription, onUnSubscription, onClientDown] = getClientInteractionFunctions(
      subscriptionForwarder,
      unsubscriptionForwarder,
      bucketIdGenerator)

    // // Call each API independently; callbacks only assert on recorders.
    onSubscription('EUR/USD', 'clientA', (err) => {
      expect(err).not.toBeNull()
      expect(err.err_code).toBe(err_codes.io_error)
    })
    // Should forward the subscription internally

    expect(subs.length).toBe(1)
    expect(subs[0].bucket).toBe('bucket-EUR/USD')

    onSubscription('EUR/USD', 'clientB', (err2) => {
      expect(err2).toBeNull()
    })
    expect(subs[0].bucket).toBe('bucket-EUR/USD')

    // subscriptionForwarder should not be called again for clientB
    //expect(subs.length).toBe(1)

    setTimeout(() => {
      // 1st call for subscription should have received error and re-forwarded
      // the subscription internally
      expect(subs.length).toBe(numFailedAttempts + 1)
    }, 500)

    // A pending callback is still there, that was sent in the retry
    setTimeout(() => { done() }, 1000)
  })
})

