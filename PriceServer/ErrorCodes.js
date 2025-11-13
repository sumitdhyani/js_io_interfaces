const err_codes = {
  price_provider_down: "price_provider_down",
  all_price_provider_down: "all_price_provider_down",
  duplicate_subscription: "duplicate_subscription",
  spurious_unsubscription: "spurious_unsubscription",
  io_error:  "io_error" 
}


function gerErrorObject(errCode, errMessage = null) {
  if (err_codes[errCode] !== undefined) {
    return { err_code: errCode, message: errMessage ? errMessage : err_codes[errCode] }
  }
  else return {}
}

module.exports = [err_codes, gerErrorObject]