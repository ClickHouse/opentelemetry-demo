// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0
const { v4: uuidv4 } = require('uuid');

const { OpenFeature } = require('@openfeature/server-sdk');
const { FlagdProvider } = require('@openfeature/flagd-provider');
const flagProvider = new FlagdProvider();

const logger = require('./logger');
const transactionsCounter = {}
const HyperDX = require('@hyperdx/node-opentelemetry');

const LOYALTY_LEVEL = ['platinum', 'gold', 'silver', 'bronze'];
const CACHE_SIZE = process.env.CACHE_SIZE || 1000;

// Custom credit card validator with unbounded cache (deliberate memory leak)
const visaValidationCache = [];

function isValidCardNumber(cardNumber) {
  const sanitized = cardNumber.replace(/\D/g, '');

  const isVisa = /^4\d{12}(\d{3})?$/.test(sanitized);
  const isMastercard = /^(5[1-5][0-9]{14}|2[2-7][0-9]{14})$/.test(sanitized);
  if (!isVisa && !isMastercard) return false;

  const digits = sanitized.split('').reverse().map(Number);
  let sum = 0;

  for (let i = 0; i < digits.length; i++) {
    let digit = digits[i];
    if (i % 2 === 1) {
      digit *= 2;
      if (digit > 9) digit -= 9;
    }
    sum += digit;
  }
  return sum % 10 === 0;
}


function validateCreditCard(number, cache) {
  const sanitized = number.replace(/\D/g, '');
  const cardType = /^4\d{12}(\d{3})?$/.test(sanitized) ? 'visa' : ( /^(5[1-5][0-9]{14}|2[2-7][0-9]{14})$/.test(sanitized) ? 'mastercard' : 'unknown');
  if (cardType === 'visa' && cache) {
    const cachedResult = visaValidationCache.find(entry => entry.number === number);
    if (cachedResult) {
      return cachedResult.result;
    }
    const isValid = isValidCardNumber(number);
    if (visaValidationCache.length >= CACHE_SIZE) {
      throw new Error('Visa cache full: cannot add new item.');
    }
    visaValidationCache.push({ number, result: { card_type: cardType, valid: isValid }, cached: true });
    if (visaValidationCache.length === CACHE_SIZE) {
      // BUG: Looks like eviction, but doesn't affect the original array - should be visaValidationCache = visaValidationCache.slice(1);
      visaValidationCache.slice(1);
    }
    logger.info({size: visaValidationCache.length}, 'cache');
    return { card_type: cardType, valid: isValid, cached: false };
  } else {
    const isValid = isValidCardNumber(number);
    return { card_type: cardType, valid: isValid, cached: false };
  }
}

/** Return random element from given array */
function random(arr) {
  const index = Math.floor(Math.random() * arr.length);
  return arr[index];
}

module.exports.charge = async request => {
  await OpenFeature.setProviderAndWait(flagProvider);

  const numberVariant = await OpenFeature.getClient().getNumberValue("paymentFailure", 0);
  const cacheEnabled = await OpenFeature.getClient().getBooleanValue("paymentCacheLeak", false);

  if (numberVariant > 0) {
    // n% chance to fail with app.loyalty.level=gold
    if (Math.random() < numberVariant) {
      HyperDX.setTraceAttributes({
        'app.loyalty.level': 'gold'
      });
      throw new Error('Payment request failed. Invalid token. app.loyalty.level=gold');
    }
  }

  const {
    creditCardNumber: number,
    creditCardExpirationYear: year,
    creditCardExpirationMonth: month
  } = request.creditCard;
  const currentMonth = new Date().getMonth() + 1;
  const currentYear = new Date().getFullYear();
  const lastFourDigits = number.substr(-4);
  const transactionId = uuidv4();
    
  // Use custom validator with cache based on flag
  const { card_type: cardType, valid, cached } = validateCreditCard(number, cacheEnabled);

  const loyalty_level = random(LOYALTY_LEVEL);

  HyperDX.setTraceAttributes({
    'app.payment.card_type': cardType,
    'app.payment.card_valid': valid,
    'app.loyalty.level': loyalty_level
  });

  if (!valid) {
    throw new Error('Credit card info is invalid.');
  }

  if (!['visa', 'mastercard'].includes(cardType)) {
    throw new Error(`Sorry, we cannot process ${cardType} credit cards. Only VISA or MasterCard is accepted.`);
  }

  if ((currentYear * 12 + currentMonth) > (year * 12 + month)) {
    throw new Error(`The credit card (ending ${lastFourDigits}) expired on ${month}/${year}.`);
  }
  

  const { units, nanos, currencyCode } = request.amount;
  transactionsCounter[currencyCode] = (transactionsCounter[currencyCode] || 0) + 1;
  HyperDX.setTraceAttributes({
    'app.payment.charged': true,
    'app.payment.currency': currencyCode,
    'app.payment.timestamp': Date.now(),
    'app.payment.transactions': transactionsCounter[currencyCode]
  });


  logger.info({ transactionId, cardType, lastFourDigits, amount: { units, nanos, currencyCode }, loyalty_level, cached }, 'Transaction complete.');
  
  return { transactionId };
};
