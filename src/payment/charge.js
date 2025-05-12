// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0
const { context, propagation, trace, metrics } = require('@opentelemetry/api');
const { v4: uuidv4 } = require('uuid');

const { OpenFeature } = require('@openfeature/server-sdk');
const { FlagdProvider } = require('@openfeature/flagd-provider');
const flagProvider = new FlagdProvider();

const logger = require('./logger');
const tracer = trace.getTracer('payment');
const meter = metrics.getMeter('payment');
const transactionsCounter = meter.createCounter('app.payment.transactions');

const LOYALTY_LEVEL = ['platinum', 'gold', 'silver', 'bronze'];

// Custom credit card validator with unbounded cache (deliberate memory leak)
const cardValidationCache = [];

function isValidCardNumber(cardNumber) {
  const sanitized = cardNumber.replace(/\D/g, '');
  if (!/^4\d{15}$/.test(sanitized)) return false;

  const digits = sanitized.split('').map(Number);
  let sum = 0;

  for (let i = 0; i < digits.length; i++) {
    let digit = digits[i];
    if (i % 2 === 0) {
      digit *= 2;
      if (digit > 9) digit -= 9;
    }
    sum += digit;
  }

  return sum % 10 === 0;
}

function validateCreditCard(number, cache) {
  // Check cache first
  const cachedResult = cardValidationCache.find(entry => entry.number === number);
  if (cachedResult) {
    return cachedResult.result;
  }

  // Validate card using Luhn algorithm
  const isValid = isValidCardNumber(number);
  const cardType = number.startsWith('4') ? 'visa' : 'unknown';

  const result = { card_type: cardType, valid: isValid };

  if (cache) {
    // Cache the result (unbounded growth)
    cardValidationCache.push({ number, result });
  }

  return result;
}

/** Return random element from given array */
function random(arr) {
  const index = Math.floor(Math.random() * arr.length);
  return arr[index];
}

module.exports.charge = async request => {
  const span = tracer.startSpan('charge');

  await OpenFeature.setProviderAndWait(flagProvider);

  const numberVariant = await OpenFeature.getClient().getNumberValue("paymentFailure", 0);
  const cacheEnabled = await OpenFeature.getClient().getBooleanValue("paymentCacheLeak", false);

  if (numberVariant > 0) {
    // n% chance to fail with app.loyalty.level=gold
    if (Math.random() < numberVariant) {
      span.setAttributes({'app.loyalty.level': 'gold' });
      span.end();

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
  const { card_type: cardType, valid } = validateCreditCard(number, cacheEnabled);

  const loyalty_level = random(LOYALTY_LEVEL);

  span.setAttributes({
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

  // Check baggage for synthetic_request=true, and add charged attribute accordingly
  const baggage = propagation.getBaggage(context.active());
  if (baggage && baggage.getEntry('synthetic_request') && baggage.getEntry('synthetic_request').value === 'true') {
    span.setAttribute('app.payment.charged', false);
  } else {
    span.setAttribute('app.payment.charged', true);
  }

  const { units, nanos, currencyCode } = request.amount;
  logger.info({ transactionId, cardType, lastFourDigits, amount: { units, nanos, currencyCode }, loyalty_level }, 'Transaction complete.');
  transactionsCounter.add(1, { 'app.payment.currency': currencyCode });
  span.end();

  return { transactionId };
};
