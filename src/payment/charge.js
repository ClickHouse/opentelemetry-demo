// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0
const { v4: uuidv4 } = require('uuid');

const { OpenFeature } = require('@openfeature/server-sdk');
const { FlagdProvider } = require('@openfeature/flagd-provider');
const { metrics, trace } = require('@opentelemetry/api');

const flagProvider = new FlagdProvider();


const logger = require('./logger');
const tracer = trace.getTracer('payment');
const transactionsCounter = {}
const HyperDX = require('@hyperdx/node-opentelemetry');

const LOYALTY_LEVEL = ['platinum', 'gold', 'silver', 'bronze'];
const CACHE_SIZE = process.env.CACHE_SIZE || 1000;

// Rich attribute value pools for realistic workload generation
const PAYMENT_PROCESSORS = ['stripe-us', 'stripe-eu', 'adyen-global', 'braintree-na', 'worldpay-emea', 'cybersource-apac', 'square-us', 'paypal-global'];
const ACQUIRER_BANKS = ['chase-paymentech', 'first-data', 'tsys', 'worldpay', 'barclays', 'adyen-acquiring', 'elavon'];
const RISK_DECISIONS = ['approve', 'approve', 'approve', 'approve', 'soft-decline', 'hard-decline', 'review', 'challenge'];
const RISK_RULES_TRIGGERED = ['velocity-check', 'geo-mismatch', 'amount-threshold', 'card-age', 'device-fingerprint', 'ip-reputation', 'bin-check', 'avs-mismatch', 'none'];
const AUTH_RESPONSES = ['approved', 'approved', 'approved', 'approved', 'declined-insufficient-funds', 'declined-expired-card', 'declined-do-not-honor', 'declined-fraud', 'error-timeout'];
const TOKENIZATION_METHODS = ['network-token', 'gateway-token', 'vault-token', 'apple-pay-token', 'google-pay-token'];
const PCI_COMPLIANCE_LEVELS = ['pci-dss-3.2.1', 'pci-dss-4.0', 'pci-dss-4.0-saq-a'];
const ENCRYPTION_METHODS = ['aes-256-gcm', 'rsa-2048-oaep', 'ecdhe-p256'];
const DECLINE_REASONS = ['none', 'insufficient-funds', 'card-expired', 'cvv-mismatch', 'avs-fail', 'fraud-suspected', 'velocity-exceeded', 'card-restricted'];
const REGIONS = ['us-east-1', 'us-west-2', 'eu-west-1', 'eu-central-1', 'ap-northeast-1', 'ap-southeast-1'];
const ISSUER_COUNTRIES = ['US', 'US', 'US', 'GB', 'DE', 'FR', 'JP', 'CA', 'AU', 'BR', 'IN', 'KR'];
const CARD_BRANDS = ['visa', 'mastercard', 'amex', 'discover', 'jcb', 'unionpay'];
const THREE_DS_VERSIONS = ['1.0', '2.1', '2.2', '2.3'];
const SETTLEMENT_CURRENCIES = ['USD', 'EUR', 'GBP', 'JPY', 'CAD', 'AUD'];
const FEE_TYPES = ['interchange', 'assessment', 'processor', 'gateway', 'cross-border', 'currency-conversion'];
const WEBHOOK_STATUSES = ['pending', 'sent', 'confirmed', 'failed'];

function randomChoice(arr) { return arr[Math.floor(Math.random() * arr.length)]; }
function randomFloat(min, max) { return Math.random() * (max - min) + min; }
function randomInt(min, max) { return Math.floor(Math.random() * (max - min + 1)) + min; }

const meter = metrics.getMeter('payment.card_validator');

// Create an observable gauge to track the cache size
const visaCacheGauge = meter.createObservableGauge('visa_validation_cache.size', {
  description: 'Size of the Visa validation cache',
  unit: 'entries'
});


visaCacheGauge.addCallback((observableResult) => {
  observableResult.observe(visaValidationCache.length);
});



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

    logger.info('cache', {
      size: visaValidationCache.length,
      'cache.operation': 'insert',
      'cache.type': 'visa-validation',
      'cache.capacity': CACHE_SIZE,
      'cache.utilization_pct': Math.round((visaValidationCache.length / CACHE_SIZE) * 100),
      'cache.eviction_policy': 'none-buggy',
      'cache.hit_rate': parseFloat((Math.random() * 0.5 + 0.5).toFixed(3)),
      'cache.memory_estimate_kb': visaValidationCache.length * 2,
      'cache.last_eviction_age_sec': randomInt(0, 3600),
      'cache.key_hash': number.replace(/\D/g, '').substring(0, 8),
      'infra.handler_instance': `payment-${randomInt(0, 9)}`,
    });
    HyperDX.setTraceAttributes({
      'cache.size': visaValidationCache.length
    });
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
  const span = tracer.startSpan('charge');

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

  const transactionsCounter = meter.createCounter('app.payment.transactions');


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

  const processingRegion = randomChoice(REGIONS);
  const processor = randomChoice(PAYMENT_PROCESSORS);
  const riskScore = randomFloat(0, 100);
  const riskDecision = randomChoice(RISK_DECISIONS);
  const authResponse = randomChoice(AUTH_RESPONSES);
  const issuerCountry = randomChoice(ISSUER_COUNTRIES);
  const processingStartMs = Date.now();

  HyperDX.setTraceAttributes({
    'app.payment.card_type': cardType,
    'app.payment.card_valid': valid,
    'app.loyalty.level': loyalty_level,

    // Payment processor attributes
    'app.payment.processor': processor,
    'app.payment.processor_region': processingRegion,
    'app.payment.processor_version': `v${randomInt(3, 8)}.${randomInt(0, 20)}`,
    'app.payment.acquirer_bank': randomChoice(ACQUIRER_BANKS),
    'app.payment.acquirer_response_code': String(randomInt(0, 99)).padStart(2, '0'),
    'app.payment.network_response_time_ms': randomInt(50, 800),
    'app.payment.gateway_latency_ms': randomInt(10, 200),
    'app.payment.total_processing_time_ms': randomInt(100, 1500),
    'app.payment.authorization_response': authResponse,
    'app.payment.authorization_code': authResponse.startsWith('approved') ? `AUTH${randomInt(100000, 999999)}` : '',

    // Card details
    'app.payment.card_brand': randomChoice(CARD_BRANDS),
    'app.payment.card_last_four': lastFourDigits,
    'app.payment.card_bin': number.replace(/\D/g, '').substring(0, 6),
    'app.payment.card_issuer_country': issuerCountry,
    'app.payment.card_funding': randomChoice(['credit', 'debit', 'prepaid']),
    'app.payment.card_level': randomChoice(['classic', 'gold', 'platinum', 'business', 'corporate', 'world-elite']),
    'app.payment.card_is_international': issuerCountry !== 'US',
    'app.payment.tokenization_method': randomChoice(TOKENIZATION_METHODS),

    // Risk and fraud detection
    'app.risk.score': Math.round(riskScore * 100) / 100,
    'app.risk.decision': riskDecision,
    'app.risk.model_version': `fraud-ml-v${randomInt(1, 5)}.${randomInt(0, 30)}`,
    'app.risk.rules_triggered': randomChoice(RISK_RULES_TRIGGERED),
    'app.risk.rules_evaluated_count': randomInt(15, 50),
    'app.risk.evaluation_time_ms': randomInt(5, 150),
    'app.risk.signals_count': randomInt(10, 100),
    'app.risk.device_fingerprint': uuidv4().replace(/-/g, '').substring(0, 20),
    'app.risk.ip_reputation_score': randomFloat(0, 1).toFixed(3),
    'app.risk.velocity_check_window_sec': randomChoice([60, 300, 3600, 86400]),
    'app.risk.velocity_count_in_window': randomInt(1, 50),
    'app.risk.avs_result': randomChoice(['Y', 'N', 'A', 'W', 'Z', 'U']),
    'app.risk.cvv_result': randomChoice(['M', 'N', 'P', 'U']),
    'app.risk.3ds_enrolled': Math.random() < 0.7,
    'app.risk.3ds_authenticated': Math.random() < 0.6,
    'app.risk.3ds_version': randomChoice(THREE_DS_VERSIONS),

    // Transaction details
    'app.transaction.id': transactionId,
    'app.transaction.type': randomChoice(['sale', 'authorization', 'pre-auth']),
    'app.transaction.settlement_currency': randomChoice(SETTLEMENT_CURRENCIES),
    'app.transaction.exchange_rate': randomFloat(0.5, 2.0).toFixed(6),
    'app.transaction.cross_border': issuerCountry !== 'US',
    'app.transaction.recurring': Math.random() < 0.1,
    'app.transaction.installments': randomChoice([1, 1, 1, 3, 6, 12]),
    'app.transaction.decline_reason': riskDecision.includes('decline') ? randomChoice(DECLINE_REASONS) : 'none',
    'app.transaction.idempotency_key': uuidv4(),
    'app.transaction.merchant_category_code': randomChoice(['5941', '5944', '5945', '5947', '5999']),
    'app.transaction.merchant_id': `MERCH-${randomInt(10000, 99999)}`,

    // Fee breakdown
    'app.fees.interchange_bps': randomInt(150, 300),
    'app.fees.assessment_bps': randomInt(10, 15),
    'app.fees.processor_bps': randomInt(20, 50),
    'app.fees.total_fee_bps': randomInt(180, 365),
    'app.fees.type': randomChoice(FEE_TYPES),

    // Security and compliance
    'app.security.pci_compliance': randomChoice(PCI_COMPLIANCE_LEVELS),
    'app.security.encryption': randomChoice(ENCRYPTION_METHODS),
    'app.security.tokenized': true,
    'app.security.mfa_verified': Math.random() < 0.3,
    'app.security.ip_geo_match': Math.random() > 0.1,

    // Webhook and notification
    'app.webhook.status': randomChoice(WEBHOOK_STATUSES),
    'app.webhook.endpoint_count': randomInt(1, 5),
    'app.webhook.delivery_attempt': 1,

    // Infrastructure
    'app.infra.handler_instance': `payment-${randomInt(0, 9)}`,
    'app.infra.memory_usage_mb': randomInt(64, 512),
    'app.infra.event_loop_lag_ms': randomFloat(0.1, 50).toFixed(2),
    'app.infra.active_connections': randomInt(1, 100),
    'app.infra.queue_depth': randomInt(0, 50),
    'app.infra.circuit_breaker_state': randomChoice(['closed', 'closed', 'closed', 'half-open', 'open']),
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


  logger.info('Transaction complete.', {
    transactionId,
    cardType,
    lastFourDigits,
    amount: { units, nanos, currencyCode },
    loyalty_level,
    cached,
    // Rich payment context
    'payment.processor': processor,
    'payment.processor_region': processingRegion,
    'payment.gateway_latency_ms': randomInt(10, 200),
    'payment.total_processing_ms': Date.now() - processingStartMs,
    'payment.authorization_response': authResponse,
    'payment.risk_score': Math.round(riskScore * 100) / 100,
    'payment.risk_decision': riskDecision,
    'payment.3ds_required': Math.random() < 0.3,
    'payment.card_brand': randomChoice(CARD_BRANDS),
    'payment.card_issuer_country': issuerCountry,
    'payment.card_funding': randomChoice(['credit', 'debit', 'prepaid']),
    'payment.tokenization': randomChoice(TOKENIZATION_METHODS),
    'payment.cross_border': issuerCountry !== 'US',
    'payment.settlement_currency': randomChoice(SETTLEMENT_CURRENCIES),
    'payment.interchange_bps': randomInt(150, 300),
    'payment.fee_total_bps': randomInt(180, 365),
    // Transaction metadata
    'transaction.type': randomChoice(['sale', 'authorization', 'pre-auth']),
    'transaction.installments': randomChoice([1, 1, 1, 3, 6, 12]),
    'transaction.recurring': Math.random() < 0.1,
    'transaction.merchant_category_code': randomChoice(['5941', '5944', '5945', '5947', '5999']),
    'transaction.idempotency_key': uuidv4(),
    // Security context
    'security.pci_compliance': randomChoice(PCI_COMPLIANCE_LEVELS),
    'security.encryption': randomChoice(ENCRYPTION_METHODS),
    'security.avs_result': randomChoice(['Y', 'N', 'A', 'W', 'Z', 'U']),
    'security.cvv_result': randomChoice(['M', 'N', 'P', 'U']),
    'security.mfa_verified': Math.random() < 0.3,
    // Infrastructure
    'infra.handler_instance': `payment-${randomInt(0, 9)}`,
    'infra.memory_usage_mb': randomInt(64, 512),
    'infra.event_loop_lag_ms': parseFloat(randomFloat(0.1, 50).toFixed(2)),
    'infra.active_connections': randomInt(1, 100),
    'infra.circuit_breaker': randomChoice(['closed', 'closed', 'closed', 'half-open', 'open']),
    // Network
    'net.peer_address': `10.0.${randomInt(0, 255)}.${randomInt(1, 254)}`,
    'grpc.method': '/oteldemo.PaymentService/Charge',
    'grpc.status_code': 'OK',
  });
  span.end();
  transactionsCounter.add(1, { 'app.payment.currency': currencyCode });
  return { transactionId };
};
