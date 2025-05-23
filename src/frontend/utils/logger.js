// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

const HyperDX = require('@hyperdx/node-opentelemetry');
const winston = require('winston');

const logger = winston.createLogger({
  level: 'info',
  format: winston.format.json(),
  transports: [
    new winston.transports.Console(),
    HyperDX.getWinstonTransport('info', { // Send logs info and above
      detectResources: true,
    }),
  ],
});

module.exports = logger;
