// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0


const winston = require('winston');

const logger = winston.createLogger({
  level: 'info',
  format: winston.format.json(),
  transports: [
    new winston.transports.Console(),
    
  ],
});

module.exports = logger;
