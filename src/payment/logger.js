// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

const pino = require('pino')
const HyperDX = require('@hyperdx/node-opentelemetry');

const logger = pino(
  pino.transport({
    mixin: HyperDX.getPinoMixinFunction,
    targets: [
      HyperDX.getPinoTransport('info', { // Send logs info and above
        detectResources: true,
      }),
    ],
  }),
);

module.exports = logger;
