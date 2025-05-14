// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

const HyperDX = require('@hyperdx/node-opentelemetry');

HyperDX.init({
  apiKey: 'YOUR_INGESTION_API_KEY',
  service: 'payment',
  url: process.env.OTEL_EXPORTER_OTLP_ENDPOINT || 'http://localhost:4318',
});
