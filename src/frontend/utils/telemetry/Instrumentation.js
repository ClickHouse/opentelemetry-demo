// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

const HyperDX = require('@hyperdx/node-opentelemetry');


HyperDX.init({
  apiKey: 'YOUR_INGESTION_API_KEY',
  service: 'frontend',
  url: process.env.PUBLIC_OTEL_EXPORTER_OTLP_ENDPOINT || 'http://localhost:4318',
});
