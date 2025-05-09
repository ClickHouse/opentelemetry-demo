// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0
import HyperDX from '@hyperdx/browser';

const {
  NEXT_PUBLIC_OTEL_SERVICE_NAME = '',
  NEXT_PUBLIC_OTEL_EXPORTER_OTLP_TRACES_ENDPOINT = '',
  IS_SYNTHETIC_REQUEST = '',
  PUBLIC_OTEL_EXPORTER_OTLP_ENDPOINT = ''
} = typeof window !== 'undefined' ? window.ENV : {};

console.log(PUBLIC_OTEL_EXPORTER_OTLP_ENDPOINT)
const FrontendTracer = async () => {

  HyperDX.init({
    apiKey: 'YOUR_INGESTION_API_KEY',
    service: 'frontend',
    tracePropagationTargets: [/.*/i],
    consoleCapture: true,
    advancedNetworkCapture: true, 
    url: PUBLIC_OTEL_EXPORTER_OTLP_ENDPOINT || 'http://localhost:4318',
  });

};

export default FrontendTracer;
