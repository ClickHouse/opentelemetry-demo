// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0
import HyperDX from '@hyperdx/browser';

const {
  NEXT_PUBLIC_OTEL_EXPORTER_OTLP_TRACES_ENDPOINT = '',
  IS_SYNTHETIC_REQUEST = '',
  PUBLIC_OTEL_EXPORTER_OTLP_ENDPOINT = '',
  NEXT_PUBLIC_HYPERDX_API_KEY = '',
} = typeof window !== 'undefined' ? window.ENV : {};

console.log("testing env vars in FrontendTracer.ts", {NEXT_PUBLIC_HYPERDX_API_KEY, NEXT_PUBLIC_OTEL_EXPORTER_OTLP_TRACES_ENDPOINT, IS_SYNTHETIC_REQUEST, PUBLIC_OTEL_EXPORTER_OTLP_ENDPOINT});
const FrontendTracer = async () => {

  HyperDX.init({
    apiKey: NEXT_PUBLIC_HYPERDX_API_KEY || '',
    service: 'frontend',
    tracePropagationTargets: [/.*/i],
    consoleCapture: true,
    advancedNetworkCapture: true, 
    url: PUBLIC_OTEL_EXPORTER_OTLP_ENDPOINT || 'http://localhost:4318',
  });

};

export default FrontendTracer;
