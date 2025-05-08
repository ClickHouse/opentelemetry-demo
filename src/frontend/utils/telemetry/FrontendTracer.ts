// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

import HyperDX from '@hyperdx/browser';

const FrontendTracer = async () => {

  HyperDX.init({
    apiKey: 'NOT_USED_RIGHT_NOW',
    service: 'frontend',
    tracePropagationTargets: [/.*/i],
    consoleCapture: true,
    advancedNetworkCapture: true, 
    url: 'http://localhost:4318',
  });

};

export default FrontendTracer;
