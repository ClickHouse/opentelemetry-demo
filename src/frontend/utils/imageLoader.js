// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0
/*
  * We connect to image-provider through the envoy proxy, straight from the browser, for this we need to know the current hostname and port.
  * During building and serverside rendering, these are undefined so we use some conditionals and default values.
  */

export default function imageLoader({ src, width, quality }) {
  return `${src}?w=${width}&q=${quality || 75}`
}
