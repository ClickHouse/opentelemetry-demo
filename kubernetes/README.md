# OpenTelemetry Demo with ClickStack on Kubernetes

This project deploys the OpenTelemetry Demo instrumented with ClickStack on a Kubernetes cluster. The demo sends traces, metrics, and logs to ClickHouse.

## Requirements

- A running Kubernetes cluster

## (Optional) Install cert-manager

If your setup needs TLS certificates, install [cert-manager](https://cert-manager.io/) using Helm:

```
helm repo add jetstack https://charts.jetstack.io # Add Cert manager repo

helm install cert-manager jetstack/cert-manager --namespace cert-manager --create-namespace --set startupapicheck.timeout=5m --set installCRDs=true --set global.leaderElection.namespace=cert-manager
```

## Deploy the OpenTelemetry Demo

Apply the main demo configuration to deploy the OpenTelemetry demo without any instrumentation. 

```
kubectl apply --namespace otel-demo -f kubernetes/opentelemetry-demo.yaml
```

## Deploy ClickStack

This section uses the [official Helm chart](https://clickhouse.com/docs/use-cases/observability/clickstack/deployment/helm) to get Clickstack deploy. 

### Add the Helm chart repository 

Add the HyperDX Helm repository:

```
helm repo add clickstack https://clickhouse.github.io/ClickStack-helm-charts
helm repo update
```

### Install the ClickStack operators

Install the ClickStack operators:

```
helm install clickstack-operators clickstack/clickstack-operators -n otel-demo
```

### All in one

To deploy the ClickStack with ClickHouse included:

```
helm install my-clickstack clickstack/clickstack --set global.storageClassName="standard-rwo" -n otel-demo
```

You might need to adjust the storageClassName according to your Kubernetes cluster configuration. 

### Use ClickHouse Cloud

If you'd rather use ClickHouse Cloud, you can deploy Clickstack and [disable the included ClickHouse](https://clickhouse.com/docs/use-cases/observability/clickstack/deployment/helm#using-clickhouse-cloud). 

```
# specify ClickHouse Cloud credentials
export CLICKHOUSE_URL=<CLICKHOUSE_CLOUD_URL> # full https url
export CLICKHOUSE_USER=<CLICKHOUSE_USER>
export CLICKHOUSE_PASSWORD=<CLICKHOUSE_PASSWORD>
export CLICKHOUSE_DATABASE=<CLICKHOUSE_DATABASE>

helm install my-clickstack clickstack/clickstack --set global.storageClassName="standard-rwo" \
  -n otel-demo \
  --set clickhouse.enabled=false \
  --set hyperdx.config.CLICKHOUSE_USER=${CLICKHOUSE_USER} \
  --set hyperdx.config.CLICKHOUSE_ENDPOINT=${CLICKHOUSE_URL} \
  --set hyperdx.config.CLICKHOUSE_SERVER_ENDPOINT=${CLICKHOUSE_URL} \
  --set hyperdx.secrets.CLICKHOUSE_PASSWORD=${CLICKHOUSE_PASSWORD} \
  --set hyperdx.secrets.CLICKHOUSE_APP_PASSWORD=${CLICKHOUSE_PASSWORD} \
  --set hyperdx.useExistingConfigSecret.useExistingConfigSecret=true \
  --set hyperdx.useExistingConfigSecret.existingConfigSecret="clickhouse-cloud-config" \
  --set hyperdx.useExistingConfigSecret.existingConfigConnectionsKey="connections.json" \
  --set hyperdx.useExistingConfigSecret.existingConfigSourcesKey="sources.json"
```

### Access HyperDX UI

Check the pods initialization: 

```
kubectl get pods -l "app.kubernetes.io/name=clickstack" -n otel-demo
```

Once the pods are correctly initialized, access the HyperDX UI using port-forward:

```
kubectl -n otel-demo port-forward svc/my-clickstack-app 3000:3000
```

You can then access the UI at `http://localhost:8080`

### Instrument OpenTelemetry demo 

We customized the OpenTelemetry demo application to send data to the Clickstack OTel collector. You simply need to provide the Ingestion API Key for the pods to start sending data to ClickStack. 

Create a [new account](https://clickhouse.com/docs/use-cases/observability/clickstack/getting-started#navigate-to-hyperdx-ui) and retrieve the [Ingestion API Key](https://clickhouse.com/docs/use-cases/observability/clickstack/getting-started/sample-data#copy-ingestion-api-key). 

Create a new Kubernetes secret with the Ingestion API Key:

```
kubectl create secret generic hyperdx-secret \
--from-literal=HYPERDX_API_KEY=<INGESTION_API_KEY> \
-n otel-demo
```

Then restart the OpenTelemetry demo application pods to take in account the Ingestion API Key.

```
kubectl rollout restart deployment -n otel-demo -l app.kubernetes.io/part-of=opentelemetry-demo
```

## Install Kubernetes Metrics Collector

To collect cluster-level metrics (CPU, memory, etc.), deploy the OpenTelemetry Collector with both deployment and daemonset modes.

### Add the OpenTelemetry Helm repo

```
helm repo add open-telemetry https://open-telemetry.github.io/opentelemetry-helm-charts # Add Otel Helm repo
```

### Deploy the collector components

```
helm install --namespace otel-demo k8s-otel-deployment open-telemetry/opentelemetry-collector -f kubernetes/k8s-otel/deployment.yaml 

helm install --namespace otel-demo k8s-otel-daemonset open-telemetry/opentelemetry-collector -f kubernetes/k8s-otel/daemonset.yaml
```

