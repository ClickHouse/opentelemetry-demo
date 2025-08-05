# OpenTelemetry Demo with ClickStack on Kubernetes

This project deploys the OpenTelemetry Demo instrumented with ClickStack on a Kubernetes cluster. The demo sends traces, metrics, and logs to ClickHouse.

## Requirements

- A running Kubernetes cluster
- A ClickHouse deployment (self-hosted or managed)

## 1. (Optional) Install cert-manager

If your setup needs TLS certificates, install [cert-manager](https://cert-manager.io/) using Helm:

```
helm repo add jetstack https://charts.jetstack.io # Add Cert manager repo

helm install cert-manager jetstack/cert-manager --namespace cert-manager --create-namespace --set startupapicheck.timeout=5m --set installCRDs=true --set global.leaderElection.namespace=cert-manager
```

## 2. Create namespace and ClickHouse credentials

Create a namespace for the demo and provide ClickHouse connection details as a Kubernetes secret.

```
kubectl create namespace otel-demo

kubectl -n otel-demo create secret generic otel-k8s-secret \
  --from-literal=CLICKHOUSE_HOST='<CLICKHOUSE_HOST>' \
  --from-literal=CLICKHOUSE_USER='<CLICKHOUSE_USER>' \
  --from-literal=CLICKHOUSE_PASSWORD='<CLICKHOUSE_PASSWORD>' \
  --from-literal=CLICKHOUSE_DB='default'
```

If you’re using a database name other than default, make sure it exists in ClickHouse before continuing.

## 3. Deploy the OpenTelemetry Demo

Apply the main demo configuration:

```
kubectl apply --namespace otel-demo -f kubernetes/opentelemetry-demo.yaml
```

## 4. Install Kubernetes Metrics Collector

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

