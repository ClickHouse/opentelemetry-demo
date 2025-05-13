# Installation

The deployment uses Helm chart for some components and kubectl apply for the main otel demo. 

## Install Cert manager (if necessary)

```
helm repo add jetstack https://charts.jetstack.io # Add Cert manager repo

helm install cert-manager jetstack/cert-manager --namespace cert-manager --create-namespace --set startupapicheck.timeout=5m --set installCRDs=true --set global.leaderElection.namespace=cert-manager
```

## Install main demo

```
kubectl apply --namespace otel-demo -f kubernetes/opentelemetry-demo.yaml
```

## Install collector for Kubernetes metrics

Add repository if not present. 

```
helm repo add open-telemetry https://open-telemetry.github.io/opentelemetry-helm-charts # Add Otel Helm repo
```

Install deployment and daemonset to collect metrics. 

```
helm install --namespace otel-demo k8s-otel-deployment open-telemetry/opentelemetry-collector -f kubernetes/k8s-otel/deployment.yaml 

helm install --namespace otel-demo k8s-otel-daemonset open-telemetry/opentelemetry-collector -f kubernetes/k8s-otel/daemonset.yaml
```

