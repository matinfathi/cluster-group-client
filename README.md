# Cluster Group Client

A Python client designed to create, delete, and manage groups across a multi-node cluster reliably over unstable HTTP APIs.

## Prerequisites & Configuration

Copy the example environment file and define your cluster node URLs:

```bash
cp .env.example .env
```

`NODE_URLS` expects a comma-separated list of node endpoints:
```bash
NODE_URLS=http://node1.example.com,http://node2.example.com,http://node3.example.com
```

## Running Locally

Using `uv`:
```bash
uv sync
uv run python src/cluster_group_client/main.py
```

Using standard `pip`:
```bash
pip install -r requirements.txt
python src/cluster_group_client/main.py
```

## Docker

Build the Docker image:
```bash
docker build -t cluster-group-client .
```

Run the container:
```bash
docker run --rm --env-file .env cluster-group-client
```

## Kubernetes Deployment

The [`manifests/`](manifests/) directory contains deployment configurations:
- **`configmap.yaml`**: Injects `NODE_URLS` to the pods.
- **`job.yaml`**: Runs the client to completion as a one-off batch task.
- **`deployment.yaml`**: Runs the client as a persistent service/worker.

Deploy to a cluster (e.g., Minikube):
```bash
# Load local image into Minikube if not pushed to a registry
minikube image load cluster-group-client:latest

# Apply all manifests via Kustomize
kubectl apply -k manifests
```

## Key System Design Decisions

- **Incremental Retries with Backoff**: Requests use 3 attempts with progressive delays (`0.5s`, `1.0s`). This smooths over transient network blips and temporary `500` server errors without thundering herd effects on recovering nodes.
- **Idempotency via Existence Check on 400**: If a node returns HTTP `400` during creation (indicating the group might already exist), the client queries `GET /v1/group/{groupId}/`. If the group exists, it treats the call as successful. This handles cases where a previous attempt succeeded on the server but timed out before the client received the response.
- **Post-Failure Verification**: Before triggering rollbacks on partial cluster failures, all failed nodes are queried (`GET`) to verify if the group was actually created despite network timeouts or dropped responses.
- **Compensating Rollbacks**: If any node permanently fails to register the group, the client triggers delete operations across all nodes where the group was successfully created to maintain cross-cluster consistency.
