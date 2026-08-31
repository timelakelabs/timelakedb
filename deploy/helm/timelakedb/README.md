# TimeLakeDB Helm chart

Deploy TimeLakeDB on Kubernetes. This encodes the topology the project actually
tests, so you don't hand-roll StatefulSets, Services and the config/secret
plumbing and get the cluster-role split subtly wrong.

Two modes: `mode: single` (one `role=all` node) and `mode: cluster` (the
router / ingester / querier / compactor split).

## Install — single node

```sh
helm install tldb deploy/helm/timelakedb -n timelakedb --create-namespace
```

One `role=all` node: a StatefulSet with a PVC for the data dir, a client
ClusterIP Service (HTTP `1963`, Flight SQL `1964`), and a headless Service for
stable identity.

## Install — cluster

```sh
# A self-contained cluster with a bundled dev Consul + MinIO (for a smoke):
helm install tldb deploy/helm/timelakedb -n timelakedb --create-namespace \
  --set mode=cluster --set cluster.minio.enabled=true

# Production: external S3, and pin the image tag:
helm install tldb deploy/helm/timelakedb -n timelakedb --create-namespace \
  --set mode=cluster \
  --set objectStore.enabled=true --set objectStore.url=s3://my-bucket/timelake \
  --set objectStore.existingSecret=my-s3-creds \
  --set image.tag=v0.1.0
```

Cluster mode brings up **ingesters as a StatefulSet** (durable WAL + PVC, CL-2
paired) and the **router, queriers and compactor as Deployments** (they own no
durable data). Clients talk only to the router's ClusterIP Service (`tldb-timelakedb`).

Discovery is **Consul, not static `TIMELAKE_PEERS`**. A static peers list cannot
be expressed from a shared StatefulSet pod template: an ingester replicates to
its first ingester peer without filtering itself out, so every pod would need a
*different* list. Consul has each node self-register (node id = pod name, address
= pod IP) and returns peers with self excluded, so `cluster.ingester.replicas` is
a value you can actually turn. A shared object store (`objectStore` external S3,
or the bundled dev MinIO) is required — queriers replay one catalog and there is
no shared local disk.

Verified on a real cluster in `docs/evidence/helm-cluster-smoke.log`: 2 ingesters +
2 queriers + 1 compactor + 1 router self-register in Consul, a write through the
router shards to the ingesters (204), and a read returns it through router →
querier → ingester buffers.

### The internal listener stays internal

The intra-cluster listener (`1965`) is only on the **headless** ingester Service
and is reached by pod IP over the cluster network. It is never on the router's
client Service and never a LoadBalancer — exposing it would reopen the exposure
the de-published port closed, and C3 makes intra-cluster mTLS required.

## Smoke it

```sh
kubectl -n timelakedb port-forward svc/tldb-timelakedb 1963:1963 &
curl -fs http://localhost:1963/health
curl -fs -XPOST 'http://localhost:1963/api/v3/write_lp?db=poc&precision=ns' \
  --data-binary 'cpu,host=a usage=0.9 1'
curl -fs -XPOST http://localhost:1963/api/sql -H 'content-type: application/json' \
  -d '{"db":"poc","sql":"SELECT COUNT(*) AS n FROM cpu"}'
```

Verified on a real cluster in `docs/evidence/helm-single-node-smoke.log`.

## Why a StatefulSet, not a Deployment

A `role=all` node (and, in cluster mode, an ingester) owns durable local state —
the WAL and the data dir. A Deployment reschedules onto a fresh empty volume and
loses un-flushed WAL; a StatefulSet keeps stable identity and reattaches the same
PVC. The querier and router own no data and will be Deployments in phase 2.

## Safe defaults

- **Non-root** (uid 1000, P0-2), **read-only rootfs** with a tmpfs `/tmp`, all
  capabilities dropped, `fsGroup: 1000` so the PVC is writable.
- Listeners bind `0.0.0.0` (the deb/rpm packaging's `127.0.0.1` would be
  unreachable behind a Service).
- The chart **refuses to render `dataAuth: off` behind a LoadBalancer/NodePort**
  Service — an unauthenticated write endpoint on the internet is never silent.
  Set `dataAuth: required` (and issue a token) before exposing the data plane.

## Key values

| Value | Default | Notes |
|---|---|---|
| `image.repository` / `image.tag` | `ghcr.io/timelakelabs/timelakedb` / appVersion | Pin `tag` to a release. |
| `dataAuth` | `off` | `off`\|`optional`\|`required` (SEC-4). |
| `adminBootstrapPassword` | `""` | Empty ⇒ seeds admin/admin, quarantined until rotated. |
| `persistence.size` / `.storageClass` | `10Gi` / cluster default | The data-dir PVC. |
| `objectStore.enabled` + `.url` | `false` | S3-compatible store (SEC-1/C0). Inline creds or `existingSecret`. |
| `encryption.key` / `.existingSecret` | `""` | 64-hex key for encryption at rest (SEC-1). |
| `tls.enabled` + `.existingSecret` | `false` | Secret with `tls.crt`/`tls.key` (+ `ca.crt` for client auth, SEC-3). |
| `service.type` | `ClusterIP` | LoadBalancer/NodePort require `dataAuth != off`. |

Secrets go into a chart-managed Secret when supplied inline, or reference an
`existingSecret` you manage out of band; nothing sensitive lands in a ConfigMap.

See `values.yaml` for the full list.
