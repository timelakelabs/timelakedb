{{/* Chart name, overridable via nameOverride (kept short for the 63-char label cap). */}}
{{- define "timelakedb.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "timelakedb.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "timelakedb.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "timelakedb.labels" -}}
helm.sh/chart: {{ include "timelakedb.chart" . }}
{{ include "timelakedb.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "timelakedb.selectorLabels" -}}
app.kubernetes.io/name: {{ include "timelakedb.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "timelakedb.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "timelakedb.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{/* The image reference, tag defaulting to the chart's appVersion. */}}
{{- define "timelakedb.image" -}}
{{- printf "%s:%s" .Values.image.repository (default .Chart.AppVersion .Values.image.tag) -}}
{{- end -}}

{{/* Non-empty when any inline secret value was supplied, so a chart-managed
     Secret is rendered. Object-store creds and the encryption key are skipped
     when an existingSecret is named for them. */}}
{{- define "timelakedb.hasChartSecret" -}}
{{- if or
      (and .Values.objectStore.enabled .Values.objectStore.accessKeyId (not .Values.objectStore.existingSecret))
      (and (eq .Values.mode "cluster") .Values.cluster.minio.enabled)
      (and .Values.encryption.key (not .Values.encryption.existingSecret))
      .Values.adminBootstrapPassword -}}
true
{{- end -}}
{{- end -}}

{{/* The Consul discovery URL — the bundled dev Consul, or an external one. */}}
{{- define "timelakedb.discoveryUrl" -}}
{{- if .Values.cluster.discovery.embedded -}}
consul://{{ include "timelakedb.fullname" . }}-consul:8500
{{- else -}}
{{- required "cluster.discovery.externalUrl is required when discovery.embedded is false" .Values.cluster.discovery.externalUrl -}}
{{- end -}}
{{- end -}}

{{/* Per-pod identity env shared by every cluster node. TIMELAKE_NODE_ID is the
     pod name (unique), and the ADVERTISED addresses are the pod IP — exactly
     how the Consul discovery drill wires a node, and what a peer connects to.
     Consul excludes self from the peers it returns, so one env serves every pod
     of a StatefulSet, which static TIMELAKE_PEERS could never do. Ordering
     matters: POD_IP/POD_NAME must precede the values that interpolate them. */}}
{{- define "timelakedb.podIdentityEnv" -}}
- name: POD_IP
  valueFrom:
    fieldRef:
      fieldPath: status.podIP
- name: POD_NAME
  valueFrom:
    fieldRef:
      fieldPath: metadata.name
- name: TIMELAKE_NODE_ID
  value: "$(POD_NAME)"
- name: TIMELAKE_ADDR
  value: "0.0.0.0:{{ .Values.service.httpPort }}"
- name: TIMELAKE_FLIGHT_ADDR
  value: "0.0.0.0:{{ .Values.service.flightPort }}"
- name: TIMELAKE_DATA_ADDR
  value: "$(POD_IP):{{ .Values.service.httpPort }}"
- name: TIMELAKE_DISCOVERY
  value: {{ include "timelakedb.discoveryUrl" . | quote }}
{{- end -}}

{{/* envFrom sources shared by every cluster node: the cluster ConfigMap, the
     chart Secret (if any inline creds), and any referenced existingSecrets. */}}
{{- define "timelakedb.clusterEnvFrom" -}}
- configMapRef:
    name: {{ include "timelakedb.fullname" . }}-cluster
{{- if include "timelakedb.hasChartSecret" . }}
- secretRef:
    name: {{ include "timelakedb.fullname" . }}
{{- end }}
{{- with .Values.objectStore.existingSecret }}
- secretRef:
    name: {{ . }}
{{- end }}
{{- with .Values.encryption.existingSecret }}
- secretRef:
    name: {{ . }}
{{- end }}
{{- end -}}

{{/* TLS env (paths into the mounted secret), shared by every node. */}}
{{- define "timelakedb.tlsEnv" -}}
{{- if .Values.tls.enabled }}
- name: TIMELAKE_TLS_CERT
  value: /etc/timelake/tls/tls.crt
- name: TIMELAKE_TLS_KEY
  value: /etc/timelake/tls/tls.key
{{- if .Values.tls.clientCa }}
- name: TIMELAKE_TLS_CLIENT_CA
  value: /etc/timelake/tls/ca.crt
{{- end }}
{{- end }}
{{- end -}}

{{/* The read-only rootfs security context, shared by every node container. */}}
{{- define "timelakedb.containerSecurityContext" -}}
readOnlyRootFilesystem: true
allowPrivilegeEscalation: false
capabilities:
  drop: ["ALL"]
{{- end -}}

{{- define "timelakedb.podSecurityContext" -}}
runAsNonRoot: true
runAsUser: 1000
runAsGroup: 1000
fsGroup: 1000
{{- end -}}

{{/* The discovery endpoint as an HTTP base (for the wait-consul probe). */}}
{{- define "timelakedb.discoveryHttp" -}}
{{- if .Values.cluster.discovery.embedded -}}
http://{{ include "timelakedb.fullname" . }}-consul:8500
{{- else -}}
{{- .Values.cluster.discovery.externalUrl | replace "consul://" "http://" -}}
{{- end -}}
{{- end -}}

{{/* Wait for Consul to have a leader before the node starts. Without it a node
     that comes up before Consul degrades to an empty membership — harmless for
     a data node (it just has no peers yet) but the router REFUSES to start with
     no ingesters, so it would CrashLoopBackOff until Consul caught up. This
     turns that race into an orderly wait. */}}
{{- define "timelakedb.waitConsulInit" -}}
{{- if eq .Values.mode "cluster" }}
- name: wait-consul
  image: {{ include "timelakedb.image" . | quote }}
  imagePullPolicy: {{ .Values.image.pullPolicy }}
  command: ["sh", "-c"]
  args:
    - |
      until curl -fs {{ include "timelakedb.discoveryHttp" . }}/v1/status/leader >/dev/null 2>&1; do
        echo "waiting for consul..."; sleep 2
      done
  securityContext:
    {{- include "timelakedb.containerSecurityContext" . | nindent 4 }}
{{- end }}
{{- end -}}

{{/* When the bundled dev MinIO is used, a node that opens the store must not
     start until the bucket exists. This initContainer waits for MinIO and
     creates the bucket (idempotently), so the node's catalog replay finds it.
     Skipped for external object stores — that bucket is the operator's. */}}
{{- define "timelakedb.ensureBucketInit" -}}
{{- if and (eq .Values.mode "cluster") .Values.cluster.minio.enabled }}
- name: ensure-bucket
  image: minio/mc:latest
  envFrom:
    - secretRef:
        name: {{ include "timelakedb.fullname" . }}
  command: ["sh", "-c"]
  args:
    - |
      until mc --config-dir /tmp/mc alias set m \
        http://{{ include "timelakedb.fullname" . }}-minio:9000 \
        "$AWS_ACCESS_KEY_ID" "$AWS_SECRET_ACCESS_KEY" 2>/dev/null; do
        echo "waiting for minio..."; sleep 2
      done
      mc --config-dir /tmp/mc mb --ignore-existing m/{{ .Values.cluster.minio.bucket }}
  securityContext:
    {{- include "timelakedb.containerSecurityContext" . | nindent 4 }}
  volumeMounts:
    - name: tmp
      mountPath: /tmp
{{- end }}
{{- end -}}

{{/* Refuse configurations that would silently expose an unauthenticated write
     endpoint. Rendered once from NOTES/validation include. */}}
{{- define "timelakedb.validate" -}}
{{- if and (eq .Values.dataAuth "off") (or (eq .Values.service.type "LoadBalancer") (eq .Values.service.type "NodePort")) -}}
{{- fail "dataAuth is \"off\" but service.type exposes the data plane externally (LoadBalancer/NodePort). Set dataAuth to \"optional\" or \"required\", or keep service.type ClusterIP. An open write endpoint is never a silent default." -}}
{{- end -}}
{{- if and .Values.tls.enabled (not .Values.tls.existingSecret) -}}
{{- fail "tls.enabled is true but tls.existingSecret is empty — name the Secret holding tls.crt/tls.key." -}}
{{- end -}}
{{- if and .Values.objectStore.enabled (not .Values.objectStore.url) -}}
{{- fail "objectStore.enabled is true but objectStore.url is empty — set it to s3://bucket/prefix." -}}
{{- end -}}
{{- if eq .Values.mode "cluster" -}}
{{- if not (or .Values.objectStore.enabled .Values.cluster.minio.enabled) -}}
{{- fail "mode is \"cluster\" but no object store is configured — queriers share one store, there is no shared local disk. Set objectStore.enabled (external S3) or cluster.minio.enabled (a bundled dev MinIO)." -}}
{{- end -}}
{{- if and .Values.objectStore.enabled .Values.cluster.minio.enabled -}}
{{- fail "both objectStore.enabled and cluster.minio.enabled are set — pick one object store." -}}
{{- end -}}
{{- end -}}
{{- end -}}
