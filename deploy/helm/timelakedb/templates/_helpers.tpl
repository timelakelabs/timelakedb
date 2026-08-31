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
      (and .Values.encryption.key (not .Values.encryption.existingSecret))
      .Values.adminBootstrapPassword -}}
true
{{- end -}}
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
{{- end -}}
