{{/*
Expand the name of the chart.
*/}}
{{- define "scg.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "scg.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Chart name + version label value.
*/}}
{{- define "scg.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels.
*/}}
{{- define "scg.labels" -}}
helm.sh/chart: {{ include "scg.chart" . }}
{{ include "scg.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels (used in pod templates and services).
*/}}
{{- define "scg.selectorLabels" -}}
app.kubernetes.io/name: {{ include "scg.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Selector labels narrowed to the gateway pods (excludes the Redis pod).
*/}}
{{- define "scg.gateway.selectorLabels" -}}
{{ include "scg.selectorLabels" . }}
app.kubernetes.io/component: gateway
{{- end }}

{{/*
Full label set for gateway resources (includes helm.sh/chart etc.).
*/}}
{{- define "scg.gateway.labels" -}}
{{ include "scg.labels" . }}
app.kubernetes.io/component: gateway
{{- end }}

{{/*
Full label set for the bundled Redis resources.
*/}}
{{- define "scg.redis.labels" -}}
{{ include "scg.labels" . }}
app.kubernetes.io/component: redis
{{- end }}

{{/*
Selector labels narrowed to the bundled Redis pod.
*/}}
{{- define "scg.redis.selectorLabels" -}}
{{ include "scg.selectorLabels" . }}
app.kubernetes.io/component: redis
{{- end }}

{{/*
ServiceAccount name to use. Defaults to the release fullname; users can
override.
*/}}
{{- define "scg.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "scg.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Hostname of the bundled Redis Service (only used when redis.enabled).
We expose it as a helper so the ConfigMap and any external integration
points at one place.
*/}}
{{- define "scg.redis.host" -}}
{{- printf "%s-redis" (include "scg.fullname" .) }}
{{- end }}

{{/*
Effective Redis URL the gateway will dial. When redis.enabled is true
we synthesize redis://<svc>:6379; otherwise we fall back to
.Values.affinityStore.redis.url. Used by the ConfigMap.
*/}}
{{- define "scg.redis.url" -}}
{{- if .Values.redis.enabled }}
{{- printf "redis://%s:6379" (include "scg.redis.host" .) }}
{{- else }}
{{- required "affinityStore.redis.url is required when redis.enabled is false" .Values.affinityStore.redis.url }}
{{- end }}
{{- end }}
