{{/*
Expand the name of the chart.
*/}}
{{- define "tcp-bridge.name" -}}
{{- .Chart.Name | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "tcp-bridge.fullname" -}}
{{- $name := .Chart.Name }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "tcp-bridge.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "tcp-bridge.labels" -}}
helm.sh/chart: {{ include "tcp-bridge.chart" . }}
{{ include "tcp-bridge.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "tcp-bridge.selectorLabels" -}}
app.kubernetes.io/name: {{ include "tcp-bridge.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app: {{ include "tcp-bridge.name" . }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "tcp-bridge.serviceAccountName" -}}
{{- include "tcp-bridge.fullname" . }}
{{- end }}
