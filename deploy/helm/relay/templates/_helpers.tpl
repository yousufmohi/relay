{{- define "relay.image" -}}
{{ .root.Values.image.registry }}relay/{{ .name }}:{{ .root.Values.image.tag }}
{{- end -}}

{{- define "relay.labels" -}}
app.kubernetes.io/part-of: relay
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version }}
{{- end -}}

{{- define "relay.otel" -}}
{{- if .Values.tracing.jaeger.enabled }}
- name: OTEL_EXPORTER_OTLP_ENDPOINT
  value: http://{{ .Release.Name }}-jaeger:4317
- name: OTEL_EXPORTER_OTLP_INSECURE
  value: "true"
{{- else if .Values.tracing.otlpEndpoint }}
- name: OTEL_EXPORTER_OTLP_ENDPOINT
  value: {{ .Values.tracing.otlpEndpoint | quote }}
- name: OTEL_EXPORTER_OTLP_INSECURE
  value: "true"
{{- end }}
{{- end -}}
