{{- define "gb.name" -}}{{ .Release.Name }}{{- end -}}

{{- define "gb.labels" -}}
app.kubernetes.io/part-of: gpu-broker
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version }}
{{- end -}}

{{- define "gb.selector" -}}
app.kubernetes.io/instance: {{ .root.Release.Name }}
app.kubernetes.io/component: {{ .component }}
{{- end -}}

{{- define "gb.image" -}}
{{- if .Values.image.digest -}}{{ .Values.image.repository }}@{{ .Values.image.digest }}
{{- else -}}{{ .Values.image.repository }}:{{ required "image.digest (preferred) or image.tag must be set" .Values.image.tag }}{{- end -}}
{{- end -}}

{{- define "gb.secretName" -}}{{ default (printf "%s-secrets" .Release.Name) .Values.secrets.existingSecret }}{{- end -}}
{{- define "gb.dbSecretName" -}}{{ default (printf "%s-db" .Release.Name) .Values.database.existingSecret }}{{- end -}}

{{/* Env shared by every database role. */}}
{{- define "gb.dbEnv" -}}
- name: GPUB_DATABASE_URL
  valueFrom: { secretKeyRef: { name: {{ include "gb.dbSecretName" . }}, key: url } }
- name: GPUB_DISPATCH_KEY_FILE
  value: /secrets/dispatch-key
- name: POD_NAME
  valueFrom: { fieldRef: { fieldPath: metadata.name } }
{{- end -}}

{{- define "gb.secretVolume" -}}
- name: secrets
  secret:
    secretName: {{ include "gb.secretName" . }}
    defaultMode: 0440
{{- end -}}

{{- define "gb.podSecurity" -}}
securityContext:
{{ toYaml .Values.podSecurityContext | indent 2 }}
{{- end -}}

{{- define "gb.containerSecurity" -}}
securityContext:
{{ toYaml .Values.containerSecurityContext | indent 2 }}
{{- end -}}

{{- define "gb.cpPlacement" -}}
{{- with .Values.controlPlane.nodeSelector }}
nodeSelector: {{- toYaml . | nindent 2 }}
{{- end }}
{{- with .Values.controlPlane.tolerations }}
tolerations: {{- toYaml . | nindent 2 }}
{{- end }}
{{- end -}}
