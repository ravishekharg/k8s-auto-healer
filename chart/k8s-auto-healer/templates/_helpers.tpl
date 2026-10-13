{{- define "healer.name" -}}{{ .Release.Name }}{{- end -}}
{{- define "healer.labels" -}}
app.kubernetes.io/name: k8s-auto-healer
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Values.image.tag | quote }}
{{- end -}}
{{- define "healer.selector" -}}
app.kubernetes.io/name: k8s-auto-healer
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}
{{- define "healer.podRules" -}}
- apiGroups: [""]
  resources: ["pods"]
  verbs: ["get", "list", "watch", "delete"]   # delete only for stuck-Terminating pods
- apiGroups: [""]
  resources: ["pods/eviction"]
  verbs: ["create"]                            # PDB-aware removal of unhealthy pods
- apiGroups: [""]
  resources: ["events"]
  verbs: ["create"]
{{- end -}}
