#!/bin/bash
# Runs once at first boot: Docker (to build Relay images), the NVIDIA
# container toolkit, single-node k3s (which detects the nvidia runtime), the
# NVIDIA device plugin, and Helm. Writes /var/lib/relay-ready when done.
set -euxo pipefail
export DEBIAN_FRONTEND=noninteractive

apt-get update
apt-get install -y docker.io git make jq curl golang-go
usermod -aG docker ubuntu

# NVIDIA container toolkit (driver is preinstalled on the DLAMI).
curl -fsSL https://nvidia.github.io/libnvidia-container/gpgkey | gpg --dearmor -o /usr/share/keyrings/nvidia-container-toolkit-keyring.gpg
curl -fsSL https://nvidia.github.io/libnvidia-container/stable/deb/nvidia-container-toolkit.list \
  | sed 's#deb https://#deb [signed-by=/usr/share/keyrings/nvidia-container-toolkit-keyring.gpg] https://#g' \
  > /etc/apt/sources.list.d/nvidia-container-toolkit.list
apt-get update
apt-get install -y nvidia-container-toolkit

# k3s picks up the nvidia container runtime automatically when the toolkit is present.
curl -sfL https://get.k3s.io | INSTALL_K3S_EXEC="--write-kubeconfig-mode 644 --disable traefik" sh -
until kubectl get nodes 2>/dev/null | grep -q " Ready"; do sleep 5; done
mkdir -p /home/ubuntu/.kube && cp /etc/rancher/k3s/k3s.yaml /home/ubuntu/.kube/config && chown -R ubuntu /home/ubuntu/.kube

cat <<'EOF' | kubectl apply -f -
apiVersion: node.k8s.io/v1
kind: RuntimeClass
metadata: {name: nvidia}
handler: nvidia
---
apiVersion: apps/v1
kind: DaemonSet
metadata: {name: nvidia-device-plugin, namespace: kube-system}
spec:
  selector: {matchLabels: {name: nvidia-device-plugin}}
  template:
    metadata: {labels: {name: nvidia-device-plugin}}
    spec:
      runtimeClassName: nvidia
      tolerations: [{key: nvidia.com/gpu, operator: Exists, effect: NoSchedule}]
      containers:
        - name: plugin
          image: nvcr.io/nvidia/k8s-device-plugin:v0.17.0
          securityContext: {allowPrivilegeEscalation: false, capabilities: {drop: [ALL]}}
          volumeMounts: [{name: dp, mountPath: /var/lib/kubelet/device-plugins}]
      volumes: [{name: dp, hostPath: {path: /var/lib/kubelet/device-plugins}}]
EOF

curl -fsSL https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash
touch /var/lib/relay-ready
