#!/usr/bin/env bash
# GPU benchmark session: create the VM, deploy Relay with vLLM workers, run
# E1, E2, E3 and E8 against it, copy results back and plot them locally.
# Always destroy afterwards with `make bench-down`.
#
# Prerequisites: terraform, AWS credentials in the environment, an SSH key at
# ~/.ssh/id_ed25519(.pub), and accepted spot quota for g6 instances.
set -euo pipefail
cd "$(dirname "$0")/.."
TF=deploy/terraform/gpu-bench
EXPS=${EXPS:-"e1 e2 e3 e8"}
# vLLM on 4x L4 sustains several times the simulated workers' rate.
RATE_SCALE=${RATE_SCALE:-3}

terraform -chdir=$TF init -input=false
terraform -chdir=$TF apply -auto-approve -input=false
IP=$(terraform -chdir=$TF output -raw public_ip)
SSH="ssh -o StrictHostKeyChecking=accept-new ubuntu@$IP"

echo "waiting for cloud-init on $IP ..."
until $SSH test -f /var/lib/relay-ready 2>/dev/null; do sleep 15; done

rsync -az --exclude .venv --exclude bin --exclude results --exclude .git -e "ssh -o StrictHostKeyChecking=accept-new" ./ ubuntu@$IP:relay/

$SSH bash -s <<EOF
set -euxo pipefail
cd relay
for c in router sidecar autoscaler loadgen simworker; do
  docker build -q --build-arg COMPONENT=\$c -t relay/\$c:dev .
  docker save relay/\$c:dev | sudo k3s ctr images import -
done
helm upgrade --install relay deploy/helm/relay -n relay --create-namespace \
  --set worker.flavor=vllm --set worker.replicas=4 --set router.replicas=1 \
  --set-string router.env.STALL_TIMEOUT_MS=5000 --wait --timeout 30m
python3 -m venv .venv && .venv/bin/pip install -q -r experiments/requirements.txt
for e in $EXPS; do
  RELAY_TARGET=k8s RELAY_VARIANT=gpu RELAY_RATE_SCALE=$RATE_SCALE RELAY_ROUTER_URL=http://localhost:30080 \
    .venv/bin/python experiments/run.py \$e
done
nvidia-smi --query-gpu=name,memory.total --format=csv > results/gpu-hardware.csv
EOF

mkdir -p results
rsync -az -e "ssh -o StrictHostKeyChecking=accept-new" "ubuntu@$IP:relay/results/" results/
RELAY_VARIANT=gpu make report
echo "Done. Results in results/*-gpu, report in docs/report.md. Now run: make bench-down"
