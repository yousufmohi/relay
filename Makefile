# Relay — common tasks. Run `make help`.
SHELL := /bin/bash
# Git Bash on Windows rewrites "/run/..." arguments into Windows paths; turn that off.
export MSYS_NO_PATHCONV := 1
GO ?= go
PY ?= $(if $(wildcard .venv/bin/python),.venv/bin/python,$(if $(wildcard .venv/Scripts/python.exe),.venv/Scripts/python.exe,python3))
COMPONENTS := router sidecar autoscaler loadgen simworker
TAG ?= dev
CLUSTER ?= relay
NAME ?= e1

.PHONY: help
help: ## list targets
	@grep -E '^[a-zA-Z0-9_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  %-16s %s\n", $$1, $$2}'

# ---------------------------------------------------------------- code
.PHONY: build test lint proto fmt
build: ## build all Go binaries into bin/
	@mkdir -p bin
	@for c in $(COMPONENTS); do $(GO) build -o bin/$$c ./$$c/cmd/$$c || exit 1; done

test: ## unit + integration tests (race detector where supported)
	$(GO) test -race -count=1 ./... 2>/dev/null || $(GO) test -count=1 ./...

lint: ## gofmt, go vet, staticcheck, buf lint
	@test -z "$$(gofmt -l $$(git ls-files '*.go' | grep -v '^gen/'))" || (gofmt -l . ; exit 1)
	$(GO) vet ./...
	$(GO) run honnef.co/go/tools/cmd/staticcheck@2025.1.1 ./...
	$(GO) run github.com/bufbuild/buf/cmd/buf@v1.47.2 lint

proto: ## regenerate gen/ from proto/
	PATH="$$PATH:$$($(GO) env GOPATH)/bin" buf generate

fmt:
	gofmt -w $$(git ls-files '*.go' | grep -v '^gen/')

venv: ## python env for experiments
	python3 -m venv .venv && $(PY) -m pip install -q -r experiments/requirements.txt

# ---------------------------------------------------------------- images / local
.PHONY: images compose-up compose-sim compose-down
images: ## build all container images (relay/<component>:$(TAG))
	@for c in $(COMPONENTS); do docker build -q --build-arg COMPONENT=$$c -t relay/$$c:$(TAG) . || exit 1; done

compose-up: ## router + 3 llama.cpp CPU workers (downloads Qwen2.5-0.5B GGUF once)
	docker compose -f deploy/docker-compose.yml up --build -d router

compose-sim: ## router + 3 simulated workers on :8081 (no model needed)
	docker compose -f deploy/docker-compose.yml --profile sim up --build -d simrouter

compose-down:
	docker compose -f deploy/docker-compose.yml --profile sim down

# ---------------------------------------------------------------- experiments
.PHONY: exp exp-quick exp-k8s report dashboard
exp: build ## run one experiment locally: make exp NAME=e1  (NAME=all for every one)
	$(PY) experiments/run.py $(NAME)

exp-quick: build ## short smoke version of an experiment
	$(PY) experiments/run.py $(NAME) --quick

exp-k8s: build ## run an experiment against the k3s cluster: make exp-k8s NAME=e4
	RELAY_TARGET=k8s $(PY) experiments/run.py $(NAME)

report: ## re-plot every experiment from results/ and rebuild docs/report.md
	$(PY) experiments/run.py all --analyze-only

dashboard: ## regenerate the Grafana dashboard JSON
	$(PY) deploy/grafana/gen_dashboard.py

# ---------------------------------------------------------------- kubernetes (k3s via k3d)
.PHONY: cluster-up cluster-down
cluster-up: images ## k3s cluster + Prometheus + Grafana + Chaos Mesh + Relay
	k3d cluster create $(CLUSTER) --agents 2 -p "30080:30080@server:0" -p "30300:30300@server:0" --wait || true
	k3d image import -c $(CLUSTER) $(foreach c,$(COMPONENTS),relay/$(c):$(TAG))
	helm repo add prometheus-community https://prometheus-community.github.io/helm-charts >/dev/null
	helm repo add grafana https://grafana.github.io/helm-charts >/dev/null
	helm repo add chaos-mesh https://charts.chaos-mesh.org >/dev/null
	helm repo update >/dev/null
	helm upgrade --install prometheus prometheus-community/prometheus -n monitoring --create-namespace \
	  -f deploy/k8s/prometheus-values.yaml --wait
	helm upgrade --install grafana grafana/grafana -n monitoring -f deploy/k8s/grafana-values.yaml --wait
	helm upgrade --install chaos-mesh chaos-mesh/chaos-mesh -n chaos-mesh --create-namespace \
	  --set chaosDaemon.runtime=containerd --set chaosDaemon.socketPath=/run/k3s/containerd/containerd.sock \
	  --set dashboard.create=false --version 2.7.2 --wait
	helm upgrade --install relay deploy/helm/relay -n relay --create-namespace \
	  --set tracing.jaeger.enabled=true --set autoscaler.enabled=true --wait
	@echo "router: http://localhost:30080   grafana: http://localhost:30300 (admin/relay)"

cluster-down:
	k3d cluster delete $(CLUSTER)

# ---------------------------------------------------------------- GPU benchmarks
.PHONY: bench-gpu bench-down
bench-gpu: ## create GPU VM, run E1-E3 + E8 on vLLM, copy results back (costs money!)
	scripts/bench-gpu.sh

bench-down: ## destroy the GPU VM
	cd deploy/terraform/gpu-bench && terraform destroy -auto-approve
