"""Generates the Relay Grafana dashboard JSON.

    python deploy/grafana/gen_dashboard.py

Writes deploy/grafana/dashboards/relay.json and a copy inside the Helm chart
(deploy/helm/relay/dashboards/relay.json), which ships it as a ConfigMap.
"""
import json
from pathlib import Path

HERE = Path(__file__).resolve().parent

PANELS = [
    # (title, unit, [(expr, legend)], width)
    ("Requests/s by status", "reqps", [("sum by (status) (rate(relay_requests_total[1m]))", "{{status}}")], 8),
    ("TTFT p50 / p99 by policy", "s", [
        ("histogram_quantile(0.5, sum by (le, policy) (rate(relay_ttft_seconds_bucket[1m])))", "p50 {{policy}}"),
        ("histogram_quantile(0.99, sum by (le, policy) (rate(relay_ttft_seconds_bucket[1m])))", "p99 {{policy}}")], 8),
    ("Prefix-cache hit rate (worker-reported)", "percentunit", [
        ("sum by (policy) (rate(relay_cached_tokens_total[1m])) / sum by (policy) (rate(relay_prompt_tokens_total[1m]))", "{{policy}}")], 8),
    ("Inter-token latency p50 / p99", "s", [
        ("histogram_quantile(0.5, sum by (le) (rate(relay_itl_seconds_bucket[1m])))", "p50"),
        ("histogram_quantile(0.99, sum by (le) (rate(relay_itl_seconds_bucket[1m])))", "p99")], 8),
    ("Routing match length (blocks) p50 / p90", "short", [
        ("histogram_quantile(0.5, sum by (le) (rate(relay_route_match_blocks_bucket[1m])))", "p50"),
        ("histogram_quantile(0.9, sum by (le) (rate(relay_route_match_blocks_bucket[1m])))", "p90")], 8),
    ("Bounded-load exclusions/s", "short", [("sum(rate(relay_route_bounded_load_exclusions_total[1m]))", "excluded")], 8),
    ("Admission queue depth & in-flight", "short", [
        ("sum(relay_queue_depth)", "queued (router)"), ("sum(relay_inflight_requests)", "in-flight")], 8),
    ("429 / 503 rejections/s", "reqps", [("sum by (reason) (rate(relay_rejected_total[1m]))", "{{reason}}")], 8),
    ("Failovers/s and pause p99", "short", [
        ("sum(rate(relay_failovers_total[1m]))", "failovers/s"),
        ("histogram_quantile(0.99, sum by (le) (rate(relay_failover_pause_seconds_bucket[5m])))", "pause p99 (s)")], 8),
    ("Worker queue depth (sidecar)", "short", [("max by (worker) (relay_worker_queue_depth)", "{{worker}}")], 8),
    ("Worker KV-cache usage (sidecar)", "percentunit", [("max by (worker) (relay_worker_kv_cache_usage)", "{{worker}}")], 8),
    ("Worker prefix-cache hit rate (sidecar)", "percentunit", [
        ("max by (worker) (rate(relay_worker_prefix_cache_hit_tokens_total[1m])) / max by (worker) (rate(relay_worker_prefix_cache_query_tokens_total[1m]))", "{{worker}}")], 8),
    ("Circuit breaker state (0 closed, 1 open, 2 half-open)", "short", [("max by (worker) (relay_circuit_state)", "{{worker}}")], 8),
    ("Eligible workers per router", "short", [("sum by (pod) (relay_worker_eligible)", "{{pod}}")], 8),
    ("Autoscaler replicas", "short", [
        ("max(relay_autoscaler_replicas)", "current"), ("max(relay_autoscaler_desired_replicas)", "desired")], 8),
    ("Autoscaler decisions", "short", [("sum by (direction, reason) (increase(relay_autoscaler_decisions_total[5m]))", "{{direction}}: {{reason}}")], 8),
]


def build() -> dict:
    panels, x, y = [], 0, 0
    for i, (title, unit, targets, w) in enumerate(PANELS):
        if x + w > 24:
            x, y = 0, y + 8
        panels.append({
            "id": i + 1, "type": "timeseries", "title": title,
            "datasource": {"type": "prometheus", "uid": "${datasource}"},
            "gridPos": {"x": x, "y": y, "w": w, "h": 8},
            "fieldConfig": {"defaults": {"unit": unit, "custom": {"lineWidth": 2, "fillOpacity": 0}}, "overrides": []},
            "options": {"legend": {"displayMode": "list", "placement": "bottom"}, "tooltip": {"mode": "multi"}},
            "targets": [{"refId": chr(65 + j), "expr": e, "legendFormat": l} for j, (e, l) in enumerate(targets)],
        })
        x += w
    return {
        "title": "Relay — cache-aware LLM router", "uid": "relay-overview", "schemaVersion": 39, "version": 1,
        "time": {"from": "now-15m", "to": "now"}, "refresh": "5s", "tags": ["relay"],
        "templating": {"list": [{"name": "datasource", "type": "datasource", "query": "prometheus", "label": "Data source"}]},
        "panels": panels,
    }


if __name__ == "__main__":
    text = json.dumps(build(), indent=2) + "\n"
    for p in (HERE / "dashboards" / "relay.json", HERE.parent / "helm" / "relay" / "dashboards" / "relay.json"):
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(text, encoding="utf-8")
    print("wrote", len(PANELS), "panels")
