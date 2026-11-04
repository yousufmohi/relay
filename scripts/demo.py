"""Sends six chat requests that share one of two long system prompts to a running
router (default: the simulated compose stack on :8081) and prints which worker
served each one and its time to first token. Standard library only."""
import json, sys, time, urllib.request

URL = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:8081"
support = "You are a support agent for Initech Cloud. Answer briefly and cite the docs. " * 40
review = "You are a code reviewer. Point out bugs and suggest fixes in Rust code. " * 40


def ask(sys_prompt, q):
    body = json.dumps({"messages": [{"role": "system", "content": sys_prompt}, {"role": "user", "content": q}],
                       "max_tokens": 16, "stream": True}).encode()
    req = urllib.request.Request(URL + "/v1/chat/completions", body, {"Content-Type": "application/json"})
    t0 = time.time()
    with urllib.request.urlopen(req) as r:
        r.readline()
        ttft = (time.time() - t0) * 1000
        r.read()
        return r.headers["X-Relay-Worker"], ttft

for name, s, q in [("support", support, "How do I reset my password?"), ("support", support, "Where are my invoices?"),
                   ("review", review, "Review this handler."), ("support", support, "Can I change regions?"),
                   ("review", review, "Is this loop racy?"), ("support", support, "How do I delete my account?")]:
    w, t = ask(s, q)
    print(f"{name:8} {q:30} worker={w}  ttft={t:4.0f} ms")
    time.sleep(1)  # let the sidecar stats catch up, like real traffic would
