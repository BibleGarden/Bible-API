"""Finish reasons of the expansion call: 10 calls on a recitation-prone query, prompt of the mounted checkout."""
import collections, sys, time, httpx
import scripture_search as s
from deadline import Deadline
from query_rewrite import build_query_rewriter

label = sys.argv[1]
reasons = collections.Counter()
for _ in range(10):
    finish = []
    def hook(response):
        response.read()
        try:
            finish.append(response.json()["candidates"][0].get("finishReason"))
        except Exception:
            finish.append(f"HTTP {response.status_code}")
    with httpx.Client(event_hooks={"response": [hook]}) as client:
        with build_query_rewriter(stage=s.SCRIPTURE_REWRITE_PROVIDER, http_client=client, timeout=20, attempts=1) as g:
            try:
                print(s._expand(g, "How can I cope with worry?", Deadline(30)), flush=True)
            except Exception as error:
                print(type(error).__name__, flush=True)
    reasons[finish[-1] if finish else "timeout"] += 1
    time.sleep(4)
print(label, dict(reasons))
