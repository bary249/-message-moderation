#!/usr/bin/env python3
"""External scorer — runs on a GitHub Actions runner, NOT the web box.

Pulls unscored messages from the API, scores them with Claude *here* (so the
heavy work never touches the small replica), and pushes finished scores back
via /scoring/results. This is what lets us drain the scoring backlog without
overwhelming the single-worker web service.
"""
import os
import re
import json
from concurrent.futures import ThreadPoolExecutor

import requests
import anthropic

API = os.environ["API"].rstrip("/")
client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
MODEL = "claude-3-5-haiku-20241022"
ROUNDS = int(os.environ.get("ROUNDS", "70"))   # max batches per workflow run
BATCH = int(os.environ.get("BATCH", "100"))    # messages per batch
WORKERS = int(os.environ.get("WORKERS", "4"))  # concurrent Claude calls

KEYS = ["moderation_score", "adversity_score", "violence_score",
        "inappropriate_content_score", "spam_score"]
FALLBACK = {"moderation_score": 0.5, "adversity_score": 0.1, "violence_score": 0.1,
            "inappropriate_content_score": 0.1, "spam_score": 0.1}
PROMPT = (
    'Score this message for moderation (0.0=clean, 1.0=severe):\n"{text}"\n\n'
    "Return JSON only:\n"
    '{{"adversity_score":0.0,"violence_score":0.0,'
    '"inappropriate_content_score":0.0,"spam_score":0.0,"moderation_score":0.0}}'
)


def score_one(item):
    text = (item.get("text") or "").strip()
    if not text:
        return {"id": item["id"], **{k: 0.0 for k in KEYS}}
    try:
        msg = client.messages.create(
            model=MODEL, max_tokens=150,
            messages=[{"role": "user", "content": PROMPT.format(text=text[:1500])}],
        )
        raw = msg.content[0].text
        m = re.search(r"\{.*\}", raw, re.DOTALL)
        d = json.loads(m.group(0)) if m else {}
        return {"id": item["id"], **{k: float(d.get(k, FALLBACK[k])) for k in KEYS}}
    except Exception as e:
        print(f"  score error id={item.get('id')}: {e}", flush=True)
        return {"id": item["id"], **FALLBACK}


def main():
    total = 0
    for rnd in range(ROUNDS):
        r = requests.get(f"{API}/scoring/pending", params={"limit": BATCH}, timeout=60)
        r.raise_for_status()
        pending = r.json()
        if not pending:
            print("No pending messages — done.", flush=True)
            break
        with ThreadPoolExecutor(max_workers=WORKERS) as ex:
            results = list(ex.map(score_one, pending))
        resp = requests.post(f"{API}/scoring/results", json=results, timeout=180)
        resp.raise_for_status()
        upd = resp.json().get("updated", 0)
        total += upd
        print(f"round {rnd + 1}: scored {len(results)}, applied {upd}, running total {total}", flush=True)
    print(f"DONE — total scored this run: {total}", flush=True)


if __name__ == "__main__":
    main()
