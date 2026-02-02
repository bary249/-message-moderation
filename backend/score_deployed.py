#!/usr/bin/env python3
"""
Score messages on the DEPLOYED backend (Railway).
Fetches messages and triggers scoring via API calls.

Usage:
    python score_deployed.py
"""
import requests
import time
from datetime import datetime, timedelta

API_URL = "https://message-moderation-production.up.railway.app/api/v1"

# Login credentials
USERNAME = "admin"
PASSWORD = "admin123"

def main():
    # 1. Login (deployed version uses query params)
    print("Logging in to deployed backend...")
    resp = requests.post(f"{API_URL}/auth/login", params={"username": USERNAME, "password": PASSWORD})
    if resp.status_code != 200:
        print(f"Login failed: {resp.text}")
        return
    
    token = resp.json()["access_token"]
    headers = {"Authorization": f"Bearer {token}"}
    print("✓ Logged in")
    
    # 2. Fetch messages for today and yesterday
    for days_ago in [0, 1]:
        date = (datetime.now() - timedelta(days=days_ago)).strftime("%Y-%m-%d")
        print(f"\nFetching messages for {date}...")
        resp = requests.post(f"{API_URL}/snowflake/fetch-by-date", params={"target_date": date})
        if resp.status_code == 200:
            data = resp.json()
            print(f"✓ Fetched {data.get('fetched_from_snowflake', 0)} messages, {data.get('new_messages_saved', 0)} new")
        else:
            print(f"✗ Failed: {resp.text}")
    
    # 3. Check unscored count
    resp = requests.get(f"{API_URL}/moderation/queue", params={"page": 1, "status": "pending"}, headers=headers)
    if resp.status_code == 200:
        data = resp.json()
        unscored = data.get("unscored_count", 0)
        total = data.get("total_count", 0)
        print(f"\nMessages: {total} total, {unscored} unscored")
    
    # 4. Score in batches (small batches, handle timeouts)
    if unscored > 0:
        print(f"\nScoring {unscored} messages (this may take a few minutes)...")
        scored_total = 0
        errors = 0
        
        while errors < 3:
            print(f"  Scoring batch (5 messages)...", end=" ", flush=True)
            try:
                resp = requests.post(f"{API_URL}/moderation/score-batch", params={"limit": 5}, headers=headers, timeout=180)
                
                if resp.status_code != 200:
                    print(f"Error: {resp.status_code}")
                    errors += 1
                    time.sleep(5)
                    continue
                
                data = resp.json()
                scored = data.get("scored", 0)
                remaining = data.get("remaining", 0)
                
                if scored == 0:
                    print("Done!")
                    break
                
                scored_total += scored
                errors = 0  # Reset error count on success
                print(f"Scored {scored}, {remaining} remaining")
                
                if remaining == 0:
                    break
                
                time.sleep(2)  # Delay between batches
                
            except requests.exceptions.Timeout:
                print("Timeout - retrying...")
                errors += 1
                time.sleep(5)
            except Exception as e:
                print(f"Error: {e}")
                errors += 1
                time.sleep(5)
        
        print(f"\n✓ Total scored: {scored_total}")
    else:
        print("\n✓ No unscored messages")
    
    print("\nDone! Refresh the deployed frontend to see scored messages.")

if __name__ == "__main__":
    main()
