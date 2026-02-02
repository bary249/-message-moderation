#!/usr/bin/env python3
"""Simple synchronous scoring script - scores messages one at a time."""
import sys
sys.path.insert(0, '.')

import anthropic
from app.database import SessionLocal
from app.models.database import Message
from app.core.config import settings

client = anthropic.Anthropic(api_key=settings.anthropic_api_key)

def score_message(text):
    """Score a single message synchronously."""
    response = client.messages.create(
        model="claude-3-haiku-20240307",
        max_tokens=200,
        messages=[{"role": "user", "content": f"""Score this message (0.0=clean, 1.0=severe):
"{text[:500]}"

Reply ONLY with JSON: {{"score": 0.0, "spam": 0.0, "violence": 0.0, "inappropriate": 0.0}}"""}]
    )
    
    import json
    try:
        data = json.loads(response.content[0].text)
        return data.get("score", 0.5), data
    except:
        return 0.5, {}

def main():
    db = SessionLocal()
    
    while True:
        msg = db.query(Message).filter(Message.moderation_score == None).first()
        if not msg:
            print("All messages scored!")
            break
        
        remaining = db.query(Message).filter(Message.moderation_score == None).count()
        print(f"[{remaining} left] Scoring: {msg.original_message[:40]}...", end=" ", flush=True)
        
        try:
            score, data = score_message(msg.original_message)
            msg.moderation_score = score
            msg.spam_score = data.get("spam", 0.0)
            msg.violence_score = data.get("violence", 0.0)
            msg.inappropriate_content_score = data.get("inappropriate", 0.0)
            msg.adversity_score = 0.0
            msg.processed_message = msg.original_message
            db.commit()
            print(f"Score: {score:.2f}")
        except Exception as e:
            print(f"Error: {e}")
            msg.moderation_score = 0.5
            db.commit()
    
    db.close()

if __name__ == "__main__":
    main()
