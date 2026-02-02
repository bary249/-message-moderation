#!/usr/bin/env python3
"""
Complete sync script: Fetch → Remove Duplicates → Score → Push to deployed.
Run this to keep deployed backend in sync with Snowflake data.
"""
import sys
sys.path.insert(0, '.')

import asyncio
import requests
import json
import anthropic
from datetime import datetime, timedelta
from app.database import SessionLocal
from app.models.database import Message
from app.core.config import settings
from app.services.snowflake_service import snowflake_service

API_URL = "https://message-moderation-production.up.railway.app/api/v1"


async def fetch_from_snowflake(db, days=2):
    """Fetch messages from Snowflake for the last N days."""
    print(f"\n{'='*50}")
    print("STEP 1: FETCH FROM SNOWFLAKE")
    print(f"{'='*50}")
    
    if not snowflake_service.is_available():
        print("Snowflake not configured, skipping fetch")
        return
    
    total_new = 0
    for i in range(days):
        date = (datetime.now() - timedelta(days=i)).strftime('%Y-%m-%d')
        print(f"Fetching {date}...", end=" ", flush=True)
        
        try:
            messages = await snowflake_service.get_messages_by_date(date)
            new = 0
            for msg in messages:
                text = msg.get('text', '')
                if not text or text.strip() == '' or text == '📷 image':
                    continue
                
                existing = db.query(Message).filter(
                    Message.group_id == msg.get('interest_group_id'),
                    Message.sender_id == msg.get('user_id'),
                    Message.original_message == text
                ).first()
                
                if not existing:
                    db_message = Message(
                        original_message=text,
                        processed_message=text,
                        building_id=msg.get('community_id'),
                        building_name=msg.get('building_name'),
                        client_name=msg.get('client_name'),
                        group_id=msg.get('interest_group_id'),
                        group_name=msg.get('group_name'),
                        sender_id=msg.get('user_id'),
                        message_timestamp=msg.get('created_at')
                    )
                    db.add(db_message)
                    new += 1
            
            db.commit()
            total_new += new
            print(f"{new} new messages")
        except Exception as e:
            print(f"Error: {e}")
    
    print(f"Total new messages fetched: {total_new}")


def remove_duplicates(db):
    """Remove duplicate messages from local DB."""
    print(f"\n{'='*50}")
    print("STEP 2: REMOVE DUPLICATES")
    print(f"{'='*50}")
    
    from sqlalchemy import func
    
    # Find duplicates (same group_id, sender_id, original_message)
    subq = db.query(
        Message.group_id,
        Message.sender_id,
        Message.original_message,
        func.min(Message.id).label('keep_id')
    ).group_by(
        Message.group_id,
        Message.sender_id,
        Message.original_message
    ).subquery()
    
    # Get IDs to keep
    keep_ids = [r[0] for r in db.query(subq.c.keep_id).all()]
    
    total_before = db.query(Message).count()
    
    if keep_ids:
        # Delete duplicates
        deleted = db.query(Message).filter(~Message.id.in_(keep_ids)).delete(synchronize_session=False)
        db.commit()
        print(f"Removed {deleted} duplicates")
    else:
        print("No duplicates found")
    
    total_after = db.query(Message).count()
    print(f"Messages: {total_before} → {total_after}")


def score_unscored_locally(db):
    """Score any unscored messages in local DB."""
    print(f"\n{'='*50}")
    print("STEP 3: SCORE UNSCORED MESSAGES")
    print(f"{'='*50}")
    
    unscored = db.query(Message).filter(Message.moderation_score == None).all()
    
    if not unscored:
        print("All messages already scored ✓")
        return
    
    print(f"Scoring {len(unscored)} messages...")
    client = anthropic.Anthropic(api_key=settings.anthropic_api_key)
    
    for i, msg in enumerate(unscored):
        print(f"  [{i+1}/{len(unscored)}] {msg.original_message[:40]}...", end=" ", flush=True)
        try:
            response = client.messages.create(
                model="claude-3-haiku-20240307",
                max_tokens=200,
                messages=[{"role": "user", "content": f"""Score this message (0.0=clean, 1.0=severe):
"{msg.original_message[:500]}"

Reply ONLY with JSON: {{"score": 0.0, "spam": 0.0, "violence": 0.0, "inappropriate": 0.0}}"""}]
            )
            data = json.loads(response.content[0].text)
            msg.moderation_score = data.get("score", 0.5)
            msg.spam_score = data.get("spam", 0.0)
            msg.violence_score = data.get("violence", 0.0)
            msg.inappropriate_content_score = data.get("inappropriate", 0.0)
            msg.adversity_score = 0.0
            msg.processed_message = msg.original_message
            print(f"Score: {msg.moderation_score:.2f}")
        except Exception as e:
            print(f"Error: {e}")
            msg.moderation_score = 0.5
        
        db.commit()
    
    print(f"Scored {len(unscored)} messages ✓")


async def main_async():
    db = SessionLocal()
    
    # Step 1: Fetch from Snowflake
    await fetch_from_snowflake(db, days=2)
    
    # Step 2: Remove duplicates
    remove_duplicates(db)
    
    # Step 3: Score unscored
    score_unscored_locally(db)
    
    return db


def main():
    # Run async fetch first
    db = asyncio.run(main_async())
    
    print(f"\n{'='*50}")
    print("STEP 4: PUSH TO DEPLOYED")
    print(f"{'='*50}")
    
    messages = db.query(Message).all()
    print(f"Found {len(messages)} local messages")
    
    # Convert to dict
    data = []
    for m in messages:
        data.append({
            "original_message": m.original_message,
            "processed_message": m.processed_message,
            "building_id": m.building_id,
            "building_name": m.building_name,
            "client_name": m.client_name,
            "group_id": m.group_id,
            "group_name": m.group_name,
            "sender_id": m.sender_id,
            "message_timestamp": str(m.message_timestamp) if m.message_timestamp else None,
            "moderation_score": m.moderation_score,
            "adversity_score": m.adversity_score,
            "violence_score": m.violence_score,
            "inappropriate_content_score": m.inappropriate_content_score,
            "spam_score": m.spam_score,
            "is_reviewed": m.is_reviewed
        })
    db.close()
    
    # Push to deployed backend in batches
    batch_size = 50
    for i in range(0, len(data), batch_size):
        batch = data[i:i+batch_size]
        print(f"Pushing batch {i//batch_size + 1} ({len(batch)} messages)...", end=" ", flush=True)
        
        resp = requests.post(f"{API_URL}/moderation/bulk-import", json=batch, timeout=60)
        if resp.status_code == 200:
            result = resp.json()
            print(f"✓ Imported: {result.get('imported')}, Updated: {result.get('updated')}")
        else:
            print(f"✗ Error: {resp.text}")
    
    print("\nDone! Refresh deployed frontend to see messages.")

if __name__ == "__main__":
    main()
