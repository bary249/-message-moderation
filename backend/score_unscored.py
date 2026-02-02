#!/usr/bin/env python3
"""
Score all unscored messages in the database.
Optionally fetches messages from Snowflake for specified dates first.

Usage:
    python score_unscored.py                    # Score all unscored messages
    python score_unscored.py --fetch            # Fetch today & yesterday from Snowflake, then score
    python score_unscored.py --fetch --days 3   # Fetch last 3 days, then score
    python score_unscored.py --batch 50         # Score in batches of 50 (default: 20)
"""
import sys
import asyncio
import argparse
import hashlib
from datetime import datetime, timedelta

sys.path.insert(0, '.')

from app.database import SessionLocal
from app.models.database import Message, ScoredMessage
from app.services.claude_moderator import ClaudeModerator
from app.services.snowflake_service import snowflake_service

claude_moderator = ClaudeModerator()


async def fetch_messages_for_date(db, target_date: str):
    """Fetch messages from Snowflake for a specific date."""
    print(f"\n[FETCH] Fetching messages for {target_date}...")
    
    if not snowflake_service.is_available():
        print("[FETCH] ERROR: Snowflake not configured")
        return 0
    
    messages = await snowflake_service.get_messages_by_date(target_date)
    print(f"[FETCH] Got {len(messages)} messages from Snowflake")
    
    new_count = 0
    for msg in messages:
        text = msg.get("text", "")
        if not text or text.strip() == "" or text == "📷 image":
            continue
        
        # Check if already exists
        existing = db.query(Message).filter(
            Message.group_id == msg.get("interest_group_id"),
            Message.sender_id == msg.get("user_id"),
            Message.original_message == text
        ).first()
        
        if existing:
            continue
        
        # Check for cached scores
        msg_hash = hashlib.md5(text.encode()).hexdigest()
        cached = db.query(ScoredMessage).filter(
            ScoredMessage.group_id == msg.get("interest_group_id"),
            ScoredMessage.sender_id == msg.get("user_id"),
            ScoredMessage.message_hash == msg_hash
        ).first()
        
        db_message = Message(
            original_message=text,
            processed_message=cached.processed_message if cached else text,
            building_id=msg.get("community_id"),
            building_name=msg.get("building_name"),
            client_name=msg.get("client_name"),
            group_id=msg.get("interest_group_id"),
            group_name=msg.get("group_name"),
            sender_id=msg.get("user_id"),
            message_timestamp=msg.get("created_at"),
            moderation_score=cached.moderation_score if cached else None,
            adversity_score=cached.adversity_score if cached else None,
            violence_score=cached.violence_score if cached else None,
            inappropriate_content_score=cached.inappropriate_content_score if cached else None,
            spam_score=cached.spam_score if cached else None
        )
        db.add(db_message)
        new_count += 1
    
    db.commit()
    print(f"[FETCH] Saved {new_count} new messages")
    return new_count


async def score_message(msg, claude_moderator, semaphore):
    """Score a single message with Claude."""
    try:
        async with semaphore:
            result = await asyncio.wait_for(
                claude_moderator.moderate_message(msg.original_message),
                timeout=60.0
            )
        msg.processed_message = result.processed_message
        msg.moderation_score = result.moderation_score
        msg.adversity_score = result.adversity_score
        msg.violence_score = result.violence_score
        msg.inappropriate_content_score = result.inappropriate_content_score
        msg.spam_score = result.spam_score
        return True
    except asyncio.TimeoutError:
        print(f"  TIMEOUT: {msg.original_message[:40]}...")
        msg.moderation_score = 0.5
        return False
    except Exception as e:
        print(f"  ERROR ({type(e).__name__}): {e}")
        msg.moderation_score = 0.5
        return False


async def score_all_unscored(db, batch_size: int = 20):
    """Score all unscored messages in batches."""
    total_unscored = db.query(Message).filter(Message.moderation_score == None).count()
    
    if total_unscored == 0:
        print("\n[SCORE] No unscored messages found.")
        return
    
    print(f"\n[SCORE] Found {total_unscored} unscored messages")
    print(f"[SCORE] Processing in batches of {batch_size}...")
    
    semaphore = asyncio.Semaphore(2)  # Limit concurrent API calls
    total_scored = 0
    batch_num = 0
    
    while True:
        batch_num += 1
        unscored = db.query(Message).filter(
            Message.moderation_score == None
        ).limit(batch_size).all()
        
        if not unscored:
            break
        
        print(f"\n[BATCH {batch_num}] Scoring {len(unscored)} messages...")
        
        for i, msg in enumerate(unscored):
            print(f"  [{i+1}/{len(unscored)}] {msg.original_message[:50]}...")
            success = await score_message(msg, claude_moderator, semaphore)
            
            # Cache the score
            if msg.moderation_score is not None:
                msg_hash = hashlib.md5(msg.original_message.encode()).hexdigest()
                existing = db.query(ScoredMessage).filter(
                    ScoredMessage.group_id == msg.group_id,
                    ScoredMessage.sender_id == msg.sender_id,
                    ScoredMessage.message_hash == msg_hash
                ).first()
                
                if existing:
                    existing.moderation_score = msg.moderation_score
                    existing.adversity_score = msg.adversity_score
                    existing.violence_score = msg.violence_score
                    existing.inappropriate_content_score = msg.inappropriate_content_score
                    existing.spam_score = msg.spam_score
                    existing.processed_message = msg.processed_message
                else:
                    cached = ScoredMessage(
                        group_id=msg.group_id,
                        sender_id=msg.sender_id,
                        message_hash=msg_hash,
                        moderation_score=msg.moderation_score,
                        adversity_score=msg.adversity_score,
                        violence_score=msg.violence_score,
                        inappropriate_content_score=msg.inappropriate_content_score,
                        spam_score=msg.spam_score,
                        processed_message=msg.processed_message
                    )
                    db.add(cached)
            
            total_scored += 1
            await asyncio.sleep(0.3)  # Rate limit
        
        db.commit()
        print(f"[BATCH {batch_num}] Complete. Total scored: {total_scored}/{total_unscored}")
    
    print(f"\n[SCORE] ✅ Finished! Scored {total_scored} messages.")


async def main():
    parser = argparse.ArgumentParser(description='Score unscored messages')
    parser.add_argument('--fetch', action='store_true', help='Fetch from Snowflake first')
    parser.add_argument('--days', type=int, default=2, help='Days to fetch (default: 2 = today + yesterday)')
    parser.add_argument('--batch', type=int, default=20, help='Batch size for scoring (default: 20)')
    args = parser.parse_args()
    
    db = SessionLocal()
    
    try:
        # Optionally fetch from Snowflake
        if args.fetch:
            print("=" * 60)
            print("FETCHING FROM SNOWFLAKE")
            print("=" * 60)
            
            for i in range(args.days):
                date = (datetime.now() - timedelta(days=i)).strftime('%Y-%m-%d')
                await fetch_messages_for_date(db, date)
        
        # Score all unscored
        print("\n" + "=" * 60)
        print("SCORING UNSCORED MESSAGES")
        print("=" * 60)
        
        await score_all_unscored(db, batch_size=args.batch)
        
    finally:
        db.close()


if __name__ == "__main__":
    asyncio.run(main())
