from fastapi import APIRouter, Depends, HTTPException, status, BackgroundTasks
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from sqlalchemy.orm import Session
from typing import List, Optional
from app.database import get_db, SessionLocal
from app.models.schemas import *
from app.models.database import Message, Moderator, ModerationReview, ScoredMessage
import hashlib
from app.services.claude_moderator import ClaudeModerator
from app.services.snowflake_service import snowflake_service
from app.core.security import verify_token, verify_password, create_access_token, get_password_hash
from datetime import datetime

router = APIRouter()
security = HTTPBearer()
claude_moderator = ClaudeModerator()

# Auth dependency
async def get_current_moderator(credentials: HTTPAuthorizationCredentials = Depends(security), db: Session = Depends(get_db)):
    token = credentials.credentials
    payload = verify_token(token)
    username = payload.get("sub")
    
    moderator = db.query(Moderator).filter(Moderator.username == username).first()
    if moderator is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Moderator not found"
        )
    return moderator

# Message intake endpoint
@router.post("/messages", response_model=MessageSubmissionResponse)
async def submit_message(message: MessageCreate, db: Session = Depends(get_db)):
    """Receive and moderate a new message"""
    
    # Get moderation analysis
    moderation_result = await claude_moderator.moderate_message(message.original_message)
    
    # Determine auto-approval/rejection
    auto_approved = claude_moderator.should_auto_approve(moderation_result)
    auto_rejected = claude_moderator.should_auto_reject(moderation_result)
    
    # Set approval status
    is_approved = None
    status_text = "pending_review"
    
    if auto_approved:
        is_approved = True
        status_text = "auto_approved"
    elif auto_rejected:
        is_approved = False
        status_text = "rejected"
    
    # Create message record
    db_message = Message(
        original_message=message.original_message,
        processed_message=moderation_result.processed_message,
        building_id=message.building_id,
        group_id=message.group_id,
        sender_id=message.sender_id,
        moderation_score=moderation_result.moderation_score,
        # is_approved and auto_approved removed as they are not in DB model
        adversity_score=moderation_result.adversity_score,
        violence_score=moderation_result.violence_score,
        inappropriate_content_score=moderation_result.inappropriate_content_score,
        spam_score=moderation_result.spam_score
    )
    
    db.add(db_message)
    db.commit()
    db.refresh(db_message)
    
    return MessageSubmissionResponse(
        message_id=db_message.id,
        moderation_result=moderation_result,
        status=status_text
    )

# Get moderation queue
@router.get("/moderation/queue", response_model=ModerationQueueResponse)
async def get_moderation_queue(
    page: int = 1,
    per_page: int = 20,
    status: Optional[str] = "pending",
    sort_by: Optional[str] = "time_desc",
    score_min: Optional[float] = None,
    score_max: Optional[float] = None,
    current_moderator: Moderator = Depends(get_current_moderator),
    db: Session = Depends(get_db)
):
    """Get messages pending moderation"""
    
    query = db.query(Message)
    
    # Filter by review status
    if status == "pending":
        query = query.filter(Message.is_reviewed == False)
    elif status == "reviewed":
        query = query.filter(Message.is_reviewed == True)
    # 'all' or any other value returns everything
    
    # Filter by score range (only if both are provided)
    if score_min is not None and score_max is not None:
        query = query.filter(
            (Message.moderation_score >= score_min) & 
            (Message.moderation_score <= score_max) |
            (Message.moderation_score == None)  # Include unscored
        )
    
    # Server-side sorting
    if sort_by == "score":
        query = query.order_by(Message.moderation_score.desc().nullslast())
    elif sort_by == "group":
        query = query.order_by(Message.group_name.asc().nullslast())
    elif sort_by == "time_asc":
        query = query.order_by(Message.message_timestamp.asc().nullslast())
    else:  # time_desc (default)
        query = query.order_by(Message.message_timestamp.desc().nullslast())
    
    # Pagination
    offset = (page - 1) * per_page
    total_count = query.count()
    messages = query.offset(offset).limit(per_page).all()
    
    # Count unscored messages (across all, not just current page)
    unscored_count = db.query(Message).filter(Message.moderation_score == None).count()
    
    return ModerationQueueResponse(
        pending_messages=messages,
        total_count=total_count,
        unscored_count=unscored_count,
        page=page,
        per_page=per_page
    )


# Clear all messages from local database
@router.delete("/moderation/clear-all")
async def clear_all_messages(
    current_moderator: Moderator = Depends(get_current_moderator),
    db: Session = Depends(get_db)
):
    """Delete all messages from the local moderation database.
    This does NOT affect Snowflake - scores are stored locally only."""
    
    count = db.query(Message).count()
    db.query(ModerationReview).delete()
    db.query(Message).delete()
    db.commit()
    
    return {
        "status": "success",
        "deleted_count": count,
        "message": "All messages cleared from local database. Scores are NOT persisted to Snowflake."
    }

# Review a message
@router.post("/moderation/review/{message_id}", response_model=ReviewResponse)
async def review_message(
    message_id: int,
    review: ReviewCreate,
    current_moderator: Moderator = Depends(get_current_moderator),
    db: Session = Depends(get_db)
):
    """Moderator reviews and makes decision on a message"""
    
    # Get message
    message = db.query(Message).filter(Message.id == message_id).first()
    if not message:
        raise HTTPException(status_code=404, detail="Message not found")
    
    # Mark as reviewed
    if review.action == "reviewed":
        message.is_reviewed = True
        message.reviewed_at = datetime.utcnow()
    else:
        raise HTTPException(status_code=400, detail="Invalid action. Use 'reviewed'.")
    
    # Create review record
    db_review = ModerationReview(
        message_id=message_id,
        moderator_id=current_moderator.id,
        action=review.action,
        reasoning=review.reasoning,
        confidence_score=1.0
    )
    
    db.add(db_review)
    db.commit()
    db.refresh(db_review)
    
    return db_review

# Get message details
@router.get("/messages/{message_id}", response_model=MessageResponse)
async def get_message(
    message_id: int,
    current_moderator: Moderator = Depends(get_current_moderator),
    db: Session = Depends(get_db)
):
    """Get detailed information about a specific message"""
    
    message = db.query(Message).filter(Message.id == message_id).first()
    if not message:
        raise HTTPException(status_code=404, detail="Message not found")
    
    return message

# Auth endpoints
class LoginRequest(BaseModel):
    username: str
    password: str

@router.post("/auth/login", response_model=Token)
async def login(login_data: LoginRequest, db: Session = Depends(get_db)):
    """Moderator login"""
    
    moderator = db.query(Moderator).filter(Moderator.username == login_data.username).first()
    if not moderator or not verify_password(login_data.password, moderator.hashed_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    if not moderator.is_active:
        raise HTTPException(status_code=400, detail="Inactive moderator")
    
    access_token = create_access_token(data={"sub": moderator.username})
    return {"access_token": access_token, "token_type": "bearer"}

# Create moderator (admin only)
@router.post("/moderators", response_model=ModeratorResponse)
async def create_moderator(
    moderator: ModeratorCreate,
    db: Session = Depends(get_db)
):
    """Create a new moderator account"""
    
    # Check if username or email already exists
    existing = db.query(Moderator).filter(
        (Moderator.username == moderator.username) | 
        (Moderator.email == moderator.email)
    ).first()
    
    if existing:
        raise HTTPException(status_code=400, detail="Username or email already exists")
    
    # Create new moderator
    hashed_password = get_password_hash(moderator.password)
    db_moderator = Moderator(
        username=moderator.username,
        email=moderator.email,
        hashed_password=hashed_password
    )
    
    db.add(db_moderator)
    db.commit()
    db.refresh(db_moderator)
    
    return db_moderator


# =========================================================================
# Snowflake Data API - Fetch Group Messages from DWH
# =========================================================================

@router.get("/snowflake/status")
async def get_snowflake_status():
    """GET: Check if Snowflake connection is available."""
    return {
        "available": snowflake_service.is_available(),
        "message": "Snowflake ready" if snowflake_service.is_available() else "Snowflake credentials not configured"
    }


@router.get("/snowflake/messages")
async def get_snowflake_messages(
    community_id: Optional[str] = None,
    interest_group_id: Optional[str] = None,
    limit: int = 100,
    days_back: int = 7,
    since_timestamp: Optional[str] = None
):
    """
    GET: Fetch group messages directly from Snowflake DWH.
    
    Use this to pull messages for moderation analysis.
    Data is near real-time (synced continuously).
    """
    if not snowflake_service.is_available():
        raise HTTPException(status_code=503, detail="Snowflake not configured")
    
    try:
        return await snowflake_service.get_group_messages(
            community_id=community_id,
            interest_group_id=interest_group_id,
            limit=limit,
            days_back=days_back,
            since_timestamp=since_timestamp
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/snowflake/groups")
async def get_snowflake_groups(
    community_id: Optional[str] = None,
    limit: int = 100
):
    """GET: Fetch interest groups from Snowflake."""
    if not snowflake_service.is_available():
        raise HTTPException(status_code=503, detail="Snowflake not configured")
    
    try:
        return await snowflake_service.get_interest_groups(
            community_id=community_id,
            limit=limit
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/snowflake/stats")
async def get_snowflake_stats(
    community_id: Optional[str] = None,
    days_back: int = 7
):
    """GET: Message statistics from Snowflake."""
    if not snowflake_service.is_available():
        raise HTTPException(status_code=503, detail="Snowflake not configured")
    
    try:
        return await snowflake_service.get_message_stats(
            community_id=community_id,
            days_back=days_back
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Background task for ingestion (runs after HTTP response)
async def run_ingestion_task(community_id: Optional[str], limit: int, days_back: int):
    """Background task that fetches from Snowflake and moderates messages."""
    import asyncio
    import time
    
    start_time = time.time()
    print(f"[BACKGROUND] Starting ingestion: limit={limit}, days_back={days_back}")
    
    try:
        # Fetch messages from Snowflake
        snowflake_start = time.time()
        messages = await snowflake_service.get_group_messages(
            community_id=community_id,
            limit=limit,
            days_back=days_back
        )
        snowflake_time = time.time() - snowflake_start
        print(f"[BACKGROUND] Fetched {len(messages)} messages from Snowflake in {snowflake_time:.1f}s")
        
        # Filter out empty/irrelevant messages
        valid_messages = []
        for msg in messages:
            text = msg.get("text", "")
            if text and text.strip() != "" and text != "📷 image":
                valid_messages.append(msg)
        
        if not valid_messages:
            print("[BACKGROUND] No valid messages to process")
            return
        
        print(f"[BACKGROUND] Processing {len(valid_messages)} valid messages through Claude")
        
        # Semaphore to limit concurrent calls to Claude
        sem = asyncio.Semaphore(3)  # Reduced to 3 to avoid overwhelming Claude API
        
        claude_start = time.time()
        processed_count = [0]  # Use list for mutable counter in closure
        
        async def process_single_message(msg, idx):
            text = msg.get("text", "")
            try:
                async with sem:
                    moderation_result = await claude_moderator.moderate_message(text)
                processed_count[0] += 1
                print(f"[CLAUDE] Processed {processed_count[0]}/{len(valid_messages)}: {text[:30]}...")
                return {"msg": msg, "moderation": moderation_result, "text": text}
            except Exception as e:
                print(f"[CLAUDE] Error on message {idx}: {e}")
                # Return dummy moderation on error
                from app.schemas import MessageModerationResult
                return {"msg": msg, "moderation": MessageModerationResult(
                    moderation_score=0.5, adversity_score=0.0, violence_score=0.0,
                    inappropriate_content_score=0.0, spam_score=0.0,
                    processed_message=text, reasoning=f"Error: {e}"
                ), "text": text}
        
        # Run all moderation tasks in parallel
        tasks = [process_single_message(msg, i) for i, msg in enumerate(valid_messages)]
        processed_results = await asyncio.gather(*tasks)
        claude_time = time.time() - claude_start
        print(f"[BACKGROUND] Claude moderation complete in {claude_time:.1f}s")
        
        print(f"[BACKGROUND] Moderation complete, saving to database")
        
        # Batch DB Write
        new_db_objects = []
        db = SessionLocal()
        try:
            for item in processed_results:
                msg = item['msg']
                moderation = item['moderation']
                text = item['text']
                
                # Check existence
                existing = db.query(Message).filter(
                    Message.group_id == msg.get("interest_group_id"),
                    Message.sender_id == msg.get("user_id"),
                    Message.original_message == text
                ).first()
                
                if existing:
                    continue
                
                # Debug: log what we got from Snowflake
                print(f"[DEBUG] Message data: group_name={msg.get('group_name')}, building_name={msg.get('building_name')}")
                
                db_message = Message(
                    original_message=text,
                    processed_message=moderation.processed_message,
                    building_id=msg.get("community_id"),
                    building_name=msg.get("building_name"),
                    client_name=msg.get("client_name"),
                    group_id=msg.get("interest_group_id"),
                    group_name=msg.get("group_name"),
                    sender_id=msg.get("user_id"),
                    message_timestamp=msg.get("created_at"),
                    moderation_score=moderation.moderation_score,
                    adversity_score=moderation.adversity_score,
                    violence_score=moderation.violence_score,
                    inappropriate_content_score=moderation.inappropriate_content_score,
                    spam_score=moderation.spam_score
                )
                new_db_objects.append(db_message)
            
            if new_db_objects:
                print(f"[BACKGROUND] Batch committing {len(new_db_objects)} messages...")
                db.add_all(new_db_objects)
                db.commit()
                print(f"[BACKGROUND] Successfully saved {len(new_db_objects)} messages")
            else:
                print("[BACKGROUND] No new messages to save (all duplicates)")
        finally:
            db.close()
        
        total_time = time.time() - start_time
        print(f"[BACKGROUND] *** INGESTION COMPLETE in {total_time:.1f}s (Snowflake: {snowflake_time:.1f}s, Claude: {claude_time:.1f}s) ***")
            
    except Exception as e:
        import traceback
        traceback.print_exc()
        print(f"[BACKGROUND] Error during ingestion: {str(e)}")


@router.post("/snowflake/ingest")
async def ingest_from_snowflake(
    community_id: Optional[str] = None,
    limit: int = 50,
    days_back: int = 1
):
    """
    POST: Trigger background ingestion from Snowflake.
    
    Returns immediately and processes messages in the background.
    Refresh the queue after ~1-2 minutes to see new messages.
    """
    import asyncio
    
    if not snowflake_service.is_available():
        raise HTTPException(status_code=503, detail="Snowflake not configured")
    
    # Use create_task for truly independent background task (won't be cancelled)
    asyncio.create_task(run_ingestion_task(community_id, limit, days_back))
    
    return {
        "status": "started",
        "message": "Ingestion started in background. Refresh queue in 1-2 minutes to see new messages.",
        "params": {"limit": limit, "days_back": days_back}
    }


@router.post("/snowflake/fetch-by-date")
async def fetch_messages_by_date(
    target_date: str,
    db: Session = Depends(get_db)
):
    """
    Fetch ALL messages from Snowflake for a specific date and store them WITHOUT scoring.
    Returns the count of messages fetched.
    """
    if not snowflake_service.is_available():
        raise HTTPException(status_code=503, detail="Snowflake not configured")
    
    print(f"[FETCH] Fetching messages for date: {target_date}")
    
    # Fetch messages from Snowflake for that date
    messages = await snowflake_service.get_messages_by_date(target_date)
    
    print(f"[FETCH] Got {len(messages)} messages from Snowflake")
    
    # Filter out empty/image messages and store without scoring
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
        
        # Check if we have cached scores for this message
        msg_hash = hashlib.md5(text.encode()).hexdigest()
        cached = db.query(ScoredMessage).filter(
            ScoredMessage.group_id == msg.get("interest_group_id"),
            ScoredMessage.sender_id == msg.get("user_id"),
            ScoredMessage.message_hash == msg_hash
        ).first()
        
        # Use cached scores if available, otherwise null (unscored)
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
    
    # Count how many were restored from cache
    restored = db.query(Message).filter(
        Message.moderation_score != None
    ).count()
    print(f"[FETCH] Saved {new_count} new messages ({restored} with cached scores)")
    
    return {
        "status": "success",
        "fetched_from_snowflake": len(messages),
        "new_messages_saved": new_count,
        "target_date": target_date
    }


@router.get("/moderation/score-stream")
async def score_stream(
    db: Session = Depends(get_db)
):
    """
    Continuous scoring stream - scores all unscored messages automatically.
    """
    from sse_starlette.sse import EventSourceResponse
    import asyncio
    import json
    
    async def event_generator():
        while True:
            # Get unscored messages
            unscored = db.query(Message).filter(
                Message.moderation_score == None
            ).limit(50).all()
            
            if not unscored:
                # No messages to score, wait and check again
                yield {
                    "event": "waiting",
                    "data": json.dumps({
                        "status": "waiting",
                        "message": "No unscored messages"
                    })
                }
                await asyncio.sleep(5)
                continue
            
            # Score messages one by one
            for i, msg in enumerate(unscored):
                try:
                    # Score the message with shorter timeout
                    result = await asyncio.wait_for(
                        claude_moderator.moderate_message(msg.original_message),
                        timeout=30.0
                    )
                    
                    # Update message in DB
                    msg.processed_message = result.processed_message
                    msg.moderation_score = result.moderation_score
                    msg.adversity_score = result.adversity_score
                    msg.violence_score = result.violence_score
                    msg.inappropriate_content_score = result.inappropriate_content_score
                    msg.spam_score = result.spam_score
                    
                    # Save to persistent cache
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
                    
                    db.commit()
                    
                    # Send scored message to client
                    yield {
                        "event": "scored",
                        "data": json.dumps({
                            "message_id": msg.id,
                            "moderation_score": msg.moderation_score,
                            "adversity_score": msg.adversity_score,
                            "violence_score": msg.violence_score,
                            "inappropriate_content_score": msg.inappropriate_content_score,
                            "spam_score": msg.spam_score,
                            "processed_message": msg.processed_message
                        })
                    }
                    
                except asyncio.TimeoutError:
                    # Set default score on timeout
                    msg.moderation_score = 0.5
                    msg.processed_message = claude_moderator.remove_pii(msg.original_message)
                    db.commit()
                    
                    yield {
                        "event": "scored",
                        "data": json.dumps({
                            "message_id": msg.id,
                            "moderation_score": 0.5,
                            "adversity_score": 0.1,
                            "violence_score": 0.1,
                            "inappropriate_content_score": 0.1,
                            "spam_score": 0.1,
                            "processed_message": msg.processed_message
                        })
                    }
                except Exception as e:
                    # Set default score on error
                    msg.moderation_score = 0.5
                    msg.processed_message = claude_moderator.remove_pii(msg.original_message)
                    db.commit()
                    
                    yield {
                        "event": "scored",
                        "data": json.dumps({
                            "message_id": msg.id,
                            "moderation_score": 0.5,
                            "adversity_score": 0.1,
                            "violence_score": 0.1,
                            "inappropriate_content_score": 0.1,
                            "spam_score": 0.1,
                            "processed_message": msg.processed_message
                        })
                    }
                
                # Small delay between requests
                await asyncio.sleep(0.5)
    
    return EventSourceResponse(event_generator())


@router.post("/moderation/score-batch")
async def score_batch(
    limit: int = 20,
    db: Session = Depends(get_db)
):
    """
    Score the next batch of unscored messages using Claude.
    Returns the number of messages scored.
    """
    import asyncio
    import time
    
    # Get unscored messages (moderation_score is NULL)
    unscored = db.query(Message).filter(
        Message.moderation_score == None
    ).limit(limit).all()
    
    if not unscored:
        return {
            "status": "complete",
            "scored": 0,
            "remaining": 0,
            "message": "No unscored messages remaining"
        }
    
    # Count total remaining
    total_unscored = db.query(Message).filter(Message.moderation_score == None).count()
    
    print(f"[SCORE] Scoring {len(unscored)} messages ({total_unscored} total unscored)")
    
    start_time = time.time()
    sem = asyncio.Semaphore(2)  # Reduced concurrency to avoid rate limits
    scored_count = [0]
    failed_count = [0]
    
    async def score_message(msg):
        try:
            async with sem:
                # Per-message timeout of 60s
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
            scored_count[0] += 1
            print(f"[SCORE] {scored_count[0]}/{len(unscored)}: {msg.original_message[:30]}...")
        except asyncio.TimeoutError:
            print(f"[SCORE] TIMEOUT: {msg.original_message[:30]}...")
            msg.moderation_score = 0.5  # Default on timeout
            failed_count[0] += 1
        except Exception as e:
            print(f"[SCORE] Error ({type(e).__name__}): {e}")
            msg.moderation_score = 0.5  # Default on error
            failed_count[0] += 1
    
    # Process sequentially to avoid rate limits (more reliable)
    print(f"[SCORE] Starting sequential processing...")
    for i, msg in enumerate(unscored):
        print(f"[SCORE] Processing message {i+1}...")
        await score_message(msg)
        await asyncio.sleep(0.3)  # Small delay between requests
    
    # Save scores to persistent cache
    for msg in unscored:
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
    
    db.commit()
    elapsed = time.time() - start_time
    remaining = total_unscored - len(unscored)
    
    print(f"[SCORE] Complete in {elapsed:.1f}s. {remaining} messages remaining. Scores cached.")
    
    return {
        "status": "success",
        "scored": len(unscored),
        "remaining": remaining,
        "elapsed_seconds": round(elapsed, 1)
    }


# Helper to get message hash
def get_message_hash(text: str) -> str:
    return hashlib.md5(text.encode()).hexdigest()


@router.delete("/moderation/remove-duplicates")
async def remove_duplicates(
    current_moderator: Moderator = Depends(get_current_moderator),
    db: Session = Depends(get_db)
):
    """Remove duplicate messages (same group_id, sender_id, original_message)."""
    from sqlalchemy import func
    
    # Find duplicates
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
    
    # Get all IDs to keep
    keep_ids = db.query(subq.c.keep_id).all()
    keep_ids = [r[0] for r in keep_ids]
    
    # Count before
    total_before = db.query(Message).count()
    
    # Delete duplicates (not in keep list)
    if keep_ids:
        db.query(Message).filter(~Message.id.in_(keep_ids)).delete(synchronize_session=False)
    
    db.commit()
    
    total_after = db.query(Message).count()
    removed = total_before - total_after
    
    return {
        "status": "success",
        "removed": removed,
        "remaining": total_after,
        "message": f"Removed {removed} duplicate messages"
    }


@router.get("/moderation/score-cache-stats")
async def get_score_cache_stats(
    current_moderator: Moderator = Depends(get_current_moderator),
    db: Session = Depends(get_db)
):
    """Get stats about the persistent score cache."""
    total_cached = db.query(ScoredMessage).count()
    return {
        "cached_scores": total_cached,
        "message": "Scores in cache will be restored when fetching messages"
    }
