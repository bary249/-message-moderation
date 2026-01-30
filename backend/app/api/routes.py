from fastapi import APIRouter, Depends, HTTPException, status, BackgroundTasks
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from sqlalchemy.orm import Session
from typing import List, Optional
from app.database import get_db, SessionLocal
from app.models.schemas import *
from app.models.database import Message, Moderator, ModerationReview
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
    
    # Order by message_timestamp descending (newest first)
    query = query.order_by(Message.message_timestamp.desc().nullslast())
    
    # Pagination
    offset = (page - 1) * per_page
    total_count = query.count()
    messages = query.offset(offset).limit(per_page).all()
    
    return ModerationQueueResponse(
        pending_messages=messages,
        total_count=total_count,
        page=page,
        per_page=per_page
    )

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
@router.post("/auth/login", response_model=Token)
async def login(username: str, password: str, db: Session = Depends(get_db)):
    """Moderator login"""
    
    moderator = db.query(Moderator).filter(Moderator.username == username).first()
    if not moderator or not verify_password(password, moderator.hashed_password):
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
        
        # Store WITHOUT moderation scores (null scores = unscored)
        db_message = Message(
            original_message=text,
            processed_message=text,  # No PII removal yet
            building_id=msg.get("community_id"),
            building_name=msg.get("building_name"),
            client_name=msg.get("client_name"),
            group_id=msg.get("interest_group_id"),
            group_name=msg.get("group_name"),
            sender_id=msg.get("user_id"),
            message_timestamp=msg.get("created_at"),
            moderation_score=None,  # Unscored
            adversity_score=None,
            violence_score=None,
            inappropriate_content_score=None,
            spam_score=None
        )
        db.add(db_message)
        new_count += 1
    
    db.commit()
    print(f"[FETCH] Saved {new_count} new messages (unscored)")
    
    return {
        "status": "success",
        "fetched_from_snowflake": len(messages),
        "new_messages_saved": new_count,
        "target_date": target_date
    }


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
    sem = asyncio.Semaphore(3)
    scored_count = [0]
    
    async def score_message(msg):
        try:
            async with sem:
                result = await claude_moderator.moderate_message(msg.original_message)
            msg.processed_message = result.processed_message
            msg.moderation_score = result.moderation_score
            msg.adversity_score = result.adversity_score
            msg.violence_score = result.violence_score
            msg.inappropriate_content_score = result.inappropriate_content_score
            msg.spam_score = result.spam_score
            scored_count[0] += 1
            print(f"[SCORE] {scored_count[0]}/{len(unscored)}: {msg.original_message[:30]}...")
        except Exception as e:
            print(f"[SCORE] Error: {e}")
            msg.moderation_score = 0.5  # Default on error
    
    tasks = [score_message(msg) for msg in unscored]
    await asyncio.gather(*tasks)
    
    db.commit()
    elapsed = time.time() - start_time
    remaining = total_unscored - len(unscored)
    
    print(f"[SCORE] Complete in {elapsed:.1f}s. {remaining} messages remaining.")
    
    return {
        "status": "success",
        "scored": len(unscored),
        "remaining": remaining,
        "elapsed_seconds": round(elapsed, 1)
    }
