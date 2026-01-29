from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from sqlalchemy.orm import Session
from typing import List, Optional
from app.database import get_db
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
        is_approved=is_approved,
        auto_approved=auto_approved,
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
    
    # Filter by status (or 'all' to get everything)
    if status == "pending":
        query = query.filter(Message.is_approved.is_(None))
    elif status == "approved":
        query = query.filter(Message.is_approved == True)
    elif status == "rejected":
        query = query.filter(Message.is_approved == False)
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


@router.post("/snowflake/ingest")
async def ingest_from_snowflake(
    community_id: Optional[str] = None,
    limit: int = 50,
    days_back: int = 1,
    db: Session = Depends(get_db)
):
    """
    POST: Pull messages from Snowflake and run them through moderation.
    
    Fetches recent messages from Snowflake DWH and processes each through
    the AI moderation pipeline, storing results in the local database.
    """
    if not snowflake_service.is_available():
        raise HTTPException(status_code=503, detail="Snowflake not configured")
    
    try:
        # Fetch messages from Snowflake
        messages = await snowflake_service.get_group_messages(
            community_id=community_id,
            limit=limit,
            days_back=days_back
        )
        
        results = []
        for msg in messages:
            # Skip empty messages or images
            text = msg.get("text", "")
            if not text or text.strip() == "" or text == "📷 image":
                continue
            
            # Check if already processed (by message_id)
            existing = db.query(Message).filter(
                Message.group_id == msg.get("interest_group_id"),
                Message.sender_id == msg.get("user_id"),
                Message.original_message == text
            ).first()
            
            if existing:
                continue
            
            # Run through moderation
            moderation_result = await claude_moderator.moderate_message(text)
            
            # Determine auto-approval/rejection
            auto_approved = claude_moderator.should_auto_approve(moderation_result)
            auto_rejected = claude_moderator.should_auto_reject(moderation_result)
            
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
                original_message=text,
                processed_message=moderation_result.processed_message,
                building_id=msg.get("community_id"),  # Using community as building
                group_id=msg.get("interest_group_id"),
                sender_id=msg.get("user_id"),
                moderation_score=moderation_result.moderation_score,
                is_approved=is_approved,
                auto_approved=auto_approved,
                adversity_score=moderation_result.adversity_score,
                violence_score=moderation_result.violence_score,
                inappropriate_content_score=moderation_result.inappropriate_content_score,
                spam_score=moderation_result.spam_score
            )
            
            db.add(db_message)
            db.commit()
            db.refresh(db_message)
            
            results.append({
                "message_id": db_message.id,
                "status": status_text,
                "moderation_score": moderation_result.moderation_score,
                "group_name": msg.get("group_name"),
                "text_preview": text[:50] + "..." if len(text) > 50 else text
            })
        
        return {
            "ingested_count": len(results),
            "total_fetched": len(messages),
            "results": results
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
