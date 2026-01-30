from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import asyncio
from app.api.routes import router
from app.core.config import settings
from app.services.snowflake_service import snowflake_service
from app.services.claude_moderator import ClaudeModerator
from app.database import SessionLocal
from app.models.database import Message

# Background task for auto-polling Snowflake
async def poll_snowflake_messages():
    """Background task: Poll Snowflake for new messages every 60 seconds."""
    # Wait for app to fully start before first sync
    await asyncio.sleep(5)
    
    claude_moderator = ClaudeModerator()
    
    while True:
        try:
            if snowflake_service.is_available():
                print("[AutoSync] Fetching new messages from Snowflake...")
                messages = await snowflake_service.get_group_messages(limit=50, days_back=1)
                
                db = SessionLocal()
                ingested = 0
                try:
                    for msg in messages:
                        text = msg.get("text", "")
                        if not text or text.strip() == "" or text == "📷 image":
                            continue
                        
                        # Check if already processed
                        existing = db.query(Message).filter(
                            Message.group_id == msg.get("interest_group_id"),
                            Message.sender_id == msg.get("user_id"),
                            Message.original_message == text
                        ).first()
                        
                        if existing:
                            continue
                        
                        # Run through moderation (scoring only, no auto-approval)
                        moderation_result = await claude_moderator.moderate_message(text)
                        
                        # Parse timestamp from Snowflake
                        msg_ts = None
                        try:
                            from datetime import datetime
                            ts_str = msg.get("created_at", "")
                            if ts_str and ts_str != "None":
                                msg_ts = datetime.fromisoformat(ts_str.replace(" ", "T"))
                        except:
                            pass
                        
                        db_message = Message(
                            original_message=text,
                            processed_message=moderation_result.processed_message,
                            building_id=msg.get("community_id"),
                            group_id=msg.get("interest_group_id"),
                            group_name=msg.get("group_name"),
                            sender_id=msg.get("user_id"),
                            message_timestamp=msg_ts,
                            moderation_score=moderation_result.moderation_score,
                            is_reviewed=False,
                            adversity_score=moderation_result.adversity_score,
                            violence_score=moderation_result.violence_score,
                            inappropriate_content_score=moderation_result.inappropriate_content_score,
                            spam_score=moderation_result.spam_score
                        )
                        
                        db.add(db_message)
                        db.commit()
                        ingested += 1
                        
                finally:
                    db.close()
                
                if ingested > 0:
                    print(f"[AutoSync] Ingested {ingested} new messages")
                else:
                    print("[AutoSync] No new messages")
            else:
                print("[AutoSync] Snowflake not configured, skipping...")
                
        except Exception as e:
            print(f"[AutoSync] Error: {e}")
        
        await asyncio.sleep(60)  # Poll every 60 seconds


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup/shutdown lifecycle manager."""
    # Startup: Start background polling task
    task = asyncio.create_task(poll_snowflake_messages())
    print("[AutoSync] Started Snowflake polling task")
    yield
    # Shutdown: Cancel task
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


app = FastAPI(
    title="Message Moderation API",
    description="API for moderating interest group messages",
    version="1.0.0",
    lifespan=lifespan
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "https://venn-message-moderation.netlify.app",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routes
app.include_router(router, prefix="/api/v1")

@app.get("/")
async def root():
    return {"message": "Message Moderation API"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
