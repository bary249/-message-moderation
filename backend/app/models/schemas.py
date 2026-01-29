from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import datetime

# Message schemas
class MessageCreate(BaseModel):
    original_message: str
    building_id: str
    group_id: str
    sender_id: str

class MessageModerationResult(BaseModel):
    moderation_score: float = Field(ge=0.0, le=1.0)
    adversity_score: float = Field(ge=0.0, le=1.0)
    violence_score: float = Field(ge=0.0, le=1.0)
    inappropriate_content_score: float = Field(ge=0.0, le=1.0)
    spam_score: float = Field(ge=0.0, le=1.0)
    processed_message: str
    reasoning: Optional[str] = None

class MessageResponse(BaseModel):
    id: int
    original_message: str
    processed_message: str
    building_id: str
    building_name: Optional[str] = None
    group_id: str
    group_name: Optional[str] = None
    sender_id: str
    message_timestamp: Optional[datetime] = None
    timestamp: datetime
    moderation_score: float
    is_reviewed: Optional[bool] = None
    adversity_score: float
    violence_score: float
    inappropriate_content_score: float
    spam_score: float
    created_at: datetime
    reviewed_at: Optional[datetime] = None

    class Config:
        from_attributes = True

# Moderator schemas
class ModeratorCreate(BaseModel):
    username: str
    email: str
    password: str

class ModeratorResponse(BaseModel):
    id: int
    username: str
    email: str
    is_active: bool
    created_at: datetime

    class Config:
        from_attributes = True

# Auth schemas
class Token(BaseModel):
    access_token: str
    token_type: str

class TokenData(BaseModel):
    username: Optional[str] = None

# Review schemas
class ReviewCreate(BaseModel):
    action: str  # "approve", "reject", "escalate"
    reasoning: Optional[str] = None
    confidence_score: float = Field(ge=0.0, le=1.0)

class ReviewResponse(BaseModel):
    id: int
    message_id: int
    moderator_id: int
    action: str
    reasoning: Optional[str]
    confidence_score: float
    created_at: datetime

    class Config:
        from_attributes = True

# API Response schemas
class MessageSubmissionResponse(BaseModel):
    message_id: int
    moderation_result: MessageModerationResult
    status: str  # "auto_approved", "pending_review", "rejected"

class ModerationQueueResponse(BaseModel):
    pending_messages: List[MessageResponse]
    total_count: int
    page: int
    per_page: int
