from sqlalchemy import Column, Integer, String, DateTime, Float, Text, Boolean, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from datetime import datetime

Base = declarative_base()

class Moderator(Base):
    __tablename__ = "moderators"
    
    id = Column(Integer, primary_key=True, index=True)
    username = Column(String, unique=True, index=True)
    email = Column(String, unique=True, index=True)
    hashed_password = Column(String)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    reviews = relationship("ModerationReview", back_populates="moderator")

class Message(Base):
    __tablename__ = "messages"
    
    id = Column(Integer, primary_key=True, index=True)
    original_message = Column(Text)
    processed_message = Column(Text)  # PII-free version
    building_id = Column(String, index=True)  # community_id from Snowflake
    building_name = Column(String, nullable=True)  # Looked up from building DB
    client_name = Column(String, nullable=True)  # Organization/client name from Snowflake
    group_id = Column(String, index=True)
    group_name = Column(String, nullable=True)  # From Snowflake DIM_INTREST_GROUP
    sender_id = Column(String, index=True)  # Non-PII identifier
    message_timestamp = Column(DateTime, nullable=True, index=True)  # Original timestamp from Snowflake
    timestamp = Column(DateTime, default=datetime.utcnow, index=True)
    
    # Moderation results
    moderation_score = Column(Float)  # 0.0 (clean) to 1.0 (severe)
    is_reviewed = Column(Boolean, default=False)  # True = reviewed by moderator
    
    # AI analysis
    adversity_score = Column(Float)
    violence_score = Column(Float)
    inappropriate_content_score = Column(Float)
    spam_score = Column(Float)
    
    created_at = Column(DateTime, default=datetime.utcnow)
    reviewed_at = Column(DateTime, nullable=True)
    
    # Relationships
    reviews = relationship("ModerationReview", back_populates="message")

class ModerationReview(Base):
    __tablename__ = "moderation_reviews"
    
    id = Column(Integer, primary_key=True, index=True)
    message_id = Column(Integer, ForeignKey("messages.id"))
    moderator_id = Column(Integer, ForeignKey("moderators.id"))
    
    action = Column(String)  # "approve", "reject", "escalate"
    reasoning = Column(Text)
    confidence_score = Column(Float)
    
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    message = relationship("Message", back_populates="reviews")
    moderator = relationship("Moderator", back_populates="reviews")
