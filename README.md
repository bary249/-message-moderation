# Message Moderation System

A comprehensive message moderation system for interest group communications with AI-powered content analysis and moderator review interface.

## Architecture

### Components
1. **Message Intake API** - Receives messages from interest groups
2. **AI Moderation Service** - Processes messages through Claude for content scoring (PII-free)
3. **Moderation Dashboard** - Web interface for moderator review with authentication
4. **Database Layer** - Stores messages, metadata, and moderation results

### Key Features
- **PII Removal**: Automatically strips emails, phone numbers, addresses, names before AI analysis
- **Hard Moderation Scoring**: Adversity, violence, inappropriate content, spam detection
- **Auto-moderation**: Messages below 30% risk auto-approved, above 80% auto-rejected
- **Building/Group Tracking**: Full metadata with timestamps
- **User Authentication**: JWT-based auth for moderators
- **Review Workflow**: Approve, reject, or escalate messages

## Tech Stack
- **Backend**: Python/FastAPI
- **Frontend**: React/TypeScript with Material-UI
- **Database**: SQLite (dev) / PostgreSQL (prod)
- **AI**: Claude API for content moderation
- **Auth**: JWT-based authentication

## Quick Start

### 1. Backend Setup
```bash
cd backend
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt

# Create .env file
cp .env.example .env
# Edit .env and add your ANTHROPIC_API_KEY

# Initialize database and create demo user
python init_db.py

# Run backend
uvicorn app.main:app --reload --port 8000
```

### 2. Frontend Setup
```bash
cd frontend
npm install
npm start
```

### 3. Login
- URL: http://localhost:3000
- Username: `admin`
- Password: `admin123`

## API Endpoints

### Message Intake (Public)
```
POST /api/v1/messages
{
  "original_message": "Hello everyone!",
  "building_id": "building-123",
  "group_id": "interest-group-456",
  "sender_id": "user-789"
}
```

### Moderation Queue (Authenticated)
```
GET /api/v1/moderation/queue?status=pending&page=1
```

### Review Message (Authenticated)
```
POST /api/v1/moderation/review/{message_id}
{
  "action": "approve",  // or "reject", "escalate"
  "reasoning": "Clean message"
}
```

## Moderation Scores

Each message receives scores from 0.0 (clean) to 1.0 (severe):
- **adversity_score**: Hostility, aggression, personal attacks
- **violence_score**: Threats, incitement to violence
- **inappropriate_content_score**: Adult content, hate speech, discrimination
- **spam_score**: Commercial spam, repetitive content
- **moderation_score**: Overall risk (max of individual scores)

## Data Flow
1. Message received from interest group API
2. PII automatically removed/masked
3. Cleaned message sent to Claude for moderation scoring
4. Results stored with original metadata (building, group, timestamp)
5. Moderators review flagged messages via web dashboard
6. Approved messages can proceed, rejected messages are blocked
