# Message Moderation Backend Deployment

## Quick Deploy Options

### Option 1: Railway.app (Recommended - Free Tier)

1. Go to https://railway.app
2. Click "New Project" → "Deploy from GitHub repo" or "Empty Project"
3. If empty project: Click "Add Service" → "GitHub Repo" or drag the backend folder
4. Add environment variables:
   - `ANTHROPIC_API_KEY` - Your Claude API key
   - `SNOWFLAKE_USER` - Snowflake username
   - `SNOWFLAKE_PASSWORD` - Snowflake password
   - `SNOWFLAKE_ACCOUNT` - Snowflake account (e.g., TXGRDSC-SE24846)
   - `SNOWFLAKE_WAREHOUSE` - DWH_V2
   - `SNOWFLAKE_ROLE` - ACCOUNTADMIN
   - `SECRET_KEY` - Generate a random string for JWT

5. Deploy! Railway will use `railway.json` config automatically.

### Option 2: Render.com (Free Tier)

1. Go to https://render.com
2. New → Web Service
3. Connect your repo or use "Deploy from a directory"
4. Set environment variables (same as above)
5. Deploy!

### Option 3: Docker (Any Cloud Provider)

```bash
# Build
docker build -t message-moderation-backend .

# Run
docker run -p 8000:8000 \
  -e ANTHROPIC_API_KEY=your_key \
  -e SNOWFLAKE_USER=your_user \
  -e SNOWFLAKE_PASSWORD=your_pass \
  -e SNOWFLAKE_ACCOUNT=your_account \
  -e SNOWFLAKE_WAREHOUSE=DWH_V2 \
  -e SNOWFLAKE_ROLE=ACCOUNTADMIN \
  -e SECRET_KEY=your_secret \
  message-moderation-backend
```

## After Backend is Deployed

Once you have your backend URL (e.g., `https://your-app.railway.app`), update the frontend:

1. Create `/frontend/.env.production`:
```
REACT_APP_API_URL=https://your-backend-url.railway.app
```

2. Update `/frontend/src/services/api.ts` to use the env var.

3. Rebuild and redeploy frontend.

## Environment Variables Reference

| Variable | Required | Description |
|----------|----------|-------------|
| `ANTHROPIC_API_KEY` | Yes | Claude API key for moderation |
| `SNOWFLAKE_USER` | Yes | Snowflake username |
| `SNOWFLAKE_PASSWORD` | Yes | Snowflake password |
| `SNOWFLAKE_ACCOUNT` | Yes | Snowflake account ID |
| `SNOWFLAKE_WAREHOUSE` | Yes | Snowflake warehouse (DWH_V2) |
| `SNOWFLAKE_ROLE` | Yes | Snowflake role |
| `SECRET_KEY` | Yes | JWT signing secret |
| `DATABASE_URL` | No | Database URL (default: SQLite) |
