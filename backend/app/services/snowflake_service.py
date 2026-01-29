"""
Snowflake Service - READ-ONLY access to Venn's Snowflake DWH for group messages.
Only SELECT queries allowed.
"""
import os
import re
import pandas as pd
import snowflake.connector
from dotenv import load_dotenv
from typing import Optional, List
from datetime import datetime, timedelta

load_dotenv()

# Blocked SQL keywords that modify data
WRITE_KEYWORDS = [
    'INSERT', 'UPDATE', 'DELETE', 'DROP', 'CREATE', 'ALTER', 
    'TRUNCATE', 'REPLACE', 'MERGE', 'UPSERT', 'COPY', 'PUT',
    'GRANT', 'REVOKE', 'EXECUTE', 'CALL'
]


def _validate_read_only(query: str) -> None:
    """Validate that query is read-only. Raises ValueError if write operation detected."""
    normalized = query.strip().upper()
    for keyword in WRITE_KEYWORDS:
        if re.match(rf'^\s*{keyword}\b', normalized):
            raise ValueError(f"Write operations not allowed. Blocked keyword: {keyword}")


def get_connection() -> snowflake.connector.SnowflakeConnection:
    """Get a Snowflake connection."""
    return snowflake.connector.connect(
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        role=os.getenv('SNOWFLAKE_ROLE'),
    )


def query_db(query: str) -> pd.DataFrame:
    """Execute a read-only query on Snowflake."""
    _validate_read_only(query)
    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            return cursor.fetch_pandas_all()


class SnowflakeService:
    """Service for fetching group messages from Snowflake DWH."""
    
    def is_available(self) -> bool:
        """Check if Snowflake credentials are configured."""
        return all([
            os.getenv('SNOWFLAKE_USER'),
            os.getenv('SNOWFLAKE_PASSWORD'),
            os.getenv('SNOWFLAKE_ACCOUNT'),
        ])
    
    async def get_group_messages(
        self,
        community_id: Optional[str] = None,
        interest_group_id: Optional[str] = None,
        limit: int = 100,
        days_back: int = 7,
        since_timestamp: Optional[str] = None
    ) -> List[dict]:
        """
        GET: Fetch group chat messages from Snowflake for moderation.
        
        Args:
            community_id: Filter by community
            interest_group_id: Filter by specific group
            limit: Max messages to return
            days_back: How many days back to fetch
            since_timestamp: Only fetch messages after this timestamp (for incremental sync)
        """
        if since_timestamp:
            cutoff = since_timestamp
        else:
            cutoff = (datetime.utcnow() - timedelta(days=days_back)).strftime('%Y-%m-%d %H:%M:%S')
        
        where_clauses = [f"m.CREATED_AT >= '{cutoff}'"]
        
        if community_id:
            where_clauses.append(f"m.COMMUNITY_ID = '{community_id}'")
        if interest_group_id:
            where_clauses.append(f"m.INTEREST_GROUP_ID = '{interest_group_id}'")
        
        where_sql = " AND ".join(where_clauses)
        
        query = f"""
        SELECT 
            m.ID,
            m.MESSAGE_ID,
            m.TEXT,
            m.USER_ID,
            m.INTEREST_GROUP_ID,
            m.COMMUNITY_ID,
            m.ORGANIZATION_ID,
            m.CREATED_AT,
            m.UPDATED_AT,
            g.NAME as GROUP_NAME,
            g.TYPE as GROUP_TYPE,
            g.IS_PUBLIC as GROUP_IS_PUBLIC
        FROM DWH_V2.BI.DIM_INTREST_GROUP_MESSAGES m
        LEFT JOIN DWH_V2.BI.DIM_INTREST_GROUP g ON m.INTEREST_GROUP_ID = g.INTEREST_GROUP_ID
        WHERE {where_sql}
        ORDER BY m.CREATED_AT DESC
        LIMIT {limit}
        """
        
        df = query_db(query)
        
        messages = []
        for _, row in df.iterrows():
            messages.append({
                "id": row.get("ID"),
                "message_id": row.get("MESSAGE_ID"),
                "text": row.get("TEXT"),
                "user_id": row.get("USER_ID"),
                "interest_group_id": row.get("INTEREST_GROUP_ID"),
                "community_id": row.get("COMMUNITY_ID"),
                "organization_id": row.get("ORGANIZATION_ID"),
                "created_at": str(row.get("CREATED_AT")),
                "updated_at": str(row.get("UPDATED_AT")),
                "group_name": row.get("GROUP_NAME"),
                "group_type": row.get("GROUP_TYPE"),
                "group_is_public": row.get("GROUP_IS_PUBLIC"),
            })
        
        return messages
    
    async def get_interest_groups(
        self,
        community_id: Optional[str] = None,
        limit: int = 100
    ) -> List[dict]:
        """GET: Fetch interest groups."""
        where_sql = f"WHERE COMMUNITY_ID = '{community_id}'" if community_id else ""
        
        query = f"""
        SELECT 
            INTEREST_GROUP_ID,
            NAME,
            DESCRIPTION,
            TYPE,
            IS_PUBLIC,
            COMMUNITY_ID,
            ORGANIZATION_ID,
            CREATED_AT
        FROM DWH_V2.BI.DIM_INTREST_GROUP
        {where_sql}
        ORDER BY CREATED_AT DESC
        LIMIT {limit}
        """
        
        df = query_db(query)
        
        groups = []
        for _, row in df.iterrows():
            groups.append({
                "id": row.get("INTEREST_GROUP_ID"),
                "name": row.get("NAME"),
                "description": row.get("DESCRIPTION"),
                "type": row.get("TYPE"),
                "is_public": row.get("IS_PUBLIC"),
                "community_id": row.get("COMMUNITY_ID"),
                "organization_id": row.get("ORGANIZATION_ID"),
                "created_at": str(row.get("CREATED_AT")),
            })
        
        return groups
    
    async def get_message_stats(
        self,
        community_id: Optional[str] = None,
        days_back: int = 7
    ) -> dict:
        """GET: Message statistics."""
        cutoff_date = (datetime.utcnow() - timedelta(days=days_back)).strftime('%Y-%m-%d')
        
        where_sql = f"AND COMMUNITY_ID = '{community_id}'" if community_id else ""
        
        query = f"""
        SELECT 
            COUNT(*) as total_messages,
            COUNT(DISTINCT USER_ID) as unique_users,
            COUNT(DISTINCT INTEREST_GROUP_ID) as active_groups,
            MAX(CREATED_AT) as latest_message
        FROM DWH_V2.BI.DIM_INTREST_GROUP_MESSAGES
        WHERE CREATED_AT >= '{cutoff_date}' {where_sql}
        """
        
        df = query_db(query)
        
        if len(df) > 0:
            row = df.iloc[0]
            return {
                "total_messages": int(row.get("TOTAL_MESSAGES", 0)),
                "unique_users": int(row.get("UNIQUE_USERS", 0)),
                "active_groups": int(row.get("ACTIVE_GROUPS", 0)),
                "latest_message": str(row.get("LATEST_MESSAGE")),
                "period_days": days_back
            }
        
        return {"total_messages": 0, "unique_users": 0, "active_groups": 0, "latest_message": None, "period_days": days_back}


# Singleton instance
snowflake_service = SnowflakeService()
