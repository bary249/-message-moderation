#!/usr/bin/env python3
"""
Get the latest group message from Snowflake DWH.
Usage: python get_latest_message.py
"""
import sys
sys.path.insert(0, '.')

from app.services.snowflake_service import snowflake_service
import asyncio


async def get_latest_message():
    """Fetch and display the latest group message."""
    
    if not snowflake_service.is_available():
        print("❌ Snowflake not configured. Check .env file.")
        return
    
    messages = await snowflake_service.get_group_messages(limit=1, days_back=1)
    
    if not messages:
        print("No messages found in the last 24 hours.")
        return
    
    msg = messages[0]
    
    print("=" * 60)
    print("LATEST GROUP MESSAGE")
    print("=" * 60)
    print(f"Message ID:      {msg.get('message_id')}")
    print(f"Group Name:      {msg.get('group_name')}")
    print(f"Group ID:        {msg.get('interest_group_id')}")
    print(f"Group Type:      {msg.get('group_type')}")
    print(f"Community ID:    {msg.get('community_id')}")
    print(f"User ID:         {msg.get('user_id')}")
    print(f"Created At:      {msg.get('created_at')} (UTC)")
    print(f"Updated At:      {msg.get('updated_at')}")
    print("-" * 60)
    print(f"Text:\n{msg.get('text')}")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(get_latest_message())
