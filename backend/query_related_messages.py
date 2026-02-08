"""
Query messages from "Related" groups from the last 5 days.
"""
import sys
sys.path.insert(0, '/Users/barak.b/Venn/MessageModeration/backend')

from app.services.snowflake_service import query_db
from datetime import datetime, timedelta

# Calculate 5 days ago
cutoff = (datetime.utcnow() - timedelta(days=5)).strftime('%Y-%m-%d %H:%M:%S')

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
    g.NAME as GROUP_NAME,
    g.TYPE as GROUP_TYPE,
    b.NAME as BUILDING_NAME,
    o.NAME as CLIENT_NAME
FROM DWH_V2.BI.DIM_INTREST_GROUP_MESSAGES m
LEFT JOIN DWH_V2.BI.DIM_INTREST_GROUP g ON m.INTEREST_GROUP_ID = g.INTEREST_GROUP_ID
LEFT JOIN DWH_V2.BI.DIM_BUILDINGS b ON m.COMMUNITY_ID = b.COMMUNITY_ID
LEFT JOIN DWH_V2.BI.DIM_ORGANIZATION o ON m.ORGANIZATION_ID = o.ID
WHERE m.CREATED_AT >= '{cutoff}'
  AND g.NAME = 'Related'
ORDER BY m.CREATED_AT DESC
"""

print(f"Fetching messages from 'Related' groups since {cutoff}...")
df = query_db(query)

print(f"\nFound {len(df)} messages\n")
print("=" * 80)

for idx, row in df.iterrows():
    print(f"[{row['CREATED_AT']}] {row['BUILDING_NAME']} / {row['CLIENT_NAME']}")
    print(f"Text: {row['TEXT'][:200] if row['TEXT'] else '(empty)'}...")
    print("-" * 40)

# Also save to CSV for easy review
output_file = '/Users/barak.b/Venn/MessageModeration/backend/related_messages_last_5_days.csv'
df.to_csv(output_file, index=False)
print(f"\nSaved to {output_file}")
