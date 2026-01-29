import anthropic
import re
from typing import Dict, Optional
from app.core.config import settings
from app.models.schemas import MessageModerationResult

class ClaudeModerator:
    def __init__(self):
        api_key = settings.anthropic_api_key
        if api_key:
            self.client = anthropic.Anthropic(api_key=api_key)
        else:
            self.client = None
            print("[ClaudeModerator] Warning: ANTHROPIC_API_KEY not set, moderation will use dummy scores")
    
    def remove_pii(self, message: str) -> str:
        """Remove personally identifiable information from message"""
        # Remove email addresses
        message = re.sub(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', '[EMAIL]', message)
        
        # Remove phone numbers
        message = re.sub(r'\b\d{3}[-.]?\d{3}[-.]?\d{4}\b', '[PHONE]', message)
        
        # Remove addresses (basic pattern)
        message = re.sub(r'\d+\s+([A-Z][a-z]*\s*)+\b(st|street|ave|avenue|rd|road|dr|drive|ln|lane|blvd|boulevard)\b', '[ADDRESS]', message, flags=re.IGNORECASE)
        
        # Remove full names (basic pattern - capitalized words that look like names)
        message = re.sub(r'\b([A-Z][a-z]+\s)+[A-Z][a-z]+\b', '[NAME]', message)
        
        return message.strip()
    
    async def moderate_message(self, original_message: str) -> MessageModerationResult:
        """Analyze message for content moderation"""
        # Remove PII first
        processed_message = self.remove_pii(original_message)
        
        # If no API key, return dummy scores
        if not self.client:
            return MessageModerationResult(
                moderation_score=0.5,
                adversity_score=0.0,
                violence_score=0.0,
                inappropriate_content_score=0.0,
                spam_score=0.0,
                processed_message=processed_message,
                reasoning="No ANTHROPIC_API_KEY configured"
            )
        
        # Claude prompt for moderation
        prompt = f"""
        Analyze the following message for content moderation. Score each category from 0.0 (clean) to 1.0 (severe violation).

        Message: "{processed_message}"

        Provide scores for:
        1. adversity_score: Hostility, aggression, personal attacks
        2. violence_score: Threats, incitement to violence, graphic descriptions
        3. inappropriate_content_score: Adult content, hate speech, discrimination
        4. spam_score: Commercial spam, repetitive content, low-quality posts

        Also provide an overall moderation_score (highest of individual scores) and brief reasoning.

        Respond in JSON format:
        {{
            "adversity_score": 0.0,
            "violence_score": 0.0,
            "inappropriate_content_score": 0.0,
            "spam_score": 0.0,
            "moderation_score": 0.0,
            "reasoning": "Brief explanation of scores"
        }}
        """
        
        try:
            response = self.client.messages.create(
                model="claude-sonnet-4-20250514",
                max_tokens=1000,
                messages=[{"role": "user", "content": prompt}]
            )
            
            # Parse Claude's response
            content = response.content[0].text
            
            # Extract JSON from response (handle markdown code blocks)
            import json
            import re
            
            # Try to extract JSON from markdown code block or raw JSON
            json_match = re.search(r'```(?:json)?\s*(\{.*?\})\s*```', content, re.DOTALL)
            if json_match:
                json_str = json_match.group(1)
            else:
                # Try to find raw JSON object
                json_match = re.search(r'\{[^{}]*"moderation_score"[^{}]*\}', content, re.DOTALL)
                if json_match:
                    json_str = json_match.group(0)
                else:
                    json_str = content
            
            try:
                scores_data = json.loads(json_str)
            except json.JSONDecodeError:
                # Fallback if JSON parsing fails
                scores_data = {
                    "adversity_score": 0.1,
                    "violence_score": 0.1,
                    "inappropriate_content_score": 0.1,
                    "spam_score": 0.1,
                    "moderation_score": 0.1,
                    "reasoning": f"Failed to parse: {content[:100]}"
                }
            
            return MessageModerationResult(
                moderation_score=scores_data.get("moderation_score", 0.1),
                adversity_score=scores_data.get("adversity_score", 0.0),
                violence_score=scores_data.get("violence_score", 0.0),
                inappropriate_content_score=scores_data.get("inappropriate_content_score", 0.0),
                spam_score=scores_data.get("spam_score", 0.0),
                processed_message=processed_message,
                reasoning=scores_data.get("reasoning", "")
            )
            
        except Exception as e:
            # Fallback in case of API failure
            return MessageModerationResult(
                moderation_score=0.5,  # Medium risk on failure
                adversity_score=0.1,
                violence_score=0.1,
                inappropriate_content_score=0.1,
                spam_score=0.1,
                processed_message=processed_message,
                reasoning=f"AI moderation failed: {str(e)}"
            )
    
    def should_auto_approve(self, moderation_result: MessageModerationResult) -> bool:
        """Determine if message should be auto-approved based on scores"""
        # Auto-approve if all scores are below threshold
        threshold = 0.3  # 30% threshold for auto-approval
        
        return (
            moderation_result.adversity_score < threshold and
            moderation_result.violence_score < threshold and
            moderation_result.inappropriate_content_score < threshold and
            moderation_result.spam_score < threshold
        )
    
    def should_auto_reject(self, moderation_result: MessageModerationResult) -> bool:
        """Determine if message should be auto-rejected based on scores"""
        # Auto-reject if any score is above severe threshold
        severe_threshold = 0.8  # 80% threshold for auto-rejection
        
        return (
            moderation_result.adversity_score >= severe_threshold or
            moderation_result.violence_score >= severe_threshold or
            moderation_result.inappropriate_content_score >= severe_threshold or
            moderation_result.spam_score >= severe_threshold
        )
