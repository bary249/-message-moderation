"""Initialize database and create demo moderator"""
import sys
import time
sys.path.insert(0, '.')

def init_database():
    """Initialize DB with retry logic for Railway's internal networking."""
    max_retries = 5
    retry_delay = 2
    
    for attempt in range(max_retries):
        try:
            # Import here to avoid connection at module load
            from app.database import engine, SessionLocal
            from app.models.database import Base, Moderator
            from app.core.security import get_password_hash
            
            # Create all tables
            print("Creating database tables...")
            Base.metadata.create_all(bind=engine)
            print("Tables created successfully!")
            
            # Create demo moderator
            db = SessionLocal()
            try:
                existing = db.query(Moderator).filter(Moderator.username == "admin").first()
                if not existing:
                    print("Creating demo moderator (admin/admin123)...")
                    moderator = Moderator(
                        username="admin",
                        email="admin@example.com",
                        hashed_password=get_password_hash("admin123"),
                        is_active=True
                    )
                    db.add(moderator)
                    db.commit()
                    print("Demo moderator created!")
                else:
                    print("Demo moderator already exists.")
            finally:
                db.close()
            
            print("\nDatabase initialized successfully!")
            print("Login credentials: admin / admin123")
            return True
            
        except Exception as e:
            print(f"DB connection attempt {attempt + 1}/{max_retries} failed: {e}")
            if attempt < max_retries - 1:
                print(f"Retrying in {retry_delay}s...")
                time.sleep(retry_delay)
                retry_delay *= 2  # Exponential backoff
            else:
                print("Max retries reached. DB init will happen on first request.")
                return False

if __name__ == "__main__":
    init_database()
