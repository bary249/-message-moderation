"""Initialize database and create demo moderator"""
import sys
sys.path.insert(0, '.')

from app.database import engine, SessionLocal
from app.models.database import Base, Moderator
from app.core.security import get_password_hash

def init_database():
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

if __name__ == "__main__":
    init_database()
