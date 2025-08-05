from sqlalchemy import Column, Integer, ForeignKey, DateTime, String
from datetime import datetime
from database.base import Base

class Send(Base):
    __tablename__ = "sends"
    id = Column(Integer, primary_key=True, index=True)
    prospect_id = Column(Integer, ForeignKey("prospects.id"))
    sequence_step_id = Column(Integer, ForeignKey("sequences.id"))
    sent_at = Column(DateTime, default=datetime.utcnow)
    status = Column(String)  # "success", "failed", etc.