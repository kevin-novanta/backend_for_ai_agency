from sqlalchemy import Column, Integer, String, Enum, Time
from database.base import Base

class Campaign(Base):
    __tablename__ = "campaigns"
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, nullable=False)
    status = Column(Enum("draft", "active", "paused", "completed", name="campaign_status"), default="draft")
    send_window = Column(String)  # e.g. "09:00-17:00"
    sending_email_id = Column(String, nullable=False)  # email account used