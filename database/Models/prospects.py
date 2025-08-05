from sqlalchemy import Column, Integer, String
from database.base import Base

class Prospect(Base):
    __tablename__ = "prospects"
    id = Column(Integer, primary_key=True, index=True)
    email = Column(String, index=True, unique=True)
    first_name = Column(String)
    company = Column(String)
    status = Column(String)  # e.g., "new", "contacted", "replied", "bounced"
    last_step_sent = Column(Integer)  # step order