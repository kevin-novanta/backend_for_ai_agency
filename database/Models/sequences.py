from sqlalchemy import Column, Integer, ForeignKey
from database.base import Base

class Sequence(Base):
    __tablename__ = "sequences"
    id = Column(Integer, primary_key=True, index=True)
    campaign_id = Column(Integer, ForeignKey("campaigns.id"))
    step_order = Column(Integer)
    delay_hours = Column(Integer)
    template_id = Column(Integer, ForeignKey("templates.id"))