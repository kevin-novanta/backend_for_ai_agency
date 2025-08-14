from sqlalchemy import Column, Integer, Text, String
from database.base import Base

class Template(Base):
    __tablename__ = "templates"
    id = Column(Integer, primary_key=True, index=True)
    subject = Column(String)
    body_html = Column(Text)
    body_plain = Column(Text)
    spintax = Column(Text)  # Optional