from .base import Base, engine
from .campaigns import Campaign
from .sequences import Sequence
from .templates import Template
from .prospects import Prospect
from .sends import Send

def init_db():
    Base.metadata.create_all(bind=engine)

if __name__ == "__main__":
    init_db()