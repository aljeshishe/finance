import enum
from os.path import abspath, dirname
from sqlalchemy import Column, String, Integer, Enum, DateTime
from sqlalchemy.orm import relationship, backref, scoped_session
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

Base = declarative_base()

class Recomendation(enum.Enum):
    SELL = enum.auto()
    HOLD = enum.auto()
    BUY = enum.auto()


class Evaluation(enum.Enum):
    OVERVALUED = enum.auto()
    UNDERVALUED = enum.auto()
    NEAR_FAIR_VALUE = enum.auto()


class Pattern(enum.Enum):
    Bearish = enum.auto()
    Bullish = enum.auto()
    Neutral = enum.auto()


class Item(Base):
    __tablename__ = 'item'
    id = Column(Integer, primary_key=True)
    ticker = Column(String(6))
    insert_date_time = Column(DateTime())
    eval = Column(Enum(Evaluation))
    recmd_short = Column(Enum(Recomendation))
    recmd_mid = Column(Enum(Recomendation))
    recmd_long = Column(Enum(Recomendation))
    pattern = Column(Enum(Pattern))
    pattern_type = Column(String(50))
    est_return = Column(Integer)

    def __repr__(self):
        return f'Item<{self.ticker} insert_date_time:{self.insert_date_time} \
eval:{self.eval} recmd_short:{self.recmd_short} \
recmd_mid:{self.recmd_mid} recmd_long:{self.recmd_long} \
pattern:{self.pattern} pattern_type:{self.pattern_type} est_return:{self.est_return}>'

SQLALCHEMY_DATABASE_NAME = 'finance'
SQLALCHEMY_SERVER_URI = 'mysql+pymysql://root:root@127.0.0.1'
SQLALCHEMY_DATABASE_URI = '%s/%s?charset=UTF8MB4' % (SQLALCHEMY_SERVER_URI, SQLALCHEMY_DATABASE_NAME)

engine = create_engine(SQLALCHEMY_DATABASE_URI, echo=False)
Session = scoped_session(sessionmaker(bind=engine))
