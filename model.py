from os.path import abspath, dirname
from sqlalchemy import Column, String, Integer
from sqlalchemy.orm import relationship, backref, scoped_session
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

Base = declarative_base()


class Item(Base):
    __tablename__ = 'item'
    id = Column(Integer, primary_key=True)
    name = Column(String(1000))


SQLALCHEMY_DATABASE_NAME = 'db_template'
SQLALCHEMY_SERVER_URI = 'mysql+pymysql://root:root@127.0.0.1'
SQLALCHEMY_DATABASE_URI = '%s/%s?charset=utf8' % (SQLALCHEMY_SERVER_URI, SQLALCHEMY_DATABASE_NAME)

engine = create_engine(SQLALCHEMY_DATABASE_URI, echo=False)
Session = scoped_session(sessionmaker(bind=engine))
