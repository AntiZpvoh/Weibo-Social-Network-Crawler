from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, UniqueConstraint, Index
from sqlalchemy import create_engine
import sys
import datetime

Base = declarative_base()
engine = create_engine("mysql+mysqlconnector://root:82870808Qyy@localhost:3306/weibo?charset=utf8")
Session = sessionmaker(bind=engine)
session = Session()
FOLLOWER = 0
FOLLOWEE = 1

class User(Base):
    __tablename__ = "weibo_user"

    id = Column(String(64), primary_key=True)
    name =  Column(String(64), unique=True)
    gender = Column(Integer)
    location = Column(String(64))
    create_time = Column(DateTime, default = datetime.datetime.now())

    def __init__(self, id, name, location):
        self.id = id
        self.name = name
        self.location = location
        self.create_time = datetime.datetime.now()

class UserRelationship(Base):
    __tablename__ = "weibo_user_relationship"

    rid = Column(String(64), primary_key=True)
    followee_id = Column(String(64))
    follower_id = Column(String(64))
    create_time = Column(DateTime, default = datetime.datetime.now())

    def __init__(self,followee_id, follower_id):
        self.rid = "{}_{}".format(followee_id, follower_id)
        self.followee_id = followee_id
        self.follower_id = follower_id
        self.create_time = datetime.datetime.now()
        
class InfoQueue(Base):
    __tablename__ = "info_queue"
    
    url = Column(String(64), primary_key=True)
    follow_or_fan = Column(Integer, primary_key=True)
    source_uid = Column(String(64), primary_key=True)
    create_time = Column(DateTime, default = datetime.datetime.now())
    
    def __init__(self, url, follow_or_fan, source_uid):
        self.url = url
        self.follow_or_fan = follow_or_fan
        self.source_uid = source_uid
        self.create_time = datetime.datetime.now()

class FollowQueue(Base):
    __tablename__ = "follow_queue"

    uid = Column(String(64), primary_key=True)
    create_time = Column(DateTime, default = datetime.datetime.now())
    
    def __init__(self, uid):
        self.uid = uid
        self.create_time = datetime.datetime.now()


def init_db():
    Base.metadata.create_all(engine)

def drop_db():
    Base.metadata.drop_all(engine)

def restart_session():
    global session, Session
    session.commit()
    session.close()
    session = Session()

if __name__=="__main__":
    choice = sys.argv[1]
    if choice=='init':
        init_db()
    elif choice=='drop':
        drop_db()