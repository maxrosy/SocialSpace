from sqlalchemy import *
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
import configparser
from SocialAPI.Helper import Helper


rootPath = Helper().getRootPath()
cfp = configparser.ConfigParser()
cfp.read(rootPath + '/conf/social.conf')
username = cfp.get('db','user')
password = cfp.get('db','password')
host = cfp.get('db','host')
port = cfp.get('db','port')
db = cfp.get('db','db')

dblink = "mysql+mysqldb://{}:{}@{}:{}/{}?charset=utf8mb4".format(username,password,host,port,db)

Base = declarative_base()
engine = create_engine(dblink,echo=False)
metadata = MetaData(bind=engine)

"""
class User(Base):
    __table__ = Table('weibo_user_info',metadata,autoload=True)
    post = relationship('PostStatus',backref='user')
    user_growth = relationship('UserGrowth',backref='user')
    user_tag = relationship('UserTag', backref='user')
    attitude = relationship('Attitude', backref='user')


class PostStatus(Base):
    __table__ = Table('weibo_user_post',metadata,autoload=True)
    comment = relationship('Comment',backref='post')
    media = relationship('Media', backref='post')
    postCrawl = relationship('PostCrawl',backref='post')
    attitude = relationship('Attitude',backref='post')

class UserGrowth(Base):
    __table__ = Table('weibo_user_growth_daily',metadata,autoload=True)

class Comment(Base):
    __table__ = Table('weibo_comment',metadata,autoload=True)

class Media(Base):
    __table__ = Table('weibo_media',metadata,autoload=True)

class UserTag(Base):
    __table__ = Table('weibo_user_tag',metadata,autoload=True)

class TaskHistory(Base):
    __table__ = Table('task_history',metadata,autoload=True)

class PostCrawl(Base):
    __table__ = Table('weibo_crawl_post',metadata,autoload=True)

class Attitude(Base):
    __table__ = Table('weibo_post_attitude',metadata,autoload=True)
"""
class Kol(Base):
    __table__ = Table('weibo_kol',metadata,autoload=True)