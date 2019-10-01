from sqlalchemy import *
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
import configparser
from SocialAPI.Helper import Helper


rootPath = Helper().getRootPath()
cfp = configparser.ConfigParser()
cfp.read(rootPath + '/conf/social.conf')
username = cfp.get('mysql','user')
password = cfp.get('mysql','password')
host = cfp.get('mysql','host')
port = cfp.get('mysql','port')
db = cfp.get('mysql','db')

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

class MasterUid(Base):
    __table__ = Table('master_uid',metadata,autoload=True)

class WeixinAccount(Base):
    __table__ = Table('weixin_account',metadata,autoload=True)

class IdataAccount(Base):
    __table__ = Table('idata_account',metadata,autoload=True)

class MasterWeiboSearch(Base):
    __table__ = Table('master_weibo_search',metadata,autoload=True)

class WeiboUserAttitude(Base):
    __table__ = Table('weibo_user_attitude',metadata,autoload=True)

class WeiboUserComment(Base):
    __table__ = Table('weibo_user_comment',metadata,autoload=True)

class WeiboUserRepost(Base):
    __table__ = Table('weibo_user_repost',metadata,autoload=True)

class WeiboLastMentionedPost(Base):
    __tablename__ ='weibo_last_mentioned_post'
    uid = Column(BIGINT,primary_key=true)
    since_id = Column(VARCHAR(128))

class WeiboSearchLimitedLastAttitude(Base):
    __tablename__ ='weibo_search_limited_last_attitude'
    pid = Column(BIGINT,primary_key=true)
    created_at = Column(DATETIME)
    since_id = Column(VARCHAR(128))

class WeiboKolLastAttitude(Base):
    __tablename__ ='weibo_kol_last_attitude'
    pid = Column(BIGINT,primary_key=true)
    created_at = Column(DATETIME)
    since_id = Column(VARCHAR(128))

class WeiboSearchLimitedLastComment(Base):
    __tablename__ ='weibo_search_limited_last_comment'
    pid = Column(BIGINT,primary_key=true)
    created_at = Column(DATETIME)
    since_id = Column(VARCHAR(128))

class WeiboKolLastComment(Base):
    __tablename__ ='weibo_kol_last_comment'
    pid = Column(BIGINT,primary_key=true)
    created_at = Column(DATETIME)
    since_id = Column(VARCHAR(128))

class WeiboSearchLimitedLastRepost(Base):
    __tablename__ ='weibo_search_limited_last_repost'
    pid = Column(BIGINT,primary_key=true)
    created_at = Column(DATETIME)
    since_id = Column(VARCHAR(128))

class WeiboKolLastRepost(Base):
    __tablename__ ='weibo_kol_last_repost'
    pid = Column(BIGINT,primary_key=true)
    created_at = Column(DATETIME)
    since_id = Column(VARCHAR(128))

class WeiboMentionLastAttitude(Base):
    __tablename__ ='weibo_mention_last_attitude'
    pid = Column(BIGINT,primary_key=true)
    created_at = Column(DATETIME)
    since_id = Column(VARCHAR(128))

class WeiboMentionLastComment(Base):
    __tablename__ ='weibo_mention_last_comment'
    pid = Column(BIGINT,primary_key=true)
    created_at = Column(DATETIME)
    since_id = Column(VARCHAR(128))

class WeiboUserInfo(Base):
    __table__ = Table('weibo_user_info',metadata,autoload=True)

class MonsterWeiboPost(Base):
    __tablename__ ='monster_posts_weibo'
    post_id = Column(BIGINT,primary_key=true)

class MasterUidInitial(Base):
    __table__ = Table('master_uid_initial',metadata,autoload=True)
