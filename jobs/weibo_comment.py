
import pandas as pd
from SocialAPI.SocialAPI.SocialWeiboAPI import SocialWeiboAPI
from SocialAPI.Helper import Helper
from SocialAPI.Model import engine, PostStatus, Comment

if __name__ == '__main__':

    weibo = SocialWeiboAPI()
    session = weibo.createSession()
    last_week = weibo.getStrTime(7)
    #pids = session.query(PostStatus.id).order_by(PostStatus.id.desc()).limit(1).all()
    pids = session.query(PostStatus.id).filter(PostStatus.created_at>last_week).order_by(PostStatus.created_at.desc()).all()
    for pid in pids:
        weibo.getCommentsShow(pid[0],count=200)