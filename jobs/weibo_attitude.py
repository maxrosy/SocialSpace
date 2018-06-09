import pandas as pd
from SocialAPI.SocialAPI.SocialWeiboAPI import SocialWeiboAPI
from SocialAPI.Helper import Helper
from SocialAPI.Model import PostStatus, Kol

if __name__ == '__main__':
    # Get the last 2000 comments for each post at most
    rootPath = Helper().getRootPath()

    weibo = SocialWeiboAPI()
    session = weibo.createSession()
    startTime = weibo.getStrTime(-7)
    pidList = []
    uids = session.query(Kol.uid).filter(Kol.status == 1).all()
    uidList = [uid[0] for uid in uids]

    for uid in uidList:
        pids = session.query(PostStatus.id).filter(PostStatus.uid==uid, PostStatus.created_at>=startTime).all()
        pidList += pids
    session.close()
    for pid in pidList:
        weibo.getAttitudesShow(pid[0],count=100)