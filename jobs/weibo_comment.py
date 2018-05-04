import pandas as pd
from SocialAPI.SocialAPI.SocialWeiboAPI import SocialWeiboAPI
from SocialAPI.Helper import Helper
from SocialAPI.Model import engine, PostStatus, Comment

if __name__ == '__main__':
    # Get the last 2000 comments for each post at most
    rootPath = Helper().getRootPath()
    df = pd.read_csv(rootPath + '/input/uid.csv', ';')
    uidList = list(df['uid'].apply(str))
    df.drop_duplicates(inplace=True)

    weibo = SocialWeiboAPI()
    session = weibo.createSession()
    startTime = weibo.getStrTime(-7)
    pidList = []
    for uid in uidList:

        #pids = session.query(PostStatus.id).order_by(PostStatus.id.desc()).limit(1).all()
        #pids = session.query(PostStatus.id).filter(PostStatus.created_at>last_week).order_by(PostStatus.created_at.desc()).all()
        pids = session.query(PostStatus.id).filter(PostStatus.uid==uid, PostStatus.created_at>=startTime).all()
        pidList += pids
    session.close()
    for pid in pidList:
        weibo.getCommentsShow(pid[0],count=200)