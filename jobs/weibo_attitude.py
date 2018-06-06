import pandas as pd
from SocialAPI.SocialAPI.SocialWeiboAPI import SocialWeiboAPI
from SocialAPI.Helper import Helper
from SocialAPI.Model import PostStatus

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
        pids = session.query(PostStatus.id).filter(PostStatus.uid==uid, PostStatus.created_at>=startTime).all()
        pidList += pids
    session.close()
    for pid in pidList:
        weibo.getAttitudesShow(pid[0],count=100)