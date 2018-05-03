import pandas as pd
from SocialAPI.SocialAPI.SocialWeiboAPI import SocialWeiboAPI
from SocialAPI.Helper import Helper

if __name__ == '__main__':
    rootPath = Helper().getRootPath()
    df = pd.read_csv(rootPath + '/input/uid.csv',';')
    uidList = list(df['uid'].apply(str))

    weibo = SocialWeiboAPI()

    for uid in uidList:
        weibo.getUserTimelineOther(uid, start_day=-7)