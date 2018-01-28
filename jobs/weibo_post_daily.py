import sys
import pandas as pd
sys.path.append('/home/max/SocialSpace')
from SocialAPI.SocialAPI.SocialWeiboAPI import SocialWeiboAPI
from SocialAPI.Helper import Helper

if __name__ == '__main__':
    rootPath = Helper().getRootPath()
    df = pd.read_csv(rootPath + '/input/uid.csv',';')
    uidList = list(df['uid'].apply(str))
    uidGroup = [','.join(uidList[i:i+20]) for i in range(0,103,20)]

    weibo = SocialWeiboAPI()
    for uids in uidGroup:
        weibo.getUserTimelineBatch(uids,count=200)