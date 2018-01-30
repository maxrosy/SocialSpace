import sys
import pandas as pd
sys.path.append('/home/max/SocialSpace')
from SocialAPI.SocialAPI.SocialWeiboAPI import SocialWeiboAPI
from SocialAPI.Helper import Helper

if __name__ == '__main__':
    rootPath = Helper().getRootPath()
    df = pd.read_csv(rootPath + '/input/uid.csv',';')
    uidList = list(df['uid'].apply(str))
    n = 101 #len(df)
    uidGroup = [','.join(uidList[i:i+100]) for i in range(0,n,100)]

    weibo = SocialWeiboAPI()
    for uids in uidGroup:
        weibo.getUsersCountBatch(uids)