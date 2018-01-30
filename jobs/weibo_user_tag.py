import pandas as pd
from SocialAPI.SocialAPI.SocialWeiboAPI import SocialWeiboAPI
from SocialAPI.Helper import Helper
import asyncio
import uvloop

if __name__ == '__main__':
    rootPath = Helper().getRootPath()
    df = pd.read_csv(rootPath + '/input/uid.csv',';')
    uidList = list(df['uid'].apply(str))
    n = 101 # len(df)
    uidGroup = [','.join(uidList[i:i+20]) for i in range(0,n,20)]

    weibo = SocialWeiboAPI()

    tasks = [weibo.getTagsBatchOther(uids) for uids in uidGroup]
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    loop = asyncio.new_event_loop()
    loop.run_until_complete(asyncio.wait(tasks))
    loop.close()