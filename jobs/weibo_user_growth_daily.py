import pandas as pd
from SocialAPI.SocialAPI.SocialWeiboAPI import SocialWeiboAPI
from SocialAPI.Helper import Helper
import asyncio
import uvloop
from SocialAPI.Model import Kol
from datetime import datetime
import os

if __name__ == '__main__':
    rootPath = Helper().getRootPath()

    weibo = SocialWeiboAPI()

    session = weibo.createSession()
    uids = session.query(Kol.uid).filter(Kol.status == 1).all()
    uidList = [str(uid[0]) for uid in uids]
    uidGroup = [','.join(uidList[i:i + 100]) for i in range(0, len(uidList), 100)]

    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    loop = asyncio.new_event_loop()
    tasks = [asyncio.ensure_future(weibo.getUsersCountBatch(uids),loop=loop) for uids in uidGroup]
    loop.run_until_complete(asyncio.wait(tasks))
    result = [task.result() for task in tasks]
    df = pd.concat(result, ignore_index=True)
    filePath = rootPath + '/output/weibo_user_growth_daily'
    os.makedirs(filePath, exist_ok=True)
    #fileName = 'weibo_user_growth_daily_' + datetime.now().strftime("%Y_%m_%d_%H_%M_%S") + '.csv'
    filePath = filePath + '/' + 'weibo_user_info.csv'
    if os.path.exists(filePath):
        weibo.writeDataFrameToCsv(df, filePath, sep="|", header=False)
    else:
        weibo.writeDataFrameToCsv(df, filePath, sep="|")
    loop.close()
    session.close()
