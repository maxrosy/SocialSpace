import pandas as pd
from SocialAPI.SocialAPI.WeiboAPI import SocialWeiboAPI
from SocialAPI.Helper import Helper
import asyncio
import uvloop
from datetime import datetime
from SocialAPI.Model import Kol
import os

if __name__ == '__main__':
    rootPath = Helper().getRootPath()

    weibo = SocialWeiboAPI()
    session = weibo.createSession()
    uids = session.query(Kol.uid).all()
    uidList = [str(uid[0]) for uid in uids]
    #uidList = ['1828260462']
    uidGroup = [','.join(uidList[i:i + 50]) for i in range(0, len(uidList), 50)]


    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    loop = asyncio.new_event_loop()
    tasks = [asyncio.ensure_future(weibo.getUserShowBatchOther(uids), loop=loop) for uids in uidGroup]
    loop.run_until_complete(asyncio.wait(tasks))
    result = [task.result() for task in tasks]

    loop.close()
    session.close()





