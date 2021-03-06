from SocialAPI.SocialAPI.WeiboAPI import SocialWeiboAPI
from SocialAPI.Helper import Helper
import asyncio
import uvloop
from SocialAPI.Model import MasterUid


if __name__ == '__main__':
    rootPath = Helper().getRootPath()

    weibo = SocialWeiboAPI()

    session = weibo.createSession()
    uids = session.query(MasterUid.uid)\
        .filter(MasterUid.crawl_master==1,MasterUid.crawl_user_growth==1)\
        .all()
    uidList = [str(uid[0]) for uid in uids]

    uidGroup = [','.join(uidList[i:i + 100]) for i in range(0, len(uidList), 100)]

    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    loop = asyncio.new_event_loop()
    tasks = [asyncio.ensure_future(weibo.get_users_count_batch(uids),loop=loop) for uids in uidGroup]
    loop.run_until_complete(asyncio.wait(tasks))
    result = [task.result() for task in tasks]

    loop.close()
    session.close()
